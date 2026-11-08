import hashlib
import io
import json
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import ModuleType
from unittest.mock import Mock, patch

from boxx_readback import main


EXPECTED_MEMBERS = ("registration.json", "manifest.json", "actions/BOXX/page-001.json")
PREFIX = "research/v2/input/fixture/"
PRIVATE_MARKER = "fixture-private-value"


def fixture(rate, *, event_changes=None, page_token=None, complete=True, fail=None):
    bodies = {}
    event = {
        "id": PRIVATE_MARKER, "symbol": "BOXX", "currency": "USD",
        "ex_date": "2024-08-13", "payable_date": "2024-08-14", "rate": rate,
    }
    event.update(event_changes or {})
    page = EXPECTED_MEMBERS[-1]

    def record(name, body):
        bodies[name] = json.dumps(body).encode()
        return {
            "name": name, "generation": "7", "bytes": len(bodies[name]),
            "sha256": hashlib.sha256(bodies[name]).hexdigest(),
        }

    member = record(page, {
        "corporate_actions": {"cash_dividends": [event]}, "next_page_token": page_token,
    })
    manifest = record("manifest.json", {
        "schema_version": "qsl.research.raw_sip_input.v1",
        "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
        "window_start": "2021-12-01", "window_end": "2026-08-25",
        "no_order": True, "research_only": True, "currency": "USD",
        "inputs": [
            {"symbol": "BOXX", "kind": "actions", "count": 1, "complete_pagination": complete,
             "pages": [member]},
            {"symbol": "SOXL", "kind": "actions"},
        ],
    })
    registration = record("registration.json", {
        "schema_version": "rs04.archive-registration.v1",
        "producer_revision": "synthetic", "no_order": True, "manifest": manifest,
        "members": [member, {"name": "actions/SOXL/page-001.json"}],
    })
    binding = {
        "input_root": "gs://fixture-archive/research/v2/input/",
        "dataset": "fixture", "producer_revision": "synthetic", "project_id": "fixture-project",
        "registration": registration, "manifest": manifest,
    }

    calls = []

    def blob(name, generation):
        member_name = name.removeprefix(PREFIX)
        body = bodies[member_name]
        obj = Mock(size=len(body), generation=str(generation))

        def request(kind, **kwargs):
            calls.append((member_name, kind, generation, kwargs))
            if fail == (member_name, kind):
                raise RuntimeError(PRIVATE_MARKER)
            return body

        obj.reload.side_effect = lambda **kwargs: request("metadata", **kwargs)
        obj.download_as_bytes.side_effect = lambda **kwargs: request("body", **kwargs)
        return obj

    client = Mock()
    client.bucket.return_value.blob.side_effect = blob
    return binding, client, calls


class BoxxReadbackTests(unittest.TestCase):
    def run_main(self, data, client, expected):
        storage = Mock()
        storage.Client.return_value = client
        cloud = ModuleType("google.cloud")
        cloud.storage = storage
        out, err = io.StringIO(), io.StringIO()
        with (
            patch.dict(os.environ, {"RUNNER_OS": "Linux", "RUNNER_TEMP": "/fixture"}),
            patch.dict(sys.modules, {"google.cloud": cloud}),
            patch("inspect_gcs.binding", return_value=data),
            redirect_stdout(out), redirect_stderr(err),
        ):
            status = main()
        storage.Client.assert_called_once_with(project=data["project_id"])
        self.assertEqual(out.getvalue(), expected + "\n")
        self.assertEqual(err.getvalue(), "")
        return status

    def test_all_amount_outcomes_and_cent_boundaries(self):
        for rate, code in (
            ("0.2905900", "BOXX_MATCH_CBOE_EXACT"),
            (0.29059, "BOXX_MATCH_CBOE_EXACT"),
            ("0.285", "BOXX_DIFFERENT_EXACT_SAME_CENT"),
            ("0.294999", "BOXX_DIFFERENT_EXACT_SAME_CENT"),
            ("0.295", "BOXX_DIFFERENT_CENT"),
            ("0.284999", "BOXX_DIFFERENT_CENT"),
        ):
            with self.subTest(rate=rate):
                data, client, calls = fixture(rate)
                self.assertEqual(self.run_main(data, client, code), 0)
                self.assertEqual([(name, kind) for name, kind, _, _ in calls], [
                    (name, kind) for name in EXPECTED_MEMBERS for kind in ("metadata", "body")
                ])
                for _, kind, generation, kwargs in calls:
                    self.assertEqual(generation, 7)
                    self.assertEqual(kwargs, {
                        "if_generation_match": 7, "retry": None, "timeout": 30,
                        **({"raw_download": True} if kind == "body" else {}),
                    })

    def test_date_or_old_receipt_mismatch_stops_without_amount(self):
        for rate, changes, code in (
            ("0.29", {}, "BOXX_PRIOR_RECEIPT_MISMATCH"),
            ("0.291", {"payable_date": "2024-08-15"}, "BOXX_EVENT_MISMATCH"),
            ("0.291", {"ex_date": "2024-08-12"}, "BOXX_EVENT_MISMATCH"),
        ):
            with self.subTest(code=code):
                data, client, calls = fixture(rate, event_changes=changes)
                self.assertEqual(self.run_main(data, client, code), 2)
                self.assertEqual(len(calls), 6)

    def test_bad_amount_event_scope_or_page_stops(self):
        for rate, options in (
            (PRIVATE_MARKER, {}), ("NaN", {}), (True, {}), ("0", {}),
            ("0.291", {"event_changes": {"currency": "EUR"}}),
            ("0.291", {"event_changes": {"status": "cancelled"}}),
            ("0.291", {"page_token": PRIVATE_MARKER}),
            ("0.291", {"complete": False}),
        ):
            with self.subTest(rate=rate, options=options):
                data, client, calls = fixture(rate, **options)
                self.assertEqual(self.run_main(data, client, "BOXX_READ_STOPPED"), 2)
                self.assertEqual(len(calls), 4 if options.get("complete") is False else 6)

    def test_registration_mismatch_and_transport_failure_stop_without_retry(self):
        data, client, calls = fixture("0.291")
        data["producer_revision"] = PRIVATE_MARKER
        self.assertEqual(self.run_main(data, client, "BOXX_READ_STOPPED"), 2)
        self.assertEqual(len(calls), 2)

        data, client, calls = fixture("0.291", fail=("manifest.json", "metadata"))
        self.assertEqual(self.run_main(data, client, "BOXX_READ_STOPPED"), 2)
        self.assertEqual([(name, kind) for name, kind, _, _ in calls], [
            ("registration.json", "metadata"), ("registration.json", "body"),
            ("manifest.json", "metadata"),
        ])

    def test_unapproved_runner_stops_before_binding(self):
        out = io.StringIO()
        with (
            patch.dict(os.environ, {"RUNNER_OS": "fixture", "RUNNER_TEMP": "/fixture"}),
            patch("inspect_gcs.binding") as binding,
            redirect_stdout(out),
        ):
            self.assertEqual(main(), 2)
        self.assertEqual(out.getvalue(), "BOXX_RUNNER_NOT_APPROVED\n")
        binding.assert_not_called()


if __name__ == "__main__":
    unittest.main()
