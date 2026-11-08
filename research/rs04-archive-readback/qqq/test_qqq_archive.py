import csv
import hashlib
import io
import json
import unittest
from copy import deepcopy
from datetime import date

from qqq_archive import ARCHIVE_COUNT, END, ISSUER_COUNT, ISSUER_URL, PAGE_NAME, START, reconcile


class FixedArchive:
    def __init__(self):
        self.rows = [
            {"id": f"fixture-{i}", "symbol": "QQQ", "currency": "USD",
             "ex_date": date(2022, 1, i).isoformat(), "payable_date": "2022-01-25", "rate": "0.25"}
            for i in range(1, ARCHIVE_COUNT + 1)
        ]
        self.issuer = [
            {"symbol": "QQQ", "event_kind": "cash_distribution", "ex_or_split_date": row["ex_date"],
             "payable_date": row["payable_date"], "issuer_amount_or_ratio": row["rate"],
             "amount_basis": "issuer_reported_usd_per_share_unreconciled", "source_url": ISSUER_URL}
            for row in self.rows[:ISSUER_COUNT]
        ]
        self.calls = []
        self.objects = {}

    def public_csv(self):
        output = io.StringIO()
        writer = csv.DictWriter(output, self.issuer[0].keys())
        writer.writeheader()
        writer.writerows(self.issuer)
        output.seek(0)
        return output

    def put(self, name, data, generation):
        body = json.dumps(data).encode()
        ref = (generation, len(body), hashlib.sha256(body).hexdigest())
        self.objects[name] = (ref, body)
        return {"name": name, "generation": ref[0], "bytes": ref[1], "sha256": ref[2]}

    def bind(self, *, wrong_member=False):
        page = self.put(PAGE_NAME, {"corporate_actions": {"cash_dividends": self.rows}}, "1")
        manifest = self.put("manifest.json", {
            "schema_version": "qsl.research.raw_sip_input.v1",
            "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
            "window_start": START.isoformat(), "window_end": END.isoformat(),
            "no_order": True, "research_only": True, "currency": "USD",
            "inputs": [{"kind": "actions", "symbol": "QQQ", "count": ARCHIVE_COUNT,
                        "complete_pagination": True, "pages": [page]},
                       {"kind": "bars", "symbol": "QQQ"}],
        }, "2")
        member = deepcopy(page)
        if wrong_member:
            member["sha256"] = "a" * 64
        registration = self.put("registration.json", {
            "schema_version": "rs04.archive-registration.v1", "no_order": True,
            "producer_revision": "fixture-source", "manifest": manifest,
            "members": [member, {"name": "bars/QQQ/page-001.json"}],
        }, "3")
        return {"registration": registration, "manifest": manifest, "producer_revision": "fixture-source"}

    def read(self, name, ref):
        self.calls.append(name)
        expected, body = self.objects[name]
        if ref != expected or hashlib.sha256(body).hexdigest() != ref[2]:
            raise ValueError("OBJECT_BYTES_MISMATCH")
        return body


class FixedArchiveTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FixedArchive()

    def run_candidate(self, **bind):
        f = self.fixture
        return reconcile(f.bind(**bind), f.read, f.public_csv())

    def test_reads_metadata_and_one_actions_page_and_preserves_extra_row(self):
        result = self.run_candidate()
        self.assertEqual(result["status"], "QQQ_COMPARED")
        self.assertEqual(result["exact_matches"], 20)
        self.assertEqual(result["classification_uncertain"], 20)
        self.assertEqual(result["archive_unmatched"], 1)
        self.assertEqual(self.fixture.calls, ["registration.json", "manifest.json", PAGE_NAME])
        self.assertTrue(all(type(value) is int for key, value in result.items() if key != "status"))

    def test_mismatched_member_blocks_read_of_that_page(self):
        self.assertEqual(self.run_candidate(wrong_member=True),
                         {"status": "PARKED", "reason_code": "QQQ_MEMBER_IDENTITY_MISMATCH"})
        self.assertEqual(self.fixture.calls, ["registration.json", "manifest.json"])

    def test_issuer_mismatch_blocks_every_read(self):
        self.fixture.issuer.pop()
        self.assertEqual(self.run_candidate(),
                         {"status": "PARKED", "reason_code": "ISSUER_COUNT_MISMATCH"})
        self.assertEqual(self.fixture.calls, [])

    def test_tampered_bytes_fail_closed(self):
        binding = self.fixture.bind()
        ref, data = self.fixture.objects[PAGE_NAME]
        self.fixture.objects[PAGE_NAME] = (ref, data + b" ")
        self.assertEqual(reconcile(binding, self.fixture.read, self.fixture.public_csv()),
                         {"status": "PARKED", "reason_code": "OBJECT_BYTES_MISMATCH"})

    def test_distinct_native_ids_preserve_identical_same_day_rows(self):
        self.fixture.rows[-1]["ex_date"] = self.fixture.rows[0]["ex_date"]
        self.assertEqual(self.run_candidate()["archive_unmatched"], 1)

    def test_same_day_row_without_native_id_stops(self):
        self.fixture.rows[-1]["ex_date"] = self.fixture.rows[0]["ex_date"]
        self.fixture.rows[-1].pop("id")
        self.assertEqual(self.run_candidate(),
                         {"status": "PARKED", "reason_code": "QQQ_SAME_DAY_IDENTITY_UNVERIFIED"})

    def test_boolean_amount_stops(self):
        self.fixture.rows[-1]["rate"] = True
        self.assertEqual(self.run_candidate(),
                         {"status": "PARKED", "reason_code": "CASH_EVENT_INVALID"})


if __name__ == "__main__":
    unittest.main()
