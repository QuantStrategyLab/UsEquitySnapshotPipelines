import csv
import io
import json
from pathlib import Path
import pytest
import local_candidate as c

ROOT = Path(__file__).parent
CONFIG = (ROOT / "research-baseline-defaults.json").read_bytes()
ISSUER = (ROOT / "issuer-soxl.csv").read_bytes()


def qualification():
    evidence = {s: ("synthetic " + s).encode() for s in c.SYMBOLS}
    plan = {
        "schema": "rs04.soxl.local-plan.v1",
        "manifest_sha256": "b" * 64,
        "split_coverage": {
            s: {
                "start": "2022-12-28" if s == "BOXX" else "2021-12-01",
                "end": "2026-08-25",
                "scope": "forward_and_reverse_splits_complete",
                "source": "https://example.invalid/synthetic",
                "evidence_sha256": c.digest(evidence[s]),
                "events": [{"date": "2024-03-07", "ratio": "3"}] if s == "SOXX" else [],
            }
            for s in c.SYMBOLS
        },
    }
    raw = json.dumps(plan).encode()
    return raw, c.digest(raw), evidence


def test_coverage_stops_before_client(tmp_path):
    plan, sha, evidence = qualification()
    called = []
    for invalid in ({}, {**evidence, "BOXX": b"changed"}):
        with pytest.raises(ValueError, match="SPLIT_COVERAGE"):
            c.run_local(
                plan_bytes=plan,
                reviewed_digest=sha,
                evidence=invalid,
                config_bytes=CONFIG,
                issuer_bytes=ISSUER,
                data={},
                client_factory=lambda: called.append(True),
                scratch=tmp_path,
                source_sha="a" * 40,
            )
    assert called == []


def setup_objects(monkeypatch):
    events = {s: {"cash_dividends": [], "forward_splits": []} for s in c.SYMBOLS}
    for row in csv.DictReader(io.StringIO(ISSUER.decode())):
        s = row["symbol"]
        if row["event_kind"] == "cash_distribution":
            events[s]["cash_dividends"].append(
                {
                    "ex_date": row["ex_or_split_date"],
                    "payable_date": row["payable_date"],
                    "rate": row["issuer_amount_or_ratio"],
                }
            )
        else:
            events[s]["forward_splits"].append(
                {"ex_date": row["ex_or_split_date"], "new_rate": row["issuer_amount_or_ratio"], "old_rate": "1"}
            )
    objects, members, entries = {}, [], []
    for s in c.SYMBOLS:
        for kind in ("actions", "bars"):
            name = f"{kind}/{s}/page-001.json"
            obj = (
                {"corporate_actions": events[s], "next_page_token": None}
                if kind == "actions"
                else {"bars": [], "next_page_token": None}
            )
            body = json.dumps(obj).encode()
            objects[name] = body
            ref = {"name": name, "generation": "1", "bytes": len(body), "sha256": c.digest(body)}
            members.append(ref)
            entries.append(
                {
                    "kind": kind,
                    "symbol": s,
                    "count": sum(map(len, events[s].values())) if kind == "actions" else 0,
                    "complete_pagination": True,
                    "pages": [{k: ref[k] for k in ("name", "bytes", "sha256")}],
                }
            )
    manifest = {
        "schema_version": "qsl.research.raw_sip_input.v1",
        "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
        "window_start": "2021-12-01",
        "window_end": "2026-08-25",
        "no_order": True,
        "research_only": True,
        "currency": "USD",
        "inputs": entries,
    }
    objects["manifest.json"] = json.dumps(manifest).encode()
    manifest_ref = {
        "name": "manifest.json",
        "generation": "1",
        "bytes": len(objects["manifest.json"]),
        "sha256": c.digest(objects["manifest.json"]),
    }
    objects["registration.json"] = json.dumps(
        {
            "schema_version": "rs04.archive-registration.v1",
            "producer_revision": "a" * 40,
            "no_order": True,
            "manifest": manifest_ref,
            "members": members,
        }
    ).encode()
    data = {
        "producer_revision": "a" * 40,
        "manifest": manifest_ref,
        "registration": {
            "generation": "1",
            "bytes": len(objects["registration.json"]),
            "sha256": c.digest(objects["registration.json"]),
        },
    }
    return objects, data


def test_fixed_scope_events_before_prices(tmp_path, monkeypatch):
    objects, data = setup_objects(monkeypatch)
    calls = []

    def read(name, ref):
        calls.append(name)
        return objects[name]

    monkeypatch.setattr(c, "registered_calendar", lambda: ({s: [] for s in c.SYMBOLS}, "synthetic"))
    from types import SimpleNamespace

    monkeypatch.setattr(c, "normalize_archive", lambda *a, **kw: SimpleNamespace(receipt={}))
    assert len(c.prepare(tmp_path, data, read, ISSUER, data["manifest"]["sha256"]).receipt["archive_source_binding"]) == 64
    assert set(calls) == c.NAMES and len(calls) == 8
    assert all(name.startswith("actions/") for name in calls[2:5])
    calls.clear()
    bad = ISSUER.replace(b"0.290590", b"0.29")
    with pytest.raises(ValueError, match="EVENT_MISMATCH"):
        c.prepare(tmp_path, data, read, bad, data["manifest"]["sha256"])
    assert len(calls) == 5 and not any(x.startswith("bars/") for x in calls)


def test_original_exact_reader_generation_bytes_and_no_retry():
    from inspect_gcs import exact_read

    body = b"synthetic"

    class Blob:
        size = len(body)
        generation = 42

        def reload(self, **kw):
            assert kw == {"if_generation_match": 42, "retry": None, "timeout": 30}

        def download_as_bytes(self, **kw):
            assert kw == {"if_generation_match": 42, "raw_download": True, "retry": None, "timeout": 30}
            return body

    class Bucket:
        def blob(self, name, generation):
            assert name == "fixed" and generation == 42
            return Blob()

    assert exact_read(Bucket(), "fixed", ("42", len(body), c.digest(body))) == body
    with pytest.raises(ValueError, match="OBJECT_BYTES_MISMATCH"):
        exact_read(Bucket(), "fixed", ("42", len(body), "0" * 64))


def test_public_status_does_not_expose_exception(monkeypatch):
    def fail(**kwargs):
        raise ValueError("private synthetic detail")

    monkeypatch.setattr(c, "run_local", fail)
    status, result = c.run_local_status()
    assert status == {"status": "PARKED", "reason_code": "SOXL_LOCAL_CANDIDATE_STOPPED"}
    assert result is None and "private" not in json.dumps(status)


def test_independent_split_events_reject_extra_or_inverse(tmp_path):
    plan, _, evidence = qualification()
    for symbol, events in [
        ("SOXL", [{"date": "2024-03-07", "ratio": "2"}]),
        ("SOXX", [{"date": "2024-03-07", "ratio": "0.333333333333"}]),
    ]:
        altered = json.loads(plan)
        altered["split_coverage"][symbol]["events"] = events
        raw = json.dumps(altered).encode()
        called = []
        with pytest.raises(ValueError, match="SPLIT_EVENTS_MISMATCH"):
            c.run_local(
                plan_bytes=raw,
                reviewed_digest=c.digest(raw),
                evidence=evidence,
                config_bytes=CONFIG,
                issuer_bytes=ISSUER,
                data={},
                client_factory=lambda: called.append(True),
                scratch=tmp_path,
                source_sha="a" * 40,
            )
        assert not called


def test_gcs_reader_complete_request_budget(tmp_path, monkeypatch):
    from types import SimpleNamespace

    objects, data = setup_objects(monkeypatch)
    data.update(input_root="gs://synthetic/research/v2/input", dataset="fixture")
    calls = []

    class Blob:
        def __init__(self, name, generation):
            self.name, self.generation = name, generation
            self.body = objects[name.removeprefix("research/v2/input/fixture/")]
            self.size = len(self.body)

        def reload(self, **kw):
            assert kw == {"if_generation_match": 1, "retry": None, "timeout": 30}
            calls.append(("metadata", self.name))

        def download_as_bytes(self, **kw):
            assert kw == {"if_generation_match": 1, "raw_download": True, "retry": None, "timeout": 30}
            calls.append(("bytes", self.name))
            return self.body

    class Bucket:
        def blob(self, name, generation):
            return Blob(name, generation)

    class Client:
        def bucket(self, name):
            assert name == "synthetic"
            return Bucket()

    monkeypatch.setattr(c, "registered_calendar", lambda: ({s: [] for s in c.SYMBOLS}, "synthetic"))
    monkeypatch.setattr(c, "normalize_archive", lambda *a, **kw: SimpleNamespace(receipt={}))
    c.prepare(tmp_path, data, c.gcs_reader(Client(), data), ISSUER, data["manifest"]["sha256"])
    assert len(calls) == 16 and sum(k == "metadata" for k, _ in calls) == 8
    assert {n.removeprefix("research/v2/input/fixture/") for _, n in calls} == c.NAMES
