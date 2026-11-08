"""Synthetic storage -> exact reader -> real archive bridge -> SOXL matrix -> projection."""

import json
import math
import random

import exploratory as c
import registered_history
from check_local_candidate import CONFIG, ISSUER, setup_objects


def test_same_call_e2e(tmp_path, monkeypatch):
    objects, data = setup_objects(monkeypatch)
    calendar, _ = c.registered_calendar()
    manifest = json.loads(objects["manifest.json"])
    manifest.update(feed="sip", price_adjustment="raw")
    registration = json.loads(objects["registration.json"])
    for seed, symbol in enumerate(c.SYMBOLS, 1):
        rng, value, rows = random.Random(seed), 100.0, []
        for day in calendar[symbol]:
            value *= math.exp(rng.gauss(0.0003, 0.012 if symbol != "BOXX" else 0.0002))
            if symbol == "SOXX" and day.isoformat() == "2024-03-07":
                value /= 3
            rows.append({"t": f"{day}T05:00:00Z", "o": value, "h": value, "l": value, "c": value, "v": 1000})
        name = f"bars/{symbol}/page-001.json"
        body = json.dumps({"symbol": symbol, "bars": rows, "next_page_token": None}).encode()
        objects[name] = body
        member = next(m for m in registration["members"] if m["name"] == name)
        member.update(bytes=len(body), sha256=c.digest(body))
        item = next(i for i in manifest["inputs"] if i["symbol"] == symbol and i["kind"] == "bars")
        item["count"] = len(rows)
        item["pages"][0].update(bytes=len(body), sha256=c.digest(body))
    body = json.dumps(manifest).encode()
    objects["manifest.json"] = body
    manifest_ref = {"name": "manifest.json", "generation": "1", "bytes": len(body), "sha256": c.digest(body)}
    registration["manifest"] = manifest_ref
    objects["registration.json"] = json.dumps(registration).encode()
    data.update(
        input_root="gs://synthetic/research/v2/input",
        dataset="fixture",
        manifest=manifest_ref,
        registration={
            "generation": "1",
            "bytes": len(objects["registration.json"]),
            "sha256": c.digest(objects["registration.json"]),
        },
    )
    monkeypatch.setattr(c, "MANIFEST_SHA256", manifest_ref["sha256"])
    monkeypatch.setattr(registered_history, "MANIFEST_SHA256", manifest_ref["sha256"])
    plan = json.dumps({"schema": "rs04.soxl.exploratory-plan.v1",
                       "manifest_sha256": manifest_ref["sha256"],
                       "independent_full_split_coverage": "UNPROVEN"}).encode()
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

    status, result = c.run_local_status(
        plan_bytes=plan,
        reviewed_digest=c.digest(plan),
        config_bytes=CONFIG,
        issuer_bytes=ISSUER,
        data=data,
        client_factory=lambda: c.gcs_reader(Client(), data),
        scratch=tmp_path,
        source_sha="a" * 40,
    )
    assert status == {"status": "COMPUTED", "reason_code": "SOXL_EXPLORATORY_AGGREGATE_READY", "classification": "EXPLORATORY"}
    assert len(calls) == 16
    assert {name.removeprefix("research/v2/input/fixture/") for _, name in calls} == c.NAMES
    assert len(result["scenarios"]) == 21
    assert {x["profile"] for x in result["scenarios"]} == {"SOXL"}
    assert {x["status"] for x in result["scenarios"]} == {"COMPUTED"}
    assert len(result["primary"]) == 2
    assert all(x["main"]["sessions"] == 567 and x["tail"]["sessions"] == 32 for x in result["scenarios"])
    serialized = json.dumps(result, allow_nan=False)
    assert not any(
        s in serialized for s in ("orders", "opening_prices", "cash_dividends", "corporate_actions", "fixture/")
    )

    assert result["scope"]["classification"] == "EXPLORATORY"
    assert result["scope"]["independent_full_split_coverage"] == "UNPROVEN"
    assert result["scope"]["eligible_for_promotion"] is False
