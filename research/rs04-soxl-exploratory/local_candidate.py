"""Local candidate only: injected reader, no authentication, result write, or CLI."""

import hashlib
import io
import json
import re
from datetime import date
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from archive_bridge import normalize_archive
from registered_history import (
    REGISTERED_CAPS,
    REGISTERED_COST_POLICY,
    evaluate_registered_history,
    registered_calendar,
)
from private_result import project_result
from soxl_reconcile import SYMBOLS, archive_events, issuer_events, reference

ISSUER_SHA = "7c0308adae85aec65b9cf791bc8cedb1d3c8caaeb126990a5a184f78f56371e1"
CONFIG_SHA = "99bfd7446da13d39cfe734fd4bbbf86ca6722d411cbf71a7f68059d9d4abffaa"
NAMES = frozenset(
    {"registration.json", "manifest.json"}
    | {f"{kind}/{symbol}/page-001.json" for kind in ("actions", "bars") for symbol in SYMBOLS}
)


def digest(body):
    return hashlib.sha256(body).hexdigest()


def qualify(plan_bytes, reviewed_digest, evidence, config_bytes, issuer_bytes):
    """Require a reviewed bound plan and exact evidence bytes; authenticity needs external review."""
    if not re.fullmatch(r"[0-9a-f]{64}", reviewed_digest) or digest(plan_bytes) != reviewed_digest:
        raise ValueError("PLAN_UNBOUND")
    if digest(config_bytes) != CONFIG_SHA or digest(issuer_bytes) != ISSUER_SHA:
        raise ValueError("FROZEN_INPUT_MISMATCH")
    plan = json.loads(plan_bytes)
    if plan.get("schema") != "rs04.soxl.local-plan.v1" or not re.fullmatch(r"[0-9a-f]{64}", plan.get("manifest_sha256", "")):
        raise ValueError("PLAN_SCOPE_MISMATCH")
    rows = plan.get("split_coverage")
    if not isinstance(rows, dict) or set(rows) != set(SYMBOLS) or set(evidence) != set(SYMBOLS):
        raise ValueError("SPLIT_COVERAGE_MISSING")
    for symbol in SYMBOLS:
        row = rows[symbol]
        if (
            not isinstance(row, dict)
            or row.get("start") != ("2022-12-28" if symbol == "BOXX" else "2021-12-01")
            or row.get("end") != "2026-08-25"
            or row.get("scope") != "forward_and_reverse_splits_complete"
            or not isinstance(row.get("source"), str)
            or not row["source"].startswith("https://")
            or not isinstance(evidence[symbol], bytes)
            or not evidence[symbol]
            or row.get("evidence_sha256") != digest(evidence[symbol])
        ):
            raise ValueError("SPLIT_COVERAGE_MISMATCH")
    expected = issuer_events(io.StringIO(issuer_bytes.decode("utf-8"), newline=""))
    for symbol in SYMBOLS:
        rows = plan["split_coverage"][symbol].get("events")
        if not isinstance(rows, list):
            raise ValueError("SPLIT_EVENTS_MISSING")
        try:
            normalized = [(date.fromisoformat(row["date"]), Fraction(Decimal(row["ratio"]))) for row in rows]
        except (KeyError, TypeError, ValueError, ArithmeticError):
            raise ValueError("SPLIT_EVENTS_INVALID") from None
        from collections import Counter

        if any(ratio <= 0 for _, ratio in normalized) or Counter(normalized) != Counter(expected[symbol]["split"]):
            raise ValueError("SPLIT_EVENTS_MISMATCH")
    config = json.loads(config_bytes)
    if config["production_effective"] is not False:
        raise ValueError("CONFIG_SCOPE_MISMATCH")
    return config["effective_core_configs"]["SOXL"]


def prepare(scratch, data, read_exact, issuer_bytes, expected_manifest_sha256):
    """Fetch actions and compare all events before fetching any price page."""
    if data["manifest"]["sha256"] != expected_manifest_sha256:
        raise ValueError("MANIFEST_MISMATCH")
    remaining, cache = set(NAMES), {}

    def read(name, ref):
        if name in cache:
            if cache[name][0] != ref:
                raise ValueError("REFERENCE_MISMATCH")
            return cache[name][1]
        if name not in remaining:
            raise ValueError("READ_SCOPE_MISMATCH")
        remaining.remove(name)
        body = read_exact(name, ref)
        if len(body) != ref[1] or digest(body) != ref[2]:
            raise ValueError("BYTES_MISMATCH")
        cache[name] = (ref, body)
        return body

    expected = issuer_events(io.StringIO(issuer_bytes.decode("utf-8"), newline=""))
    actual = archive_events(data, read)
    from collections import Counter

    if any(Counter(actual[s][kind]) != Counter(expected[s][kind]) for s in SYMBOLS for kind in ("cash", "split")):
        raise ValueError("EVENT_MISMATCH")
    reg = json.loads(cache["registration.json"][1])
    manifest_raw = cache["manifest.json"][1]
    manifest = json.loads(manifest_raw)
    members = {x["name"]: x for x in reg["members"]}
    bar_refs = {}
    for symbol in SYMBOLS:
        items = [x for x in manifest["inputs"] if x["kind"] == "bars" and x["symbol"] == symbol]
        name = f"bars/{symbol}/page-001.json"
        if len(items) != 1 or items[0]["complete_pagination"] is not True or len(items[0]["pages"]) != 1:
            raise ValueError("BAR_SCOPE_MISMATCH")
        page, ref = items[0]["pages"][0], reference(members[name])
        if page["name"] != name or (page["bytes"], page["sha256"]) != ref[1:]:
            raise ValueError("BAR_REFERENCE_MISMATCH")
        bar_refs[name] = ref
    for name, ref in bar_refs.items():
        read(name, ref)
    if remaining:
        raise ValueError("READ_SCOPE_INCOMPLETE")
    for name, (_, body) in cache.items():
        if name == "registration.json":
            continue
        path = Path(scratch) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    calendar, _ = registered_calendar()
    source = normalize_archive(
        Path(scratch),
        expected_manifest_sha256,
        expected_sessions={s: calendar[s] for s in SYMBOLS},
        event_coverage=set(SYMBOLS),
        profiles=("SOXL",),
    )
    source.receipt["archive_source_binding"] = digest(
        json.dumps([(name, cache[name][0]) for name in sorted(cache)], separators=(",", ":")).encode()
    )
    return source


def gcs_reader(client, data):
    """Bind the existing exact GCS reader; caller creates the client only after qualification."""
    from inspect_gcs import exact_read

    bucket = client.bucket(data["input_root"].split("/")[2])
    prefix = f"research/v2/input/{data['dataset']}/"
    return lambda name, ref: exact_read(bucket, prefix + name, ref)


def run_local(
    *, plan_bytes, reviewed_digest, evidence, config_bytes, issuer_bytes, data, client_factory, scratch, source_sha
):
    """Return an aggregate object only in memory. client_factory is never called before qualification."""
    config = qualify(plan_bytes, reviewed_digest, evidence, config_bytes, issuer_bytes)
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("SOURCE_SHA_INVALID")
    expected_manifest_sha256 = json.loads(plan_bytes)["manifest_sha256"]
    if data["manifest"]["sha256"] != expected_manifest_sha256:
        raise ValueError("MANIFEST_MISMATCH")
    read_exact = client_factory()
    source = prepare(scratch, data, read_exact, issuer_bytes, expected_manifest_sha256)
    result = evaluate_registered_history(
        source,
        expected_manifest_sha256=expected_manifest_sha256,
        effective_core_configs={"SOXL": config},
        input_bindings={"SOXL": reviewed_digest},
        confirmed_research_caps={"SOXL": REGISTERED_CAPS["SOXL"]},
        cash_source="BOXX",
        confirmed_cost_policy=REGISTERED_COST_POLICY,
        profiles=("SOXL",),
    )
    result["d10"] = REGISTERED_COST_POLICY
    result["provenance"] = {
        "source_sha": source_sha,
        "manifest_sha256": expected_manifest_sha256,
        "issuer_csv_sha256": ISSUER_SHA,
        "plan_sha256": reviewed_digest,
        "archive_source_binding": source.receipt["archive_source_binding"],
        "calendar_version": result["registered_history"]["calendar_version"],
    }
    return project_result(result)


def run_local_status(**kwargs):
    """Fixed public status and private in-memory aggregate; never writes a result."""
    try:
        result = run_local(**kwargs)
    except Exception:
        return {"status": "PARKED", "reason_code": "SOXL_LOCAL_CANDIDATE_STOPPED"}, None
    return {"status": "COMPUTED", "reason_code": "SOXL_LOCAL_AGGREGATE_READY"}, result
