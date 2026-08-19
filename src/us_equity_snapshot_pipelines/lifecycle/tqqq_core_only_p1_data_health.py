"""Pure structural health assessment for frozen TQQQ P1 input payloads.

The module deliberately has no provider, storage, credential, retry, or replay
dependency.  It turns an already acquired bars payload into a deterministic
health result that an external scheduler may record and act on.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date

from .tqqq_core_only_p1_binding import (
    INPUT_CONTRACT_ID,
    P2_V4_CONTRACT,
    TqqqCoreOnlyCandidateContract,
    build_tqqq_core_only_p1_binding_for_contract,
    expected_tqqq_core_only_sessions_for_contract,
    tqqq_core_only_p1_binding_sha256_for_contract,
)

_SCHEMA_VERSION = "qsl.tqqq_core_only_p1_input_health.v1"
_UNIVERSE = ("QQQ", "TQQQ", "QQQM", "BOXX")


def _canonical_sha256(value: object) -> str | None:
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(canonical).hexdigest()


def _base_result(
    contract: TqqqCoreOnlyCandidateContract,
    *,
    observed_at: str,
) -> dict[str, object]:
    if not isinstance(observed_at, str) or not observed_at:
        raise ValueError("observed_at must be a non-empty string")
    binding = build_tqqq_core_only_p1_binding_for_contract(contract)
    candidate = binding["candidate"]
    assert isinstance(candidate, Mapping)
    return {
        "schema_version": _SCHEMA_VERSION,
        "candidate": dict(candidate),
        "research_input_contract_id": INPUT_CONTRACT_ID,
        "binding_sha256": tqqq_core_only_p1_binding_sha256_for_contract(binding, contract),
        "observed_at": observed_at,
    }


def _ranges(sessions: tuple[date, ...], missing: set[date]) -> list[dict[str, str]]:
    ranges: list[dict[str, str]] = []
    start: date | None = None
    end: date | None = None
    for session in sessions:
        if session in missing:
            if start is None:
                start = session
            end = session
        elif start is not None and end is not None:
            ranges.append({"start": start.isoformat(), "end": end.isoformat()})
            start = None
            end = None
    if start is not None and end is not None:
        ranges.append({"start": start.isoformat(), "end": end.isoformat()})
    return ranges


def _parse_sessions(value: object) -> tuple[date, ...] | None:
    if not isinstance(value, Mapping) or not isinstance(value.get("bars"), list):
        return None
    sessions: list[date] = []
    try:
        for bar in value["bars"]:
            if not isinstance(bar, Mapping) or not isinstance(bar.get("t"), str):
                return None
            timestamp = bar["t"]
            if len(timestamp) < 10:
                return None
            sessions.append(date.fromisoformat(timestamp[:10]))
    except ValueError:
        return None
    return tuple(sessions)


def build_tqqq_core_only_p1_input_unavailable_health(
    *,
    observed_at: str,
    contract: TqqqCoreOnlyCandidateContract = P2_V4_CONTRACT,
) -> dict[str, object]:
    """Record a temporary provider outage without invalidating prior snapshots."""
    result = _base_result(contract, observed_at=observed_at)
    result.update(
        {
            "status": "DEFERRED",
            "verdict": "INCONCLUSIVE",
            "reason_codes": ["INPUT_UNAVAILABLE"],
            "bars_payload_sha256": None,
            "coverage": {},
        }
    )
    return result


def assess_tqqq_core_only_p1_input_health(
    payload: object,
    *,
    observed_at: str,
    contract: TqqqCoreOnlyCandidateContract = P2_V4_CONTRACT,
) -> dict[str, object]:
    """Assess an already-acquired payload without network, storage, or side effects.

    A gap is reported as ``DEFERRED`` together with its exact expected-session
    ranges.  It does not turn a temporary or partial acquisition into a verdict
    about prior snapshots, strategy quality, or any later P3/P4/P5/P6 stage.
    Malformed, duplicate, non-monotonic, or unexpected sessions are quarantined.
    """
    result = _base_result(contract, observed_at=observed_at)
    result["bars_payload_sha256"] = _canonical_sha256(payload)
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != "tqqq_core_only_private_bars.v1"
        or not isinstance(payload.get("symbols"), Mapping)
        or set(payload["symbols"]) != set(_UNIVERSE)
    ):
        result.update(
            {
                "status": "QUARANTINED",
                "verdict": "REJECTED",
                "reason_codes": ["MALFORMED_PAYLOAD"],
                "coverage": {},
            }
        )
        return result

    symbols = payload["symbols"]
    assert isinstance(symbols, Mapping)
    expected_by_symbol = expected_tqqq_core_only_sessions_for_contract(contract)
    coverage: dict[str, object] = {}
    reason_codes: set[str] = set()
    has_missing = False

    for symbol in _UNIVERSE:
        expected = expected_by_symbol[symbol]
        observed = _parse_sessions(symbols[symbol])
        if observed is None:
            reason_codes.add("MALFORMED_BARS")
            coverage[symbol] = {
                "expected_sessions": len(expected),
                "observed_sessions": None,
                "missing_sessions": None,
                "missing_ranges": [],
            }
            continue
        observed_set = set(observed)
        if len(observed_set) != len(observed):
            reason_codes.add("DUPLICATE_SESSION")
        if any(later <= earlier for earlier, later in zip(observed, observed[1:])):
            reason_codes.add("NON_MONOTONIC_SESSIONS")
        expected_set = set(expected)
        if observed_set - expected_set:
            reason_codes.add("UNEXPECTED_SESSION")
        missing = expected_set - observed_set
        if missing:
            has_missing = True
        coverage[symbol] = {
            "expected_sessions": len(expected),
            "observed_sessions": len(observed),
            "missing_sessions": len(missing),
            "missing_ranges": _ranges(expected, missing),
        }

    result["coverage"] = coverage
    if reason_codes:
        result.update(
            {
                "status": "QUARANTINED",
                "verdict": "REJECTED",
                "reason_codes": sorted(reason_codes),
            }
        )
    elif has_missing:
        result.update(
            {
                "status": "DEFERRED",
                "verdict": "INCONCLUSIVE",
                "reason_codes": ["MISSING_SESSIONS"],
            }
        )
    else:
        result.update(
            {
                "status": "ACCEPTED",
                "verdict": "READY",
                "reason_codes": ["COMPLETE"],
            }
        )
    return result
