"""Freeze the SOXL P3 evidence-window and cost replay plan.

This offline planner consumes only the hashed output of the SOXL P1-bars
materializer.  It does not read bars, invoke a strategy, calculate returns,
write an artifact, schedule work, access credentials, or create an order.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from .soxl_core_only_p2_v2_contract import P2_V2_CONTRACT
from .soxl_core_only_p3_input_materializer import MATERIALIZED_INPUT_SCHEMA


EVIDENCE_PLAN_SCHEMA = "qsl.soxl-soxx-core-only-p3-evidence-plan.v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COST_BPS = (5, 10, 15)
_OOS_SESSION_COUNT = 252
_OOS_MINIMUM_CUTOFF = "2026-08-04"
_FOLDS = (
    ("sequential_evidence_fold_1", "2023-07-03", "2023-12-29"),
    ("sequential_evidence_fold_2", "2024-07-01", "2024-12-31"),
    ("sequential_evidence_fold_3", "2025-03-03", "2025-07-31"),
)


class SoxlCoreOnlyP3EvidencePlanError(ValueError):
    """Fail-closed error with no input rows or strategy result."""


def _fail() -> None:
    raise SoxlCoreOnlyP3EvidencePlanError("invalid SOXL core-only P3 evidence plan input")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyP3EvidencePlanError("invalid SOXL core-only P3 evidence plan input") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _date(value: object) -> str:
    if not isinstance(value, str):
        _fail()
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail()
    if parsed.isoformat() != value:
        _fail()
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        _fail()
    return value


def _session_dates(materialized: Mapping[str, object]) -> tuple[tuple[str, ...], str]:
    payload = _mapping(materialized)
    expected = {
        "schema_version",
        "p1_identity",
        "p2_identity",
        "indicator_spec",
        "sessions",
        "materialized_input_sha256",
    }
    if set(payload) != expected or payload["schema_version"] != MATERIALIZED_INPUT_SCHEMA:
        _fail()
    claimed_digest = payload.pop("materialized_input_sha256")
    if _digest(claimed_digest) != _sha256(payload):
        _fail()
    p1 = _mapping(payload["p1_identity"])
    if set(p1) != {"input_manifest_sha256", "binding_sha256", "bars_member_sha256", "date_cutoff"}:
        _fail()
    for key in ("input_manifest_sha256", "binding_sha256", "bars_member_sha256"):
        _digest(p1[key])
    cutoff = _date(p1["date_cutoff"])
    p2 = _mapping(payload["p2_identity"])
    if p2 != {"candidate_id": P2_V2_CONTRACT.candidate_id, "config_sha256": P2_V2_CONTRACT.config_sha256}:
        _fail()
    indicator_spec = _mapping(payload["indicator_spec"])
    if indicator_spec.get("id") != "soxl-soxx-core-only-close-indicators.v1":
        _fail()
    sessions = payload["sessions"]
    if not isinstance(sessions, list) or len(sessions) < _OOS_SESSION_COUNT:
        _fail()
    dates: list[str] = []
    for raw_session in sessions:
        session = _mapping(raw_session)
        if set(session) != {"as_of", "market_data", "prices"}:
            _fail()
        as_of = session["as_of"]
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        session_date = _date(as_of.removesuffix("T00:00:00+00:00"))
        if dates and session_date <= dates[-1]:
            _fail()
        dates.append(session_date)
    if dates[-1] != cutoff:
        _fail()
    return tuple(dates), claimed_digest


def _window_dates(dates: Sequence[str], *, start: str, end: str) -> tuple[str, ...]:
    if start not in dates or end not in dates or start > end:
        _fail()
    result = tuple(value for value in dates if start <= value <= end)
    if len(result) < 2 or result[0] != start or result[-1] != end:
        _fail()
    return result


def build_soxl_core_only_p3_evidence_plan(materialized: Mapping[str, object]) -> dict[str, object]:
    """Return fixed fold/OOS requests before any strategy replay is invoked."""
    dates, materialized_input_sha256 = _session_dates(materialized)
    if dates[-1] < _OOS_MINIMUM_CUTOFF:
        _fail()
    requests: list[dict[str, object]] = []
    for fold_id, start, end in _FOLDS:
        selected = _window_dates(dates, start=start, end=end)
        for cost_bps in _COST_BPS:
            requests.append(
                {
                    "window_id": fold_id,
                    "window_kind": "purged_sequential_evidence",
                    "session_dates": list(selected),
                    "cost_bps": cost_bps,
                }
            )
    oos_dates = tuple(dates[-_OOS_SESSION_COUNT:])
    if len(oos_dates) != _OOS_SESSION_COUNT or oos_dates[-1] != dates[-1]:
        _fail()
    for cost_bps in _COST_BPS:
        requests.append(
            {
                "window_id": "trailing_252_xnys_session_oos",
                "window_kind": "rolling_locked_oos",
                "session_dates": list(oos_dates),
                "cost_bps": cost_bps,
            }
        )
    result: dict[str, object] = {
        "schema_version": EVIDENCE_PLAN_SCHEMA,
        "p1_identity": _mapping(materialized)["p1_identity"],
        "p2_identity": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "materialized_input_sha256": materialized_input_sha256,
        "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        "purge_sessions": 1,
        "cost_bps": list(_COST_BPS),
        "requests": requests,
    }
    result["evidence_plan_sha256"] = _sha256(result)
    return result


__all__ = [
    "EVIDENCE_PLAN_SCHEMA",
    "SoxlCoreOnlyP3EvidencePlanError",
    "build_soxl_core_only_p3_evidence_plan",
]
