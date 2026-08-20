"""Build a sanitized SOXL P3 evidence summary from fixed isolated replays.

This module has no provider, storage, scheduler, credential, broker, or order
integration.  Its caller supplies an isolated replay executor; all requests
are reconstructed from the frozen P1/P2 material and evidence plan before an
executor can be called.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from typing import Any

from .soxl_core_only_p3_evidence_plan import build_soxl_core_only_p3_evidence_plan
from .soxl_core_only_p3_input_materializer import MATERIALIZED_INPUT_SCHEMA
from .soxl_core_only_p2_v2_contract import P2_V2_CONTRACT


EVIDENCE_SUMMARY_SCHEMA = "qsl.soxl-soxx-core-only-p3-evidence-summary.v1"
_REPLAY_INPUT_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-input.v1"
_ISOLATED_REPLAY_SCHEMA = "qsl.soxl-core-only-p3-isolated-replay-result.v1"
_STATEFUL_REPLAY_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-result.v1"
_INITIAL_EQUITY = 100_000.0


class SoxlCoreOnlyP3EvidenceSummaryError(ValueError):
    """Fail-closed error without raw inputs or strategy targets."""


def _fail() -> None:
    raise SoxlCoreOnlyP3EvidenceSummaryError("invalid SOXL core-only P3 evidence summary input")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyP3EvidenceSummaryError("invalid SOXL core-only P3 evidence summary input") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail()
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0) or (nonnegative and result < 0.0):
        _fail()
    return result


def _materialized_sessions(value: Mapping[str, object]) -> dict[str, dict[str, object]]:
    payload = _mapping(value)
    if payload.get("schema_version") != MATERIALIZED_INPUT_SCHEMA:
        _fail()
    sessions = payload.get("sessions")
    if not isinstance(sessions, list):
        _fail()
    result: dict[str, dict[str, object]] = {}
    for raw_session in sessions:
        session = _mapping(raw_session)
        as_of = session.get("as_of")
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        date = as_of.removesuffix("T00:00:00+00:00")
        if date in result:
            _fail()
        result[date] = session
    return result


def _replay_summary(value: Mapping[str, object], *, cost_bps: int) -> tuple[dict[str, object], dict[str, str]]:
    outer = _mapping(value)
    claimed_outer_digest = outer.pop("result_sha256", None)
    if (
        outer.get("schema_version") != _ISOLATED_REPLAY_SCHEMA
        or outer.get("status") != "SUCCESS"
        or not isinstance(claimed_outer_digest, str)
        or claimed_outer_digest != _sha256(outer)
    ):
        _fail()
    execution_identity = _mapping(outer.get("execution_identity"))
    p2_identity = _mapping(outer.get("p2_identity"))
    if p2_identity != {
        "candidate_id": P2_V2_CONTRACT.candidate_id,
        "config_sha256": P2_V2_CONTRACT.config_sha256,
    }:
        _fail()
    replay = _mapping(outer.get("replay"))
    claimed_replay_digest = replay.pop("output_sha256", None)
    if (
        replay.get("schema_version") != _STATEFUL_REPLAY_SCHEMA
        or replay.get("cost_bps") != cost_bps
        or not isinstance(claimed_replay_digest, str)
        or claimed_replay_digest != _sha256(replay)
    ):
        _fail()
    initial_equity = _finite(replay.get("initial_equity"), positive=True)
    final_equity = _finite(replay.get("final_equity"), positive=True)
    cost_total = _finite(replay.get("cost_total"), nonnegative=True)
    turnover = _finite(replay.get("one_way_turnover"), nonnegative=True)
    decisions = replay.get("decisions")
    if not isinstance(decisions, list) or not decisions:
        _fail()
    equity_curve = []
    for raw_decision in decisions:
        decision = _mapping(raw_decision)
        equity_curve.append(_finite(decision.get("equity_before_signal"), positive=True))
    equity_curve.append(final_equity)
    peak = equity_curve[0]
    max_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1.0 - (equity / peak))
    summary = {
        "initial_equity": initial_equity,
        "final_equity": final_equity,
        "net_return": (final_equity / initial_equity) - 1.0,
        "max_drawdown": max_drawdown,
        "one_way_turnover": turnover,
        "cost_total": cost_total,
        "executed_signal_count": replay.get("executed_signal_count"),
        "unexecuted_final_signal": replay.get("unexecuted_final_signal"),
        "replay_result_sha256": claimed_replay_digest,
    }
    if not isinstance(summary["executed_signal_count"], int) or summary["executed_signal_count"] < 1:
        _fail()
    if summary["unexecuted_final_signal"] is not True:
        _fail()
    return summary, {key: str(value) for key, value in execution_identity.items()} | {
        f"p2_{key}": str(value) for key, value in p2_identity.items()
    }


def build_soxl_core_only_p3_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Execute exactly the fixed replay requests and return metrics-only evidence."""
    expected_plan = build_soxl_core_only_p3_evidence_plan(materialized)
    if _mapping(evidence_plan) != expected_plan or not callable(replay_executor):
        _fail()
    sessions_by_date = _materialized_sessions(materialized)
    runs: list[dict[str, object]] = []
    common_execution_identity: dict[str, str] | None = None
    for request in expected_plan["requests"]:
        item = _mapping(request)
        dates = item.get("session_dates")
        if not isinstance(dates, list) or not dates:
            _fail()
        try:
            sessions = [sessions_by_date[date] for date in dates]
        except (KeyError, TypeError):
            _fail()
        replay_input = {
            "schema_version": _REPLAY_INPUT_SCHEMA,
            "initial_equity": _INITIAL_EQUITY,
            "cost_bps": item["cost_bps"],
            "sessions": sessions,
        }
        replay_input_sha256 = _sha256(replay_input)
        summary, execution_identity = _replay_summary(
            replay_executor(replay_input),
            cost_bps=int(item["cost_bps"]),
        )
        if common_execution_identity is None:
            common_execution_identity = execution_identity
        elif execution_identity != common_execution_identity:
            _fail()
        runs.append(
            {
                "window_id": item["window_id"],
                "window_kind": item["window_kind"],
                "cost_bps": item["cost_bps"],
                "replay_input_sha256": replay_input_sha256,
                "metrics": summary,
            }
        )
    result: dict[str, object] = {
        "schema_version": EVIDENCE_SUMMARY_SCHEMA,
        "status": "SUCCESS",
        "p1_identity": expected_plan["p1_identity"],
        "p2_identity": expected_plan["p2_identity"],
        "materialized_input_sha256": expected_plan["materialized_input_sha256"],
        "evidence_plan_sha256": expected_plan["evidence_plan_sha256"],
        "execution_identity": common_execution_identity,
        "runs": runs,
    }
    result["evidence_summary_sha256"] = _sha256(result)
    return result


__all__ = [
    "EVIDENCE_SUMMARY_SCHEMA",
    "SoxlCoreOnlyP3EvidenceSummaryError",
    "build_soxl_core_only_p3_evidence_summary",
]
