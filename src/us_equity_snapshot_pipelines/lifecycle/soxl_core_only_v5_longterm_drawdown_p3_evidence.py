"""P3 evidence adapter that enforces SOXL's unlevered SOXX benchmark gate.

This research-only adapter keeps the fixed replay mechanics shared with v4,
then evaluates every costed run against zero-turnover SOXX buy-and-hold on the
same assured close sequence.  It grants neither automatic promotion nor
execution authority.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping

from .levered_strategy_benchmark import (
    LeveredStrategyBenchmarkError,
    assess_relative_longterm_compounding,
    build_same_window_buy_and_hold_benchmark,
)
from .soxl_core_only_free_split_close_p3_evidence import (
    SoxlCoreOnlyFreeSplitCloseP3EvidenceError,
    build_soxl_core_only_free_split_close_p3_evidence_plan,
    build_soxl_core_only_free_split_close_p3_evidence_summary,
)
from .soxl_core_only_p2_v5_longterm_drawdown_contract import P2_V5_LONGTERM_DRAWDOWN_CONTRACT

_BENCHMARK_SYMBOL = "SOXX"
_BENCHMARK_POLICY = "buy_and_hold_unlevered_same_assured_close_series"


class SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError(ValueError):
    """Fail-closed v5 evidence error without raw price data in diagnostics."""


def _fail() -> None:
    raise SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError("invalid SOXL v5 long-term P3 evidence input")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError(
            "invalid SOXL v5 long-term P3 evidence input"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _sessions_by_date(materialized: Mapping[str, object]) -> dict[str, dict[str, object]]:
    sessions = materialized.get("sessions")
    if not isinstance(sessions, list):
        _fail()
    result: dict[str, dict[str, object]] = {}
    for raw_session in sessions:
        session = _mapping(raw_session)
        as_of = session.get("as_of")
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        session_date = as_of.removesuffix("T00:00:00+00:00")
        if session_date in result:
            _fail()
        result[session_date] = session
    return result


def build_soxl_core_only_v5_longterm_drawdown_p3_evidence_plan(
    materialized: Mapping[str, object],
) -> dict[str, object]:
    """Build only the fixed v5 P3 requests; no parameter selection occurs here."""
    try:
        return build_soxl_core_only_free_split_close_p3_evidence_plan(
            materialized,
            p2_contract=P2_V5_LONGTERM_DRAWDOWN_CONTRACT,
        )
    except SoxlCoreOnlyFreeSplitCloseP3EvidenceError as exc:
        raise SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError(
            "invalid SOXL v5 long-term P3 evidence input"
        ) from exc


def build_soxl_core_only_v5_longterm_drawdown_p3_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Return metrics-only v5 evidence plus the non-waivable SOXX gate result."""
    expected_plan = build_soxl_core_only_v5_longterm_drawdown_p3_evidence_plan(materialized)
    if _mapping(evidence_plan) != expected_plan:
        _fail()
    try:
        base = build_soxl_core_only_free_split_close_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=expected_plan,
            replay_executor=replay_executor,
            p2_contract=P2_V5_LONGTERM_DRAWDOWN_CONTRACT,
        )
    except SoxlCoreOnlyFreeSplitCloseP3EvidenceError as exc:
        raise SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError(
            "invalid SOXL v5 long-term P3 evidence input"
        ) from exc
    result = _mapping(base)
    claimed_digest = result.pop("evidence_summary_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(result):
        _fail()
    requests = expected_plan.get("requests")
    runs = result.get("runs")
    if not isinstance(requests, list) or not isinstance(runs, list) or len(requests) != len(runs):
        _fail()
    sessions_by_date = _sessions_by_date(materialized)
    reviewed_runs: list[dict[str, object]] = []
    for request, raw_run in zip(requests, runs, strict=True):
        item = _mapping(request)
        run = _mapping(raw_run)
        dates = item.get("session_dates")
        if (
            not isinstance(dates, list)
            or not dates
            or run.get("window_id") != item.get("window_id")
            or run.get("window_kind") != item.get("window_kind")
            or run.get("cost_bps") != item.get("cost_bps")
        ):
            _fail()
        try:
            benchmark = build_same_window_buy_and_hold_benchmark(
                [sessions_by_date[date] for date in dates],
                symbol=_BENCHMARK_SYMBOL,
            )
            acceptance = assess_relative_longterm_compounding(_mapping(run.get("metrics")), benchmark)
        except (KeyError, TypeError, LeveredStrategyBenchmarkError):
            _fail()
        reviewed_runs.append(
            {
                **run,
                "benchmark": benchmark,
                "longterm_compounding_gate": acceptance,
            }
        )
    result["runs"] = reviewed_runs
    result["longterm_compounding_gate"] = {
        "benchmark_symbol": _BENCHMARK_SYMBOL,
        "benchmark_policy": _BENCHMARK_POLICY,
        "strategy_max_drawdown_must_not_exceed_benchmark": True,
        "require_incremental_calmar_after_cost": True,
        "all_fixed_folds_and_cost_scenarios_passed": all(
            run["longterm_compounding_gate"]["passed"] for run in reviewed_runs
        ),
        "automatic_promotion": False,
    }
    result["evidence_summary_sha256"] = _sha256(result)
    return result


__all__ = [
    "SoxlCoreOnlyV5LongtermDrawdownP3EvidenceError",
    "build_soxl_core_only_v5_longterm_drawdown_p3_evidence_plan",
    "build_soxl_core_only_v5_longterm_drawdown_p3_evidence_summary",
]
