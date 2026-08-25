"""SOXL v6 P3 evidence with separate risk and long-compounding verdicts.

The v6 adapter keeps v5's source and isolated-replay mechanics.  It adds one
pre-registered continuous 756-session request per cost scenario, uses every
short window solely for its non-waivable drawdown comparison, and reserves
Calmar comparison for the continuous long horizon.  It is metrics-only and
does not implement the later forward-confirmation checkpoint.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from functools import partial

from .levered_strategy_benchmark import (
    LeveredStrategyBenchmarkError,
    aggregate_relative_benchmark_policy,
    assess_relative_drawdown,
    assess_relative_longterm_compounding,
    build_same_window_buy_and_hold_benchmark,
)
from .soxl_core_only_free_split_close_p3_evidence import (
    SoxlCoreOnlyFreeSplitCloseP3EvidenceError,
    _load_module,
    build_soxl_core_only_free_split_close_p3_evidence_plan,
)
from .soxl_core_only_free_split_close_p3_input_materializer import MATERIALIZED_INPUT_SCHEMA
from .soxl_core_only_p2_v6_longterm_compounding_contract import P2_V6_LONGTERM_COMPOUNDING_CONTRACT

_BENCHMARK_SYMBOL = "SOXX"
_BENCHMARK_POLICY = "buy_and_hold_unlevered_same_assured_close_series"
_CONTINUOUS_LONG_WINDOW_ID = "continuous_756_xnys_session_long_horizon"
_CONTINUOUS_LONG_WINDOW_KIND = "continuous_long_horizon"
_CONTINUOUS_LONG_SESSION_COUNT = 756


class SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError(ValueError):
    """Fail-closed v6 evidence error without raw price data in diagnostics."""


def _fail() -> None:
    raise SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError("invalid SOXL v6 long-term P3 evidence input")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError(
            "invalid SOXL v6 long-term P3 evidence input"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _sessions_by_date(materialized: Mapping[str, object]) -> dict[str, dict[str, object]]:
    payload = _mapping(materialized)
    if payload.get("schema_version") != MATERIALIZED_INPUT_SCHEMA:
        _fail()
    raw_sessions = payload.get("sessions")
    if not isinstance(raw_sessions, list):
        _fail()
    result: dict[str, dict[str, object]] = {}
    for raw_session in raw_sessions:
        session = _mapping(raw_session)
        as_of = session.get("as_of")
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        session_date = as_of.removesuffix("T00:00:00+00:00")
        if session_date in result:
            _fail()
        result[session_date] = session
    return result


def build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan(
    materialized: Mapping[str, object],
) -> dict[str, object]:
    """Build fixed folds/OOS plus one continuous long-horizon request per cost."""
    try:
        base = build_soxl_core_only_free_split_close_p3_evidence_plan(
            materialized,
            p2_contract=P2_V6_LONGTERM_COMPOUNDING_CONTRACT,
        )
    except SoxlCoreOnlyFreeSplitCloseP3EvidenceError as exc:
        raise SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError(
            "invalid SOXL v6 long-term P3 evidence input"
        ) from exc
    result = _mapping(base)
    dates = tuple(_sessions_by_date(materialized))
    if len(dates) < _CONTINUOUS_LONG_SESSION_COUNT:
        _fail()
    requests = result.get("requests")
    costs = result.get("cost_bps")
    if not isinstance(requests, list) or not isinstance(costs, list) or set(costs) != {5, 10, 15}:
        _fail()
    long_dates = list(dates[-_CONTINUOUS_LONG_SESSION_COUNT:])
    if len(long_dates) != _CONTINUOUS_LONG_SESSION_COUNT:
        _fail()
    requests = [dict(item) if isinstance(item, Mapping) else _fail() for item in requests]
    for cost_bps in costs:
        if type(cost_bps) is not int:
            _fail()
        requests.append(
            {
                "window_id": _CONTINUOUS_LONG_WINDOW_ID,
                "window_kind": _CONTINUOUS_LONG_WINDOW_KIND,
                "session_dates": long_dates,
                "cost_bps": cost_bps,
            }
        )
    result["schema_version"] = "qsl.soxl-soxx-core-only-p3-evidence-plan.v2"
    result["requests"] = requests
    result.pop("evidence_plan_sha256", None)
    result["evidence_plan_sha256"] = _sha256(result)
    return result


def _build_base_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    expected_plan = build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan(materialized)
    if _mapping(evidence_plan) != expected_plan or not callable(replay_executor):
        _fail()
    summary = _load_module("soxl_core_only_p3_evidence_summary.py", "qsl_soxl_core_only_p3_v6_summary_core")
    summary.P2_V3_CONTRACT = P2_V6_LONGTERM_COMPOUNDING_CONTRACT
    summary.MATERIALIZED_INPUT_SCHEMA = MATERIALIZED_INPUT_SCHEMA
    summary.build_soxl_core_only_p3_evidence_plan = partial(
        build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan,
    )
    try:
        return summary.build_soxl_core_only_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=expected_plan,
            replay_executor=replay_executor,
        )
    except ValueError as exc:
        raise SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError(
            "invalid SOXL v6 long-term P3 evidence input"
        ) from exc


def build_soxl_core_only_v6_longterm_compounding_p3_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Return metrics-only v6 evidence; a later forward gate remains required."""
    result = _mapping(_build_base_summary(
        materialized=materialized,
        evidence_plan=evidence_plan,
        replay_executor=replay_executor,
    ))
    claimed_digest = result.pop("evidence_summary_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(result):
        _fail()
    requests = _mapping(evidence_plan).get("requests")
    runs = result.get("runs")
    if not isinstance(requests, list) or not isinstance(runs, list) or len(requests) != len(runs):
        _fail()
    sessions_by_date = _sessions_by_date(materialized)
    short_gates: list[dict[str, bool]] = []
    long_gates: list[dict[str, bool]] = []
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
            metrics = _mapping(run.get("metrics"))
            if item["window_kind"] == _CONTINUOUS_LONG_WINDOW_KIND:
                gate = assess_relative_longterm_compounding(metrics, benchmark)
                long_gates.append(gate)
                gate_scope = "longterm_compounding"
            else:
                gate = assess_relative_drawdown(metrics, benchmark)
                short_gates.append(gate)
                gate_scope = "drawdown_only"
        except (KeyError, TypeError, LeveredStrategyBenchmarkError):
            _fail()
        reviewed_runs.append({
            **run,
            "benchmark": benchmark,
            "relative_benchmark_gate_scope": gate_scope,
            "relative_benchmark_gate": gate,
        })
    if len(long_gates) != 3 or len(short_gates) != 12:
        _fail()
    result["runs"] = reviewed_runs
    result["relative_benchmark_policy"] = {
        "benchmark_symbol": _BENCHMARK_SYMBOL,
        "benchmark_policy": _BENCHMARK_POLICY,
        "short_window_calmar": "diagnostic_only_not_a_promotion_veto",
        "strategy_max_drawdown_must_not_exceed_benchmark": True,
        "require_incremental_calmar_after_cost_on_continuous_long_horizon": True,
        "continuous_long_horizon_xnys_sessions": _CONTINUOUS_LONG_SESSION_COUNT,
        **aggregate_relative_benchmark_policy(
            short_window_drawdown_gates=short_gates,
            long_window_compounding_gates=long_gates,
            forward_confirmation_satisfied=False,
        ),
    }
    result["evidence_summary_sha256"] = _sha256(result)
    return result


__all__ = [
    "SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError",
    "build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan",
    "build_soxl_core_only_v6_longterm_compounding_p3_evidence_summary",
]
