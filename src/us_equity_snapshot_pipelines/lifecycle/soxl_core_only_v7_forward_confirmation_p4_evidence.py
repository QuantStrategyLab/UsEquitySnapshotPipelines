"""Fixed-window P4 forward confirmation for the immutable SOXL V7 candidate.

P4 consumes only a later assured P1 materialization and a separately frozen
policy.  It does not acquire prices, persist observations, call a broker, or
grant promotion authority.  In particular, it evaluates the *first* complete
post-freeze window once rather than continually relabelling a rolling window.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from functools import partial
from typing import Any

from .levered_strategy_benchmark import (
    LeveredStrategyBenchmarkError,
    assess_relative_longterm_compounding,
    build_same_window_buy_and_hold_benchmark,
)
from .soxl_core_only_free_split_close_p3_evidence import (
    SoxlCoreOnlyFreeSplitCloseP3EvidenceError,
    _load_module,
)
from .soxl_core_only_free_split_close_p3_input_materializer import MATERIALIZED_INPUT_SCHEMA
from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from .soxl_core_only_p4_v7_forward_confirmation_contract import (
    P4_V7_FORWARD_CONFIRMATION_CONTRACT,
    SoxlCoreOnlyP4V7ForwardConfirmationContractError,
    validate_soxl_core_only_p4_v7_forward_confirmation_policy,
)

FORWARD_CONFIRMATION_PLAN_SCHEMA = "qsl.soxl-soxx-core-only-p4-forward-confirmation-plan.v1"
FORWARD_CONFIRMATION_SUMMARY_SCHEMA = "qsl.soxl-soxx-core-only-p4-forward-confirmation-summary.v1"
_BENCHMARK_SYMBOL = "SOXX"
_BENCHMARK_POLICY = "buy_and_hold_unlevered_same_assured_close_series"
_WINDOW_ID = "first_fixed_252_xnys_sessions_after_v7_policy_freeze"
_WINDOW_KIND = "forward_confirmation"


class SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError(ValueError):
    """Fail-closed P4 error without raw price or account material."""


class SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete(
    SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError
):
    """The only expected non-error P4 waiting state: the fixed window is incomplete."""


def _fail() -> None:
    raise SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError("invalid SOXL V7 P4 forward-confirmation input")


def _window_incomplete() -> None:
    raise SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete("SOXL V7 P4 forward window is incomplete")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError(
            "invalid SOXL V7 P4 forward-confirmation input"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _sessions_by_date(materialized: Mapping[str, object]) -> dict[str, dict[str, object]]:
    payload = _mapping(materialized)
    if set(payload) != {
        "schema_version",
        "p1_identity",
        "p2_identity",
        "indicator_spec",
        "sessions",
        "materialized_input_sha256",
    } or payload["schema_version"] != MATERIALIZED_INPUT_SCHEMA:
        _fail()
    claimed_digest = payload.pop("materialized_input_sha256")
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(payload):
        _fail()
    if _mapping(payload["p2_identity"]) != {
        "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
        "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
    }:
        _fail()
    raw_sessions = payload["sessions"]
    if not isinstance(raw_sessions, list):
        _fail()
    result: dict[str, dict[str, object]] = {}
    previous_date: str | None = None
    for raw_session in raw_sessions:
        session = _mapping(raw_session)
        as_of = session.get("as_of")
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        session_date = as_of.removesuffix("T00:00:00+00:00")
        if session_date in result or (previous_date is not None and session_date <= previous_date):
            _fail()
        result[session_date] = session
        previous_date = session_date
    return result


def build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
    materialized: Mapping[str, object],
    *,
    policy: object,
) -> dict[str, object]:
    """Select exactly the first eligible 252-session post-freeze P4 window."""
    try:
        validated_policy = validate_soxl_core_only_p4_v7_forward_confirmation_policy(policy)
    except SoxlCoreOnlyP4V7ForwardConfirmationContractError as exc:
        raise SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError(
            "invalid SOXL V7 P4 forward-confirmation input"
        ) from exc
    payload = _mapping(materialized)
    sessions_by_date = _sessions_by_date(payload)
    forward_dates = [
        session_date
        for session_date in sessions_by_date
        if session_date >= P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session
    ]
    required_count = P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count
    if (
        len(forward_dates) < required_count
        or not forward_dates
        or forward_dates[0] != P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session
    ):
        _window_incomplete()
    selected_dates = forward_dates[:required_count]
    p1_identity = _mapping(payload["p1_identity"])
    p2_identity = _mapping(payload["p2_identity"])
    materialized_digest = payload["materialized_input_sha256"]
    if not isinstance(materialized_digest, str):
        _fail()
    result: dict[str, object] = {
        "schema_version": FORWARD_CONFIRMATION_PLAN_SCHEMA,
        "p1_identity": p1_identity,
        "p2_identity": p2_identity,
        "p4_policy_identity": {
            "policy_config_sha256": P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256,
            "baseline_p3_evidence_summary_sha256": (
                P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256
            ),
            "first_forward_xnys_session": P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session,
            "forward_session_count": required_count,
            "initial_state": "100_percent_cash_at_first_forward_signal",
        },
        "materialized_input_sha256": materialized_digest,
        "cost_bps": list(P4_V7_FORWARD_CONFIRMATION_CONTRACT.cost_scenarios_bps),
        "requests": [
            {
                "window_id": _WINDOW_ID,
                "window_kind": _WINDOW_KIND,
                "session_dates": selected_dates,
                "cost_bps": cost_bps,
            }
            for cost_bps in P4_V7_FORWARD_CONFIRMATION_CONTRACT.cost_scenarios_bps
        ],
    }
    result["evidence_plan_sha256"] = _sha256(result)
    if validated_policy["candidate"] != p2_identity:
        _fail()
    return result


def _build_base_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
    policy: object,
) -> dict[str, object]:
    expected_plan = build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
        materialized,
        policy=policy,
    )
    if _mapping(evidence_plan) != expected_plan or not callable(replay_executor):
        _fail()
    try:
        summary = _load_module("soxl_core_only_p3_evidence_summary.py", "qsl_soxl_core_only_p4_v7_summary_core")
        summary.P2_V3_CONTRACT = P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
        summary.MATERIALIZED_INPUT_SCHEMA = MATERIALIZED_INPUT_SCHEMA
        summary.build_soxl_core_only_p3_evidence_plan = partial(
            build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan,
            policy=policy,
        )
        return summary.build_soxl_core_only_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=expected_plan,
            replay_executor=replay_executor,
        )
    except (SoxlCoreOnlyFreeSplitCloseP3EvidenceError, ValueError) as exc:
        raise SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError(
            "invalid SOXL V7 P4 forward-confirmation input"
        ) from exc


def build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
    policy: object,
) -> dict[str, object]:
    """Return a metrics-only, one-time P4 forward-confirmation verdict."""
    result = _mapping(
        _build_base_summary(
            materialized=materialized,
            evidence_plan=evidence_plan,
            replay_executor=replay_executor,
            policy=policy,
        )
    )
    claimed_digest = result.pop("evidence_summary_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(result):
        _fail()
    requests = _mapping(evidence_plan).get("requests")
    runs = result.get("runs")
    if (
        not isinstance(requests, list)
        or not isinstance(runs, list)
        or len(requests) != 3
        or len(runs) != 3
        or len(requests) != len(runs)
    ):
        _fail()
    if [
        _mapping(request).get("cost_bps") for request in requests
    ] != list(P4_V7_FORWARD_CONFIRMATION_CONTRACT.cost_scenarios_bps):
        _fail()
    sessions_by_date = _sessions_by_date(materialized)
    reviewed_runs: list[dict[str, object]] = []
    gates: list[dict[str, bool]] = []
    for request, raw_run in zip(requests, runs, strict=True):
        item = _mapping(request)
        run = _mapping(raw_run)
        dates = item.get("session_dates")
        if (
            item.get("window_id") != _WINDOW_ID
            or item.get("window_kind") != _WINDOW_KIND
            or not isinstance(dates, list)
            or len(dates) != P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count
            or run.get("window_id") != _WINDOW_ID
            or run.get("window_kind") != _WINDOW_KIND
            or run.get("cost_bps") != item.get("cost_bps")
        ):
            _fail()
        try:
            benchmark = build_same_window_buy_and_hold_benchmark(
                [sessions_by_date[session_date] for session_date in dates],
                symbol=_BENCHMARK_SYMBOL,
            )
            gate = assess_relative_longterm_compounding(_mapping(run.get("metrics")), benchmark)
        except (KeyError, TypeError, LeveredStrategyBenchmarkError):
            _fail()
        gates.append(gate)
        reviewed_runs.append({
            **run,
            "benchmark": benchmark,
            "relative_benchmark_gate_scope": "forward_confirmation",
            "relative_benchmark_gate": gate,
        })
    all_passed = all(gate["passed"] for gate in gates)
    result["schema_version"] = FORWARD_CONFIRMATION_SUMMARY_SCHEMA
    result["runs"] = reviewed_runs
    result["forward_confirmation_policy"] = {
        "benchmark_symbol": _BENCHMARK_SYMBOL,
        "benchmark_policy": _BENCHMARK_POLICY,
        "first_forward_xnys_session": P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session,
        "forward_session_count": P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count,
        "initial_state": "100_percent_cash_at_first_forward_signal",
        "cost_scenarios_bps": list(P4_V7_FORWARD_CONFIRMATION_CONTRACT.cost_scenarios_bps),
        "baseline_p3_evidence_summary_sha256": (
            P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256
        ),
        "all_forward_drawdown_gates_passed": all(
            gate["max_drawdown_not_exceeding_benchmark"] for gate in gates
        ),
        "all_forward_incremental_calmar_gates_passed": all(
            gate["incremental_calmar_after_cost"] for gate in gates
        ),
        "forward_confirmation_satisfied": all_passed,
        "strategy_verdict": (
            "PASS_REQUIRES_SEPARATE_HUMAN_PROMOTION"
            if all_passed
            else "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
        ),
        "automatic_promotion": False,
    }
    result["evidence_summary_sha256"] = _sha256(result)
    return result


__all__ = [
    "FORWARD_CONFIRMATION_PLAN_SCHEMA",
    "FORWARD_CONFIRMATION_SUMMARY_SCHEMA",
    "SoxlCoreOnlyV7ForwardConfirmationP4EvidenceError",
    "SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete",
    "build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan",
    "build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary",
]
