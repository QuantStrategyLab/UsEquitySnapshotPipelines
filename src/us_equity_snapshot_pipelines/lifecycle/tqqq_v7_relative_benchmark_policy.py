"""TQQQ P2 v7 acceptance policy bound to its independent replay identity.

This module consumes replay metrics only.  It never reads prices, fetches
market data, writes evidence, or exposes an order/promotion path.
"""

from __future__ import annotations

from .levered_strategy_benchmark import aggregate_relative_benchmark_policy
from .tqqq_promotion_runner import TqqqPromotionResearchResult
from .tqqq_qqq_relative_benchmark import (
    TqqqQqqRelativeBenchmarkError,
    assess_tqqq_qqq_relative_benchmark,
)

_CANDIDATE_ID = "tqqq_core_only_p2_v7_relative_benchmark"
_COSTS = (5, 10, 15)
_SHORT_WINDOW_COUNT = 4
_WINDOW_COUNT = _SHORT_WINDOW_COUNT + 1
_LONG_HORIZON_SESSIONS = 756


class TqqqV7RelativeBenchmarkPolicyError(ValueError):
    """Fail closed without disclosing input data or equity paths."""


def _fail() -> None:
    raise TqqqV7RelativeBenchmarkPolicyError("invalid TQQQ v7 relative-benchmark evidence")


def evaluate_tqqq_v7_relative_benchmark_policy(
    result: TqqqPromotionResearchResult,
) -> dict[str, object]:
    """Apply the immutable v7 QQQ-relative policy to all cost scenarios.

    Four chronological short windows test only the non-waivable drawdown
    ceiling.  One continuous 756-session window tests drawdown plus post-cost
    incremental Calmar.  A retrospective pass remains non-promotable until a
    separately bound forward-confirmation receipt is present.
    """
    if (
        type(result) is not TqqqPromotionResearchResult
        or result.identity.candidate_profile != _CANDIDATE_ID
        or result.identity.candidate_variant != _CANDIDATE_ID
        or result.authority_scope != "RESEARCH_ONLY"
        or not result.learning_only
        or not result.no_order
        or not result.size_zero_required
        or result.promotion_eligible
        or result.live_ready
        or result.executable_plan
        or result.order_client_intents
        or tuple(scenario.total_cost_bps for scenario in result.scenarios) != _COSTS
    ):
        _fail()

    short_gates: list[dict[str, object]] = []
    long_gates: list[dict[str, object]] = []
    scenario_records: list[dict[str, object]] = []
    try:
        for scenario in result.scenarios:
            windows = scenario.windows
            if len(windows) != _WINDOW_COUNT:
                _fail()
            short_records: list[dict[str, object]] = []
            for window in windows[:_SHORT_WINDOW_COUNT]:
                assessment = assess_tqqq_qqq_relative_benchmark(
                    window.relative_metrics,
                    require_incremental_calmar=False,
                )
                gate = assessment["gate"]
                if not isinstance(gate, dict) or len(window.sessions) < 3:
                    _fail()
                short_gates.append(gate)
                short_records.append(
                    {
                        "start": window.start_date.isoformat(),
                        "end": window.end_date.isoformat(),
                        "sessions": len(window.sessions),
                        "gate": gate,
                    }
                )
            long_window = windows[-1]
            if len(long_window.sessions) != _LONG_HORIZON_SESSIONS:
                _fail()
            long_assessment = assess_tqqq_qqq_relative_benchmark(
                long_window.relative_metrics,
                require_incremental_calmar=True,
            )
            long_gate = long_assessment["gate"]
            if not isinstance(long_gate, dict):
                _fail()
            long_gates.append(long_gate)
            scenario_records.append(
                {
                    "total_cost_bps": scenario.total_cost_bps,
                    "short_windows": short_records,
                    "continuous_long_horizon": {
                        "start": long_window.start_date.isoformat(),
                        "end": long_window.end_date.isoformat(),
                        "sessions": len(long_window.sessions),
                        "gate": long_gate,
                    },
                }
            )
    except (AttributeError, TqqqQqqRelativeBenchmarkError):
        _fail()

    return {
        "schema_version": "qsl.tqqq-p2-v7-relative-benchmark-policy.v1",
        "candidate_id": _CANDIDATE_ID,
        "benchmark_symbol": "QQQ",
        "benchmark_policy": "buy_and_hold_unlevered_same_assured_close_series",
        "short_window_calmar": "diagnostic_only_not_a_promotion_veto",
        "strategy_max_drawdown_must_not_exceed_benchmark": True,
        "continuous_long_horizon_xnys_sessions": _LONG_HORIZON_SESSIONS,
        "require_incremental_calmar_after_cost_on_continuous_long_horizon": True,
        "scenarios": scenario_records,
        **aggregate_relative_benchmark_policy(
            short_window_drawdown_gates=short_gates,
            long_window_compounding_gates=long_gates,
            forward_confirmation_satisfied=False,
        ),
    }


__all__ = [
    "TqqqV7RelativeBenchmarkPolicyError",
    "evaluate_tqqq_v7_relative_benchmark_policy",
]
