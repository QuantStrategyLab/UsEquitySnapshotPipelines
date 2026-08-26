from __future__ import annotations

from datetime import date, timedelta

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    TqqqCostScenarioResult,
    TqqqEpisodeSummary,
    TqqqPromotionIdentity,
    TqqqPromotionResearchResult,
    TqqqQqqRelativeMetrics,
    TqqqWindowEvidence,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_v7_relative_benchmark_policy import (
    evaluate_tqqq_v7_relative_benchmark_policy,
)


def _metrics(*, calmar_ratio: float = 3.0) -> TqqqQqqRelativeMetrics:
    return TqqqQqqRelativeMetrics(
        benchmark_symbol="QQQ", strategy_total_return=0.4, qqq_total_return=0.2,
        boxx_total_return=0.03, excess_total_return=0.2, strategy_cagr=0.3,
        qqq_cagr=0.1, excess_cagr=0.2, strategy_max_drawdown=-0.1,
        qqq_max_drawdown=-0.2, max_drawdown_delta=0.1,
        strategy_recovery_sessions=3, qqq_recovery_sessions=5,
        strategy_unrecovered_at_end=False, qqq_unrecovered_at_end=False,
        up_market_capture=1.0, down_market_capture=0.5, alpha=0.1, beta=0.8,
        information_ratio=1.0, sharpe_ratio=1.0, sortino_ratio=1.0,
        calmar_ratio=calmar_ratio, annualized_volatility=0.2, var_95=-0.03,
        cvar_95=-0.04, turnover=0.2, trade_count=3, win_rate=0.6,
        profit_factor=1.5, information_coefficient=0.8,
    )


def _window(session_count: int, *, calmar_ratio: float = 3.0) -> TqqqWindowEvidence:
    sessions = tuple(date(2024, 1, 2) + timedelta(days=index) for index in range(session_count))
    episode = TqqqEpisodeSummary(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, None, None)
    return TqqqWindowEvidence(
        start_date=sessions[0], end_date=sessions[-1], prior_state_sha256="a" * 64,
        final_state_sha256="b" * 64, relative_metrics=_metrics(calmar_ratio=calmar_ratio),
        episode_summary=episode, decision_count=0, risk_assessment_count=0,
        sessions=sessions, switching_traces=(),
    )


def _result(*, long_calmar: float = 3.0) -> TqqqPromotionResearchResult:
    identity = TqqqPromotionIdentity(
        qpk_revision="a" * 40, ues_revision="b" * 40, runner_revision="c" * 40,
        config_sha256="d" * 64, input_manifest_sha256="e" * 64,
        mandate_receipt_sha256="f" * 64, initial_state_sha256="0" * 64,
        candidate_profile="tqqq_core_only_p2_v7_relative_benchmark",
        candidate_variant="tqqq_core_only_p2_v7_relative_benchmark",
    )
    scenarios = tuple(
        TqqqCostScenarioResult(
            total_cost_bps=cost, cost_model_scope="ALL_IN_PER_SIDE", promotion_run=None,
            windows=tuple([*(_window(3, calmar_ratio=-10.0) for _ in range(4)), _window(756, calmar_ratio=long_calmar)]),
        )
        for cost in (5, 10, 15)
    )
    return TqqqPromotionResearchResult(
        identity=identity, timing_sha256="1" * 64, scenarios=scenarios,
        frozen_trial_ledger={}, systematic_reporting=None,
    )


def test_v7_policy_keeps_short_calmar_diagnostic_and_requires_future_confirmation() -> None:
    policy = evaluate_tqqq_v7_relative_benchmark_policy(_result())

    assert policy["strategy_verdict"] == "PASS_PENDING_FORWARD_CONFIRMATION"
    assert policy["short_window_drawdown_all_passed"] is True
    assert policy["long_window_incremental_calmar_all_passed"] is True
    assert policy["automatic_promotion"] is False
    assert policy["scenarios"][0]["short_windows"][0]["gate"]["passed"] is True


def test_v7_policy_rejects_a_long_horizon_without_incremental_calmar() -> None:
    policy = evaluate_tqqq_v7_relative_benchmark_policy(_result(long_calmar=0.5))

    assert policy["strategy_verdict"] == "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
    assert policy["long_window_incremental_calmar_all_passed"] is False
