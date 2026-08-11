from __future__ import annotations

from dataclasses import replace
from datetime import date
from unittest.mock import patch

import pytest
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    TqqqPromotionContractError,
    TqqqPromotionIdentity,
    TqqqPromotionPlan,
    TqqqPromotionRunner,
    TqqqWindowReplay,
    run_tqqq_promotion_research,
)

QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
RUNNER_REVISION = "1" * 40


def _identity() -> TqqqPromotionIdentity:
    return TqqqPromotionIdentity(
        qpk_revision=QPK_REVISION,
        ues_revision=UES_REVISION,
        runner_revision=RUNNER_REVISION,
        platform_execution_revision="2" * 40,
        config_sha256="3" * 64,
        input_manifest_sha256="4" * 64,
        mandate_receipt_sha256="5" * 64,
        initial_state_sha256="6" * 64,
    )


def _plan() -> TqqqPromotionPlan:
    return TqqqPromotionPlan(
        folds=(
            PurgedWalkForwardFold(
                train_start=date(2018, 1, 2),
                train_end=date(2019, 6, 28),
                test_start=date(2019, 7, 30),
                test_end=date(2020, 1, 31),
            ),
            PurgedWalkForwardFold(
                train_start=date(2020, 3, 2),
                train_end=date(2021, 8, 31),
                test_start=date(2021, 10, 1),
                test_end=date(2022, 3, 31),
            ),
            PurgedWalkForwardFold(
                train_start=date(2022, 5, 2),
                train_end=date(2023, 10, 31),
                test_start=date(2023, 12, 1),
                test_end=date(2024, 5, 31),
            ),
        ),
        locked_oos_start=date(2024, 7, 1),
        locked_oos_end=date(2025, 7, 1),
        purge_days=20,
        embargo_days=20,
    )


class SyntheticReplay:
    def __init__(self) -> None:
        self.calls: list[tuple[date, date, int, str]] = []

    def __call__(
        self,
        start_date: date,
        end_date: date,
        total_cost_bps: int,
        prior_state_sha256: str,
    ) -> TqqqWindowReplay:
        self.calls.append((start_date, end_date, total_cost_bps, prior_state_sha256))
        call_number = (len(self.calls) - 1) % 4 + 1
        strategy = (100.0, 102.0, 99.0, 105.0)
        benchmark = (100.0, 101.0, 100.0, 103.0)
        return TqqqWindowReplay(
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=prior_state_sha256,
            final_state_sha256=f"{call_number:x}" * 64,
            strategy_equity=strategy,
            qqq_total_return_equity=benchmark,
            asset_weights=(("TQQQ", 0.05), ("QQQM", 0.20), ("BOXX", 0.10)),
            turnover=0.4,
            trade_count=2,
            decision_count=4,
            risk_assessment_count=4,
            warmup_sessions=257,
        )


def _run(replay: SyntheticReplay | None = None):
    replay = replay or SyntheticReplay()
    with patch(
        "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
        return_value=RUNNER_REVISION,
    ):
        result = run_tqqq_promotion_research(_identity(), _plan(), replay)
    return result, replay


def test_delegates_exact_three_cost_scenarios_to_qpk_promotion_orchestrator() -> None:
    with patch.object(
        BacktestOrchestrator,
        "run_promotion",
        autospec=True,
        wraps=BacktestOrchestrator.run_promotion,
    ) as run_promotion:
        result, replay = _run()

    assert TqqqPromotionRunner.runner_kind == "real"
    assert run_promotion.call_count == 3
    assert [scenario.total_cost_bps for scenario in result.scenarios] == [5, 10, 15]
    assert all(scenario.cost_model_scope == "ALL_IN_PER_SIDE" for scenario in result.scenarios)
    assert len(replay.calls) == 12
    assert all(
        scenario.promotion_run.source_revision == UES_REVISION
        for scenario in result.scenarios
    )


def test_explicit_entrypoints_preserve_continuous_state_and_qqq_relative_metrics() -> None:
    result, replay = _run()

    for offset in (0, 4, 8):
        calls = replay.calls[offset : offset + 4]
        assert calls[0][3] == _identity().initial_state_sha256
        assert [call[3] for call in calls[1:]] == ["1" * 64, "2" * 64, "3" * 64]

    locked = result.scenarios[0].windows[-1]
    metrics = locked.relative_metrics
    assert metrics.benchmark_symbol == "QQQ"
    assert metrics.strategy_total_return == pytest.approx(0.05)
    assert metrics.qqq_total_return == pytest.approx(0.03)
    assert metrics.excess_total_return == pytest.approx(0.02)
    assert metrics.strategy_max_drawdown < 0.0
    assert metrics.qqq_max_drawdown < 0.0
    assert metrics.max_drawdown_delta == pytest.approx(
        metrics.strategy_max_drawdown - metrics.qqq_max_drawdown
    )
    assert metrics.strategy_recovery_sessions is not None
    assert metrics.strategy_unrecovered_at_end is False
    assert metrics.up_market_capture > 0.0
    assert metrics.down_market_capture > 0.0
    assert metrics.alpha is not None
    assert metrics.beta is not None
    assert metrics.information_ratio is not None
    assert metrics.var_95 <= metrics.cvar_95 + abs(metrics.var_95)
    assert metrics.turnover == pytest.approx(0.4)
    assert metrics.trade_count == 2


def test_result_is_research_only_and_has_no_execution_reachability() -> None:
    result, _ = _run()

    assert result.authority_scope == "RESEARCH_ONLY"
    assert result.no_order is True
    assert result.size_zero_required is True
    assert result.promotion_eligible is False
    assert result.live_ready is False
    assert result.executable_plan == ()
    assert result.order_client_intents == ()


@pytest.mark.parametrize(
    "update,message",
    [
        ({"data_available": False}, "data unavailable"),
        ({"asset_weights": (("QQQ", 0.1), ("QQQM", 0.0), ("BOXX", 0.0))}, "ETF-only"),
        ({"asset_weights": (("TQQQ", 0.15), ("QQQM", 0.10), ("BOXX", 0.0))}, "effective exposure"),
        ({"income_layer_enabled": True}, "income layer"),
        ({"option_overlay_enabled": True}, "option overlay"),
        ({"order_intents": (object(),)}, "order intents"),
        ({"executable_plan": (object(),)}, "executable plan"),
        ({"risk_assessment_count": 3}, "exactly once"),
        ({"market_regime_control_sha256": ""}, "state/plugin"),
        ({"warmup_sessions": 256}, "warmup"),
        ({"cash_reset": True}, "cash reset"),
    ],
)
def test_noncanonical_or_unavailable_replay_fails_closed(
    update: dict[str, object], message: str
) -> None:
    replay = SyntheticReplay()
    original = replay.__call__

    def invalid(*args, **kwargs):
        return replace(original(*args, **kwargs), **update)

    with (
        patch.object(replay, "__call__", side_effect=invalid),
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),pytest.raises(TqqqPromotionContractError, match=message)
    ):
        run_tqqq_promotion_research(_identity(), _plan(), invalid)


def test_identity_and_timing_are_immutable_and_fail_closed_before_replay() -> None:
    replay = SyntheticReplay()
    identity = replace(_identity(), qpk_revision="7" * 40)

    with patch(
        "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
        return_value=RUNNER_REVISION,
    ), pytest.raises(TqqqPromotionContractError, match="QPK revision"):
        run_tqqq_promotion_research(identity, _plan(), replay)

    assert replay.calls == []
