from __future__ import annotations

from datetime import date

import pytest

from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    TqqqPromotionContractError,
    TqqqPromotionPlan,
    _cost_scenarios,
    _validate_plan,
)


def _plan() -> TqqqPromotionPlan:
    return TqqqPromotionPlan(
        folds=(
            PurgedWalkForwardFold(date(2018,1,2), date(2020,12,31), date(2022,1,3), date(2022,12,30)),
            PurgedWalkForwardFold(date(2018,1,2), date(2021,12,31), date(2023,1,3), date(2023,12,29)),
            PurgedWalkForwardFold(date(2018,1,2), date(2022,12,30), date(2024,1,2), date(2024,6,28)),
        ), locked_oos_start=date(2025, 8, 1), locked_oos_end=date(2026, 7, 31),
        purge_days=252, embargo_days=0,
    )


def test_exact_core_only_plan_and_costs_are_accepted() -> None:
    _validate_plan(_plan())
    assert _cost_scenarios({"turnover_cost_bps": 5.0, "stress_turnover_cost_bps": [10.0, 25.0]}) == (5, 10, 25)


@pytest.mark.parametrize("bad", [
    TqqqPromotionPlan(_plan().folds, date(2025, 7, 2), date(2026, 7, 31), 252, 0),
    TqqqPromotionPlan(_plan().folds, date(2025, 8, 1), date(2026, 7, 31), 20, 20),
])
def test_old_oos_or_purge_plan_is_rejected(bad: TqqqPromotionPlan) -> None:
    with pytest.raises(TqqqPromotionContractError):
        _validate_plan(bad)


def test_active_runner_uses_qpk_walk_forward_not_promotion_api() -> None:
    import inspect
    import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner as runner

    source = inspect.getsource(runner.run_tqqq_promotion_research)
    assert ".walk_forward(" in source
    assert ".run_promotion(" not in source
