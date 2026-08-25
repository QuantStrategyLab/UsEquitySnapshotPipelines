from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.levered_strategy_benchmark import (
    LeveredStrategyBenchmarkError,
    assess_relative_longterm_compounding,
    build_same_window_buy_and_hold_benchmark,
)


def _sessions() -> list[dict[str, object]]:
    return [
        {"as_of": "2026-01-02T00:00:00+00:00", "prices": {"SOXX": 100.0}},
        {"as_of": "2026-01-05T00:00:00+00:00", "prices": {"SOXX": 120.0}},
        {"as_of": "2026-01-06T00:00:00+00:00", "prices": {"SOXX": 90.0}},
        {"as_of": "2026-01-07T00:00:00+00:00", "prices": {"SOXX": 108.0}},
    ]


def test_same_window_buy_hold_emits_metrics_only_with_zero_turnover() -> None:
    result = build_same_window_buy_and_hold_benchmark(_sessions(), symbol="SOXX")

    assert result["benchmark_symbol"] == "SOXX"
    assert result["benchmark_policy"] == "buy_and_hold_unlevered_same_assured_close_series"
    assert result["net_return"] == pytest.approx(0.08)
    assert result["max_drawdown"] == pytest.approx(0.25)
    assert result["one_way_turnover"] == 0.0
    assert result["cost_total"] == 0.0
    assert "prices" not in result
    assert "sessions" not in result


def test_relative_gates_require_both_lower_drawdown_and_calmar_increment() -> None:
    benchmark = build_same_window_buy_and_hold_benchmark(_sessions(), symbol="SOXX")

    assert assess_relative_longterm_compounding(
        {"max_drawdown": 0.20, "calmar": float(benchmark["calmar"]) + 0.01},
        benchmark,
    ) == {
        "max_drawdown_not_exceeding_benchmark": True,
        "incremental_calmar_after_cost": True,
        "passed": True,
    }
    assert assess_relative_longterm_compounding(
        {"max_drawdown": 0.20, "calmar": float(benchmark["calmar"])},
        benchmark,
    )["passed"] is False
    assert assess_relative_longterm_compounding(
        {"max_drawdown": 0.250001, "calmar": float(benchmark["calmar"]) + 1.0},
        benchmark,
    )["passed"] is False


def test_benchmark_rejects_duplicate_or_unusable_price_observations() -> None:
    invalid = _sessions()
    invalid[2]["as_of"] = invalid[1]["as_of"]

    with pytest.raises(LeveredStrategyBenchmarkError):
        build_same_window_buy_and_hold_benchmark(invalid, symbol="SOXX")
