from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import TqqqQqqRelativeMetrics
from us_equity_snapshot_pipelines.lifecycle.tqqq_qqq_relative_benchmark import (
    TqqqQqqRelativeBenchmarkError,
    assess_tqqq_qqq_relative_benchmark,
    build_tqqq_qqq_relative_compounding_metrics,
)


def _metrics(**overrides: object) -> TqqqQqqRelativeMetrics:
    values: dict[str, object] = {
        "benchmark_symbol": "QQQ",
        "strategy_total_return": 0.4,
        "qqq_total_return": 0.2,
        "boxx_total_return": 0.03,
        "excess_total_return": 0.2,
        "strategy_cagr": 0.3,
        "qqq_cagr": 0.1,
        "excess_cagr": 0.2,
        "strategy_max_drawdown": -0.1,
        "qqq_max_drawdown": -0.2,
        "max_drawdown_delta": 0.1,
        "strategy_recovery_sessions": 3,
        "qqq_recovery_sessions": 5,
        "strategy_unrecovered_at_end": False,
        "qqq_unrecovered_at_end": False,
        "up_market_capture": 1.0,
        "down_market_capture": 0.5,
        "alpha": 0.1,
        "beta": 0.8,
        "information_ratio": 1.0,
        "sharpe_ratio": 1.0,
        "sortino_ratio": 1.0,
        "calmar_ratio": 3.0,
        "annualized_volatility": 0.2,
        "var_95": -0.03,
        "cvar_95": -0.04,
        "turnover": 0.2,
        "trade_count": 3,
        "win_rate": 0.6,
        "profit_factor": 1.5,
        "information_coefficient": 0.8,
    }
    values.update(overrides)
    return TqqqQqqRelativeMetrics(**values)


def test_tqqq_adapter_normalizes_negative_drawdown_before_applying_qqq_gate() -> None:
    normalized = build_tqqq_qqq_relative_compounding_metrics(_metrics())

    assert normalized == {
        "strategy_metrics": {"max_drawdown": 0.1, "calmar": 3.0},
        "benchmark_metrics": {"max_drawdown": 0.2, "calmar": 0.5},
    }
    assert assess_tqqq_qqq_relative_benchmark(
        _metrics(), require_incremental_calmar=True
    )["gate"]["passed"] is True


def test_tqqq_short_window_gate_does_not_use_calmar_but_keeps_drawdown_hard() -> None:
    assessment = assess_tqqq_qqq_relative_benchmark(
        _metrics(calmar_ratio=-100.0), require_incremental_calmar=False
    )
    assert assessment["gate"] == {
        "max_drawdown_not_exceeding_benchmark": True,
        "passed": True,
    }


@pytest.mark.parametrize(
    "candidate",
    [
        _metrics(benchmark_symbol="QQQM"),
        _metrics(strategy_max_drawdown=0.1),
        _metrics(qqq_max_drawdown=0.0),
        _metrics(strategy_max_drawdown=0.0),
    ],
)
def test_tqqq_adapter_rejects_wrong_benchmark_or_ambiguous_drawdown(candidate: TqqqQqqRelativeMetrics) -> None:
    with pytest.raises(TqqqQqqRelativeBenchmarkError):
        build_tqqq_qqq_relative_compounding_metrics(candidate)
