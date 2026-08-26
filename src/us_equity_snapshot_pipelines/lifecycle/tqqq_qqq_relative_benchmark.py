"""TQQQ-to-QQQ adapter for the shared levered-compounding policy.

The established TQQQ runner represents drawdown as a negative return while
the candidate-neutral benchmark helper uses positive drawdown magnitudes.  The
conversion belongs at this narrow candidate boundary, not in either historical
metric engine.  Current P2 v5 acceptance does not invoke this module; a future
candidate must bind it explicitly in its own P1/P3 identity.
"""

from __future__ import annotations

import math

from .levered_strategy_benchmark import (
    LeveredStrategyBenchmarkError,
    assess_relative_drawdown,
    assess_relative_longterm_compounding,
)
from .tqqq_promotion_runner import TqqqQqqRelativeMetrics


class TqqqQqqRelativeBenchmarkError(ValueError):
    """Fail-closed error that exposes neither bars nor equity paths."""


def _fail() -> None:
    raise TqqqQqqRelativeBenchmarkError("invalid TQQQ/QQQ relative benchmark input")


def _finite(value: object) -> float:
    if type(value) not in {int, float} or not math.isfinite(float(value)):
        _fail()
    return float(value)


def build_tqqq_qqq_relative_compounding_metrics(
    metrics: TqqqQqqRelativeMetrics, *, require_incremental_calmar: bool = True
) -> dict[str, dict[str, float]]:
    """Normalize the runner's negative-loss drawdowns for the shared helper.

    Calmar is undefined without a drawdown.  The long-horizon compounding
    rule therefore fails closed on a zero-drawdown edge case rather than
    manufacturing an infinite ratio.  Short chronological windows use only
    the drawdown ceiling, so zero is a valid observed magnitude there.
    """
    if (
        type(metrics) is not TqqqQqqRelativeMetrics
        or metrics.benchmark_symbol != "QQQ"
        or type(require_incremental_calmar) is not bool
    ):
        _fail()
    strategy_drawdown = _finite(metrics.strategy_max_drawdown)
    benchmark_drawdown = _finite(metrics.qqq_max_drawdown)
    _finite(metrics.strategy_cagr)
    benchmark_cagr = _finite(metrics.qqq_cagr)
    if strategy_drawdown > 0.0 or benchmark_drawdown > 0.0:
        _fail()
    strategy_calmar = _finite(metrics.calmar_ratio)
    if require_incremental_calmar and (
        strategy_drawdown == 0.0 or benchmark_drawdown == 0.0
    ):
        _fail()
    benchmark_calmar = (
        benchmark_cagr / abs(benchmark_drawdown)
        if benchmark_drawdown != 0.0
        else 0.0
    )
    if not math.isfinite(benchmark_calmar):
        _fail()
    return {
        "strategy_metrics": {
            "max_drawdown": abs(strategy_drawdown),
            "calmar": strategy_calmar,
        },
        "benchmark_metrics": {
            "max_drawdown": abs(benchmark_drawdown),
            "calmar": benchmark_calmar,
        },
    }


def assess_tqqq_qqq_relative_benchmark(
    metrics: TqqqQqqRelativeMetrics,
    *,
    require_incremental_calmar: bool,
) -> dict[str, object]:
    """Apply a pre-registered QQQ-relative gate to one aligned TQQQ replay."""
    if type(require_incremental_calmar) is not bool:
        _fail()
    normalized = build_tqqq_qqq_relative_compounding_metrics(
        metrics, require_incremental_calmar=require_incremental_calmar
    )
    try:
        gate = (
            assess_relative_longterm_compounding(**normalized)
            if require_incremental_calmar
            else assess_relative_drawdown(**normalized)
        )
    except LeveredStrategyBenchmarkError:
        _fail()
    return {
        "benchmark_symbol": "QQQ",
        "drawdown_convention": "normalized_from_negative_loss",
        "require_incremental_calmar": require_incremental_calmar,
        "gate": gate,
    }


__all__ = [
    "TqqqQqqRelativeBenchmarkError",
    "assess_tqqq_qqq_relative_benchmark",
    "build_tqqq_qqq_relative_compounding_metrics",
]
