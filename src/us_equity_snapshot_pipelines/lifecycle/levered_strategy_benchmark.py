"""Sanitized, candidate-agnostic benchmarks for levered-strategy P3 research.

The helper compares a strategy with the unlevered underlying on exactly the
same assured session sequence.  It accepts already materialized observations,
never acquires data, and deliberately returns metrics only: raw prices and
session rows cannot appear in P3 evidence or logs through this boundary.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

_INITIAL_EQUITY = 100_000.0
_TRADING_SESSIONS_PER_YEAR = 252.0
_REQUIRED_STRATEGY_METRICS = ("max_drawdown", "calmar")


class LeveredStrategyBenchmarkError(ValueError):
    """Fail-closed benchmark error without raw market data in its message."""


def _fail() -> None:
    raise LeveredStrategyBenchmarkError("invalid levered-strategy benchmark input")


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail()
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0) or (nonnegative and result < 0.0):
        _fail()
    return result


def _price_sequence(sessions: Sequence[Mapping[str, object]], *, symbol: str) -> list[float]:
    if not isinstance(symbol, str) or not symbol or len(sessions) < 3:
        _fail()
    prices: list[float] = []
    seen_dates: set[str] = set()
    for session in sessions:
        if not isinstance(session, Mapping):
            _fail()
        as_of = session.get("as_of")
        market_prices = session.get("prices")
        if (
            not isinstance(as_of, str)
            or not as_of.endswith("T00:00:00+00:00")
            or as_of in seen_dates
            or not isinstance(market_prices, Mapping)
        ):
            _fail()
        seen_dates.add(as_of)
        prices.append(_finite(market_prices.get(symbol), positive=True))
    return prices


def build_same_window_buy_and_hold_benchmark(
    sessions: Sequence[Mapping[str, object]],
    *,
    symbol: str,
    initial_equity: float = _INITIAL_EQUITY,
) -> dict[str, object]:
    """Return metrics for zero-turnover unlevered buy-and-hold on these sessions.

    The first assured close fixes the entry value, so all daily returns begin
    with the next session.  This mirrors a same-window close-to-close
    comparison and avoids introducing an unobserved fill price.
    """
    initial = _finite(initial_equity, positive=True)
    prices = _price_sequence(sessions, symbol=symbol)
    equity_curve = [initial]
    for price in prices[1:]:
        equity_curve.append(initial * (price / prices[0]))
    returns = [
        (equity_curve[index] / equity_curve[index - 1]) - 1.0
        for index in range(1, len(equity_curve))
    ]
    if len(returns) < 2:
        _fail()
    peak = equity_curve[0]
    max_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1.0 - (equity / peak))
    mean_return = sum(returns) / len(returns)
    sample_variance = sum((item - mean_return) ** 2 for item in returns) / (len(returns) - 1)
    sample_deviation = math.sqrt(sample_variance)
    sharpe = 0.0 if sample_deviation == 0.0 else math.sqrt(_TRADING_SESSIONS_PER_YEAR) * mean_return / sample_deviation
    final_equity = equity_curve[-1]
    cagr = (final_equity / initial) ** (_TRADING_SESSIONS_PER_YEAR / len(returns)) - 1.0
    calmar = 0.0 if max_drawdown == 0.0 else cagr / max_drawdown
    win_rate = sum(item > 0.0 for item in returns) / len(returns)
    if not all(math.isfinite(item) for item in (sharpe, cagr, calmar, win_rate)):
        _fail()
    return {
        "benchmark_symbol": symbol,
        "benchmark_policy": "buy_and_hold_unlevered_same_assured_close_series",
        "initial_equity": initial,
        "final_equity": final_equity,
        "net_return": (final_equity / initial) - 1.0,
        "max_drawdown": max_drawdown,
        "one_way_turnover": 0.0,
        "cost_total": 0.0,
        "sharpe": sharpe,
        "cagr": cagr,
        "calmar": calmar,
        "win_rate": win_rate,
    }


def assess_relative_longterm_compounding(
    strategy_metrics: Mapping[str, object],
    benchmark_metrics: Mapping[str, object],
) -> dict[str, bool]:
    """Apply the non-waivable MDD and Calmar promotion gates after costs."""
    if not isinstance(strategy_metrics, Mapping) or not isinstance(benchmark_metrics, Mapping):
        _fail()
    strategy = {
        field: _finite(strategy_metrics.get(field), nonnegative=field == "max_drawdown")
        for field in _REQUIRED_STRATEGY_METRICS
    }
    benchmark = {
        field: _finite(benchmark_metrics.get(field), nonnegative=field == "max_drawdown")
        for field in _REQUIRED_STRATEGY_METRICS
    }
    drawdown_passed = strategy["max_drawdown"] <= benchmark["max_drawdown"]
    calmar_increment_passed = strategy["calmar"] > benchmark["calmar"]
    return {
        "max_drawdown_not_exceeding_benchmark": drawdown_passed,
        "incremental_calmar_after_cost": calmar_increment_passed,
        "passed": drawdown_passed and calmar_increment_passed,
    }


__all__ = [
    "LeveredStrategyBenchmarkError",
    "assess_relative_longterm_compounding",
    "build_same_window_buy_and_hold_benchmark",
]
