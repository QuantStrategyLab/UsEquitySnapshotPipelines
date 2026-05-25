from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BacktestWindow:
    name: str
    start: pd.Timestamp | None
    end: pd.Timestamp | None
    description: str = ""


def _normalize_returns(portfolio_returns: pd.Series) -> pd.Series:
    returns = pd.Series(portfolio_returns).copy()
    returns.index = pd.to_datetime(returns.index, errors="coerce").tz_localize(None).normalize()
    returns = pd.to_numeric(returns, errors="coerce")
    returns = returns.dropna()
    returns = returns.loc[returns.index.notna()].sort_index()
    return returns


def _last_session_start(index: pd.DatetimeIndex, sessions: int) -> pd.Timestamp:
    sessions = max(1, int(sessions))
    return pd.Timestamp(index[max(0, len(index) - sessions)]).normalize()


def _years_back(end: pd.Timestamp, years: int) -> pd.Timestamp:
    return pd.Timestamp(end - pd.DateOffset(years=int(years))).normalize()


def build_standard_windows(portfolio_returns: pd.Series) -> tuple[BacktestWindow, ...]:
    returns = _normalize_returns(portfolio_returns)
    if returns.empty:
        return ()

    index = pd.DatetimeIndex(returns.index)
    end = pd.Timestamp(index[-1]).normalize()
    year_start = pd.Timestamp(year=end.year, month=1, day=1)
    windows = [
        BacktestWindow("live_short_ytd", year_start, end, "calendar YTD through archive end"),
        BacktestWindow("live_short_3m", _last_session_start(index, 63), end, "last 63 return observations"),
        BacktestWindow("live_short_6m", _last_session_start(index, 126), end, "last 126 return observations"),
        BacktestWindow("live_short_1y", _last_session_start(index, 252), end, "last 252 return observations"),
        BacktestWindow("post_2022_bull", pd.Timestamp("2023-01-03"), end, "post-2022 bull recovery"),
        BacktestWindow("rate_bear_2022", pd.Timestamp("2022-01-03"), pd.Timestamp("2022-12-30"), "2022 rate bear"),
        BacktestWindow(
            "covid_crash_2020",
            pd.Timestamp("2020-02-18"),
            pd.Timestamp("2020-04-30"),
            "COVID crash window",
        ),
        BacktestWindow(
            "trade_war_2018_2019",
            pd.Timestamp("2018-01-02"),
            pd.Timestamp("2019-12-31"),
            "2018-2019 trade-war window",
        ),
        BacktestWindow(
            "long_15y_to_date",
            _years_back(end, 15),
            end,
            "latest 15-year real-product replay; dotcom/GFC stay in crisis-specific research",
        ),
    ]
    return tuple(windows)


def _summarize_window(returns: pd.Series) -> dict[str, float | str | int]:
    equity_curve = (1.0 + returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    sharpe = float(returns.mean() / std * np.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")
    return {
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Observations": int(len(returns)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Final Equity": float(equity_curve.iloc[-1]),
    }


def _benchmark_window_summary(benchmark_returns: pd.Series, *, start: pd.Timestamp, end: pd.Timestamp):
    subset = _normalize_returns(benchmark_returns).loc[start:end]
    if subset.empty:
        return None
    return _summarize_window(subset)


def build_benchmark_returns(
    price_history: pd.DataFrame,
    *,
    symbols: Iterable[str] = ("QQQ", "SPY"),
) -> dict[str, pd.Series]:
    if price_history is None or len(price_history) == 0:
        return {}

    prices = pd.DataFrame(price_history).copy()
    if not {"symbol", "as_of", "close"} <= set(prices.columns):
        return {}

    prices["symbol"] = prices["symbol"].astype(str).str.upper()
    prices["as_of"] = pd.to_datetime(prices["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["symbol", "as_of", "close"]).sort_values(["symbol", "as_of"])

    benchmark_returns: dict[str, pd.Series] = {}
    for symbol in tuple(symbols):
        symbol_text = str(symbol or "").strip().upper()
        if not symbol_text:
            continue
        close = prices.loc[prices["symbol"].eq(symbol_text)].set_index("as_of")["close"].sort_index()
        close = close.loc[~close.index.duplicated(keep="last")]
        returns = close.pct_change(fill_method=None).dropna()
        if not returns.empty:
            benchmark_returns[symbol_text] = returns.rename(f"{symbol_text.lower()}_return")
    return benchmark_returns


def build_window_summary(
    portfolio_returns: pd.Series,
    windows: Iterable[BacktestWindow] | None = None,
    benchmark_returns: Mapping[str, pd.Series] | None = None,
    benchmark_tolerance: float = 0.0,
    primary_benchmark_symbol: str = "SPY",
) -> pd.DataFrame:
    returns = _normalize_returns(portfolio_returns)
    if returns.empty:
        return pd.DataFrame()

    normalized_benchmarks = {
        str(symbol).strip().upper(): _normalize_returns(series)
        for symbol, series in dict(benchmark_returns or {}).items()
        if str(symbol).strip()
    }
    benchmark_tolerance = max(0.0, float(benchmark_tolerance or 0.0))
    primary_benchmark = str(primary_benchmark_symbol or "").strip().upper()

    rows = []
    for window in tuple(windows) if windows is not None else build_standard_windows(returns):
        start = returns.index[0] if window.start is None else pd.Timestamp(window.start).normalize()
        end = returns.index[-1] if window.end is None else pd.Timestamp(window.end).normalize()
        subset = returns.loc[(returns.index >= start) & (returns.index <= end)]
        if subset.empty:
            continue
        window_summary = _summarize_window(subset)
        benchmark_start = pd.Timestamp(window_summary["Start"]).normalize()
        benchmark_end = pd.Timestamp(window_summary["End"]).normalize()
        benchmark_drawdowns = []
        for symbol, series in normalized_benchmarks.items():
            benchmark_summary = _benchmark_window_summary(series, start=benchmark_start, end=benchmark_end)
            if not benchmark_summary:
                continue
            benchmark_drawdown = float(benchmark_summary["Max Drawdown"])
            benchmark_drawdowns.append(benchmark_drawdown)
            window_summary[f"{symbol} Max Drawdown"] = benchmark_drawdown
            window_summary[f"{symbol} Drawdown Difference"] = float(
                window_summary["Max Drawdown"] - benchmark_drawdown
            )
            window_summary[f"Within {symbol} Drawdown"] = bool(
                float(window_summary["Max Drawdown"]) >= benchmark_drawdown - benchmark_tolerance
            )
            if symbol == primary_benchmark:
                window_summary["Primary Benchmark"] = symbol
                window_summary["Primary Benchmark Max Drawdown"] = benchmark_drawdown
                window_summary["Drawdown Difference vs Primary Benchmark"] = float(
                    window_summary["Max Drawdown"] - benchmark_drawdown
                )
                window_summary["Within Primary Benchmark Drawdown"] = bool(
                    float(window_summary["Max Drawdown"]) >= benchmark_drawdown - benchmark_tolerance
                )
        if benchmark_drawdowns:
            worst_benchmark_drawdown = min(benchmark_drawdowns)
            window_summary["Worst Benchmark Max Drawdown"] = worst_benchmark_drawdown
            window_summary["Drawdown Difference vs Worst Benchmark"] = float(
                window_summary["Max Drawdown"] - worst_benchmark_drawdown
            )
            window_summary["Within Worst Benchmark Drawdown"] = bool(
                float(window_summary["Max Drawdown"]) >= worst_benchmark_drawdown - benchmark_tolerance
            )
        rows.append(
            {
                "Window": window.name,
                "Requested Start": "" if window.start is None else str(pd.Timestamp(window.start).date()),
                "Requested End": "" if window.end is None else str(pd.Timestamp(window.end).date()),
                "Description": window.description,
                **window_summary,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "BacktestWindow",
    "build_benchmark_returns",
    "build_standard_windows",
    "build_window_summary",
]
