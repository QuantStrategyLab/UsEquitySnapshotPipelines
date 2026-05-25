from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.backtest_windows import build_benchmark_returns, build_window_summary


def test_build_window_summary_includes_short_and_regime_windows() -> None:
    dates = pd.bdate_range("2021-12-15", "2024-03-29")
    returns = pd.Series(0.001, index=dates, name="portfolio_return")
    returns.loc[pd.Timestamp("2022-06-13")] = -0.05

    summary = build_window_summary(returns)

    assert {"live_short_ytd", "live_short_3m", "rate_bear_2022", "post_2022_bull", "long_15y_to_date"} <= set(
        summary["Window"]
    )
    rate_bear = summary.loc[summary["Window"].eq("rate_bear_2022")].iloc[0]
    assert rate_bear["Start"] == "2022-01-03"
    assert rate_bear["End"] == "2022-12-30"
    assert rate_bear["Max Drawdown"] < 0


def test_build_window_summary_compares_portfolio_drawdown_to_benchmarks() -> None:
    dates = pd.bdate_range("2022-01-03", "2022-03-31")
    returns = pd.Series(0.001, index=dates, name="portfolio_return")
    returns.iloc[20] = -0.08
    qqq_returns = pd.Series(0.001, index=dates, name="qqq_return")
    qqq_returns.iloc[20] = -0.10
    spy_returns = pd.Series(0.001, index=dates, name="spy_return")
    spy_returns.iloc[20] = -0.05

    summary = build_window_summary(
        returns,
        benchmark_returns={"QQQ": qqq_returns, "SPY": spy_returns},
    )
    rate_bear = summary.loc[summary["Window"].eq("rate_bear_2022")].iloc[0]

    assert rate_bear["QQQ Max Drawdown"] < 0
    assert rate_bear["SPY Max Drawdown"] < 0
    assert bool(rate_bear["Within QQQ Drawdown"]) is True
    assert bool(rate_bear["Within SPY Drawdown"]) is False
    assert rate_bear["Primary Benchmark"] == "SPY"
    assert rate_bear["Primary Benchmark Max Drawdown"] == rate_bear["SPY Max Drawdown"]
    assert bool(rate_bear["Within Primary Benchmark Drawdown"]) is False
    assert bool(rate_bear["Within Worst Benchmark Drawdown"]) is True


def test_build_window_summary_aligns_benchmark_to_actual_portfolio_window() -> None:
    portfolio_dates = pd.bdate_range("2024-01-02", "2024-03-29")
    returns = pd.Series(0.001, index=portfolio_dates, name="portfolio_return")
    returns.iloc[20] = -0.06

    benchmark_dates = pd.bdate_range("2022-01-03", "2024-03-29")
    spy_returns = pd.Series(0.001, index=benchmark_dates, name="spy_return")
    spy_returns.loc[pd.Timestamp("2022-06-13")] = -0.50
    spy_returns.loc[portfolio_dates[20]] = -0.05

    summary = build_window_summary(returns, benchmark_returns={"SPY": spy_returns})
    long_window = summary.loc[summary["Window"].eq("long_15y_to_date")].iloc[0]

    assert long_window["Start"] == "2024-01-02"
    assert long_window["SPY Max Drawdown"] > -0.10
    assert bool(long_window["Within Primary Benchmark Drawdown"]) is False


def test_build_benchmark_returns_extracts_qqq_and_spy_from_price_history() -> None:
    dates = pd.bdate_range("2024-01-02", periods=4)
    prices = pd.DataFrame(
        [
            {"symbol": symbol, "as_of": as_of, "close": close}
            for symbol, closes in {
                "QQQ": [100.0, 101.0, 99.0, 102.0],
                "SPY": [90.0, 91.0, 89.0, 92.0],
                "TQQQ": [50.0, 52.0, 48.0, 53.0],
            }.items()
            for as_of, close in zip(dates, closes)
        ]
    )

    benchmarks = build_benchmark_returns(prices)

    assert set(benchmarks) == {"QQQ", "SPY"}
    assert len(benchmarks["QQQ"]) == 3
