from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.tech_communication_pullback_backtest import run_period_backtests


def _synthetic_price_history() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", "2025-06-30")
    specs = {
        "QQQ": (100.0, 0.00035, 100_000_000),
        "SPY": (100.0, 0.00025, 100_000_000),
        "BOXX": (100.0, 0.00005, 10_000_000),
        "AAPL": (100.0, 0.00070, 80_000_000),
        "MSFT": (95.0, 0.00065, 75_000_000),
        "META": (90.0, 0.00060, 70_000_000),
        "NFLX": (80.0, 0.00050, 60_000_000),
    }
    rows = []
    for symbol, (base, daily_return, volume) in specs.items():
        for idx, as_of in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of,
                    "close": base * ((1.0 + daily_return) ** idx),
                    "volume": volume,
                }
            )
    return pd.DataFrame(rows)


def _synthetic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology", "start_date": "2024-01-01", "end_date": None},
            {"symbol": "MSFT", "sector": "Information Technology", "start_date": "2024-01-01", "end_date": None},
            {"symbol": "META", "sector": "Communication", "start_date": "2024-01-01", "end_date": None},
            {"symbol": "NFLX", "sector": "Communication", "start_date": "2024-01-01", "end_date": None},
        ]
    )


def test_tech_communication_pullback_period_backtest_smoke() -> None:
    results = run_period_backtests(
        _synthetic_price_history(),
        _synthetic_universe(),
        periods="smoke:2025-03-01:2025-06-30",
        turnover_cost_bps=0.0,
    )

    result = results["smoke"]
    assert result.summary["Benchmark Symbol"] == "QQQ"
    assert result.summary["Broad Benchmark Symbol"] == "SPY"
    assert result.summary["CAGR"] > 0.0
    assert not result.rebalance_log.empty
    assert result.rebalance_log["selected_count"].max() > 0
    assert result.weights_history.sum(axis=1).max() <= 1.0000001
