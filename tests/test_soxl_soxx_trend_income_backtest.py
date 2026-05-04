from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest import run_backtest


def _build_synthetic_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=280)
    rows = []
    for idx, as_of in enumerate(dates):
        soxx = 100.0 + idx * 0.6
        soxl = 50.0 + idx * 1.1
        boxx = 100.0
        qqqi = 50.0 + idx * 0.05
        spyi = 50.0 + idx * 0.03
        for symbol, close in (
            ("SOXL", soxl),
            ("SOXX", soxx),
            ("BOXX", boxx),
            ("QQQI", qqqi),
            ("SPYI", spyi),
        ):
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def test_soxl_soxx_trend_income_backtest_produces_summary() -> None:
    prices = _build_synthetic_prices()
    result = run_backtest(
        prices,
        initial_equity=100_000.0,
        start_date="2023-07-03",
        end_date="2024-01-05",
        turnover_cost_bps=5.0,
    )

    summary = result["summary"]

    assert summary["Start"] >= "2023-07-03"
    assert summary["End"] == "2024-01-05"
    assert summary["CAGR"] > 0
    assert summary["Max Drawdown"] <= 0
    assert not result["trades"].empty
    assert not result["signal_history"].empty
