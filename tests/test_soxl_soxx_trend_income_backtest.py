from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest import build_indicator_history, run_backtest


def _build_synthetic_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
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


def _build_chandelier_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        soxx = 100.0 + idx * 0.6
        if idx == 260:
            soxx -= 18.0
        values = {
            "SOXL": 50.0 + idx * 1.1,
            "SOXX": soxx,
            "BOXX": 100.0,
            "QQQI": 50.0 + idx * 0.05,
            "SPYI": 50.0 + idx * 0.03,
        }
        for symbol, close in values.items():
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of,
                    "open": close - 0.1,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1_000_000.0,
                }
            )
    return pd.DataFrame(rows)


def test_soxl_soxx_trend_income_backtest_produces_summary() -> None:
    prices = _build_synthetic_prices()
    result = run_backtest(
        prices,
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
    )

    summary = result["summary"]

    assert summary["Start"] >= "2023-10-02"
    assert summary["End"] == "2024-03-29"
    assert summary["CAGR"] > 0
    assert summary["Max Drawdown"] <= 0
    assert not result["trades"].empty
    assert not result["signal_history"].empty
    assert "trend_rsi14" in result["signal_history"].columns
    assert "trend_bb_upper" in result["signal_history"].columns
    assert result["signal_history"]["trend_rsi14"].notna().any()
    assert result["signal_history"]["trend_bb_upper"].notna().any()


def test_soxl_soxx_chandelier_stop_research_overlay_moves_soxl_to_boxx() -> None:
    result = run_backtest(
        _build_chandelier_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        chandelier_stop_enabled=True,
        chandelier_window=22,
        chandelier_atr_multiple=1.0,
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["chandelier_stop_triggered"].astype(bool)]

    assert result["summary"]["Chandelier Stops"] >= 1
    assert not triggered.empty
    assert triggered["chandelier_stop_line"].notna().all()
    assert (triggered["chandelier_stop_close"] < triggered["chandelier_stop_line"]).all()


def test_soxl_soxx_dynamic_rsi_quantile_uses_floor() -> None:
    dates = pd.bdate_range("2023-01-02", periods=320)
    close_matrix = pd.DataFrame(
        {
            "SOXL": [50.0 + idx * 0.4 for idx in range(len(dates))],
            "SOXX": [100.0 + idx * 0.2 for idx in range(len(dates))],
        },
        index=dates,
    )

    indicators = build_indicator_history(
        close_matrix,
        dynamic_rsi_quantile_window=252,
        dynamic_rsi_quantile=0.90,
        dynamic_rsi_floor=70.0,
    )
    soxx = indicators["soxx"]

    assert {"rsi14", "rsi14_raw", "rsi14_dynamic_threshold", "bb_upper"}.issubset(soxx.columns)
    assert soxx["rsi14_dynamic_threshold"].dropna().ge(70.0).all()
    pd.testing.assert_series_equal(
        soxx["rsi14"].dropna(),
        soxx["rsi14_raw"].dropna(),
        check_names=False,
    )
