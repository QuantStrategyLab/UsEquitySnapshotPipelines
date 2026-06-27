from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest import build_indicator_history, run_backtest


def _build_synthetic_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        xlk = 100.0 + idx * 0.6
        tecl = 50.0 + idx * 1.1
        boxx = 100.0
        qqqi = 50.0 + idx * 0.05
        spyi = 50.0 + idx * 0.03
        for symbol, close in (
            ("TECL", tecl),
            ("XLK", xlk),
            ("BOXX", boxx),
            ("SCHD", 70.0 + idx * 0.02),
            ("DGRO", 60.0 + idx * 0.02),
            ("SGOV", 100.0 + idx * 0.005),
            ("QQQI", qqqi),
            ("SPYI", spyi),
        ):
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def _build_chandelier_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        xlk = 100.0 + idx * 0.6
        if idx == 260:
            xlk -= 18.0
        values = {
            "TECL": 50.0 + idx * 1.1,
            "XLK": xlk,
            "BOXX": 100.0,
            "SCHD": 70.0 + idx * 0.02,
            "DGRO": 60.0 + idx * 0.02,
            "SGOV": 100.0 + idx * 0.005,
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


def _build_volatile_xlk_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        shock = 0.0
        if 240 <= idx < 255:
            shock = 4.0 if idx % 2 == 0 else -4.0
        values = {
            "TECL": 50.0 + idx * 1.1 + shock * 2.0,
            "XLK": 100.0 + idx * 0.6 + shock,
            "BOXX": 100.0,
            "SCHD": 70.0 + idx * 0.02,
            "DGRO": 60.0 + idx * 0.02,
            "SGOV": 100.0 + idx * 0.005,
            "QQQI": 50.0 + idx * 0.05,
            "SPYI": 50.0 + idx * 0.03,
        }
        for symbol, close in values.items():
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def _build_high_volatility_xlk_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        shock = 0.0
        if 240 <= idx < 255:
            shock = 15.0 if idx % 2 == 0 else -15.0
        values = {
            "TECL": 50.0 + idx * 1.1 + shock * 2.0,
            "XLK": 100.0 + idx * 0.6 + shock,
            "BOXX": 100.0,
            "SCHD": 70.0 + idx * 0.02,
            "DGRO": 60.0 + idx * 0.02,
            "SGOV": 100.0 + idx * 0.005,
            "QQQI": 50.0 + idx * 0.05,
            "SPYI": 50.0 + idx * 0.03,
        }
        for symbol, close in values.items():
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def _build_dual_ma_research_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        tecl = 210.0 - idx * 0.25 if idx >= 220 else 50.0 + idx * 0.65
        values = {
            "TECL": tecl,
            "XLK": 100.0 + idx * 0.6,
            "BOXX": 100.0,
            "SCHD": 70.0 + idx * 0.02,
            "DGRO": 60.0 + idx * 0.02,
            "SGOV": 100.0 + idx * 0.005,
            "QQQI": 50.0 + idx * 0.05,
            "SPYI": 50.0 + idx * 0.03,
        }
        for symbol, close in values.items():
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def test_tecl_xlk_trend_income_backtest_produces_summary() -> None:
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
    assert "trend_realized_volatility_10" in result["signal_history"].columns
    assert "trend_realized_volatility_20" in result["signal_history"].columns
    assert "blend_gate_volatility_delever_threshold_mode" in result["signal_history"].columns
    assert "blend_gate_volatility_delever_dynamic_threshold" in result["signal_history"].columns
    assert "tecl_delever_overlay_triggered" in result["signal_history"].columns
    assert "income_layer_activation_multiplier" in result["signal_history"].columns
    assert result["signal_history"]["trend_rsi14"].notna().any()
    assert result["signal_history"]["trend_bb_upper"].notna().any()
    assert result["signal_history"]["trend_realized_volatility_10"].notna().any()
    assert result["signal_history"]["trend_realized_volatility_20"].notna().any()
    assert result["signal_history"]["blend_gate_volatility_delever_threshold_mode"].eq("rolling_percentile").all()


def test_build_indicator_history_includes_xlk_realized_volatility() -> None:
    prices = _build_synthetic_prices()
    close_matrix = prices.pivot(index="as_of", columns="symbol", values="close")

    indicators = build_indicator_history(close_matrix)

    assert "realized_volatility" in indicators["xlk"].columns
    assert "ma10" in indicators["tecl"].columns
    assert "ma30" in indicators["tecl"].columns
    assert "realized_volatility_10" in indicators["xlk"].columns
    assert "realized_volatility_20" in indicators["xlk"].columns
    assert "realized_volatility_10_dynamic_threshold" in indicators["xlk"].columns
    assert "realized_volatility_10_dynamic_sample_count" in indicators["xlk"].columns
    assert indicators["xlk"]["realized_volatility_10"].notna().any()
    assert indicators["xlk"]["realized_volatility_20"].notna().any()
    assert (
        indicators["xlk"]["realized_volatility_10_dynamic_threshold"]
        .dropna()
        .between(
            0.50,
            0.75,
        )
        .all()
    )


def test_tecl_xlk_live_volatility_delever_moves_tecl_to_xlk() -> None:
    result = run_backtest(
        _build_high_volatility_xlk_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["blend_gate_volatility_delever_triggered"].astype(bool)]

    assert result["summary"]["TECL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["blend_gate_volatility_delever_window"].eq(10).all()
    assert triggered["blend_gate_volatility_delever_metric"].ge(0.55).all()
    assert triggered["blend_gate_volatility_delever_redirect_symbol"].eq("XLK").all()


def test_tecl_xlk_chandelier_stop_research_overlay_moves_tecl_to_boxx() -> None:
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


def test_tecl_xlk_volatility_delever_research_overlay_keeps_partial_tecl() -> None:
    result = run_backtest(
        _build_volatile_xlk_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        tecl_delever_overlay_kind="volatility",
        tecl_delever_overlay_symbol="XLK",
        tecl_delever_overlay_window=10,
        tecl_delever_overlay_threshold=0.20,
        tecl_delever_overlay_retention_ratio=0.50,
        tecl_delever_overlay_redirect_symbol="XLK",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["tecl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["TECL Delever Stops"] >= 1
    assert result["summary"]["Chandelier Stops"] == 0
    assert not triggered.empty
    assert triggered["tecl_delever_overlay_kind"].eq("volatility").all()
    assert triggered["tecl_delever_overlay_metric"].ge(0.20).all()
    assert triggered["tecl_delever_overlay_retention_ratio"].eq(0.50).all()


def test_tecl_xlk_dynamic_volatility_delever_research_overlay_records_thresholds() -> None:
    result = run_backtest(
        _build_high_volatility_xlk_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        tecl_delever_overlay_kind="volatility",
        tecl_delever_overlay_symbol="XLK",
        tecl_delever_overlay_window=10,
        tecl_delever_overlay_threshold=0.55,
        tecl_delever_overlay_threshold_mode="rolling_percentile",
        tecl_delever_overlay_threshold_lookback=60,
        tecl_delever_overlay_threshold_percentile=0.90,
        tecl_delever_overlay_threshold_min_periods=20,
        tecl_delever_overlay_threshold_floor=0.20,
        tecl_delever_overlay_threshold_cap=0.50,
        tecl_delever_overlay_retention_ratio=0.0,
        tecl_delever_overlay_redirect_symbol="XLK",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["tecl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["TECL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["tecl_delever_overlay_threshold_mode"].eq("rolling_percentile").all()
    assert triggered["tecl_delever_overlay_dynamic_threshold"].notna().all()
    assert triggered["tecl_delever_overlay_dynamic_sample_count"].ge(20).all()
    assert triggered["tecl_delever_overlay_threshold"].le(0.50).all()
    assert (triggered["tecl_delever_overlay_metric"] >= triggered["tecl_delever_overlay_threshold"]).all()


def test_tecl_xlk_dual_ma_research_overlay_keeps_partial_tecl() -> None:
    result = run_backtest(
        _build_dual_ma_research_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        tecl_delever_overlay_kind="dual_ma",
        tecl_delever_overlay_symbol="TECL",
        tecl_delever_overlay_fast_window=10,
        tecl_delever_overlay_slow_window=30,
        tecl_delever_overlay_retention_ratio=0.50,
        tecl_delever_overlay_redirect_symbol="XLK",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["tecl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["TECL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["tecl_delever_overlay_kind"].eq("dual_ma").all()
    assert triggered["tecl_delever_overlay_fast_window"].eq(10).all()
    assert triggered["tecl_delever_overlay_slow_window"].eq(30).all()
    assert (triggered["tecl_delever_overlay_fast_ma"] < triggered["tecl_delever_overlay_slow_ma"]).all()
    assert triggered["tecl_delever_overlay_retention_ratio"].eq(0.50).all()


def test_tecl_xlk_dynamic_rsi_quantile_uses_floor() -> None:
    dates = pd.bdate_range("2023-01-02", periods=320)
    close_matrix = pd.DataFrame(
        {
            "TECL": [50.0 + idx * 0.4 for idx in range(len(dates))],
            "XLK": [100.0 + idx * 0.2 for idx in range(len(dates))],
        },
        index=dates,
    )

    indicators = build_indicator_history(
        close_matrix,
        dynamic_rsi_quantile_window=252,
        dynamic_rsi_quantile=0.90,
        dynamic_rsi_floor=70.0,
    )
    xlk = indicators["xlk"]

    assert {"rsi14", "rsi14_raw", "rsi14_dynamic_threshold", "bb_upper"}.issubset(xlk.columns)
    assert xlk["rsi14_dynamic_threshold"].dropna().ge(70.0).all()
    pd.testing.assert_series_equal(
        xlk["rsi14"].dropna(),
        xlk["rsi14_raw"].dropna(),
        check_names=False,
    )
