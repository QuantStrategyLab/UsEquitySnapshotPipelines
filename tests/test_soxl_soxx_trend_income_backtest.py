from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import intraday_crash_circuit_breaker_research as intraday
from us_equity_snapshot_pipelines.pipelines import soxl_soxx_trend_income_backtest as backtest
from us_equity_snapshot_pipelines.pipelines.soxl_soxx_trend_income_backtest import build_indicator_history, run_backtest


def _accounting_prices(soxl=(100.0, 100.0, 100.0), boxx=(100.0, 100.0, 100.0)) -> pd.DataFrame:
    return pd.DataFrame(
        {"symbol": symbol, "as_of": date, "close": close}
        for date, soxl_close, boxx_close in zip(pd.bdate_range("2024-01-02", periods=3), soxl, boxx)
        for symbol, close in (("SOXL", soxl_close), ("SOXX", 100.0), ("BOXX", boxx_close))
    )


def _buy_once_then_hold(monkeypatch, first_targets):
    accounts = []

    def plan(_indicators, account, **_kwargs):
        accounts.append(account)
        targets = first_targets if len(accounts) == 1 else account["market_values"]
        return {"targets": targets, "threshold_value": 1e-7, "current_min_trade": 0.0}

    monkeypatch.setattr(
        backtest, "_indicator_snapshot_at",
        lambda *_args: {symbol: {"price": 100.0, "ma_trend": 100.0} for symbol in ("soxl", "soxx")},
    )
    monkeypatch.setattr(backtest, "build_rebalance_plan", plan)
    return accounts


def test_accounting_no_trade_preserves_shares_and_natural_weight_drift(monkeypatch):
    accounts = _buy_once_then_hold(monkeypatch, {"SOXL": 50_000.0, "BOXX": 50_000.0})
    result = run_backtest(
        _accounting_prices(soxl=(100.0, 110.0, 100.0)), start_date="2024-01-02", turnover_cost_bps=0.0
    )

    assert accounts[1]["market_values"]["SOXL"] == pytest.approx(55_000.0)
    assert accounts[1]["market_values"]["BOXX"] == pytest.approx(50_000.0)
    # History is indexed by the end date of each holding interval.
    assert result["weights_history"].iloc[1]["SOXL"] == pytest.approx(0.5)
    assert result["weights_history"].iloc[2]["SOXL"] == pytest.approx(55_000.0 / 105_000.0)
    assert result["weights_history"].iloc[0]["BOXX"] == 1.0
    assert result["turnover_history"].iloc[1:].tolist() == pytest.approx([0.5, 0.0])
    assert result["summary"]["Total Return"] == pytest.approx(0.0, abs=1e-12)
    years = 2 / 365.25
    assert result["summary"]["Rebalances/Year"] == pytest.approx(1 / years)
    assert result["summary"]["Turnover/Year"] == pytest.approx(0.5 / years)


def test_accounting_returns_and_drawdown_include_cost_from_initial_equity(monkeypatch):
    accounts = _buy_once_then_hold(monkeypatch, {"SOXL": 50_000.0, "BOXX": 25_000.0})
    result = run_backtest(_accounting_prices(), start_date="2024-01-02", turnover_cost_bps=100.0)

    # Sell 75k BOXX, buy 50k SOXL: one-way turnover includes the 25k cash leg.
    assert accounts[1]["total_strategy_equity"] == pytest.approx(99_250.0)
    assert result["portfolio_returns"].dropna().tolist() == pytest.approx([-0.0075, 0.0])
    assert result["summary"]["Final Equity"] == pytest.approx(99_250.0 / 100_000.0)
    assert result["summary"]["Max Drawdown"] == pytest.approx(-0.0075)
    assert result["summary"]["Start"] == "2024-01-02"


@pytest.mark.parametrize(
    "weights,targets,expected_equity,expected_turnover",
    [
        ({"BOXX": 1.0}, {"SOXL": 100_000.0}, 99_000.0, 1.0),
        ({"__cash__": 1.0}, {"SOXL": 100_000.0}, 100_000.0 / 1.01, 1.0 / 1.01),
        ({"SOXL": 1.0}, {}, 99_000.0, 1.0),
    ],
)
def test_accounting_full_allocation_reserves_fees_and_includes_cash_turnover(
    weights, targets, expected_equity, expected_turnover
):
    next_weights, turnover, equity = backtest._execute_rebalance(
        current_weights=weights,
        target_values=targets,
        equity=100_000.0,
        threshold_value=0.0,
        current_min_trade=0.0,
        turnover_cost_bps=100.0,
    )

    assert equity == pytest.approx(expected_equity)
    assert turnover == pytest.approx(expected_turnover)
    assert equity + 100_000.0 * turnover * 0.01 == pytest.approx(100_000.0)
    assert sum(next_weights.values()) == pytest.approx(1.0)
    assert min(next_weights.values()) >= 0.0


def test_accounting_without_signals_still_values_initial_boxx(monkeypatch):
    monkeypatch.setattr(backtest, "_indicator_snapshot_at", lambda *_args: {})
    result = run_backtest(
        _accounting_prices(boxx=(100.0, 101.0, 102.0)), start_date="2024-01-02", turnover_cost_bps=100.0
    )

    assert result["summary"]["Total Return"] == pytest.approx(0.02)
    assert result["weights_history"]["BOXX"].tolist() == [1.0, 1.0, 1.0]
    assert result["turnover_history"].tolist() == [0.0, 0.0, 0.0]
    assert result["signal_history"].empty
    assert result["trades"].empty


def test_accounting_missing_signal_keeps_existing_position_and_fee_in_nav(monkeypatch):
    _buy_once_then_hold(monkeypatch, {"SOXL": 50_000.0, "BOXX": 50_000.0})
    snapshots = iter([
        {symbol: {"price": 100.0, "ma_trend": 100.0} for symbol in ("soxl", "soxx")}, {},
    ])
    monkeypatch.setattr(backtest, "_indicator_snapshot_at", lambda *_args: next(snapshots))
    result = run_backtest(
        _accounting_prices(soxl=(100.0, 110.0, 100.0)), start_date="2024-01-02", turnover_cost_bps=100.0
    )

    # Selling 50k BOXX pays 500; the remaining holdings are 49.5k SOXL + 50k BOXX.
    assert result["portfolio_returns"].dropna().tolist() == pytest.approx([0.0445, 99_500.0 / 104_450.0 - 1.0])
    assert result["summary"]["Final Equity"] == pytest.approx(0.995)
    assert result["turnover_history"].tolist() == pytest.approx([0.0, 0.5, 0.0])
    assert len(result["signal_history"]) == 1


def test_accounting_strategy_failure_cannot_be_reported_as_successful_cash_hold(monkeypatch):
    _buy_once_then_hold(monkeypatch, {})

    def fail(*_args, **_kwargs):
        raise ValueError("private strategy detail")

    monkeypatch.setattr(backtest, "build_rebalance_plan", fail)
    with pytest.raises(RuntimeError, match="^Strategy evaluation failed during research backtest$"):
        run_backtest(_accounting_prices(), start_date="2024-01-02")


def test_accounting_intraday_consumer_uses_the_net_return_capital_basis(monkeypatch):
    _buy_once_then_hold(monkeypatch, {"SOXL": 100_000.0})
    prices = _accounting_prices(soxl=(100.0, 90.0, 90.0))
    spec = replace(intraday.STRATEGY_SPECS["soxl"], start_date="2024-01-02")
    result = intraday._run_core_backtest(spec, prices)
    adjusted, events = intraday.apply_crash_circuit_breaker(
        portfolio_returns=result["portfolio_returns"], weights_history=result["weights_history"],
        prices=prices, risk_symbols=spec.risk_symbols, threshold=-0.05, circuit_cost_bps=0.0,
    )

    # The real consumer uses 5 bps: 50 fee, 99,950 invested, then a 5% loss.
    assert result["portfolio_returns"].iloc[1] == pytest.approx(99_950.0 * 0.90 / 100_000.0 - 1.0)
    assert adjusted.iloc[0] == pytest.approx(99_950.0 * 0.95 / 100_000.0 - 1.0)
    assert len(events) == 1


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
        soxx = 100.0 + idx * 0.6
        if idx == 260:
            soxx -= 18.0
        values = {
            "SOXL": 50.0 + idx * 1.1,
            "SOXX": soxx,
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


def _build_volatile_soxx_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        shock = 0.0
        if 240 <= idx < 255:
            shock = 4.0 if idx % 2 == 0 else -4.0
        values = {
            "SOXL": 50.0 + idx * 1.1 + shock * 2.0,
            "SOXX": 100.0 + idx * 0.6 + shock,
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


def _build_high_volatility_soxx_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=420)
    rows = []
    for idx, as_of in enumerate(dates):
        shock = 0.0
        if 240 <= idx < 255:
            shock = 15.0 if idx % 2 == 0 else -15.0
        values = {
            "SOXL": 50.0 + idx * 1.1 + shock * 2.0,
            "SOXX": 100.0 + idx * 0.6 + shock,
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
        soxl = 210.0 - idx * 0.25 if idx >= 220 else 50.0 + idx * 0.65
        values = {
            "SOXL": soxl,
            "SOXX": 100.0 + idx * 0.6,
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
    assert "trend_realized_volatility_10" in result["signal_history"].columns
    assert "trend_realized_volatility_20" in result["signal_history"].columns
    assert "blend_gate_volatility_delever_threshold_mode" in result["signal_history"].columns
    assert "blend_gate_volatility_delever_dynamic_threshold" in result["signal_history"].columns
    assert "soxl_delever_overlay_triggered" in result["signal_history"].columns
    assert "income_layer_activation_multiplier" in result["signal_history"].columns
    assert result["signal_history"]["trend_rsi14"].notna().any()
    assert result["signal_history"]["trend_bb_upper"].notna().any()
    assert result["signal_history"]["trend_realized_volatility_10"].notna().any()
    assert result["signal_history"]["trend_realized_volatility_20"].notna().any()
    assert result["signal_history"]["blend_gate_volatility_delever_threshold_mode"].eq("rolling_percentile").all()


def test_build_indicator_history_includes_soxx_realized_volatility() -> None:
    prices = _build_synthetic_prices()
    close_matrix = prices.pivot(index="as_of", columns="symbol", values="close")

    indicators = build_indicator_history(close_matrix)

    assert "realized_volatility" in indicators["soxx"].columns
    assert "ma10" in indicators["soxl"].columns
    assert "ma30" in indicators["soxl"].columns
    assert "realized_volatility_10" in indicators["soxx"].columns
    assert "realized_volatility_20" in indicators["soxx"].columns
    assert "realized_volatility_10_dynamic_threshold" in indicators["soxx"].columns
    assert "realized_volatility_10_dynamic_sample_count" in indicators["soxx"].columns
    assert indicators["soxx"]["realized_volatility_10"].notna().any()
    assert indicators["soxx"]["realized_volatility_20"].notna().any()
    assert (
        indicators["soxx"]["realized_volatility_10_dynamic_threshold"]
        .dropna()
        .between(
            0.50,
            0.75,
        )
        .all()
    )


def test_soxl_soxx_live_volatility_delever_moves_soxl_to_soxx() -> None:
    result = run_backtest(
        _build_high_volatility_soxx_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["blend_gate_volatility_delever_triggered"].astype(bool)]

    assert result["summary"]["SOXL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["blend_gate_volatility_delever_window"].eq(10).all()
    assert triggered["blend_gate_volatility_delever_metric"].ge(0.55).all()
    assert triggered["blend_gate_volatility_delever_redirect_symbol"].eq("SOXX").all()


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


def test_soxl_soxx_volatility_delever_research_overlay_keeps_partial_soxl() -> None:
    result = run_backtest(
        _build_volatile_soxx_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        soxl_delever_overlay_kind="volatility",
        soxl_delever_overlay_symbol="SOXX",
        soxl_delever_overlay_window=10,
        soxl_delever_overlay_threshold=0.20,
        soxl_delever_overlay_retention_ratio=0.50,
        soxl_delever_overlay_redirect_symbol="SOXX",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["soxl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["SOXL Delever Stops"] >= 1
    assert result["summary"]["Chandelier Stops"] == 0
    assert not triggered.empty
    assert triggered["soxl_delever_overlay_kind"].eq("volatility").all()
    assert triggered["soxl_delever_overlay_metric"].ge(0.20).all()
    assert triggered["soxl_delever_overlay_retention_ratio"].eq(0.50).all()


def test_soxl_soxx_dynamic_volatility_delever_research_overlay_records_thresholds() -> None:
    result = run_backtest(
        _build_high_volatility_soxx_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        soxl_delever_overlay_kind="volatility",
        soxl_delever_overlay_symbol="SOXX",
        soxl_delever_overlay_window=10,
        soxl_delever_overlay_threshold=0.55,
        soxl_delever_overlay_threshold_mode="rolling_percentile",
        soxl_delever_overlay_threshold_lookback=60,
        soxl_delever_overlay_threshold_percentile=0.90,
        soxl_delever_overlay_threshold_min_periods=20,
        soxl_delever_overlay_threshold_floor=0.20,
        soxl_delever_overlay_threshold_cap=0.50,
        soxl_delever_overlay_retention_ratio=0.0,
        soxl_delever_overlay_redirect_symbol="SOXX",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["soxl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["SOXL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["soxl_delever_overlay_threshold_mode"].eq("rolling_percentile").all()
    assert triggered["soxl_delever_overlay_dynamic_threshold"].notna().all()
    assert triggered["soxl_delever_overlay_dynamic_sample_count"].ge(20).all()
    assert triggered["soxl_delever_overlay_threshold"].le(0.50).all()
    assert (triggered["soxl_delever_overlay_metric"] >= triggered["soxl_delever_overlay_threshold"]).all()


def test_soxl_soxx_volatility_reentry_hysteresis_and_cooldown_hold_delevered_state() -> None:
    result = run_backtest(
        _build_high_volatility_soxx_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        soxl_delever_overlay_kind="volatility",
        soxl_delever_overlay_symbol="SOXX",
        soxl_delever_overlay_window=10,
        soxl_delever_overlay_threshold=0.20,
        soxl_delever_overlay_reentry_hysteresis=0.05,
        soxl_delever_overlay_reentry_cooldown_days=2,
        soxl_delever_overlay_retention_ratio=0.0,
        soxl_delever_overlay_redirect_symbol="SOXX",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["soxl_delever_overlay_triggered"].astype(bool)]
    held_after_raw_trigger = triggered.loc[~triggered["soxl_delever_overlay_raw_triggered"].astype(bool)]

    assert not triggered.empty
    assert not held_after_raw_trigger.empty
    assert triggered["soxl_delever_overlay_reentry_hysteresis"].eq(0.05).all()
    assert triggered["soxl_delever_overlay_reentry_cooldown_days"].eq(2).all()
    assert (
        triggered["soxl_delever_overlay_reentry_threshold"]
        == triggered["soxl_delever_overlay_threshold"] - 0.05
    ).all()


def test_soxl_soxx_dual_ma_research_overlay_keeps_partial_soxl() -> None:
    result = run_backtest(
        _build_dual_ma_research_prices(),
        initial_equity=100_000.0,
        start_date="2023-10-02",
        end_date="2024-03-29",
        turnover_cost_bps=5.0,
        soxl_delever_overlay_kind="dual_ma",
        soxl_delever_overlay_symbol="SOXL",
        soxl_delever_overlay_fast_window=10,
        soxl_delever_overlay_slow_window=30,
        soxl_delever_overlay_retention_ratio=0.50,
        soxl_delever_overlay_redirect_symbol="SOXX",
    )

    signal_history = result["signal_history"]
    triggered = signal_history.loc[signal_history["soxl_delever_overlay_triggered"].astype(bool)]

    assert result["summary"]["SOXL Delever Stops"] >= 1
    assert not triggered.empty
    assert triggered["soxl_delever_overlay_kind"].eq("dual_ma").all()
    assert triggered["soxl_delever_overlay_fast_window"].eq(10).all()
    assert triggered["soxl_delever_overlay_slow_window"].eq(30).all()
    assert (triggered["soxl_delever_overlay_fast_ma"] < triggered["soxl_delever_overlay_slow_ma"]).all()
    assert triggered["soxl_delever_overlay_retention_ratio"].eq(0.50).all()


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
