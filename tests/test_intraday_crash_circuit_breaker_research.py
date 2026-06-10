from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.intraday_crash_circuit_breaker_research import (
    _align_hourly_close_to_daily_prices,
    apply_crash_circuit_breaker,
)
from us_equity_snapshot_pipelines.intraday_scheme_research import (
    HourlyOverlayRule,
    apply_execution_timing_overlay,
    apply_hourly_overlay,
)


def test_hourly_circuit_uses_first_threshold_breach_not_intraday_low() -> None:
    prices = pd.DataFrame(
        [
            {"symbol": "TQQQ", "as_of": "2024-01-02", "close": 100.0},
            {"symbol": "TQQQ", "as_of": "2024-01-03", "close": 90.0},
        ]
    )
    hourly_close = pd.DataFrame(
        {"TQQQ": [98.0, 94.0, 89.0]},
        index=pd.to_datetime(["2024-01-03 14:30", "2024-01-03 15:30", "2024-01-03 16:30"]),
    )
    portfolio_returns = pd.Series([-0.10], index=pd.to_datetime(["2024-01-03"]))
    weights_history = pd.DataFrame({"TQQQ": [1.0]}, index=pd.to_datetime(["2024-01-03"]))

    adjusted, events = apply_crash_circuit_breaker(
        portfolio_returns=portfolio_returns,
        weights_history=weights_history,
        prices=prices,
        risk_symbols=["TQQQ"],
        threshold=-0.05,
        circuit_cost_bps=0.0,
        hourly_close=hourly_close,
    )

    assert round(float(adjusted.iloc[0]), 4) == -0.06
    assert round(float(events.iloc[0]["trigger_return"]), 4) == -0.06
    assert events.iloc[0]["trigger_time"] == "2024-01-03 15:30:00"


def test_hourly_close_alignment_matches_local_daily_price_scale() -> None:
    prices = pd.DataFrame(
        [
            {"symbol": "TQQQ", "as_of": "2024-01-02", "close": 50.0},
            {"symbol": "TQQQ", "as_of": "2024-01-03", "close": 51.0},
        ]
    )
    hourly_close = pd.DataFrame(
        {"TQQQ": [200.0, 204.0]},
        index=pd.to_datetime(["2024-01-03 14:30", "2024-01-03 21:00"]),
    )

    aligned = _align_hourly_close_to_daily_prices(hourly_close, prices)

    assert aligned is not None
    assert round(float(aligned.loc[pd.Timestamp("2024-01-03 14:30"), "TQQQ"]), 2) == 50.0
    assert round(float(aligned.loc[pd.Timestamp("2024-01-03 21:00"), "TQQQ"]), 2) == 51.0


def test_hourly_overlay_can_reenter_same_day_after_recovery() -> None:
    prices = pd.DataFrame(
        [
            {"symbol": "SOXL", "as_of": "2024-01-02", "close": 100.0},
            {"symbol": "SOXL", "as_of": "2024-01-03", "close": 103.0},
        ]
    )
    hourly_close = pd.DataFrame(
        {"SOXL": [92.0, 96.0, 103.0]},
        index=pd.to_datetime(["2024-01-03 15:30", "2024-01-03 16:30", "2024-01-03 20:30"]),
    )
    portfolio_returns = pd.Series([0.03], index=pd.to_datetime(["2024-01-03"]))
    weights_history = pd.DataFrame({"SOXL": [1.0]}, index=pd.to_datetime(["2024-01-03"]))

    adjusted, events = apply_hourly_overlay(
        portfolio_returns=portfolio_returns,
        weights_history=weights_history,
        prices=prices,
        risk_symbols=["SOXL"],
        hourly_close=hourly_close,
        rule=HourlyOverlayRule(name="test_reentry", mode="same_day_reentry", threshold=-0.08),
        circuit_cost_bps=0.0,
    )

    assert events.iloc[0]["event_type"] == "same_day_reentry"
    assert round(float(adjusted.iloc[0]), 4) == -0.0129


def test_execution_timing_overlay_delays_new_weight_until_slot() -> None:
    prices = pd.DataFrame(
        [
            {"symbol": "TQQQ", "as_of": "2024-01-01", "close": 100.0},
            {"symbol": "TQQQ", "as_of": "2024-01-02", "close": 100.0},
            {"symbol": "TQQQ", "as_of": "2024-01-03", "close": 110.0},
        ]
    )
    intraday_close = pd.DataFrame(
        {"TQQQ": [105.0, 110.0]},
        index=pd.to_datetime(["2024-01-03 14:45", "2024-01-03 20:00"]),
    )
    portfolio_returns = pd.Series([0.0, 0.10], index=pd.to_datetime(["2024-01-02", "2024-01-03"]))
    weights_history = pd.DataFrame({"TQQQ": [0.0, 1.0]}, index=pd.to_datetime(["2024-01-02", "2024-01-03"]))

    adjusted, events = apply_execution_timing_overlay(
        portfolio_returns=portfolio_returns,
        weights_history=weights_history,
        prices=prices,
        intraday_close=intraday_close,
        slot="first_15m",
    )

    assert round(float(events.iloc[0]["pre_execution_adjustment"]), 4) == -0.05
    assert round(float(adjusted.loc[pd.Timestamp("2024-01-03")]), 4) == 0.05
