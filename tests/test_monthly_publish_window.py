from __future__ import annotations

from datetime import date

import pandas as pd

from us_equity_snapshot_pipelines.monthly_publish_window import (
    evaluate_monthly_publish_decision,
    resolve_month_end_trading_day,
)


def _prices_through(as_of_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "SPY", "as_of": "2026-04-09", "close": 100.0, "volume": 1_000_000},
            {"symbol": "QQQ", "as_of": as_of_date, "close": 120.0, "volume": 1_000_000},
        ]
    )


def test_skips_when_latest_price_date_is_not_month_end() -> None:
    decision = evaluate_monthly_publish_decision(price_history=_prices_through("2026-04-10"))

    assert decision.should_publish is False
    assert decision.snapshot_as_of == date(2026, 4, 10)
    assert decision.month_end_trading_day == date(2026, 4, 30)
    assert decision.reason == "snapshot_as_of_is_not_month_end_trading_day"


def test_publishes_when_latest_price_date_is_month_end() -> None:
    decision = evaluate_monthly_publish_decision(price_history=_prices_through("2026-04-30"))

    assert decision.should_publish is True
    assert decision.snapshot_as_of == date(2026, 4, 30)
    assert decision.month_end_trading_day == date(2026, 4, 30)
    assert decision.to_github_outputs()["should_publish"] == "true"


def test_uses_nyse_holiday_fallback_for_good_friday_month_end() -> None:
    month_end_trading_day, calendar_source = resolve_month_end_trading_day("2024-03-29")

    assert month_end_trading_day == date(2024, 3, 28)
    assert calendar_source in {"nyse_holiday_fallback", "pandas_market_calendars:NYSE"}
