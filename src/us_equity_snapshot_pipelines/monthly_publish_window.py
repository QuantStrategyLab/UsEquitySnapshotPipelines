from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .contracts import RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE, TECH_COMMUNICATION_PULLBACK_PROFILE


MONTHLY_SNAPSHOT_PROFILES = frozenset(
    {
        TECH_COMMUNICATION_PULLBACK_PROFILE,
        RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE,
    }
)


@dataclass(frozen=True)
class MonthlyPublishDecision:
    should_publish: bool
    snapshot_as_of: date
    month_end_trading_day: date
    reason: str
    calendar_source: str

    def to_github_outputs(self) -> dict[str, str]:
        return {
            "should_publish": str(self.should_publish).lower(),
            "snapshot_as_of": self.snapshot_as_of.isoformat(),
            "month_end_trading_day": self.month_end_trading_day.isoformat(),
            "publish_reason": self.reason,
            "calendar_source": self.calendar_source,
        }


def read_price_history(path: str | Path) -> pd.DataFrame:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise EnvironmentError("prices path is required")
    table_path = Path(raw_path)
    if not table_path.exists():
        raise FileNotFoundError(f"prices file not found: {table_path}")

    suffix = table_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(table_path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(table_path, orient="records", lines=suffix == ".jsonl")
    if suffix == ".parquet":
        return pd.read_parquet(table_path)
    raise ValueError("Unsupported prices format; expected .csv, .json, .jsonl, or .parquet")


def resolve_snapshot_as_of(price_history: pd.DataFrame, as_of_date: str | date | None = None) -> date:
    if as_of_date:
        return _coerce_date(as_of_date)
    if "as_of" not in price_history.columns:
        raise ValueError("prices missing required column: as_of")
    parsed = pd.to_datetime(price_history["as_of"], errors="coerce")
    if parsed.notna().sum() == 0:
        raise ValueError("prices as_of column contains no parseable dates")
    return pd.Timestamp(parsed.max()).date()


def evaluate_monthly_publish_decision(
    *,
    price_history: pd.DataFrame,
    as_of_date: str | date | None = None,
) -> MonthlyPublishDecision:
    snapshot_as_of = resolve_snapshot_as_of(price_history, as_of_date=as_of_date)
    month_end_trading_day, calendar_source = resolve_month_end_trading_day(snapshot_as_of)
    should_publish = snapshot_as_of == month_end_trading_day
    reason = (
        "snapshot_as_of_is_month_end_trading_day"
        if should_publish
        else "snapshot_as_of_is_not_month_end_trading_day"
    )
    return MonthlyPublishDecision(
        should_publish=should_publish,
        snapshot_as_of=snapshot_as_of,
        month_end_trading_day=month_end_trading_day,
        reason=reason,
        calendar_source=calendar_source,
    )


def resolve_month_end_trading_day(value: str | date) -> tuple[date, str]:
    target_date = _coerce_date(value)
    calendar_result = _resolve_month_end_trading_day_with_market_calendar(target_date)
    if calendar_result is not None:
        return calendar_result, "pandas_market_calendars:NYSE"
    return _resolve_month_end_trading_day_with_nyse_holiday_fallback(target_date), "nyse_holiday_fallback"


def _resolve_month_end_trading_day_with_market_calendar(target_date: date) -> date | None:
    try:
        import pandas_market_calendars as market_calendars  # type: ignore[import-not-found]
    except Exception:
        return None

    month_last_day = date(
        target_date.year,
        target_date.month,
        calendar.monthrange(target_date.year, target_date.month)[1],
    )
    try:
        nyse = market_calendars.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=date(target_date.year, target_date.month, 1).isoformat(),
            end_date=month_last_day.isoformat(),
        )
    except Exception:
        return None
    if schedule.empty:
        return None
    return pd.Timestamp(schedule.index[-1]).date()


def _resolve_month_end_trading_day_with_nyse_holiday_fallback(target_date: date) -> date:
    month_last_day = date(
        target_date.year,
        target_date.month,
        calendar.monthrange(target_date.year, target_date.month)[1],
    )
    holidays = _nyse_holidays(target_date.year)
    if target_date.month == 12:
        holidays |= _nyse_holidays(target_date.year + 1)

    candidate = month_last_day
    while candidate.weekday() >= 5 or candidate in holidays:
        candidate -= timedelta(days=1)
    return candidate


def _nyse_holidays(year: int) -> set[date]:
    holidays = {
        _observed_holiday(date(year, 1, 1)),
        _nth_weekday(year, 1, calendar.MONDAY, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, calendar.MONDAY, 3),  # Washington's Birthday
        _easter_date(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, calendar.MONDAY),  # Memorial Day
        _observed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, calendar.MONDAY, 1),  # Labor Day
        _nth_weekday(year, 11, calendar.THURSDAY, 4),  # Thanksgiving Day
        _observed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_holiday(date(year, 6, 19)))
    return holidays


def _observed_holiday(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year, month, calendar.monthrange(year, month)[1])
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    leap = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * leap) // 451
    month = (h + leap - 7 * m + 114) // 31
    day = ((h + leap - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _write_github_outputs(path: str | Path, outputs: dict[str, str]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fh:
        for key, value in outputs.items():
            fh.write(f"{key}={value}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check whether a monthly snapshot should be published.")
    parser.add_argument("--prices", required=True, help="Resolved local price history file")
    parser.add_argument("--as-of", dest="as_of_date", help="Optional snapshot date override")
    parser.add_argument("--env-output", help="Optional GitHub output file path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    price_history = read_price_history(args.prices)
    decision = evaluate_monthly_publish_decision(price_history=price_history, as_of_date=args.as_of_date)
    outputs = decision.to_github_outputs()
    for key, value in outputs.items():
        print(f"{key}={value}")
    if args.env_output:
        _write_github_outputs(args.env_output, outputs)
    return 0
