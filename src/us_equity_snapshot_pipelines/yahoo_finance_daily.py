"""Non-live Yahoo Finance adjusted-daily cross-check adapter.

This module wraps the repository's existing public Yahoo chart downloader in
the shared source-observation contract.  It is intentionally a diagnostic
cross-check: it never publishes a P1 root, chooses a fallback, changes a
strategy, or accesses a broker.  A caller must obtain independent agreement
through the shared multi-source assurance gate before it can use any result as
a canonical research input.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from hashlib import sha256
from urllib.error import HTTPError, URLError

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_INVALID,
    SOURCE_OBSERVATION_READY,
    SOURCE_OBSERVATION_UNAVAILABLE,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

from .yfinance_prices import PRICE_FIELD_ADJUSTED_CLOSE, download_yahoo_chart_price_history

YAHOO_FINANCE_DAILY_SOURCE_ID = "yahoo_finance_chart_1day_adjusted"
YAHOO_FINANCE_ADJUSTMENT_BASIS = "total_return_adjusted"

YAHOO_FINANCE_RATE_LIMITED = "YAHOO_FINANCE_RATE_LIMITED"
YAHOO_FINANCE_SERVICE_UNAVAILABLE = "YAHOO_FINANCE_SERVICE_UNAVAILABLE"
YAHOO_FINANCE_TRANSPORT_UNAVAILABLE = "YAHOO_FINANCE_TRANSPORT_UNAVAILABLE"
YAHOO_FINANCE_REQUEST_REJECTED = "YAHOO_FINANCE_REQUEST_REJECTED"
YAHOO_FINANCE_PAYLOAD_INVALID = "YAHOO_FINANCE_PAYLOAD_INVALID"


class YahooFinanceUnavailableError(RuntimeError):
    """Safe terminal state for a temporarily unavailable public cross-check."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class YahooFinancePayloadError(RuntimeError):
    """The public response cannot be safely normalized as adjusted daily bars."""


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return sha256(encoded).hexdigest()


def _reason_for_http_status(status: int) -> str:
    if status == 429:
        return YAHOO_FINANCE_RATE_LIMITED
    if 500 <= status <= 599:
        return YAHOO_FINANCE_SERVICE_UNAVAILABLE
    return YAHOO_FINANCE_REQUEST_REJECTED


def observe_yahoo_finance_adjusted_daily_bars(
    *,
    symbol: str,
    start_date: str,
    date_cutoff: str,
) -> DailyBarSourceObservation:
    """Acquire one public adjusted-daily observation without persistence.

    Yahoo Finance is deliberately an independent cross-check rather than a
    fallback.  Any unavailable or malformed response remains a non-publishable
    source state for the caller to pass into multi-source assurance.
    """

    normalized_symbol = str(symbol or "").strip().upper()
    try:
        exclusive_end = (date.fromisoformat(date_cutoff) + timedelta(days=1)).isoformat()
        date.fromisoformat(start_date)
        frame = download_yahoo_chart_price_history(
            [normalized_symbol],
            start=start_date,
            end=exclusive_end,
            price_field=PRICE_FIELD_ADJUSTED_CLOSE,
        )
        bars = _normalize_daily_bars(
            frame.to_dict(orient="records"),
            symbol=normalized_symbol,
            start_date=start_date,
            date_cutoff=date_cutoff,
        )
        source_artifact_sha256 = _canonical_sha256(
            {
                "source_id": YAHOO_FINANCE_DAILY_SOURCE_ID,
                "symbol": normalized_symbol,
                "start_date": start_date,
                "date_cutoff": date_cutoff,
                "adjustment_basis": YAHOO_FINANCE_ADJUSTMENT_BASIS,
                "bars": [bar.to_dict() for bar in bars],
            }
        )
        snapshot = DailyBarSourceSnapshot(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            symbol=normalized_symbol,
            date_cutoff=date_cutoff,
            adjustment_basis=YAHOO_FINANCE_ADJUSTMENT_BASIS,
            source_artifact_sha256=source_artifact_sha256,
            bars=bars,
        )
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_READY,
            snapshot=snapshot,
        )
    except YahooFinanceUnavailableError as exc:
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_UNAVAILABLE,
            reason_codes=(exc.reason_code,),
        )
    except HTTPError as exc:
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_UNAVAILABLE,
            reason_codes=(_reason_for_http_status(exc.code),),
        )
    except (URLError, TimeoutError, OSError):
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_UNAVAILABLE,
            reason_codes=(YAHOO_FINANCE_TRANSPORT_UNAVAILABLE,),
        )
    except (TypeError, ValueError, KeyError, YahooFinancePayloadError):
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_INVALID,
            reason_codes=(YAHOO_FINANCE_PAYLOAD_INVALID,),
        )
    except RuntimeError:
        return DailyBarSourceObservation(
            source_id=YAHOO_FINANCE_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_UNAVAILABLE,
            reason_codes=(YAHOO_FINANCE_REQUEST_REJECTED,),
        )


def _normalize_daily_bars(
    rows: object,
    *,
    symbol: str,
    start_date: str,
    date_cutoff: str,
) -> tuple[DailyBar, ...]:
    try:
        if not isinstance(rows, list) or not rows:
            raise TypeError
        bars = tuple(
            DailyBar(
                session_date=_session_date(raw["as_of"]),
                open=raw["open"],
                high=raw["high"],
                low=raw["low"],
                close=raw["close"],
                volume=raw["volume"],
            )
            for raw in rows
            if isinstance(raw, dict) and str(raw.get("symbol") or "").upper() == symbol
        )
        if len(bars) != len(rows) or not bars:
            raise ValueError
        ordered = tuple(sorted(bars, key=lambda bar: bar.session_date))
        if ordered[0].session_date < start_date or ordered[-1].session_date > date_cutoff:
            raise ValueError
        return ordered
    except (KeyError, TypeError, ValueError):
        raise YahooFinancePayloadError from None


def _session_date(value: object) -> str:
    if hasattr(value, "date"):
        return value.date().isoformat()
    return date.fromisoformat(str(value)[:10]).isoformat()


__all__ = [
    "YAHOO_FINANCE_ADJUSTMENT_BASIS",
    "YAHOO_FINANCE_DAILY_SOURCE_ID",
    "YAHOO_FINANCE_PAYLOAD_INVALID",
    "YAHOO_FINANCE_RATE_LIMITED",
    "YAHOO_FINANCE_REQUEST_REJECTED",
    "YAHOO_FINANCE_SERVICE_UNAVAILABLE",
    "YAHOO_FINANCE_TRANSPORT_UNAVAILABLE",
    "YahooFinancePayloadError",
    "YahooFinanceUnavailableError",
    "observe_yahoo_finance_adjusted_daily_bars",
]
