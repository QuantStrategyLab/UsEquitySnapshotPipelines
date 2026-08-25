"""Non-live Twelve Data adjusted-daily source adapter.

This adapter is deliberately limited to source observation.  It never writes a
P1 root, chooses a fallback, changes a strategy, or accesses a broker.  A
caller must pass its result to the shared multi-source assurance gate before
any research input can become canonical.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, timedelta
from hashlib import sha256
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_INVALID,
    SOURCE_OBSERVATION_READY,
    SOURCE_OBSERVATION_UNAVAILABLE,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

TWELVE_DATA_DAILY_SOURCE_ID = "twelve_data_1day_split_adjusted"
TWELVE_DATA_ADJUSTMENT_BASIS = "split_adjusted"
TWELVE_DATA_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"
TWELVE_DATA_TIMEOUT_SECONDS = 30

TWELVE_DATA_SECRET_NOT_CONFIGURED = "TWELVE_DATA_SECRET_NOT_CONFIGURED"
TWELVE_DATA_AUTH_OR_ENTITLEMENT = "TWELVE_DATA_AUTH_OR_ENTITLEMENT"
TWELVE_DATA_RATE_LIMITED = "TWELVE_DATA_RATE_LIMITED"
TWELVE_DATA_SERVICE_UNAVAILABLE = "TWELVE_DATA_SERVICE_UNAVAILABLE"
TWELVE_DATA_TRANSPORT_UNAVAILABLE = "TWELVE_DATA_TRANSPORT_UNAVAILABLE"
TWELVE_DATA_REQUEST_REJECTED = "TWELVE_DATA_REQUEST_REJECTED"
TWELVE_DATA_PAYLOAD_INVALID = "TWELVE_DATA_PAYLOAD_INVALID"


class TwelveDataUnavailableError(RuntimeError):
    """Safe terminal state for an unavailable provider, without response text."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class TwelveDataPayloadError(RuntimeError):
    """The provider responded but its daily series cannot be safely normalized."""


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
    if status in {401, 403}:
        return TWELVE_DATA_AUTH_OR_ENTITLEMENT
    if status == 429:
        return TWELVE_DATA_RATE_LIMITED
    if 500 <= status <= 599:
        return TWELVE_DATA_SERVICE_UNAVAILABLE
    return TWELVE_DATA_REQUEST_REJECTED


class TwelveDataAdjustedDailyClient:
    """Minimal authenticated client for the fixed adjusted-daily endpoint."""

    def __init__(self, api_key: str) -> None:
        if not isinstance(api_key, str) or not api_key.strip():
            raise TwelveDataUnavailableError(TWELVE_DATA_SECRET_NOT_CONFIGURED)
        self._authorization = f"apikey {api_key.strip()}"

    def fetch_daily_bars(
        self,
        *,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> tuple[DailyBar, ...]:
        try:
            exclusive_end = (date.fromisoformat(date_cutoff) + timedelta(days=1)).isoformat()
        except (TypeError, ValueError) as exc:
            raise TwelveDataPayloadError from exc
        query = urlencode(
            {
                "symbol": symbol,
                "interval": "1day",
                "start_date": start_date,
                # The endpoint uses an exclusive end boundary.  The
                # normalizer below still rejects any returned row after the
                # requested completed-session cutoff.
                "end_date": exclusive_end,
                "adjust": "splits",
            }
        )
        request = Request(
            f"{TWELVE_DATA_TIME_SERIES_URL}?{query}",
            headers={
                "Authorization": self._authorization,
                "User-Agent": "us-equity-snapshot-pipelines/1.0",
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=TWELVE_DATA_TIMEOUT_SECONDS) as response:
                if response.status != 200:
                    raise TwelveDataUnavailableError(_reason_for_http_status(response.status))
                payload = json.loads(response.read())
        except TwelveDataUnavailableError:
            raise
        except HTTPError as exc:
            raise TwelveDataUnavailableError(_reason_for_http_status(exc.code)) from None
        except (URLError, TimeoutError, OSError):
            raise TwelveDataUnavailableError(TWELVE_DATA_TRANSPORT_UNAVAILABLE) from None
        except (TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            raise TwelveDataPayloadError from None
        return _normalize_daily_bars(payload, symbol=symbol, date_cutoff=date_cutoff)


def observe_twelve_data_adjusted_daily_bars(
    *,
    api_key: str | None,
    symbol: str,
    start_date: str,
    date_cutoff: str,
) -> DailyBarSourceObservation:
    """Acquire one independent source observation with no persistence side effect."""

    normalized_symbol = str(symbol or "").strip().upper()
    try:
        client = TwelveDataAdjustedDailyClient(str(api_key or ""))
        bars = client.fetch_daily_bars(
            symbol=normalized_symbol,
            start_date=start_date,
            date_cutoff=date_cutoff,
        )
        source_artifact_sha256 = _canonical_sha256(
            {
                "source_id": TWELVE_DATA_DAILY_SOURCE_ID,
                "symbol": normalized_symbol,
                "start_date": start_date,
                "date_cutoff": date_cutoff,
                "adjustment_basis": TWELVE_DATA_ADJUSTMENT_BASIS,
                "bars": [bar.to_dict() for bar in bars],
            }
        )
        snapshot = DailyBarSourceSnapshot(
            source_id=TWELVE_DATA_DAILY_SOURCE_ID,
            symbol=normalized_symbol,
            date_cutoff=date_cutoff,
            adjustment_basis=TWELVE_DATA_ADJUSTMENT_BASIS,
            source_artifact_sha256=source_artifact_sha256,
            bars=bars,
        )
        return DailyBarSourceObservation(
            source_id=TWELVE_DATA_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_READY,
            snapshot=snapshot,
        )
    except TwelveDataUnavailableError as exc:
        return DailyBarSourceObservation(
            source_id=TWELVE_DATA_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_UNAVAILABLE,
            reason_codes=(exc.reason_code,),
        )
    except (TwelveDataPayloadError, ValueError):
        return DailyBarSourceObservation(
            source_id=TWELVE_DATA_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_INVALID,
            reason_codes=(TWELVE_DATA_PAYLOAD_INVALID,),
        )


def _normalize_daily_bars(
    payload: object,
    *,
    symbol: str,
    date_cutoff: str,
) -> tuple[DailyBar, ...]:
    try:
        if not isinstance(payload, Mapping) or not isinstance(payload.get("values"), list):
            raise TypeError
        meta = payload.get("meta")
        if not isinstance(meta, Mapping) or str(meta.get("symbol") or "").upper() != symbol:
            raise ValueError
        bars = tuple(
            DailyBar(
                session_date=str(raw["datetime"]),
                open=raw["open"],
                high=raw["high"],
                low=raw["low"],
                close=raw["close"],
                volume=raw["volume"],
            )
            for raw in payload["values"]
            if isinstance(raw, Mapping)
        )
        if len(bars) != len(payload["values"]) or not bars:
            raise ValueError
        ordered = tuple(sorted(bars, key=lambda bar: bar.session_date))
        if ordered[-1].session_date > date_cutoff:
            raise ValueError
        return ordered
    except (KeyError, TypeError, ValueError):
        raise TwelveDataPayloadError from None


__all__ = [
    "TWELVE_DATA_ADJUSTMENT_BASIS",
    "TWELVE_DATA_AUTH_OR_ENTITLEMENT",
    "TWELVE_DATA_DAILY_SOURCE_ID",
    "TWELVE_DATA_PAYLOAD_INVALID",
    "TWELVE_DATA_RATE_LIMITED",
    "TWELVE_DATA_REQUEST_REJECTED",
    "TWELVE_DATA_SECRET_NOT_CONFIGURED",
    "TWELVE_DATA_SERVICE_UNAVAILABLE",
    "TWELVE_DATA_TIME_SERIES_URL",
    "TWELVE_DATA_TRANSPORT_UNAVAILABLE",
    "TwelveDataAdjustedDailyClient",
    "TwelveDataPayloadError",
    "TwelveDataUnavailableError",
    "observe_twelve_data_adjusted_daily_bars",
]
