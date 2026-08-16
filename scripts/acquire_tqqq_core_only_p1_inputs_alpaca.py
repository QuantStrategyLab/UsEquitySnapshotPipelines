"""Data-only publisher for injected TQQQ core-only Alpaca SIP bars transport."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    CANDIDATE_ID,
    TqqqCoreOnlyHistoricalBarsProvider,
    TqqqCoreOnlyP1BindingError,
    publish_tqqq_core_only_p1_inputs as _publish,
)

_ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
_START_DATES = {
    "QQQ": "2018-01-02",
    "TQQQ": "2018-01-02",
    "QQQM": "2020-10-13",
    "BOXX": "2022-12-28",
}
_DATE_CUTOFF = "2026-07-31"


class AlpacaBarsTransport(Protocol):
    """Injected transport boundary; session construction stays outside this module."""

    def __call__(self, *, url: str, params: Mapping[str, str]) -> Mapping[str, object]: ...


class AlpacaSipHttpTransport:
    """One-shot HTTPS transport that confines Alpaca keys to request headers."""

    def __init__(self, api_key_id: str, api_secret_key: str) -> None:
        if not isinstance(api_key_id, str) or not api_key_id or not isinstance(api_secret_key, str) or not api_secret_key:
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
        self._headers = {
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
        }

    def __call__(self, *, url: str, params: Mapping[str, str]) -> Mapping[str, object]:
        if url != _ALPACA_BARS_URL:
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
        request = Request(f"{url}?{urlencode(params)}", headers=self._headers, method="GET")
        try:
            with urlopen(request, timeout=60) as response:  # noqa: S310 - fixed HTTPS Alpaca endpoint
                if response.status != 200:
                    raise ValueError
                payload = json.loads(response.read())
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None
        if not isinstance(payload, Mapping):
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
        return payload


class AlpacaSipHistoricalBarsProvider:
    """Concrete one-request-per-symbol Alpaca SIP adapter."""

    def __init__(self, transport: AlpacaBarsTransport) -> None:
        self._transport = transport

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> dict[str, object]:
        if (
            symbol not in _START_DATES
            or calendar_id != "XNYS"
            or timezone != "America/New_York"
            or adjustment_policy != "total_return_adjusted"
            or feed != "SIP"
            or date_cutoff != _DATE_CUTOFF
        ):
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
        try:
            response = self._transport(
                url=_ALPACA_BARS_URL,
                params={
                    "symbols": symbol,
                    "timeframe": "1Day",
                    "start": _START_DATES[symbol],
                    "end": _DATE_CUTOFF,
                    "adjustment": "all",
                    "feed": "sip",
                    "sort": "asc",
                    "limit": "10000",
                },
            )
            return {"bars": _normalize_bars(response, symbol)}
        except TqqqCoreOnlyP1BindingError:
            raise
        except Exception:
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None


def _normalize_bars(response: Mapping[str, object], symbol: str) -> list[dict[str, object]]:
    try:
        bars_by_symbol = response["bars"]
        if not isinstance(bars_by_symbol, Mapping):
            raise TypeError
        raw_bars = bars_by_symbol[symbol]
        if not isinstance(raw_bars, list):
            raise TypeError
        normalized: list[dict[str, object]] = []
        for raw in raw_bars:
            if not isinstance(raw, Mapping):
                raise TypeError
            session = date.fromisoformat(str(raw["t"])[:10])
            bar: dict[str, object] = {"date": session.isoformat()}
            for source_field, target_field in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")):
                value = float(raw[source_field])
                if not math.isfinite(value):
                    raise ValueError
                bar[target_field] = value
            normalized.append(bar)
        return normalized
    except (KeyError, TypeError, ValueError):
        raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None


def publish_tqqq_core_only_p1_inputs(
    provider: TqqqCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Publish one immutable four-symbol root through the injected Alpaca provider."""
    return _publish(provider, output_root=output_root, observed_at=observed_at, producer=producer)


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish injected TQQQ core-only P1 Alpaca SIP inputs.")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    provider: TqqqCoreOnlyHistoricalBarsProvider | None = None,
    producer: Mapping[str, object] | None = None,
) -> int:
    args = _arguments(argv)
    if provider is None or producer is None:
        print('{"status":"PARKED"}')
        return 2
    try:
        print(json.dumps(publish_tqqq_core_only_p1_inputs(provider, output_root=args.output_root, observed_at=args.observed_at, producer=producer), sort_keys=True))
        return 0
    except TqqqCoreOnlyP1BindingError:
        print(json.dumps({"candidate_id": CANDIDATE_ID, "status": "PARKED"}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
