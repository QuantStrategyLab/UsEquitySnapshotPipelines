"""Data-only publisher adapter for injected SOXL/SOXX Alpaca SIP daily bars.

The executable intentionally does not construct credentials from the local
environment.  A future non-live runner must inject a provider and producer
after it has separately established its scope.  With no injected provider,
this entry point reports a sanitized PARKED result and performs no I/O.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from time import sleep
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import SoxlCoreOnlyP1BindingError
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_publisher import (
    SoxlCoreOnlyHistoricalBarsProvider,
    SoxlCoreOnlyP1InputUnavailableError,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_publisher import (
    publish_soxl_core_only_p1_inputs as _publish,
)

_ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
_START_DATES = {"SOXL": "2022-01-03", "SOXX": "2022-01-03", "BOXX": "2022-12-28"}
_SIP_FORBIDDEN_MAX_ATTEMPTS = 2
_SIP_FORBIDDEN_RETRY_DELAY_SECONDS = 60
_PROVIDER_RETRY_NOT_TRIGGERED = "NOT_TRIGGERED"
_PROVIDER_RETRY_RECOVERED = "SIP_403_RECOVERED"
_PROVIDER_RETRY_EXHAUSTED = "SIP_403_EXHAUSTED"
_AVAILABILITY_REASON_CODES = frozenset(
    {
        "INPUT_UNAVAILABLE",
        "ALPACA_AUTHENTICATION_FAILED",
        "ALPACA_SIP_ACCESS_FORBIDDEN",
        "ALPACA_RATE_LIMITED",
        "ALPACA_SERVICE_UNAVAILABLE",
        "ALPACA_TRANSPORT_UNAVAILABLE",
        "ALPACA_REQUEST_REJECTED",
    }
)


class P1InputUnavailableError(SoxlCoreOnlyP1InputUnavailableError):
    """A fixed-provider availability outcome, never a strategy conclusion."""

    def __init__(self, reason_code: object = "INPUT_UNAVAILABLE") -> None:
        code = (
            reason_code
            if isinstance(reason_code, str) and reason_code in _AVAILABILITY_REASON_CODES
            else "INPUT_UNAVAILABLE"
        )
        self.reason_code = code
        super().__init__(code)


def _availability_reason_for_http_status(status: object) -> str:
    if status == 401:
        return "ALPACA_AUTHENTICATION_FAILED"
    if status == 403:
        return "ALPACA_SIP_ACCESS_FORBIDDEN"
    if status == 429:
        return "ALPACA_RATE_LIMITED"
    if isinstance(status, int) and 500 <= status <= 599:
        return "ALPACA_SERVICE_UNAVAILABLE"
    return "ALPACA_REQUEST_REJECTED"


class AlpacaBarsTransport(Protocol):
    """Injected HTTPS boundary; request setup remains outside the P1 lifecycle."""

    def __call__(self, *, url: str, params: Mapping[str, str]) -> Mapping[str, object]: ...


class AlpacaSipHttpTransport:
    """Fixed-request HTTPS transport that confines Alpaca keys to fixed request headers."""

    def __init__(
        self,
        api_key_id: str,
        api_secret_key: str,
        *,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        if not isinstance(api_key_id, str) or not api_key_id or not isinstance(api_secret_key, str) or not api_secret_key:
            raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")
        self._headers = {"APCA-API-KEY-ID": api_key_id, "APCA-API-SECRET-KEY": api_secret_key}
        self._sleep_fn = sleep_fn
        self._forbidden_retry_count = 0
        self._forbidden_retry_exhausted = False

    @property
    def provider_retry_state(self) -> str:
        """Return a sanitized summary of this P1 transport's bounded 403 path."""
        if self._forbidden_retry_exhausted:
            return _PROVIDER_RETRY_EXHAUSTED
        if self._forbidden_retry_count:
            return _PROVIDER_RETRY_RECOVERED
        return _PROVIDER_RETRY_NOT_TRIGGERED

    def __call__(self, *, url: str, params: Mapping[str, str]) -> Mapping[str, object]:
        if url != _ALPACA_BARS_URL:
            raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")
        request = Request(f"{url}?{urlencode(params)}", headers=self._headers, method="GET")
        for attempt_index in range(_SIP_FORBIDDEN_MAX_ATTEMPTS):
            try:
                with urlopen(request, timeout=60) as response:
                    status = response.status
                    payload = json.loads(response.read()) if status == 200 else None
                if status != 200:
                    if status == 403 and attempt_index == 0:
                        self._forbidden_retry_count += 1
                        self._sleep_fn(_SIP_FORBIDDEN_RETRY_DELAY_SECONDS)
                        continue
                    if status == 403:
                        self._forbidden_retry_exhausted = True
                    raise P1InputUnavailableError(_availability_reason_for_http_status(status))
            except P1InputUnavailableError:
                raise
            except HTTPError as exc:
                if exc.code == 403 and attempt_index == 0:
                    self._forbidden_retry_count += 1
                    self._sleep_fn(_SIP_FORBIDDEN_RETRY_DELAY_SECONDS)
                    continue
                if exc.code == 403:
                    self._forbidden_retry_exhausted = True
                raise P1InputUnavailableError(_availability_reason_for_http_status(exc.code)) from None
            except (URLError, TimeoutError, OSError):
                raise P1InputUnavailableError("ALPACA_TRANSPORT_UNAVAILABLE") from None
            except (TypeError, ValueError, json.JSONDecodeError):
                raise SoxlCoreOnlyP1BindingError("data-only acquisition failed") from None
            if not isinstance(payload, Mapping):
                raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")
            return payload
        raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")


class AlpacaSipHistoricalBarsProvider:
    """Concrete one-request-per-symbol Alpaca SIP adapter for the frozen universe."""

    def __init__(self, transport: AlpacaBarsTransport, *, date_cutoff: str) -> None:
        if not isinstance(date_cutoff, str):
            raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")
        self._transport = transport
        self._date_cutoff = date_cutoff

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
            or date_cutoff != self._date_cutoff
        ):
            raise SoxlCoreOnlyP1BindingError("data-only acquisition failed")
        try:
            response = self._transport(
                url=_ALPACA_BARS_URL,
                params={
                    "symbols": symbol,
                    "timeframe": "1Day",
                    "start": _START_DATES[symbol],
                    "end": self._date_cutoff,
                    "adjustment": "all",
                    "feed": "sip",
                    "sort": "asc",
                    "limit": "10000",
                },
            )
            return {"bars": _normalize_bars(response, symbol)}
        except SoxlCoreOnlyP1BindingError:
            raise
        except Exception:  # noqa: BLE001 - transport availability must not leak provider details
            raise P1InputUnavailableError("INPUT_UNAVAILABLE") from None


def _normalize_bars(response: Mapping[str, object], symbol: str) -> list[dict[str, object]]:
    try:
        bars_by_symbol = response["bars"]
        if not isinstance(bars_by_symbol, Mapping) or not isinstance(bars_by_symbol[symbol], list):
            raise TypeError
        normalized: list[dict[str, object]] = []
        for raw in bars_by_symbol[symbol]:
            if not isinstance(raw, Mapping):
                raise TypeError
            session = date.fromisoformat(str(raw["t"])[:10])
            bar: dict[str, object] = {"date": session.isoformat()}
            for source_field, target_field in (
                ("o", "open"),
                ("h", "high"),
                ("l", "low"),
                ("c", "close"),
                ("v", "volume"),
            ):
                value = float(raw[source_field])
                if not math.isfinite(value):
                    raise ValueError
                bar[target_field] = value
            normalized.append(bar)
        return normalized
    except (KeyError, TypeError, ValueError):
        raise SoxlCoreOnlyP1BindingError("data-only acquisition failed") from None


def publish_soxl_core_only_p1_inputs(
    provider: SoxlCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    date_cutoff: str,
) -> dict[str, object]:
    """Publish the frozen three-symbol root through the injected Alpaca port."""
    return _publish(
        provider,
        output_root=output_root,
        observed_at=observed_at,
        producer=producer,
        date_cutoff=date_cutoff,
    )


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish injected SOXL core-only P1 Alpaca SIP inputs.")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--date-cutoff", required=True)
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    provider: SoxlCoreOnlyHistoricalBarsProvider | None = None,
    producer: Mapping[str, object] | None = None,
) -> int:
    args = _arguments(argv)
    if provider is None or producer is None:
        print(json.dumps({"status": "PARKED"}, sort_keys=True))
        return 2
    try:
        print(
            json.dumps(
                publish_soxl_core_only_p1_inputs(
                    provider,
                    output_root=args.output_root,
                    observed_at=args.observed_at,
                    producer=producer,
                    date_cutoff=args.date_cutoff,
                ),
                sort_keys=True,
            )
        )
        return 0
    except SoxlCoreOnlyP1InputUnavailableError:
        print(json.dumps({"reason": "INPUT_UNAVAILABLE", "status": "PARKED", "verdict": "INCONCLUSIVE"}, sort_keys=True))
        return 2
    except SoxlCoreOnlyP1BindingError:
        print(json.dumps({"status": "PARKED"}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
