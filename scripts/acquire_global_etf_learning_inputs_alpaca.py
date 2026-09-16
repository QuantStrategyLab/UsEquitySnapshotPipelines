"""Bounded, memory-only Alpaca input adapter for the Global ETF learning window.

The CLI is deliberately plan-only.  A separately authorized non-live runner may
inject a plain ``get_transport(url, params)`` callable into
``collect_global_etf_learning_inputs``; this module never reads credentials or
writes provider responses, bars, or temporary files.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from collections.abc import Mapping
from datetime import date, datetime
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

_ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
SYMBOLS = (
    "EWY",
    "EWT",
    "INDA",
    "FXI",
    "EWJ",
    "VGK",
    "VOO",
    "XLK",
    "SMH",
    "GLD",
    "SLV",
    "USO",
    "DBA",
    "XLE",
    "XLF",
    "ITA",
    "XLP",
    "XLU",
    "XLV",
    "IHI",
    "VNQ",
    "KRE",
    "SPY",
    "EFA",
    "EEM",
    "AGG",
    "BIL",
)
START_DATE = "2016-01-01"
END_DATE = "2024-12-31"
WARMUP_START = "2016-01-01"
LEARNING_START = "2017-01-01"
_REQUEST_START = "2016-01-01T00:00:00-05:00"
_REQUEST_END = "2025-01-01T00:00:00-05:00"
_TIMEZONE = ZoneInfo("America/New_York")
_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_GCS_BUCKET = "qsl-runtime-logs-shared"
_GCS_OBJECT = "global-etf-learning/2017-2024/20260916/bars.json"
_GITHUB_ACTIONS = "GITHUB_ACTIONS"
_GITHUB_WORKFLOW = "GITHUB_WORKFLOW"
_REQUEST_PARAMS = {
    "timeframe": "1Day",
    "start": _REQUEST_START,
    "end": _REQUEST_END,
    "asof": END_DATE,
    "feed": "sip",
    "adjustment": "all",
    "currency": "USD",
    "sort": "asc",
    "limit": "10000",
}


class GlobalEtfLearningInputError(ValueError):
    """A sanitized, fail-closed input or provider error."""


class GetTransport(Protocol):
    def __call__(self, url: str, params: Mapping[str, str]) -> Mapping[str, object]: ...


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise GlobalEtfLearningInputError("redirect rejected")


class AlpacaHttpsTransport:
    """One-shot fixed-host HTTPS transport with no proxy, redirect, or retry."""

    def __init__(self, api_key_id: str, api_secret_key: str, *, opener=None) -> None:
        if not isinstance(api_key_id, str) or not api_key_id or not isinstance(api_secret_key, str) or not api_secret_key:
            raise GlobalEtfLearningInputError("credentials unavailable")
        self._headers = {"APCA-API-KEY-ID": api_key_id, "APCA-API-SECRET-KEY": api_secret_key}
        self._opener = opener or build_opener(ProxyHandler({}), _RejectRedirects())

    def __call__(self, url: str, params: Mapping[str, str]) -> Mapping[str, object]:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "data.alpaca.markets" or parsed.path != "/v2/stocks/bars":
            raise GlobalEtfLearningInputError("provider request rejected")
        if parsed.params or parsed.query or parsed.fragment:
            raise GlobalEtfLearningInputError("provider request rejected")
        if set(params) != set(_REQUEST_PARAMS) | {"symbols"} or params.get("symbols") not in SYMBOLS:
            raise GlobalEtfLearningInputError("provider request rejected")
        if dict(params) != _request_params(str(params["symbols"])):
            raise GlobalEtfLearningInputError("provider request rejected")
        request = Request(f"{url}?{urlencode(params)}", headers=self._headers, method="GET")
        try:
            with self._opener.open(request, timeout=30) as response:
                if response.status != 200:
                    raise GlobalEtfLearningInputError("provider request failed")
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) > _MAX_RESPONSE_BYTES:
                    raise GlobalEtfLearningInputError("provider response too large")
                body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(body) > _MAX_RESPONSE_BYTES:
                    raise GlobalEtfLearningInputError("provider response too large")
            payload = json.loads(body)
        except GlobalEtfLearningInputError:
            raise
        except (HTTPError, URLError, OSError, TypeError, ValueError, json.JSONDecodeError):
            raise GlobalEtfLearningInputError("provider request failed") from None
        if not isinstance(payload, Mapping):
            raise GlobalEtfLearningInputError("invalid provider response")
        return payload


def _expected_xnys_sessions() -> tuple[date, ...]:
    """Return the fixed XNYS sessions without extending the calendar bounds."""
    try:
        import exchange_calendars as xcals

        calendar = xcals.get_calendar("XNYS", start=START_DATE, end=END_DATE)
        sessions = calendar.sessions
        return tuple(item.date() for item in sessions)
    except Exception:  # noqa: BLE001 - calendar details must not escape the boundary
        raise GlobalEtfLearningInputError("calendar unavailable") from None


def _session_from_timestamp(value: object) -> date:
    if not isinstance(value, str):
        raise GlobalEtfLearningInputError("invalid bar timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(_TIMEZONE).date()
    except (TypeError, ValueError, OverflowError):
        raise GlobalEtfLearningInputError("invalid bar timestamp") from None


def _finite_number(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise GlobalEtfLearningInputError("invalid bar value")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise GlobalEtfLearningInputError("invalid bar value") from None
    if not math.isfinite(number):
        raise GlobalEtfLearningInputError("invalid bar value")
    if positive and number <= 0:
        raise GlobalEtfLearningInputError("invalid bar value")
    if nonnegative and number < 0:
        raise GlobalEtfLearningInputError("invalid bar value")
    return number


def _normalize_symbol_bars(
    payload: Mapping[str, object], *, symbol: str, expected_sessions: tuple[date, ...]
) -> list[dict[str, object]]:
    if payload.get("next_page_token") not in (None, ""):
        raise GlobalEtfLearningInputError("pagination is not allowed")
    bars = payload.get("bars")
    if not isinstance(bars, Mapping) or set(bars) != {symbol} or not isinstance(bars.get(symbol), list):
        raise GlobalEtfLearningInputError("bars symbols do not exactly match request")

    normalized: list[dict[str, object]] = []
    seen: set[date] = set()
    for raw in bars[symbol]:
        if not isinstance(raw, Mapping):
            raise GlobalEtfLearningInputError("invalid bar")
        session = _session_from_timestamp(raw.get("t"))
        if session in seen:
            raise GlobalEtfLearningInputError("duplicate bar")
        seen.add(session)
        open_price = _finite_number(raw.get("o"), positive=True)
        high_price = _finite_number(raw.get("h"), positive=True)
        low_price = _finite_number(raw.get("l"), positive=True)
        close_price = _finite_number(raw.get("c"), positive=True)
        volume = _finite_number(raw.get("v"), nonnegative=True)
        if low_price > min(open_price, high_price, close_price) or high_price < max(open_price, low_price, close_price):
            raise GlobalEtfLearningInputError("invalid bar range")
        normalized.append(
            {
                "date": session.isoformat(),
                "symbol": symbol,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }
        )

    observed = tuple(item["date"] for item in normalized)
    expected = tuple(item.isoformat() for item in expected_sessions)
    if observed != tuple(sorted(observed)):
        raise GlobalEtfLearningInputError("bars are not sorted")
    if observed != expected:
        raise GlobalEtfLearningInputError("bars do not cover the fixed XNYS sessions")
    return normalized


def _request_params(symbol: str) -> dict[str, str]:
    params = dict(_REQUEST_PARAMS)
    params["symbols"] = symbol
    return params


def collect_global_etf_learning_inputs(get_transport: GetTransport) -> dict[str, object]:
    """Collect and validate the fixed learning window through an injected port."""
    if not callable(get_transport):
        raise GlobalEtfLearningInputError("transport unavailable")
    expected_sessions = _expected_xnys_sessions()
    if not expected_sessions:
        raise GlobalEtfLearningInputError("calendar unavailable")

    rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        params = _request_params(symbol)
        try:
            payload = get_transport(_ALPACA_BARS_URL, params)
        except Exception:  # noqa: BLE001 - provider details and credentials must not escape
            raise GlobalEtfLearningInputError("provider request failed") from None
        if not isinstance(payload, Mapping):
            raise GlobalEtfLearningInputError("invalid provider response")
        rows.extend(_normalize_symbol_bars(payload, symbol=symbol, expected_sessions=expected_sessions))

    return {
        "schema_version": "global_etf_learning_inputs.v1",
        "bars": rows,
        "summary": {
            "provider": "Alpaca",
            "feed": "sip",
            "adjustment": "all",
            "asof": END_DATE,
            "currency": "USD",
            "symbol_count": len(SYMBOLS),
            "bar_count": len(rows),
            "first_session": expected_sessions[0].isoformat(),
            "last_session": expected_sessions[-1].isoformat(),
            "pit_verified": False,
        },
        "controls": {"no_order": True, "promotion_eligible": False, "live_ready": False},
    }


def _require_cloud_environment(environ: Mapping[str, str]) -> None:
    if environ.get(_GITHUB_ACTIONS) != "true" or not environ.get(_GITHUB_WORKFLOW):
        raise GlobalEtfLearningInputError("cloud execution unavailable")


def _cloud_target_preflight(storage_client: object) -> object:
    try:
        bucket = storage_client.bucket(_GCS_BUCKET)
        target = bucket.blob(_GCS_OBJECT)
        if target.exists(retry=None, timeout=30):
            raise GlobalEtfLearningInputError("target object already exists")
        return target
    except GlobalEtfLearningInputError:
        raise
    except Exception:  # noqa: BLE001 - GCS details must not escape
        raise GlobalEtfLearningInputError("bucket preflight unavailable") from None


def execute_global_etf_learning_inputs_cloud(
    *,
    storage_client: object,
    http_transport: GetTransport | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Run one explicit GitHub Actions collection and create one GCS object."""
    environment = os.environ if environ is None else environ
    _require_cloud_environment(environment)
    key_id = environment.get("ALPACA_API_KEY_ID")
    secret = environment.get("ALPACA_API_SECRET_KEY")
    if not key_id or not secret:
        raise GlobalEtfLearningInputError("credentials unavailable")
    target = _cloud_target_preflight(storage_client)
    transport = http_transport or AlpacaHttpsTransport(key_id, secret)
    result = collect_global_etf_learning_inputs(transport)
    result["learning_only"] = True
    result["size_zero_required"] = True
    result["storage"] = "gcs_single_object"
    result["gcs_bucket"] = _GCS_BUCKET
    result["gcs_object"] = _GCS_OBJECT
    payload = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    try:
        target.upload_from_string(
            payload,
            content_type="application/json",
            if_generation_match=0,
            retry=None,
            timeout=30,
        )
    except Exception:  # noqa: BLE001 - upload details must not escape or retry
        raise GlobalEtfLearningInputError("object upload unknown") from None
    return {
        "status": "COLLECTED_AND_UPLOADED",
        "summary": result["summary"],
        "controls": result["controls"],
        "learning_only": True,
        "size_zero_required": True,
        "pit_verified": False,
        "storage": "gcs_single_object",
    }


def _plan() -> dict[str, object]:
    return {
        "status": "PLAN_ONLY",
        "symbols": list(SYMBOLS),
        "start": START_DATE,
        "end": END_DATE,
        "warmup_start": WARMUP_START,
        "learning_start": LEARNING_START,
        "request_count": len(SYMBOLS),
        "request": {"url": _ALPACA_BARS_URL, "params": {**_REQUEST_PARAMS, "symbols": "<one fixed symbol per request>"}},
        "pit_verified": False,
        "controls": {"no_order": True, "promotion_eligible": False, "live_ready": False},
        "persistence": "memory_only",
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Print the fixed Global ETF learning input plan.")
    parser.add_argument("--execute-cloud", action="store_true", help="run only inside the approved GitHub Actions workflow")
    args = parser.parse_args(argv)
    if not args.execute_cloud:
        print(json.dumps(_plan(), sort_keys=True, separators=(",", ":")))
        return
    try:
        _require_cloud_environment(os.environ)
        from google.cloud import storage

        result = execute_global_etf_learning_inputs_cloud(storage_client=storage.Client())
    except GlobalEtfLearningInputError as exc:
        raise SystemExit(str(exc)) from None
    except Exception:
        raise SystemExit("cloud execution unavailable") from None
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
