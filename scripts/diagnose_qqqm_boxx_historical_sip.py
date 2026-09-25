#!/usr/bin/env python3
"""One-shot, non-live Alpaca historical SIP access diagnosis for QQQM/BOXX."""
from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

SYMBOLS = ("QQQM", "BOXX")
BASE_URL = "https://data.alpaca.markets/v2/stocks"
PARAMS = {
    "timeframe": "1Day",
    "start": "2024-06-03T00:00:00-04:00",
    "end": "2024-06-08T00:00:00-04:00",
    "feed": "sip",
    "adjustment": "raw",
    "currency": "USD",
    "sort": "asc",
    "limit": "5",
}
MAX_BODY_BYTES = 64 * 1024


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def _message(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value[:512]
    text = re.sub(r"https?://\S+", "[url]", text, flags=re.IGNORECASE)
    text = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[email]", text)
    text = re.sub(r"\b[A-Za-z0-9_-]{16,}\b", "[redacted]", text)
    text = re.sub(r"[^A-Za-z0-9 .,;:()/_\[\]-]", " ", text)
    return " ".join(text.split())[:160]


def _provider_error(body: bytes) -> tuple[int | str | None, str | None]:
    try:
        value = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None, None
    if not isinstance(value, dict):
        return None, None
    code = value.get("code")
    if not (isinstance(code, int) and 0 <= code <= 999999999999):
        code = None
    return code, _message(value.get("message"))


def diagnose(symbol: str, key_id: str, secret: str, *, opener=None) -> dict[str, object]:
    if symbol not in SYMBOLS:
        raise ValueError("SYMBOL_OUT_OF_SCOPE")
    if not key_id or not secret:
        return {"symbol": symbol, "status": "CREDENTIALS_NOT_CONFIGURED"}
    opener = opener or build_opener(ProxyHandler({}), _NoRedirect())
    url = f"{BASE_URL}/{symbol}/bars?{urlencode(PARAMS)}"
    request = Request(url, headers={"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret})
    try:
        with opener.open(request, timeout=30) as response:
            body = response.read(MAX_BODY_BYTES + 1)
            if len(body) > MAX_BODY_BYTES:
                return {"symbol": symbol, "http_status": response.status, "status": "RESPONSE_TOO_LARGE"}
            value = json.loads(body)
            bars = value.get("bars") if isinstance(value, dict) else None
            if not isinstance(bars, list) or len(bars) > 5:
                return {"symbol": symbol, "http_status": response.status, "status": "SUCCESS_FORMAT_INVALID"}
            times = []
            for bar in bars:
                stamp = bar.get("t") if isinstance(bar, dict) else None
                if not isinstance(stamp, str) or len(stamp) > 40:
                    return {"symbol": symbol, "http_status": response.status, "status": "SUCCESS_FORMAT_INVALID"}
                parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    return {"symbol": symbol, "http_status": response.status, "status": "SUCCESS_FORMAT_INVALID"}
                observed = parsed.astimezone(UTC)
                if not (datetime(2024, 6, 3, 4, tzinfo=UTC) <= observed < datetime(2024, 6, 8, 4, tzinfo=UTC)):
                    return {"symbol": symbol, "http_status": response.status, "status": "SUCCESS_FORMAT_INVALID"}
                times.append(observed.isoformat().replace("+00:00", "Z"))
            return {"symbol": symbol, "http_status": response.status,
                    "status": "ACCESS_OK" if bars else "EMPTY_SUCCESS",
                    "bar_count": len(bars), "first_bar_time": times[0] if times else None,
                    "last_bar_time": times[-1] if times else None}
    except HTTPError as exc:
        code, message = _provider_error(exc.read(MAX_BODY_BYTES))
        return {"symbol": symbol, "http_status": exc.code, "status": "PROVIDER_REJECTED",
                "provider_code": code, "provider_message": message}
    except (TimeoutError, URLError, OSError):
        return {"symbol": symbol, "status": "TRANSPORT_FAILED"}
    except (ValueError, UnicodeDecodeError):
        return {"symbol": symbol, "status": "SUCCESS_FORMAT_INVALID"}


def main() -> int:
    key_id = os.environ.get("ALPACA_API_KEY_ID", "")
    secret = os.environ.get("ALPACA_API_SECRET_KEY", "")
    results = [diagnose(symbol, key_id, secret) for symbol in SYMBOLS]
    payload = {
        "schema_version": "qsl.qqqm_boxx_sip_cause.v1",
        "observed_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "request": {"symbols": list(SYMBOLS), "timeframe": PARAMS["timeframe"],
                    "start": PARAMS["start"], "end_exclusive": PARAMS["end"],
                    "feed": PARAMS["feed"], "adjustment": PARAMS["adjustment"],
                    "currency": PARAMS["currency"], "limit_per_symbol": 5},
        "results": results,
        "request_count": sum(result["status"] != "CREDENTIALS_NOT_CONFIGURED" for result in results),
        "no_order": True,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0 if all(result["status"] == "ACCESS_OK" for result in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
