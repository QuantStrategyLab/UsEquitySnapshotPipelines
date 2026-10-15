"""Bounded non-live HTTP diagnostic; success bodies and auth never persist."""
from __future__ import annotations

from datetime import UTC, date, datetime, time
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from alpaca_feed_windows import feed_windows

STARTS = {"QQQ": "2018-01-02", "TQQQ": "2018-01-02", "QQQM": "2020-10-13",
          "SOXL": "2022-01-03", "SOXX": "2022-01-03", "BOXX": "2022-12-28"}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def safe_message(exc: HTTPError, sensitive: tuple[str, ...]) -> str:
    """Keep only the bounded provider message, never arbitrary error envelopes."""
    try:
        body = exc.read(4097)
        message = json.loads(body).get("message", "") if len(body) <= 4096 else ""
    except (OSError, ValueError, AttributeError):
        message = ""
    if not isinstance(message, str) or not message:
        return "PROVIDER_MESSAGE_UNAVAILABLE"
    for value in sensitive:
        if value:
            message = message.replace(value, "<redacted>")
    message = re.sub(r"https?://\S+|[\w.+-]+@[\w.-]+|\bU\d{7,}\b|\b\d{7,}\b|[A-Za-z0-9_+/=-]{24,}",
                     "<redacted>", message, flags=re.IGNORECASE)
    message = re.sub(r"(?i)(key|secret|token|password|authorization)\s*[:=]\s*\S+",
                     "<redacted>", message)
    return " ".join(message.split())[:256]


def diagnose(env, *, now: datetime, opener) -> dict:
    cutoff = env.get("DATE_CUTOFF", "")
    try:
        parsed = date.fromisoformat(cutoff)
        if parsed.isoformat() != cutoff:
            raise ValueError
    except ValueError:
        raise ValueError("DATE_CUTOFF must be an exact YYYY-MM-DD date") from None
    # A daily cutoff means the complete New York calendar day, capped at now.
    end = min(datetime.combine(parsed, time.max, ZoneInfo("America/New_York")), now)
    headers = {"APCA-API-KEY-ID": env.get("ALPACA_API_KEY_ID", ""),
               "APCA-API-SECRET-KEY": env.get("ALPACA_API_SECRET_KEY", "")}
    records = []
    stop = False
    for symbol, start in STARTS.items():
        windows = feed_windows(datetime.fromisoformat(start).replace(tzinfo=UTC),
                               end, now=now, env=env)
        for window in windows:
            record = {"symbol": symbol, **window, "http_status": None}
            if not all(headers.values()):
                record["status"] = "ALPACA_SECRET_NOT_CONFIGURED"
                stop = True
            else:
                params = {"symbols": symbol, "timeframe": "1Day", "adjustment": "all",
                          "sort": "asc", "limit": "1",
                          **{k: window[k] for k in ("start", "end", "feed")}}
                request = Request("https://data.alpaca.markets/v2/stocks/bars?" + urlencode(params),
                                  headers=headers)
                try:
                    with opener.open(request, timeout=30) as response:
                        record.update(status="ALPACA_ACCESS_OK", http_status=response.status)
                except HTTPError as exc:
                    record.update(status={401: "ALPACA_AUTHENTICATION_FAILED",
                                          403: "ALPACA_ACCESS_FORBIDDEN",
                                          429: "ALPACA_RATE_LIMITED"}.get(exc.code, "ALPACA_REQUEST_REJECTED"),
                                  http_status=exc.code,
                                  provider_message=safe_message(exc, tuple(headers.values())))
                    stop = True  # No retries or historical SIP -> IEX substitution on denial.
                except (TimeoutError, URLError, OSError):
                    record["status"] = "ALPACA_TRANSPORT_UNAVAILABLE"
                    stop = True
            records.append(record)
            if stop:
                break
        if stop:
            break
    return {"schema_version": "qsl.alpaca_access_diagnostic.v2", "date_cutoff": cutoff,
            "price_basis": "split_dividend_adjusted", "segments": records,
            "status": "ALPACA_ACCESS_OK" if records and not stop else "ALPACA_DIAGNOSTIC_STOPPED",
            "max_requests": 12, "no_retry": True, "no_order": True,
            "observed_at": now.isoformat()}


def main() -> int:
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_OS") != "Linux":
        raise SystemExit("CLOUD_ONLY")
    result = diagnose(os.environ, now=datetime.now(UTC),
                      opener=build_opener(ProxyHandler({}), NoRedirect()))
    Path("diagnostic.json").write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    # The provider message is confined to the diagnostic artifact.
    print("ALPACA_DIAGNOSTIC_STATUS=" + result["status"])
    return 0 if result["status"] == "ALPACA_ACCESS_OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
