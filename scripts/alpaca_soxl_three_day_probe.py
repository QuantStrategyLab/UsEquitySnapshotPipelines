"""One fixed read-only GET; persist schema, coverage and hashes, never market values."""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from http.client import HTTPException
import json
import math
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

URL = "https://data.alpaca.markets/v2/stocks/SOXL/bars"
DATES = ["2026-10-19", "2026-10-20", "2026-10-21"]
FIELDS = {"t", "o", "h", "l", "c", "v", "n", "vw"}
PARAMS = {
    "timeframe": "1Day", "start": "2026-10-19T00:00:00-04:00",
    "end": "2026-10-21T23:59:59-04:00", "feed": "sip",
    "adjustment": "raw", "asof": "2026-10-21", "currency": "USD",
    "sort": "asc", "limit": "10",
}
MAX_BYTES = 65536


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def validate(payload: object) -> dict:
    if not isinstance(payload, dict) or payload.get("symbol") != "SOXL":
        raise ValueError
    rows = payload.get("bars")
    if not isinstance(rows, list) or len(rows) > 3 or "next_page_token" not in payload:
        raise ValueError
    if payload["next_page_token"] is not None and not isinstance(payload["next_page_token"], str):
        raise ValueError
    dates = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != FIELDS:
            raise ValueError
        timestamp = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError
        local = timestamp.astimezone(ZoneInfo("America/New_York"))
        if any((local.hour, local.minute, local.second, local.microsecond)):
            raise ValueError
        session_date = local.date().isoformat()
        if session_date not in DATES:
            raise ValueError
        if not all(not isinstance(row[field], bool) and isinstance(row[field], (int, float))
                   and math.isfinite(row[field]) for field in FIELDS - {"t"}):
            raise ValueError
        if not all(row[field] > 0 for field in ("o", "h", "l", "c", "vw")):
            raise ValueError
        if not row["l"] <= min(row["o"], row["c"]) <= max(row["o"], row["c"]) <= row["h"]:
            raise ValueError
        if row["v"] < 0 or row["n"] < 0 or int(row["n"]) != row["n"]:
            raise ValueError
        dates.append(session_date)
    if dates != sorted(set(dates)):
        raise ValueError
    return {"schema_valid": True, "row_count": len(rows), "session_dates": dates,
            "fields": sorted(FIELDS), "missing_probe_dates": sorted(set(DATES) - set(dates)),
            "unexpected_pagination": bool(payload["next_page_token"])}


def probe(env, *, opener) -> dict:
    result = {
        "schema_version": "qsl.alpaca_soxl_three_day_probe.v1", "task": "ALPACA-SOXL-PROBE-01",
        "endpoint": URL, "request_params": PARAMS, "expected_probe_dates": DATES,
        "expected_fields": sorted(FIELDS), "max_requests": 1, "requests_attempted": 0,
        "automatic_retries": 0, "pagination_followup": False, "redirects_followed": False,
        "request_started_at": None, "response_received_at": None, "http_status": None,
        "response_bytes": None, "response_sha256": None, "schema_valid": None,
        "raw_persisted": False, "historical_pit_verified": False, "b4_admissible": False,
        "calendar_authority": "UNVERIFIED_PROBE_DATES_ONLY", "provider_revision": "UNKNOWN",
        "historical_first_seen": "UNKNOWN", "financial_conclusion": None,
        "real_b4_diagnostic_executed": False, "dsr_executed": False, "orders_submitted": 0,
        "status": "BLOCKED_EXECUTION_CONTEXT", "exit_code": 21,
    }
    expected_context = {"GITHUB_ACTIONS": "true", "RUNNER_OS": "Linux",
                        "GITHUB_REF": "refs/heads/main", "GITHUB_RUN_ATTEMPT": "1",
                        "DATE_CUTOFF": DATES[-1]}
    if any(env.get(name) != value for name, value in expected_context.items()):
        return result
    key = env.get("ALPACA_API_KEY_ID")
    secret = env.get("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        result.update(status="BLOCKED_CREDENTIAL_ENTRY_UNAVAILABLE", exit_code=20)
        return result
    request = Request(URL + "?" + urlencode(PARAMS), method="GET", headers={
        "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret})
    result.update(requests_attempted=1, request_started_at=datetime.now(UTC).isoformat(), exit_code=22)
    try:
        with opener.open(request, timeout=20) as response:
            result["http_status"] = response.status
            raw = response.read(MAX_BYTES + 1)
        result["response_received_at"] = datetime.now(UTC).isoformat()
        if len(raw) > MAX_BYTES:
            result.update(status="RESPONSE_TOO_LARGE", exit_code=24)
            return result
        result.update(response_bytes=len(raw), response_sha256=hashlib.sha256(raw).hexdigest())
        if result["http_status"] != 200:
            result.update(status="HTTP_REQUEST_FAILED", exit_code=22)
            return result
        result.update(validate(json.loads(raw)))
        complete = not result["missing_probe_dates"] and not result["unexpected_pagination"]
        result.update(status="SCHEMA_AND_COVERAGE_PASS" if complete else "COVERAGE_INCOMPLETE",
                      exit_code=0 if complete else 23)
    except HTTPError as exc:
        result.update(http_status=exc.code, status={401: "AUTHENTICATION_FAILED", 403: "FEED_FORBIDDEN",
                      429: "RATE_LIMITED"}.get(exc.code, "HTTP_REQUEST_FAILED"))
        exc.close()
    except (HTTPException, URLError, TimeoutError, OSError):
        result["status"] = "TRANSPORT_FAILED"
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        result.update(status="RESPONSE_SCHEMA_REJECTED", schema_valid=False, exit_code=24)
    return result


def main() -> int:
    try:
        output = Path("diagnostic.json").open("x", encoding="utf-8")
    except FileExistsError:
        print("OUTPUT_EXISTS_NO_REQUEST")
        return 21
    with output:
        json.dump({"status": "RESERVED_OUTCOME_UNKNOWN_DO_NOT_RETRY"}, output)
        output.flush()
        result = probe(os.environ, opener=build_opener(ProxyHandler({}), NoRedirect()))
        rendered = json.dumps(result, sort_keys=True, allow_nan=False)
        output.seek(0)
        output.truncate()
        output.write(rendered + "\n")
    print("ALPACA_SOXL_PROBE=" + rendered)
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
