#!/usr/bin/env python3
"""One-shot bounded raw SIP input acquisition for the six fixed research symbols."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

BUCKET = "qsl-research-evidence-831478360303"
PREFIX = "research/v2/input/qqqm-boxx-raw-20260925-001/"
HOST = "https://data.alpaca.markets"
END = "2025-01-01T00:00:00-05:00"
ACTION_END = "2024-12-31"
ACTION_START = None
ASOF = "2024-12-31"
MAX_PAGES = 20
MAX_BYTES = 100 * 1024 * 1024
MAX_PAGE_BYTES = 16 * 1024 * 1024
WINDOWS = {
    "QQQM": ("2020-10-13T00:00:00-04:00", "2020-10-13"),
    "BOXX": ("2022-12-28T00:00:00-05:00", "2022-12-28"),
    "SOXL": ("2022-01-03T00:00:00-05:00", "2022-01-03"),
    "SOXX": ("2022-01-03T00:00:00-05:00", "2022-01-03"),
    "TQQQ": ("2022-01-03T00:00:00-05:00", "2022-01-03"),
    "QQQ": ("2022-01-03T00:00:00-05:00", "2022-01-03"),
}
R9_WORKFLOW = "R9 Raw Temporal Extension"
R9_SCOPE = os.environ.get("GITHUB_WORKFLOW") == R9_WORKFLOW
R9_LICENSE_RECORD_SHA256 = "cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182"
MAX_STORAGE_OPERATIONS = 200
MAX_STORAGE_TRANSFER_BYTES = 1024 * 1024 * 1024
if R9_SCOPE:
    PREFIX = "research/v2/input/r9-temporal-extension-20260926-001/"
    END = "2026-08-26T00:00:00-04:00"
    ACTION_START = "2024-10-01"
    ACTION_END = "2026-08-25"
    ASOF = "2026-08-25"
    MAX_PAGES = 60
    MAX_BYTES = 128 * 1024 * 1024
    MAX_PAGE_BYTES = 4 * 1024 * 1024
    WINDOWS = {symbol: ("2025-01-01T00:00:00-05:00", "2025-01-01")
               for symbol in WINDOWS}
ACTION_TYPES = frozenset({
    "capital_gains_distributions", "cash_dividends", "cash_mergers", "forward_splits",
    "name_changes", "partial_calls", "redemptions", "reorganizations", "reverse_splits",
    "rights_distributions", "spin_offs", "stock_and_cash_mergers", "stock_dividends",
    "stock_mergers", "unit_splits", "worthless_removals",
})


class AcquisitionError(Exception):
    def __init__(self, code: str, *, status: int | None = None,
                 provider_code: int | None = None, provider_message: str | None = None) -> None:
        self.code = code
        self.status = status
        self.provider_code = provider_code
        self.provider_message = provider_message
        super().__init__(code)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _provider_error(body: bytes) -> int | None:
    try:
        value = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    if not (isinstance(code, int) and not isinstance(code, bool) and 0 <= code <= 999999999999):
        return None
    return code


class PrivateStore:
    """Fixed-bucket create-only write followed by exact-generation byte readback."""

    def __init__(self, client: Any) -> None:
        self.bucket = client.bucket(BUCKET)
        self.operations = 0
        self.transfer_bytes = 0

    def create_and_verify(self, name: str, body: bytes) -> dict[str, object]:
        if not name or name.startswith("/") or ".." in name or "//" in name:
            raise AcquisitionError("OBJECT_NAME_REJECTED")
        if R9_SCOPE:
            if len(body) > MAX_PAGE_BYTES:
                raise AcquisitionError("OBJECT_MULTIPART_LIMIT_EXCEEDED")
            if self.operations + 3 > MAX_STORAGE_OPERATIONS:
                raise AcquisitionError("STORAGE_OPERATION_BUDGET_EXHAUSTED")
            if self.transfer_bytes + 2 * len(body) > MAX_STORAGE_TRANSFER_BYTES:
                raise AcquisitionError("STORAGE_TRANSFER_BUDGET_EXHAUSTED")
            self.operations += 3  # One small multipart upload, reload, exact-generation download.
            self.transfer_bytes += 2 * len(body)
        blob = self.bucket.blob(PREFIX + name)
        try:
            blob.upload_from_string(body, content_type="application/json",
                                    if_generation_match=0, retry=None, timeout=30)
            blob.reload(retry=None, timeout=30)
            generation = blob.generation
            if generation is None or not str(generation).isdigit():
                raise AcquisitionError("OBJECT_READBACK_UNKNOWN")
            readback = blob.download_as_bytes(if_generation_match=int(generation),
                                              retry=None, timeout=30)
        except AcquisitionError:
            raise
        except Exception:  # noqa: BLE001 - no cloud error details in public logs
            raise AcquisitionError("OBJECT_WRITE_OR_READBACK_UNKNOWN") from None
        if readback != body:
            raise AcquisitionError("OBJECT_READBACK_MISMATCH")
        return {"uri": f"gs://{BUCKET}/{PREFIX}{name}", "generation": str(generation),
                "bytes": len(body), "sha256": _sha(body)}


class BoundedProvider:
    def __init__(self, key_id: str, secret: str, *, opener: Any = None) -> None:
        if not key_id or not secret:
            raise AcquisitionError("CREDENTIALS_NOT_CONFIGURED")
        self.headers = {"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret}
        self.opener = opener or build_opener(ProxyHandler({}), _NoRedirect())
        self.pages = 0
        self.bytes = 0
        self.completed: list[dict[str, object]] = []

    def get(self, path: str, params: dict[str, str]) -> tuple[bytes, dict[str, Any]]:
        if path not in {"/v1/corporate-actions", *(f"/v2/stocks/{s}/bars" for s in WINDOWS)}:
            raise AcquisitionError("ENDPOINT_OUT_OF_SCOPE")
        if self.pages >= MAX_PAGES:
            raise AcquisitionError("PAGE_BUDGET_EXHAUSTED")
        request = Request(f"{HOST}{path}?{urlencode(params)}", headers=self.headers,
                          method="GET")
        self.pages += 1  # Count attempted requests, including failures.
        remaining = min(MAX_PAGE_BYTES, MAX_BYTES - self.bytes)
        if remaining < 1:
            raise AcquisitionError("BYTE_BUDGET_EXHAUSTED")
        try:
            with self.opener.open(request, timeout=30) as response:
                if getattr(response, "status", 200) != 200:
                    raise AcquisitionError("PROVIDER_HTTP_UNEXPECTED", status=response.status)
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) > remaining:
                    raise AcquisitionError("BYTE_BUDGET_EXHAUSTED")
                body = response.read(remaining)
        except HTTPError as exc:
            error_body = exc.read(min(64 * 1024, remaining))
            self.bytes += len(error_body)
            provider_code = _provider_error(error_body)
            raise AcquisitionError("PROVIDER_REJECTED", status=exc.code,
                                   provider_code=provider_code) from None
        except (TimeoutError, URLError, OSError):
            raise AcquisitionError("PROVIDER_TRANSPORT_FAILED") from None
        self.bytes += len(body)
        if len(body) == remaining and content_length is None:
            raise AcquisitionError("RESPONSE_SIZE_AMBIGUOUS")
        if len(body) == remaining and content_length is not None and int(content_length) != len(body):
            raise AcquisitionError("BYTE_BUDGET_EXHAUSTED")
        try:
            data = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise AcquisitionError("PROVIDER_JSON_INVALID") from None
        if not isinstance(data, dict):
            raise AcquisitionError("PROVIDER_SCHEMA_INVALID")
        return body, data


def _next_token(data: dict[str, Any], seen: set[str]) -> str | None:
    token = data.get("next_page_token")
    if token is None or token == "":
        return None
    if not isinstance(token, str) or len(token) > 2048 or token in seen:
        raise AcquisitionError("PAGINATION_INVALID")
    seen.add(token)
    return token


def _bars_summary(data: dict[str, Any], symbol: str, previous: str | None,
                  start: str) -> tuple[int, str | None, str | None]:
    bars = data.get("bars")
    if not isinstance(bars, list) or len(bars) > 10000:
        raise AcquisitionError("BAR_SCHEMA_INVALID")
    first: str | None = None
    last = previous
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(END).astimezone(UTC)
    for item in bars:
        if not isinstance(item, dict):
            raise AcquisitionError("BAR_SCHEMA_INVALID")
        stamp = item.get("t")
        if not isinstance(stamp, str) or len(stamp) > 40:
            raise AcquisitionError("BAR_TIMESTAMP_INVALID")
        try:
            current = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if current.tzinfo is None:
                raise ValueError
            current = current.astimezone(UTC)
            normalized = current.isoformat().replace("+00:00", "Z")
            if not (start_dt <= current < end_dt):
                raise ValueError
        except ValueError:
            raise AcquisitionError("BAR_TIMESTAMP_INVALID") from None
        if last is not None and current <= datetime.fromisoformat(last.replace("Z", "+00:00")):
            raise AcquisitionError("BAR_DUPLICATE_OR_ORDER")
        for field in ("o", "h", "l", "c", "v"):
            value = item.get(field)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
                raise AcquisitionError("BAR_OHLCV_INVALID")
            if value <= 0 and field != "v":
                raise AcquisitionError("BAR_OHLCV_INVALID")
            if value < 0 and field == "v":
                raise AcquisitionError("BAR_OHLCV_INVALID")
        if item["l"] > min(item["o"], item["c"], item["h"]) or item["h"] < max(item["o"], item["c"], item["l"]):
            raise AcquisitionError("BAR_OHLCV_INVALID")
        first = first or normalized
        last = normalized
    if data.get("symbol") not in (None, symbol):
        raise AcquisitionError("BAR_SYMBOL_MISMATCH")
    return len(bars), first, last


def _action_summary(data: dict[str, Any], symbol: str) -> int:
    actions = data.get("corporate_actions")
    if not isinstance(actions, dict) or set(actions) - ACTION_TYPES:
        raise AcquisitionError("ACTION_SCHEMA_INVALID")
    count = 0
    for category in actions.values():
        if not isinstance(category, list):
            raise AcquisitionError("ACTION_SCHEMA_INVALID")
        count += len(category)
        for item in category:
            if not isinstance(item, dict) or item.get("symbol") not in (None, symbol):
                raise AcquisitionError("ACTION_SYMBOL_MISMATCH")
    if count > 1000:
        raise AcquisitionError("ACTION_SCHEMA_INVALID")
    return count


def _collect(provider: BoundedProvider, store: PrivateStore, symbol: str,
             kind: str) -> dict[str, object]:
    start_time, start_date = WINDOWS[symbol]
    if kind == "bars":
        path = f"/v2/stocks/{symbol}/bars"
        params = {"timeframe": "1Day", "start": start_time, "end": END,
                  "asof": ASOF, "feed": "sip", "adjustment": "raw", "currency": "USD",
                  "sort": "asc", "limit": "10000"}
    else:
        path = "/v1/corporate-actions"
        params = {"symbols": symbol, "region": "us", "start": ACTION_START or start_date,
                  "end": ACTION_END, "data_quality": "all", "sort": "asc", "limit": "1000"}
    seen: set[str] = set()
    token: str | None = None
    page = 0
    count = 0
    first: str | None = None
    last: str | None = None
    objects: list[dict[str, object]] = []
    start_utc = datetime.fromisoformat(start_time).astimezone(UTC).isoformat().replace("+00:00", "Z")
    while True:
        query = dict(params)
        if token is not None:
            query["page_token"] = token
        body, data = provider.get(path, query)
        if kind == "bars":
            size, page_first, page_last = _bars_summary(data, symbol, last, start_utc)
            first = first or page_first
            last = page_last or last
        else:
            size = _action_summary(data, symbol)
        count += size
        page += 1
        objects.append(store.create_and_verify(f"{kind}/{symbol}/page-{page:03d}.json", body))
        token = _next_token(data, seen)
        if token is None:
            break
        if not size:
            raise AcquisitionError("EMPTY_PAGE_WITH_CONTINUATION")
    return {"symbol": symbol, "kind": kind, "request": params, "count": count,
            "first_bar_time": first if kind == "bars" else None,
            "last_bar_time": last if kind == "bars" else None,
            "pages": objects, "complete_pagination": True}


def run(store: PrivateStore, provider: BoundedProvider) -> dict[str, object]:
    probe = {"schema_version": "qsl.research.private_write_probe.v1",
             "scope": PREFIX, "observed_at": _utc_now(), "no_order": True}
    marker = store.create_and_verify("_write_probe.json",
                                     json.dumps(probe, sort_keys=True).encode())
    inputs = []
    for symbol in WINDOWS:
        inputs.append(_collect(provider, store, symbol, "bars"))
        provider.completed.append({"symbol": symbol, "kind": "bars", "count": inputs[-1]["count"],
                                   "first_bar_time": inputs[-1]["first_bar_time"],
                                   "last_bar_time": inputs[-1]["last_bar_time"]})
    for symbol in WINDOWS:
        inputs.append(_collect(provider, store, symbol, "actions"))
        provider.completed.append({"symbol": symbol, "kind": "actions", "count": inputs[-1]["count"]})
    manifest = {"schema_version": "qsl.research.raw_sip_input.v1",
                "retrieved_at": _utc_now(), "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
                "feed": "sip", "price_adjustment": "raw", "calendar": "XNYS",
                "timezone": "America/New_York", "currency": "USD",
                "license_retention": "private research only; retain in qsl-research-evidence bucket; no redistribution",
                "scope": PREFIX, "write_probe": marker, "inputs": inputs,
                "provider_page_requests": provider.pages, "provider_response_bytes": provider.bytes,
                "bar_timestamp_meaning": "left edge of daily bar, not decision availability",
                "corporate_action_limitation": "provider does not guarantee creation time; historical availability not proven",
                "no_order": True, "research_only": True, "execution_authorized": False}
    if R9_SCOPE:
        manifest["license_basis_record_sha256"] = R9_LICENSE_RECORD_SHA256
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    manifest_identity = store.create_and_verify("manifest.json", encoded)
    result = {"status": "COMPLETE_SOURCE_DOWNLOAD", "manifest": manifest_identity,
            "provider_page_requests": provider.pages, "provider_response_bytes": provider.bytes,
            "coverage": [{"symbol": item["symbol"], "kind": item["kind"], "count": item["count"],
                          "first_bar_time": item["first_bar_time"],
                          "last_bar_time": item["last_bar_time"]} for item in inputs],
            "no_order": True}
    if R9_SCOPE:
        result["storage_operations"] = store.operations
        result["storage_transfer_bytes"] = store.transfer_bytes
    return result


def main() -> int:
    result: dict[str, object]
    provider: BoundedProvider | None = None
    try:
        if (os.environ.get("GITHUB_ACTIONS") != "true"
                or os.environ.get("GITHUB_REF") != "refs/heads/main"
                or os.environ.get("GITHUB_WORKFLOW") not in
                ("QQQM BOXX Raw Historical Inputs", R9_WORKFLOW)):
            raise AcquisitionError("EXECUTION_CONTEXT_REJECTED")
        if R9_SCOPE and os.environ.get("R9_LICENSE_RECORD_SHA256") != R9_LICENSE_RECORD_SHA256:
            raise AcquisitionError("LICENSE_RECORD_UNVERIFIED")
        key_id = os.environ.get("ALPACA_API_KEY_ID", "")
        secret = os.environ.get("ALPACA_API_SECRET_KEY", "")
        provider = BoundedProvider(key_id, secret)
        from google.cloud import storage  # existing locked workflow dependency

        result = run(PrivateStore(storage.Client()), provider)
    except AcquisitionError as exc:
        result = {"status": exc.code, "http_status": exc.status,
                  "provider_code": exc.provider_code,
                  "provider_message": exc.provider_message,
                  "provider_page_requests": provider.pages if provider else 0,
                  "provider_response_bytes": provider.bytes if provider else 0,
                  "completed_inputs": provider.completed if provider else [],
                  "scope": PREFIX, "no_order": True}
    except Exception:  # noqa: BLE001 - no secret or provider body escapes
        result = {"status": "RUNTIME_FAILURE_SANITIZED", "scope": PREFIX, "no_order": True}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "COMPLETE_SOURCE_DOWNLOAD" else 2


if __name__ == "__main__":
    raise SystemExit(main())
