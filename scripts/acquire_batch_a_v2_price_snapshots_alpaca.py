"""Batch A v2 Alpaca SIP price_snapshot collector (qsl.research.price_snapshot.v2).

Independent of the Global ETF / R3 adapters. Default CLI is PLAN_ONLY: no
credentials, no network, no file writes. Execute is restricted to main-branch
GitHub Actions with an explicit batch id and injected or cloud transport/storage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

_ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
_SCHEMA = "qsl.research.price_snapshot.v2"
_GCS_BUCKET = "qsl-research-evidence-831478360303"
_CODE_VERSION = "acquire_batch_a_v2_price_snapshots_alpaca.v1"
_LICENSE_RETENTION = (
    "alpaca_sip_research_private_gcs_create_only; "
    "retain only under qsl-research-evidence private bucket policy; no redistribution"
)
_SOURCE_REVISION = "alpaca.stocks.bars.v2/sip/1Day/adjustment=all/2016-01-01_2025-01-01"
_TIMEZONE_NAME = "America/New_York"
_TIMEZONE = ZoneInfo(_TIMEZONE_NAME)
_CALENDAR = "XNYS"
_REQUEST_START_DATE = "2016-01-01"
_REQUEST_END_EXCLUSIVE = "2025-01-01"
_CALENDAR_END = "2024-12-31"
_WARMUP_START = "2016-01-01"
_LEARNING_START = "2017-01-01"
_REQUEST_START = "2016-01-01T00:00:00-05:00"
_REQUEST_END = "2025-01-01T00:00:00-05:00"
_ASOF = "2024-12-31"
_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
_GITHUB_ACTIONS = "GITHUB_ACTIONS"
_GITHUB_WORKFLOW = "GITHUB_WORKFLOW"
_GITHUB_REF = "GITHUB_REF"
_MAIN_REF = "refs/heads/main"
_CSV_COLUMNS = ("symbol", "as_of", "open", "high", "low", "close", "volume")
_REQUEST_PARAMS = {
    "timeframe": "1Day",
    "start": _REQUEST_START,
    "end": _REQUEST_END,
    "asof": _ASOF,
    "feed": "sip",
    "adjustment": "all",
    "currency": "USD",
    "sort": "asc",
    "limit": "10000",
}
SLEEVES: dict[str, tuple[str, ...]] = {
    "soxl_soxx": ("SOXX", "SOXL"),
    "tqqq_qqq": ("QQQ", "TQQQ"),
}
_ALL_SYMBOLS: tuple[str, ...] = tuple(symbol for symbols in SLEEVES.values() for symbol in symbols)


class BatchAV2AcquisitionError(ValueError):
    """Sanitized fail-closed acquisition error."""

    def __init__(self, code: str, *, status: str = "PARKED") -> None:
        self.code = code
        self.status = status
        super().__init__(code)


class GetTransport(Protocol):
    def __call__(self, url: str, params: Mapping[str, str]) -> Mapping[str, object]: ...


class ObjectStorage(Protocol):
    def exists(self, object_path: str) -> bool: ...

    def upload_create_only(self, object_path: str, payload: bytes, *, content_type: str) -> None: ...

    def readback_identity(self, object_path: str) -> tuple[str, int]: ...


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise BatchAV2AcquisitionError("provider request rejected")


class AlpacaHttpsTransport:
    """One-shot fixed-host HTTPS transport with no proxy, redirect, or retry."""

    def __init__(self, api_key_id: str, api_secret_key: str, *, opener=None) -> None:
        if (
            not isinstance(api_key_id, str)
            or not api_key_id
            or not isinstance(api_secret_key, str)
            or not api_secret_key
        ):
            raise BatchAV2AcquisitionError("credentials unavailable")
        self._headers = {"APCA-API-KEY-ID": api_key_id, "APCA-API-SECRET-KEY": api_secret_key}
        self._opener = opener or build_opener(ProxyHandler({}), _RejectRedirects())

    def __call__(self, url: str, params: Mapping[str, str]) -> Mapping[str, object]:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "data.alpaca.markets" or parsed.path != "/v2/stocks/bars":
            raise BatchAV2AcquisitionError("provider request rejected")
        if parsed.params or parsed.query or parsed.fragment:
            raise BatchAV2AcquisitionError("provider request rejected")
        if set(params) != set(_REQUEST_PARAMS) | {"symbols"} or params.get("symbols") not in _ALL_SYMBOLS:
            raise BatchAV2AcquisitionError("provider request rejected")
        if dict(params) != _request_params(str(params["symbols"])):
            raise BatchAV2AcquisitionError("provider request rejected")
        request = Request(f"{url}?{urlencode(params)}", headers=self._headers, method="GET")
        try:
            with self._opener.open(request, timeout=30) as response:
                if getattr(response, "status", 200) != 200:
                    raise BatchAV2AcquisitionError("provider request failed")
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) > _MAX_RESPONSE_BYTES:
                    raise BatchAV2AcquisitionError("provider response too large")
                body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(body) > _MAX_RESPONSE_BYTES:
                    raise BatchAV2AcquisitionError("provider response too large")
            payload = json.loads(body)
        except BatchAV2AcquisitionError:
            raise
        except HTTPError as exc:
            if exc.code in {403, 429}:
                raise BatchAV2AcquisitionError(f"provider http {exc.code}") from None
            raise BatchAV2AcquisitionError("provider request failed") from None
        except (URLError, OSError, TypeError, ValueError, json.JSONDecodeError):
            raise BatchAV2AcquisitionError("provider request failed") from None
        if not isinstance(payload, Mapping):
            raise BatchAV2AcquisitionError("invalid provider response")
        return payload


class GcsObjectStorage:
    """Create-only GCS storage bound to the fixed research evidence bucket."""

    def __init__(self, storage_client: object) -> None:
        self._client = storage_client
        try:
            self._bucket = storage_client.bucket(_GCS_BUCKET)
        except Exception:  # noqa: BLE001 - storage details must not escape
            raise BatchAV2AcquisitionError("bucket preflight unavailable") from None

    def exists(self, object_path: str) -> bool:
        try:
            return bool(self._bucket.blob(object_path).exists(retry=None, timeout=30))
        except BatchAV2AcquisitionError:
            raise
        except Exception:  # noqa: BLE001
            raise BatchAV2AcquisitionError("bucket preflight unavailable") from None

    def upload_create_only(self, object_path: str, payload: bytes, *, content_type: str) -> None:
        try:
            blob = self._bucket.blob(object_path)
            blob.upload_from_string(
                payload,
                content_type=content_type,
                if_generation_match=0,
                retry=None,
                timeout=30,
            )
        except Exception:  # noqa: BLE001 - never retry or leak details
            raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN") from None

    def readback_identity(self, object_path: str) -> tuple[str, int]:
        try:
            blob = self._bucket.blob(object_path)
            blob.reload(retry=None, timeout=30)
            generation = blob.generation
            size = blob.size
            if generation is None or size is None:
                raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
            generation_text = str(generation)
            if not generation_text.isdigit():
                raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
            byte_count = int(size)
            if byte_count < 1:
                raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
            return generation_text, byte_count
        except BatchAV2AcquisitionError:
            raise
        except Exception:  # noqa: BLE001
            raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN") from None


def validate_batch_id(batch_id: object) -> str:
    if not isinstance(batch_id, str) or _BATCH_ID_RE.fullmatch(batch_id) is None:
        raise BatchAV2AcquisitionError("batch id rejected")
    if "/" in batch_id or ".." in batch_id or batch_id in {".", ".."}:
        raise BatchAV2AcquisitionError("batch id rejected")
    return batch_id


def _request_params(symbol: str) -> dict[str, str]:
    params = dict(_REQUEST_PARAMS)
    params["symbols"] = symbol
    return params


def _dataset_id(batch_id: str, sleeve: str) -> str:
    return f"{batch_id}/{sleeve}"


def _object_path(batch_id: str, sleeve: str, name: str) -> str:
    return f"research/v2/input/{batch_id}/{sleeve}/{name}"


def _sleeve_object_names(batch_id: str) -> tuple[str, ...]:
    names: list[str] = []
    for sleeve in SLEEVES:
        for filename in ("prices.csv", "object_identity.json", "prices.csv.manifest.json"):
            names.append(_object_path(batch_id, sleeve, filename))
    return tuple(names)


def _expected_xnys_sessions() -> tuple[date, ...]:
    try:
        import exchange_calendars as xcals

        calendar = xcals.get_calendar(_CALENDAR, start=_REQUEST_START_DATE, end=_CALENDAR_END)
        sessions = calendar.sessions
        return tuple(item.date() for item in sessions)
    except Exception:  # noqa: BLE001
        raise BatchAV2AcquisitionError("calendar unavailable") from None


def _session_from_timestamp(value: object) -> date:
    if not isinstance(value, str):
        raise BatchAV2AcquisitionError("invalid bar timestamp")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(_TIMEZONE).date()
    except (TypeError, ValueError, OverflowError):
        raise BatchAV2AcquisitionError("invalid bar timestamp") from None


def _finite_number(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise BatchAV2AcquisitionError("invalid bar value")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise BatchAV2AcquisitionError("invalid bar value") from None
    if not math.isfinite(number):
        raise BatchAV2AcquisitionError("invalid bar value")
    if positive and number <= 0:
        raise BatchAV2AcquisitionError("invalid bar value")
    if nonnegative and number < 0:
        raise BatchAV2AcquisitionError("invalid bar value")
    return 0.0 if number == 0.0 else number


def _normalize_symbol_bars(
    payload: Mapping[str, object], *, symbol: str, expected_sessions: tuple[date, ...]
) -> list[dict[str, object]]:
    if payload.get("next_page_token") not in (None, ""):
        raise BatchAV2AcquisitionError("pagination is not allowed")
    bars = payload.get("bars")
    if not isinstance(bars, Mapping) or set(bars) != {symbol} or not isinstance(bars.get(symbol), list):
        raise BatchAV2AcquisitionError("bars symbols do not exactly match request")

    normalized: list[dict[str, object]] = []
    seen: set[date] = set()
    for raw in bars[symbol]:
        if not isinstance(raw, Mapping):
            raise BatchAV2AcquisitionError("invalid bar")
        session = _session_from_timestamp(raw.get("t"))
        if session in seen:
            raise BatchAV2AcquisitionError("duplicate bar")
        seen.add(session)
        open_price = _finite_number(raw.get("o"), positive=True)
        high_price = _finite_number(raw.get("h"), positive=True)
        low_price = _finite_number(raw.get("l"), positive=True)
        close_price = _finite_number(raw.get("c"), positive=True)
        volume = _finite_number(raw.get("v"), nonnegative=True)
        if low_price > min(open_price, high_price, close_price) or high_price < max(
            open_price, low_price, close_price
        ):
            raise BatchAV2AcquisitionError("invalid bar range")
        normalized.append(
            {
                "symbol": symbol,
                "as_of": session.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }
        )

    observed = tuple(item["as_of"] for item in normalized)
    expected = tuple(item.isoformat() for item in expected_sessions)
    if observed != tuple(sorted(observed)):
        raise BatchAV2AcquisitionError("bars are not sorted")
    if observed != expected:
        raise BatchAV2AcquisitionError("bars do not cover the fixed XNYS sessions")
    return normalized


def _canonical_prices_csv(rows: Sequence[Mapping[str, object]]) -> bytes:
    ordered = sorted(rows, key=lambda row: (str(row["as_of"]), str(row["symbol"])))
    lines = [",".join(_CSV_COLUMNS)]
    for row in ordered:
        lines.append(
            ",".join(
                (
                    str(row["symbol"]),
                    str(row["as_of"]),
                    *(
                        format(float(row[field]), ".17g")
                        for field in ("open", "high", "low", "close", "volume")
                    ),
                )
            )
        )
    return ("\n".join(lines) + "\n").encode()


def _json_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _require_execute_environment(environ: Mapping[str, str]) -> None:
    if environ.get(_GITHUB_ACTIONS) != "true" or not environ.get(_GITHUB_WORKFLOW):
        raise BatchAV2AcquisitionError("cloud execution unavailable")
    if environ.get(_GITHUB_REF) != _MAIN_REF:
        raise BatchAV2AcquisitionError("main branch required")
    for banned in ("LIVE_TRADING", "PAPER_TRADING_ENABLED", "RUNTIME_TARGET_ENABLED"):
        value = environ.get(banned, "").strip().lower()
        if value in {"1", "true", "yes", "on"}:
            raise BatchAV2AcquisitionError("nonlive environment required")


def _preflight_absent(storage: ObjectStorage, batch_id: str) -> None:
    for object_path in _sleeve_object_names(batch_id):
        try:
            present = storage.exists(object_path)
        except BatchAV2AcquisitionError:
            raise
        except Exception:  # noqa: BLE001
            raise BatchAV2AcquisitionError("bucket preflight unavailable") from None
        if present:
            raise BatchAV2AcquisitionError("target object already exists")


def collect_sleeve_rows(
    get_transport: GetTransport,
    *,
    symbols: Sequence[str],
    expected_sessions: tuple[date, ...],
) -> list[dict[str, object]]:
    if not callable(get_transport):
        raise BatchAV2AcquisitionError("transport unavailable")
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        params = _request_params(symbol)
        try:
            payload = get_transport(_ALPACA_BARS_URL, params)
        except BatchAV2AcquisitionError:
            raise
        except Exception:  # noqa: BLE001
            raise BatchAV2AcquisitionError("provider request failed") from None
        if not isinstance(payload, Mapping):
            raise BatchAV2AcquisitionError("invalid provider response")
        rows.extend(_normalize_symbol_bars(payload, symbol=symbol, expected_sessions=expected_sessions))
    return rows


def _build_manifest(
    *,
    batch_id: str,
    sleeve: str,
    symbols: Sequence[str],
    rows: Sequence[Mapping[str, object]],
    prices_object: str,
    generation: str,
    byte_count: int,
    sha256: str,
    retrieved_at: str,
    code_version: str,
) -> dict[str, object]:
    dataset_id = _dataset_id(batch_id, sleeve)
    counts = {symbol: 0 for symbol in symbols}
    first: dict[str, str] = {}
    last: dict[str, str] = {}
    for row in sorted(rows, key=lambda item: (str(item["as_of"]), str(item["symbol"]))):
        symbol = str(row["symbol"])
        as_of = str(row["as_of"])
        counts[symbol] += 1
        first.setdefault(symbol, as_of)
        last[symbol] = as_of
    if set(counts) != set(symbols) or any(count < 1 for count in counts.values()):
        raise BatchAV2AcquisitionError("counts invalid")
    coverage = {symbol: {"start": first[symbol], "end": last[symbol]} for symbol in symbols}
    return {
        "schema": _SCHEMA,
        "research_only": True,
        "dataset_id": dataset_id,
        "provider": "alpaca",
        "feed": "sip",
        "price_field": "adjusted_close",
        "adjustment": "all",
        "calendar": _CALENDAR,
        "timezone": _TIMEZONE_NAME,
        "license_retention": _LICENSE_RETENTION,
        "code_version": code_version,
        "source_revision": _SOURCE_REVISION,
        "retrieved_at": retrieved_at,
        "symbols": list(symbols),
        "request": {"start": _REQUEST_START_DATE, "end_exclusive": _REQUEST_END_EXCLUSIVE},
        "gcs": {
            "bucket": _GCS_BUCKET,
            "object": prices_object,
            "generation": generation,
            "bytes": byte_count,
            "sha256": sha256,
        },
        "counts": counts,
        "coverage": coverage,
    }


def _safe_upload(storage: ObjectStorage, object_path: str, payload: bytes, *, content_type: str) -> None:
    try:
        storage.upload_create_only(object_path, payload, content_type=content_type)
    except BatchAV2AcquisitionError:
        raise
    except Exception:  # noqa: BLE001 - upload outcome unknown; never retry or clean up
        raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN") from None


def _safe_readback(storage: ObjectStorage, object_path: str) -> tuple[str, int]:
    try:
        generation, byte_count = storage.readback_identity(object_path)
    except BatchAV2AcquisitionError:
        raise
    except Exception:  # noqa: BLE001
        raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN") from None
    if not isinstance(generation, str) or not generation.isdigit():
        raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
    if not isinstance(byte_count, int) or isinstance(byte_count, bool) or byte_count < 1:
        raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
    return generation, byte_count


def _upload_sleeve(
    storage: ObjectStorage,
    *,
    batch_id: str,
    sleeve: str,
    symbols: Sequence[str],
    rows: Sequence[Mapping[str, object]],
    retrieved_at: str,
    code_version: str,
) -> dict[str, object]:
    prices_object = _object_path(batch_id, sleeve, "prices.csv")
    identity_object = _object_path(batch_id, sleeve, "object_identity.json")
    manifest_object = _object_path(batch_id, sleeve, "prices.csv.manifest.json")
    prices_bytes = _canonical_prices_csv(rows)
    sha256 = hashlib.sha256(prices_bytes).hexdigest()
    _safe_upload(storage, prices_object, prices_bytes, content_type="text/csv; charset=utf-8")
    generation, byte_count = _safe_readback(storage, prices_object)
    if byte_count != len(prices_bytes):
        raise BatchAV2AcquisitionError("OBJECT_UPLOAD_UNKNOWN", status="UNKNOWN")
    identity = {
        "bucket": _GCS_BUCKET,
        "object": prices_object,
        "generation": generation,
        "bytes": byte_count,
        "sha256": sha256,
    }
    _safe_upload(storage, identity_object, _json_bytes(identity), content_type="application/json")
    manifest = _build_manifest(
        batch_id=batch_id,
        sleeve=sleeve,
        symbols=symbols,
        rows=rows,
        prices_object=prices_object,
        generation=generation,
        byte_count=byte_count,
        sha256=sha256,
        retrieved_at=retrieved_at,
        code_version=code_version,
    )
    _safe_upload(storage, manifest_object, _json_bytes(manifest), content_type="application/json")
    return {
        "sleeve": sleeve,
        "dataset_id": _dataset_id(batch_id, sleeve),
        "symbols": list(symbols),
        "prices_object": prices_object,
        "generation": generation,
        "bytes": byte_count,
        "sha256": sha256,
        "bar_count": len(rows),
    }


def execute_batch_a_v2_acquisition(
    *,
    batch_id: str,
    storage: ObjectStorage,
    http_transport: GetTransport,
    environ: Mapping[str, str] | None = None,
    retrieved_at: str | None = None,
    code_version: str | None = None,
) -> dict[str, object]:
    """Run one explicit non-live acquisition for both Batch A sleeves."""
    environment = os.environ if environ is None else environ
    _require_execute_environment(environment)
    validated_batch_id = validate_batch_id(batch_id)
    _preflight_absent(storage, validated_batch_id)
    expected_sessions = _expected_xnys_sessions()
    if not expected_sessions:
        raise BatchAV2AcquisitionError("calendar unavailable")

    sleeve_rows: dict[str, list[dict[str, object]]] = {}
    for sleeve, symbols in SLEEVES.items():
        sleeve_rows[sleeve] = collect_sleeve_rows(
            http_transport, symbols=symbols, expected_sessions=expected_sessions
        )

    stamp = retrieved_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    version = code_version or environment.get("GITHUB_SHA") or _CODE_VERSION
    if not isinstance(version, str) or not version:
        version = _CODE_VERSION

    uploaded: list[dict[str, object]] = []
    for sleeve, symbols in SLEEVES.items():
        uploaded.append(
            _upload_sleeve(
                storage,
                batch_id=validated_batch_id,
                sleeve=sleeve,
                symbols=symbols,
                rows=sleeve_rows[sleeve],
                retrieved_at=stamp,
                code_version=version,
            )
        )
    return {
        "status": "COLLECTED_AND_UPLOADED",
        "schema": _SCHEMA,
        "batch_id": validated_batch_id,
        "bucket": _GCS_BUCKET,
        "sleeves": uploaded,
        "request": {
            "url": _ALPACA_BARS_URL,
            "params": {**_REQUEST_PARAMS, "symbols": "<one fixed symbol per request>"},
        },
        "controls": {
            "no_order": True,
            "research_only": True,
            "execution_authorized": False,
        },
    }


def plan_payload() -> dict[str, object]:
    return {
        "status": "PLAN_ONLY",
        "schema": _SCHEMA,
        "sleeves": {name: list(symbols) for name, symbols in SLEEVES.items()},
        "symbols": list(_ALL_SYMBOLS),
        "request_count": len(_ALL_SYMBOLS),
        "warmup_start": _WARMUP_START,
        "learning_start": _LEARNING_START,
        "request": {
            "start": _REQUEST_START_DATE,
            "end_exclusive": _REQUEST_END_EXCLUSIVE,
            "url": _ALPACA_BARS_URL,
            "params": {**_REQUEST_PARAMS, "symbols": "<one fixed symbol per request>"},
        },
        "bucket": _GCS_BUCKET,
        "object_prefix": "research/v2/input/<batch_id>/{soxl_soxx,tqqq_qqq}/",
        "objects_per_sleeve": ["prices.csv", "object_identity.json", "prices.csv.manifest.json"],
        "controls": {
            "no_order": True,
            "research_only": True,
            "execution_authorized": False,
        },
        "persistence": "plan_only",
    }


def _failure_payload(exc: BatchAV2AcquisitionError) -> dict[str, object]:
    return {
        "status": exc.status,
        "reason_code": exc.code,
        "no_order": True,
        "research_only": True,
        "execution_authorized": False,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Batch A v2 Alpaca SIP price snapshot plan/execute entry.")
    parser.add_argument("--execute", action="store_true", help="run only inside approved main GitHub Actions")
    parser.add_argument("--batch-id", default=None, help="explicit research/v2 batch id; required for execute")
    args = parser.parse_args(argv)
    if not args.execute:
        print(json.dumps(plan_payload(), sort_keys=True, separators=(",", ":")))
        return
    try:
        if args.batch_id is None:
            raise BatchAV2AcquisitionError("batch id rejected")
        _require_execute_environment(os.environ)
        from google.cloud import storage

        result = execute_batch_a_v2_acquisition(
            batch_id=args.batch_id,
            storage=GcsObjectStorage(storage.Client()),
            http_transport=AlpacaHttpsTransport(
                os.environ.get("ALPACA_API_KEY_ID", ""),
                os.environ.get("ALPACA_API_SECRET_KEY", ""),
            ),
        )
    except BatchAV2AcquisitionError as exc:
        print(json.dumps(_failure_payload(exc), sort_keys=True, separators=(",", ":")))
        raise SystemExit(2) from None
    except Exception:  # noqa: BLE001 - cloud bootstrap details must not escape
        print(
            json.dumps(
                {
                    "status": "PARKED",
                    "reason_code": "cloud execution unavailable",
                    "no_order": True,
                    "research_only": True,
                    "execution_authorized": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        raise SystemExit(2) from None
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
