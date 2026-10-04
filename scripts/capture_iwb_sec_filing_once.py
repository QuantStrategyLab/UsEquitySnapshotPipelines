#!/usr/bin/env python3
"""Explicit, bounded single-accession raw capture; importing this module never fetches."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from http.client import HTTPException
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from us_equity_snapshot_pipelines.russell_1000_history import IwbSecFilingInputVersion

ACCESSION = "0001004726-26-003726"
REPORT_PERIOD = date(2026, 3, 31)
CIK = "0001100663"
SERIES_ID = "S000004347"
CLASS_ID = "C000012077"
TICKER = "IWB"
INDEX_URL = "https://www.sec.gov/Archives/edgar/data/1100663/0001004726-26-003726-index.htm"
XML_URL = "https://www.sec.gov/Archives/edgar/data/1100663/000100472626003726/primary_doc.xml"
MAX_INDEX_BYTES = 1024 * 1024
MAX_XML_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_INTERVAL_SECONDS = 1.0
_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}\Z")
_UA_REFERENCE_RE = re.compile(r"env:[A-Za-z_][A-Za-z0-9_]*\Z")
_CHALLENGE_MARKERS = (
    b"request rate threshold exceeded",
    b"undeclared automated tool",
    b"access denied",
    b"captcha",
    b"verify you are human",
    b"cf-chl-",
    b"security challenge",
)


class CaptureError(Exception):
    """Only allowlisted error codes, never payloads, headers or identity values."""

    def __init__(self, code: str, *, status: int | None = None) -> None:
        self.code = code
        self.status = status
        super().__init__(code)


@dataclass(frozen=True)
class CaptureConfig:
    run: bool
    index_url: str
    xml_url: str
    accepted_timezone: str
    version_id: str
    user_agent: str
    user_agent_reference: str


@dataclass(frozen=True)
class CaptureResult:
    version: IwbSecFilingInputVersion
    manifest: dict[str, Any]


class SystemClock:
    """Wall clock records response completion; monotonic clock bounds requests."""

    @staticmethod
    def now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()

    @staticmethod
    def sleep(seconds: float) -> None:
        time.sleep(seconds)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CaptureError("CLOCK_TIMESTAMP_INVALID")
    return value


def _monotonic(clock: Any) -> float:
    value = clock.monotonic()
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise CaptureError("CLOCK_MONOTONIC_INVALID")
    return value


def _validate_config(config: CaptureConfig) -> None:
    if config.run is not True:
        raise CaptureError("EXPLICIT_RUN_REQUIRED")
    if config.index_url != INDEX_URL or config.xml_url != XML_URL:
        raise CaptureError("TARGET_URL_OUT_OF_SCOPE")
    if not isinstance(config.version_id, str) or not _VERSION_RE.fullmatch(config.version_id):
        raise CaptureError("VERSION_ID_INVALID")
    if not isinstance(config.accepted_timezone, str) or not config.accepted_timezone:
        raise CaptureError("ACCEPTED_TIMEZONE_REQUIRED")
    try:
        ZoneInfo(config.accepted_timezone)
    except (ZoneInfoNotFoundError, ValueError):
        raise CaptureError("ACCEPTED_TIMEZONE_INVALID") from None
    if not isinstance(config.user_agent_reference, str) or not _UA_REFERENCE_RE.fullmatch(config.user_agent_reference):
        raise CaptureError("USER_AGENT_EXPLICIT_REFERENCE_REQUIRED")
    if (
        not isinstance(config.user_agent, str)
        or not config.user_agent.strip()
        or len(config.user_agent) > 512
        or any(not 32 <= ord(char) <= 126 for char in config.user_agent)
    ):
        raise CaptureError("USER_AGENT_INVALID")


def _header(headers: Any, name: str) -> str | None:
    values = headers.get_all(name) if hasattr(headers, "get_all") else None
    if values is not None:
        if len(values) != 1:
            raise CaptureError("SEC_RESPONSE_HEADERS_AMBIGUOUS")
        return str(values[0])
    value = headers.get(name)
    return None if value is None else str(value)


def _fetch_once(
    opener: Any, clock: Any, *, url: str, limit: int, user_agent: str
) -> tuple[bytes, dict[str, Any], float]:
    request = Request(url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"}, method="GET")
    started = _monotonic(clock)
    _aware(clock.now())
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise CaptureError("SEC_HTTP_REJECTED", status=response.status)
            if response.geturl() != url:
                raise CaptureError("SEC_RESPONSE_URL_MISMATCH")
            encoding = _header(response.headers, "Content-Encoding")
            if encoding is not None and encoding.strip().lower() != "identity":
                raise CaptureError("SEC_COMPRESSION_UNSUPPORTED")
            raw_length = _header(response.headers, "Content-Length")
            length = None
            if raw_length is not None:
                if not re.fullmatch(r"[0-9]{1,12}", raw_length):
                    raise CaptureError("SEC_CONTENT_LENGTH_INVALID")
                length = int(raw_length)
                if length > limit:
                    raise CaptureError("SEC_RESPONSE_TOO_LARGE")
            parts = []
            size = 0
            while True:
                if _monotonic(clock) - started > REQUEST_TIMEOUT_SECONDS:
                    raise CaptureError("SEC_REQUEST_DEADLINE_EXCEEDED")
                chunk = response.read(min(64 * 1024, limit + 1 - size))
                if not isinstance(chunk, bytes):
                    raise CaptureError("SEC_RESPONSE_BYTES_INVALID")
                size += len(chunk)
                if size > limit:
                    raise CaptureError("SEC_RESPONSE_TOO_LARGE")
                if not chunk:
                    # Immediately after EOF, before parsing, closing, or local writes.
                    completed_at = _aware(clock.now())
                    completed_monotonic = _monotonic(clock)
                    break
                parts.append(chunk)
            if completed_monotonic < started:
                raise CaptureError("CLOCK_MONOTONIC_INVALID")
            if completed_monotonic - started > REQUEST_TIMEOUT_SECONDS:
                raise CaptureError("SEC_REQUEST_DEADLINE_EXCEEDED")
            if length is not None and size != length:
                raise CaptureError("SEC_CONTENT_LENGTH_MISMATCH")
            if not size:
                raise CaptureError("SEC_RESPONSE_EMPTY")
    except HTTPError as exc:
        # Do not read, log, retry, or solve any rejection/challenge body.
        raise CaptureError("SEC_HTTP_REJECTED", status=exc.code) from None
    except (URLError, TimeoutError, OSError, HTTPException):
        raise CaptureError("SEC_TRANSPORT_FAILED") from None
    body = b"".join(parts)
    if any(marker in body.lower() for marker in _CHALLENGE_MARKERS):
        raise CaptureError("SEC_CHALLENGE_REJECTED")
    return (
        body,
        {
            "url": url,
            "http_status": 200,
            "bytes": size,
            "sha256": _sha(body),
            "completed_at": completed_at.isoformat(),
        },
        completed_monotonic,
    )


class LocalStore:
    """New version directory, exclusive files, exact-byte/hash readback, manifest last."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.directory: Path | None = None

    def reserve(self, version_id: str) -> None:
        if self.directory is not None or not _VERSION_RE.fullmatch(version_id):
            raise CaptureError("LOCAL_RESERVATION_INVALID")
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            directory = self.root / version_id
            directory.mkdir(mode=0o700)
        except FileExistsError:
            raise CaptureError("LOCAL_VERSION_ALREADY_EXISTS") from None
        except OSError:
            raise CaptureError("LOCAL_WRITE_OR_READBACK_FAILED") from None
        self.directory = directory

    def create_and_verify(self, name: str, body: bytes) -> dict[str, Any]:
        if self.directory is None or name not in {"filing-index.html", "primary_doc.xml", ".manifest.pending"}:
            raise CaptureError("LOCAL_OBJECT_OUT_OF_SCOPE")
        path = self.directory / name
        try:
            with path.open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            readback = path.read_bytes()
        except OSError:
            raise CaptureError("LOCAL_WRITE_OR_READBACK_FAILED") from None
        if len(readback) != len(body) or _sha(readback) != _sha(body) or readback != body:
            raise CaptureError("LOCAL_READBACK_MISMATCH")
        return {"name": name, "bytes": len(body), "sha256": _sha(body)}

    def publish_manifest(self, body: bytes) -> None:
        self.create_and_verify(".manifest.pending", body)
        assert self.directory is not None
        try:
            # Atomic, create-only publication; no partial canonical success file.
            os.link(self.directory / ".manifest.pending", self.directory / "manifest.json")
        except OSError:
            raise CaptureError("LOCAL_MANIFEST_PUBLICATION_FAILED") from None
        # A pending-file cleanup error must not turn an already-published receipt
        # into a claimed failure. It contains the same verified bytes.
        try:
            (self.directory / ".manifest.pending").unlink()
        except OSError:
            pass


def run_capture(
    config: CaptureConfig,
    *,
    storage: Any,
    opener: Any = None,
    clock: Any = None,
) -> CaptureResult:
    """At most two serial attempts. Fakes are injectable for offline verification."""
    _validate_config(config)
    from us_equity_snapshot_pipelines.russell_1000_history import (
        IwbSecFilingAdapterError,
        bind_iwb_sec_filing_input_version,
        parse_iwb_sec_filing_index_html,
        parse_iwb_sec_nport_xml_bytes,
    )

    clock = SystemClock() if clock is None else clock
    _aware(clock.now())
    _monotonic(clock)
    storage.reserve(config.version_id)
    # Keep the runtime's existing proxy configuration. No ProxyHandler override.
    opener = build_opener(_NoRedirect()) if opener is None else opener
    index_body, index_receipt, index_finished = _fetch_once(
        opener,
        clock,
        url=config.index_url,
        limit=MAX_INDEX_BYTES,
        user_agent=config.user_agent,
    )
    try:
        index = parse_iwb_sec_filing_index_html(index_body, accepted_timezone=config.accepted_timezone)
    except IwbSecFilingAdapterError:
        raise CaptureError("SEC_INDEX_INPUT_REJECTED") from None
    if index.accession_number != ACCESSION or index.report_period != REPORT_PERIOD or index.cik != CIK:
        raise CaptureError("SEC_FIXED_TARGET_IDENTITY_MISMATCH")
    remaining = max(0.0, REQUEST_INTERVAL_SECONDS - (_monotonic(clock) - index_finished))
    if remaining:
        clock.sleep(remaining)
    if _monotonic(clock) - index_finished < REQUEST_INTERVAL_SECONDS:
        raise CaptureError("SEC_REQUEST_INTERVAL_UNVERIFIED")
    xml_body, xml_receipt, _ = _fetch_once(
        opener,
        clock,
        url=config.xml_url,
        limit=MAX_XML_BYTES,
        user_agent=config.user_agent,
    )
    observed_at = max(datetime.fromisoformat(item["completed_at"]) for item in (index_receipt, xml_receipt))
    try:
        meta, _ = parse_iwb_sec_nport_xml_bytes(xml_body, expected_index=index)
        if (meta["cik"], meta["series_id"], meta["class_id"], meta["ticker"], meta["report_period"]) != (
            CIK,
            SERIES_ID,
            CLASS_ID,
            TICKER,
            REPORT_PERIOD,
        ):
            raise CaptureError("SEC_FIXED_TARGET_IDENTITY_MISMATCH")
        version = bind_iwb_sec_filing_input_version(
            index_html_bytes=index_body,
            nport_xml_bytes=xml_body,
            observed_at=observed_at,
            version_id=config.version_id,
            accepted_timezone=config.accepted_timezone,
            qualification="raw_capture_unqualified",
        )
    except IwbSecFilingAdapterError:
        raise CaptureError("SEC_XML_OR_BINDING_INPUT_REJECTED") from None
    raw_objects = []
    for name, body in (("filing-index.html", index_body), ("primary_doc.xml", xml_body)):
        entry = storage.create_and_verify(name, body)
        if entry != {"name": name, "bytes": len(body), "sha256": _sha(body)}:
            raise CaptureError("LOCAL_READBACK_METADATA_MISMATCH")
        raw_objects.append(entry)
    manifest = {
        "schema_version": "qsl.research.iwb_sec_single_filing_receipt.v1",
        "status": "capture_complete",
        "version_id": config.version_id,
        "accession_number": ACCESSION,
        "report_period": REPORT_PERIOD.isoformat(),
        "cik": CIK,
        "series_id": SERIES_ID,
        "class_id": CLASS_ID,
        "ticker": TICKER,
        "accepted_at": version.accepted_at.isoformat(),
        "accepted_timezone": config.accepted_timezone,
        "observed_at": observed_at.isoformat(),
        "observed_at_basis": "max_complete_response_receipt_time_not_storage_time",
        "raw_binding_sha256": version.raw_binding_sha256,
        "responses": [index_receipt, xml_receipt],
        "raw_objects": raw_objects,
        "request_count": 2,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "minimum_serial_interval_seconds": REQUEST_INTERVAL_SECONDS,
        "index_byte_limit": MAX_INDEX_BYTES,
        "xml_byte_limit": MAX_XML_BYTES,
        "client_identity": "explicit_executor_reference_value_not_recorded",
        "raw_xsd_validation": "not_validated",
        "adapter_schema_claim": version.schema_claim,
        "qualification": version.qualification,
        "holdings_status_counts": dict(Counter(item.status for item in version.holdings)),
        "first_public_visibility_proven": False,
        "historical_pit_proven": False,
        "production_eligible": False,
        "trading_eligible": False,
    }
    storage.publish_manifest((json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode("utf-8"))
    return CaptureResult(version=version, manifest=manifest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", required=True, help="Explicitly permit the two target requests")
    parser.add_argument("--index-url", required=True)
    parser.add_argument("--xml-url", required=True)
    parser.add_argument("--accepted-timezone", required=True, help="Explicit IANA timezone for index Accepted")
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--user-agent-env", required=True, help="Executor-configured environment reference; no default")
    args = parser.parse_args(argv)
    try:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.user_agent_env):
            raise CaptureError("USER_AGENT_EXPLICIT_REFERENCE_REQUIRED")
        identity = os.environ.get(args.user_agent_env)
        if identity is None:
            raise CaptureError("USER_AGENT_REFERENCE_UNCONFIGURED")
        result = run_capture(
            CaptureConfig(
                run=args.run,
                index_url=args.index_url,
                xml_url=args.xml_url,
                accepted_timezone=args.accepted_timezone,
                version_id=args.version_id,
                user_agent=identity,
                user_agent_reference=f"env:{args.user_agent_env}",
            ),
            storage=LocalStore(args.output_dir),
        )
    except CaptureError as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": exc.code,
                    "http_status": exc.status,
                    "production_eligible": False,
                    "trading_eligible": False,
                },
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "status": "capture_complete",
                "version_id": result.version.version_id,
                "observed_at": result.manifest["observed_at"],
                "production_eligible": False,
                "trading_eligible": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
