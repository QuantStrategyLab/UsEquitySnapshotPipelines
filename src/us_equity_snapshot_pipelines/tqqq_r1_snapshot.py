"""Provider-free local publication boundary for the TQQQ R1 price snapshot."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
CALENDAR = "XNYS.regular.v1"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
MAX_SESSION = "2100-12-31"
MAX_ROW_COUNT = 100_000
MAX_ENVELOPE_BYTES = 16 * 1024 * 1024
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAME = "snapshot.json"


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotRequest:
    prices: pd.DataFrame
    output_dir: Path
    mode: str = MODE
    plugin: str = PLUGIN
    size: int = 0


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    snapshot_sha256: str
    snapshot_identity: str


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _normalize_session(value: object) -> pd.Timestamp:
    if pd.api.types.is_bool(value):
        _invalid("invalid session")
    try:
        session = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid session") from exc
    if pd.isna(session) or session.tz is not None:
        _invalid("invalid session")
    return session.normalize()


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _xnys_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))
    return holidays


def _is_xnys_regular_session(session: pd.Timestamp) -> bool:
    value = session.date()
    if value.weekday() >= 5 or value < date.fromisoformat(REQUESTED_LOWER_BOUND) or value > date.fromisoformat(MAX_SESSION):
        return False
    return value not in _xnys_holidays(value.year - 1).union(_xnys_holidays(value.year), _xnys_holidays(value.year + 1))


def _normalized_prices(prices: object) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("prices must be a DataFrame")
    if not 0 < len(prices) <= MAX_ROW_COUNT:
        _invalid("row count exceeds bound")
    required = ("session", "symbol", PRICE_FIELD)
    if list(prices.columns) != list(required):
        _invalid("prices must contain exact columns")
    normalized = prices.copy()
    if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
        _invalid("prices contain noncanonical symbol")
    normalized["session"] = normalized["session"].map(_normalize_session)
    if not normalized["session"].map(_is_xnys_regular_session).all():
        _invalid("session must be an XNYS regular session")
    if normalized.duplicated(["session", "symbol"]).any():
        _invalid("duplicate session for symbol")
    if not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")
    adjusted_close = normalized[PRICE_FIELD]
    if (
        pd.api.types.is_bool_dtype(adjusted_close)
        or adjusted_close.map(pd.api.types.is_bool).any()
        or pd.api.types.is_complex_dtype(adjusted_close)
        or adjusted_close.map(pd.api.types.is_complex).any()
    ):
        _invalid("adjusted_close must be real")
    normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if normalized[PRICE_FIELD].isna().any() or not normalized[PRICE_FIELD].map(math.isfinite).all() or (normalized[PRICE_FIELD] <= 0).any():
        _invalid("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _admit(request: object) -> tuple[SnapshotRequest, pd.DataFrame]:
    if type(request) is not SnapshotRequest:
        _invalid("request must be a SnapshotRequest")
    if request.mode != MODE:
        _invalid("mode must be core_only")
    if request.plugin != PLUGIN:
        _invalid("plugin must be ABSENT_DISABLED")
    if type(request.size) is not int or request.size != 0:
        _invalid("size must be zero")
    if not isinstance(request.output_dir, Path) or request.output_dir.name in ("", ".", ".."):
        _invalid("output_dir must be a concrete Path")
    return request, _normalized_prices(request.prices)


def _records(prices: pd.DataFrame) -> list[dict[str, str]]:
    return [
        {"session": session.strftime("%Y-%m-%d"), "symbol": symbol, PRICE_FIELD: format(value, ".17g")}
        for session, symbol, value in prices[["session", "symbol", PRICE_FIELD]].itertuples(index=False, name=None)
    ]


def _identity_payload(records: list[dict[str, str]]) -> dict[str, object]:
    return {
        "calendar": CALENDAR,
        "contract_version": CONTRACT_VERSION,
        "records": records,
        "request": {"mode": MODE, "plugin": PLUGIN, "size": 0},
        "row_count": len(records),
        "symbols": list(SYMBOLS),
    }


def _envelope_from_prices(prices: pd.DataFrame) -> tuple[dict[str, object], str]:
    payload = _identity_payload(_records(prices))
    identity = _sha256(_canonical_bytes(payload))
    return {**payload, "snapshot_identity": identity}, identity


def _parse_envelope(content: bytes) -> dict[str, Any]:
    if len(content) > MAX_ENVELOPE_BYTES:
        _invalid("snapshot exceeds size bound")
    try:
        envelope = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("invalid snapshot envelope") from exc
    if type(envelope) is not dict or _canonical_bytes(envelope) != content:
        _invalid("invalid snapshot envelope")
    return envelope


def _validated_envelope(content: bytes) -> tuple[dict[str, object], str]:
    envelope = _parse_envelope(content)
    expected_keys = {"calendar", "contract_version", "records", "request", "row_count", "snapshot_identity", "symbols"}
    if set(envelope) != expected_keys or envelope.get("calendar") != CALENDAR or envelope.get("contract_version") != CONTRACT_VERSION:
        _invalid("invalid snapshot envelope")
    if envelope.get("request") != {"mode": MODE, "plugin": PLUGIN, "size": 0} or envelope.get("symbols") != list(SYMBOLS):
        _invalid("invalid snapshot envelope")
    records = envelope.get("records")
    if type(records) is not list or not 0 < len(records) <= MAX_ROW_COUNT or envelope.get("row_count") != len(records):
        _invalid("invalid snapshot envelope")
    if any(type(record) is not dict or set(record) != {"session", "symbol", PRICE_FIELD} or any(type(value) is not str for value in record.values()) for record in records):
        _invalid("invalid snapshot envelope")
    prices = _normalized_prices(pd.DataFrame(records, columns=["session", "symbol", PRICE_FIELD]))
    canonical_records = _records(prices)
    if records != canonical_records:
        _invalid("invalid snapshot envelope")
    payload = _identity_payload(canonical_records)
    identity = _sha256(_canonical_bytes(payload))
    if envelope.get("snapshot_identity") != identity:
        _invalid("snapshot identity mismatch")
    return {**payload, "snapshot_identity": identity}, identity


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def verify_tqqq_r1_snapshot(output_dir: str | Path, *, expected_snapshot_sha256: str) -> SnapshotResult:
    output = Path(output_dir)
    if output.is_symlink():
        _invalid("snapshot root symlink is not allowed")
    try:
        names = tuple(path.name for path in output.iterdir()) if output.is_dir() else ()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot") from exc
    if names != (OUTPUT_FILENAME,):
        _invalid(f"unexpected output files: {names}")
    snapshot_path = output / OUTPUT_FILENAME
    try:
        metadata = snapshot_path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_ENVELOPE_BYTES:
            _invalid("snapshot must be a bounded regular non-symlink file")
        content = snapshot_path.read_bytes()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot") from exc
    snapshot_sha256 = _sha256(content)
    if type(expected_snapshot_sha256) is not str or snapshot_sha256 != expected_snapshot_sha256:
        _invalid("trusted snapshot hash mismatch")
    _, identity = _validated_envelope(content)
    return SnapshotResult(output_dir=output, snapshot_sha256=snapshot_sha256, snapshot_identity=identity)


def materialize_tqqq_r1_snapshot(request: object, *_legacy_arguments: object) -> SnapshotResult:
    """Admit one typed local request and atomically publish its canonical envelope."""
    if _legacy_arguments:
        _invalid("request must be a SnapshotRequest")
    admitted_request, prices = _admit(request)
    destination = admitted_request.output_dir
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    envelope, identity = _envelope_from_prices(prices)
    content = _canonical_bytes(envelope)
    if len(content) > MAX_ENVELOPE_BYTES:
        _invalid("snapshot exceeds size bound")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        destination.mkdir()
    except FileExistsError as exc:
        raise SnapshotValidationError(f"immutable output already exists: {destination}") from exc
    fd, temporary_name = tempfile.mkstemp(prefix=f".{OUTPUT_FILENAME}.", dir=destination)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(destination)
        os.replace(temporary, destination / OUTPUT_FILENAME)
        _fsync_directory(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        destination.rmdir()
        raise
    snapshot_sha256 = _sha256(content)
    result = verify_tqqq_r1_snapshot(destination, expected_snapshot_sha256=snapshot_sha256)
    if result.snapshot_identity != identity:
        _invalid("snapshot identity mismatch")
    return result
