"""Provider-free local materializer for the TQQQ R1 canonical snapshot envelope."""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal
from numbers import Integral
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
CANONICAL_FILENAME = "snapshot.json"
MAX_ROWS = 10_000
MAX_SNAPSHOT_BYTES = 5_000_000
EXCEPTIONAL_XNYS_CLOSURES = frozenset({date(2012, 10, 29), date(2012, 10, 30), date(2018, 12, 5), date(2025, 1, 9)})


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class TqqqR1SnapshotRequest:
    prices: pd.DataFrame
    mode: str = MODE
    plugin: str = PLUGIN
    size: int = 0


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise SnapshotValidationError("invalid canonical envelope") from exc


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    value = date(year, month, 1)
    return value + timedelta(days=(weekday - value.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    value = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return value - timedelta(days=(value.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    adjustment = (32 + 2 * e + 2 * i - h - k) % 7
    month_offset = (h + adjustment - 7 * ((a + 11 * h + 22 * adjustment) // 451) + 114) // 31
    day_of_month = (h + adjustment - 7 * ((a + 11 * h + 22 * adjustment) // 451) + 114) % 31 + 1
    return date(year, month_offset, day_of_month)


def _observed(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _xnys_holidays(year: int) -> set[date]:
    new_year = date(year, 1, 1)
    holidays = {
        new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year,
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


def _is_xnys_regular_session(value: pd.Timestamp) -> bool:
    session = value.date()
    return session.weekday() < 5 and session not in _xnys_holidays(session.year) and session not in EXCEPTIONAL_XNYS_CLOSURES


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


def _is_canonical_session(value: object) -> bool:
    return type(value) is str and len(value) == 10 and value[4] == "-" and value[7] == "-" and value.replace("-", "").isdigit()


def _safely_round_trips(raw: object, numeric: object) -> bool:
    try:
        canonical = float(numeric)
        if isinstance(raw, Integral):
            return int(canonical) == int(raw)
        if isinstance(raw, Decimal):
            return Decimal.from_float(canonical) == raw
        return float(raw) == canonical
    except (OverflowError, TypeError, ValueError):
        return False


def _normalize_prices(prices: object, *, canonical: bool = False) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("prices must be a DataFrame")
    required = ["session", "symbol", PRICE_FIELD]
    if (set(prices.columns) != set(required) if canonical else list(prices.columns) != required):
        _invalid("prices must contain exact columns")
    if not 0 < len(prices) <= MAX_ROWS:
        _invalid("row limit")
    normalized = prices.loc[:, required].copy()
    if canonical:
        if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
            _invalid("invalid canonical envelope")
        if not normalized["session"].map(_is_canonical_session).all():
            _invalid("invalid canonical envelope")
    else:
        normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    normalized["session"] = normalized["session"].map(_normalize_session)
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        _invalid("session precedes requested lower bound")
    if not normalized["session"].map(_is_xnys_regular_session).all():
        _invalid("session is not an XNYS regular session")
    if set(normalized["symbol"]) != set(SYMBOLS) or normalized.duplicated(["session", "symbol"]).any():
        _invalid("each session must contain exactly QQQ and TQQQ")
    if not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")

    adjusted_close = normalized[PRICE_FIELD]
    is_datetime = pd.api.types.is_datetime64_any_dtype(adjusted_close) or pd.api.types.is_timedelta64_dtype(adjusted_close)
    has_datetime_value = adjusted_close.map(lambda value: isinstance(value, (date, datetime, timedelta, pd.Timestamp, pd.Timedelta))).any()
    if is_datetime or has_datetime_value:
        _invalid("datetime-like adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if normalized[PRICE_FIELD].isna().any() or not normalized[PRICE_FIELD].map(math.isfinite).all() or (normalized[PRICE_FIELD] <= 0).any():
        _invalid("adjusted_close must be positive finite")
    if not all(_safely_round_trips(raw, numeric) for raw, numeric in zip(adjusted_close.tolist(), normalized[PRICE_FIELD].tolist())):
        _invalid("adjusted_close cannot safely round-trip through canonical float")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _request_metadata() -> dict[str, object]:
    return {
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
    }


def _rows_from_prices(prices: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {"session": row.session.strftime("%Y-%m-%d"), "symbol": row.symbol, PRICE_FIELD: float(row.adjusted_close)}
        for row in prices.itertuples(index=False)
    ]


def _envelope(prices: pd.DataFrame) -> dict[str, object]:
    envelope: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "request": _request_metadata(),
        "rows": _rows_from_prices(prices),
        "row_count": len(prices),
    }
    envelope["snapshot_identity"] = _sha256(_canonical_bytes(envelope))
    return envelope


def _has_exact_type(value: object, expected: object) -> bool:
    if type(value) is not type(expected):
        return False
    if type(expected) is dict:
        return set(value) == set(expected) and all(_has_exact_type(value[key], expected[key]) for key in expected)
    if type(expected) is list:
        return len(value) == len(expected) and all(_has_exact_type(actual, wanted) for actual, wanted in zip(value, expected))
    return value == expected


def _read_regular_file_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SnapshotValidationError("canonical envelope must be a regular no-follow file") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_SNAPSHOT_BYTES:
            _invalid("canonical envelope must be a bounded regular file")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                _invalid("canonical envelope changed during readback")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            _invalid("canonical envelope changed during readback")
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size) != (after.st_dev, after.st_ino, after.st_size):
            _invalid("canonical envelope changed during readback")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _parse_envelope(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("invalid canonical envelope") from exc
    if type(value) is not dict:
        _invalid("invalid canonical envelope")
    return value


def _validate_envelope(envelope: dict[str, Any]) -> None:
    expected_request = _request_metadata()
    if set(envelope) != {"contract_version", "request", "rows", "row_count", "snapshot_identity"}:
        _invalid("invalid canonical envelope")
    if type(envelope["contract_version"]) is not str or envelope["contract_version"] != CONTRACT_VERSION:
        _invalid("invalid canonical envelope")
    if not _has_exact_type(envelope["request"], expected_request):
        _invalid("invalid canonical envelope")
    if type(envelope["row_count"]) is not int or not 0 < envelope["row_count"] <= MAX_ROWS:
        _invalid("invalid canonical envelope")
    if type(envelope["snapshot_identity"]) is not str or len(envelope["snapshot_identity"]) != 64:
        _invalid("invalid canonical envelope")
    rows = envelope["rows"]
    if type(rows) is not list or len(rows) != envelope["row_count"]:
        _invalid("invalid canonical envelope")
    if any(
        type(row) is not dict
        or set(row) != {"session", "symbol", PRICE_FIELD}
        or type(row["session"]) is not str
        or type(row["symbol"]) is not str
        or type(row[PRICE_FIELD]) is not float
        for row in rows
    ):
        _invalid("invalid canonical envelope")
    prices = _normalize_prices(pd.DataFrame(rows), canonical=True)
    if _rows_from_prices(prices) != rows:
        _invalid("invalid canonical envelope")
    without_identity = dict(envelope)
    received_identity = without_identity.pop("snapshot_identity")
    if _sha256(_canonical_bytes(without_identity)) != received_identity:
        _invalid("invalid canonical envelope identity")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _create_durable_parents(parent: Path) -> None:
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    for directory in reversed(missing):
        directory.mkdir()
        _fsync_directory(directory.parent)
        _fsync_directory(directory)


def _write_staged_envelope(path: Path, raw: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        written = 0
        while written < len(raw):
            written += os.write(descriptor, raw[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def verify_tqqq_r1_snapshot(output_dir: str | Path, *, expected_manifest_sha256: str) -> SnapshotResult:
    output = Path(output_dir)
    try:
        root = output.lstat()
    except OSError as exc:
        raise SnapshotValidationError("unable to read canonical envelope") from exc
    if stat.S_ISLNK(root.st_mode):
        _invalid("root symlink is not allowed")
    if not stat.S_ISDIR(root.st_mode):
        _invalid("canonical envelope layout is required")
    try:
        names = sorted(member.name for member in output.iterdir())
    except OSError as exc:
        raise SnapshotValidationError("unable to read canonical envelope") from exc
    if names != [CANONICAL_FILENAME]:
        _invalid("canonical envelope layout is required")
    raw = _read_regular_file_no_follow(output / CANONICAL_FILENAME)
    if type(expected_manifest_sha256) is not str or _sha256(raw) != expected_manifest_sha256:
        _invalid("trusted canonical envelope hash mismatch")
    envelope = _parse_envelope(raw)
    _validate_envelope(envelope)
    if raw != _canonical_bytes(envelope) + b"\n":
        _invalid("invalid canonical envelope")
    return SnapshotResult(output_dir=output, manifest_sha256=_sha256(raw))


def materialize_tqqq_r1_snapshot(request: object, output_dir: str | Path) -> SnapshotResult:
    """Validate a typed local request and atomically publish one canonical envelope."""
    if type(request) is not TqqqR1SnapshotRequest:
        _invalid("typed request is required; legacy materializer arguments are rejected")
    if type(request.mode) is not str or request.mode != MODE:
        _invalid("mode must be core_only")
    if type(request.plugin) is not str or request.plugin != PLUGIN:
        _invalid("plugin must be ABSENT_DISABLED")
    if type(request.size) is not int or request.size != 0:
        _invalid("size must be zero")
    prices = _normalize_prices(request.prices)
    envelope = _envelope(prices)
    raw = _canonical_bytes(envelope) + b"\n"
    if len(raw) > MAX_SNAPSHOT_BYTES:
        _invalid("canonical envelope exceeds byte limit")
    digest = _sha256(raw)
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    _create_durable_parents(destination.parent)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    installed = False
    try:
        _write_staged_envelope(temporary / CANONICAL_FILENAME, raw)
        _fsync_directory(temporary)
        verify_tqqq_r1_snapshot(temporary, expected_manifest_sha256=digest)
        os.replace(temporary, destination)
        installed = True
        _fsync_directory(destination.parent)
    except Exception as exc:
        if installed:
            try:
                shutil.rmtree(destination)
                _fsync_directory(destination.parent)
            except Exception as rollback_exc:
                raise SnapshotValidationError("publication failed; destination state is unknown") from rollback_exc
            raise SnapshotValidationError("publication failed; destination was rolled back") from exc
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=digest)
