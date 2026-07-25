"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import hashlib
from io import BytesIO
import json
import math
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")
BOUND_OUTPUT_FILENAMES = ("COMPLETE", *OUTPUT_FILENAMES)
CALENDAR_SCHEMA = "qsl.r1.xnys.session.v1"
E4_SCHEMA = "qsl.tqqq.r1.e4.endpoint-observation.v1"
TQQQ_FIRST_USABLE_SESSION = "2010-02-11"


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_e4_observation(
    observation: dict[str, object],
    *,
    calendar_sha256: str,
    completed_last_session: str,
    successor_session: str,
    successor_close_utc: str,
    e3_digest: str,
) -> None:
    expected = {
        "schema": E4_SCHEMA,
        "calendar_sha256": calendar_sha256,
        "completed_last_session": completed_last_session,
        "successor_session": successor_session,
        "successor_close_utc": successor_close_utc,
        "e3_digest": e3_digest,
    }
    if type(observation) is not dict or set(observation) != set(expected) | {"endpoint_observed_at_utc"} or any(
        observation[key] != value for key, value in expected.items()
    ):
        _invalid("invalid E4 endpoint observation")
    try:
        observed = datetime.strptime(str(observation["endpoint_observed_at_utc"]), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise SnapshotValidationError("invalid E4 endpoint observation") from exc
    if observed > _utc_now():
        _invalid("future E4 endpoint observation")


def _read_calendar_bytes(path: str | Path, *, expected_sha256: str) -> bytes:
    """Read a calendar only through a stable, single-link regular-file descriptor."""
    candidate = Path(path)
    try:
        before = os.lstat(candidate)
    except OSError as exc:
        raise SnapshotValidationError("calendar artifact is unavailable") from exc
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        _invalid("calendar artifact must be a single-link regular file")
    try:
        fd = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise SnapshotValidationError("calendar artifact cannot be opened safely") from exc
    try:
        opened = os.fstat(fd)
        identity = (before.st_dev, before.st_ino, before.st_size, stat.S_IFMT(before.st_mode))
        if identity != (opened.st_dev, opened.st_ino, opened.st_size, stat.S_IFMT(opened.st_mode)) or opened.st_nlink != 1:
            _invalid("calendar artifact identity changed before read")
        chunks: list[bytes] = []
        while chunk := os.read(fd, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(fd)
        if identity != (after.st_dev, after.st_ino, after.st_size, stat.S_IFMT(after.st_mode)) or after.st_nlink != 1:
            _invalid("calendar artifact identity changed during read")
    finally:
        os.close(fd)
    raw = b"".join(chunks)
    if type(expected_sha256) is not str or hashlib.sha256(raw).hexdigest() != expected_sha256:
        _invalid("calendar artifact hash mismatch")
    return raw


def _validated_calendar(path: str | Path, expected_sha256: str) -> tuple[tuple[str, ...], str]:
    raw = _read_calendar_bytes(path, expected_sha256=expected_sha256)
    try:
        rows = [_parse_json_object(line, "calendar row") for line in raw.splitlines()]
    except SnapshotValidationError:
        raise
    if len(rows) != 4165:
        _invalid("calendar must contain exactly 4165 rows")
    dates: list[str] = []
    for row in rows:
        if set(row) != {"schema", "session_date", "open_utc", "close_utc", "early_close"} or row.get("schema") != CALENDAR_SCHEMA:
            _invalid("invalid calendar schema")
        if type(row["session_date"]) is not str or type(row["open_utc"]) is not str or type(row["close_utc"]) is not str or type(row["early_close"]) is not bool:
            _invalid("invalid calendar schema")
        try:
            date = datetime.strptime(row["session_date"], "%Y-%m-%d").date()
            opened = datetime.strptime(row["open_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            closed = datetime.strptime(row["close_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise SnapshotValidationError("invalid calendar time") from exc
        if date.weekday() >= 5 or opened >= closed or opened.date() != date or closed.date() != date:
            _invalid("calendar holiday or time fields are inconsistent")
        dates.append(row["session_date"])
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        _invalid("calendar rows are missing, duplicate, extra, or out of order")
    successor = dates[-1]
    if (pd.Timestamp(successor) - pd.Timestamp(dates[-2])).days not in (1, 3):
        _invalid("calendar successor is not adjacent")
    return tuple(dates[:-1]), successor


def _require_calendar_coverage(prices: pd.DataFrame, completed_dates: tuple[str, ...]) -> None:
    observed = {
        symbol: tuple(prices.loc[prices["symbol"].eq(symbol), "session"].dt.strftime("%Y-%m-%d"))
        for symbol in SYMBOLS
    }
    if observed["QQQ"] != completed_dates or observed["TQQQ"] != tuple(date for date in completed_dates if date >= TQQQ_FIRST_USABLE_SESSION):
        _invalid("prices must exactly cover calendar-bound QQQ and TQQQ sessions")


def _normalize_session(value: object) -> pd.Timestamp:
    if pd.api.types.is_bool(value):
        _invalid("invalid session")
    try:
        session = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid session") from exc
    if pd.isna(session):
        _invalid("invalid session")
    if session.tz is not None:
        _invalid("timezone-aware session is not allowed")
    return session.normalize()


def _is_canonical_session(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:].isdigit()
    )


def _normalized_prices(
    prices: pd.DataFrame,
    *,
    require_exact_columns: bool = False,
    require_canonical_symbols: bool = False,
    require_canonical_sessions: bool = False,
    allow_asymmetric_symbols: bool = False,
) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("prices must be a DataFrame")
    required = ("session", "symbol", PRICE_FIELD)
    if any(list(prices.columns).count(column) != 1 for column in required):
        _invalid("required columns must appear exactly once")
    missing = set(required).difference(prices.columns)
    if missing:
        _invalid(f"missing required columns: {', '.join(sorted(missing))}")
    if require_exact_columns and list(prices.columns) != list(required):
        _invalid("prices.csv must contain exact columns")

    normalized = prices.loc[:, list(required)].copy()
    if require_canonical_symbols:
        if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
            _invalid("prices.csv contains noncanonical symbol")
    else:
        normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    received = set(normalized["symbol"])
    if received != set(SYMBOLS):
        missing_symbols = sorted(set(SYMBOLS).difference(received))
        unexpected_symbols = sorted(received.difference(SYMBOLS))
        _invalid(f"missing required symbol or unexpected symbol: missing={missing_symbols}, unexpected={unexpected_symbols}")

    raw_sessions = normalized["session"]
    normalized["session"] = raw_sessions.map(_normalize_session)
    if require_canonical_sessions and not raw_sessions.map(_is_canonical_session).all():
        _invalid("prices.csv contains noncanonical session")
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        _invalid("session precedes requested lower bound")
    if (normalized["session"].dt.dayofweek >= 5).any():
        _invalid("observed session must be a weekday")
    if normalized.duplicated(["session", "symbol"]).any():
        _invalid("duplicate session for symbol")
    if not allow_asymmetric_symbols and not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")

    adjusted_close = normalized[PRICE_FIELD]
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    if pd.api.types.is_datetime64_any_dtype(adjusted_close) or pd.api.types.is_timedelta64_dtype(adjusted_close):
        _invalid("datetime-like adjusted_close is not allowed")
    normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if (
        normalized[PRICE_FIELD].isna().any()
        or not normalized[PRICE_FIELD].map(math.isfinite).all()
        or (normalized[PRICE_FIELD] <= 0).any()
    ):
        _invalid("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _write_prices(path: Path, prices: pd.DataFrame) -> None:
    output = prices.copy()
    output["session"] = output["session"].dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.17g")


def _parse_json_object(raw: bytes, name: str) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if type(value) is not dict:
        _invalid(f"invalid {name}")
    return value


def _has_exact_type(value: object, expected: object) -> bool:
    if type(value) is not type(expected):
        return False
    if type(expected) is dict:
        return set(value) == set(expected) and all(_has_exact_type(value[key], expected[key]) for key in expected)
    if type(expected) is list:
        return len(value) == len(expected) and all(_has_exact_type(actual, wanted) for actual, wanted in zip(value, expected))
    return value == expected


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    calendar_path: str | Path | None = None,
    expected_calendar_sha256: str | None = None,
) -> SnapshotResult:
    output = Path(output_dir)
    output_filenames = BOUND_OUTPUT_FILENAMES if calendar_path is not None else OUTPUT_FILENAMES
    if output.is_symlink():
        _invalid("snapshot root symlink is not allowed")
    try:
        names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if names != tuple(sorted(output_filenames)):
        _invalid(f"unexpected output files: {names}")
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in output_filenames):
        _invalid("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in output_filenames}
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc

    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if type(expected_manifest_sha256) is not str or member_hashes["manifest.json"] != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")

    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    manifest = _parse_json_object(members["manifest.json"], "manifest")
    validation = _parse_json_object(members["validation.json"], "validation")
    if set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        _invalid("invalid sha256sums")
    for name, expected in sums.items():
        if type(expected) is not str or member_hashes[name] != expected:
            _invalid(f"hash mismatch: {name}")

    try:
        prices = _normalized_prices(
            pd.read_csv(BytesIO(members["prices.csv"])),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
            allow_asymmetric_symbols=calendar_path is not None,
        )
    except (UnicodeDecodeError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("invalid prices.csv") from exc

    expected_manifest = {
        "contract_version": CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": member_hashes["prices.csv"],
    }
    if not _has_exact_type(manifest, expected_manifest):
        _invalid("invalid manifest")
    expected_validation = {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)}
    if not _has_exact_type(validation, expected_validation):
        _invalid("invalid validation")
    if (calendar_path is None) != (expected_calendar_sha256 is None):
        _invalid("calendar path and expected hash must be supplied together")
    if calendar_path is not None:
        completed_dates, _ = _validated_calendar(calendar_path, expected_calendar_sha256)
        _require_calendar_coverage(prices, completed_dates)
        if output.name != expected_manifest_sha256 or members["COMPLETE"] != f"{expected_manifest_sha256}\n".encode("ascii"):
            _invalid("invalid immutable completion marker")
    return SnapshotResult(output_dir=output, manifest_sha256=member_hashes["manifest.json"])


def _write_exclusive(path: Path, payload: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(payload)
        while view:
            view = view[os.write(fd, view) :]
        os.fsync(fd)
    finally:
        os.close(fd)


def _owned_directory_cleanup(path: Path, identity: tuple[int, int]) -> bool:
    try:
        current = os.lstat(path)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != identity:
            return False
        children = tuple(path.iterdir())
        if any(child.name not in BOUND_OUTPUT_FILENAMES or child.is_symlink() or not child.is_file() for child in children):
            return False
        for child in children:
            child.unlink()
        path.rmdir()
        return True
    except OSError:
        return False


def _materialize_calendar_bound(
    prices: pd.DataFrame,
    output_root: Path,
    *,
    calendar_path: str | Path,
    expected_calendar_sha256: str,
) -> SnapshotResult:
    """Publish calendar-bound bytes through a digest-addressed exclusive directory."""
    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".tqqq-r1-staging.", dir=output_root.parent))
    try:
        _write_prices(staging / "prices.csv", prices)
        _write_json(staging / "validation.json", {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)})
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "symbols": list(SYMBOLS),
            "requested_lower_bound": REQUESTED_LOWER_BOUND,
            "price_field": PRICE_FIELD,
            "plugin": PLUGIN,
            "mode": MODE,
            "size": 0,
            "row_count": len(prices),
            "prices_sha256": _sha256(staging / "prices.csv"),
        }
        _write_json(staging / "manifest.json", manifest)
        _write_json(staging / "sha256sums.json", {name: _sha256(staging / name) for name in OUTPUT_FILENAMES if name != "sha256sums.json"})
        manifest_sha256 = _sha256(staging / "manifest.json")
        payloads = {name: (staging / name).read_bytes() for name in OUTPUT_FILENAMES}
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / manifest_sha256
    for attempt in range(2):
        try:
            os.mkdir(destination, 0o700)
        except FileExistsError as exc:
            raise SnapshotValidationError(f"immutable output already exists: {destination}") from exc
        created = os.lstat(destination)
        identity = (created.st_dev, created.st_ino)
        try:
            for name in OUTPUT_FILENAMES:
                _write_exclusive(destination / name, payloads[name])
            _write_exclusive(destination / "COMPLETE", f"{manifest_sha256}\n".encode("ascii"))
            result = verify_tqqq_r1_snapshot(destination, expected_manifest_sha256=manifest_sha256, calendar_path=calendar_path, expected_calendar_sha256=expected_calendar_sha256)
            current = os.lstat(destination)
            if (current.st_dev, current.st_ino) != identity:
                _invalid("publication directory identity changed")
            return result
        except OSError as exc:
            if attempt == 0 and _owned_directory_cleanup(destination, identity):
                continue
            raise SnapshotValidationError("immutable publication failed") from exc
        except Exception:
            _owned_directory_cleanup(destination, identity)
            raise
    _invalid("immutable publication retry exhausted")


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    calendar_path: str | Path | None = None,
    expected_calendar_sha256: str | None = None,
    mode: str = MODE,
    plugin: str = PLUGIN,
    size: int = 0,
) -> SnapshotResult:
    """Validate fixture/local input and atomically write the four immutable contract files."""
    if mode != MODE:
        _invalid("mode must be core_only")
    if plugin != PLUGIN:
        _invalid("plugin must be ABSENT_DISABLED")
    if size != 0:
        _invalid("size must be zero")
    normalized = _normalized_prices(prices, allow_asymmetric_symbols=calendar_path is not None)
    if (calendar_path is None) != (expected_calendar_sha256 is None):
        _invalid("calendar path and expected hash must be supplied together")
    if calendar_path is not None:
        completed_dates, _ = _validated_calendar(calendar_path, expected_calendar_sha256)
        _require_calendar_coverage(normalized, completed_dates)
        return _materialize_calendar_bound(
            normalized,
            Path(output_dir),
            calendar_path=calendar_path,
            expected_calendar_sha256=expected_calendar_sha256,
        )
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        prices_path = temporary / "prices.csv"
        _write_prices(prices_path, normalized)
        validation = {"valid": True, "row_count": len(normalized), "symbols": list(SYMBOLS)}
        _write_json(temporary / "validation.json", validation)
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "symbols": list(SYMBOLS),
            "requested_lower_bound": REQUESTED_LOWER_BOUND,
            "price_field": PRICE_FIELD,
            "plugin": PLUGIN,
            "mode": MODE,
            "size": 0,
            "row_count": len(normalized),
            "prices_sha256": _sha256(prices_path),
        }
        _write_json(temporary / "manifest.json", manifest)
        _write_json(
            temporary / "sha256sums.json",
            {name: _sha256(temporary / name) for name in ("prices.csv", "manifest.json", "validation.json")},
        )
        manifest_sha256 = _sha256(temporary / "manifest.json")
        verify_tqqq_r1_snapshot(
            temporary,
            expected_manifest_sha256=manifest_sha256,
            calendar_path=calendar_path,
            expected_calendar_sha256=expected_calendar_sha256,
        )
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)
