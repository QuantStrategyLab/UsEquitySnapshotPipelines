"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
import errno
from io import BytesIO
import json
import math
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass
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
_TRUSTED_CALENDAR_SCHEMA = "qsl.r1.xnys.session.v1"
_TRUSTED_ENDPOINT_SCHEMA = "qsl.tqqq.calendar-endpoint-packet.v1"
_TRUSTED_RUNTIME_SCHEMA = "qsl.tqqq.runtime-source-anchor.v1"
_TRUSTED_MANIFEST_SCHEMA = "qsl.tqqq.calendar-endpoint-trusted-snapshot.v1"
_TRUSTED_COMPLETE_SCHEMA = "qsl.tqqq.snapshot-completion.v1"
_TRUSTED_CONTRACT_VERSION = "tqqq_r1_calendar_endpoint_trusted_snapshot.v1"
_QQQ_FIRST_SESSION = "2010-01-04"
_TQQQ_FIRST_USABLE_SESSION = "2010-02-11"
_CALENDAR_MAX_BYTES = 8 * 1024 * 1024
_JSON_MAX_BYTES = 1024 * 1024
_PRICES_MAX_BYTES = 64 * 1024 * 1024
_TRUSTED_OUTPUT_FILENAMES = ("COMPLETE.json", "manifest.json", "prices.csv")


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
    require_session_symbol_pairs: bool = True,
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
    if require_session_symbol_pairs and not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
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
) -> SnapshotResult:
    output = Path(output_dir)
    if output.is_symlink():
        _invalid("snapshot root symlink is not allowed")
    try:
        names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        _invalid(f"unexpected output files: {names}")
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in OUTPUT_FILENAMES):
        _invalid("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in OUTPUT_FILENAMES}
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
    return SnapshotResult(output_dir=output, manifest_sha256=member_hashes["manifest.json"])


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
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
    normalized = _normalized_prices(prices)
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
        verify_tqqq_r1_snapshot(temporary, expected_manifest_sha256=manifest_sha256)
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _digest_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _is_hex(value: object, length: int) -> bool:
    return type(value) is str and len(value) == length and all(character in "0123456789abcdef" for character in value)


def _canonical_json_bytes(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_regular_bytes_once(path: str | Path, *, limit: int, failure: str) -> bytes:
    required_flags = ("O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_NONBLOCK")
    if any(not hasattr(os, flag) for flag in required_flags):
        _invalid(failure)
    try:
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            _invalid(failure)
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError(failure) from exc
    try:
        opened = os.fstat(fd)
        identity = (before.st_dev, before.st_ino, before.st_mode, before.st_nlink, before.st_size, before.st_mtime_ns)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_nlink, opened.st_size, opened.st_mtime_ns) != identity
        ):
            _invalid(failure)
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                _invalid(failure)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1):
            _invalid(failure)
        after = os.fstat(fd)
        if (after.st_dev, after.st_ino, after.st_mode, after.st_nlink, after.st_size, after.st_mtime_ns) != identity:
            _invalid(failure)
        return b"".join(chunks)
    except OSError as exc:
        raise SnapshotValidationError(failure) from exc
    finally:
        os.close(fd)


def _parse_exact_object(raw: bytes, failure: str) -> dict[str, Any]:
    try:
        return _parse_json_object(raw, "trusted object")
    except SnapshotValidationError as exc:
        raise SnapshotValidationError(failure) from exc


def _parse_canonical_utc(value: object, failure: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _invalid(failure)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise SnapshotValidationError(failure) from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        _invalid(failure)
    return parsed


def _parse_calendar(raw: bytes) -> list[dict[str, object]]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise SnapshotValidationError("CALENDAR_SCHEMA_INVALID") from exc
    if not lines:
        _invalid("CALENDAR_SCHEMA_INVALID")
    rows: list[dict[str, object]] = []
    previous = ""
    for line in lines:
        row = _parse_exact_object(line.encode("utf-8"), "CALENDAR_SCHEMA_INVALID")
        if set(row) != {"schema", "session_date", "open_utc", "close_utc", "early_close"}:
            _invalid("CALENDAR_SCHEMA_INVALID")
        session = row["session_date"]
        if not _is_canonical_session(session) or session <= previous:
            _invalid("CALENDAR_SCHEMA_INVALID")
        try:
            date = datetime.strptime(session, "%Y-%m-%d").date()
        except ValueError as exc:
            raise SnapshotValidationError("CALENDAR_SCHEMA_INVALID") from exc
        opened = _parse_canonical_utc(row["open_utc"], "CALENDAR_SCHEMA_INVALID")
        closed = _parse_canonical_utc(row["close_utc"], "CALENDAR_SCHEMA_INVALID")
        if row["schema"] != _TRUSTED_CALENDAR_SCHEMA or type(row["early_close"]) is not bool or opened >= closed:
            _invalid("CALENDAR_SCHEMA_INVALID")
        if opened.date() != date or closed.date() != date:
            _invalid("CALENDAR_SCHEMA_INVALID")
        rows.append(row)
        previous = session
    if rows[0]["session_date"] != _QQQ_FIRST_SESSION:
        _invalid("CALENDAR_SCHEMA_INVALID")
    return rows


def _validate_runtime_anchor(raw: bytes, expected_digest: str) -> None:
    anchor = _parse_exact_object(raw, "RUNTIME_SOURCE_IDENTITY_MISMATCH")
    keys = {"schema", "repo", "ref", "commit", "parent", "tree", "verified_at_utc", "files", "authority_event_id"}
    paths = {
        "src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py": Path(__file__),
        "src/us_equity_snapshot_pipelines/yfinance_prices.py": Path(__file__).with_name("yfinance_prices.py"),
    }
    if set(anchor) != keys or anchor["schema"] != _TRUSTED_RUNTIME_SCHEMA:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    if anchor["repo"] != "QuantStrategyLab/UsEquitySnapshotPipelines" or anchor["ref"] != "refs/heads/main":
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    if not all(_is_hex(anchor[key], 40) for key in ("commit", "parent", "tree")):
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    _parse_canonical_utc(anchor["verified_at_utc"], "RUNTIME_SOURCE_IDENTITY_MISMATCH")
    files = anchor["files"]
    if type(files) is not dict or set(files) != set(paths) or not _is_hex(expected_digest, 64):
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    for name, source in paths.items():
        metadata = files[name]
        if type(metadata) is not dict or set(metadata) != {"git_blob_sha1", "content_sha256", "size_bytes"}:
            _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
        raw_source = _read_regular_bytes_once(source, limit=_JSON_MAX_BYTES, failure="RUNTIME_SOURCE_IDENTITY_MISMATCH")
        if not _is_hex(metadata["git_blob_sha1"], 40) or metadata["content_sha256"] != _digest_bytes(raw_source):
            _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
        if type(metadata["size_bytes"]) is not int or metadata["size_bytes"] != len(raw_source):
            _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")


def _trusted_prices(prices: pd.DataFrame, completed_sessions: list[str]) -> pd.DataFrame:
    normalized = _normalized_prices(
        prices,
        require_exact_columns=True,
        require_canonical_symbols=True,
        require_canonical_sessions=True,
        require_session_symbol_pairs=False,
    )
    expected = {"QQQ": completed_sessions, "TQQQ": [day for day in completed_sessions if day >= _TQQQ_FIRST_USABLE_SESSION]}
    for symbol, sessions in expected.items():
        actual = normalized.loc[normalized["symbol"].eq(symbol), "session"].dt.strftime("%Y-%m-%d").tolist()
        if actual != sessions:
            _invalid("EXACT_SESSION_SET_MISMATCH")
    if _TQQQ_FIRST_USABLE_SESSION not in expected["TQQQ"]:
        _invalid("EXACT_SESSION_SET_MISMATCH")
    return normalized


def _open_private_root(output_root: str | Path) -> tuple[int, os.stat_result]:
    if any(not hasattr(os, flag) for flag in ("O_RDONLY", "O_CLOEXEC", "O_NOFOLLOW", "O_DIRECTORY")):
        _invalid("IMMUTABLE_CREATE_CONFLICT")
    try:
        fd = os.open(output_root, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY)
        info = os.fstat(fd)
    except OSError as exc:
        raise SnapshotValidationError("IMMUTABLE_CREATE_CONFLICT") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        os.close(fd)
        _invalid("IMMUTABLE_CREATE_CONFLICT")
    return fd, info


def _write_new_member(directory_fd: int, name: str, raw: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
        try:
            written = 0
            while written < len(raw):
                written += os.write(fd, raw[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise SnapshotValidationError("IMMUTABLE_CREATE_CONFLICT") from exc


def _read_member(directory_fd: int, name: str, *, limit: int) -> bytes:
    try:
        before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            _invalid("STRICT_READBACK_FAILED")
        fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    try:
        opened = os.fstat(fd)
        identity = (before.st_dev, before.st_ino, before.st_mode, before.st_nlink, before.st_size, before.st_mtime_ns)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_nlink, opened.st_size, opened.st_mtime_ns) != identity
        ):
            _invalid("STRICT_READBACK_FAILED")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                _invalid("STRICT_READBACK_FAILED")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1):
            _invalid("STRICT_READBACK_FAILED")
        after = os.fstat(fd)
        if (after.st_dev, after.st_ino, after.st_mode, after.st_nlink, after.st_size, after.st_mtime_ns) != identity:
            _invalid("STRICT_READBACK_FAILED")
        return b"".join(chunks)
    except OSError as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    finally:
        os.close(fd)


def _remove_incomplete_package(root_fd: int, name: str, identity: tuple[int, int]) -> None:
    """Best-effort cleanup limited to the directory this invocation created."""
    try:
        current = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != identity:
            return
        directory_fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY, dir_fd=root_fd)
        try:
            members = tuple(os.listdir(directory_fd))
            if any(member not in _TRUSTED_OUTPUT_FILENAMES for member in members):
                return
            for member in members:
                info = os.stat(member, dir_fd=directory_fd, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    return
            for member in members:
                os.unlink(member, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)
        os.rmdir(name, dir_fd=root_fd)
    except OSError:
        return


def _validate_persisted_snapshot(manifest: dict[str, Any], prices_raw: bytes) -> None:
    fixed = {
        "schema": _TRUSTED_MANIFEST_SCHEMA,
        "contract_version": _TRUSTED_CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "qqq_first_session": _QQQ_FIRST_SESSION,
        "tqqq_first_usable_session": _TQQQ_FIRST_USABLE_SESSION,
        "price_field": PRICE_FIELD,
        "mode": MODE,
        "plugin": PLUGIN,
        "size": 0,
    }
    if any(manifest.get(key) != value or type(manifest.get(key)) is not type(value) for key, value in fixed.items()):
        _invalid("STRICT_READBACK_FAILED")
    if (
        type(manifest.get("row_count")) is not int
        or type(manifest.get("completed_session_count")) is not int
        or manifest["row_count"] <= 0
        or manifest["completed_session_count"] <= 0
        or not _is_canonical_session(manifest.get("required_last_completed_session"))
    ):
        _invalid("STRICT_READBACK_FAILED")
    try:
        prices = pd.read_csv(BytesIO(prices_raw))
        normalized = _normalized_prices(
            prices,
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
            require_session_symbol_pairs=False,
        )
    except (SnapshotValidationError, UnicodeDecodeError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    qqq_sessions = normalized.loc[normalized["symbol"].eq("QQQ"), "session"].dt.strftime("%Y-%m-%d").tolist()
    tqqq_sessions = normalized.loc[normalized["symbol"].eq("TQQQ"), "session"].dt.strftime("%Y-%m-%d").tolist()
    if (
        len(normalized) != manifest["row_count"]
        or len(qqq_sessions) != manifest["completed_session_count"]
        or not qqq_sessions
        or qqq_sessions[0] != _QQQ_FIRST_SESSION
        or qqq_sessions[-1] != manifest["required_last_completed_session"]
        or not tqqq_sessions
        or any(session < _TQQQ_FIRST_USABLE_SESSION or session not in qqq_sessions for session in tqqq_sessions)
    ):
        _invalid("STRICT_READBACK_FAILED")


def _validate_endpoint(raw: bytes, calendar: list[dict[str, object]], calendar_sha256: str, runtime_sha256: str) -> tuple[list[str], str]:
    packet = _parse_exact_object(raw, "CALENDAR_ENDPOINT_PACKET_INVALID")
    keys = {
        "schema", "venue", "calendar_request_floor", "required_first_session", "required_last_completed_session",
        "required_last_completed_close_utc", "next_session", "next_session_close_utc", "endpoint_observed_at_utc",
        "completed_session_count", "calendar_evidence_session_count", "calendar_evidence_sha256", "runtime_anchor_sha256",
        "authority_event_id",
    }
    if set(packet) != keys or packet["schema"] != _TRUSTED_ENDPOINT_SCHEMA or packet["venue"] != "XNYS":
        _invalid("CALENDAR_ENDPOINT_PACKET_INVALID")
    if packet["calendar_request_floor"] != REQUESTED_LOWER_BOUND or packet["required_first_session"] != _QQQ_FIRST_SESSION:
        _invalid("CALENDAR_ENDPOINT_PACKET_INVALID")
    count = packet["completed_session_count"]
    if type(count) is not int or count <= 0 or packet["calendar_evidence_session_count"] != count + 1 or len(calendar) != count + 1:
        _invalid("CALENDAR_SUCCESSOR_EVIDENCE_MISSING")
    completed, successor = calendar[:count], calendar[count]
    if (
        packet["required_last_completed_session"] != completed[-1]["session_date"]
        or packet["required_last_completed_close_utc"] != completed[-1]["close_utc"]
        or packet["next_session"] != successor["session_date"]
        or packet["next_session_close_utc"] != successor["close_utc"]
        or packet["calendar_evidence_sha256"] != calendar_sha256
        or packet["runtime_anchor_sha256"] != runtime_sha256
    ):
        _invalid("CALENDAR_ENDPOINT_PACKET_INVALID")
    observed = _parse_canonical_utc(packet["endpoint_observed_at_utc"], "CALENDAR_ENDPOINT_PACKET_INVALID")
    last_close = _parse_canonical_utc(packet["required_last_completed_close_utc"], "CALENDAR_ENDPOINT_PACKET_INVALID")
    next_close = _parse_canonical_utc(packet["next_session_close_utc"], "CALENDAR_ENDPOINT_PACKET_INVALID")
    now = _utc_now()
    if now.tzinfo is None or now.utcoffset() is None or not (last_close <= observed <= now < next_close):
        _invalid("CALENDAR_ENDPOINT_STALE_AT_OBSERVATION")
    return [row["session_date"] for row in completed], packet["required_last_completed_session"]


def materialize_tqqq_calendar_endpoint_trusted_snapshot(
    prices: pd.DataFrame,
    output_root: str | Path,
    *,
    calendar_path: str | Path,
    endpoint_packet_path: str | Path,
    runtime_anchor_path: str | Path,
    expected_calendar_sha256: object,
    expected_endpoint_packet_sha256: object,
    expected_runtime_source_identity_sha256: object,
) -> SnapshotResult:
    """Materialize a size-zero snapshot only from externally digested trusted inputs."""
    if not all(_is_hex(value, 64) for value in (expected_calendar_sha256, expected_endpoint_packet_sha256, expected_runtime_source_identity_sha256)):
        _invalid("CALENDAR_ENDPOINT_PACKET_INVALID")
    calendar_raw = _read_regular_bytes_once(calendar_path, limit=_CALENDAR_MAX_BYTES, failure="CALENDAR_SCHEMA_INVALID")
    runtime_raw = _read_regular_bytes_once(runtime_anchor_path, limit=_JSON_MAX_BYTES, failure="RUNTIME_SOURCE_IDENTITY_MISMATCH")
    endpoint_raw = _read_regular_bytes_once(endpoint_packet_path, limit=_JSON_MAX_BYTES, failure="CALENDAR_ENDPOINT_PACKET_INVALID")
    if _digest_bytes(calendar_raw) != expected_calendar_sha256:
        _invalid("CALENDAR_SCHEMA_INVALID")
    if _digest_bytes(runtime_raw) != expected_runtime_source_identity_sha256:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    if _digest_bytes(endpoint_raw) != expected_endpoint_packet_sha256:
        _invalid("CALENDAR_ENDPOINT_PACKET_INVALID")
    calendar = _parse_calendar(calendar_raw)
    _validate_runtime_anchor(runtime_raw, expected_runtime_source_identity_sha256)
    completed_sessions, last_session = _validate_endpoint(
        endpoint_raw, calendar, expected_calendar_sha256, expected_runtime_source_identity_sha256
    )
    normalized = _trusted_prices(prices, completed_sessions)
    prices_bytes = normalized.assign(session=normalized["session"].dt.strftime("%Y-%m-%d")).to_csv(
        index=False, lineterminator="\n", float_format="%.17g"
    ).encode("utf-8")
    manifest: dict[str, object] = {
        "schema": _TRUSTED_MANIFEST_SCHEMA,
        "contract_version": _TRUSTED_CONTRACT_VERSION,
        "symbols": list(SYMBOLS), "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "qqq_first_session": _QQQ_FIRST_SESSION, "tqqq_first_usable_session": _TQQQ_FIRST_USABLE_SESSION,
        "price_field": PRICE_FIELD, "mode": MODE, "plugin": PLUGIN, "size": 0, "row_count": len(normalized),
        "prices_sha256": _digest_bytes(prices_bytes), "calendar_evidence_sha256": expected_calendar_sha256,
        "runtime_anchor_sha256": expected_runtime_source_identity_sha256, "endpoint_packet_sha256": expected_endpoint_packet_sha256,
        "required_last_completed_session": last_session, "completed_session_count": len(completed_sessions),
    }
    manifest_bytes = _canonical_json_bytes(manifest)
    manifest_sha256 = _digest_bytes(manifest_bytes)
    root_fd, root_info = _open_private_root(output_root)
    package_name = f"sha256-{manifest_sha256}"
    package_identity: tuple[int, int] | None = None
    try:
        try:
            os.mkdir(package_name, mode=0o700, dir_fd=root_fd)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                _invalid("IMMUTABLE_CREATE_CONFLICT")
            raise SnapshotValidationError("IMMUTABLE_CREATE_CONFLICT") from exc
        package_fd = os.open(package_name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY, dir_fd=root_fd)
        try:
            package_info = os.fstat(package_fd)
            if not stat.S_ISDIR(package_info.st_mode) or package_info.st_uid != os.getuid() or stat.S_IMODE(package_info.st_mode) != 0o700:
                _invalid("IMMUTABLE_CREATE_CONFLICT")
            package_identity = (package_info.st_dev, package_info.st_ino)
            _write_new_member(package_fd, "prices.csv", prices_bytes)
            _write_new_member(package_fd, "manifest.json", manifest_bytes)
            if (
                _read_member(package_fd, "prices.csv", limit=_PRICES_MAX_BYTES) != prices_bytes
                or _read_member(package_fd, "manifest.json", limit=_JSON_MAX_BYTES) != manifest_bytes
            ):
                _invalid("STRICT_READBACK_FAILED")
            _write_new_member(package_fd, "COMPLETE.json", _canonical_json_bytes({"schema": _TRUSTED_COMPLETE_SCHEMA, "manifest_sha256": manifest_sha256}))
            os.fsync(package_fd)
            os.fsync(root_fd)
        finally:
            os.close(package_fd)
        if os.fstat(root_fd).st_ino != root_info.st_ino or os.fstat(root_fd).st_dev != root_info.st_dev:
            _invalid("IMMUTABLE_CREATE_CONFLICT")
        return verify_tqqq_calendar_endpoint_trusted_snapshot(output_root, expected_manifest_sha256=manifest_sha256)
    except Exception:
        if package_identity is not None:
            _remove_incomplete_package(root_fd, package_name, package_identity)
        raise
    finally:
        os.close(root_fd)


def verify_tqqq_calendar_endpoint_trusted_snapshot(output_root: str | Path, *, expected_manifest_sha256: object) -> SnapshotResult:
    if not _is_hex(expected_manifest_sha256, 64):
        _invalid("STRICT_READBACK_FAILED")
    root_fd, _ = _open_private_root(output_root)
    package_name = f"sha256-{expected_manifest_sha256}"
    try:
        package_fd = os.open(package_name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY, dir_fd=root_fd)
        try:
            names = tuple(sorted(os.listdir(package_fd)))
            if names != _TRUSTED_OUTPUT_FILENAMES:
                _invalid("STRICT_READBACK_FAILED")
            members = {name: _read_member(package_fd, name, limit=_PRICES_MAX_BYTES if name == "prices.csv" else _JSON_MAX_BYTES) for name in names}
        finally:
            os.close(package_fd)
    except OSError as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    finally:
        os.close(root_fd)
    if _digest_bytes(members["manifest.json"]) != expected_manifest_sha256:
        _invalid("STRICT_READBACK_FAILED")
    manifest = _parse_exact_object(members["manifest.json"], "STRICT_READBACK_FAILED")
    complete = _parse_exact_object(members["COMPLETE.json"], "STRICT_READBACK_FAILED")
    required = {
        "schema", "contract_version", "symbols", "requested_lower_bound", "qqq_first_session", "tqqq_first_usable_session",
        "price_field", "mode", "plugin", "size", "row_count", "prices_sha256", "calendar_evidence_sha256", "runtime_anchor_sha256",
        "endpoint_packet_sha256", "required_last_completed_session", "completed_session_count",
    }
    if set(manifest) != required or manifest.get("schema") != _TRUSTED_MANIFEST_SCHEMA or manifest.get("contract_version") != _TRUSTED_CONTRACT_VERSION:
        _invalid("STRICT_READBACK_FAILED")
    if complete != {"schema": _TRUSTED_COMPLETE_SCHEMA, "manifest_sha256": expected_manifest_sha256}:
        _invalid("STRICT_READBACK_FAILED")
    if not all(_is_hex(manifest.get(key), 64) for key in ("prices_sha256", "calendar_evidence_sha256", "runtime_anchor_sha256", "endpoint_packet_sha256")):
        _invalid("STRICT_READBACK_FAILED")
    if manifest["prices_sha256"] != _digest_bytes(members["prices.csv"]):
        _invalid("STRICT_READBACK_FAILED")
    _validate_persisted_snapshot(manifest, members["prices.csv"])
    return SnapshotResult(output_dir=Path(output_root) / package_name, manifest_sha256=expected_manifest_sha256)
