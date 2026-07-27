"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot.

The filesystem contract requires Linux or macOS with POSIX descriptor-relative no-follow support.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
from io import BytesIO
import json
import math
import os
import shutil
import stat
import sys
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
_FILESYSTEM_RUNTIME_ERROR = (
    "TQQQ R1 snapshot filesystem contract requires Linux or macOS POSIX descriptor-anchored no-follow support"
)


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


def _canonical_adjusted_close(value: object) -> int | float:
    if type(value) is not str:
        _invalid("prices.csv contains noncanonical adjusted_close")
    if value in {"True", "False"}:
        _invalid("boolean adjusted_close is not allowed")
    if not value or value.strip() != value:
        _invalid("prices.csv contains noncanonical adjusted_close")
    try:
        integer = int(value)
    except ValueError:
        integer = None
    if integer is not None and str(integer) == value:
        if integer <= 0 or integer > sys.float_info.max:
            _invalid("adjusted_close must be positive finite")
        return integer
    try:
        numeric = float(value)
    except ValueError as exc:
        raise SnapshotValidationError("prices.csv contains noncanonical adjusted_close") from exc
    if not math.isfinite(numeric) or numeric <= 0:
        _invalid("adjusted_close must be positive finite")
    if format(numeric, ".17g") != value:
        _invalid("prices.csv contains noncanonical adjusted_close")
    return numeric


def _normalized_prices(
    prices: pd.DataFrame,
    *,
    require_exact_columns: bool = False,
    require_canonical_symbols: bool = False,
    require_canonical_sessions: bool = False,
    require_canonical_adjusted_close: bool = False,
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
    if not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")

    adjusted_close = normalized[PRICE_FIELD]
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    if pd.api.types.is_datetime64_any_dtype(adjusted_close) or pd.api.types.is_timedelta64_dtype(adjusted_close):
        _invalid("datetime-like adjusted_close is not allowed")
    if require_canonical_adjusted_close:
        normalized[PRICE_FIELD] = pd.Series(
            [_canonical_adjusted_close(value) for value in adjusted_close],
            index=normalized.index,
            dtype=object,
        )
    else:
        if adjusted_close.map(lambda value: type(value) is str).any():
            _invalid("string adjusted_close is not allowed")
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


def _snapshot_member_names(root_fd: int) -> tuple[str, ...]:
    with os.scandir(root_fd) as entries:
        return tuple(sorted(entry.name for entry in entries))


def _require_supported_filesystem_runtime() -> None:
    supported_platform = sys.platform == "darwin" or sys.platform.startswith("linux")
    if (
        os.name != "posix"
        or not supported_platform
        or not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or os.open not in getattr(os, "supports_dir_fd", ())
        or os.stat not in getattr(os, "supports_follow_symlinks", ())
    ):
        _invalid(_FILESYSTEM_RUNTIME_ERROR)


def _read_regular_file_at(root_fd: int, name: str) -> bytes:
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    member_fd = -1
    try:
        member_fd = os.open(name, flags, dir_fd=root_fd)
        before = os.fstat(member_fd)
        if not stat.S_ISREG(before.st_mode):
            _invalid("snapshot members must be regular non-symlink files")
        chunks: list[bytes] = []
        while chunk := os.read(member_fd, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(member_fd)
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    finally:
        if member_fd >= 0:
            os.close(member_fd)
    identity = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, field) != getattr(after, field) for field in identity):
        _invalid("snapshot member identity changed during readback")
    content = b"".join(chunks)
    if len(content) != before.st_size:
        _invalid("snapshot member size changed during readback")
    return content


def _read_snapshot_members(output: Path) -> dict[str, bytes]:
    _require_supported_filesystem_runtime()
    root_fd = -1
    try:
        root_fd = os.open(
            output,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        root_before = os.fstat(root_fd)
        if not stat.S_ISDIR(root_before.st_mode):
            _invalid("snapshot root must be a regular directory")
        names = _snapshot_member_names(root_fd)
        if names != tuple(sorted(OUTPUT_FILENAMES)):
            _invalid(f"unexpected output files: {names}")
        members = {name: _read_regular_file_at(root_fd, name) for name in OUTPUT_FILENAMES}
        if _snapshot_member_names(root_fd) != names:
            _invalid("snapshot members changed during readback")
        root_after = os.fstat(root_fd)
        try:
            path_after = os.stat(output, follow_symlinks=False)
        except OSError as exc:
            raise SnapshotValidationError("snapshot root identity changed during readback") from exc
        if (
            not stat.S_ISDIR(path_after.st_mode)
            or (root_before.st_dev, root_before.st_ino) != (root_after.st_dev, root_after.st_ino)
            or (root_before.st_dev, root_before.st_ino) != (path_after.st_dev, path_after.st_ino)
        ):
            _invalid("snapshot root identity changed during readback")
        return members
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    finally:
        if root_fd >= 0:
            os.close(root_fd)


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    """Verify an immutable snapshot on a supported Linux/macOS filesystem runtime."""
    output = Path(output_dir)
    members = _read_snapshot_members(output)

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
            pd.read_csv(BytesIO(members["prices.csv"]), dtype=str, keep_default_na=False),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
            require_canonical_adjusted_close=True,
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


def _publish_directory_no_clobber(source: Path, destination: Path) -> None:
    at_fdcwd = -100
    rename_noreplace = 0x00000001
    rename_excl = 0x00000004
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    if sys.platform == "darwin" and hasattr(libc, "renamex_np"):
        rename = libc.renamex_np
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, rename_excl)
    elif sys.platform.startswith("linux") and hasattr(libc, "renameat2"):
        rename = libc.renameat2
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(at_fdcwd, source_bytes, at_fdcwd, destination_bytes, rename_noreplace)
    else:
        _invalid("atomic no-clobber publication is unavailable")
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        _invalid(f"immutable output already exists: {destination}")
    raise OSError(error, os.strerror(error), destination)


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    mode: str = MODE,
    plugin: str = PLUGIN,
    size: int = 0,
) -> SnapshotResult:
    """Validate and atomically write the four immutable files on a supported Linux/macOS runtime."""
    _require_supported_filesystem_runtime()
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
        _publish_directory_no_clobber(temporary, destination)
    except Exception:
        try:
            shutil.rmtree(temporary)
        except FileNotFoundError:
            pass
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)
