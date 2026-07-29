"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import json
import math
import os
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass, field
from io import BytesIO
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
_MAX_FLOAT_INTEGER_DECIMAL = format(sys.float_info.max, ".0f")


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str
    verified_members: tuple[tuple[str, bytes], ...] = field(default=(), repr=False, compare=False)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _admit_raw_numeric(value: object) -> None:
    """Reject non-canonical or out-of-range values before pandas numeric coercion."""
    if isinstance(value, (bool, complex)) or pd.api.types.is_bool(value) or pd.api.types.is_complex(value):
        _invalid("invalid adjusted_close numeric form")
    if isinstance(value, int) and not isinstance(value, bool):
        if value > 0 and value.bit_length() > 1024:
            _invalid("adjusted_close exceeds finite numeric range")
        try:
            text = str(value)
        except ValueError as exc:
            raise SnapshotValidationError("adjusted_close exceeds finite numeric range") from exc
        if value <= 0 or len(text) > len(_MAX_FLOAT_INTEGER_DECIMAL) or (
            len(text) == len(_MAX_FLOAT_INTEGER_DECIMAL) and text > _MAX_FLOAT_INTEGER_DECIMAL
        ):
            _invalid("adjusted_close exceeds finite numeric range")
        return
    if isinstance(value, str):
        text = value.strip()
        if text.lower() in {"true", "false"}:
            _invalid("boolean adjusted_close is not allowed")
        if text.isdigit():
            if text != "0" and text.startswith("0"):
                _invalid("noncanonical adjusted_close integer")
            if text == "0" or len(text) > len(_MAX_FLOAT_INTEGER_DECIMAL) or (
                len(text) == len(_MAX_FLOAT_INTEGER_DECIMAL) and text > _MAX_FLOAT_INTEGER_DECIMAL
            ):
                _invalid("adjusted_close exceeds finite numeric range")
            return
        try:
            number = float(text)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SnapshotValidationError("invalid adjusted_close numeric form") from exc
        if not math.isfinite(number) or number <= 0:
            _invalid("adjusted_close exceeds finite numeric range")
        return
    if isinstance(value, float):
        if not math.isfinite(value) or value <= 0:
            _invalid("adjusted_close exceeds finite numeric range")
        return
    try:
        text = str(value)
    except Exception as exc:
        raise SnapshotValidationError("invalid adjusted_close numeric form") from exc
    _admit_raw_numeric(text)


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
    for value in adjusted_close.array:
        _admit_raw_numeric(value)
    try:
        normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid adjusted_close numeric form") from exc
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


def _publish_noreplace(source: Path, destination: Path) -> None:
    """Publish a directory without clobbering an existing destination."""
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise SnapshotValidationError("required no-clobber capability unavailable")
        renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        renameat2.restype = ctypes.c_int
        parent_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
        parent_fd = os.open(destination.parent, parent_flags)
        try:
            result = renameat2(
                parent_fd,
                source.name.encode(),
                parent_fd,
                destination.name.encode(),
                1,
            )
        finally:
            os.close(parent_fd)
        if result != 0:
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                raise SnapshotValidationError(f"immutable output already exists: {destination}")
            raise SnapshotValidationError("atomic no-clobber publish failed")
        return
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        renamex_np = getattr(libc, "renamex_np", None)
        if renamex_np is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise SnapshotValidationError("required no-clobber capability unavailable")
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        result = renamex_np(str(source).encode(), str(destination).encode(), 4)
        if result != 0:
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                raise SnapshotValidationError(f"immutable output already exists: {destination}")
            raise SnapshotValidationError("atomic no-clobber publish failed")
        return
    raise SnapshotValidationError("unsupported platform for atomic no-clobber publish")

def _has_exact_type(value: object, expected: object) -> bool:
    if type(value) is not type(expected):
        return False
    if type(expected) is dict:
        return set(value) == set(expected) and all(_has_exact_type(value[key], expected[key]) for key in expected)
    if type(expected) is list:
        return len(value) == len(expected) and all(_has_exact_type(actual, wanted) for actual, wanted in zip(value, expected))
    return value == expected


def _verify_snapshot_members(
    members: dict[str, bytes],
    *,
    output_dir: Path,
    expected_manifest_sha256: str,
) -> SnapshotResult:
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
            pd.read_csv(BytesIO(members["prices.csv"]), dtype={PRICE_FIELD: "string"}),
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
    return SnapshotResult(
        output_dir=output_dir,
        manifest_sha256=member_hashes["manifest.json"],
        verified_members=tuple(sorted(members.items())),
    )


def read_tqqq_r1_snapshot_member_fd(directory_fd: int, name: str) -> bytes:
    if name not in OUTPUT_FILENAMES:
        _invalid("invalid snapshot member")
    try:
        member_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    try:
        member_stat = os.fstat(member_fd)
        if not stat.S_ISREG(member_stat.st_mode):
            _invalid("snapshot members must be regular non-symlink files")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(member_fd, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    finally:
        os.close(member_fd)


def verify_tqqq_r1_snapshot_fd(
    directory_fd: int,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    """Verify snapshot members relative to an already-open stable directory."""
    if type(directory_fd) is not int or directory_fd < 0:
        _invalid("invalid snapshot directory descriptor")
    try:
        names = tuple(sorted(os.listdir(directory_fd)))
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        _invalid(f"unexpected output files: {names}")
    members = {name: read_tqqq_r1_snapshot_member_fd(directory_fd, name) for name in OUTPUT_FILENAMES}
    try:
        if Path("/proc/self/fd").is_dir():
            output_dir = Path(f"/proc/self/fd/{directory_fd}").resolve(strict=True)
        elif sys.platform == "darwin":
            output_dir = Path(
                fcntl.fcntl(directory_fd, fcntl.F_GETPATH, b"\0" * 1024).split(b"\0", 1)[0].decode()
            )
        else:
            _invalid("stable snapshot directory capability is unavailable")
    except (OSError, UnicodeDecodeError) as exc:
        raise SnapshotValidationError("unable to resolve snapshot directory descriptor") from exc
    return _verify_snapshot_members(
        members,
        output_dir=output_dir,
        expected_manifest_sha256=expected_manifest_sha256,
    )


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
    return _verify_snapshot_members(
        members,
        output_dir=output,
        expected_manifest_sha256=expected_manifest_sha256,
    )


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
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)
