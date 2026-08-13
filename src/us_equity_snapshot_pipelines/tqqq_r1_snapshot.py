"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import ctypes
import csv
import errno
import hashlib
import json
import math
import os
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, time
from io import StringIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from quant_platform_kit.data.research_input import (
    InvalidResearchInputEvidence,
    canonical_research_input_manifest_bytes,
    read_research_input_manifest_json,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")
_MAX_FLOAT_INTEGER_DECIMAL = format(sys.float_info.max, ".0f")
_MEMBER_BYTE_LIMITS = {
    "prices.csv": 16 * 1024 * 1024,
    "manifest.json": 1024 * 1024,
    "validation.json": 1024 * 1024,
    "sha256sums.json": 1024 * 1024,
}
_MAX_TOTAL_MEMBER_BYTES = 18 * 1024 * 1024
_MAX_CSV_ROWS = 500_000
_MAX_JSON_NUMBER_DIGITS = 128
_PROOF_MANIFEST_NAME = "research-input-manifest.json"
_PROOF_SNAPSHOT_DIR = "tqqq_r1_snapshot"
_PROOF_FILENAMES = tuple(sorted(f"{_PROOF_SNAPSHOT_DIR}/{name}" for name in OUTPUT_FILENAMES))
_PROOF_MANIFEST_BYTE_LIMIT = 1024 * 1024
_PROOF_TOTAL_BYTE_LIMIT = _MAX_TOTAL_MEMBER_BYTES + _PROOF_MANIFEST_BYTE_LIMIT


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str


@dataclass(frozen=True)
class LegacyTqqqSnapshotAssessment:
    """Preserved legacy identity; never grants replay or promotion authority."""

    comparison_status: str
    manifest_sha256: str | None


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


def _parse_prices_csv(raw: bytes) -> pd.DataFrame:
    """Parse the single accepted CSV lexical form before semantic normalization."""
    try:
        reader = csv.reader(StringIO(raw.decode("utf-8"), newline=""), strict=True)
        if next(reader, None) != ["session", "symbol", PRICE_FIELD]:
            _invalid("prices.csv must contain exact columns")
        rows: list[list[str]] = []
        for row in reader:
            if len(rows) >= _MAX_CSV_ROWS or len(row) != 3:
                _invalid("invalid prices.csv")
            rows.append(row)
    except (UnicodeDecodeError, csv.Error, ValueError) as exc:
        raise SnapshotValidationError("invalid prices.csv") from exc
    return _normalized_prices(
        pd.DataFrame(rows, columns=["session", "symbol", PRICE_FIELD]),
        require_exact_columns=True,
        require_canonical_symbols=True,
        require_canonical_sessions=True,
    )


def _parse_json_object(raw: bytes, name: str) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def parse_integer(value: str) -> int:
        if len(value) > _MAX_JSON_NUMBER_DIGITS:
            raise ValueError("integer exceeds resource limit")
        return int(value)

    def parse_float(value: str) -> float:
        if len(value) > _MAX_JSON_NUMBER_DIGITS:
            raise ValueError("float exceeds resource limit")
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite")
        return parsed

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_int=parse_integer,
            parse_float=parse_float,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, OverflowError, RecursionError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if type(value) is not dict:
        _invalid(f"invalid {name}")
    return value


def _require_descriptor_capabilities() -> None:
    required_flags = ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
    if (
        sys.platform not in {"darwin"} and not sys.platform.startswith("linux")
    ) or any(not hasattr(os, flag) for flag in required_flags):
        _invalid("required descriptor capability unavailable")
    if os.scandir not in os.supports_fd or os.open not in os.supports_dir_fd:
        _invalid("required descriptor capability unavailable")


def _stable_member_identity(member: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        member.st_dev,
        member.st_ino,
        member.st_mode,
        member.st_size,
        member.st_mtime_ns,
        member.st_ctime_ns,
    )


def _read_member_from_root(root_fd: int, name: str, remaining_bytes: int) -> bytes:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        member_fd = os.open(name, flags, dir_fd=root_fd)
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    try:
        try:
            before = os.fstat(member_fd)
        except OSError as exc:
            raise SnapshotValidationError("unable to read snapshot members") from exc
        if not stat.S_ISREG(before.st_mode):
            _invalid("snapshot members must be regular non-symlink files")
        if before.st_size > _MEMBER_BYTE_LIMITS.get(name, remaining_bytes):
            _invalid("snapshot member exceeds size limit")
        if before.st_size > remaining_bytes:
            _invalid("snapshot members exceed total size limit")
        raw = bytearray()
        read_limit = before.st_size + 1
        while len(raw) < read_limit:
            try:
                chunk = os.read(member_fd, read_limit - len(raw))
            except OSError as exc:
                raise SnapshotValidationError("unable to read snapshot members") from exc
            if not chunk:
                break
            raw.extend(chunk)
        try:
            after = os.fstat(member_fd)
        except OSError as exc:
            raise SnapshotValidationError("unable to read snapshot members") from exc
        if len(raw) != before.st_size or _stable_member_identity(before) != _stable_member_identity(after):
            _invalid("snapshot member changed during read")
        return bytes(raw)
    finally:
        os.close(member_fd)


def _read_snapshot_members(output: Path) -> dict[str, bytes]:
    _require_descriptor_capabilities()
    root_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        root_fd = os.open(output, root_flags)
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    try:
        try:
            root_stat = os.fstat(root_fd)
        except OSError as exc:
            raise SnapshotValidationError("unable to read snapshot members") from exc
        if not stat.S_ISDIR(root_stat.st_mode):
            _invalid("snapshot root must be a directory")
        names: set[str] = set()
        try:
            with os.scandir(root_fd) as entries:
                for entry_count, entry in enumerate(entries, start=1):
                    if entry_count > len(OUTPUT_FILENAMES):
                        _invalid("unexpected output files")
                    names.add(entry.name)
        except OSError as exc:
            raise SnapshotValidationError("unable to read snapshot members") from exc
        if names != set(OUTPUT_FILENAMES):
            _invalid("unexpected output files")
        members: dict[str, bytes] = {}
        total_size = 0
        for name in OUTPUT_FILENAMES:
            raw = _read_member_from_root(root_fd, name, _MAX_TOTAL_MEMBER_BYTES - total_size)
            total_size += len(raw)
            members[name] = raw
        return members
    finally:
        os.close(root_fd)


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


def _verify_tqqq_r1_snapshot_members(
    output: Path, members: dict[str, bytes], *, expected_manifest_sha256: str
) -> SnapshotResult:
    if (
        type(expected_manifest_sha256) is not str
        or len(expected_manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_manifest_sha256)
    ):
        _invalid("invalid trusted manifest hash")
    manifest_sha256 = hashlib.sha256(members["manifest.json"]).hexdigest()
    if manifest_sha256 != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")

    manifest = _parse_json_object(members["manifest.json"], "manifest")
    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        _invalid("invalid sha256sums")
    for name, expected in sums.items():
        if type(expected) is not str or member_hashes[name] != expected:
            _invalid(f"hash mismatch: {name}")

    try:
        validation = _parse_json_object(members["validation.json"], "validation")
        prices = _parse_prices_csv(members["prices.csv"])
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
    return SnapshotResult(output_dir=output, manifest_sha256=manifest_sha256)


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    output = Path(output_dir)
    members = _read_snapshot_members(output)
    return _verify_tqqq_r1_snapshot_members(output, members, expected_manifest_sha256=expected_manifest_sha256)


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


def _canonical_timestamp(value: object, field: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _invalid(f"{field} must be timezone-aware")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _require_git_identity(value: object, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        _invalid(f"invalid producer {field}")
    return value


def _read_tqqq_r1_research_input_proof(output: Path) -> tuple[bytes, dict[str, bytes]]:
    _require_descriptor_capabilities()
    root_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        root_fd = os.open(output, root_flags)
    except OSError as exc:
        raise SnapshotValidationError("unable to read research input proof") from exc
    try:
        if not stat.S_ISDIR(os.fstat(root_fd).st_mode):
            _invalid("research input proof root must be a directory")
        names: set[str] = set()
        with os.scandir(root_fd) as entries:
            for entry_count, entry in enumerate(entries, start=1):
                if entry_count > 2:
                    _invalid("unexpected research input proof members")
                names.add(entry.name)
        if names != {_PROOF_MANIFEST_NAME, _PROOF_SNAPSHOT_DIR}:
            _invalid("unexpected research input proof members")
        manifest_raw = _read_member_from_root(root_fd, _PROOF_MANIFEST_NAME, _PROOF_MANIFEST_BYTE_LIMIT)
        inner_fd = os.open(_PROOF_SNAPSHOT_DIR, root_flags, dir_fd=root_fd)
        try:
            if not stat.S_ISDIR(os.fstat(inner_fd).st_mode):
                _invalid("research input snapshot member must be a directory")
            names = set()
            with os.scandir(inner_fd) as entries:
                for entry_count, entry in enumerate(entries, start=1):
                    if entry_count > len(OUTPUT_FILENAMES):
                        _invalid("unexpected research input snapshot members")
                    names.add(entry.name)
            if names != set(OUTPUT_FILENAMES):
                _invalid("unexpected research input snapshot members")
            members: dict[str, bytes] = {}
            total_size = len(manifest_raw)
            inner_total_size = 0
            for name in OUTPUT_FILENAMES:
                raw = _read_member_from_root(
                    inner_fd,
                    name,
                    min(
                        _PROOF_TOTAL_BYTE_LIMIT - total_size,
                        _MAX_TOTAL_MEMBER_BYTES - inner_total_size,
                    ),
                )
                total_size += len(raw)
                inner_total_size += len(raw)
                members[f"{_PROOF_SNAPSHOT_DIR}/{name}"] = raw
            return manifest_raw, members
        finally:
            os.close(inner_fd)
    except (OSError, SnapshotValidationError):
        raise
    except Exception as exc:
        raise SnapshotValidationError("unable to read research input proof") from exc
    finally:
        os.close(root_fd)


def _research_input_manifest(
    prices: pd.DataFrame,
    *,
    producer_commit_sha: str,
    producer_tree_sha: str,
    observed_at: str,
    as_of: str,
    members: dict[str, bytes],
) -> dict[str, object]:
    normalized = _normalized_prices(prices)
    session = normalized["session"].max().date()
    effective_at = datetime.combine(session, time(), tzinfo=ZoneInfo("America/New_York")).isoformat()
    prices_sha256 = hashlib.sha256(members[f"{_PROOF_SNAPSHOT_DIR}/prices.csv"]).hexdigest()
    return validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"uesp.tqqq-r1.synthetic-proof.{prices_sha256}.v1",
            "research_input_contract_id": CONTRACT_VERSION,
            "domain": "us_equity",
            "profile": "tqqq_r1_synthetic_fixture_proof.v1",
            "artifact_type": "immutable_price_snapshot",
            "observed_at": observed_at,
            "effective_at": effective_at,
            "as_of": as_of,
            "producer": {
                "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
                "commit_sha": producer_commit_sha,
                "tree_sha": producer_tree_sha,
                "tool": "us_equity_snapshot_pipelines.tqqq_r1_snapshot.materialize_tqqq_r1_research_input_proof",
                "tool_version": "tqqq_r1_research_input_proof.v1",
            },
            "calendar": {
                "calendar_id": "UESP_TQQQ_R1_SYNTHETIC_FIXTURE_V1",
                "timezone": "America/New_York",
                "session_date": session.isoformat(),
                "source": "tqqq_r1_snapshot.weekday_session_contract",
                "source_revision": producer_commit_sha,
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "tqqq_r1_snapshot.adjusted_close",
                "source_revision": producer_commit_sha,
            },
            "sources": [
                {
                    "source_id": "uesp:tqqq-r1:canonical-prices:v1",
                    "revision": CONTRACT_VERSION,
                    "observed_at": observed_at,
                    "content_sha256": prices_sha256,
                }
            ],
            "members": [
                {
                    "path": path,
                    "media_type": "text/csv" if path.endswith("prices.csv") else "application/json",
                    "size_bytes": len(members[path]),
                    "sha256": hashlib.sha256(members[path]).hexdigest(),
                }
                for path in _PROOF_FILENAMES
            ],
        }
    )


def _validate_tqqq_r1_research_input_proof_claims(manifest: dict[str, Any], members: dict[str, bytes]) -> None:
    """Bind the generic QPK manifest to the enclosed TQQQ snapshot proof."""
    try:
        prices_raw = members[f"{_PROOF_SNAPSHOT_DIR}/prices.csv"]
        prices_sha256 = hashlib.sha256(prices_raw).hexdigest()
        session = _parse_prices_csv(prices_raw)["session"].max().date()
        producer = manifest["producer"]
        commit_sha = producer["commit_sha"]
        tree_sha = producer["tree_sha"]
        if (
            type(commit_sha) is not str
            or type(tree_sha) is not str
            or len(commit_sha) != 40
            or len(tree_sha) != 40
            or any(character not in "0123456789abcdef" for character in commit_sha + tree_sha)
        ):
            _invalid("invalid research input proof claims")
        expected_producer = {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": commit_sha,
            "tree_sha": tree_sha,
            "tool": "us_equity_snapshot_pipelines.tqqq_r1_snapshot.materialize_tqqq_r1_research_input_proof",
            "tool_version": "tqqq_r1_research_input_proof.v1",
        }
        expected_calendar = {
            "calendar_id": "UESP_TQQQ_R1_SYNTHETIC_FIXTURE_V1",
            "timezone": "America/New_York",
            "session_date": session.isoformat(),
            "source": "tqqq_r1_snapshot.weekday_session_contract",
            "source_revision": commit_sha,
        }
        expected_source = {
            "source_id": "uesp:tqqq-r1:canonical-prices:v1",
            "revision": CONTRACT_VERSION,
            "observed_at": manifest["observed_at"],
            "content_sha256": prices_sha256,
        }
        expected_adjustment = {
            "policy": "total_return_adjusted",
            "source": "tqqq_r1_snapshot.adjusted_close",
            "source_revision": commit_sha,
        }
        effective_at = datetime.combine(session, time(), tzinfo=ZoneInfo("America/New_York")).isoformat()
        if (
            manifest["schema_version"] != "research_input_manifest.v1"
            or manifest["manifest_id"] != f"uesp.tqqq-r1.synthetic-proof.{prices_sha256}.v1"
            or manifest["research_input_contract_id"] != CONTRACT_VERSION
            or manifest["domain"] != "us_equity"
            or manifest["profile"] != "tqqq_r1_synthetic_fixture_proof.v1"
            or manifest["artifact_type"] != "immutable_price_snapshot"
            or manifest["producer"] != expected_producer
            or manifest["calendar"] != expected_calendar
            or manifest["adjustment"] != expected_adjustment
            or manifest["effective_at"] != effective_at
            or manifest["sources"] != [expected_source]
        ):
            _invalid("invalid research input proof claims")
    except (KeyError, TypeError, ValueError, SnapshotValidationError):
        _invalid("invalid research input proof claims")


def verify_tqqq_r1_research_input_proof(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    """Strictly verify the detached QPK manifest and existing immutable snapshot."""
    if (
        type(expected_manifest_sha256) is not str
        or len(expected_manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_manifest_sha256)
    ):
        _invalid("invalid trusted research input manifest hash")
    output = Path(output_dir)
    try:
        manifest_raw, members = _read_tqqq_r1_research_input_proof(output)
        if hashlib.sha256(manifest_raw).hexdigest() != expected_manifest_sha256:
            _invalid("trusted research input manifest hash mismatch")
        manifest = read_research_input_manifest_json(manifest_raw)
        if manifest_raw != canonical_research_input_manifest_bytes(manifest):
            _invalid("research input manifest is not canonical")
        if research_input_manifest_sha256(manifest) != expected_manifest_sha256:
            _invalid("research input manifest digest mismatch")
        declared = {member["path"]: member for member in manifest["members"]}
        if set(declared) != set(_PROOF_FILENAMES):
            _invalid("invalid research input member mapping")
        for path, raw in members.items():
            member = declared[path]
            expected_media_type = "text/csv" if path.endswith("prices.csv") else "application/json"
            if (
                member["media_type"] != expected_media_type
                or member["size_bytes"] != len(raw)
                or member["sha256"] != hashlib.sha256(raw).hexdigest()
            ):
                _invalid("research input member hash mismatch")
        _validate_tqqq_r1_research_input_proof_claims(manifest, members)
        inner_manifest_sha256 = hashlib.sha256(members[f"{_PROOF_SNAPSHOT_DIR}/manifest.json"]).hexdigest()
        inner_members = {Path(path).name: raw for path, raw in members.items()}
        _verify_tqqq_r1_snapshot_members(
            output / _PROOF_SNAPSHOT_DIR, inner_members, expected_manifest_sha256=inner_manifest_sha256
        )
    except (InvalidResearchInputEvidence, OSError, KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("invalid research input proof") from None
    return SnapshotResult(output_dir=output, manifest_sha256=expected_manifest_sha256)


def assess_tqqq_r1_legacy_source(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> LegacyTqqqSnapshotAssessment:
    """Authenticate preserved legacy bytes without treating them as runnable input."""
    if not (
        isinstance(expected_manifest_sha256, str)
        and len(expected_manifest_sha256) == 64
        and all(character in "0123456789abcdef" for character in expected_manifest_sha256)
    ):
        _invalid("invalid trusted legacy manifest hash")
    try:
        _require_descriptor_capabilities()
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        root_fd = os.open(Path(output_dir), flags)
        try:
            snapshot_fd = os.open("snapshot", flags, dir_fd=root_fd)
            try:
                names: set[str] = set()
                with os.scandir(snapshot_fd) as entries:
                    for entry_count, entry in enumerate(entries, start=1):
                        if entry_count > 2:
                            _invalid("legacy snapshot integrity mismatch")
                        names.add(entry.name)
                if names != {"input-manifest.json", "bars.json"}:
                    _invalid("legacy snapshot integrity mismatch")
                manifest_raw = _read_member_from_root(snapshot_fd, "input-manifest.json", _PROOF_MANIFEST_BYTE_LIMIT)
                bars_raw = _read_member_from_root(
                    snapshot_fd, "bars.json", _MAX_TOTAL_MEMBER_BYTES - len(manifest_raw)
                )
            finally:
                os.close(snapshot_fd)
        finally:
            os.close(root_fd)
        manifest = _parse_json_object(manifest_raw, "legacy input manifest")
        member = manifest.get("members")
        valid_snapshot = (
            hashlib.sha256(manifest_raw).hexdigest() == expected_manifest_sha256
            and member
            == [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(bars_raw),
                    "sha256": hashlib.sha256(bars_raw).hexdigest(),
                }
            ]
        )
    except (OSError, SnapshotValidationError, TypeError, ValueError):
        valid_snapshot = False
        manifest = {}
    if not valid_snapshot:
        _invalid("legacy snapshot integrity mismatch")
    producer = manifest.get("producer")
    if not isinstance(producer, dict) or (
        producer.get("repository"),
        producer.get("tool"),
        producer.get("tool_version"),
    ) != (
        "QuantStrategyLab/UsEquitySnapshotPipelines",
        "tqqq_ibkr_paper_single_acquisition",
        "v1",
    ):
        return LegacyTqqqSnapshotAssessment("NOT_COMPARABLE", expected_manifest_sha256)
    return LegacyTqqqSnapshotAssessment("NOT_COMPARABLE", expected_manifest_sha256)


def materialize_tqqq_r1_research_input_proof(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    producer_commit_sha: str,
    producer_tree_sha: str,
    observed_at: datetime,
    as_of: datetime,
) -> SnapshotResult:
    """Publish one local synthetic TQQQ proof package without data acquisition."""
    commit_sha = _require_git_identity(producer_commit_sha, "commit")
    tree_sha = _require_git_identity(producer_tree_sha, "tree")
    observed = _canonical_timestamp(observed_at, "observed_at")
    cutoff = _canonical_timestamp(as_of, "as_of")
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        inner = materialize_tqqq_r1_snapshot(prices, temporary / _PROOF_SNAPSHOT_DIR)
        members = _read_snapshot_members(inner.output_dir)
        proof_members = {f"{_PROOF_SNAPSHOT_DIR}/{name}": raw for name, raw in members.items()}
        manifest = _research_input_manifest(
            prices,
            producer_commit_sha=commit_sha,
            producer_tree_sha=tree_sha,
            observed_at=observed,
            as_of=cutoff,
            members=proof_members,
        )
        manifest_raw = canonical_research_input_manifest_bytes(manifest)
        manifest_sha256 = research_input_manifest_sha256(manifest)
        (temporary / _PROOF_MANIFEST_NAME).write_bytes(manifest_raw)
        verify_tqqq_r1_research_input_proof(temporary, expected_manifest_sha256=manifest_sha256)
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)
