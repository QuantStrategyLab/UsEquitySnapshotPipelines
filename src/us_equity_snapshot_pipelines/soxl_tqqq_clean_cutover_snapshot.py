"""Offline, caller-bound clean-cutover snapshot materialization."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Mapping

MAX_MEMBER_BYTES = 1_048_576
_MEMBERS = frozenset({"manifest.json", "payload.json", "publication.json"})
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
PAIR_SYMBOLS = {"QQQ_TQQQ": ("QQQ", "TQQQ"), "SOXX_SOXL": ("SOXX", "SOXL")}


class SnapshotValidationError(ValueError):
    """Raised when snapshot evidence violates the clean-cutover contract."""


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_id: str
    path: Path
    manifest_sha256: str
    payload_sha256: str


def _fail(message: str) -> None:
    raise SnapshotValidationError(message)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8") + b"\n"
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise SnapshotValidationError("noncanonical JSON evidence") from exc


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _parse_canonical(raw: bytes, label: str) -> dict[str, object]:
    def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(ValueError(f"non-finite {token}")),
        )
        if type(value) is not dict or _canonical(value) != raw:
            _fail(f"invalid canonical {label}")
        return value
    except SnapshotValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise SnapshotValidationError(f"invalid canonical {label}") from exc


def _require_string(value: object, name: str, *, nonempty: bool = True) -> str:
    if type(value) is not str or (nonempty and not value):
        _fail(f"invalid {name}")
    return value


def _require_sha256(value: object, name: str) -> str:
    value = _require_string(value, name)
    if len(value) != 64 or value.lower() != value or any(char not in "0123456789abcdef" for char in value):
        _fail(f"invalid {name}")
    return value


def _require_date(value: object, name: str) -> str:
    value = _require_string(value, name)
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if parsed.isoformat() != value:
        _fail(f"invalid {name}")
    return value


def _require_timestamp(value: object, name: str) -> str:
    value = _require_string(value, name)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        _fail(f"invalid {name}")
    return value


def _read_regular(path: str | Path, label: str) -> bytes:
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY | _NOFOLLOW)
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError(f"unable to open {label}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size < 0 or info.st_size > MAX_MEMBER_BYTES:
            _fail(f"invalid {label}")
        chunks: list[bytes] = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(fd, min(65_536, remaining))
            if not chunk:
                _fail(f"truncated {label}")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1):
            _fail(f"oversized {label}")
        return b"".join(chunks)
    except SnapshotValidationError:
        raise
    except (OSError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError(f"unable to read {label}") from exc
    finally:
        os.close(fd)


def _calendar_sessions(path: str | Path, expected_sha256: str) -> frozenset[str]:
    expected_sha256 = _require_sha256(expected_sha256, "calendar_sha256")
    raw = _read_regular(path, "calendar")
    if _sha256(raw) != expected_sha256:
        _fail("calendar digest mismatch")
    value = _parse_canonical(raw, "calendar")
    if set(value) != {"schema", "exchange", "timezone", "sessions"}:
        _fail("invalid calendar shape")
    if value["schema"] != "xnys_calendar_v1" or value["exchange"] != "XNYS" or value["timezone"] != "America/New_York":
        _fail("invalid calendar identity")
    sessions = value["sessions"]
    if type(sessions) is not list or not sessions:
        _fail("invalid calendar sessions")
    parsed = [_require_date(item, "calendar session") for item in sessions]
    if parsed != sorted(parsed) or len(parsed) != len(set(parsed)):
        _fail("invalid calendar sessions")
    return frozenset(parsed)


def _validate_rows(pair_id: str, rows: Iterable[Mapping[str, object]], sessions: frozenset[str]) -> list[dict[str, object]]:
    if type(pair_id) is not str or pair_id not in PAIR_SYMBOLS:
        _fail("invalid pair_id")
    try:
        supplied = list(rows)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid rows") from exc
    if not supplied:
        _fail("invalid rows")
    normalized: list[dict[str, object]] = []
    for row in supplied:
        if type(row) is not dict or set(row) != {"session", "symbol", "adjusted_close"}:
            _fail("invalid row")
        session = _require_date(row["session"], "session")
        symbol = _require_string(row["symbol"], "symbol")
        value = row["adjusted_close"]
        if type(value) not in {int, float}:
            _fail("invalid adjusted_close")
        try:
            adjusted_close = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise SnapshotValidationError("invalid adjusted_close") from exc
        if not math.isfinite(adjusted_close) or adjusted_close <= 0:
            _fail("invalid adjusted_close")
        if session not in sessions:
            _fail("session absent from calendar")
        normalized.append({"session": session, "symbol": symbol, "adjusted_close": adjusted_close})
    symbols = PAIR_SYMBOLS[pair_id]
    ordered_sessions = sorted({str(row["session"]) for row in normalized})
    expected = [(session, symbol) for session in ordered_sessions for symbol in symbols]
    actual = [(str(row["session"]), str(row["symbol"])) for row in normalized]
    if actual != expected:
        _fail("rows must be complete and canonically ordered")
    return normalized


def _build_members(
    pair_id: str,
    rows: list[dict[str, object]],
    source_identity: str,
    producer_identity: str,
    generated_at: str,
) -> tuple[bytes, bytes, bytes, str, str, str]:
    source_identity = _require_string(source_identity, "source_identity")
    producer_identity = _require_string(producer_identity, "producer_identity")
    generated_at = _require_timestamp(generated_at, "generated_at")
    sessions = [str(row["session"]) for row in rows[::2]]
    symbols = PAIR_SYMBOLS[pair_id]
    snapshot_id = "clean_cutover_" + _sha256(_canonical({"pair_id": pair_id, "rows": rows}))[:24]
    payload = {"pair_id": pair_id, "rows": rows, "snapshot_id": snapshot_id}
    payload_raw = _canonical(payload)
    payload_sha256 = _sha256(payload_raw)
    manifest = {
        "coverage": {
            "completed_sessions": sessions,
            "first_session": sessions[0],
            "last_session": sessions[-1],
            "per_symbol_counts": {symbol: len(sessions) for symbol in symbols},
            "row_count": len(rows),
        },
        "evidence_generation": "clean_cutover_v1",
        "generated_at": generated_at,
        "offline_fixture": True,
        "pair_id": pair_id,
        "payload_sha256": payload_sha256,
        "plugin": "ABSENT_DISABLED",
        "producer_identity": producer_identity,
        "schema": "soxl_tqqq_clean_cutover_snapshot.v1",
        "size": 0,
        "snapshot_id": snapshot_id,
        "source_identity": source_identity,
        "symbols": list(symbols),
    }
    manifest_raw = _canonical(manifest)
    manifest_sha256 = _sha256(manifest_raw)
    publication_raw = _canonical(
        {
            "complete": True,
            "manifest_sha256": manifest_sha256,
            "payload_sha256": payload_sha256,
            "snapshot_id": snapshot_id,
        }
    )
    for raw in (manifest_raw, payload_raw, publication_raw):
        if len(raw) > MAX_MEMBER_BYTES:
            _fail("serialized member exceeds bound")
    return manifest_raw, payload_raw, publication_raw, snapshot_id, manifest_sha256, payload_sha256


def _write_member(directory_fd: int, name: str, raw: bytes) -> None:
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW, 0o600, dir_fd=directory_fd)
        try:
            written = 0
            while written < len(raw):
                written += os.write(fd, raw[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
    except (OSError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("exclusive publication failed") from exc


def _reserve(destination: str | Path) -> int:
    try:
        target = Path(destination)
        if target.name in {"", ".", ".."}:
            _fail("invalid destination")
        parent = target.parent
        parent_info = os.lstat(parent)
        if stat.S_ISLNK(parent_info.st_mode):
            _fail("invalid destination parent")
        parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW)
        try:
            if not stat.S_ISDIR(os.fstat(parent_fd).st_mode):
                _fail("invalid destination parent")
            os.mkdir(target.name, 0o700, dir_fd=parent_fd)
            directory_fd = os.open(target.name, os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)
        return directory_fd
    except SnapshotValidationError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError("destination reservation failed") from exc


def materialize_clean_cutover_snapshot(
    pair_id: str,
    rows: Iterable[Mapping[str, object]],
    destination: str | Path,
    *,
    calendar_path: str | Path,
    calendar_sha256: str,
    source_identity: str,
    producer_identity: str,
    generated_at: str,
) -> SnapshotResult:
    calendar_sessions = _calendar_sessions(calendar_path, calendar_sha256)
    normalized_rows = _validate_rows(pair_id, rows, calendar_sessions)
    manifest, payload, publication, snapshot_id, manifest_sha256, payload_sha256 = _build_members(
        pair_id, normalized_rows, source_identity, producer_identity, generated_at
    )
    directory_fd = _reserve(destination)
    try:
        _write_member(directory_fd, "manifest.json", manifest)
        _write_member(directory_fd, "payload.json", payload)
        _write_member(directory_fd, "publication.json", publication)
        os.fsync(directory_fd)
    except SnapshotValidationError:
        raise
    except OSError as exc:
        raise SnapshotValidationError("publication failed") from exc
    finally:
        os.close(directory_fd)
    result = strict_readback_clean_cutover_snapshot(
        destination,
        calendar_path=calendar_path,
        calendar_sha256=calendar_sha256,
        expected_manifest_sha256=manifest_sha256,
    )
    if result.snapshot_id != snapshot_id or result.payload_sha256 != payload_sha256:
        _fail("strict readback mismatch")
    return result


def _read_member(directory_fd: int, name: str) -> bytes:
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_size < 0 or info.st_size > MAX_MEMBER_BYTES:
            _fail("invalid publication member")
        fd = os.open(name, os.O_RDONLY | _NOFOLLOW, dir_fd=directory_fd)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                _fail("invalid publication member")
            raw = os.read(fd, info.st_size)
            if len(raw) != info.st_size or os.read(fd, 1):
                _fail("invalid publication member")
            return raw
        finally:
            os.close(fd)
    except SnapshotValidationError:
        raise
    except (OSError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("unable to read publication member") from exc


def _validate_publication(manifest: dict[str, object], payload: dict[str, object], marker: dict[str, object], sessions: frozenset[str], manifest_raw: bytes, payload_raw: bytes) -> tuple[str, str, str]:
    manifest_keys = {"schema", "snapshot_id", "pair_id", "symbols", "evidence_generation", "offline_fixture", "plugin", "size", "source_identity", "producer_identity", "generated_at", "coverage", "payload_sha256"}
    if set(manifest) != manifest_keys or set(payload) != {"snapshot_id", "pair_id", "rows"} or set(marker) != {"complete", "snapshot_id", "manifest_sha256", "payload_sha256"}:
        _fail("invalid publication shape")
    if manifest["schema"] != "soxl_tqqq_clean_cutover_snapshot.v1" or manifest["evidence_generation"] != "clean_cutover_v1" or manifest["offline_fixture"] is not True or manifest["plugin"] != "ABSENT_DISABLED" or type(manifest["size"]) is not int or manifest["size"] != 0:
        _fail("invalid clean-cutover disposition")
    pair_id = _require_string(manifest["pair_id"], "pair_id")
    if pair_id not in PAIR_SYMBOLS or payload["pair_id"] != pair_id:
        _fail("invalid pair binding")
    symbols = manifest["symbols"]
    if type(symbols) is not list or tuple(symbols) != PAIR_SYMBOLS[pair_id] or any(type(symbol) is not str for symbol in symbols):
        _fail("invalid symbols")
    snapshot_id = _require_string(manifest["snapshot_id"], "snapshot_id")
    if payload["snapshot_id"] != snapshot_id or marker["snapshot_id"] != snapshot_id or type(marker["complete"]) is not bool or marker["complete"] is not True:
        _fail("invalid snapshot binding")
    _require_string(manifest["source_identity"], "source_identity")
    _require_string(manifest["producer_identity"], "producer_identity")
    _require_timestamp(manifest["generated_at"], "generated_at")
    payload_sha256 = _require_sha256(manifest["payload_sha256"], "payload_sha256")
    manifest_sha256 = _sha256(manifest_raw)
    if payload_sha256 != _sha256(payload_raw) or marker["payload_sha256"] != payload_sha256 or marker["manifest_sha256"] != manifest_sha256:
        _fail("invalid digest binding")
    rows = payload["rows"]
    if type(rows) is not list:
        _fail("invalid rows")
    normalized = _validate_rows(pair_id, rows, sessions)
    if normalized != rows:
        _fail("noncanonical rows")
    coverage = manifest["coverage"]
    if type(coverage) is not dict or set(coverage) != {"completed_sessions", "first_session", "last_session", "per_symbol_counts", "row_count"}:
        _fail("invalid coverage")
    completed = coverage["completed_sessions"]
    if type(completed) is not list or not completed:
        _fail("invalid completed_sessions")
    completed_dates = [_require_date(item, "completed_session") for item in completed]
    if completed_dates != sorted(completed_dates) or len(completed_dates) != len(set(completed_dates)):
        _fail("invalid completed_sessions")
    if coverage["first_session"] != completed_dates[0] or coverage["last_session"] != completed_dates[-1] or completed_dates != [row["session"] for row in rows[::2]]:
        _fail("invalid session coverage")
    counts = coverage["per_symbol_counts"]
    if type(counts) is not dict or set(counts) != set(PAIR_SYMBOLS[pair_id]):
        _fail("invalid symbol coverage")
    if any(type(counts[symbol]) is not int or counts[symbol] != len(completed_dates) for symbol in PAIR_SYMBOLS[pair_id]):
        _fail("invalid symbol coverage")
    if type(coverage["row_count"]) is not int or coverage["row_count"] != len(rows) or coverage["row_count"] != len(completed_dates) * 2:
        _fail("invalid row coverage")
    return snapshot_id, manifest_sha256, payload_sha256


def strict_readback_clean_cutover_snapshot(
    destination: str | Path,
    *,
    calendar_path: str | Path,
    calendar_sha256: str,
    expected_manifest_sha256: str | None = None,
) -> SnapshotResult:
    sessions = _calendar_sessions(calendar_path, calendar_sha256)
    try:
        directory_fd = os.open(os.fspath(destination), os.O_RDONLY | os.O_DIRECTORY | _NOFOLLOW)
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError("unable to open publication") from exc
    try:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode) or set(os.listdir(directory_fd)) != _MEMBERS:
            _fail("incomplete publication")
        manifest_raw = _read_member(directory_fd, "manifest.json")
        payload_raw = _read_member(directory_fd, "payload.json")
        marker_raw = _read_member(directory_fd, "publication.json")
    except SnapshotValidationError:
        raise
    except (OSError, ValueError) as exc:
        raise SnapshotValidationError("unable to read publication") from exc
    finally:
        os.close(directory_fd)
    manifest = _parse_canonical(manifest_raw, "manifest")
    payload = _parse_canonical(payload_raw, "payload")
    marker = _parse_canonical(marker_raw, "publication")
    snapshot_id, manifest_sha256, payload_sha256 = _validate_publication(manifest, payload, marker, sessions, manifest_raw, payload_raw)
    if expected_manifest_sha256 is not None and _require_sha256(expected_manifest_sha256, "expected_manifest_sha256") != manifest_sha256:
        _fail("external manifest digest mismatch")
    return SnapshotResult(snapshot_id, Path(destination), manifest_sha256, payload_sha256)
