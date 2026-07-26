"""Offline-only immutable snapshots for the SOXL/TQQQ clean cutover."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA = "soxl_tqqq_clean_cutover_snapshot.v1"
EVIDENCE_GENERATION = "clean_cutover_v1"
PLUGIN = "ABSENT_DISABLED"
TIMEZONE = "America/New_York"
ADJUSTMENT_SEMANTICS = "adjusted_close"
PRODUCER_CONTRACT_VERSION = "soxl_tqqq_clean_cutover_snapshot.materializer.v1"
INVALID_EVIDENCE = "INVALID_EVIDENCE"
SIZE = 0
OUTPUT_FILENAMES = ("manifest.json", "payload.json", "publication.json")
PAIR_SYMBOLS = {
    "QQQ_TQQQ": ("QQQ", "TQQQ"),
    "SOXX_SOXL": ("SOXX", "SOXL"),
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SnapshotValidationError(ValueError):
    """Raised when an offline fixture cannot satisfy the clean-cutover contract."""

    status = INVALID_EVIDENCE


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    snapshot_id: str


def _invalid(message: str) -> None:
    raise SnapshotValidationError(f"{INVALID_EVIDENCE}: {message}")


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _is_iso_date(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _is_utc_timestamp(value: object) -> bool:
    if type(value) is not str or not value.endswith("Z"):
        return False
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").isoformat().replace("+00:00", "Z") == value
    except ValueError:
        return False


def _parse_object(raw: bytes, name: str) -> dict[str, Any]:
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
        raise SnapshotValidationError(f"{INVALID_EVIDENCE}: invalid {name}") from exc
    if type(value) is not dict:
        _invalid(f"invalid {name}")
    return value


def _validate_pair_id(pair_id: object) -> tuple[str, tuple[str, str]]:
    if type(pair_id) is not str or pair_id not in PAIR_SYMBOLS:
        _invalid("unknown pair_id")
    return pair_id, PAIR_SYMBOLS[pair_id]


def _validate_rows(
    pair_id: str,
    rows: Iterable[Mapping[str, object]],
    *,
    require_canonical_order: bool,
) -> list[dict[str, object]]:
    _, symbols = _validate_pair_id(pair_id)
    normalized: list[dict[str, object]] = []
    try:
        iterator = iter(rows)
    except TypeError as exc:
        raise SnapshotValidationError(f"{INVALID_EVIDENCE}: rows must be iterable") from exc
    for row in iterator:
        if not isinstance(row, Mapping) or set(row) != {"session", "symbol", "adjusted_close"}:
            _invalid("rows must use the exact declared columns")
        session = row["session"]
        symbol = row["symbol"]
        adjusted_close = row["adjusted_close"]
        if not _is_iso_date(session):
            _invalid("session must be a canonical ISO date")
        if type(symbol) is not str or symbol not in symbols:
            _invalid("each session must contain exactly the pair symbols")
        if type(adjusted_close) not in (int, float) or isinstance(adjusted_close, bool) or not math.isfinite(adjusted_close) or adjusted_close <= 0:
            _invalid("adjusted_close must be positive finite")
        normalized.append({"session": session, "symbol": symbol, "adjusted_close": adjusted_close})
    if not normalized:
        _invalid("rows must not be empty")
    canonical = sorted(normalized, key=lambda row: (str(row["session"]), str(row["symbol"])))
    if require_canonical_order and normalized != canonical:
        _invalid("rows must be canonically sorted by session and symbol")
    sessions: dict[str, list[str]] = {}
    for row in normalized:
        sessions.setdefault(str(row["session"]), []).append(str(row["symbol"]))
    if any(sorted(found) != sorted(symbols) for found in sessions.values()):
        _invalid("each session must contain exactly the pair symbols")
    if len({(row["session"], row["symbol"]) for row in normalized}) != len(normalized):
        _invalid("duplicate pair session")
    return canonical


def _compatibility() -> dict[str, str]:
    return {
        "legacy_read": "historical_read_only",
        "legacy_write": "forbidden",
        "dual_read": "forbidden",
        "auto_migration": "forbidden",
        "fallback": "forbidden",
        "side_by_side_namespace": "required_new_namespace",
    }


def _validate_manifest(manifest: object, payload_sha256: str, expected_calendar_sha256: str) -> tuple[str, tuple[str, str]]:
    if type(manifest) is not dict:
        _invalid("invalid manifest")
    required = {
        "schema", "pair_id", "symbols", "payload_sha256", "calendar_sha256", "timezone", "coverage",
        "adjustment_semantics", "evidence_generation", "plugin", "offline_fixture", "source_identity",
        "producer_identity", "producer_contract_version", "materialized_at", "materialization_receipt", "compatibility", "size",
    }
    if set(manifest) != required:
        _invalid("manifest fields are not exact")
    pair_id, symbols = _validate_pair_id(manifest["pair_id"])
    coverage = manifest["coverage"]
    if manifest["calendar_sha256"] != expected_calendar_sha256:
        _invalid("calendar digest mismatch")
    if (
        manifest["schema"] != SCHEMA
        or manifest["symbols"] != list(symbols)
        or manifest["payload_sha256"] != payload_sha256
        or manifest["timezone"] != TIMEZONE
        or manifest["adjustment_semantics"] != ADJUSTMENT_SEMANTICS
        or manifest["evidence_generation"] != EVIDENCE_GENERATION
        or manifest["plugin"] != PLUGIN
        or manifest["offline_fixture"] is not True
        or manifest["compatibility"] != _compatibility()
        or manifest["size"] != SIZE
        or type(manifest["source_identity"]) is not str
        or not manifest["source_identity"]
        or type(manifest["producer_identity"]) is not str
        or not manifest["producer_identity"]
        or manifest["producer_contract_version"] != PRODUCER_CONTRACT_VERSION
        or not _is_utc_timestamp(manifest["materialized_at"])
        or manifest["materialization_receipt"] != "offline_fixture"
    ):
        _invalid("invalid manifest binding")
    if type(coverage) is not dict or set(coverage) != {"first_available_session", "last_available_session", "completed_sessions", "row_count", "per_symbol_counts"}:
        _invalid("invalid coverage")
    completed = coverage["completed_sessions"]
    if (
        type(completed) is not list
        or not completed
        or any(not _is_iso_date(item) for item in completed)
        or completed != sorted(completed)
        or coverage["first_available_session"] != completed[0]
        or coverage["last_available_session"] != completed[-1]
        or type(coverage["row_count"]) is not int
        or coverage["row_count"] != len(completed) * len(symbols)
        or coverage["per_symbol_counts"] != {symbol: len(completed) for symbol in symbols}
    ):
        _invalid("invalid coverage")
    return pair_id, symbols


def _read_members(output_dir: Path) -> dict[str, bytes]:
    if output_dir.is_symlink() or not output_dir.is_dir():
        _invalid("snapshot root must be a regular directory")
    try:
        names = tuple(sorted(path.name for path in output_dir.iterdir()))
    except OSError as exc:
        raise SnapshotValidationError(f"{INVALID_EVIDENCE}: unable to read snapshot") from exc
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        _invalid("publication members are not exact")
    members: dict[str, bytes] = {}
    for name in OUTPUT_FILENAMES:
        path = output_dir / name
        if path.is_symlink() or not path.is_file():
            _invalid("publication members must be regular files")
        try:
            members[name] = path.read_bytes()
        except OSError as exc:
            raise SnapshotValidationError(f"{INVALID_EVIDENCE}: unable to read snapshot") from exc
    return members


def verify_clean_cutover_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_calendar_sha256: str,
) -> SnapshotResult:
    """Strictly read an immutable snapshot using caller-owned digest expectations."""
    if not _is_sha256(expected_manifest_sha256):
        _invalid("expected manifest SHA-256 is required")
    if not _is_sha256(expected_calendar_sha256):
        _invalid("expected calendar SHA-256 is required")
    output = Path(output_dir)
    members = _read_members(output)
    manifest_sha256 = _sha256(members["manifest.json"])
    if manifest_sha256 != expected_manifest_sha256:
        _invalid("manifest digest mismatch")
    manifest = _parse_object(members["manifest.json"], "manifest")
    payload = _parse_object(members["payload.json"], "payload")
    publication = _parse_object(members["publication.json"], "publication")
    pair_id, symbols = _validate_manifest(manifest, _sha256(members["payload.json"]), expected_calendar_sha256)
    if type(payload) is not dict or set(payload) != {"schema", "pair_id", "symbols", "adjustment_semantics", "rows"}:
        _invalid("payload fields are not exact")
    if payload["schema"] != SCHEMA or payload["pair_id"] != pair_id or payload["symbols"] != list(symbols) or payload["adjustment_semantics"] != ADJUSTMENT_SEMANTICS:
        _invalid("invalid payload binding")
    rows = _validate_rows(pair_id, payload["rows"], require_canonical_order=True)
    completed = manifest["coverage"]["completed_sessions"]
    if [row["session"] for row in rows[::len(symbols)]] != completed:
        _invalid("payload coverage mismatch")
    snapshot_id = f"sha256-{expected_manifest_sha256}"
    if publication != {"schema": "soxl_tqqq_clean_cutover_publication.v1", "complete": True, "snapshot_id": snapshot_id, "manifest_sha256": expected_manifest_sha256}:
        _invalid("invalid publication marker")
    return SnapshotResult(output_dir=output, snapshot_id=snapshot_id)


def materialize_clean_cutover_snapshot(
    *,
    pair_id: str,
    rows: Iterable[Mapping[str, object]],
    output_dir: str | Path,
    calendar_sha256: str,
    source_identity: str,
    producer_identity: str,
    materialized_at: str,
    evidence_generation: str = EVIDENCE_GENERATION,
    plugin: str = PLUGIN,
    legacy_read: str = "historical_read_only",
    offline_fixture: bool = True,
    size: int = SIZE,
) -> SnapshotResult:
    """Publish one new, fixture-only pair snapshot without legacy or provider access."""
    _, symbols = _validate_pair_id(pair_id)
    if evidence_generation != EVIDENCE_GENERATION or plugin != PLUGIN or legacy_read != "historical_read_only":
        _invalid("clean-cutover generation, plugin, and legacy binding are required")
    if offline_fixture is not True or size != SIZE:
        _invalid("offline fixture and size zero are required")
    if not _is_sha256(calendar_sha256):
        _invalid("calendar SHA-256 is required")
    if type(source_identity) is not str or not source_identity or type(producer_identity) is not str or not producer_identity or not _is_utc_timestamp(materialized_at):
        _invalid("source and producer identity with UTC materialized_at are required")
    normalized_rows = _validate_rows(pair_id, rows, require_canonical_order=False)
    completed_sessions = sorted({str(row["session"]) for row in normalized_rows})
    payload = {
        "schema": SCHEMA,
        "pair_id": pair_id,
        "symbols": list(symbols),
        "adjustment_semantics": ADJUSTMENT_SEMANTICS,
        "rows": normalized_rows,
    }
    payload_bytes = _canonical_json(payload)
    manifest = {
        "schema": SCHEMA,
        "pair_id": pair_id,
        "symbols": list(symbols),
        "payload_sha256": _sha256(payload_bytes),
        "calendar_sha256": calendar_sha256,
        "timezone": TIMEZONE,
        "coverage": {
            "first_available_session": completed_sessions[0],
            "last_available_session": completed_sessions[-1],
            "completed_sessions": completed_sessions,
            "row_count": len(normalized_rows),
            "per_symbol_counts": {symbol: len(completed_sessions) for symbol in symbols},
        },
        "adjustment_semantics": ADJUSTMENT_SEMANTICS,
        "evidence_generation": EVIDENCE_GENERATION,
        "plugin": PLUGIN,
        "offline_fixture": True,
        "source_identity": source_identity,
        "producer_identity": producer_identity,
        "producer_contract_version": PRODUCER_CONTRACT_VERSION,
        "materialized_at": materialized_at,
        "materialization_receipt": "offline_fixture",
        "compatibility": _compatibility(),
        "size": SIZE,
    }
    manifest_bytes = _canonical_json(manifest)
    manifest_sha256 = _sha256(manifest_bytes)
    publication = {
        "schema": "soxl_tqqq_clean_cutover_publication.v1",
        "complete": True,
        "snapshot_id": f"sha256-{manifest_sha256}",
        "manifest_sha256": manifest_sha256,
    }
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid("immutable output already exists")
    parent = destination.parent
    if parent.is_symlink() or not parent.is_dir():
        _invalid("output parent must be an existing regular directory")
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=parent))
    try:
        (temporary / "payload.json").write_bytes(payload_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        (temporary / "publication.json").write_bytes(_canonical_json(publication))
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, snapshot_id=f"sha256-{manifest_sha256}")
