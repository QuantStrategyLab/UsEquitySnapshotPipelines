"""Offline clean-cutover_v1 boundary for a quarantined QQQ/TQQQ payload.

This module deliberately has no provider, credential, network, or runtime-config access.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "soxl_tqqq_clean_cutover_snapshot.v1"
EVIDENCE_GENERATION = "clean_cutover_v1"
PAIR_ID = "QQQ_TQQQ"
SYMBOLS = ("QQQ", "TQQQ")
PLUGIN_STATE = "ABSENT_DISABLED"


class SnapshotValidationError(ValueError):
    """Raised when quarantined input or a readback violates the frozen boundary."""


@dataclass(frozen=True)
class RetrievalReceipt:
    source_sha256: str
    retrieved_at: str
    source_identity: str


@dataclass(frozen=True)
class QuarantinedRawPayload:
    payload: bytes
    receipt: RetrievalReceipt


@dataclass(frozen=True)
class CleanCutoverSnapshot:
    path: Path
    snapshot_identity: str
    external_manifest_sha256: str


def _digest(value: str, name: str) -> str:
    if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise SnapshotValidationError(f"invalid {name}")
    return value


def quarantine_raw_payload(payload: bytes, receipt: Mapping[str, Any]) -> QuarantinedRawPayload:
    """Bind caller-supplied bytes to an immutable retrieval receipt; never acquires data."""
    if not isinstance(payload, bytes) or not payload:
        raise SnapshotValidationError("quarantined payload must be non-empty bytes")
    if not isinstance(receipt, Mapping):
        raise SnapshotValidationError("invalid retrieval receipt")
    source_sha256 = _digest(receipt.get("source_sha256"), "source_sha256")
    if hashlib.sha256(payload).hexdigest() != source_sha256:
        raise SnapshotValidationError("source digest mismatch")
    retrieved_at = receipt.get("retrieved_at")
    source_identity = receipt.get("source_identity")
    if type(retrieved_at) is not str or not retrieved_at or type(source_identity) is not str or not source_identity:
        raise SnapshotValidationError("invalid retrieval receipt identity")
    return QuarantinedRawPayload(payload, RetrievalReceipt(source_sha256, retrieved_at, source_identity))


def _canonical_number(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise SnapshotValidationError("adjusted_close must be positive finite canonical numeric")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise SnapshotValidationError("adjusted_close must be positive finite canonical numeric") from exc
    if not number.is_finite() or number <= 0:
        raise SnapshotValidationError("adjusted_close must be positive finite canonical numeric")
    # Decimal canonical text is stable and rejects exponent notation in persisted output.
    if number.adjusted() > 308 or number.adjusted() < -324:
        raise SnapshotValidationError("adjusted_close exceeds finite numeric range")
    text = format(number, "f")
    if "." in text:
        integer, fraction = text.split(".", 1)
        text = integer + ("." + fraction.rstrip("0") if fraction.rstrip("0") else "")
    if text in {"", "0"}:
        raise SnapshotValidationError("adjusted_close must be positive finite canonical numeric")
    return text


def _rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, Mapping) and "rows" in raw:
        raw = raw["rows"]
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise SnapshotValidationError("rows must be a sequence")
    rows: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping) or set(row) != {"session", "symbol", "adjusted_close"}:
            raise SnapshotValidationError("row must contain exact session, symbol, adjusted_close fields")
        session, symbol = row["session"], row["symbol"]
        if type(session) is not str or len(session) != 10 or session[4] != "-" or session[7] != "-":
            raise SnapshotValidationError("session must be canonical YYYY-MM-DD")
        try:
            if date.fromisoformat(session).isoformat() != session:
                raise ValueError
        except ValueError as exc:
            raise SnapshotValidationError("session must be canonical YYYY-MM-DD") from exc
        if type(symbol) is not str or symbol not in SYMBOLS:
            raise SnapshotValidationError("symbol must be QQQ or TQQQ")
        rows.append({"session": session, "symbol": symbol, "adjusted_close": _canonical_number(row["adjusted_close"])})
    if not rows:
        raise SnapshotValidationError("rows must not be empty")
    rows.sort(key=lambda row: (row["session"], row["symbol"]))
    keys = [(r["session"], r["symbol"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise SnapshotValidationError("duplicate (session,symbol)")
    sessions = sorted({r["session"] for r in rows})
    for session in sessions:
        if {r["symbol"] for r in rows if r["session"] == session} != set(SYMBOLS):
            raise SnapshotValidationError("each session must contain exactly QQQ and TQQQ")
    return rows


def _payload_rows(quarantined: QuarantinedRawPayload | bytes | Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(quarantined, QuarantinedRawPayload):
        try:
            return _rows(json.loads(quarantined.payload.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotValidationError("invalid quarantined JSON payload") from exc
    if isinstance(quarantined, bytes):
        try:
            return _rows(json.loads(quarantined.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotValidationError("invalid quarantined JSON payload") from exc
    return _rows(quarantined)


def materialize_clean_cutover_snapshot(
    quarantined: QuarantinedRawPayload | bytes | Sequence[Mapping[str, Any]],
    destination: str | os.PathLike[str],
    *,
    source_sha256: str,
    calendar_sha256: str,
    external_manifest_sha256: str,
    sessions: Sequence[str],
) -> CleanCutoverSnapshot:
    """Materialize synthetic/quarantined rows into one immutable JSON snapshot."""
    source_sha256 = _digest(source_sha256, "source_sha256")
    calendar_sha256 = _digest(calendar_sha256, "calendar_sha256")
    external_manifest_sha256 = _digest(external_manifest_sha256, "external_manifest_sha256")
    if isinstance(quarantined, QuarantinedRawPayload) and quarantined.receipt.source_sha256 != source_sha256:
        raise SnapshotValidationError("source digest mismatch")
    if isinstance(quarantined, bytes) and hashlib.sha256(quarantined).hexdigest() != source_sha256:
        raise SnapshotValidationError("source digest mismatch")
    rows = _payload_rows(quarantined)
    actual_sessions = sorted({r["session"] for r in rows})
    expected_sessions = list(sessions)
    if expected_sessions != actual_sessions:
        raise SnapshotValidationError("session coverage mismatch")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "evidence_generation": EVIDENCE_GENERATION,
        "pair_id": PAIR_ID,
        "plugin_state": PLUGIN_STATE,
        "size_zero": True,
        "source_sha256": source_sha256,
        "calendar_sha256": calendar_sha256,
        "external_manifest_sha256": external_manifest_sha256,
        "adjusted_price_field": "adjusted_close",
        "timezone": "UTC",
        "sessions": actual_sessions,
        "rows": rows,
        "snapshot_identity": f"sha256-{external_manifest_sha256}",
    }
    payload["content_sha256"] = hashlib.sha256((json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()).hexdigest()
    encoded = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise SnapshotValidationError("destination already exists")
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp_name, target)
        except FileExistsError as exc:
            raise SnapshotValidationError("destination already exists") from exc
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
    return CleanCutoverSnapshot(target, payload["snapshot_identity"], external_manifest_sha256)


def strict_readback_clean_cutover_snapshot(
    path: str | os.PathLike[str],
    *,
    expected_source_sha256: str,
    expected_calendar_sha256: str,
    expected_external_manifest_sha256: str,
    expected_content_sha256: str | None = None,
) -> CleanCutoverSnapshot:
    """Read and verify without following symlinks or writing anything."""
    path = Path(path)
    current = Path(path.anchor)
    for part in path.parts[1:-1]:
        current = current / part
        if current.is_symlink():
            raise SnapshotValidationError("symlink path is not allowed")
    if path.is_symlink():
        raise SnapshotValidationError("symlink path is not allowed")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise SnapshotValidationError("snapshot readback failed") from exc
    try:
        raw = os.read(fd, 16 * 1024 * 1024)
    finally:
        os.close(fd)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("invalid snapshot JSON") from exc
    if not isinstance(payload, Mapping):
        raise SnapshotValidationError("invalid snapshot object")
    for key, expected in (("source_sha256", expected_source_sha256), ("calendar_sha256", expected_calendar_sha256), ("external_manifest_sha256", expected_external_manifest_sha256)):
        _digest(expected, key)
        if payload.get(key) != expected:
            raise SnapshotValidationError(f"{key} mismatch")
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("evidence_generation") != EVIDENCE_GENERATION or payload.get("pair_id") != PAIR_ID or payload.get("plugin_state") != PLUGIN_STATE or payload.get("size_zero") is not True or payload.get("timezone") != "UTC" or payload.get("adjusted_price_field") != "adjusted_close":
        raise SnapshotValidationError("snapshot identity mismatch")
    rows = _rows(payload.get("rows"))
    content = payload.get("content_sha256")
    _digest(content, "content_sha256")
    unsigned = dict(payload)
    unsigned.pop("content_sha256", None)
    actual_content = hashlib.sha256((json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()).hexdigest()
    if actual_content != content or (expected_content_sha256 is not None and content != expected_content_sha256):
        raise SnapshotValidationError("snapshot content digest mismatch")
    if payload.get("sessions") != sorted({r["session"] for r in rows}) or payload.get("snapshot_identity") != f"sha256-{expected_external_manifest_sha256}":
        raise SnapshotValidationError("snapshot identity mismatch")
    return CleanCutoverSnapshot(path, payload["snapshot_identity"], expected_external_manifest_sha256)
