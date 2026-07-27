"""Offline trusted snapshot boundary for the QQQ/TQQQ clean-cutover package."""
from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, ClassVar

SCHEMA_VERSION = "soxl_tqqq_clean_cutover_snapshot.v1"
EVIDENCE_GENERATION = "clean_cutover_v1"
_SYMBOLS = {"QQQ", "TQQQ"}
_REQUIRED = {
    "schema_version", "evidence_generation", "pair_id", "plugin_state", "size_zero",
    "source_sha256", "calendar_sha256", "external_manifest_sha256", "adjusted_price_field",
    "timezone", "sessions", "rows", "snapshot_id", "content_sha256",
}


class SnapshotValidationError(ValueError):
    """Raised when an immutable snapshot fails the trusted boundary."""


def _digest(value: object, label: str) -> str:
    if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise SnapshotValidationError(f"invalid {label}")
    return value


@dataclass(frozen=True)
class ExternalBindings:
    source_sha256: str
    calendar_sha256: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        _digest(self.source_sha256, "source_sha256")
        _digest(self.calendar_sha256, "calendar_sha256")
        _digest(self.manifest_sha256, "manifest_sha256")


def _canonical_json(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SnapshotValidationError("duplicate JSON key")
        result[key] = value
    return result


def _canonical_decimal(value: object) -> str:
    if type(value) is not str or not value or value.startswith("+") or value.startswith("-"):
        raise SnapshotValidationError("adjusted_close must be canonical")
    if value.startswith("0") and value != "0" and not value.startswith("0."):
        raise SnapshotValidationError("adjusted_close must be canonical")
    if value.count(".") > 1 or any(c not in "0123456789." for c in value):
        raise SnapshotValidationError("adjusted_close must be canonical")
    if value.endswith(".") or ("." in value and value.endswith("0")) or value == "0":
        raise SnapshotValidationError("adjusted_close must be canonical positive numeric")
    try:
        if float(value) <= 0 or not __import__("math").isfinite(float(value)):
            raise ValueError
    except ValueError as exc:
        raise SnapshotValidationError("adjusted_close must be canonical positive numeric") from exc
    return value


def _validate(payload: object, bindings: ExternalBindings) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != _REQUIRED:
        raise SnapshotValidationError("snapshot fields do not match canonical contract")
    constants = {
        "schema_version": SCHEMA_VERSION, "evidence_generation": EVIDENCE_GENERATION,
        "pair_id": "QQQ_TQQQ", "plugin_state": "ABSENT_DISABLED", "size_zero": True,
        "adjusted_price_field": "adjusted_close", "timezone": "UTC",
        "source_sha256": bindings.source_sha256, "calendar_sha256": bindings.calendar_sha256,
        "external_manifest_sha256": bindings.manifest_sha256,
        "snapshot_id": f"sha256-{bindings.manifest_sha256}",
    }
    for key, expected in constants.items():
        if payload.get(key) != expected:
            raise SnapshotValidationError(f"{key} mismatch")
    sessions = payload["sessions"]
    if not isinstance(sessions, list) or not sessions or sessions != sorted(set(sessions)):
        raise SnapshotValidationError("sessions must be sorted and unique")
    for session in sessions:
        if type(session) is not str or len(session) != 10:
            raise SnapshotValidationError("session must be canonical")
        try:
            if date.fromisoformat(session).isoformat() != session:
                raise ValueError
        except ValueError as exc:
            raise SnapshotValidationError("session must be canonical") from exc
    rows = payload["rows"]
    if not isinstance(rows, list) or not rows:
        raise SnapshotValidationError("rows must be non-empty")
    expected_pairs: list[tuple[str, str]] = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"session", "symbol", "adjusted_close"}:
            raise SnapshotValidationError("row fields do not match canonical contract")
        session, symbol = row["session"], row["symbol"]
        if session not in sessions or symbol not in _SYMBOLS:
            raise SnapshotValidationError("row identity mismatch")
        _canonical_decimal(row["adjusted_close"])
        expected_pairs.append((session, symbol))
    if expected_pairs != sorted(expected_pairs) or len(expected_pairs) != len(set(expected_pairs)):
        raise SnapshotValidationError("rows must be sorted and unique")
    for session in sessions:
        if {(s, sym) for s, sym in expected_pairs if s == session} != {(session, "QQQ"), (session, "TQQQ")}:
            raise SnapshotValidationError("each session must contain QQQ and TQQQ")
    content = payload["content_sha256"]
    _digest(content, "content_sha256")
    unsigned = dict(payload)
    unsigned.pop("content_sha256")
    expected_content = hashlib.sha256(_canonical_json(unsigned)).hexdigest()
    if content != expected_content:
        raise SnapshotValidationError("content digest mismatch")
    return payload


@dataclass(frozen=True, init=False)
class TrustedSnapshotPackage:
    """Canonical, externally-bound snapshot package; construct only via ``read``."""

    path: Path
    snapshot_id: str
    _bytes: bytes
    _TOKEN: ClassVar[object] = object()

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise TypeError("TrustedSnapshotPackage must be created by read()")

    @classmethod
    def _create(cls, path: Path, raw: bytes, payload: dict[str, Any]) -> "TrustedSnapshotPackage":
        self = object.__new__(cls)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "snapshot_id", payload["snapshot_id"])
        object.__setattr__(self, "_bytes", raw)
        return self

    @classmethod
    def read(cls, path: str | os.PathLike[str], *, root: str | os.PathLike[str], bindings: ExternalBindings) -> "TrustedSnapshotPackage":
        root_path = Path(root).resolve()
        candidate = Path(path)
        lexical = candidate if candidate.is_absolute() else Path.cwd() / candidate
        lexical = Path(os.path.abspath(lexical))
        try:
            relative = lexical.relative_to(root_path)
        except ValueError as exc:
            raise SnapshotValidationError("path escapes root") from exc
        target = root_path / relative
        current = root_path
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise SnapshotValidationError("symlink path component")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(target, flags)
        except OSError as exc:
            raise SnapshotValidationError("snapshot readback failed") from exc
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise SnapshotValidationError("snapshot must be regular file")
            raw = b""
            while chunk := os.read(fd, 1024 * 1024):
                raw += chunk
        finally:
            os.close(fd)
        try:
            payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotValidationError("invalid snapshot JSON") from exc
        checked = _validate(payload, bindings)
        if _canonical_json(checked) != raw:
            raise SnapshotValidationError("non-canonical readback")
        return cls._create(target, raw, checked)

    def to_bytes(self) -> bytes:
        return self._bytes
