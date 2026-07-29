"""Verified local boundary for a TQQQ R1 snapshot, receipt, and offline calendar."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Any

from . import tqqq_r1_snapshot

_PACKAGE_VERSION = "tqqq_trusted_snapshot_package.v1"
_RECEIPT_VERSION = "tqqq_snapshot_receipt.v1"
_CALENDAR_VERSION = "tqqq_offline_calendar_evidence.v1"
_HEX_SHA256_LENGTH = 64
_VERIFIED_CONSTRUCTION = object()


class TrustedSnapshotPackageError(ValueError):
    """Raised when local package evidence cannot be verified."""


@dataclass(frozen=True, init=False)
class TrustedSnapshotPackage:
    """A package returned only after snapshot, receipt, and calendar verification."""

    snapshot_dir: Path
    session: str
    snapshot_manifest_sha256: str
    receipt_sha256: str
    calendar_sha256: str

    def __init__(
        self,
        *,
        _verified: object,
        snapshot_dir: Path,
        session: str,
        snapshot_manifest_sha256: str,
        receipt_sha256: str,
        calendar_sha256: str,
    ) -> None:
        if _verified is not _VERIFIED_CONSTRUCTION:
            raise TrustedSnapshotPackageError("TrustedSnapshotPackage requires verified loader")
        object.__setattr__(self, "snapshot_dir", snapshot_dir)
        object.__setattr__(self, "session", session)
        object.__setattr__(self, "snapshot_manifest_sha256", snapshot_manifest_sha256)
        object.__setattr__(self, "receipt_sha256", receipt_sha256)
        object.__setattr__(self, "calendar_sha256", calendar_sha256)


def _invalid(message: str) -> None:
    raise TrustedSnapshotPackageError(message)


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _invalid("invalid strict JSON")
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    _invalid("invalid strict JSON")


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        _invalid("invalid strict JSON")
    return parsed


def read_strict_json(payload: bytes, name: str) -> object:
    """Decode JSON while rejecting duplicate keys and non-finite constants."""
    if type(payload) is not bytes or type(name) is not str:
        _invalid("invalid strict JSON")
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_no_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_finite_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TrustedSnapshotPackageError) as exc:
        if isinstance(exc, TrustedSnapshotPackageError):
            raise
        raise TrustedSnapshotPackageError(f"invalid strict JSON: {name}") from exc


def write_strict_json(path: str | Path, payload: object) -> None:
    """Write canonical JSON and reject NaN/Infinity rather than serializing them."""
    destination = Path(path)
    if destination.is_symlink():
        _invalid("strict JSON destination symlink is not allowed")
    try:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TrustedSnapshotPackageError("non-finite or invalid strict JSON") from exc
    try:
        destination.write_bytes(encoded + b"\n")
    except OSError as exc:
        raise TrustedSnapshotPackageError("unable to write strict JSON") from exc


def _read_regular(path: str | Path, name: str) -> bytes:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        _invalid(f"{name} must be a regular non-symlink file")
    try:
        return source.read_bytes()
    except OSError as exc:
        raise TrustedSnapshotPackageError(f"unable to read {name}") from exc


def _read_evidence(path: str | Path, name: str) -> tuple[object, str]:
    payload = _read_regular(path, name)
    return read_strict_json(payload, name), hashlib.sha256(payload).hexdigest()


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == _HEX_SHA256_LENGTH and all(character in "0123456789abcdef" for character in value)


def _session(value: object, name: str) -> str:
    if type(value) is not str or len(value) != 10:
        _invalid(f"invalid {name} session")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise TrustedSnapshotPackageError(f"invalid {name} session") from exc
    if parsed.isoformat() != value:
        _invalid(f"invalid {name} session")
    if parsed.weekday() >= 5:
        _invalid(f"{name} session must be a weekday")
    return value


def _exact_object(value: object, expected_keys: set[str], name: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != expected_keys:
        _invalid(f"invalid {name}")
    return value


def _package_manifest(value: object) -> dict[str, Any]:
    manifest = _exact_object(
        value,
        {"contract_version", "snapshot_manifest_sha256", "receipt_sha256", "calendar_sha256", "session"},
        "package manifest",
    )
    if manifest["contract_version"] != _PACKAGE_VERSION:
        _invalid("invalid package manifest")
    if not all(_is_sha256(manifest[key]) for key in ("snapshot_manifest_sha256", "receipt_sha256", "calendar_sha256")):
        _invalid("invalid package manifest")
    _session(manifest["session"], "package")
    return manifest


def _receipt(value: object) -> dict[str, Any]:
    receipt = _exact_object(
        value,
        {"contract_version", "snapshot_manifest_sha256", "calendar_sha256", "session"},
        "receipt",
    )
    if receipt["contract_version"] != _RECEIPT_VERSION:
        _invalid("invalid receipt")
    if not all(_is_sha256(receipt[key]) for key in ("snapshot_manifest_sha256", "calendar_sha256")):
        _invalid("invalid receipt")
    _session(receipt["session"], "receipt")
    return receipt


def _calendar(value: object) -> dict[str, Any]:
    calendar = _exact_object(value, {"contract_version", "calendar", "sessions"}, "calendar evidence")
    if calendar["contract_version"] != _CALENDAR_VERSION or calendar["calendar"] != "XNYS" or type(calendar["sessions"]) is not list:
        _invalid("invalid calendar evidence")
    sessions = [_session(session, "calendar") for session in calendar["sessions"]]
    if not sessions or len(sessions) != len(set(sessions)) or sessions != sorted(sessions):
        _invalid("invalid calendar evidence")
    return calendar


def _snapshot_sessions(snapshot_dir: Path) -> set[str]:
    try:
        rows = csv.DictReader(StringIO(_read_regular(snapshot_dir / "prices.csv", "snapshot prices").decode("utf-8")))
        return {row["session"] for row in rows}
    except (KeyError, UnicodeDecodeError, csv.Error) as exc:
        raise TrustedSnapshotPackageError("invalid verified snapshot prices") from exc


def load_verified_trusted_snapshot_package(
    snapshot_dir: str | Path,
    package_manifest_path: str | Path,
    receipt_path: str | Path,
    calendar_evidence_path: str | Path,
    *,
    expected_snapshot_manifest_sha256: str,
    expected_package_manifest_sha256: str,
    expected_receipt_sha256: str,
) -> TrustedSnapshotPackage:
    """Verify all local evidence against caller-owned external trust anchors."""
    if not all(
        _is_sha256(value)
        for value in (
            expected_snapshot_manifest_sha256,
            expected_package_manifest_sha256,
            expected_receipt_sha256,
        )
    ):
        _invalid("invalid expected evidence hash")
    try:
        verified_snapshot = tqqq_r1_snapshot.verify_tqqq_r1_snapshot(
            snapshot_dir,
            expected_manifest_sha256=expected_snapshot_manifest_sha256,
        )
    except tqqq_r1_snapshot.SnapshotValidationError as exc:
        raise TrustedSnapshotPackageError("invalid verified snapshot") from exc
    try:
        resolved_snapshot_dir = verified_snapshot.output_dir.resolve(strict=True)
    except OSError as exc:
        raise TrustedSnapshotPackageError("unable to resolve verified snapshot") from exc

    manifest_value, package_manifest_sha256 = _read_evidence(package_manifest_path, "package manifest")
    receipt_value, receipt_sha256 = _read_evidence(receipt_path, "receipt")
    calendar_value, calendar_sha256 = _read_evidence(calendar_evidence_path, "calendar evidence")
    if package_manifest_sha256 != expected_package_manifest_sha256:
        _invalid("package manifest hash binding mismatch")
    if receipt_sha256 != expected_receipt_sha256:
        _invalid("receipt hash binding mismatch")
    manifest = _package_manifest(manifest_value)
    receipt = _receipt(receipt_value)
    calendar = _calendar(calendar_value)

    if (
        manifest["snapshot_manifest_sha256"] != expected_snapshot_manifest_sha256
        or receipt["snapshot_manifest_sha256"] != expected_snapshot_manifest_sha256
    ):
        _invalid("snapshot manifest hash binding mismatch")
    if manifest["receipt_sha256"] != receipt_sha256:
        _invalid("receipt hash binding mismatch")
    if manifest["calendar_sha256"] != calendar_sha256 or receipt["calendar_sha256"] != calendar_sha256:
        _invalid("calendar hash binding mismatch")
    if manifest["session"] != receipt["session"] or manifest["session"] not in calendar["sessions"]:
        _invalid("calendar session binding mismatch")
    if manifest["session"] not in _snapshot_sessions(resolved_snapshot_dir):
        _invalid("snapshot session binding mismatch")

    return TrustedSnapshotPackage(
        _verified=_VERIFIED_CONSTRUCTION,
        snapshot_dir=resolved_snapshot_dir,
        session=manifest["session"],
        snapshot_manifest_sha256=expected_snapshot_manifest_sha256,
        receipt_sha256=receipt_sha256,
        calendar_sha256=calendar_sha256,
    )
