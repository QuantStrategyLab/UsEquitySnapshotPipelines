"""Offline-only trusted QQQ/TQQQ clean-cutover snapshot package boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat


SCHEMA_VERSION = "soxl_tqqq_clean_cutover_snapshot.v1"
GENERATION = "clean_cutover_v1"
PLUGIN = "ABSENT_DISABLED"
PAIR_ID = "QQQ_TQQQ"
SYMBOLS = ("QQQ", "TQQQ")
CALENDAR_TIMEZONE = "America/New_York"
TOP_LEVEL_FIELDS = frozenset(
    {
        "calendar_timezone",
        "generation",
        "offline_fixture",
        "pair_id",
        "plugin",
        "rows",
        "schema_version",
        "sessions",
        "size",
        "symbols",
    }
)
ROW_FIELDS = frozenset({"adjusted_close", "session", "symbol"})
CANONICAL_DECIMAL_PATTERN = r"(?:0\.[0-9]*[1-9]|[1-9][0-9]*(?:\.[0-9]*[1-9])?)"
_DECIMAL_RE = re.compile(rf"^{CANONICAL_DECIMAL_PATTERN}$")
MAX_DECIMAL_LENGTH = 32
MAX_JSON_INT_DIGITS = 64
MAX_READ_BYTES = 16 * 1_024 * 1_024
_READ_CHUNK_BYTES = 65_536
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PACKAGE_TOKEN = object()


class SnapshotValidationError(ValueError):
    """The immutable offline snapshot did not satisfy its trust contract."""


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def canonical_json_bytes(value: object) -> bytes:
    """Serialize JSON with the exact encoding shared by runtime and schema tests."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SnapshotValidationError("cannot canonicalize JSON") from exc
    if len(encoded) > MAX_READ_BYTES:
        _invalid("canonical JSON exceeds maximum read size")
    return encoded


def _validate_sha256(value: object, field: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        _invalid(f"invalid {field}")


def _is_canonical_session(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _validate_expected_sessions(value: object) -> None:
    if type(value) is not tuple or not value:
        _invalid("expected_sessions must be a nonempty tuple")
    if any(not _is_canonical_session(session) for session in value):
        _invalid("expected_sessions contains an invalid session")
    if tuple(sorted(value)) != value or len(set(value)) != len(value):
        _invalid("expected_sessions must be sorted and unique")


def _validate_external_bindings(bindings: "ExternalBindings") -> None:
    _validate_sha256(bindings.source_sha256, "source_sha256")
    _validate_sha256(bindings.calendar_sha256, "calendar_sha256")
    _validate_sha256(bindings.manifest_sha256, "manifest_sha256")
    _validate_sha256(bindings.content_sha256, "content_sha256")
    _validate_expected_sessions(bindings.expected_sessions)


@dataclass(frozen=True, slots=True)
class ExternalBindings:
    """Typed control-plane receipt binding for one complete offline fixture file."""

    source_sha256: str
    calendar_sha256: str
    manifest_sha256: str
    content_sha256: str
    expected_sessions: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_external_bindings(self)


@dataclass(frozen=True, slots=True, init=False)
class TrustedSnapshotPackage:
    """Immutable package that only this module's strict factory can create."""

    canonical_bytes: bytes
    external_bindings: ExternalBindings
    snapshot_id: str

    def __init__(self, *, _token: object, canonical_bytes: bytes, external_bindings: ExternalBindings) -> None:
        if _token is not _PACKAGE_TOKEN:
            raise TypeError("TrustedSnapshotPackage must be created by build_trusted_snapshot_package")
        object.__setattr__(self, "canonical_bytes", canonical_bytes)
        object.__setattr__(self, "external_bindings", external_bindings)
        object.__setattr__(self, "snapshot_id", f"sha256-{external_bindings.manifest_sha256}")


def public_json_schema() -> dict[str, object]:
    """Generate the checked-in public schema from the runtime constants."""
    decimal_schema = {"maxLength": MAX_DECIMAL_LENGTH, "pattern": rf"^{CANONICAL_DECIMAL_PATTERN}$", "type": "string"}
    row_schema = {
        "additionalProperties": False,
        "properties": {
            "adjusted_close": decimal_schema,
            "session": {"format": "date", "type": "string"},
            "symbol": {"enum": list(SYMBOLS), "type": "string"},
        },
        "required": sorted(ROW_FIELDS),
        "type": "object",
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": False,
        "properties": {
            "calendar_timezone": {"const": CALENDAR_TIMEZONE, "type": "string"},
            "generation": {"const": GENERATION, "type": "string"},
            "offline_fixture": {"const": True, "type": "boolean"},
            "pair_id": {"const": PAIR_ID, "type": "string"},
            "plugin": {"const": PLUGIN, "type": "string"},
            "rows": {"items": row_schema, "minItems": 2, "type": "array"},
            "schema_version": {"const": SCHEMA_VERSION, "type": "string"},
            "sessions": {
                "items": {"format": "date", "type": "string"},
                "minItems": 1,
                "type": "array",
                "uniqueItems": True,
            },
            "size": {"const": 0, "type": "integer"},
            "symbols": {
                "items": False,
                "minItems": 2,
                "prefixItems": [{"const": SYMBOLS[0]}, {"const": SYMBOLS[1]}],
                "type": "array",
            },
        },
        "required": sorted(TOP_LEVEL_FIELDS),
        "type": "object",
    }


def _required_open_flags(*, directory: bool = False, nonblocking: bool = False) -> int:
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW
        if directory:
            flags |= os.O_DIRECTORY
        if nonblocking:
            flags |= os.O_NONBLOCK
    except AttributeError as exc:
        raise SnapshotValidationError("descriptor-safe open capability is unavailable") from exc
    if type(flags) is not int:
        _invalid("descriptor-safe open capability is unavailable")
    return flags


def _relative_components(relative_path: object) -> tuple[str, ...]:
    if not isinstance(relative_path, (str, Path)):
        _invalid("relative_path must be a path string")
    candidate = Path(relative_path)
    components = candidate.parts
    if candidate.is_absolute() or not components or any(component in {"", ".", ".."} for component in components):
        _invalid("relative_path must stay below trusted_root")
    return components


def _open_trusted_root(trusted_root: object) -> int:
    if not isinstance(trusted_root, (str, Path)):
        _invalid("trusted_root must be a path string")
    root_path = Path(trusted_root)
    if root_path.is_absolute():
        anchor = root_path.anchor
        components = root_path.parts[1:]
    else:
        anchor = "."
        components = root_path.parts
    if any(component == ".." for component in components):
        _invalid("trusted_root must not contain parent traversal")
    descriptor = os.open(anchor, _required_open_flags(directory=True))
    try:
        for component in components:
            child_descriptor = os.open(component, _required_open_flags(directory=True), dir_fd=descriptor)
            if not stat.S_ISDIR(os.fstat(child_descriptor).st_mode):
                os.close(child_descriptor)
                _invalid("trusted_root is not a directory")
            os.close(descriptor)
            descriptor = child_descriptor
        return descriptor
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _read_trusted_file(*, trusted_root: object, relative_path: object) -> bytes:
    components = _relative_components(relative_path)
    root_fd = -1
    parent_fd = -1
    file_fd = -1
    try:
        root_fd = _open_trusted_root(trusted_root)
        parent_fd = root_fd
        root_fd = -1
        for component in components[:-1]:
            child_fd = os.open(component, _required_open_flags(directory=True), dir_fd=parent_fd)
            if not stat.S_ISDIR(os.fstat(child_fd).st_mode):
                os.close(child_fd)
                _invalid("candidate ancestor is not a directory")
            os.close(parent_fd)
            parent_fd = child_fd
        file_fd = os.open(components[-1], _required_open_flags(nonblocking=True), dir_fd=parent_fd)
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            _invalid("candidate is not a regular file")
        if file_stat.st_size > MAX_READ_BYTES:
            _invalid("candidate exceeds maximum read size")
        data = bytearray()
        while True:
            chunk = os.read(file_fd, _READ_CHUNK_BYTES)
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > MAX_READ_BYTES:
                _invalid("candidate exceeds maximum read size")
        if len(data) != file_stat.st_size:
            _invalid("candidate changed during readback")
        return bytes(data)
    except SnapshotValidationError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise SnapshotValidationError("strict readback failed") from exc
    finally:
        for descriptor in (file_fd, parent_fd, root_fd):
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _reject_duplicate_keys(pairs: list[tuple[object, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if type(key) is not str or key in result:
            _invalid("duplicate or invalid JSON key")
        result[key] = value
    return result


def _parse_int(value: str) -> int:
    if len(value.lstrip("-")) > MAX_JSON_INT_DIGITS:
        _invalid("JSON integer is too large")
    try:
        return int(value)
    except ValueError as exc:
        raise SnapshotValidationError("invalid JSON integer") from exc


def _reject_json_float(_: str) -> object:
    _invalid("JSON floats are not allowed")


def _parse_canonical_json(raw: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_float,
            parse_float=_reject_json_float,
            parse_int=_parse_int,
        )
    except SnapshotValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
        raise SnapshotValidationError("invalid UTF-8 JSON") from exc
    if type(parsed) is not dict:
        _invalid("snapshot payload must be a JSON object")
    if canonical_json_bytes(parsed) != raw:
        _invalid("snapshot payload is not canonical JSON")
    return parsed


def _validate_decimal(value: object) -> None:
    if type(value) is not str or len(value) > MAX_DECIMAL_LENGTH or _DECIMAL_RE.fullmatch(value) is None:
        _invalid("invalid canonical adjusted_close")
    try:
        decimal_value = Decimal(value)
    except InvalidOperation as exc:
        raise SnapshotValidationError("invalid canonical adjusted_close") from exc
    if not decimal_value.is_finite() or decimal_value <= 0:
        _invalid("adjusted_close is outside the permitted finite range")


def _validate_payload(payload: dict[str, object], bindings: ExternalBindings) -> None:
    if set(payload) != TOP_LEVEL_FIELDS:
        _invalid("snapshot payload fields are invalid")
    constants = {
        "calendar_timezone": CALENDAR_TIMEZONE,
        "generation": GENERATION,
        "offline_fixture": True,
        "pair_id": PAIR_ID,
        "plugin": PLUGIN,
        "schema_version": SCHEMA_VERSION,
        "size": 0,
        "symbols": list(SYMBOLS),
    }
    for field, expected in constants.items():
        if payload.get(field) != expected or type(payload.get(field)) is not type(expected):
            _invalid(f"snapshot {field} is invalid")
    sessions = payload.get("sessions")
    if type(sessions) is not list or any(not _is_canonical_session(session) for session in sessions):
        _invalid("snapshot sessions are invalid")
    if tuple(sessions) != bindings.expected_sessions:
        _invalid("snapshot sessions do not match external calendar coverage")
    rows = payload.get("rows")
    if type(rows) is not list or len(rows) != 2 * len(bindings.expected_sessions):
        _invalid("snapshot rows are incomplete")
    expected_keys = [(session, symbol) for session in bindings.expected_sessions for symbol in SYMBOLS]
    observed_keys: list[tuple[str, str]] = []
    for row in rows:
        if type(row) is not dict or set(row) != ROW_FIELDS:
            _invalid("snapshot row fields are invalid")
        session = row.get("session")
        symbol = row.get("symbol")
        if not _is_canonical_session(session) or type(symbol) is not str or symbol not in SYMBOLS:
            _invalid("snapshot row identity is invalid")
        _validate_decimal(row.get("adjusted_close"))
        observed_keys.append((session, symbol))
    if observed_keys != expected_keys:
        _invalid("snapshot rows must be sorted, unique, and pair-complete")


def build_trusted_snapshot_package(
    *, trusted_root: Path | str, relative_path: Path | str, bindings: ExternalBindings
) -> TrustedSnapshotPackage:
    """Read, externally bind, and validate one offline fixture into the only public package type."""
    if type(bindings) is not ExternalBindings:
        _invalid("bindings must be an ExternalBindings value")
    _validate_external_bindings(bindings)
    raw = _read_trusted_file(trusted_root=trusted_root, relative_path=relative_path)
    if hashlib.sha256(raw).hexdigest() != bindings.content_sha256:
        _invalid("external content digest does not match before JSON parsing")
    payload = _parse_canonical_json(raw)
    _validate_payload(payload, bindings)
    return TrustedSnapshotPackage(_token=_PACKAGE_TOKEN, canonical_bytes=raw, external_bindings=bindings)


def validate_trusted_snapshot_package(package: object) -> TrustedSnapshotPackage:
    """Reject every consumer input except a factory-created immutable package."""
    if type(package) is not TrustedSnapshotPackage:
        _invalid("consumer input must be a TrustedSnapshotPackage")
    if type(package.canonical_bytes) is not bytes or type(package.external_bindings) is not ExternalBindings:
        _invalid("trusted package has invalid fields")
    _validate_external_bindings(package.external_bindings)
    if hashlib.sha256(package.canonical_bytes).hexdigest() != package.external_bindings.content_sha256:
        _invalid("trusted package content digest is invalid")
    expected_id = f"sha256-{package.external_bindings.manifest_sha256}"
    if package.snapshot_id != expected_id:
        _invalid("trusted package snapshot_id is invalid")
    payload = _parse_canonical_json(package.canonical_bytes)
    _validate_payload(payload, package.external_bindings)
    return package
