"""Local descriptor-backed verification for immutable TQQQ snapshot evidence."""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import stat
from dataclasses import dataclass, field
from datetime import date
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd

from . import tqqq_r1_snapshot

_PACKAGE_VERSION = "tqqq_trusted_snapshot_package.v1"
_RECEIPT_VERSION = "tqqq_snapshot_receipt.v1"
_CALENDAR_VERSION = "tqqq_offline_calendar_evidence.v1"
_HEX_SHA256_LENGTH = 64
MAX_EVIDENCE_BYTES = 1024 * 1024
_MAX_SNAPSHOT_MEMBER_BYTES = 16 * 1024 * 1024
_MAX_JSON_INTEGER_DIGITS = 309
_VERIFIED_CONSTRUCTION = object()


class TrustedSnapshotPackageError(ValueError):
    """Raised when untrusted local package evidence cannot be verified."""


@dataclass(frozen=True, init=False)
class TrustedSnapshotPackage:
    """Verified immutable bytes plus one owned directory descriptor."""

    snapshot_dir: Path
    session: str
    snapshot_manifest_sha256: str
    receipt_sha256: str
    calendar_sha256: str
    _snapshot_fd: int = field(repr=False, compare=False)
    _snapshot_members: tuple[tuple[str, bytes], ...] = field(
        repr=False,
        compare=False,
    )
    _lifecycle_lock: Lock = field(repr=False, compare=False)

    __hash__ = None

    def __init__(
        self,
        *,
        _verified: object,
        snapshot_dir: Path,
        session: str,
        snapshot_manifest_sha256: str,
        receipt_sha256: str,
        calendar_sha256: str,
        snapshot_fd: int,
        snapshot_members: tuple[tuple[str, bytes], ...],
    ) -> None:
        if _verified is not _VERIFIED_CONSTRUCTION:
            raise TrustedSnapshotPackageError(
                "TrustedSnapshotPackage requires verified loader",
            )
        object.__setattr__(self, "snapshot_dir", snapshot_dir)
        object.__setattr__(self, "session", session)
        object.__setattr__(
            self,
            "snapshot_manifest_sha256",
            snapshot_manifest_sha256,
        )
        object.__setattr__(self, "receipt_sha256", receipt_sha256)
        object.__setattr__(self, "calendar_sha256", calendar_sha256)
        object.__setattr__(self, "_snapshot_fd", snapshot_fd)
        object.__setattr__(self, "_snapshot_members", snapshot_members)
        object.__setattr__(self, "_lifecycle_lock", Lock())

    def close(self) -> None:
        """Atomically claim and release the owned descriptor exactly once."""
        with self._lifecycle_lock:
            descriptor = self._snapshot_fd
            if descriptor < 0:
                return
            object.__setattr__(self, "_snapshot_fd", -1)
        try:
            os.close(descriptor)
        except OSError:
            pass

    def read_snapshot_member(self, name: str) -> bytes:
        """Return immutable verified bytes; never reopen a mutable member path."""
        with self._lifecycle_lock:
            if self._snapshot_fd < 0:
                raise TrustedSnapshotPackageError(
                    "trusted snapshot package is closed",
                )
            members = dict(self._snapshot_members)
        if type(name) is not str or name not in members:
            raise TrustedSnapshotPackageError(
                "invalid verified snapshot member",
            )
        return members[name]

    def __enter__(self) -> TrustedSnapshotPackage:
        return self

    def __exit__(
        self,
        _type: object,
        _value: object,
        _traceback: object,
    ) -> None:
        self.close()

    def __copy__(self) -> TrustedSnapshotPackage:
        raise TrustedSnapshotPackageError(
            "trusted snapshot package cannot be copied",
        )

    def __deepcopy__(
        self,
        _memo: dict[int, object],
    ) -> TrustedSnapshotPackage:
        raise TrustedSnapshotPackageError(
            "trusted snapshot package cannot be copied",
        )

    def __reduce_ex__(self, _protocol: int) -> object:
        raise TrustedSnapshotPackageError(
            "trusted snapshot package cannot be pickled",
        )

    def __del__(self) -> None:
        try:
            self.close()
        except (AttributeError, TypeError):
            pass


def _invalid(message: str) -> None:
    raise TrustedSnapshotPackageError(message)


def _safe_open_flags(
    *,
    directory: bool = False,
    write: bool = False,
) -> int:
    if os.name != "posix":
        _invalid("descriptor-safe trusted-input capability is unavailable")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    non_blocking = getattr(os, "O_NONBLOCK", 0)
    directory_flag = getattr(os, "O_DIRECTORY", 0) if directory else 0
    if not no_follow or not non_blocking or (directory and not directory_flag):
        _invalid("descriptor-safe trusted-input capability is unavailable")
    access = os.O_WRONLY | os.O_CREAT if write else os.O_RDONLY
    return access | no_follow | non_blocking | directory_flag


def _read_bounded_regular_descriptor(
    descriptor: int,
    *,
    limit: int,
    nonregular_message: str,
    oversized_message: str,
    changed_message: str,
    read_error_message: str,
) -> bytes:
    try:
        initial = os.fstat(descriptor)
        if not stat.S_ISREG(initial.st_mode):
            _invalid(nonregular_message)
        if initial.st_size < 0 or initial.st_size > limit:
            _invalid(oversized_message)
        chunks: list[bytes] = []
        received = 0
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, limit - received + 1),
            )
            if not chunk:
                break
            received += len(chunk)
            if received > limit:
                _invalid(oversized_message)
            chunks.append(chunk)
        final = os.fstat(descriptor)
        stable_metadata = (
            final.st_dev == initial.st_dev
            and final.st_ino == initial.st_ino
            and final.st_mode == initial.st_mode
            and final.st_size == initial.st_size
            and final.st_mtime_ns == initial.st_mtime_ns
            and final.st_ctime_ns == initial.st_ctime_ns
        )
        if not stable_metadata or received != initial.st_size:
            _invalid(changed_message)
        return b"".join(chunks)
    except OSError as exc:
        raise TrustedSnapshotPackageError(read_error_message) from exc


def _read_regular(path: str | Path, name: str) -> bytes:
    """Read one bounded, non-symlink regular file through its descriptor."""
    try:
        descriptor = os.open(os.fspath(path), _safe_open_flags())
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            _invalid(f"{name} must be a regular non-symlink file")
        raise TrustedSnapshotPackageError(f"unable to read {name}") from exc
    try:
        return _read_bounded_regular_descriptor(
            descriptor,
            limit=MAX_EVIDENCE_BYTES,
            nonregular_message=f"{name} must be a regular non-symlink file",
            oversized_message=f"{name} exceeds size limit",
            changed_message=f"{name} changed while reading",
            read_error_message=f"unable to read {name}",
        )
    finally:
        os.close(descriptor)


def _no_duplicates(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
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


def _parse_bounded_int(value: str) -> int:
    if len(value.lstrip("-")) > _MAX_JSON_INTEGER_DIGITS:
        _invalid("invalid strict JSON")
    return int(value)


def read_strict_json(payload: bytes, name: str) -> object:
    """Decode bounded finite duplicate-free JSON and normalize failures."""
    if type(payload) is not bytes or type(name) is not str:
        _invalid("invalid strict JSON")
    if len(payload) > MAX_EVIDENCE_BYTES:
        _invalid(f"{name} exceeds size limit")
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_no_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_finite_float,
            parse_int=_parse_bounded_int,
        )
    except TrustedSnapshotPackageError:
        raise
    except (
        UnicodeDecodeError,
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
    ) as exc:
        raise TrustedSnapshotPackageError(
            f"invalid strict JSON: {name}",
        ) from exc


def write_strict_json(path: str | Path, payload: object) -> None:
    """Write canonical finite JSON without following a destination symlink."""
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise TrustedSnapshotPackageError(
            "non-finite or invalid strict JSON",
        ) from exc
    if len(encoded) + 1 > MAX_EVIDENCE_BYTES:
        _invalid("strict JSON exceeds size limit")
    try:
        descriptor = os.open(
            os.fspath(path),
            _safe_open_flags(write=True),
            0o600,
        )
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                _invalid("strict JSON destination must be a regular file")
            os.ftruncate(descriptor, 0)
            output = os.fdopen(descriptor, "wb")
            descriptor = -1
            with output:
                output.write(encoded + b"\n")
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    except OSError as exc:
        raise TrustedSnapshotPackageError(
            "unable to write strict JSON",
        ) from exc


def _admit_snapshot_dir(snapshot_dir: str | Path) -> str:
    try:
        raw_path = os.fspath(snapshot_dir)
    except TypeError as exc:
        raise TrustedSnapshotPackageError(
            "invalid snapshot directory",
        ) from exc
    if type(raw_path) is not str or not raw_path or "\x00" in raw_path:
        _invalid("invalid snapshot directory")

    separators = [os.sep]
    if os.altsep and os.altsep != os.sep:
        separators.append(os.altsep)
    if any(
        raw_path.endswith(separator)
        or raw_path.endswith(separator + ".")
        for separator in separators
    ):
        _invalid("invalid snapshot directory")

    components = [raw_path]
    for separator in separators:
        components = [part for value in components for part in value.split(separator)]
    if ".." in components:
        _invalid("invalid snapshot directory")
    return raw_path


def _open_snapshot_directory(snapshot_dir: str | Path) -> int:
    raw_path = _admit_snapshot_dir(snapshot_dir)
    try:
        return os.open(
            raw_path,
            _safe_open_flags(directory=True),
        )
    except OSError as exc:
        raise TrustedSnapshotPackageError(
            "unable to open stable snapshot directory",
        ) from exc


def _read_snapshot_member(directory_fd: int, name: str) -> bytes:
    try:
        descriptor = os.open(
            name,
            _safe_open_flags(),
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise TrustedSnapshotPackageError(
            "unable to read snapshot members",
        ) from exc
    try:
        return _read_bounded_regular_descriptor(
            descriptor,
            limit=_MAX_SNAPSHOT_MEMBER_BYTES,
            nonregular_message=(
                "snapshot members must be regular non-symlink files"
            ),
            oversized_message="snapshot member exceeds size limit",
            changed_message="snapshot member changed while reading",
            read_error_message="unable to read snapshot members",
        )
    finally:
        os.close(descriptor)


def _is_sha256(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == _HEX_SHA256_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def _exact_object(
    value: object,
    keys: set[str],
    name: str,
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        _invalid(f"invalid {name}")
    return value


def _verify_snapshot_fd(
    directory_fd: int,
    *,
    expected_manifest_sha256: str,
) -> tuple[
    str,
    tuple[tuple[str, bytes], ...],
    frozenset[str],
]:
    try:
        names = tuple(sorted(os.listdir(directory_fd)))
    except OSError as exc:
        raise TrustedSnapshotPackageError(
            "unable to read snapshot members",
        ) from exc
    if names != tuple(sorted(tqqq_r1_snapshot.OUTPUT_FILENAMES)):
        _invalid(f"unexpected output files: {names}")

    members = {
        name: _read_snapshot_member(directory_fd, name)
        for name in tqqq_r1_snapshot.OUTPUT_FILENAMES
    }
    member_hashes = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in members.items()
    }
    if (
        not _is_sha256(expected_manifest_sha256)
        or member_hashes["manifest.json"] != expected_manifest_sha256
    ):
        _invalid("trusted manifest hash mismatch")

    try:
        manifest = _exact_object(
            read_strict_json(members["manifest.json"], "snapshot manifest"),
            {
                "contract_version",
                "symbols",
                "requested_lower_bound",
                "price_field",
                "plugin",
                "mode",
                "size",
                "row_count",
                "prices_sha256",
            },
            "manifest",
        )
        if not _is_sha256(manifest["prices_sha256"]):
            _invalid("invalid manifest")
        if member_hashes["prices.csv"] != manifest["prices_sha256"]:
            _invalid("hash mismatch: prices.csv")

        prices = tqqq_r1_snapshot._normalized_prices(
            pd.read_csv(
                BytesIO(members["prices.csv"]),
                dtype={tqqq_r1_snapshot.PRICE_FIELD: "string"},
            ),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
        )
        verified_sessions = frozenset(
            prices["session"].dt.strftime("%Y-%m-%d").tolist(),
        )

        sums = _exact_object(
            read_strict_json(members["sha256sums.json"], "sha256sums"),
            {"prices.csv", "manifest.json", "validation.json"},
            "sha256sums",
        )
        for name, expected in sums.items():
            if not _is_sha256(expected) or member_hashes[name] != expected:
                _invalid(f"hash mismatch: {name}")
        validation = _exact_object(
            read_strict_json(members["validation.json"], "validation"),
            {"valid", "row_count", "symbols"},
            "validation",
        )
    except TrustedSnapshotPackageError:
        raise
    except (
        UnicodeDecodeError,
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
    ) as exc:
        raise TrustedSnapshotPackageError(
            "invalid verified snapshot",
        ) from exc

    expected_manifest = {
        "contract_version": tqqq_r1_snapshot.CONTRACT_VERSION,
        "symbols": list(tqqq_r1_snapshot.SYMBOLS),
        "requested_lower_bound": tqqq_r1_snapshot.REQUESTED_LOWER_BOUND,
        "price_field": tqqq_r1_snapshot.PRICE_FIELD,
        "plugin": tqqq_r1_snapshot.PLUGIN,
        "mode": tqqq_r1_snapshot.MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": member_hashes["prices.csv"],
    }
    if not tqqq_r1_snapshot._has_exact_type(
        manifest,
        expected_manifest,
    ):
        _invalid("invalid manifest")
    expected_validation = {
        "valid": True,
        "row_count": len(prices),
        "symbols": list(tqqq_r1_snapshot.SYMBOLS),
    }
    if not tqqq_r1_snapshot._has_exact_type(
        validation,
        expected_validation,
    ):
        _invalid("invalid validation")
    return (
        member_hashes["manifest.json"],
        tuple(sorted(members.items())),
        verified_sessions,
    )


def _session(value: object, name: str) -> str:
    if type(value) is not str or len(value) != 10:
        _invalid(f"invalid {name} session")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise TrustedSnapshotPackageError(
            f"invalid {name} session",
        ) from exc
    if parsed.isoformat() != value or parsed.weekday() >= 5:
        _invalid(f"invalid {name} session")
    return value


def _package_manifest(value: object) -> dict[str, Any]:
    manifest = _exact_object(
        value,
        {
            "contract_version",
            "snapshot_manifest_sha256",
            "receipt_sha256",
            "calendar_sha256",
            "session",
        },
        "package manifest",
    )
    if manifest["contract_version"] != _PACKAGE_VERSION or not all(
        _is_sha256(manifest[key])
        for key in (
            "snapshot_manifest_sha256",
            "receipt_sha256",
            "calendar_sha256",
        )
    ):
        _invalid("invalid package manifest")
    _session(manifest["session"], "package")
    return manifest


def _receipt(value: object) -> dict[str, Any]:
    receipt = _exact_object(
        value,
        {
            "contract_version",
            "snapshot_manifest_sha256",
            "calendar_sha256",
            "session",
        },
        "receipt",
    )
    if receipt["contract_version"] != _RECEIPT_VERSION or not all(
        _is_sha256(receipt[key])
        for key in (
            "snapshot_manifest_sha256",
            "calendar_sha256",
        )
    ):
        _invalid("invalid receipt")
    _session(receipt["session"], "receipt")
    return receipt


def _calendar(value: object) -> dict[str, Any]:
    calendar = _exact_object(
        value,
        {"contract_version", "calendar", "sessions"},
        "calendar evidence",
    )
    if (
        calendar["contract_version"] != _CALENDAR_VERSION
        or calendar["calendar"] != "XNYS"
        or type(calendar["sessions"]) is not list
    ):
        _invalid("invalid calendar evidence")
    sessions = [
        _session(item, "calendar")
        for item in calendar["sessions"]
    ]
    if (
        not sessions
        or len(sessions) != len(set(sessions))
        or sessions != sorted(sessions)
    ):
        _invalid("invalid calendar evidence")
    return calendar


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
    """Authenticate anchors before parsing and retain descriptor-stable bytes."""
    if not all(
        _is_sha256(value)
        for value in (
            expected_snapshot_manifest_sha256,
            expected_package_manifest_sha256,
            expected_receipt_sha256,
        )
    ):
        _invalid("invalid expected evidence hash")

    raw_snapshot_dir = _admit_snapshot_dir(snapshot_dir)
    snapshot_fd = _open_snapshot_directory(raw_snapshot_dir)
    try:
        try:
            _, snapshot_members, verified_sessions = _verify_snapshot_fd(
                snapshot_fd,
                expected_manifest_sha256=expected_snapshot_manifest_sha256,
            )
        except Exception as exc:
            raise TrustedSnapshotPackageError(
                "invalid verified snapshot",
            ) from exc

        package_manifest_payload = _read_regular(
            package_manifest_path,
            "package manifest",
        )
        receipt_payload = _read_regular(receipt_path, "receipt")
        calendar_payload = _read_regular(
            calendar_evidence_path,
            "calendar evidence",
        )
        package_manifest_sha256 = hashlib.sha256(
            package_manifest_payload,
        ).hexdigest()
        receipt_sha256 = hashlib.sha256(receipt_payload).hexdigest()
        calendar_sha256 = hashlib.sha256(calendar_payload).hexdigest()

        if package_manifest_sha256 != expected_package_manifest_sha256:
            _invalid("package manifest hash binding mismatch")
        if receipt_sha256 != expected_receipt_sha256:
            _invalid("receipt hash binding mismatch")

        manifest = _package_manifest(
            read_strict_json(
                package_manifest_payload,
                "package manifest",
            ),
        )
        receipt = _receipt(
            read_strict_json(receipt_payload, "receipt"),
        )
        if (
            manifest["calendar_sha256"] != calendar_sha256
            or receipt["calendar_sha256"] != calendar_sha256
        ):
            _invalid("calendar hash binding mismatch")
        calendar = _calendar(
            read_strict_json(
                calendar_payload,
                "calendar evidence",
            ),
        )
        if (
            manifest["snapshot_manifest_sha256"]
            != expected_snapshot_manifest_sha256
            or receipt["snapshot_manifest_sha256"]
            != expected_snapshot_manifest_sha256
        ):
            _invalid("snapshot manifest hash binding mismatch")
        if manifest["receipt_sha256"] != receipt_sha256:
            _invalid("receipt hash binding mismatch")
        if (
            manifest["session"] != receipt["session"]
            or manifest["session"] not in calendar["sessions"]
        ):
            _invalid("calendar session binding mismatch")
        if manifest["session"] not in verified_sessions:
            _invalid("snapshot session binding mismatch")

        return TrustedSnapshotPackage(
            _verified=_VERIFIED_CONSTRUCTION,
            snapshot_dir=Path(raw_snapshot_dir),
            session=manifest["session"],
            snapshot_manifest_sha256=expected_snapshot_manifest_sha256,
            receipt_sha256=receipt_sha256,
            calendar_sha256=calendar_sha256,
            snapshot_fd=snapshot_fd,
            snapshot_members=snapshot_members,
        )
    except Exception:
        os.close(snapshot_fd)
        raise
