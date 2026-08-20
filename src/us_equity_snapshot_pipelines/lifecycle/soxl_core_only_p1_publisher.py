"""Create and verify immutable SOXL/SOXX core-only P1 input roots.

This module is deliberately narrow.  It accepts an injected, data-only
historical-bars provider; validates the fixed Alpaca SIP/XNYS/adjusted three
asset universe; and writes one private local root with create-only semantics.
It does not construct provider credentials, access cloud storage, schedule
work, run P3, derive targets, or create an order.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Protocol

from quant_platform_kit.data.research_input import canonical_research_input_manifest_bytes

from .soxl_core_only_p1_binding import (
    BARS_SCHEMA,
    SoxlCoreOnlyP1BindingError,
    build_soxl_core_only_input_manifest,
    build_soxl_core_only_p1_binding,
    canonical_soxl_core_only_p1_binding_bytes,
    canonical_soxl_core_only_source_series_bytes,
    expected_soxl_core_only_sessions,
    validate_soxl_core_only_input_manifest,
)

_UNIVERSE = ("SOXL", "SOXX", "BOXX")
_OUTPUT_FILENAMES = frozenset({"bars.json", "binding.json", "manifest.json"})
_REMOTE_COMPLETION_SCHEMA = "qsl.soxl-soxx-core-only-p1-remote-completion.v1"
REMOTE_COMPLETION_FILENAME = "p1-complete.json"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class SoxlCoreOnlyHistoricalBarsProvider(Protocol):
    """Injected P1 port for one fixed adjusted daily-bar request per symbol."""

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> Mapping[str, object]: ...


class SoxlCoreOnlyP1InputUnavailableError(SoxlCoreOnlyP1BindingError):
    """The sole frozen provider cannot currently produce a complete input."""


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _require_new_private_output_root(output_root: str | Path) -> Path:
    destination = Path(output_root)
    if destination.exists() or destination.is_symlink():
        raise SoxlCoreOnlyP1BindingError("immutable output already exists")
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise SoxlCoreOnlyP1BindingError("output parent is unavailable")
    return destination


def _publish_noreplace(source: Path, destination: Path) -> None:
    """Atomically make an already-verified root visible without replacement."""
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise SoxlCoreOnlyP1BindingError("required no-clobber capability unavailable")
        renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        renameat2.restype = ctypes.c_int
        parent_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
        parent_fd = os.open(destination.parent, parent_flags)
        try:
            result = renameat2(parent_fd, source.name.encode(), parent_fd, destination.name.encode(), 1)
        finally:
            os.close(parent_fd)
        if result != 0:
            if ctypes.get_errno() == errno.EEXIST:
                raise SoxlCoreOnlyP1BindingError("immutable output already exists")
            raise SoxlCoreOnlyP1BindingError("atomic no-clobber publish failed")
        return
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        renamex_np = getattr(libc, "renamex_np", None)
        if renamex_np is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise SoxlCoreOnlyP1BindingError("required no-clobber capability unavailable")
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        if renamex_np(str(source).encode(), str(destination).encode(), 4) != 0:
            if ctypes.get_errno() == errno.EEXIST:
                raise SoxlCoreOnlyP1BindingError("immutable output already exists")
            raise SoxlCoreOnlyP1BindingError("atomic no-clobber publish failed")
        return
    raise SoxlCoreOnlyP1BindingError("unsupported platform for atomic no-clobber publish")


def _normalized_provider_series(
    *, symbol: str, response: object, expected_sessions: tuple[date, ...]
) -> tuple[list[dict[str, object]], str]:
    """Normalize only the fixed provider shape after proving exact coverage."""
    if not isinstance(response, Mapping) or not isinstance(response.get("bars"), list):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only historical coverage")
    rows: list[dict[str, object]] = []
    observed_sessions: list[date] = []
    try:
        for raw in response["bars"]:
            if not isinstance(raw, Mapping) or set(raw) != {"date", "open", "high", "low", "close", "volume"}:
                raise TypeError
            session = date.fromisoformat(str(raw["date"]))
            if session.isoformat() != raw["date"]:
                raise ValueError
            observed_sessions.append(session)
            rows.append(
                {
                    "session_date": session.isoformat(),
                    "bar": {
                        "open": raw["open"],
                        "high": raw["high"],
                        "low": raw["low"],
                        "close": raw["close"],
                        "volume": raw["volume"],
                    },
                }
            )
    except (KeyError, TypeError, ValueError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only historical coverage") from None
    if tuple(observed_sessions) != expected_sessions:
        raise SoxlCoreOnlyP1BindingError("incomplete SOXL core-only historical coverage")
    source_bytes = canonical_soxl_core_only_source_series_bytes(symbol=symbol, series=rows)
    try:
        source = json.loads(source_bytes)
        normalized = source["sessions"]
    except (KeyError, TypeError, json.JSONDecodeError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only historical coverage") from None
    if not isinstance(normalized, list):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only historical coverage")
    return normalized, hashlib.sha256(source_bytes).hexdigest()


def _collect_frozen_three_inputs(
    provider: SoxlCoreOnlyHistoricalBarsProvider, binding: Mapping[str, object]
) -> tuple[dict[str, list[dict[str, object]]], dict[str, str]]:
    identity = binding["data_identity"]
    if not isinstance(identity, Mapping):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only P1 binding")
    calendar = identity["calendar"]
    adjustment = identity["adjustment"]
    if not isinstance(calendar, Mapping) or not isinstance(adjustment, Mapping):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only P1 binding")
    expected_by_symbol = expected_soxl_core_only_sessions(identity["date_cutoff"])
    series: dict[str, list[dict[str, object]]] = {}
    source_content_sha256: dict[str, str] = {}
    for symbol in _UNIVERSE:
        try:
            response = provider.fetch_historical_bars(
                symbol=symbol,
                calendar_id=str(calendar["calendar_id"]),
                timezone=str(calendar["timezone"]),
                adjustment_policy=str(adjustment["policy"]),
                feed=str(identity["feed"]),
                date_cutoff=str(identity["date_cutoff"]),
            )
        except SoxlCoreOnlyP1InputUnavailableError:
            raise
        except Exception:  # noqa: BLE001 - injected provider failures must not leak raw transport details
            raise SoxlCoreOnlyP1BindingError("data-only acquisition failed") from None
        normalized, digest = _normalized_provider_series(
            symbol=symbol, response=response, expected_sessions=expected_by_symbol[symbol]
        )
        series[symbol] = normalized
        source_content_sha256[symbol] = digest
    return series, source_content_sha256


def _bars_bytes(series: Mapping[str, object]) -> bytes:
    if set(series) != set(_UNIVERSE):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only historical coverage")
    return _canonical({"schema_version": BARS_SCHEMA, "series": {symbol: series[symbol] for symbol in _UNIVERSE}})


def publish_soxl_core_only_p1_inputs(
    provider: SoxlCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    date_cutoff: str,
) -> dict[str, object]:
    """Publish one candidate-bound immutable P1 root without fallback or overwrite."""
    destination = _require_new_private_output_root(output_root)
    binding = build_soxl_core_only_p1_binding(date_cutoff=date_cutoff)
    series, source_content_sha256 = _collect_frozen_three_inputs(provider, binding)
    bars_bytes = _bars_bytes(series)
    try:
        manifest = build_soxl_core_only_input_manifest(
            binding,
            observed_at=observed_at,
            producer=producer,
            member_bytes=bars_bytes,
            source_content_sha256=source_content_sha256,
        )
        manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    except (TypeError, ValueError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input") from None
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / "binding.json").write_bytes(canonical_soxl_core_only_p1_binding_bytes(binding))
        (temporary / "bars.json").write_bytes(bars_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        manifest_sha256 = verify_soxl_core_only_input_root(temporary)
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"manifest_sha256": manifest_sha256, "status": "P1_DATA_ONLY_INPUTS_PUBLISHED"}


def _verified_member_series(member_bytes: bytes, date_cutoff: object) -> dict[str, list[dict[str, object]]]:
    try:
        payload = json.loads(member_bytes)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root") from None
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"schema_version", "series"}
        or payload["schema_version"] != BARS_SCHEMA
        or not isinstance(payload["series"], Mapping)
        or set(payload["series"]) != set(_UNIVERSE)
    ):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root")
    expected_by_symbol = expected_soxl_core_only_sessions(date_cutoff)
    normalized: dict[str, list[dict[str, object]]] = {}
    for symbol in _UNIVERSE:
        source_bytes = canonical_soxl_core_only_source_series_bytes(
            symbol=symbol, series=payload["series"][symbol]
        )
        source = json.loads(source_bytes)
        sessions = source["sessions"]
        if not isinstance(sessions, list):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root")
        try:
            actual = tuple(date.fromisoformat(str(row["session_date"])) for row in sessions)
        except (KeyError, TypeError, ValueError):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root") from None
        if actual != expected_by_symbol[symbol]:
            raise SoxlCoreOnlyP1BindingError("incomplete SOXL core-only historical coverage")
        normalized[symbol] = sessions
    if _bars_bytes(normalized) != member_bytes:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root")
    return normalized


def verify_soxl_core_only_input_root(output_root: str | Path) -> str:
    """Verify one complete local immutable input root without provider access."""
    root = Path(output_root)
    try:
        root_stat = root.lstat()
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise ValueError
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise ValueError
        if {entry.name for entry in root.iterdir()} != _OUTPUT_FILENAMES:
            raise ValueError
        binding_bytes = (root / "binding.json").read_bytes()
        bars_bytes = (root / "bars.json").read_bytes()
        manifest_bytes = (root / "manifest.json").read_bytes()
        binding = json.loads(binding_bytes)
        if binding_bytes != canonical_soxl_core_only_p1_binding_bytes(binding):
            raise ValueError
        manifest = json.loads(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            raise ValueError
        manifest_sha256 = validate_soxl_core_only_input_manifest(manifest, binding)
        series = _verified_member_series(bars_bytes, binding["data_identity"]["date_cutoff"])
        members = manifest["members"]
        if (
            not isinstance(members, list)
            or len(members) != 1
            or members[0]
            != {
                "path": "bars.json",
                "media_type": "application/json",
                "size_bytes": len(bars_bytes),
                "sha256": hashlib.sha256(bars_bytes).hexdigest(),
            }
        ):
            raise ValueError
        source_hashes = {source["source_id"]: source["content_sha256"] for source in manifest["sources"]}
        expected_hashes = {
            f"alpaca_sip_1day_adjustment_all:{symbol}": hashlib.sha256(
                canonical_soxl_core_only_source_series_bytes(symbol=symbol, series=series[symbol])
            ).hexdigest()
            for symbol in _UNIVERSE
        }
        if source_hashes != expected_hashes:
            raise ValueError
        return manifest_sha256
    except (
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
        SoxlCoreOnlyP1BindingError,
    ):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root") from None


def build_soxl_core_only_p1_remote_completion(output_root: str | Path) -> dict[str, object]:
    """Bind all verified local P1 members before a remote completion is published.

    The caller is responsible for its remote create-only upload.  This helper
    only creates deterministic completion content and never contacts storage.
    """
    root = Path(output_root)
    manifest_sha256 = verify_soxl_core_only_input_root(root)
    try:
        return {
            "schema_version": _REMOTE_COMPLETION_SCHEMA,
            "manifest_sha256": manifest_sha256,
            "members": {
                filename: hashlib.sha256((root / filename).read_bytes()).hexdigest()
                for filename in sorted(_OUTPUT_FILENAMES)
            },
        }
    except OSError:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input root") from None


def canonical_soxl_core_only_p1_remote_completion_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and deterministically encode one remote P1 completion marker."""
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema_version", "manifest_sha256", "members"}
        or value.get("schema_version") != _REMOTE_COMPLETION_SCHEMA
        or not isinstance(value.get("manifest_sha256"), str)
        or not _DIGEST.fullmatch(value["manifest_sha256"])
        or not isinstance(value.get("members"), Mapping)
        or set(value["members"]) != _OUTPUT_FILENAMES
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in value["members"].values()
        )
    ):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only completion marker")
    return _canonical(
        {
            "schema_version": _REMOTE_COMPLETION_SCHEMA,
            "manifest_sha256": value["manifest_sha256"],
            "members": {filename: value["members"][filename] for filename in sorted(_OUTPUT_FILENAMES)},
        }
    )


def verify_soxl_core_only_p1_remote_completion(
    output_root: str | Path, completion_path: str | Path
) -> str:
    """Accept a copied P1 root only if its create-only completion exactly binds it."""
    expected = build_soxl_core_only_p1_remote_completion(output_root)
    try:
        marker_bytes = Path(completion_path).read_bytes()
        marker = json.loads(marker_bytes)
        if (
            marker_bytes != canonical_soxl_core_only_p1_remote_completion_bytes(marker)
            or marker != expected
        ):
            raise ValueError
        return str(expected["manifest_sha256"])
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only completion marker") from None
