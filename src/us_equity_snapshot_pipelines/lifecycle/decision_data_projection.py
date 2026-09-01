"""Publish portable daily decision-data projections from verified P1 roots.

Native P1 roots remain the source-specific evidence boundary.  This module
does not acquire market data or contact storage; it only derives a small,
immutable and provider-neutral projection after the caller has supplied a
P1 verifier for the source root.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path

from quant_platform_kit.data import (
    DECISION_DATA_ASSURANCE_VERIFIED,
    DECISION_DATA_MODE_ARTIFACT_OPTIONAL,
    DECISION_PRICE_SERIES_MEMBER_PATH,
    DecisionDataBinding,
    canonical_decision_price_series_artifact_bytes,
    read_decision_price_series_artifact_json,
    verify_decision_price_series_artifact_members,
)
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    read_research_input_manifest_json,
    research_input_manifest_sha256,
)

_OUTPUT_FILENAMES = frozenset({"manifest.json", DECISION_PRICE_SERIES_MEMBER_PATH})


class DecisionDataProjectionError(ValueError):
    """Sanitized failure for a derived decision-data projection."""


def _fail() -> None:
    raise DecisionDataProjectionError("invalid verified decision-data projection")


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        _fail()


def _require_identifier(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail()
    return value


def _verified_parent_manifest(root: Path, parent_verifier: Callable[[Path], str]) -> tuple[str, dict[str, object]]:
    try:
        parent_sha256 = parent_verifier(root)
        if not isinstance(parent_sha256, str) or len(parent_sha256) != 64:
            _fail()
        manifest_bytes = (root / "manifest.json").read_bytes()
        manifest = read_research_input_manifest_json(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            _fail()
        if research_input_manifest_sha256(manifest) != parent_sha256:
            _fail()
        return parent_sha256, manifest
    except (DecisionDataProjectionError, OSError, TypeError, ValueError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def _verify_parent_bars_member(root: Path, parent_manifest: Mapping[str, object]) -> None:
    try:
        bars_bytes = (root / "bars.json").read_bytes()
        members = parent_manifest.get("members")
        if not isinstance(members, list):
            _fail()
        matching = [member for member in members if isinstance(member, Mapping) and member.get("path") == "bars.json"]
        if len(matching) != 1:
            _fail()
        member = matching[0]
        if (
            member.get("size_bytes") != len(bars_bytes)
            or member.get("sha256") != hashlib.sha256(bars_bytes).hexdigest()
        ):
            _fail()
    except (DecisionDataProjectionError, OSError, TypeError, ValueError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def _normalize_series(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, Mapping) or not value:
        _fail()
    normalized: dict[str, dict[str, object]] = {}
    for symbol, raw_points in value.items():
        if not isinstance(symbol, str) or not isinstance(raw_points, list) or not raw_points:
            _fail()
        normalized[symbol] = {"currency": "USD", "points": raw_points}
    return normalized


def _projection_artifact(
    *,
    parent_manifest: Mapping[str, object],
    strategy_scope: str,
    series: Mapping[str, object],
) -> dict[str, object]:
    calendar = parent_manifest.get("calendar")
    adjustment = parent_manifest.get("adjustment")
    sources = parent_manifest.get("sources")
    if not isinstance(calendar, Mapping) or not isinstance(adjustment, Mapping) or not isinstance(sources, list):
        _fail()
    session_date = calendar.get("session_date")
    policy = adjustment.get("policy")
    source_ids = [source.get("source_id") for source in sources if isinstance(source, Mapping)]
    if len(source_ids) != len(sources):
        _fail()
    artifact = {
        "schema_version": "qpk.decision_price_series_artifact.v1",
        "strategy_scope": _require_identifier(strategy_scope),
        "as_of": session_date,
        "adjustment_basis": policy,
        "source_ids": source_ids,
        "series": _normalize_series(series),
    }
    # QPK owns the strict schema, timestamp/price validation and canonical form.
    return read_decision_price_series_artifact_json(
        canonical_decision_price_series_artifact_bytes(artifact)
    )


def _projection_manifest(
    *,
    parent_manifest: Mapping[str, object],
    parent_sha256: str,
    projection_bytes: bytes,
    producer: Mapping[str, object],
) -> dict[str, object]:
    try:
        contract_id = _require_identifier(parent_manifest["research_input_contract_id"])
        manifest = {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": (
                f"decision-price-series-{parent_sha256[:24]}-"
                f"{hashlib.sha256(projection_bytes).hexdigest()[:24]}"
            ),
            "research_input_contract_id": f"{contract_id}.decision_price_series_projection.v1",
            "domain": parent_manifest["domain"],
            "profile": parent_manifest["profile"],
            "artifact_type": "immutable_verified_daily_decision_price_series_projection",
            "observed_at": parent_manifest["observed_at"],
            "effective_at": parent_manifest["effective_at"],
            "as_of": parent_manifest["as_of"],
            "producer": dict(producer),
            "calendar": dict(parent_manifest["calendar"]),
            "adjustment": dict(parent_manifest["adjustment"]),
            "sources": [dict(source) for source in parent_manifest["sources"]],
            "members": [
                {
                    "path": DECISION_PRICE_SERIES_MEMBER_PATH,
                    "media_type": "application/json",
                    "size_bytes": len(projection_bytes),
                    "sha256": hashlib.sha256(projection_bytes).hexdigest(),
                }
            ],
            "parent_manifest_sha256": parent_sha256,
        }
        manifest_bytes = canonical_research_input_manifest_bytes(manifest)
        return read_research_input_manifest_json(manifest_bytes)
    except (DecisionDataProjectionError, KeyError, TypeError, ValueError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def _projection_binding(
    *,
    projection: Mapping[str, object],
    manifest_sha256: str,
) -> DecisionDataBinding:
    try:
        return DecisionDataBinding(
            binding_id="p1-daily-decision-projection",
            strategy_scope=projection["strategy_scope"],
            mode=DECISION_DATA_MODE_ARTIFACT_OPTIONAL,
            source_ids=tuple(projection["source_ids"]),
            as_of=projection["as_of"],
            adjustment_basis=projection["adjustment_basis"],
            artifact_sha256=manifest_sha256,
            assurance_status=DECISION_DATA_ASSURANCE_VERIFIED,
        )
    except (KeyError, TypeError, ValueError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def verify_decision_price_series_projection_root(output_root: str | Path) -> str:
    """Verify a standalone, immutable projection root without provider access."""

    root = Path(output_root)
    try:
        root_stat = root.lstat()
        if (
            stat.S_ISLNK(root_stat.st_mode)
            or not stat.S_ISDIR(root_stat.st_mode)
            or stat.S_IMODE(root_stat.st_mode) != 0o700
            or {entry.name for entry in root.iterdir()} != _OUTPUT_FILENAMES
        ):
            _fail()
        manifest_bytes = (root / "manifest.json").read_bytes()
        projection_bytes = (root / DECISION_PRICE_SERIES_MEMBER_PATH).read_bytes()
        manifest = read_research_input_manifest_json(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            _fail()
        if manifest.get("artifact_type") != "immutable_verified_daily_decision_price_series_projection":
            _fail()
        members = manifest.get("members")
        if (
            not isinstance(members, list)
            or {member.get("path") for member in members if isinstance(member, Mapping)}
            != {DECISION_PRICE_SERIES_MEMBER_PATH}
            or len(members) != 1
        ):
            _fail()
        parent_sha256 = manifest.get("parent_manifest_sha256")
        if not isinstance(parent_sha256, str) or len(parent_sha256) != 64:
            _fail()
        manifest_sha256 = research_input_manifest_sha256(manifest)
        projection = read_decision_price_series_artifact_json(projection_bytes)
        verify_decision_price_series_artifact_members(
            binding=_projection_binding(projection=projection, manifest_sha256=manifest_sha256),
            manifest_bytes=manifest_bytes,
            decision_price_series_bytes=projection_bytes,
        )
        return manifest_sha256
    except (DecisionDataProjectionError, OSError, TypeError, ValueError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def _publish_noreplace(source: Path, destination: Path) -> None:
    if not sys.platform.startswith("linux"):
        _fail()
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        _fail()
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    parent_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    parent_fd = os.open(destination.parent, parent_flags)
    try:
        result = renameat2(parent_fd, source.name.encode(), parent_fd, destination.name.encode(), 1)
    finally:
        os.close(parent_fd)
    if result != 0:
        _fail()


def publish_verified_daily_price_series_projection(
    *,
    parent_root: str | Path,
    output_root: str | Path,
    parent_verifier: Callable[[Path], str],
    strategy_scope: str,
    series_extractor: Callable[[Path], Mapping[str, object]],
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Create a no-clobber projection only after a native P1 verifier passes."""

    root = Path(parent_root)
    destination = Path(output_root)
    if destination.exists() or destination.is_symlink() or destination.parent.is_symlink() or not destination.parent.is_dir():
        _fail()
    parent_sha256, parent_manifest = _verified_parent_manifest(root, parent_verifier)
    _verify_parent_bars_member(root, parent_manifest)
    try:
        series = series_extractor(root)
    except Exception:  # noqa: BLE001 - adapter failures must not disclose source-specific details
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None
    verified_after_extract, parent_manifest_after_extract = _verified_parent_manifest(root, parent_verifier)
    if verified_after_extract != parent_sha256:
        _fail()
    _verify_parent_bars_member(root, parent_manifest_after_extract)
    projection = _projection_artifact(
        parent_manifest=parent_manifest,
        strategy_scope=strategy_scope,
        series=series,
    )
    projection_bytes = canonical_decision_price_series_artifact_bytes(projection)
    manifest = _projection_manifest(
        parent_manifest=parent_manifest,
        parent_sha256=parent_sha256,
        projection_bytes=projection_bytes,
        producer=producer,
    )
    manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / DECISION_PRICE_SERIES_MEMBER_PATH).write_bytes(projection_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        manifest_sha256 = verify_decision_price_series_projection_root(temporary)
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        "status": "DECISION_PRICE_SERIES_PROJECTION_PUBLISHED",
        "manifest_sha256": manifest_sha256,
        "parent_manifest_sha256": parent_sha256,
    }


def extract_tqqq_bars_daily_series(root: str | Path) -> dict[str, list[dict[str, object]]]:
    """Adapt the verified TQQQ P1 OHLCV member into the portable daily shape."""

    try:
        payload = _read_json(Path(root) / "bars.json")
        if not isinstance(payload, Mapping) or set(payload) != {"schema_version", "symbols"}:
            _fail()
        symbols = payload.get("symbols")
        if not isinstance(symbols, Mapping) or not symbols:
            _fail()
        result: dict[str, list[dict[str, object]]] = {}
        for symbol, raw in symbols.items():
            if not isinstance(symbol, str) or not isinstance(raw, Mapping) or set(raw) != {"bars"}:
                _fail()
            rows = raw["bars"]
            if not isinstance(rows, list) or not rows:
                _fail()
            result[symbol] = [
                {"as_of": row["date"], "close": row["close"], "volume": row.get("volume")}
                for row in rows
                if isinstance(row, Mapping)
            ]
            if len(result[symbol]) != len(rows):
                _fail()
        return result
    except (DecisionDataProjectionError, KeyError, TypeError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


def extract_soxl_bars_daily_series(root: str | Path) -> dict[str, list[dict[str, object]]]:
    """Adapt the verified SOXL P1 bar member into the portable daily shape."""

    try:
        payload = _read_json(Path(root) / "bars.json")
        if not isinstance(payload, Mapping) or set(payload) != {"schema_version", "series"}:
            _fail()
        source_series = payload.get("series")
        if not isinstance(source_series, Mapping) or not source_series:
            _fail()
        result: dict[str, list[dict[str, object]]] = {}
        for symbol, rows in source_series.items():
            if not isinstance(symbol, str) or not isinstance(rows, list) or not rows:
                _fail()
            result[symbol] = [
                {
                    "as_of": row["session_date"],
                    "close": row["bar"]["close"],
                    "volume": row["bar"].get("volume"),
                }
                for row in rows
                if isinstance(row, Mapping) and isinstance(row.get("bar"), Mapping)
            ]
            if len(result[symbol]) != len(rows):
                _fail()
        return result
    except (DecisionDataProjectionError, KeyError, TypeError):
        raise DecisionDataProjectionError("invalid verified decision-data projection") from None


__all__ = [
    "DecisionDataProjectionError",
    "extract_soxl_bars_daily_series",
    "extract_tqqq_bars_daily_series",
    "publish_verified_daily_price_series_projection",
    "verify_decision_price_series_projection_root",
]
