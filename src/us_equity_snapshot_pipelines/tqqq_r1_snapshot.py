"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import hashlib
from io import BytesIO
import json
import math
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v1"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _normalized_prices(prices: pd.DataFrame, *, require_exact_columns: bool = False) -> pd.DataFrame:
    required = {"session", "symbol", PRICE_FIELD}
    missing = required.difference(prices.columns)
    if missing:
        raise SnapshotValidationError(f"missing required columns: {', '.join(sorted(missing))}")
    if require_exact_columns and list(prices.columns) != ["session", "symbol", PRICE_FIELD]:
        raise SnapshotValidationError("prices.csv must contain exact columns")
    normalized = prices.loc[:, ["session", "symbol", PRICE_FIELD]].copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    received = set(normalized["symbol"])
    if received != set(SYMBOLS):
        missing_symbols = sorted(set(SYMBOLS).difference(received))
        unexpected_symbols = sorted(received.difference(SYMBOLS))
        raise SnapshotValidationError(f"missing required symbol or unexpected symbol: missing={missing_symbols}, unexpected={unexpected_symbols}")
    normalized["session"] = pd.to_datetime(normalized["session"], errors="coerce")
    if normalized["session"].isna().any():
        raise SnapshotValidationError("invalid session")
    if normalized["session"].dt.tz is not None:
        raise SnapshotValidationError("timezone-aware session is not allowed")
    normalized["session"] = normalized["session"].dt.normalize()
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        raise SnapshotValidationError("session precedes requested lower bound")
    if (normalized["session"].dt.dayofweek >= 5).any():
        raise SnapshotValidationError("observed session must be a weekday")
    if normalized.duplicated(["session", "symbol"]).any():
        raise SnapshotValidationError("duplicate session for symbol")
    if not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        raise SnapshotValidationError("each session must contain exactly QQQ and TQQQ")
    if pd.api.types.is_bool_dtype(normalized[PRICE_FIELD]) or normalized[PRICE_FIELD].map(lambda value: isinstance(value, bool)).any():
        raise SnapshotValidationError("boolean adjusted_close is not allowed")
    normalized[PRICE_FIELD] = pd.to_numeric(normalized[PRICE_FIELD], errors="coerce")
    if normalized[PRICE_FIELD].isna().any() or not normalized[PRICE_FIELD].map(math.isfinite).all() or (normalized[PRICE_FIELD] <= 0).any():
        raise SnapshotValidationError("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _write_prices(path: Path, prices: pd.DataFrame) -> None:
    output = prices.copy()
    output["session"] = output["session"].dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.17g")


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    output = Path(output_dir)
    if output.is_symlink():
        raise SnapshotValidationError("snapshot root symlink is not allowed")
    names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        raise SnapshotValidationError(f"unexpected output files: {names}")
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in OUTPUT_FILENAMES):
        raise SnapshotValidationError("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in OUTPUT_FILENAMES}
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if not isinstance(expected_manifest_sha256, str) or member_hashes["manifest.json"] != expected_manifest_sha256:
        raise SnapshotValidationError("trusted manifest hash mismatch")
    try:
        sums = json.loads(members["sha256sums.json"])
        manifest = json.loads(members["manifest.json"])
        validation = json.loads(members["validation.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("invalid snapshot metadata") from exc
    if not isinstance(sums, dict) or set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        raise SnapshotValidationError("invalid sha256sums")
    for name, expected in sums.items():
        if not isinstance(expected, str) or member_hashes[name] != expected:
            raise SnapshotValidationError(f"hash mismatch: {name}")
    prices = _normalized_prices(pd.read_csv(BytesIO(members["prices.csv"])), require_exact_columns=True)
    if manifest != {
        "contract_version": CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": member_hashes["prices.csv"],
    }:
        raise SnapshotValidationError("invalid manifest")
    if validation != {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)}:
        raise SnapshotValidationError("invalid validation")
    return SnapshotResult(output_dir=output, manifest_sha256=member_hashes["manifest.json"])


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
        raise SnapshotValidationError("mode must be core_only")
    if plugin != PLUGIN:
        raise SnapshotValidationError("plugin must be ABSENT_DISABLED")
    if size != 0:
        raise SnapshotValidationError("size must be zero")
    normalized = _normalized_prices(prices)
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        raise SnapshotValidationError(f"immutable output already exists: {destination}")
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
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)
