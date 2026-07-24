"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import hashlib
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


def _normalized_prices(prices: pd.DataFrame) -> pd.DataFrame:
    required = {"session", "symbol", PRICE_FIELD}
    missing = required.difference(prices.columns)
    if missing:
        raise SnapshotValidationError(f"missing required columns: {', '.join(sorted(missing))}")
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
    normalized["session"] = normalized["session"].dt.normalize()
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        raise SnapshotValidationError("session precedes requested lower bound")
    if (normalized["session"].dt.dayofweek >= 5).any():
        raise SnapshotValidationError("observed session must be a weekday")
    if normalized.duplicated(["session", "symbol"]).any():
        raise SnapshotValidationError("duplicate session for symbol")
    normalized[PRICE_FIELD] = pd.to_numeric(normalized[PRICE_FIELD], errors="coerce")
    if normalized[PRICE_FIELD].isna().any() or not normalized[PRICE_FIELD].map(math.isfinite).all() or (normalized[PRICE_FIELD] <= 0).any():
        raise SnapshotValidationError("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _write_prices(path: Path, prices: pd.DataFrame) -> None:
    output = prices.copy()
    output["session"] = output["session"].dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.15g")


def verify_tqqq_r1_snapshot(output_dir: str | Path) -> SnapshotResult:
    output = Path(output_dir)
    names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        raise SnapshotValidationError(f"unexpected output files: {names}")
    try:
        sums = json.loads((output / "sha256sums.json").read_text(encoding="utf-8"))
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        validation = json.loads((output / "validation.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("invalid snapshot metadata") from exc
    if not isinstance(sums, dict) or set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        raise SnapshotValidationError("invalid sha256sums")
    for name, expected in sums.items():
        if not isinstance(expected, str) or _sha256(output / name) != expected:
            raise SnapshotValidationError(f"hash mismatch: {name}")
    prices = _normalized_prices(pd.read_csv(output / "prices.csv"))
    if manifest != {
        "contract_version": CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": sums["prices.csv"],
    }:
        raise SnapshotValidationError("invalid manifest")
    if validation != {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)}:
        raise SnapshotValidationError("invalid validation")
    return SnapshotResult(output_dir=output, manifest_sha256=sums["manifest.json"])


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
    if destination.exists():
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
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return verify_tqqq_r1_snapshot(destination)
