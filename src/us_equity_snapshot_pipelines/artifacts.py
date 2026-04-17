from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .contracts import SOURCE_PROJECT, SnapshotProfileContract


def sha256_file(path: str | Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def write_json(path: str | Path, payload: Mapping[str, Any]) -> Path:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return resolved


def default_config_sha256(*, contract: SnapshotProfileContract, config_name: str | None = None) -> str:
    payload = {
        "config_source": "strategy_manifest_default",
        "config_name": config_name or contract.profile,
        "strategy_profile": contract.profile,
        "contract_version": contract.contract_version,
    }
    content = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def resolve_snapshot_as_of(snapshot: pd.DataFrame) -> str | None:
    for column in ("as_of", "snapshot_date"):
        if column in snapshot.columns:
            value = pd.to_datetime(snapshot[column], errors="coerce").max()
            if pd.notna(value):
                return pd.Timestamp(value).date().isoformat()
    return None


def write_snapshot_manifest(
    *,
    contract: SnapshotProfileContract,
    snapshot_path: str | Path,
    snapshot: pd.DataFrame,
    config_path: str | Path | None,
    manifest_path: str | Path,
    config_name: str | None = None,
) -> Path:
    resolved_snapshot = Path(snapshot_path)
    resolved_config = Path(config_path) if config_path else None
    config_exists = resolved_config is not None and resolved_config.exists()
    payload = {
        "manifest_type": "feature_snapshot",
        "contract_version": contract.contract_version,
        "strategy_profile": contract.profile,
        "config_name": config_name or contract.profile,
        "config_path": str(resolved_config) if config_exists else "strategy_manifest_default",
        "config_sha256": (
            sha256_file(resolved_config)
            if config_exists
            else default_config_sha256(contract=contract, config_name=config_name)
        ),
        "snapshot_path": str(resolved_snapshot),
        "snapshot_sha256": sha256_file(resolved_snapshot),
        "snapshot_as_of": resolve_snapshot_as_of(snapshot),
        "row_count": int(len(snapshot)),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_project": SOURCE_PROJECT,
    }
    return write_json(manifest_path, payload)


def write_release_status_summary(
    *,
    contract: SnapshotProfileContract,
    snapshot_path: str | Path,
    manifest_path: str | Path,
    ranking_path: str | Path,
    summary_path: str | Path,
    snapshot: pd.DataFrame,
    signal_description: str,
    status_description: str,
    diagnostics: Mapping[str, Any],
) -> Path:
    payload = {
        "source_project": SOURCE_PROJECT,
        "strategy_profile": contract.profile,
        "display_name": contract.display_name,
        "contract_version": contract.contract_version,
        "release_status": "ready",
        "snapshot_path": str(snapshot_path),
        "manifest_path": str(manifest_path),
        "ranking_path": str(ranking_path),
        "snapshot_as_of": resolve_snapshot_as_of(snapshot),
        "row_count": int(len(snapshot)),
        "signal_description": signal_description,
        "status_description": status_description,
        "diagnostics": _json_safe(diagnostics),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return write_json(summary_path, payload)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value
