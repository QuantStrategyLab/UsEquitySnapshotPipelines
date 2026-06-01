from __future__ import annotations

import hashlib
import json
import shutil
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


def _safe_version_part(value: Any) -> str:
    text = str(value or "").strip()
    safe = "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in text)
    safe = safe.strip("-_.")
    return safe or "unknown"


def write_strategy_plugin_release_manifest(
    *,
    output_dir: str | Path,
    repository: str | None = None,
    git_sha: str | None = None,
    run_id: str | None = None,
    run_attempt: str | None = None,
) -> Path:
    resolved_output = Path(output_dir)
    signal_path = resolved_output / "latest_signal.json"
    if not signal_path.exists():
        raise FileNotFoundError(f"strategy plugin signal not found: {signal_path}")

    signal = json.loads(signal_path.read_text(encoding="utf-8"))
    if not isinstance(signal, Mapping):
        raise ValueError("strategy plugin latest_signal.json must contain a JSON object")

    schema_version = str(signal.get("schema_version") or "").strip()
    as_of = str(signal.get("as_of") or "").strip()
    version = "-".join(
        _safe_version_part(part)
        for part in (
            as_of,
            run_id or (str(git_sha)[:12] if git_sha else "local"),
            f"attempt-{run_attempt}" if run_attempt else None,
        )
        if part
    )
    release_dir = resolved_output / "releases" / version
    release_dir.mkdir(parents=True, exist_ok=True)

    release_artifacts: dict[str, dict[str, str]] = {}
    for source_path in sorted(path for path in resolved_output.iterdir() if path.is_file()):
        destination = release_dir / source_path.name
        shutil.copy2(source_path, destination)
        release_artifacts[source_path.name] = {
            "path": str(destination),
            "sha256": sha256_file(destination),
        }

    payload = {
        "manifest_type": "strategy_plugin_release",
        "artifact_type": "strategy_plugin_signal",
        "contract_version": schema_version,
        "schema_version": schema_version,
        "version": version,
        "strategy_profile": str(signal.get("strategy") or "").strip(),
        "plugin": str(signal.get("plugin") or "").strip(),
        "mode": str(signal.get("effective_mode") or signal.get("mode") or "").strip(),
        "as_of": as_of,
        "canonical_route": signal.get("canonical_route"),
        "suggested_action": signal.get("suggested_action"),
        "source_project": SOURCE_PROJECT,
        "producer": {
            "repository": repository or SOURCE_PROJECT,
            "git_sha": git_sha or "",
            "github_run_id": run_id or "",
            "github_run_attempt": run_attempt or "",
        },
        "current_artifacts": {
            path.name: {
                "path": str(path),
                "sha256": sha256_file(path),
            }
            for path in sorted(resolved_output.iterdir())
            if path.is_file()
        },
        "release_artifacts": release_artifacts,
        "release_dir": str(release_dir),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(release_dir / "release_manifest.json", payload)
    return write_json(resolved_output / "release_manifest.json", payload)


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


def _resolve_frame_as_of(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_datetime(frame[column], errors="coerce")
        if values.notna().any():
            return pd.Timestamp(values.max()).date().isoformat()
    return None


def _load_mapping(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    resolved = Path(path)
    if not resolved.exists():
        return {}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object expected: {resolved}")
    return dict(payload)


def _artifact_metadata(path: str | Path, frame: pd.DataFrame, *, as_of: str | None) -> dict[str, Any]:
    resolved = Path(path)
    payload: dict[str, Any] = {
        "path": str(resolved),
        "row_count": int(len(frame)),
        "as_of": as_of,
    }
    if resolved.exists():
        payload["sha256"] = sha256_file(resolved)
    return payload


def build_snapshot_input_metadata(
    *,
    prices_path: str | Path,
    universe_path: str | Path,
    price_history: pd.DataFrame,
    universe: pd.DataFrame,
    source_input_manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    source_manifest = _load_mapping(source_input_manifest_path)
    has_source_manifest = bool(source_manifest)
    source_artifacts = source_manifest.get("artifacts") if isinstance(source_manifest.get("artifacts"), Mapping) else {}
    price_as_of = (
        source_manifest.get("price_as_of")
        or _resolve_frame_as_of(price_history, ("as_of", "date", "snapshot_date"))
    )
    universe_as_of = (
        source_manifest.get("universe_as_of")
        or _resolve_frame_as_of(universe, ("snapshot_date", "as_of_date", "as_of", "start_date"))
    )
    fallback = source_manifest.get("fallback") if isinstance(source_manifest.get("fallback"), Mapping) else {}
    producer = source_manifest.get("producer") if isinstance(source_manifest.get("producer"), Mapping) else {}
    source_input_status = (
        str(source_manifest.get("source_input_status") or "").strip()
        if has_source_manifest
        else None
    )
    metadata: dict[str, Any] = {
        "price_as_of": price_as_of,
        "universe_as_of": universe_as_of,
        "source_input_status": source_input_status,
        "source_input_fallback_used": bool(source_manifest.get("universe_fallback_used", False))
        if has_source_manifest
        else None,
        "source_input_fallback_reason": source_manifest.get("fallback_reason") or fallback.get("reason"),
        "source_input_fallback_streak": source_manifest.get("fallback_streak") or fallback.get("streak"),
        "source_input_manifest_path": str(source_input_manifest_path) if source_input_manifest_path else None,
        "source_refresh_run_id": producer.get("github_run_id") or source_manifest.get("github_run_id"),
        "source_refresh_generated_at": source_manifest.get("generated_at"),
        "input_artifacts": {
            "prices": _artifact_metadata(prices_path, price_history, as_of=price_as_of),
            "universe": _artifact_metadata(universe_path, universe, as_of=universe_as_of),
        },
    }
    if source_artifacts:
        metadata["source_input_artifacts"] = _json_safe(source_artifacts)
    return {key: _json_safe(value) for key, value in metadata.items() if value is not None}


def write_snapshot_manifest(
    *,
    contract: SnapshotProfileContract,
    snapshot_path: str | Path,
    snapshot: pd.DataFrame,
    config_path: str | Path | None,
    manifest_path: str | Path,
    config_name: str | None = None,
    input_metadata: Mapping[str, Any] | None = None,
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
    if input_metadata:
        payload.update(_json_safe(dict(input_metadata)))
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
