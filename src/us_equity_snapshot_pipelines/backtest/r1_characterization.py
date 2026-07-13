"""Local-only R1 characterization for configured backtest orchestrators."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Protocol

import pandas as pd

PROFILES = ("SOXL", "TQQQ")
EXECUTION_TIMINGS = ("next_open", "next_close")
SOURCE_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class BacktestOrchestrator(Protocol):
    def run(self, *, profile: str, params: Mapping[str, Any], execution_timing: str) -> Mapping[str, Any]: ...


def _json_safe(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": str(value)}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.DataFrame):
        return {
            "type": "pandas.DataFrame",
            "columns": [_json_safe(column) for column in value.columns],
            "index": [_json_safe(item) for item in value.index],
            "dtypes": [str(dtype) for dtype in value.dtypes],
            "data": [[_json_safe(item) for item in row] for row in value.itertuples(index=False, name=None)],
        }
    if isinstance(value, pd.Series):
        return {
            "type": "pandas.Series",
            "name": _json_safe(value.name),
            "index": [_json_safe(item) for item in value.index],
            "dtype": str(value.dtype),
            "data": [_json_safe(item) for item in value.array],
        }
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key)
            if normalized_key in normalized:
                raise TypeError(f"duplicate JSON key after normalization: {normalized_key}")
            normalized[normalized_key] = _json_safe(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    raise TypeError(f"unsupported R1 artifact value type: {type(value).__name__}")


def _write_exclusive(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise FileExistsError(f"R1 artifact already exists: {path}") from None
    finally:
        temporary_path.unlink(missing_ok=True)


def characterize_profile(
    orchestrator: BacktestOrchestrator,
    profile: str,
    *,
    params: Mapping[str, Any],
    execution_timing: str,
    ephemeral_dir: str | Path,
    source_sha: str,
) -> dict[str, Any]:
    """Run one profile and atomically emit a JSON-safe local artifact."""
    profile = str(profile).strip().upper()
    if profile not in PROFILES:
        raise ValueError(f"unsupported R1 profile: {profile}")
    if execution_timing not in EXECUTION_TIMINGS:
        raise ValueError(f"unsupported execution timing: {execution_timing}")
    if not SOURCE_SHA_PATTERN.fullmatch(str(source_sha)):
        raise ValueError("source_sha must be 40 lowercase hexadecimal characters")

    raw_params = dict(params)
    result = orchestrator.run(profile=profile, params=raw_params, execution_timing=execution_timing)
    if not isinstance(result, Mapping) or not result:
        raise ValueError("BacktestOrchestrator returned no real result")
    if result.get("placeholder") is True:
        raise ValueError("placeholder backtest result is not R1 evidence")

    artifact = {
        "schema": "soxl_tqqq_r1_characterization.v1",
        "profile": profile,
        "execution_timing": execution_timing,
        "source_sha": source_sha,
        "params": _json_safe(raw_params),
        "result": _json_safe(dict(result)),
        "field_inventory": sorted(str(key) for key in result),
    }
    content = json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    run_identity = {
        "profile": profile,
        "execution_timing": execution_timing,
        "source_sha": source_sha,
        "params": artifact["params"],
    }
    run_key = json.dumps(run_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    run_digest = hashlib.sha256(run_key).hexdigest()[:16]
    output_path = Path(ephemeral_dir) / f"{profile.lower()}_{execution_timing}_{run_digest}.json"
    _write_exclusive(output_path, content)

    return {
        **artifact,
        "artifact_path": str(output_path),
        "artifact_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
    }
