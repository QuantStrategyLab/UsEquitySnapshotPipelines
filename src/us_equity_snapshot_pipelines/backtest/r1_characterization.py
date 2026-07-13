"""R1 characterization adapter for real BacktestOrchestrator runs.

This module deliberately does not implement a date loop or use the lifecycle
placeholder runner.  The caller supplies a configured orchestrator and an
ephemeral output directory.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol

PROFILES = ("SOXL", "TQQQ")
EXECUTION_TIMINGS = ("next_open", "next_close")


class BacktestOrchestrator(Protocol):
    def run(self, *, profile: str, params: Mapping[str, Any], execution_timing: str) -> Mapping[str, Any]: ...


def characterize_profile(
    orchestrator: BacktestOrchestrator,
    profile: str,
    *,
    params: Mapping[str, Any],
    execution_timing: str,
    ephemeral_dir: str | Path,
) -> dict[str, Any]:
    """Run one profile through the supplied orchestrator and emit one local artifact."""
    profile = str(profile).strip().upper()
    if profile not in PROFILES:
        raise ValueError(f"unsupported R1 profile: {profile}")
    if execution_timing not in EXECUTION_TIMINGS:
        raise ValueError(f"unsupported execution timing: {execution_timing}")

    result = orchestrator.run(profile=profile, params=dict(params), execution_timing=execution_timing)
    if not isinstance(result, Mapping) or not result:
        raise ValueError("BacktestOrchestrator returned no real result")
    if result.get("placeholder") is True:
        raise ValueError("placeholder backtest result is not R1 evidence")

    artifact = {
        "schema": "soxl_tqqq_r1_characterization.v1",
        "profile": profile,
        "execution_timing": execution_timing,
        "params": dict(params),
        "result": dict(result),
        "field_inventory": sorted(str(key) for key in result),
    }
    output_dir = Path(ephemeral_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{profile.lower()}_{execution_timing}.json"
    output_path.write_text(json.dumps(artifact, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    artifact["artifact_path"] = str(output_path)
    return artifact
