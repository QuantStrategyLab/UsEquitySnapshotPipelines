from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
USEQ_STRATEGIES_ROOT = PROJECT_ROOT.parent / "UsEquityStrategies" / "src"
QPK_ROOT = PROJECT_ROOT.parent / "QuantPlatformKit" / "src"
for path in (PROJECT_ROOT / "src", USEQ_STRATEGIES_ROOT, QPK_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

pytest.importorskip("us_equity_strategies")

from us_equity_snapshot_pipelines.global_etf_rotation_shadow_cycle import (  # noqa: E402
    SHADOW_VARIANTS,
    DEFAULT_ACTIVE_VARIANT,
    run_global_etf_rotation_shadow_cycle,
)


STAGING_SNAPSHOT = (
    PROJECT_ROOT
    / "data/output/global_etf_rotation_staging"
    / "global_etf_rotation_feature_snapshot_latest.csv"
)


@pytest.mark.skipif(not STAGING_SNAPSHOT.exists(), reason="local staging snapshot not available")
def test_run_global_etf_rotation_shadow_cycle_on_staging_snapshot(tmp_path: Path) -> None:
    """Verify shadow cycle produces diagnostics and variant comparison with 4 variants."""
    outputs = run_global_etf_rotation_shadow_cycle(
        feature_snapshot_path=STAGING_SNAPSHOT,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-01",
    )

    assert outputs.diagnostics_json.exists()
    assert outputs.variant_comparison_json.exists()

    diagnostics = json.loads(outputs.diagnostics_json.read_text(encoding="utf-8"))
    assert diagnostics["strategy_profile"] == "global_etf_rotation"
    assert diagnostics["snapshot_as_of"] == "2026-04-01"

    comparison = json.loads(outputs.variant_comparison_json.read_text(encoding="utf-8"))
    assert comparison["active_variant"] == DEFAULT_ACTIVE_VARIANT
    assert len(comparison["variants"]) == len(SHADOW_VARIANTS)

    variant_names = {v["variant"] for v in comparison["variants"]}
    assert variant_names == set(SHADOW_VARIANTS)

    active_row = next(v for v in comparison["variants"] if v["is_active"])
    assert float(active_row["turnover_delta_vs_active"]) == 0.0
    assert active_row["variant"] == DEFAULT_ACTIVE_VARIANT
