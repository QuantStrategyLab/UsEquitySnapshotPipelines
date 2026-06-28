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

from us_equity_snapshot_pipelines.russell_leader_rotation_shadow_cycle import (  # noqa: E402
    run_russell_leader_rotation_shadow_cycle,
)


STAGING_SNAPSHOT = (
    PROJECT_ROOT
    / "data/output/russell_top50_leader_rotation_staging_20260628/russell_top50_leader_rotation_feature_snapshot_latest.csv"
)


@pytest.mark.skipif(not STAGING_SNAPSHOT.exists(), reason="local staging snapshot not available")
def test_run_russell_leader_rotation_shadow_cycle_on_staging_snapshot(tmp_path: Path) -> None:
    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=STAGING_SNAPSHOT,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-01",
    )

    assert outputs.diagnostics_json.exists()
    assert outputs.variant_comparison_json.exists()
    assert outputs.shadow_review_csv.exists()
    assert outputs.shadow_review_manifest.exists()

    diagnostics = json.loads(outputs.diagnostics_json.read_text(encoding="utf-8"))
    assert diagnostics["snapshot_as_of"] == "2026-04-01"
    assert diagnostics["diagnostics"]["leader_rotation_profile_variant"] == "blend_top2_50_top4_50"
    shadow_rows = diagnostics["diagnostics"]["leader_rotation_shadow_review_rows"]
    assert len(shadow_rows) == 3
    assert {row["shadow_variant"] for row in shadow_rows} == {
        "top4_baseline",
        "blend_top2_25_top4_75",
        "blend_top2_50_top4_50",
    }

    comparison = json.loads(outputs.variant_comparison_json.read_text(encoding="utf-8"))
    assert comparison["active_variant"] == "blend_top2_50_top4_50"
    assert len(comparison["variants"]) == 3

    review_rows = pd.read_csv(outputs.shadow_review_csv)
    assert len(review_rows) == 3
    active_row = review_rows.loc[review_rows["shadow_variant"].eq("blend_top2_50_top4_50")].iloc[0]
    assert float(active_row["turnover_delta_vs_active"]) == 0.0


@pytest.mark.skipif(not STAGING_SNAPSHOT.exists(), reason="local staging snapshot not available")
def test_shadow_cycle_emits_rebalance_trades(tmp_path: Path) -> None:
    """Verify the shadow cycle emits a rebalance_trades CSV with variant-level trade rows."""
    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=STAGING_SNAPSHOT,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-01",
    )
    assert outputs.rebalance_trades_csv is not None
    assert outputs.rebalance_trades_csv.exists()
    trades = pd.read_csv(outputs.rebalance_trades_csv)
    assert not trades.empty
    assert set(trades.columns).issuperset({
        "Date",
        "Run",
        "Variant Type",
        "Symbol",
        "Previous Weight",
        "Target Weight",
        "Trade Weight Delta",
        "Abs Trade Weight Delta",
        "Trade Side",
    })
    # Verify all 3 variant types are present
    assert trades["Variant Type"].nunique() == 3
