from __future__ import annotations

import tomllib
from pathlib import Path


EXPECTED_RUSSELL_PROMOTION_ENTRYPOINTS = {
    "useq-research-russell-top50-leader-rotation-overfit-diagnostics": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_overfit_diagnostics:main",
    "useq-research-russell-top50-leader-rotation-reality-check": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_reality_check:main",
    "useq-research-russell-top50-leader-rotation-liquidity-diagnostics": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_liquidity_diagnostics:main",
    "useq-research-russell-top50-leader-rotation-spa-check": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_spa_check:main",
    "useq-research-russell-top50-leader-rotation-era-split": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_era_split_diagnostics:main",
    "useq-research-russell-top50-leader-rotation-mcs-diagnostics": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_mcs_diagnostics:main",
    "useq-research-russell-top50-leader-rotation-promotion-review": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_review:main",
    "useq-research-russell-top50-leader-rotation-promotion-bundle": "us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_bundle:main",
}


def test_russell_promotion_research_entrypoints_are_registered() -> None:
    config = tomllib.loads(Path("pyproject.toml").read_text())
    scripts = config["project"]["scripts"]

    for script_name, target in EXPECTED_RUSSELL_PROMOTION_ENTRYPOINTS.items():
        assert scripts[script_name] == target
