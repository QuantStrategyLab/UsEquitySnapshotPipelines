from __future__ import annotations

import tomllib
from pathlib import Path


EXPECTED_GLOBAL_ETF_ENTRYPOINTS = {
    "useq-build-global-etf-shadow-review": "us_equity_snapshot_pipelines.global_etf_rotation_shadow_review:main",
    "useq-build-global-etf-shadow-review-input": "us_equity_snapshot_pipelines.global_etf_rotation_shadow_review_input:main",
}


def test_global_etf_research_entrypoints_are_registered() -> None:
    config = tomllib.loads(Path("pyproject.toml").read_text())
    scripts = config["project"]["scripts"]

    for script_name, target in EXPECTED_GLOBAL_ETF_ENTRYPOINTS.items():
        assert scripts[script_name] == target
