from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.shadow_metadata import (
    build_research_only_shadow_metadata,
)


@pytest.mark.parametrize("profile", ["tqqq_core_only_p2_v5", "soxl_soxx_trend_income"])
def test_p3_evidence_projects_to_research_only_shadow_metadata(profile: str) -> None:
    metadata = build_research_only_shadow_metadata(
        {"schema_version": "strategy_evidence_package.v2", "evidence_package_id": "proof-1"},
        strategy_profile=profile,
    )
    assert metadata["mode"] == "research_only"
    assert metadata["no_order"] is True
    assert metadata["broker_access"] is False
    assert metadata["evidence_package_id"] == "proof-1"


def test_shadow_metadata_rejects_non_p3_evidence() -> None:
    with pytest.raises(ValueError, match="unsupported evidence"):
        build_research_only_shadow_metadata({"schema_version": "other"}, strategy_profile="tqqq")
