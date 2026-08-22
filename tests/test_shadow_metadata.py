from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.shadow_metadata import (
    build_research_only_shadow_metadata,
    build_research_only_forward_observation,
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


@pytest.mark.parametrize("profile", ["tqqq_core_only_p2_v5", "soxl_soxx_trend_income"])
@pytest.mark.parametrize("mode", ["synthetic", "forward"])
def test_forward_observation_binds_evidence_input_and_performance_without_orders(
    profile: str, mode: str
) -> None:
    digest = "a" * 64
    payload = build_research_only_forward_observation(
        {"schema_version": "strategy_evidence_package.v2", "evidence_package_id": "proof-1"},
        strategy_profile=profile,
        observation_id=f"{profile}-observation-1",
        observation_mode=mode,
        input_manifest_sha256=digest,
        performance_artifact_sha256="b" * 64,
    )
    assert payload["schema_version"] == "forward_observation_contract.v1"
    assert payload["input_manifest_sha256"] == digest
    assert payload["promotion_eligible"] is False
    assert payload["shadow_contract"]["no_order"] is True
    assert payload["shadow_contract"]["broker_access"] is False


def test_forward_observation_rejects_invalid_digest_or_mode() -> None:
    kwargs = dict(
        evidence={"schema_version": "strategy_evidence_package.v2", "evidence_package_id": "proof-1"},
        strategy_profile="soxl_soxx_trend_income",
        observation_id="soxl-observation-1",
        observation_mode="synthetic",
        input_manifest_sha256="a" * 64,
        performance_artifact_sha256="b" * 64,
    )
    with pytest.raises(ValueError, match="observation_mode"):
        build_research_only_forward_observation(**{**kwargs, "observation_mode": "live"})
    with pytest.raises(ValueError, match="performance_artifact_sha256"):
        build_research_only_forward_observation(**{**kwargs, "performance_artifact_sha256": "bad"})
