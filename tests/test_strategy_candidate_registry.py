from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import P2_V5_CONTRACT
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import P2_V3_CONTRACT
from us_equity_snapshot_pipelines.research.strategy_candidate_registry import (
    CURRENT_RESEARCH_CANDIDATES,
    PLUGIN,
    PORTFOLIO,
    RESEARCH_STAGES,
    SINGLE_STRATEGY,
    SOXL_SOXX_CORE_ONLY_P2_V3,
    TQQQ_CORE_ONLY_P2_V5,
    PluginBinding,
    SourceRevision,
    StrategyCandidate,
    StrategyCandidateRegistryError,
    build_research_candidate_registry,
    resolve_research_candidate,
)


def _source(repository: str = "QuantStrategyLab/example") -> SourceRevision:
    return SourceRevision(repository, "a" * 40)


def test_current_registry_contains_the_frozen_tqqq_v5_and_soxl_v3_research_candidates() -> None:
    registry = build_research_candidate_registry()

    assert CURRENT_RESEARCH_CANDIDATES == (TQQQ_CORE_ONLY_P2_V5, SOXL_SOXX_CORE_ONLY_P2_V3)
    assert TQQQ_CORE_ONLY_P2_V5.candidate_id == P2_V5_CONTRACT.candidate_id
    assert TQQQ_CORE_ONLY_P2_V5.kind == SINGLE_STRATEGY
    assert TQQQ_CORE_ONLY_P2_V5.config_sha256 == P2_V5_CONTRACT.config_sha256
    assert TQQQ_CORE_ONLY_P2_V5.permitted_stages == RESEARCH_STAGES
    assert TQQQ_CORE_ONLY_P2_V5.component_candidate_ids == ()
    assert TQQQ_CORE_ONLY_P2_V5.plugin_bindings == ()
    assert registry["candidates"][0]["candidate_sha256"] == TQQQ_CORE_ONLY_P2_V5.candidate_sha256
    assert resolve_research_candidate(P2_V5_CONTRACT.candidate_id) == TQQQ_CORE_ONLY_P2_V5
    assert SOXL_SOXX_CORE_ONLY_P2_V3.candidate_id == P2_V3_CONTRACT.candidate_id
    assert SOXL_SOXX_CORE_ONLY_P2_V3.kind == SINGLE_STRATEGY
    assert SOXL_SOXX_CORE_ONLY_P2_V3.config_sha256 == P2_V3_CONTRACT.config_sha256
    assert SOXL_SOXX_CORE_ONLY_P2_V3.permitted_stages == RESEARCH_STAGES
    assert SOXL_SOXX_CORE_ONLY_P2_V3.component_candidate_ids == ()
    assert SOXL_SOXX_CORE_ONLY_P2_V3.plugin_bindings == ()
    assert registry["candidates"][1]["candidate_sha256"] == SOXL_SOXX_CORE_ONLY_P2_V3.candidate_sha256
    assert resolve_research_candidate(P2_V3_CONTRACT.candidate_id) == SOXL_SOXX_CORE_ONLY_P2_V3


def test_portfolio_candidate_requires_multiple_distinct_components() -> None:
    with pytest.raises(StrategyCandidateRegistryError, match="at least two"):
        StrategyCandidate(
            candidate_id="portfolio_one_component",
            kind=PORTFOLIO,
            config_sha256="b" * 64,
            data_contract_id="example.data.v1",
            source_revisions=(_source(),),
            component_candidate_ids=("component_a",),
        )

    with pytest.raises(StrategyCandidateRegistryError, match="unique"):
        StrategyCandidate(
            candidate_id="portfolio_duplicate_component",
            kind=PORTFOLIO,
            config_sha256="b" * 64,
            data_contract_id="example.data.v1",
            source_revisions=(_source(),),
            component_candidate_ids=("component_a", "component_a"),
        )


def test_plugin_candidate_requires_one_versioned_plugin_binding_and_no_execution_stage() -> None:
    binding = PluginBinding(
        plugin_id="example_plugin",
        source_revision=_source("QuantStrategyLab/QuantStrategyPlugins"),
        config_sha256="c" * 64,
    )
    candidate = StrategyCandidate(
        candidate_id="example_plugin_candidate",
        kind=PLUGIN,
        config_sha256="d" * 64,
        data_contract_id="example.data.v1",
        source_revisions=(_source(),),
        plugin_bindings=(binding,),
    )

    assert candidate.permitted_stages == RESEARCH_STAGES
    with pytest.raises(StrategyCandidateRegistryError, match="exact P1/P2/P3"):
        StrategyCandidate(
            candidate_id="invalid_plugin_stage",
            kind=PLUGIN,
            config_sha256="d" * 64,
            data_contract_id="example.data.v1",
            source_revisions=(_source(),),
            plugin_bindings=(binding,),
            permitted_stages=("P1", "P2", "P3", "P4"),
        )

    with pytest.raises(StrategyCandidateRegistryError, match="plugin source revision"):
        PluginBinding(
            plugin_id="invalid_plugin",
            source_revision="not-a-source-revision",  # type: ignore[arg-type]
            config_sha256="c" * 64,
        )


def test_registry_rejects_portfolio_components_not_registered_in_the_same_catalogue() -> None:
    portfolio = StrategyCandidate(
        candidate_id="example_portfolio",
        kind=PORTFOLIO,
        config_sha256="e" * 64,
        data_contract_id="example.data.v1",
        source_revisions=(_source(),),
        component_candidate_ids=("component_a", "component_b"),
    )

    with pytest.raises(StrategyCandidateRegistryError, match="registered candidates"):
        build_research_candidate_registry((portfolio,))
