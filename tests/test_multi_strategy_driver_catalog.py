from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import (
    INPUT_CONTRACT_ID,
    P2_V3_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import P2_V5_CONTRACT
from us_equity_snapshot_pipelines.research.multi_strategy_driver_catalog import (
    CURRENT_RESEARCH_DRIVER_ROUTES,
    DAILY_RESEARCH_WIRED,
    MIGRATION_REQUIRED,
    MultiStrategyDriverCatalogError,
    ResearchDriverRoute,
    SOXL_DAILY_RESEARCH_ROUTE,
    TQQQ_DAILY_RESEARCH_ROUTE,
    build_multi_strategy_research_driver_catalog,
    canonical_route_bytes,
)


def test_catalogue_describes_tqqq_and_soxl_as_independent_daily_research_routes() -> None:
    catalogue = build_multi_strategy_research_driver_catalog()

    assert CURRENT_RESEARCH_DRIVER_ROUTES == (TQQQ_DAILY_RESEARCH_ROUTE, SOXL_DAILY_RESEARCH_ROUTE)
    assert TQQQ_DAILY_RESEARCH_ROUTE.research_identity_id == P2_V5_CONTRACT.candidate_id
    assert TQQQ_DAILY_RESEARCH_ROUTE.state == DAILY_RESEARCH_WIRED
    assert TQQQ_DAILY_RESEARCH_ROUTE.migration_blockers == ()
    assert SOXL_DAILY_RESEARCH_ROUTE.research_identity_id == P2_V3_CONTRACT.candidate_id
    assert SOXL_DAILY_RESEARCH_ROUTE.input_contract_id == INPUT_CONTRACT_ID
    assert SOXL_DAILY_RESEARCH_ROUTE.p2_config_sha256 == P2_V3_CONTRACT.config_sha256
    assert SOXL_DAILY_RESEARCH_ROUTE.p3_replay_entrypoint == "scripts/run_soxl_core_only_p3_evidence.py"
    assert SOXL_DAILY_RESEARCH_ROUTE.state == DAILY_RESEARCH_WIRED
    assert SOXL_DAILY_RESEARCH_ROUTE.migration_blockers == ()
    assert catalogue["routes"][0]["authority"] == {
        "research_only": True,
        "no_order": True,
        "p4_p5_p6_authorized": False,
    }


def test_route_digest_is_deterministic_and_contains_no_execution_authority() -> None:
    first = canonical_route_bytes(TQQQ_DAILY_RESEARCH_ROUTE)
    second = canonical_route_bytes(TQQQ_DAILY_RESEARCH_ROUTE)

    assert first == second
    payload = json.loads(first)
    assert payload["permitted_stages"] == ["P1", "P2", "P3"]
    assert payload["authority"]["no_order"] is True
    assert payload["authority"]["p4_p5_p6_authorized"] is False
    assert "broker" not in first.decode("utf-8").lower()


def test_route_states_fail_closed_when_their_migration_boundary_is_ambiguous() -> None:
    with pytest.raises(MultiStrategyDriverCatalogError, match="daily route"):
        ResearchDriverRoute(
            route_id="example.daily",
            research_identity_id="example_identity",
            input_contract_id="example.input.v1",
            p2_config_sha256="a" * 64,
            p3_replay_entrypoint="example:replay",
            state=DAILY_RESEARCH_WIRED,
            migration_blockers=("unexpected_blocker",),
        )

    with pytest.raises(MultiStrategyDriverCatalogError, match="requires explicit blockers"):
        ResearchDriverRoute(
            route_id="example.migration",
            research_identity_id="example_identity",
            input_contract_id="example.input.v1",
            p2_config_sha256="a" * 64,
            p3_replay_entrypoint="example:replay",
            state=MIGRATION_REQUIRED,
        )
