from __future__ import annotations

import copy
import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import historical_combo_p1_input_binding as p1
from us_equity_snapshot_pipelines.lifecycle import historical_combo_p3_evidence_index as evidence_index
from us_equity_snapshot_pipelines.lifecycle import historical_combo_p3_input_verifier as preflight
from us_equity_snapshot_pipelines.lifecycle import historical_combo_p3_synthetic_replay as replay


def _sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    ).hexdigest()


def _candidate() -> dict[str, str]:
    return {
        "candidate_id": "us-equity-three-sleeve-baseline",
        "candidate_revision": "a" * 40,
        "config_sha256": "b" * 64,
    }


def _components() -> list[dict[str, str]]:
    return [
        {
            "leg_id": "soxl-core",
            "strategy_id": "soxl_soxx_core_only",
            "strategy_revision": "c" * 40,
            "config_sha256": "d" * 64,
            "source_p1_sha256": "e" * 64,
            "source_date_cutoff": "2026-08-04",
        },
        {
            "leg_id": "tqqq-core",
            "strategy_id": "tqqq_core_only",
            "strategy_revision": "f" * 40,
            "config_sha256": "0" * 64,
            "source_p1_sha256": "1" * 64,
            "source_date_cutoff": "2026-08-04",
        },
    ]


def _virtual_target() -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": p1.VIRTUAL_COMBO_TARGET_SCHEMA,
        "research_only": True,
        "execution_authorized": False,
        "evidence_scope": "VIRTUAL_TARGET_CONSTRUCTION_ONLY",
        "status": "APPROVE",
        "reason_codes": (),
        "policy_sha256": "2" * 64,
        "input_sha256": "3" * 64,
        "combo_target_weights": {"BOXX": 0.2, "SOXL": 0.4, "TQQQ": 0.4},
        "summary": {"gross_risk_weight": 0.8},
    }
    value["combo_target_sha256"] = _sha256(value)
    return value


def _binding() -> dict[str, object]:
    return p1.build_historical_combo_p1_input_binding(
        candidate=_candidate(),
        common_cutoff="2026-08-04",
        component_candidates=_components(),
        pit_declaration={
            "schema_version": "qsl.point-in-time-data-declaration.v1",
            "availability_basis": "AS_OF_COMMON_CUTOFF",
            "future_data_allowed": False,
            "revised_data_allowed": False,
            "signal_execution_timing": "next_complete_trading_session_after_signal_effective_date",
        },
        cost_declaration={
            "schema_version": "qsl.research-cost-declaration.v1",
            "turnover_cost_bps": [5.0, 10.0, 25.0],
            "borrow_cost_bps": 7.0,
            "cash_yield_assumption": 0.02,
            "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        },
        virtual_combo_policy_sha256="2" * 64,
        portfolio_risk_budget_policy_sha256="4" * 64,
        virtual_combo_target=_virtual_target(),
    )


def _p2_candidate(binding: dict[str, object]) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": preflight.P2_CANDIDATE_SCHEMA,
        "research_only": True,
        "candidate_state": "FROZEN_RESEARCH_CANDIDATE",
        "p1_input_sha256": binding["input_sha256"],
        "candidate": _candidate(),
        "selection_window": {"start": "2022-01-03", "end": "2024-12-31"},
        "holdout_window": {"start": "2025-01-02", "end": "2026-08-04"},
        "legs": [
            {
                "leg_id": component["leg_id"],
                "strategy_id": component["strategy_id"],
                "strategy_revision": component["strategy_revision"],
                "config_sha256": component["config_sha256"],
                "target_weight": 0.5,
            }
            for component in _components()
        ],
        "risk_budget": {
            "schema_version": p1.PORTFOLIO_RISK_BUDGET_SCHEMA,
            "policy_sha256": "4" * 64,
        },
        "promotion_recommendation": None,
        "p4_paper_authorized": False,
        "p5_shadow_authorized": False,
        "p6_live_authorized": False,
    }
    value["candidate_sha256"] = _sha256(value)
    return value


def _segments() -> list[dict[str, str]]:
    return [
        {"segment_id": "oos-a", "start": "2025-01-02", "end": "2025-01-03"},
        {"segment_id": "oos-b", "start": "2026-08-03", "end": "2026-08-04"},
    ]


def _observations() -> list[dict[str, object]]:
    return [
        {"session": "2025-01-02", "leg_returns": {"soxl-core": -0.03, "tqqq-core": 0.01}},
        {"session": "2025-01-03", "leg_returns": {"soxl-core": 0.04, "tqqq-core": -0.01}},
        {"session": "2026-08-03", "leg_returns": {"soxl-core": 0.02, "tqqq-core": 0.03}},
        {"session": "2026-08-04", "leg_returns": {"soxl-core": -0.01, "tqqq-core": 0.02}},
    ]


def _fixture() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    binding = _binding()
    candidate = _p2_candidate(binding)
    fixture = replay.build_historical_combo_p3_synthetic_replay_input(
        p1_input_binding=binding,
        p2_candidate=candidate,
        oos_segments=_segments(),
        observations=_observations(),
    )
    return binding, candidate, fixture


def _rehash_fixture(value: dict[str, object]) -> None:
    value["input_sha256"] = _sha256({key: item for key, item in value.items() if key != "input_sha256"})


def test_bound_synthetic_replay_reports_segmented_oos_cost_metrics_without_real_evidence() -> None:
    binding, candidate, fixture = _fixture()

    result = replay.evaluate_historical_combo_p3_synthetic_replay(
        p1_input_binding=binding, p2_candidate=candidate, replay_input=fixture
    )

    assert result["status"] == replay.COMPLETE_STATUS
    assert result["research_only"] is True
    assert result["execution_authorized"] is False
    assert result["real_market_evidence"] is False
    assert result["replay_sha256"] == _sha256(
        {key: item for key, item in result.items() if key != "replay_sha256"}
    )
    payload = result["result"]
    assert payload["candidate"] == _candidate()
    assert payload["common_cutoff"] == "2026-08-04"
    assert [scenario["turnover_cost_bps"] for scenario in payload["cost_scenarios"]] == [5.0, 10.0, 25.0]
    lowest_cost, highest_cost = payload["cost_scenarios"][0], payload["cost_scenarios"][-1]
    assert [metric["segment_id"] for metric in lowest_cost["synthetic_segment_metrics"]] == [
        "oos-a",
        "oos-b",
    ]
    assert lowest_cost["summary"]["observation_count"] == 4
    assert highest_cost["summary"]["mean_segment_net_total_return"] < lowest_cost["summary"][
        "mean_segment_net_total_return"
    ]
    assert payload["promotion_recommendation"] is None
    assert payload["paper_authorized"] is False
    assert "evidence_sha256" not in json.dumps(result)

    with pytest.raises(evidence_index.HistoricalComboP3EvidenceIndexError):
        evidence_index.validate_historical_combo_p3_result(
            {
                "evidence_sha256": "9" * 64,
                "status": result["status"],
                "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
            }
        )


def test_future_observation_parks_even_when_the_preflight_identity_is_valid() -> None:
    binding, candidate, fixture = _fixture()
    fixture = copy.deepcopy(fixture)
    fixture["observations"][-1]["session"] = "2026-08-05"
    _rehash_fixture(fixture)

    result = replay.evaluate_historical_combo_p3_synthetic_replay(
        p1_input_binding=binding, p2_candidate=candidate, replay_input=fixture
    )

    assert result["status"] == replay.PARKED_STATUS
    assert result["reason_codes"] == ["FUTURE_LEAKAGE_DETECTED"]
    assert result["result"] is None


def test_replay_fixture_cannot_switch_to_a_different_frozen_p2_candidate() -> None:
    binding, candidate, fixture = _fixture()
    fixture = copy.deepcopy(fixture)
    fixture["p2_candidate_sha256"] = "8" * 64
    _rehash_fixture(fixture)

    result = replay.evaluate_historical_combo_p3_synthetic_replay(
        p1_input_binding=binding, p2_candidate=candidate, replay_input=fixture
    )

    assert result["status"] == replay.PARKED_STATUS
    assert result["reason_codes"] == ["REPLAY_P2_CANDIDATE_DIGEST_MISMATCH"]


def test_invalid_pit_binding_parks_before_any_synthetic_metric_is_calculated() -> None:
    binding, candidate, fixture = _fixture()
    binding = copy.deepcopy(binding)
    binding["pit_declaration"]["future_data_allowed"] = True
    binding["input_sha256"] = _sha256(
        {key: item for key, item in binding.items() if key != "input_sha256"}
    )

    result = replay.evaluate_historical_combo_p3_synthetic_replay(
        p1_input_binding=binding, p2_candidate=candidate, replay_input=fixture
    )

    assert result["status"] == replay.PARKED_STATUS
    assert result["reason_codes"] == ["INVALID_P1_INPUT_BINDING"]
    assert result["result"] is None
