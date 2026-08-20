from __future__ import annotations

import copy
import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import historical_combo_p1_input_binding as p1
from us_equity_snapshot_pipelines.lifecycle import historical_combo_p3_input_verifier as p3


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


def _pit() -> dict[str, object]:
    return {
        "schema_version": "qsl.point-in-time-data-declaration.v1",
        "availability_basis": "AS_OF_COMMON_CUTOFF",
        "future_data_allowed": False,
        "revised_data_allowed": False,
        "signal_execution_timing": "next_complete_trading_session_after_signal_effective_date",
    }


def _costs() -> dict[str, object]:
    return {
        "schema_version": "qsl.research-cost-declaration.v1",
        "turnover_cost_bps": [5.0, 10.0, 25.0],
        "borrow_cost_bps": 0.0,
        "cash_yield_assumption": 0.0,
        "execution_timing": "next_complete_trading_session_after_signal_effective_date",
    }


def _binding() -> dict[str, object]:
    return p1.build_historical_combo_p1_input_binding(
        candidate=_candidate(),
        common_cutoff="2026-08-04",
        component_candidates=_components(),
        pit_declaration=_pit(),
        cost_declaration=_costs(),
        virtual_combo_policy_sha256="2" * 64,
        portfolio_risk_budget_policy_sha256="4" * 64,
        virtual_combo_target=_virtual_target(),
    )


def _p2_candidate(binding: dict[str, object]) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": p3.P2_CANDIDATE_SCHEMA,
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


def _rehash_p2(value: dict[str, object]) -> None:
    value["candidate_sha256"] = _sha256(
        {key: item for key, item in value.items() if key != "candidate_sha256"}
    )


def test_p1_binding_is_canonical_and_discards_virtual_target_weights() -> None:
    binding = _binding()

    assert binding["schema_version"] == p1.SCHEMA_VERSION
    assert binding["input_sha256"] == p1.historical_combo_p1_input_sha256(binding)
    assert binding["frozen_p2"] == {
        "virtual_combo_policy_schema": p1.VIRTUAL_COMBO_POLICY_SCHEMA,
        "virtual_combo_policy_sha256": "2" * 64,
        "portfolio_risk_budget_schema": p1.PORTFOLIO_RISK_BUDGET_SCHEMA,
        "portfolio_risk_budget_policy_sha256": "4" * 64,
        "virtual_target_summary": {
            "schema_version": p1.VIRTUAL_COMBO_TARGET_SCHEMA,
            "status": "APPROVE",
            "policy_sha256": "2" * 64,
            "input_sha256": "3" * 64,
            "combo_target_sha256": _virtual_target()["combo_target_sha256"],
        },
    }
    assert "combo_target_weights" not in json.dumps(binding["frozen_p2"])
    assert json.loads(p1.canonical_historical_combo_p1_input_binding_bytes(binding)) == binding


@pytest.mark.parametrize(
    "mutate",
    (
        lambda components, target: components[1].update({"source_date_cutoff": "2026-08-05"}),
        lambda components, target: target.update({"execution_authorized": True}),
        lambda components, target: target.update({"combo_target_weights": {"BOXX": 1.0}}),
    ),
)
def test_p1_rejects_mismatched_component_cutoff_or_mutated_virtual_target(mutate) -> None:
    components = _components()
    target = _virtual_target()
    mutate(components, target)

    with pytest.raises(p1.HistoricalComboP1InputBindingError):
        p1.build_historical_combo_p1_input_binding(
            candidate=_candidate(),
            common_cutoff="2026-08-04",
            component_candidates=components,
            pit_declaration=_pit(),
            cost_declaration=_costs(),
            virtual_combo_policy_sha256="2" * 64,
            portfolio_risk_budget_policy_sha256="4" * 64,
            virtual_combo_target=target,
        )


def test_p3_preflight_returns_input_identity_only_when_all_links_match() -> None:
    binding = _binding()
    result = p3.verify_historical_combo_p3_inputs(
        p1_input_binding=binding, p2_candidate=_p2_candidate(binding)
    )

    assert result["status"] == p3.READY_STATUS
    assert result["research_only"] is True
    assert result["execution_authorized"] is False
    assert result["verified_inputs"]["p1_input_sha256"] == binding["input_sha256"]
    assert result["verified_inputs"]["common_cutoff"] == "2026-08-04"
    assert result["verified_inputs"]["p2_candidate_sha256"] == _p2_candidate(binding)["candidate_sha256"]
    encoded = json.dumps(result)
    assert "combo_target_weights" not in encoded
    assert "net_return" not in encoded
    assert "promotion" not in encoded


@pytest.mark.parametrize(
    ("mutate", "reason_code"),
    (
        (
            lambda value: value.update({"p1_input_sha256": "9" * 64}),
            "P1_P2_INPUT_DIGEST_MISMATCH",
        ),
        (
            lambda value: value["legs"][1].update({"strategy_revision": "8" * 40}),
            "P1_P2_COMPONENT_REVISION_MISMATCH",
        ),
        (
            lambda value: value["holdout_window"].update({"end": "2026-08-05"}),
            "FUTURE_LEAKAGE_DETECTED",
        ),
        (
            lambda value: value["risk_budget"].update({"policy_sha256": "7" * 64}),
            "P1_P2_RISK_POLICY_MISMATCH",
        ),
        (
            lambda value: value.update({"p5_shadow_authorized": True}),
            "INVALID_P2_CANDIDATE",
        ),
    ),
)
def test_p3_preflight_parks_any_mismatch_or_future_leakage(mutate, reason_code: str) -> None:
    binding = _binding()
    candidate = copy.deepcopy(_p2_candidate(binding))
    mutate(candidate)
    _rehash_p2(candidate)

    result = p3.verify_historical_combo_p3_inputs(p1_input_binding=binding, p2_candidate=candidate)

    assert result == {
        "schema_version": p3.SCHEMA_VERSION,
        "research_only": True,
        "execution_authorized": False,
        "status": p3.PARKED_STATUS,
        "reason_codes": [reason_code],
        "verified_inputs": None,
        "verification_sha256": None,
    }
