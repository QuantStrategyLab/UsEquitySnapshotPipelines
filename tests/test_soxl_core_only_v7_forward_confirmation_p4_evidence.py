from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v7_forward_confirmation_p4_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p4_v7_forward_confirmation_contract import (
    P4_V7_FORWARD_CONFIRMATION_CONTRACT,
    SoxlCoreOnlyP4V7ForwardConfirmationContractError,
    validate_soxl_core_only_p4_v7_forward_confirmation_policy,
)


def _policy() -> dict[str, object]:
    return json.loads(
        (Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p4_v7_forward_confirmation.json").read_text(
            encoding="utf-8"
        )
    )


def _dates(count: int) -> list[str]:
    current = date.fromisoformat(P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session)
    result: list[str] = []
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _materialized(*, session_count: int = 300) -> dict[str, object]:
    sessions = [
        {
            "as_of": f"{session_date}T00:00:00+00:00",
            "market_data": {"derived_indicators": {}},
            "prices": {"SOXL": 100.0 + index, "SOXX": 200.0 + index, "BOXX": 100.0},
        }
        for index, session_date in enumerate(_dates(session_count))
    ]
    result: dict[str, object] = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-free-split-close-materialized-input.v1",
        "p1_identity": {
            "input_manifest_sha256": "a" * 64,
            "binding_sha256": "b" * 64,
            "closes_member_sha256": "c" * 64,
            "assurance_member_sha256": "d" * 64,
            "date_cutoff": _dates(session_count)[-1],
        },
        "p2_identity": {
            "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
            "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        },
        "indicator_spec": {"id": "fixture"},
        "sessions": sessions,
    }
    result["materialized_input_sha256"] = evidence._sha256(result)
    return result


def _base_summary(plan: dict[str, object], *, rejected: bool) -> dict[str, object]:
    runs = []
    for request in plan["requests"]:
        item = dict(request)
        runs.append(
            {
                "window_id": item["window_id"],
                "window_kind": item["window_kind"],
                "cost_bps": item["cost_bps"],
                "replay_input_sha256": "e" * 64,
                "metrics": {
                    "max_drawdown": 0.01 if rejected else 0.0,
                    "calmar": 1.0,
                },
            }
        )
    result: dict[str, object] = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-summary.v1",
        "status": "SUCCESS",
        "p1_identity": plan["p1_identity"],
        "p2_identity": plan["p2_identity"],
        "materialized_input_sha256": plan["materialized_input_sha256"],
        "evidence_plan_sha256": plan["evidence_plan_sha256"],
        "execution_identity": {"revision": "f" * 40},
        "runs": runs,
    }
    result["evidence_summary_sha256"] = evidence._sha256(result)
    return result


def test_policy_is_immutable_and_binds_the_accepted_v7_baseline() -> None:
    policy = _policy()

    assert validate_soxl_core_only_p4_v7_forward_confirmation_policy(policy) == policy
    policy["forward_window"]["session_count"] = 253
    with pytest.raises(SoxlCoreOnlyP4V7ForwardConfirmationContractError):
        validate_soxl_core_only_p4_v7_forward_confirmation_policy(policy)


def test_plan_uses_only_the_first_fixed_post_freeze_window() -> None:
    materialized = _materialized()

    plan = evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
        materialized,
        policy=_policy(),
    )

    assert plan["p4_policy_identity"]["forward_session_count"] == 252
    assert plan["p4_policy_identity"]["initial_state"] == "100_percent_cash_at_first_forward_signal"
    assert len(plan["requests"]) == 3
    assert {request["cost_bps"] for request in plan["requests"]} == {5, 10, 15}
    assert all(len(request["session_dates"]) == 252 for request in plan["requests"])
    assert all(request["session_dates"][-1] == _dates(252)[-1] for request in plan["requests"])


def test_plan_parks_until_the_entire_fixed_forward_window_exists() -> None:
    with pytest.raises(evidence.SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete):
        evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
            _materialized(session_count=251),
            policy=_policy(),
        )


def test_summary_requires_forward_drawdown_and_calmar_gates(monkeypatch) -> None:
    materialized = _materialized()
    plan = evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
        materialized,
        policy=_policy(),
    )
    monkeypatch.setattr(evidence, "_build_base_summary", lambda **_kwargs: _base_summary(plan, rejected=False))

    result = evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=lambda _input: {},
        policy=_policy(),
    )

    policy = result["forward_confirmation_policy"]
    assert result["schema_version"] == evidence.FORWARD_CONFIRMATION_SUMMARY_SCHEMA
    assert policy["forward_confirmation_satisfied"] is True
    assert policy["initial_state"] == "100_percent_cash_at_first_forward_signal"
    assert policy["strategy_verdict"] == "PASS_REQUIRES_SEPARATE_HUMAN_PROMOTION"
    assert policy["automatic_promotion"] is False


def test_summary_rejects_a_forward_drawdown_failure(monkeypatch) -> None:
    materialized = _materialized()
    plan = evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(
        materialized,
        policy=_policy(),
    )
    monkeypatch.setattr(evidence, "_build_base_summary", lambda **_kwargs: _base_summary(plan, rejected=True))

    result = evidence.build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=lambda _input: {},
        policy=_policy(),
    )

    assert result["forward_confirmation_policy"]["strategy_verdict"] == "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
