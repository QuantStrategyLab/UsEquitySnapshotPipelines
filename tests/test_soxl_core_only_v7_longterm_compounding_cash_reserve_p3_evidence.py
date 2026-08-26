from __future__ import annotations

from types import SimpleNamespace

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v6_longterm_compounding_p3_evidence as v6_evidence
from us_equity_snapshot_pipelines.lifecycle import (
    soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence as evidence,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)


def test_v7_plan_binds_its_cash_reserve_candidate_identity(monkeypatch) -> None:
    observed: dict[str, object] = {}

    def build(materialized, *, p2_contract):
        observed["materialized"] = materialized
        observed["contract"] = p2_contract
        return {"plan": "v7"}

    monkeypatch.setattr(evidence, "_build_longterm_compounding_p3_evidence_plan", build)

    assert evidence.build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan({"v7": "input"}) == {
        "plan": "v7"
    }
    assert observed == {
        "materialized": {"v7": "input"},
        "contract": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
    }


def test_v7_summary_binds_its_cash_reserve_candidate_identity(monkeypatch) -> None:
    observed: dict[str, object] = {}

    def build(**kwargs):
        observed.update(kwargs)
        return {"status": "SUCCESS"}

    monkeypatch.setattr(evidence, "_build_longterm_compounding_p3_evidence_summary", build)

    result = evidence.build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary(
        materialized={"v7": "input"},
        evidence_plan={"plan": "v7"},
        replay_executor=lambda _input: {},
    )

    assert result == {"status": "SUCCESS"}
    assert observed["p2_contract"] == P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT


def test_shared_summary_core_rebuilds_the_v7_plan_with_the_v7_identity(monkeypatch) -> None:
    expected_plan = {"plan": "v7"}
    observed: dict[str, object] = {}

    def build_plan(materialized, *, p2_contract):
        observed["plan_materialized"] = materialized
        observed["plan_contract"] = p2_contract
        return expected_plan

    def build_summary(*, materialized, evidence_plan, replay_executor):
        observed["summary_materialized"] = materialized
        observed["summary_plan"] = evidence_plan
        observed["summary_rebuilt_plan"] = summary_module.build_soxl_core_only_p3_evidence_plan(materialized)
        assert callable(replay_executor)
        return {"status": "SUCCESS"}

    summary_module = SimpleNamespace(build_soxl_core_only_p3_evidence_summary=build_summary)
    monkeypatch.setattr(v6_evidence, "_build_longterm_compounding_p3_evidence_plan", build_plan)
    monkeypatch.setattr(v6_evidence, "_load_module", lambda *_args: summary_module)

    result = v6_evidence._build_base_summary(
        materialized={"v7": "input"},
        evidence_plan=expected_plan,
        replay_executor=lambda _input: {},
        p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
    )

    assert result == {"status": "SUCCESS"}
    assert observed["plan_contract"] == P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
    assert observed["summary_rebuilt_plan"] == expected_plan
