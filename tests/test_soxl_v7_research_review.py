"""Synthetic admission tests; these fixtures are never production evidence."""
from copy import deepcopy

import pytest

from test_soxl_v7_nonlive_forward_observation import _record, _materialized
from test_soxl_core_only_v7_forward_confirmation_p4_evidence import _policy, _base_summary
from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v7_forward_confirmation_p4_evidence as p4
from us_equity_snapshot_pipelines.lifecycle import soxl_v7_research_review as review


@pytest.fixture(scope="module")
def complete_record():
    return _record(count=252, observed_at="2027-09-01T00:00:00Z")


def materialized(record):
    value = _materialized(len(record["observation_sessions"]))
    value["schema_version"] = "qsl.soxl-soxx-core-only-p3-free-split-close-materialized-input.v1"
    value["indicator_spec"] = {"id": "synthetic"}
    value["materialized_input_sha256"] = p4._sha256(value)
    return value


def test_waiting_record_does_not_evaluate_or_open_ticket():
    record = _record(count=1)
    assert review.evaluate_soxl_v7_research_review(
        record=record, materialized={}, policy=_policy(),
        replay_executor=lambda _: pytest.fail("must not replay before window completion"),
    ) == (None, None)


@pytest.mark.parametrize("rejected", [False, True])
def test_financial_gate_controls_same_candidate_admission(monkeypatch, complete_record, rejected):
    inputs = materialized(complete_record)
    plan = p4.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(inputs, policy=_policy())
    monkeypatch.setattr(p4, "_build_base_summary", lambda **_: _base_summary(plan, rejected=rejected))
    summary, ticket = review.evaluate_soxl_v7_research_review(
        record=complete_record, materialized=inputs, policy=_policy(), replay_executor=lambda _: {},
    )
    assert summary["forward_confirmation_policy"]["forward_confirmation_satisfied"] is (not rejected)
    if rejected:
        assert ticket is None
    else:
        assert ticket["state"] == "awaiting_human"
        assert ticket["live_authority_granted"] is False
        assert ticket["drift_status"] == "not_applicable"
        assert ticket["shadow_evidence_kind"] == "v7_nonlive_shadow_and_simulated_paper"
        assert ticket["proposed_params"]["config_sha256"] == complete_record["candidate_config_sha256"]
        assert complete_record["record_sha256"] in " ".join(ticket["notes"])
        assert summary["evidence_summary_sha256"] in " ".join(ticket["notes"])


@pytest.mark.parametrize("mismatch", ["p1", "candidate", "sessions", "future"])
def test_rejects_mixed_or_premature_evidence_before_evaluation(complete_record, mismatch):
    record = deepcopy(complete_record)
    inputs = materialized(record)
    if mismatch == "p1":
        inputs["p1_identity"]["input_manifest_sha256"] = "0" * 64
    elif mismatch == "candidate":
        inputs["p2_identity"]["candidate_id"] = "another_soxl_candidate"
    elif mismatch == "sessions":
        inputs["sessions"].pop()
    else:
        record["observed_at"] = "2026-09-01T00:00:00Z"
        record["record_sha256"] = p4._sha256({k: v for k, v in record.items() if k != "record_sha256"})
    inputs["materialized_input_sha256"] = p4._sha256({k: v for k, v in inputs.items() if k != "materialized_input_sha256"})
    with pytest.raises(ValueError):
        review.evaluate_soxl_v7_research_review(
            record=record, materialized=inputs, policy=_policy(),
            replay_executor=lambda _: pytest.fail("mismatched input reached replay"),
        )
