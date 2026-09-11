"""A cold runner restores fixed financial results instead of replaying again."""
from copy import deepcopy
import json

import pytest

from scripts.soxl_v7_review_checkpoint import preserve_completion, restore_completion
from test_soxl_v7_research_review import complete_record, materialized, _base_summary, _policy  # noqa: F401
from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v7_forward_confirmation_p4_evidence as p4
from us_equity_snapshot_pipelines.lifecycle.soxl_v7_research_review import evaluate_soxl_v7_research_review


class Store:
    def __init__(self):
        self.items = {}

    def read(self, name):
        return deepcopy(self.items.get(name))

    def create(self, name, value):
        assert name not in self.items or self.items[name] == value
        self.items[name] = deepcopy(value)


@pytest.mark.parametrize("rejected", [False, True])
def test_cold_runner_recovers_after_completion_write_and_receipt_failure(tmp_path, monkeypatch, complete_record, rejected):  # noqa: F811
    inputs = materialized(complete_record)
    plan = p4.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(inputs, policy=_policy())
    monkeypatch.setattr(p4, "_build_base_summary", lambda **_: _base_summary(plan, rejected=rejected))
    summary, ticket = evaluate_soxl_v7_research_review(
        record=complete_record, materialized=inputs, policy=_policy(), replay_executor=lambda _: {},
    )
    first = tmp_path / "first"
    (first / "review").mkdir(parents=True)
    (first / "record.json").write_text(json.dumps(complete_record))
    (first / "review/financial-summary.json").write_text(json.dumps(summary))
    if ticket:
        (first / "review/ticket.json").write_text(json.dumps(ticket))
    store = Store()
    assert preserve_completion(first, store)
    # The next operation (publishing the final observation receipt) crashed.
    # A brand-new runner has no local ticket and must not recalculate anything.
    second = tmp_path / "second"
    monkeypatch.setattr(p4, "_build_base_summary", lambda **_: pytest.fail("unexpected rerun"))
    assert restore_completion(second, store, "2027-09-02")
    assert json.loads((second / "record.json").read_text()) == complete_record
    assert json.loads((second / "review/financial-summary.json").read_text()) == summary
    assert (second / "review/ticket.json").exists() is (not rejected)
    if ticket:
        assert json.loads((second / "review/ticket.json").read_text()) == ticket
    assert not restore_completion(tmp_path / "earlier", store, "2026-09-11")
    store.items["completed-review.json"]["financial_summary"]["p1_identity"]["input_manifest_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        restore_completion(tmp_path / "corrupt", store, "2027-09-02")


def test_no_completed_result_is_not_mistaken_for_a_pass(tmp_path):
    assert not restore_completion(tmp_path, Store(), "2027-09-02")
    assert not (tmp_path / "record.json").exists()
