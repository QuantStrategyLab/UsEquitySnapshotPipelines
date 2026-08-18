from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_p3_evidence_index as index


def _input_producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
        "tool_version": "v1",
    }


def _p3_producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "c" * 40,
        "tree_sha": "d" * 40,
        "tool": "tqqq_p1_p3_nonlive_evidence_index",
        "tool_version": "v1",
        "workflow_run_id": "123",
        "workflow_run_attempt": "1",
    }


def _result() -> dict[str, str]:
    return {
        "evidence_sha256": "e" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "PASS_READY_FOR_SEPARATE_HUMAN_PROMOTION_DECISION",
    }


def _nonlive_scope_record() -> dict[str, str]:
    return {"mandate_id": "tqqq-p1-p3-20260819", "receipt_sha256": "a" * 64}


def test_builds_canonical_nonlive_index_with_only_bound_metadata() -> None:
    value = index.build_tqqq_p3_evidence_index(
        p1_manifest_sha256="f" * 64,
        nonlive_scope_record=_nonlive_scope_record(),
        p3_result=_result(),
        input_producer=_input_producer(),
        producer=_p3_producer(),
    )

    assert value == {
        "schema_version": "qsl.tqqq-p1-p3-evidence-index.v1",
        "candidate": {
            "candidate_id": "tqqq_core_only_p2_v1",
            "config_sha256": "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69",
        },
        "nonlive_scope_record": _nonlive_scope_record(),
        "p1_manifest_sha256": "f" * 64,
        "p3_evidence_sha256": "e" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "PASS_READY_FOR_SEPARATE_HUMAN_PROMOTION_DECISION",
        "input_producer": _input_producer(),
        "producer": _p3_producer(),
        "lifecycle_claims": {
            "authority_scope": "RESEARCH_ONLY",
            "learning_only": True,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
        },
    }
    assert json.loads(index.canonical_tqqq_p3_evidence_index_bytes(value)) == value


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value.update({"unexpected": True}),
        lambda value: value.update({"status": "PARKED"}),
        lambda value: value["nonlive_scope_record"].update({"receipt_sha256": "not-a-digest"}),
        lambda value: value["producer"].update({"workflow_run_id": "0"}),
        lambda value: value["lifecycle_claims"].update({"live_ready": True}),
    ),
)
def test_rejects_unbound_or_nonlive_index_fields(mutate) -> None:
    value = index.build_tqqq_p3_evidence_index(
        p1_manifest_sha256="f" * 64,
        nonlive_scope_record=_nonlive_scope_record(),
        p3_result=_result(),
        input_producer=_input_producer(),
        producer=_p3_producer(),
    )
    mutate(value)

    with pytest.raises(index.TqqqP3EvidenceIndexError):
        index.validate_tqqq_p3_evidence_index(value)
