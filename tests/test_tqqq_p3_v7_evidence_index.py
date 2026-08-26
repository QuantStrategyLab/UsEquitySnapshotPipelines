from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_v7_evidence_index import (
    TqqqP3V7EvidenceIndexError,
    build_tqqq_p3_v7_evidence_index,
    validate_tqqq_p3_v7_evidence_index,
)


def _producer(*, tool: str) -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": tool,
        "tool_version": "v1",
    }


def _p3_producer() -> dict[str, str]:
    return {
        **_producer(tool="tqqq_p1_p3_v7_nonlive_evidence_index"),
        "workflow_run_id": "12",
        "workflow_run_attempt": "1",
    }


def _result(*, verdict: str = "PASS_PENDING_FORWARD_CONFIRMATION") -> dict[str, str]:
    return {
        "evidence_sha256": "1" * 64,
        "promotion_result_sha256": "2" * 64,
        "relative_benchmark_policy_sha256": "3" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": verdict,
    }


def test_v7_index_binds_the_new_candidate_policy_digest_without_promotion() -> None:
    index = build_tqqq_p3_v7_evidence_index(
        p1_manifest_sha256="4" * 64,
        nonlive_scope_record={"mandate_id": "tqqq-v7-p3", "receipt_sha256": "5" * 64},
        p3_result=_result(), input_producer=_producer(tool="tqqq_core_only_p1_alpaca_sip_acquisition"),
        producer=_p3_producer(),
    )

    assert validate_tqqq_p3_v7_evidence_index(index) == index
    assert index["verdict"] == "PASS_PENDING_FORWARD_CONFIRMATION"
    assert index["lifecycle_claims"]["promotion_eligible"] is False


def test_v7_index_rejects_a_human_ready_verdict_before_forward_confirmation() -> None:
    with pytest.raises(TqqqP3V7EvidenceIndexError):
        build_tqqq_p3_v7_evidence_index(
            p1_manifest_sha256="4" * 64,
            nonlive_scope_record={"mandate_id": "tqqq-v7-p3", "receipt_sha256": "5" * 64},
            p3_result=_result(verdict="PASS_REQUIRES_SEPARATE_HUMAN_PROMOTION"),
            input_producer=_producer(tool="tqqq_core_only_p1_alpaca_sip_acquisition"),
            producer=_p3_producer(),
        )
