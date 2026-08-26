from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_v8_evidence_index import (
    TqqqP3V8EvidenceIndexError,
    build_tqqq_p3_v8_evidence_index,
    validate_tqqq_p3_v8_evidence_index,
)


def _p1_producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_free_ohlcv_p1",
        "tool_version": "v1",
    }


def _p3_producer() -> dict[str, str]:
    return {
        **_p1_producer(),
        "tool": "tqqq_p1_p3_v8_free_ohlcv_evidence_index",
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


def test_v8_index_binds_free_data_provenance_without_promotion() -> None:
    index = build_tqqq_p3_v8_evidence_index(
        p1_manifest_sha256="4" * 64,
        nonlive_scope_record={"mandate_id": "tqqq-v8-free", "receipt_sha256": "5" * 64},
        p3_result=_result(),
        input_producer=_p1_producer(),
        producer=_p3_producer(),
    )

    assert validate_tqqq_p3_v8_evidence_index(index) == index
    assert index["lifecycle_claims"] == {
        "authority_scope": "RESEARCH_ONLY",
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }


def test_v8_index_rejects_provenance_from_a_different_research_revision() -> None:
    p3_producer = _p3_producer()
    p3_producer["commit_sha"] = "c" * 40

    with pytest.raises(TqqqP3V8EvidenceIndexError):
        build_tqqq_p3_v8_evidence_index(
            p1_manifest_sha256="4" * 64,
            nonlive_scope_record={"mandate_id": "tqqq-v8-free", "receipt_sha256": "5" * 64},
            p3_result=_result(),
            input_producer=_p1_producer(),
            producer=p3_producer,
        )
