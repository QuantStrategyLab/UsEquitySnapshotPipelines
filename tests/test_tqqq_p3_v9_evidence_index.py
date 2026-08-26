from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_v9_evidence_index import (
    TqqqP3V9EvidenceIndexError,
    build_tqqq_p3_v9_evidence_index,
    validate_tqqq_p3_v9_evidence_index,
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
        "tool": "tqqq_p1_p3_v9_benchmark_guard_evidence_index",
        "workflow_run_id": "12",
        "workflow_run_attempt": "1",
    }


def _result() -> dict[str, str]:
    return {
        "evidence_sha256": "1" * 64,
        "promotion_result_sha256": "2" * 64,
        "relative_benchmark_policy_sha256": "3" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "PASS_PENDING_FORWARD_CONFIRMATION",
    }


def test_v9_index_binds_the_guard_candidate_without_promotion() -> None:
    index = build_tqqq_p3_v9_evidence_index(
        p1_manifest_sha256="4" * 64,
        nonlive_scope_record={"mandate_id": "tqqq-v9-guard", "receipt_sha256": "5" * 64},
        p3_result=_result(),
        input_producer=_p1_producer(),
        producer=_p3_producer(),
    )

    assert validate_tqqq_p3_v9_evidence_index(index) == index
    assert index["candidate"]["candidate_id"] == "tqqq_core_only_p2_v9_benchmark_drawdown_guard"
    assert index["lifecycle_claims"]["no_order"] is True


def test_v9_index_rejects_mixed_research_provenance() -> None:
    producer = _p3_producer()
    producer["commit_sha"] = "c" * 40

    with pytest.raises(TqqqP3V9EvidenceIndexError):
        build_tqqq_p3_v9_evidence_index(
            p1_manifest_sha256="4" * 64,
            nonlive_scope_record={"mandate_id": "tqqq-v9-guard", "receipt_sha256": "5" * 64},
            p3_result=_result(),
            input_producer=_p1_producer(),
            producer=producer,
        )
