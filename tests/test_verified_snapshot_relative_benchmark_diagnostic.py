from __future__ import annotations

import copy
import hashlib

import pytest

from us_equity_snapshot_pipelines.lifecycle.verified_snapshot_relative_benchmark_diagnostic import (
    VerifiedSnapshotRelativeBenchmarkDiagnosticError,
    build_verified_snapshot_relative_benchmark_diagnostic,
    canonical_verified_snapshot_relative_benchmark_diagnostic_bytes,
    validate_verified_snapshot_relative_benchmark_diagnostic,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _terminal() -> dict[str, str]:
    return {
        "evidence_sha256": _digest("evidence"),
        "promotion_result_sha256": _digest("promotion"),
        "relative_benchmark_policy_sha256": _digest("policy"),
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
    }


def _record() -> dict[str, object]:
    return build_verified_snapshot_relative_benchmark_diagnostic(
        candidate={"candidate_id": "tqqq_core_only_p2_v8_free_ohlcv_relative_benchmark", "config_sha256": _digest("config")},
        input_manifest_sha256=_digest("manifest"),
        input_producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": "a" * 40,
            "tree_sha": "b" * 40,
            "tool": "tqqq_core_only_free_ohlcv_p1",
            "tool_version": "v1",
        },
        p3_terminal=_terminal(),
        relative_benchmark_summary_sha256=_digest("summary"),
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": "c" * 40,
            "tree_sha": "d" * 40,
            "tool": "verified_snapshot_relative_benchmark_diagnostic",
            "tool_version": "v1",
            "workflow_run_id": "1",
            "workflow_run_attempt": "1",
        },
    )


def test_verified_snapshot_diagnostic_is_canonical_and_explicitly_non_authoritative() -> None:
    record = _record()

    assert validate_verified_snapshot_relative_benchmark_diagnostic(record) == record
    assert canonical_verified_snapshot_relative_benchmark_diagnostic_bytes(record) == canonical_verified_snapshot_relative_benchmark_diagnostic_bytes(record)
    assert record["lifecycle_claims"] == {
        "authority_scope": "RESEARCH_ONLY",
        "diagnostic_only": True,
        "formal_evidence_index": False,
        "promotion_eligible": False,
        "live_ready": False,
        "no_order": True,
    }


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("lifecycle_claims", "formal_evidence_index"), True),
        (("source_snapshot", "input_producer", "commit_sha"), "not-a-revision"),
        (("diagnostic", "producer", "tool"), "formal_evidence_index"),
        (("diagnostic", "p3_terminal", "status"), "PARKED"),
    ],
)
def test_verified_snapshot_diagnostic_rejects_claim_and_provenance_tampering(
    path: tuple[str, ...], value: object
) -> None:
    record = copy.deepcopy(_record())
    target: dict[str, object] = record
    for key in path[:-1]:
        target = target[key]  # type: ignore[assignment,index]
    target[path[-1]] = value

    with pytest.raises(VerifiedSnapshotRelativeBenchmarkDiagnosticError):
        validate_verified_snapshot_relative_benchmark_diagnostic(record)
