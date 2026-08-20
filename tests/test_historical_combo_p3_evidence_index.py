from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import historical_combo_p3_evidence_index as index


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "historical_combo_p3_evidence_index",
        "tool_version": "v1",
        "workflow_run_id": "123",
        "workflow_run_attempt": "1",
    }


def _result() -> dict[str, str]:
    return {
        "evidence_sha256": "e" * 64,
        "status": index.P3_STATUS,
        "verdict": "PASS_RESEARCH_EVIDENCE_NOT_PROMOTION",
    }


def _value() -> dict[str, object]:
    return index.build_historical_combo_p3_evidence_index(
        candidate_id="us-equity-three-sleeve-baseline",
        p1_input_sha256="f" * 64,
        p2_candidate_sha256="d" * 64,
        p3_result=_result(),
        producer=_producer(),
    )


def test_builds_canonical_p1_p2_p3_research_only_index() -> None:
    value = _value()

    assert value == {
        "schema_version": index.SCHEMA_VERSION,
        "research_only": True,
        "candidate": {"candidate_id": "us-equity-three-sleeve-baseline"},
        "p1_input_sha256": "f" * 64,
        "p2_candidate_sha256": "d" * 64,
        "p3_evidence_sha256": "e" * 64,
        "status": index.P3_STATUS,
        "verdict": "PASS_RESEARCH_EVIDENCE_NOT_PROMOTION",
        "producer": _producer(),
        "lifecycle_claims": {
            "authority_scope": "RESEARCH_ONLY",
            "promotion_eligible": False,
            "paper_authorized": False,
            "shadow_authorized": False,
            "live_authorized": False,
            "no_order": True,
        },
    }
    assert json.loads(index.canonical_historical_combo_p3_evidence_index_bytes(value)) == value


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value.update({"unexpected": True}),
        lambda value: value.update({"research_only": False}),
        lambda value: value.update({"status": "PARKED"}),
        lambda value: value["candidate"].update({"candidate_id": "not a valid id"}),
        lambda value: value["producer"].update({"workflow_run_id": "0"}),
        lambda value: value["lifecycle_claims"].update({"shadow_authorized": True}),
    ),
)
def test_rejects_unbound_or_execution_capable_index_fields(mutate) -> None:
    value = _value()
    mutate(value)

    with pytest.raises(index.HistoricalComboP3EvidenceIndexError):
        index.validate_historical_combo_p3_evidence_index(value)


def test_rejects_non_completed_result_before_index_creation() -> None:
    with pytest.raises(
        index.HistoricalComboP3EvidenceIndexError, match="invalid completed P3 result"
    ):
        index.build_historical_combo_p3_evidence_index(
            candidate_id="us-equity-three-sleeve-baseline",
            p1_input_sha256="f" * 64,
            p2_candidate_sha256="d" * 64,
            p3_result={
                "evidence_sha256": "e" * 64,
                "status": "PARKED",
                "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
            },
            producer=_producer(),
        )
