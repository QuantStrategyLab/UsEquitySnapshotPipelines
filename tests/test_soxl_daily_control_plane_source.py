from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import P2_V3_CONTRACT
from us_equity_snapshot_pipelines.lifecycle.soxl_daily_control_plane_source import (
    SOURCE_ID,
    SOURCE_SCHEMA_VERSION,
    SoxlDailyControlPlaneSourceError,
    build_soxl_daily_control_plane_source_snapshot,
)

NOW = "2026-08-22T03:00:00Z"
REVISION = "a" * 40
MANIFEST = "b" * 64
EVIDENCE = "c" * 64


def _build(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "computed_at": NOW, "source_revision": REVISION, "p1_status": "ACCEPTED",
        "p1_manifest_sha256": MANIFEST, "p2_config_sha256": P2_V3_CONTRACT.config_sha256,
        "p3_status": "SUCCESS", "p3_evidence_sha256": EVIDENCE,
    }
    values.update(overrides)
    return build_soxl_daily_control_plane_source_snapshot(**values)


def test_completed_soxl_p3_publishes_bound_research_only_candidate() -> None:
    snapshot = _build()
    assert snapshot["schema_version"] == SOURCE_SCHEMA_VERSION
    assert snapshot["source_id"] == SOURCE_ID
    assert snapshot["errors"] == []
    assert snapshot["candidates"][0] == {
        "candidate_id": P2_V3_CONTRACT.candidate_id, "candidate_kind": "individual", "domain": "us_equity",
        "lifecycle": {"stage": "P3", "status": "verified"},
        "evidence": {"p1_input_digest": MANIFEST, "p2_config_digest": P2_V3_CONTRACT.config_sha256,
                     "p3_evidence_id": EVIDENCE, "source_revision": REVISION},
        "recommendation": {"code": "keep_research", "reason": "P3 evidence completed; candidate remains research-only."},
        "freshness": {"status": "fresh", "age_seconds": 0},
    }


def test_deferred_and_parked_states_never_publish_digests() -> None:
    deferred = _build(p1_status="DEFERRED", p1_reason_code="INPUT_UNAVAILABLE", p1_manifest_sha256="", p3_status="", p3_evidence_sha256="")
    assert deferred["candidates"][0]["evidence"]["p1_input_digest"] is None
    assert deferred["candidates"][0]["evidence"]["p3_evidence_id"] is None
    parked = _build(p3_status="PARKED", p3_evidence_sha256="", p3_failure_class="runtime_internal_failure")
    assert parked["candidates"][0]["lifecycle"] == {"stage": "P3", "status": "parked"}
    assert parked["errors"] == ["p3_parked"]


def test_parked_decision_projection_sets_attention_without_storage_metadata() -> None:
    snapshot = _build(decision_projection_status="PARKED")

    assert snapshot["errors"] == ["decision_data_projection_parked"]
    assert set(snapshot["candidates"][0]["evidence"]) == {
        "p1_input_digest",
        "p2_config_digest",
        "p3_evidence_id",
        "source_revision",
    }


@pytest.mark.parametrize("kwargs", [
    {"p2_config_sha256": "d" * 64},
    {"p1_manifest_sha256": "", "p3_status": "SUCCESS"},
    {"p3_status": "PARKED", "p3_evidence_sha256": "", "p3_failure_class": "unsafe"},
    {"decision_projection_status": "unsafe"},
    {
        "p1_status": "DEFERRED", "p1_reason_code": "INPUT_UNAVAILABLE",
        "p1_manifest_sha256": "", "p3_status": "", "p3_evidence_sha256": "",
        "decision_projection_status": "PUBLISHED",
    },
])
def test_invalid_terminal_state_fails_closed(kwargs: dict[str, object]) -> None:
    with pytest.raises(SoxlDailyControlPlaneSourceError):
        _build(**kwargs)
