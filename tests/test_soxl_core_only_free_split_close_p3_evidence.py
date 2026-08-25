from __future__ import annotations

import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_free_split_close_p3_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p3_input_materializer import (
    MATERIALIZED_INPUT_SCHEMA,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v4_free_split_close_contract import (
    P2_V4_FREE_SPLIT_CLOSE_CONTRACT,
)


def _materialized() -> dict[str, object]:
    result = {
        "schema_version": MATERIALIZED_INPUT_SCHEMA,
        "p1_identity": {
            "input_manifest_sha256": "a" * 64,
            "binding_sha256": "b" * 64,
            "closes_member_sha256": "c" * 64,
            "assurance_member_sha256": "d" * 64,
            "date_cutoff": "2026-08-18",
        },
        "p2_identity": {
            "candidate_id": P2_V4_FREE_SPLIT_CLOSE_CONTRACT.candidate_id,
            "config_sha256": P2_V4_FREE_SPLIT_CLOSE_CONTRACT.config_sha256,
        },
        "indicator_spec": {"id": "soxl-soxx-core-only-split-adjusted-close-indicators.v1"},
        "sessions": [],
    }
    result["materialized_input_sha256"] = hashlib.sha256(
        json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    return result


def test_v4_evidence_loader_uses_private_package_modules_for_relative_imports() -> None:
    planner = evidence._load_module("soxl_core_only_p3_evidence_plan.py", "test_v4_plan_core")
    summary = evidence._load_module("soxl_core_only_p3_evidence_summary.py", "test_v4_summary_core")

    assert planner.__package__ == "us_equity_snapshot_pipelines.lifecycle"
    assert summary.__package__ == "us_equity_snapshot_pipelines.lifecycle"


def test_planner_adapter_preserves_both_v4_p1_member_hashes_outside_legacy_mechanics() -> None:
    original, legacy = evidence._legacy_planner_view(_materialized())

    assert original["p1_identity"] == _materialized()["p1_identity"]
    assert set(legacy["p1_identity"]) == {
        "input_manifest_sha256",
        "binding_sha256",
        "bars_member_sha256",
        "date_cutoff",
    }
    assert legacy["p1_identity"]["bars_member_sha256"] != "c" * 64
    assert legacy["indicator_spec"]["id"] == "soxl-soxx-core-only-close-indicators.v1"


def test_planner_adapter_rejects_a_tampered_v4_materialization_digest() -> None:
    materialized = _materialized()
    materialized["p1_identity"]["closes_member_sha256"] = "e" * 64

    with pytest.raises(evidence.SoxlCoreOnlyFreeSplitCloseP3EvidenceError):
        evidence._legacy_planner_view(materialized)
