from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p3_evidence_plan as plan
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v2_contract import P2_V2_CONTRACT
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p3_input_materializer import (
    MATERIALIZED_INPUT_SCHEMA,
)


def _dates() -> list[str]:
    current = date(2022, 1, 3)
    result: list[str] = []
    while current <= date(2026, 8, 4):
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _materialized() -> dict[str, object]:
    sessions = [
        {
            "as_of": f"{session_date}T00:00:00+00:00",
            "market_data": {"derived_indicators": {}},
            "prices": {"SOXL": 1.0, "SOXX": 1.0, "BOXX": 1.0},
        }
        for session_date in _dates()
    ]
    result = {
        "schema_version": MATERIALIZED_INPUT_SCHEMA,
        "p1_identity": {
            "input_manifest_sha256": "a" * 64,
            "binding_sha256": "b" * 64,
            "bars_member_sha256": "c" * 64,
            "date_cutoff": "2026-08-04",
        },
        "p2_identity": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "indicator_spec": {"id": "soxl-soxx-core-only-close-indicators.v1"},
        "sessions": sessions,
    }
    result["materialized_input_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result


def test_plan_freezes_all_declared_windows_and_costs() -> None:
    materialized = _materialized()
    result = plan.build_soxl_core_only_p3_evidence_plan(materialized)

    assert result["schema_version"] == plan.EVIDENCE_PLAN_SCHEMA
    assert result["cost_bps"] == [5, 10, 15]
    assert result["materialized_input_sha256"] == materialized["materialized_input_sha256"]
    assert len(result["requests"]) == 12
    oos = [item for item in result["requests"] if item["window_kind"] == "rolling_locked_oos"]
    assert len(oos) == 3
    assert all(len(item["session_dates"]) == 252 for item in oos)
    assert all(item["session_dates"][-1] == "2026-08-04" for item in oos)


def test_plan_rejects_a_missing_boundary_or_early_cutoff() -> None:
    missing = _materialized()
    missing["sessions"] = [
        item for item in missing["sessions"] if item["as_of"] != "2024-12-31T00:00:00+00:00"
    ]
    missing["materialized_input_sha256"] = hashlib.sha256(
        _canonical({key: value for key, value in missing.items() if key != "materialized_input_sha256"})
    ).hexdigest()
    with pytest.raises(plan.SoxlCoreOnlyP3EvidencePlanError):
        plan.build_soxl_core_only_p3_evidence_plan(missing)

    early = _materialized()
    early["p1_identity"]["date_cutoff"] = "2026-08-03"
    early["sessions"] = early["sessions"][:-1]
    early["materialized_input_sha256"] = hashlib.sha256(
        _canonical({key: value for key, value in early.items() if key != "materialized_input_sha256"})
    ).hexdigest()
    with pytest.raises(plan.SoxlCoreOnlyP3EvidencePlanError):
        plan.build_soxl_core_only_p3_evidence_plan(early)
