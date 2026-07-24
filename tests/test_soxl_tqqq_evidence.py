from __future__ import annotations

import math
import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location("r0a_evidence", Path(__file__).parents[1] / "src/us_equity_snapshot_pipelines/evidence.py")
assert _spec and _spec.loader
_evidence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_evidence)
REQUIRED_ARTIFACT_FILES = _evidence.REQUIRED_ARTIFACT_FILES
EvidenceValidationError = _evidence.EvidenceValidationError
validate_evidence_manifest = _evidence.validate_evidence_manifest
validate_metrics_payload = _evidence.validate_metrics_payload


def _manifest() -> dict[str, object]:
    return {
        "schema": "soxl_tqqq_research_evidence.v1",
        "profile": "soxl_soxx_trend_income",
        "run_id": "r0a.test",
        "code_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "plugin_sha256": "c" * 64,
        "data_sha256": "d" * 64,
        "data_revision": "prices-2026.07",
        "calendar": "XNYS",
        "timezone": "America/New_York",
        "execution_timing": "next_open",
        "cost_model_id": "bps.5",
        "random_seed": 0,
        "as_of": "2026-07-13",
        "generated_at": "2026-07-13T18:15:03.123Z",
        "artifacts": {name: "e" * 64 for name in REQUIRED_ARTIFACT_FILES},
    }


def test_valid_manifest_needs_no_bundle_root() -> None:
    payload = _manifest()
    assert validate_evidence_manifest(payload) == payload


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["artifacts"].pop(REQUIRED_ARTIFACT_FILES[0]),
        lambda p: p["artifacts"].update({"prices.parquet": "e" * 64}),
        lambda p: p["artifacts"].update({"prices.csv": "E" * 64}),
        lambda p: p["artifacts"].update({"prices.csv": "e" * 63}),
    ],
)
def test_manifest_rejects_non_exact_artifact_mapping(mutate) -> None:
    payload = _manifest()
    mutate(payload)
    with pytest.raises(EvidenceValidationError):
        validate_evidence_manifest(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("as_of", "2026-02-30"),
        ("as_of", "2026-7-3"),
        ("generated_at", "2026-02-30T18:15:03Z"),
        ("generated_at", "2026-07-13T18:15:03+00:00"),
        ("generated_at", "2026-07-13T18:15:03+01:00"),
    ],
)
def test_manifest_rejects_annotation_only_dates(field, value) -> None:
    payload = _manifest()
    payload[field] = value
    with pytest.raises(EvidenceValidationError):
        validate_evidence_manifest(payload)


def test_metrics_require_numeric_finite_evidence() -> None:
    assert validate_metrics_payload({"metrics": {"sharpe": 1.2, "nested": {"mdd": -0.3}}})["metrics"]
    for value in (math.nan, math.inf, -math.inf):
        with pytest.raises(EvidenceValidationError):
            validate_metrics_payload({"metrics": {"risk": {"value": value}}})
    with pytest.raises(EvidenceValidationError):
        validate_metrics_payload({"metrics": {"note": "no numeric evidence", "ok": True}})
