from __future__ import annotations

import math
import importlib.util
import hashlib
import json
import os
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
validate_evidence_bundle = getattr(_evidence, "validate_evidence_bundle", None)


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


def _write_bundle(root: Path) -> None:
    root.mkdir(exist_ok=True)
    manifest = _manifest()
    identity = {field: manifest[field] for field in ("profile", "code_sha", "config_sha256", "plugin_sha256", "execution_timing", "timezone", "calendar", "as_of")}
    identity["schema"] = "champion_identity.v1"
    content = {
        "champion_identity.json": json.dumps(identity),
        "prices.csv": "date,value\n2026-07-13,1\n",
        "data_quality.json": '{"ok":true}',
        "daily_returns.csv": "date,value\n2026-07-13,0.1\n",
        "targets.csv": "date,target\n2026-07-13,1\n",
        "trades.csv": "date,symbol\n2026-07-13,SOXL\n",
        "costs.csv": "date,cost\n2026-07-13,0.01\n",
        "metrics.json": '{"metrics":{"sharpe":1.2}}',
        "walk_forward.json": '{"windows":[]}',
        "trial_ledger.jsonl": '{"trial":1}\n',
        "robustness.json": '{"ok":true}',
        "risk_sleeve.json": '{"ok":true}',
        "strategy_performance.v2.json": '{"returns":[0.1]}',
    }
    for name, value in content.items():
        (root / name).write_text(value, encoding="utf-8")
    checksums = "".join(f"{hashlib.sha256(content[name].encode()).hexdigest()}  {name}\n" for name in sorted(content))
    (root / "checksums.sha256").write_text(checksums, encoding="ascii")
    manifest["artifacts"] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in REQUIRED_ARTIFACT_FILES}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_valid_bundle_is_structure_only(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    assert validate_evidence_bundle is not None
    result = validate_evidence_bundle(tmp_path)
    assert result["bundle_integrity_valid"] is True
    assert result["content_safety_status"] == "not_evaluated"
    assert set(result) == {"bundle_integrity_valid", "content_safety_status", "manifest"}
    assert "bundle_secret_free" not in result


def test_manifest_is_validated_before_other_artifacts(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "prices.csv").write_text("bad\n", encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match="manifest"):
        validate_evidence_bundle(tmp_path)


@pytest.mark.parametrize("kind", ["missing", "extra", "directory"])
def test_bundle_root_has_exact_entries(tmp_path: Path, kind: str) -> None:
    _write_bundle(tmp_path)
    if kind == "missing":
        (tmp_path / "prices.csv").unlink()
    elif kind == "extra":
        (tmp_path / "unexpected").write_text("x", encoding="utf-8")
    else:
        (tmp_path / "unexpected").mkdir()
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)


def test_bundle_rejects_symlink_and_duplicate_json(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    target = tmp_path / "prices.csv"
    target.unlink()
    os.symlink("costs.csv", target)
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)
    _write_bundle(tmp_path / "nested")
    (tmp_path / "nested" / "data_quality.json").write_text('{"x":1,"x":2}', encoding="utf-8")
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path / "nested")


def test_bundle_checksum_champion_and_csv_shape_regressions(tmp_path: Path) -> None:
    _write_bundle(tmp_path)
    (tmp_path / "checksums.sha256").write_text("0" * 64 + "  prices.csv\n", encoding="ascii")
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)
    _write_bundle(tmp_path / "valid")
    (tmp_path / "valid" / "trades.csv").write_text("a,a\n1,2\n", encoding="utf-8")
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path / "valid")


def test_bundle_root_symlink_is_rejected(tmp_path: Path) -> None:
    real = tmp_path / "real"
    _write_bundle(real)
    os.symlink(real, tmp_path / "link")
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path / "link")


@pytest.mark.parametrize("name", _evidence._FILE_MAX_BYTES)
def test_each_file_cap_is_enforced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    _write_bundle(tmp_path)
    monkeypatch.setitem(_evidence._FILE_MAX_BYTES, name, 0)
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)


def test_bundle_total_cap_is_enforced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_bundle(tmp_path)
    monkeypatch.setattr(_evidence, "_MAX_BUNDLE_BYTES", 1)
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)
