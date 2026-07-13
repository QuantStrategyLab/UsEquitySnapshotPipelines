from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.evidence import (
    REQUIRED_ARTIFACT_FILES,
    EvidenceValidationError,
    load_champion_identity,
    validate_champion_identity,
    validate_evidence_bundle,
)


def _identity() -> dict[str, object]:
    return {
        "schema": "champion_identity.v1",
        "profile": "soxl_soxx_trend_income",
        "code_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "plugin_sha256": "c" * 64,
        "execution_timing": "next_open",
        "timezone": "America/New_York",
        "calendar": "XNYS",
        "as_of": "2026-07-14",
    }


def _write_bundle(root: Path) -> dict[str, object]:
    artifact_hashes: dict[str, str] = {}
    for name in REQUIRED_ARTIFACT_FILES:
        if name == "metrics.json":
            content = b'{"metrics": {"sharpe": {"value": 1.0}}}'
        elif name.endswith(".json") or name.endswith(".jsonl"):
            content = b"{}\n"
        elif name.endswith(".csv"):
            content = b"session_date,symbol\n2026-07-14,SOXL\n"
        else:
            content = b"fixture"
        path = root / name
        path.write_bytes(content)
        artifact_hashes[name] = hashlib.sha256(content).hexdigest()
    (root / "champion_identity.json").write_text(json.dumps(_identity()), encoding="utf-8")
    champion_bytes = (root / "champion_identity.json").read_bytes()
    artifact_hashes["champion_identity.json"] = hashlib.sha256(champion_bytes).hexdigest()
    manifest = {
        "schema": "soxl_tqqq_research_evidence.v1",
        "profile": "soxl_soxx_trend_income",
        "run_id": "fixture-run-20260714",
        "code_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "plugin_sha256": "c" * 64,
        "data_sha256": "d" * 64,
        "data_revision": "data-revision-20260714",
        "calendar": "XNYS",
        "timezone": "America/New_York",
        "execution_timing": "next_open",
        "cost_model_id": "baseline-v1",
        "random_seed": 7,
        "as_of": "2026-07-14",
        "generated_at": "2026-07-14T08:00:00Z",
        "artifacts": artifact_hashes,
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


def test_champion_identity_is_minimal_and_loadable(tmp_path: Path) -> None:
    path = tmp_path / "champion_identity.json"
    path.write_text(json.dumps(_identity()), encoding="utf-8")

    assert load_champion_identity(path) == _identity()


@pytest.mark.parametrize("field", ["code_sha", "config_sha256", "plugin_sha256"])
def test_champion_identity_rejects_missing_required_hash(field: str) -> None:
    payload = _identity()
    del payload[field]

    with pytest.raises(EvidenceValidationError, match="required"):
        validate_champion_identity(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("profile", "tqqq/not-a-profile"),
        ("code_sha", "not-a-git-sha"),
        ("config_sha256", "not-a-sha256"),
        ("execution_timing", "same_close"),
        ("as_of", "not-a-date"),
    ],
)
def test_champion_identity_rejects_invalid_contract_values(field: str, value: str) -> None:
    payload = _identity()
    payload[field] = value

    with pytest.raises(EvidenceValidationError):
        validate_champion_identity(payload)


@pytest.mark.parametrize("field", ["account_id", "balance", "order_id", "credential", "token", "service_id"])
def test_champion_identity_rejects_private_or_unknown_fields(field: str) -> None:
    payload = _identity()
    payload[field] = "must not be accepted"

    with pytest.raises(EvidenceValidationError):
        validate_champion_identity(payload)


def test_evidence_bundle_requires_data_hash_and_all_required_files(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path)
    assert validate_evidence_bundle(tmp_path)["profile"] == manifest["profile"]

    manifest.pop("data_sha256")
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(EvidenceValidationError, match="data_sha256"):
        validate_evidence_bundle(tmp_path)

    (tmp_path / "prices.parquet").unlink()
    manifest["data_sha256"] = "d" * 64
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match="required file"):
        validate_evidence_bundle(tmp_path)


def test_evidence_bundle_rejects_prose_only_metrics(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path)
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(json.dumps("Sharpe was good"), encoding="utf-8")
    manifest["artifacts"]["metrics.json"] = hashlib.sha256(metrics_path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(EvidenceValidationError, match="metrics"):
        validate_evidence_bundle(tmp_path)


def test_evidence_bundle_rejects_sensitive_artifact_columns(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path)
    trades_path = tmp_path / "trades.csv"
    trades_path.write_text("session_date,account_id\n2026-07-14,redacted\n", encoding="utf-8")
    manifest["artifacts"]["trades.csv"] = hashlib.sha256(trades_path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(tmp_path)


@pytest.mark.parametrize("artifact_name", ["../secret.txt", "/private/secret.txt", "C:\\secret.txt"])
def test_evidence_bundle_rejects_unsafe_artifact_paths(tmp_path: Path, artifact_name: str) -> None:
    manifest = _write_bundle(tmp_path)
    manifest["artifacts"][artifact_name] = "e" * 64
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(EvidenceValidationError, match="path"):
        validate_evidence_bundle(tmp_path)
