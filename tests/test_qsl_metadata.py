"""Minimal QSL compat metadata smoke tests."""

from __future__ import annotations

from pathlib import Path
import tomllib


def test_qsl_metadata_has_compat_bundle() -> None:
    path = Path(__file__).resolve().parents[1] / "qsl.toml"
    with path.open("rb") as f:
        data = tomllib.load(f)

    qsl = data["qsl"]
    assert qsl["tier"] == "pipeline"
    assert qsl["ring"] == 2
    assert qsl.get("repo") == "UsEquitySnapshotPipelines"
    compat = qsl["compat"]
    assert compat["bundle"] == "2026.07.0"
    assert qsl.get("artifact_contract") == "docs/artifact_contract.md"
    assert qsl.get("snapshot_contract") == "docs/artifact_contract.md"
