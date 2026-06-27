from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.stage_snapshot_source_inputs import main as stage_inputs_main
from scripts.validate_snapshot_artifact_layout import collect_artifact_layout, main, render_summary
from us_equity_snapshot_pipelines.contracts import get_profile_contract


def _write_core_artifacts(artifact_dir: Path) -> None:
    contract = get_profile_contract("russell_top50_leader_rotation")
    paths = contract.artifact_paths(artifact_dir)
    pd.DataFrame([{"as_of": "2023-05-31", "symbol": "AAPL", "close": 100.0}]).to_csv(paths["snapshot"], index=False)
    pd.DataFrame([{"current_rank": 1, "symbol": "AAPL", "final_score": 0.9}]).to_csv(paths["ranking"], index=False)
    paths["manifest"].write_text(
        json.dumps(
            {
                "manifest_type": "feature_snapshot",
                "contract_version": contract.contract_version,
                "strategy_profile": contract.profile,
            }
        ),
        encoding="utf-8",
    )
    paths["release_summary"].write_text(
        json.dumps(
            {
                "strategy_profile": contract.profile,
                "release_status": "ready",
                "snapshot_as_of": "2023-05-31",
            }
        ),
        encoding="utf-8",
    )


def test_collect_artifact_layout_reports_missing_source_inputs(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_core_artifacts(artifact_dir)

    layout = collect_artifact_layout("russell_top50_leader_rotation", artifact_dir)

    assert layout["missing_files"] == []
    assert layout["missing_source_inputs"] == ["prices.csv", "research_universe.csv"]


def test_render_summary_includes_missing_counts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_core_artifacts(artifact_dir)

    summary = render_summary(collect_artifact_layout("russell_top50_leader_rotation", artifact_dir))

    assert "Snapshot artifact layout diagnostics" in summary
    assert "Missing required source inputs" in summary
    assert "### Missing source inputs" in summary
    assert "- `prices.csv`" in summary
    assert "- `research_universe.csv`" in summary


def test_main_passes_when_core_files_and_source_inputs_exist(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_core_artifacts(artifact_dir)
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    research_universe_path = tmp_path / "research_universe.csv"
    prices_path.write_text("symbol,as_of,close,volume\nAAPL,2023-05-31,100,1000000\n", encoding="utf-8")
    universe_path.write_text("symbol,sector\nAAPL,Information Technology\n", encoding="utf-8")
    research_universe_path.write_text("symbol,sector,mega_rank\nAAPL,Information Technology,1\n", encoding="utf-8")
    assert (
        stage_inputs_main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--research-universe",
                str(research_universe_path),
            ]
        )
        == 0
    )
    summary_path = tmp_path / "layout.md"
    json_path = tmp_path / "layout.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_snapshot_artifact_layout.py",
            "--profile",
            "russell_top50_leader_rotation",
            "--artifact-dir",
            str(artifact_dir),
            "--summary-file",
            str(summary_path),
            "--json-file",
            str(json_path),
        ],
    )

    assert main() == 0
    assert summary_path.exists()
    assert json_path.exists()
    assert "All required core files and staged source inputs are present." in summary_path.read_text(encoding="utf-8")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["missing_files"] == []
    assert payload["missing_source_inputs"] == []


def test_main_fails_when_source_inputs_are_missing(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_core_artifacts(artifact_dir)

    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_snapshot_artifact_layout.py",
            "--profile",
            "russell_top50_leader_rotation",
            "--artifact-dir",
            str(artifact_dir),
            "--summary-file",
            str(tmp_path / "layout.md"),
            "--json-file",
            str(tmp_path / "layout.json"),
        ],
    )

    try:
        main()
    except SystemExit as exc:
        assert exc.code
        assert "missing_source_inputs=prices.csv,research_universe.csv" in str(exc)
        payload = json.loads((tmp_path / "layout.json").read_text(encoding="utf-8"))
        assert payload["missing_source_inputs"] == ["prices.csv", "research_universe.csv"]
    else:
        raise AssertionError("expected missing source inputs to fail validation")
