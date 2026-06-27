from __future__ import annotations

import json
from pathlib import Path

from scripts.stage_snapshot_source_inputs import main as stage_inputs_main
from scripts.validate_monthly_russell_crash_brake_inputs import (
    discover_russell_snapshot_artifacts,
    main,
    render_summary,
)


def _write_release_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "strategy_profile": "russell_top50_leader_rotation",
                "release_status": "ready",
                "snapshot_as_of": "2023-05-31",
            }
        ),
        encoding="utf-8",
    )


def test_discover_russell_snapshot_artifacts_reports_missing_source_inputs(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_release_summary(artifact_dir / "release_status_summary.json")

    discovered = discover_russell_snapshot_artifacts(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["missing_source_inputs"] == ["prices.csv", "research_universe.csv"]


def test_render_summary_reports_missing_source_inputs(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_release_summary(artifact_dir / "release_status_summary.json")

    summary = render_summary(discover_russell_snapshot_artifacts(tmp_path))

    assert "Russell crash-brake input diagnostics" in summary
    assert "prices.csv,research_universe.csv" in summary


def test_main_passes_when_required_staged_inputs_exist(tmp_path: Path, monkeypatch, capsys) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_release_summary(artifact_dir / "release_status_summary.json")
    prices_path = tmp_path / "prices.csv"
    research_universe_path = tmp_path / "research_universe.csv"
    universe_path = tmp_path / "universe.csv"
    prices_path.write_text("symbol,as_of,close,volume\nAAPL,2023-05-31,100,1000000\n", encoding="utf-8")
    research_universe_path.write_text("symbol,sector,mega_rank\nAAPL,Information Technology,1\n", encoding="utf-8")
    universe_path.write_text("symbol,sector,mega_rank\nAAPL,Information Technology,1\n", encoding="utf-8")

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

    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_monthly_russell_crash_brake_inputs.py",
            "--artifact-root",
            str(tmp_path),
            "--summary-file",
            str(tmp_path / "summary.md"),
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert "russell_snapshot_artifact_count=1" in captured.out
    assert (tmp_path / "summary.md").exists()


def test_main_fails_when_russell_artifact_is_missing_required_staged_inputs(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_release_summary(artifact_dir / "release_status_summary.json")

    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_monthly_russell_crash_brake_inputs.py",
            "--artifact-root",
            str(tmp_path),
            "--summary-file",
            str(tmp_path / "summary.md"),
        ],
    )

    try:
        main()
    except SystemExit as exc:
        assert exc.code
        assert "missing staged source inputs" in str(exc)
        assert (tmp_path / "summary.md").exists()
    else:
        raise AssertionError("expected missing staged source inputs to fail validation")
