from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.run_monthly_report_bundle import build_bundle, render_ai_review_input, render_job_summary
from us_equity_snapshot_pipelines.contracts import get_profile_contract, list_profile_contracts


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_profile_artifacts(root: Path, profile: str, *, snapshot_as_of: str = "2026-04-30") -> None:
    contract = get_profile_contract(profile)
    profile_dir = root / f"us-equity-snapshot-{profile}-123"
    paths = contract.artifact_paths(profile_dir)
    for key, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if key == "release_summary":
            _write_json(
                path,
                {
                    "source_project": "UsEquitySnapshotPipelines",
                    "strategy_profile": profile,
                    "release_status": "ready",
                    "snapshot_as_of": snapshot_as_of,
                    "row_count": 3,
                    "signal_description": f"{profile} signal",
                    "status_description": "ready for monthly review",
                },
            )
        elif key == "ranking":
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["current_rank", "symbol", "final_score", "selected_flag"])
                writer.writeheader()
                writer.writerow({"current_rank": "1", "symbol": "AAPL", "final_score": "0.9", "selected_flag": "true"})
        elif path.suffix == ".json":
            _write_json(path, {"strategy_profile": profile, "snapshot_as_of": snapshot_as_of})
        else:
            path.write_text("symbol,score\nAAPL,0.9\n", encoding="utf-8")


def test_build_bundle_collects_profile_summaries_from_downloaded_artifacts(tmp_path: Path) -> None:
    for contract in list_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)

    bundle = build_bundle(tmp_path, report_month="2026-04", ranking_preview_size=1)
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["status"] == "ok"
    assert bundle["report_month"] == "2026-04"
    assert bundle["missing_profile_count"] == 0
    assert len(bundle["profiles"]) == len(list_profile_contracts())
    assert "US Equity Snapshot Monthly Review Input" in markdown
    assert "AAPL" in markdown
    assert "Missing profiles: `0`" in summary


def test_build_bundle_warns_when_expected_profile_artifacts_are_missing(tmp_path: Path) -> None:
    first_profile = list_profile_contracts()[0].profile
    _write_profile_artifacts(tmp_path, first_profile)

    bundle = build_bundle(tmp_path, report_month="2026-04")

    assert bundle["status"] == "warning"
    assert bundle["missing_profile_count"] == len(list_profile_contracts()) - 1
    assert any(profile["status"] == "missing" for profile in bundle["profiles"])
