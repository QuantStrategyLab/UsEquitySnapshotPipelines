from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd

from scripts.run_monthly_report_bundle import build_bundle, render_ai_review_input, render_job_summary
from us_equity_snapshot_pipelines.contracts import get_profile_contract, list_profile_contracts, list_scheduled_profile_contracts


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_profile_artifacts(
    root: Path,
    profile: str,
    *,
    snapshot_as_of: str = "2026-04-30",
    selected_symbols: tuple[str, ...] = ("AAPL",),
    include_selected_flag: bool = True,
) -> None:
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
                    "diagnostics": {"selected_symbols": list(selected_symbols)},
                },
            )
        elif key == "ranking":
            with path.open("w", encoding="utf-8", newline="") as handle:
                fieldnames = ["current_rank", "symbol", "final_score"]
                if include_selected_flag:
                    fieldnames.append("selected_flag")
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                row = {"current_rank": "1", "symbol": "AAPL", "final_score": "0.9"}
                if include_selected_flag:
                    row["selected_flag"] = "true"
                writer.writerow(row)
                writer.writerow({"current_rank": "2", "symbol": "MSFT", "final_score": "0.8"})
        elif path.suffix == ".json":
            _write_json(path, {"strategy_profile": profile, "snapshot_as_of": snapshot_as_of})
        else:
            path.write_text("symbol,score\nAAPL,0.9\n", encoding="utf-8")


def _write_strategy_health_report(root: Path) -> None:
    output_dir = root / "live_strategy_health_global_etf"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "strategy_health_summary.csv").write_text(
        "\n".join(
            [
                "strategy,overall_health_state,overall_reason,full_window_excess_cagr,full_window_drawdown_advantage,watch_windows",
                "global_etf_rotation,watch,one or more windows require monitoring,-0.01,0.08,full",
                "monthly_variant,review_for_retirement,underperforms primary benchmark without enough drawdown advantage,-0.06,-0.02,full",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        output_dir / "run_manifest.json",
        {
            "artifact_type": "live_strategy_health_report",
            "primary_benchmark": "buy_hold_SPY",
            "policy": {"min_observations": 60},
        },
    )
    (output_dir / "strategy_health_report.md").write_text("# Health\n", encoding="utf-8")


def _write_strategy_health_error(root: Path) -> None:
    output_dir = root / "live_strategy_health_error_global_etf"
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        output_dir / "strategy_health_error.json",
        {
            "artifact_type": "live_strategy_health_error",
            "source_returns": str(root / "global_etf" / "portfolio_and_tracker_returns.csv"),
            "error_type": "ValueError",
            "error_message": "bad return matrix",
        },
    )
    (output_dir / "strategy_health_error.md").write_text("# Health Error\n", encoding="utf-8")


def test_build_bundle_collects_profile_summaries_from_downloaded_artifacts(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)

    bundle = build_bundle(tmp_path, report_month="2026-04", ranking_preview_size=1)
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["status"] == "ok"
    assert bundle["report_month"] == "2026-04"
    assert bundle["missing_profile_count"] == 0
    assert bundle["non_ready_profile_count"] == 0
    assert len(bundle["profiles"]) == len(list_scheduled_profile_contracts())
    assert "US Equity Snapshot Monthly Review Input" in markdown
    assert "AAPL" in markdown
    assert "Low-risk docs/tests/monthly-review reporting fixes may be automated" in markdown
    assert "Missing profiles: `0`" in summary
    assert "Non-ready profiles: `0`" in summary


def test_build_bundle_warns_when_expected_profile_artifacts_are_missing(tmp_path: Path) -> None:
    bundle = build_bundle(tmp_path, report_month="2026-04")

    assert bundle["status"] == "warning"
    assert bundle["missing_profile_count"] == len(list_scheduled_profile_contracts())
    assert bundle["non_ready_profile_count"] == len(list_scheduled_profile_contracts())
    assert any(profile["status"] == "missing" for profile in bundle["profiles"])


def test_build_bundle_warns_when_profile_release_status_is_not_ready(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    first_profile = list_scheduled_profile_contracts()[0].profile
    summary_path = next(tmp_path.rglob(f"us-equity-snapshot-{first_profile}-123/release_status_summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["release_status"] = "failed"
    _write_json(summary_path, summary)

    bundle = build_bundle(tmp_path, report_month="2026-04")
    markdown = render_ai_review_input(bundle)
    summary_text = render_job_summary(bundle)

    assert bundle["status"] == "warning"
    assert bundle["missing_profile_count"] == 0
    assert bundle["non_ready_profile_count"] == 1
    assert "Non-ready profiles: `1`" in markdown
    assert "Non-ready profiles: `1`" in summary_text
    assert any(profile["profile"] == first_profile and profile["status"] == "failed" for profile in bundle["profiles"])


def test_build_bundle_fills_selected_preview_from_release_diagnostics(tmp_path: Path) -> None:
    first_profile = list_scheduled_profile_contracts()[0].profile
    _write_profile_artifacts(
        tmp_path,
        first_profile,
        selected_symbols=("MSFT",),
        include_selected_flag=False,
    )

    bundle = build_bundle(tmp_path, report_month="2026-04", ranking_preview_size=2)
    preview = next(profile for profile in bundle["profiles"] if profile["profile"] == first_profile)["ranking_preview"]
    markdown = render_ai_review_input(bundle)

    assert preview[0]["selected"] == "false"
    assert preview[1]["selected"] == "true"
    assert "| 2 | MSFT | 0.8 | true |" in markdown


def test_build_bundle_includes_live_strategy_health_reports(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    _write_strategy_health_report(tmp_path)

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["status"] == "warning"
    assert len(bundle["strategy_health_reports"]) == 1
    assert bundle["strategy_health_reports"][0]["review_for_retirement_count"] == 1
    assert "Live Strategy Health Evidence" in markdown
    assert "`monthly_variant` | `review_for_retirement`" in markdown
    assert "not as permission to delete or disable a strategy automatically" in markdown
    assert "Strategy health reports: `1`" in markdown
    assert "Strategy health reports: `1`" in summary


def test_build_bundle_includes_live_strategy_health_errors(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    _write_strategy_health_error(tmp_path)

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["status"] == "warning"
    assert bundle["strategy_health_error_count"] == 1
    assert bundle["strategy_health_errors"][0]["error_type"] == "ValueError"
    assert "Strategy health reports: `0`" in markdown
    assert "Strategy health errors: `1`" in summary
    assert "Live Strategy Health Build Errors" in markdown
    assert "bad return matrix" in markdown
    assert "do not permit automated strategy removal" in markdown


def test_monthly_review_scripts_e2e_include_health_reports_and_errors(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}{os.pathsep}{env.get('PYTHONPATH', '')}"
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile, snapshot_as_of="2026-06-19")

    dates = pd.bdate_range("2026-01-02", periods=90)
    good_dir = tmp_path / "good_returns_research"
    good_dir.mkdir()
    pd.DataFrame(
        {
            "date": dates,
            "candidate_strategy": [0.0008] * len(dates),
            "lagging_strategy": [0.0001] * len(dates),
            "buy_hold_SPY": [0.0005] * len(dates),
        }
    ).to_csv(good_dir / "portfolio_and_tracker_returns.csv", index=False)
    bad_dir = tmp_path / "bad_returns_research"
    bad_dir.mkdir()
    pd.DataFrame(
        {
            "candidate_strategy": [0.0008] * len(dates),
            "buy_hold_SPY": [0.0005] * len(dates),
        }
    ).to_csv(bad_dir / "portfolio_and_tracker_returns.csv", index=False)

    health = subprocess.run(
        [
            sys.executable,
            "scripts/build_monthly_live_strategy_health_reports.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(tmp_path),
        ],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    bundle_output_dir = tmp_path / "monthly_report_bundle"
    bundle_run = subprocess.run(
        [
            sys.executable,
            "scripts/run_monthly_report_bundle.py",
            "--artifact-root",
            str(tmp_path),
            "--output-dir",
            str(bundle_output_dir),
            "--report-month",
            "2026-06",
        ],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    bundle = json.loads((bundle_output_dir / "monthly_report_bundle.json").read_text(encoding="utf-8"))
    ai_review_input = (bundle_output_dir / "ai_review_input.md").read_text(encoding="utf-8")
    job_summary = (bundle_output_dir / "job_summary.md").read_text(encoding="utf-8")
    assert "health_report_count=1" in health.stdout
    assert "health_report_error_count=1" in health.stdout
    assert "status=warning" in bundle_run.stdout
    assert bundle["status"] == "warning"
    assert bundle["missing_profile_count"] == 0
    assert bundle["non_ready_profile_count"] == 0
    assert len(bundle["strategy_health_reports"]) == 1
    assert bundle["strategy_health_error_count"] == 1
    assert bundle["strategy_health_errors"][0]["error_type"] == "ValueError"
    assert "Strategy health reports: `1`" in ai_review_input
    assert "Strategy health errors: `1`" in ai_review_input
    assert "bad_returns_research" in ai_review_input
    assert "Strategy health reports: `1`" in job_summary
    assert "Strategy health errors: `1`" in job_summary
