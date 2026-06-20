from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd

from scripts.run_monthly_report_bundle import build_bundle, render_ai_review_input, render_job_summary
from us_equity_snapshot_pipelines.contracts import get_profile_contract, list_scheduled_profile_contracts


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


def _write_ibit_dca_research_manifest(root: Path) -> None:
    output_dir = root / "ibit_smart_dca_research"
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        output_dir / "ibit_dca_research_manifest.json",
        {
            "manifest_type": "ibit_smart_dca_research",
            "artifact_schema_version": "ibit_smart_dca_research.v1",
            "inputs": {
                "config": {
                    "ibit_symbol": "IBIT",
                    "parking_symbol": "BOXX",
                    "primary_benchmark": "QQQ",
                    "secondary_benchmark": "SPY",
                    "btc_proxy_symbol": "BTC",
                    "contribution_amount": 500.0,
                },
                "proxy": {
                    "btc_proxy_symbol": "BTC",
                    "proxy_rows_filled": 2500,
                    "proxy_scale_source": "first_actual_ibit_close",
                },
                "variants": ["parking_only", "buy_only_dca", "plugin_on"],
            },
            "row_counts": {
                "ibit_dca_period_summary": 3,
                "ibit_dca_trade_ledger": 24,
                "ibit_dca_signal_consumption": 18,
                "ibit_dca_live_readiness_summary": 3,
            },
            "artifacts": {
                "ibit_dca_period_summary": {"path": "ibit_dca_period_summary.csv"},
                "ibit_dca_trade_ledger": {"path": "ibit_dca_trade_ledger.csv"},
                "ibit_dca_signal_consumption": {"path": "ibit_dca_signal_consumption.csv"},
                "ibit_dca_live_readiness_summary": {"path": "ibit_dca_live_readiness_summary.csv"},
            },
        },
    )


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


def test_build_bundle_includes_ibit_dca_research_manifests(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    _write_ibit_dca_research_manifest(tmp_path)

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["status"] == "ok"
    assert bundle["ibit_dca_research_count"] == 1
    assert bundle["ibit_dca_research_problem_count"] == 0
    research = bundle["ibit_dca_research_reports"][0]
    assert research["manifest_type"] == "ibit_smart_dca_research"
    assert research["variants"] == ["parking_only", "buy_only_dca", "plugin_on"]
    assert research["parking_symbol"] == "BOXX"
    assert research["btc_proxy_symbol"] == "BTC"
    assert research["proxy_rows_filled"] == 2500
    assert "IBIT Smart DCA Research" in markdown
    assert "`parking_only, buy_only_dca, plugin_on`" in markdown
    assert "IBIT DCA research reports: `1`" in summary


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


def test_build_bundle_collects_russell_promotion_manifest(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    manifest_path = tmp_path / "russell_top50_promotion_bundle" / "promotion_bundle_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "russell_top50_promotion_bundle",
            "artifact_schema_version": "russell_top50_promotion_bundle.v1",
            "generated_at": "2026-06-20T10:00:00+00:00",
            "candidate_runs": [
                "base_top4_cap25",
                "blend_top2_25_top4_75",
                "blend_top2_50_top4_50",
            ],
            "portfolio_nav": 5000000,
            "dsr_pbo": {"cscv_groups": 8, "effective_trials": 3},
            "inputs": {
                "summary": {"path": "concentration_variant_summary.csv"},
                "daily_returns": {"path": "concentration_variant_daily_returns.csv"},
            },
            "artifacts": {
                "live_promotion_review": {"path": "live_promotion_review.csv"},
                "spa_qqq_global": {"path": "spa_qqq/spa_global_summary.csv"},
            },
            "review_rows": [
                {
                    "run": "blend_top2_50_top4_50",
                    "required_gates_passed": True,
                    "statistical_support_level": "qqq_and_spy_reality_check_and_spa",
                    "promotion_decision": "live_design_candidate",
                    "recommended_action": "preferred_aggressive_live_design_review",
                }
            ],
        },
    )

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["promotion_bundle_count"] == 1
    assert bundle["promotion_bundle_problem_count"] == 0
    assert bundle["promotion_bundles"][0]["artifact_schema_version"] == "russell_top50_promotion_bundle.v1"
    assert bundle["promotion_bundles"][0]["review_rows"][0]["run"] == "blend_top2_50_top4_50"
    assert "Research Promotion Bundles" in markdown
    assert "preferred_aggressive_live_design_review" in markdown
    assert "DSR/PBO config" in markdown
    assert "Promotion bundles: `1`" in summary


def test_build_bundle_collects_shadow_live_ledger_manifest(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    manifest_path = tmp_path / "russell_top50_shadow_live" / "shadow_live_ledger_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "russell_top50_shadow_live_ledger",
            "artifact_schema_version": "russell_top50_shadow_live_ledger.v1",
            "generated_at": "2026-06-20T11:00:00+00:00",
            "portfolio_nav": 1000000,
            "slippage_bps": 5,
            "forward_window_days": 21,
            "safe_haven": "SGOV",
            "artifacts": {
                "shadow_live_trade_ledger": {"path": "shadow_live_trade_ledger.csv"},
                "shadow_live_holdings_ledger": {"path": "shadow_live_holdings_ledger.csv"},
                "shadow_live_rebalance_summary": {"path": "shadow_live_rebalance_summary.csv"},
            },
            "row_counts": {
                "shadow_live_trade_ledger": 12,
                "shadow_live_holdings_ledger": 18,
                "shadow_live_rebalance_summary": 3,
            },
        },
    )

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["shadow_live_ledger_count"] == 1
    assert bundle["shadow_live_ledger_problem_count"] == 0
    assert bundle["shadow_live_ledgers"][0]["row_counts"]["shadow_live_rebalance_summary"] == 3
    assert "Shadow-live Ledgers" in markdown
    assert "Trade ledger rows: `12`" in markdown
    assert "Shadow-live ledgers: `1`" in summary


def test_build_bundle_collects_capacity_stress_manifest(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    manifest_path = tmp_path / "russell_top50_capacity" / "capacity_stress_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "russell_top50_capacity_stress",
            "artifact_schema_version": "russell_top50_capacity_stress.v1",
            "generated_at": "2026-06-20T12:00:00+00:00",
            "portfolio_nav_values": [1000000, 5000000, 10000000],
            "slippage_bps_values": [5, 25, 50],
            "split_trade_days_values": [1, 2, 3],
            "min_median_net_excess_vs_qqq": 0,
            "artifacts": {
                "capacity_stress_detail": {"path": "capacity_stress_detail.csv"},
                "capacity_stress_summary": {"path": "capacity_stress_summary.csv"},
            },
            "row_counts": {
                "capacity_stress_detail": 54,
                "capacity_stress_summary": 27,
            },
        },
    )

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["capacity_stress_count"] == 1
    assert bundle["capacity_stress_problem_count"] == 0
    assert bundle["capacity_stresses"][0]["row_counts"]["capacity_stress_summary"] == 27
    assert "Capacity Stress Reports" in markdown
    assert "Summary rows: `27`" in markdown
    assert "Capacity stress reports: `1`" in summary


def test_build_bundle_collects_live_decay_monitor_manifest(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    manifest_path = tmp_path / "russell_live_decay" / "live_decay_monitor_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "generated_at": "2026-06-20T13:00:00+00:00",
            "input_format": "russell_daily",
            "strategies": ["blend_top2_50_top4_50"],
            "primary_benchmark": "QQQ",
            "secondary_benchmark": "SPY",
            "windows": [63, 126, 252],
            "policy": {"min_observations": 60},
            "expected_excess_cagr_by_strategy": {"blend_top2_50_top4_50": 0.05},
            "artifacts": {
                "live_decay_window_summary": {"path": "live_decay_window_summary.csv"},
                "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            },
            "row_counts": {
                "live_decay_window_summary": 3,
                "live_decay_strategy_summary": 1,
            },
        },
    )

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["live_decay_monitor_count"] == 1
    assert bundle["live_decay_monitor_problem_count"] == 0
    assert bundle["live_decay_monitors"][0]["strategies"] == ["blend_top2_50_top4_50"]
    assert "Live Decay Monitors" in markdown
    assert "Strategy summary rows: `1`" in markdown
    assert "Live decay monitors: `1`" in summary
