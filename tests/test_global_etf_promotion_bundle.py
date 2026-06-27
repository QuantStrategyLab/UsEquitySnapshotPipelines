from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.global_etf_promotion_bundle import (
    build_global_etf_promotion_bundle,
    main,
)


def _write_minimum_artifact_dir(root: Path) -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline85_fast15",
                "Display Name": "Liveable Blend Baseline 85 / Fast 15",
                "Candidate Group": "liveable_candidate",
                "rank": 1,
                "research_gate_passed": True,
                "review_action": "candidate_for_live_promotion_review",
            },
            {
                "Candidate": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "Display Name": "Liveable Trend/Drawdown Brake Baseline 82 / Fast 18 Floor 8",
                "Candidate Group": "liveable_candidate",
                "rank": 2,
                "research_gate_passed": True,
                "review_action": "candidate_for_live_promotion_review",
            },
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline85_fast15",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            },
            {
                "Candidate": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            },
        ]
    )
    walk_forward_summary = pd.DataFrame(
        [
            {
                "Candidate Set": "liveable_blend_baseline85_fast15,liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "Selected Candidate Counts": '{"liveable_trend_drawdown_brake_baseline82_fast18_floor8": 4}',
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "worst_oos_excess_too_low",
            }
        ]
    )
    walk_forward_windows = pd.DataFrame(
        [
            {
                "Train Window": "2020-01-01_2024-12-31",
                "Test Window": "2025",
                "Selected Candidate": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "Selection Action": "promote_candidate",
                "Test Excess CAGR vs Baseline": -0.07624338688173316,
                "Test Drawdown Delta vs Baseline": -0.0013244095459483685,
            }
        ]
    )
    returns = pd.DataFrame(
        [
            {
                "as_of": "2025-01-31",
                "liveable_blend_baseline85_fast15": 0.01,
                "liveable_trend_drawdown_brake_baseline82_fast18_floor8": 0.012,
                "live_global_etf_rotation_defensive_baseline": 0.009,
                "QQQ": 0.02,
                "SPY": 0.015,
            },
            {
                "as_of": "2025-02-28",
                "liveable_blend_baseline85_fast15": -0.02,
                "liveable_trend_drawdown_brake_baseline82_fast18_floor8": -0.03,
                "live_global_etf_rotation_defensive_baseline": -0.01,
                "QQQ": -0.04,
                "SPY": -0.03,
            },
        ]
    )
    rebalance = pd.DataFrame(
        [
            {
                "candidate_id": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "as_of": "2024-12-31",
                "next_date": "2025-01-31",
                "signal_description": "rebalance",
                "overlay_weight": 0.18,
                "base_candidate_id": "live_global_etf_rotation_defensive_baseline",
                "overlay_candidate_id": "offensive_growth_fast_top2_monthly",
            }
        ]
    )
    cost_stress = pd.DataFrame(
        [
            {
                "turnover_cost_bps": 25.0,
                "Candidate": "liveable_blend_baseline85_fast15",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
            },
            {
                "turnover_cost_bps": 25.0,
                "Candidate": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "live_gate_passed": False,
                "live_gate_reason": "rolling_5y_baseline_win_rate_below_60pct",
            },
        ]
    )
    dynamic_cost_stress = pd.DataFrame(
        [
            {
                "Estimated Portfolio NAV": 1_000_000.0,
                "Candidate": "liveable_blend_baseline85_fast15",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
            },
            {
                "Estimated Portfolio NAV": 1_000_000.0,
                "Candidate": "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
            },
        ]
    )

    ranking.to_csv(root / "ranking.csv", index=False)
    live_readiness.to_csv(root / "live_readiness_summary.csv", index=False)
    walk_forward_summary.to_csv(root / "walk_forward_selection_summary.csv", index=False)
    walk_forward_windows.to_csv(root / "walk_forward_selection_windows.csv", index=False)
    returns.to_csv(root / "portfolio_returns_with_benchmarks.csv", index=False)
    rebalance.to_csv(root / "rebalance_events.csv", index=False)
    cost_stress.to_csv(root / "cost_stress_live_readiness_summary.csv", index=False)
    dynamic_cost_stress.to_csv(root / "dynamic_cost_nav_stress_live_readiness_summary.csv", index=False)


def test_build_global_etf_promotion_bundle_writes_review_and_oos_outputs(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_minimum_artifact_dir(artifact_dir)

    output_dir = tmp_path / "bundle"
    result = build_global_etf_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=output_dir,
        candidate_ids=("liveable_trend_drawdown_brake_baseline82_fast18_floor8",),
    )

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    bundle_summary = json.loads((output_dir / "promotion_bundle_summary.json").read_text(encoding="utf-8"))

    assert review["candidate"].tolist() == ["liveable_trend_drawdown_brake_baseline82_fast18_floor8"]
    assert review["blocking_reason"].iloc[0] == "worst_oos_excess_too_low"
    assert (output_dir / "worst_oos_window_diagnostics" / "worst_oos_window_report.md").exists()
    assert manifest["manifest_type"] == "global_etf_promotion_bundle"
    assert manifest["candidate_ids"] == ["liveable_trend_drawdown_brake_baseline82_fast18_floor8"]
    assert bundle_summary["cost_stress_snapshot"]["max_value"] == 25.0
    assert bundle_summary["cost_stress_snapshot"]["failed_candidates"] == [
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8"
    ]
    assert bundle_summary["dynamic_cost_stress_snapshot"]["passed_candidates"] == [
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8"
    ]
    assert bundle_summary["worst_oos_summary"]["selected_candidate"] == (
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8"
    )
    assert result["worst_oos_summary"]["test_year"] == 2025


def test_global_etf_promotion_bundle_cli_uses_experiment_profile_default_scope(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_minimum_artifact_dir(artifact_dir)

    output_dir = tmp_path / "bundle"
    exit_code = main(
        [
            "--artifact-dir",
            str(artifact_dir),
            "--output-dir",
            str(output_dir),
            "--experiment-profile",
            "live_replacement_shortlist_v1",
        ]
    )

    manifest = json.loads((output_dir / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    review = pd.read_csv(output_dir / "live_replacement_review.csv")

    assert exit_code == 0
    assert manifest["experiment_profile"] == "live_replacement_shortlist_v1"
    assert set(review["candidate"]) == {
        "liveable_blend_baseline85_fast15",
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
    }


def test_build_global_etf_promotion_bundle_tolerates_empty_walk_forward_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_minimum_artifact_dir(artifact_dir)
    (artifact_dir / "walk_forward_selection_summary.csv").write_text("\n", encoding="utf-8")
    (artifact_dir / "walk_forward_selection_windows.csv").write_text("\n", encoding="utf-8")

    output_dir = tmp_path / "bundle"
    result = build_global_etf_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=output_dir,
        candidate_ids=("liveable_trend_drawdown_brake_baseline82_fast18_floor8",),
    )

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    assert review["candidate"].tolist() == ["liveable_trend_drawdown_brake_baseline82_fast18_floor8"]
    assert review["next_action"].iloc[0] == "collect_walk_forward_evidence"
    assert "missing_walk_forward_summary" in review["blocking_reason"].iloc[0]
    assert result["worst_oos_summary"]["status"] == "unavailable"
    assert "EmptyDataError" in result["worst_oos_summary"]["reason"]
    assert manifest["worst_oos_summary"]["status"] == "unavailable"
