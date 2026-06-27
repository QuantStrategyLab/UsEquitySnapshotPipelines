from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_promotion_review import (
    build_crash_brake_promotion_review,
    main,
)


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50_no_brake",
                "CAGR": 0.18,
                "Max Drawdown": -0.20,
                "Sharpe": 0.95,
                "Turnover/Year": 3.0,
            },
            {
                "Run": "crash_brake_top2_50_floor25",
                "CAGR": 0.17,
                "Max Drawdown": -0.18,
                "Sharpe": 0.92,
                "Turnover/Year": 3.1,
            },
            {
                "Run": "blend_top2_25_top4_75_no_brake",
                "CAGR": 0.15,
                "Max Drawdown": -0.16,
                "Sharpe": 0.88,
                "Turnover/Year": 2.9,
            },
        ]
    )


def test_build_crash_brake_promotion_review_marks_rows_research_only() -> None:
    review = build_crash_brake_promotion_review(_summary())
    indexed = review.set_index("Run")

    assert set(review["Run"]) == {
        "blend_top2_50_top4_50_no_brake",
        "crash_brake_top2_50_floor25",
        "blend_top2_25_top4_75_no_brake",
    }
    assert indexed.loc["crash_brake_top2_50_floor25", "Candidate Role"] == "panic_rebound_guard_research"
    assert indexed.loc["crash_brake_top2_50_floor25", "promotion_decision"] == "research_only"
    assert bool(indexed.loc["crash_brake_top2_50_floor25", "required_gates_passed"]) is False
    assert indexed.loc["crash_brake_top2_50_floor25", "recommended_action"] == (
        "collect_live_stress_overfit_liquidity_for_crash_brake_candidate"
    )


def test_crash_brake_promotion_review_cli_writes_manifest(tmp_path: Path) -> None:
    summary_path = tmp_path / "crash_brake_summary.csv"
    manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "review"
    _summary().to_csv(summary_path, index=False)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "russell_top50_crash_brake_research",
                "artifact_schema_version": "russell_top50_crash_brake_research.v1",
                "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
                "candidate_runs": [
                    "blend_top2_50_top4_50_no_brake",
                    "crash_brake_top2_50_floor25",
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    review = pd.read_csv(output_dir / "live_promotion_review.csv")
    manifest = json.loads((output_dir / "crash_brake_promotion_review_manifest.json").read_text(encoding="utf-8"))
    assert review["Run"].tolist() == [
        "blend_top2_50_top4_50_no_brake",
        "crash_brake_top2_50_floor25",
    ]
    assert manifest["manifest_type"] == "russell_top50_crash_brake_promotion_review"
    assert manifest["experiment_profile"] == "panic_rebound_top2_sleeve_floor_v1"
    assert manifest["candidate_runs"] == [
        "blend_top2_50_top4_50_no_brake",
        "crash_brake_top2_50_floor25",
    ]


def test_build_crash_brake_promotion_review_uses_overfit_followup_when_present() -> None:
    summary = _summary()
    overfit = pd.DataFrame(
        [
            {
                "Run": "crash_brake_top2_50_floor25",
                "overfit_gate_passed": False,
                "overfit_gate_reason": "overfit_high_risk;not_promotable_candidate_family",
            }
        ]
    )

    review = build_crash_brake_promotion_review(summary, overfit_promotion=overfit).set_index("Run")

    assert bool(review.loc["crash_brake_top2_50_floor25", "overfit_gate_passed"]) is False
    assert review.loc["crash_brake_top2_50_floor25", "overfit_gate_reason"] == (
        "overfit_high_risk;not_promotable_candidate_family"
    )
    assert "overfit_gate" in review.loc["crash_brake_top2_50_floor25", "required_gate_reason"]
    assert review.loc["blend_top2_50_top4_50_no_brake", "statistical_support_level"] == (
        "research_only_pre_registered_experiment_with_gate_followups"
    )


def test_build_crash_brake_promotion_review_uses_stress_followup_when_present() -> None:
    summary = _summary()
    stress = pd.DataFrame(
        [
            {
                "Run": "crash_brake_top2_50_floor25",
                "all_stress_gates_passed": True,
                "stress_gate_reason": "pass",
            }
        ]
    )

    review = build_crash_brake_promotion_review(summary, stress_summary=stress).set_index("Run")

    assert bool(review.loc["crash_brake_top2_50_floor25", "stress_gate_passed"]) is True
    assert review.loc["crash_brake_top2_50_floor25", "stress_gate_reason"] == "pass"
    assert review.loc["blend_top2_50_top4_50_no_brake", "statistical_support_level"] == (
        "research_only_pre_registered_experiment_with_gate_followups"
    )


def test_build_crash_brake_promotion_review_uses_liquidity_followup_when_present() -> None:
    summary = _summary()
    liquidity = pd.DataFrame(
        [
            {
                "Run": "crash_brake_top2_50_floor25",
                "Portfolio NAV": 100000.0,
                "liquidity_gate_passed": True,
                "liquidity_gate_reason": "pass",
            }
        ]
    )

    review = build_crash_brake_promotion_review(summary, liquidity_summary=liquidity).set_index("Run")

    assert bool(review.loc["crash_brake_top2_50_floor25", "liquidity_gate_passed"]) is True
    assert review.loc["crash_brake_top2_50_floor25", "liquidity_gate_reason"] == "pass"
