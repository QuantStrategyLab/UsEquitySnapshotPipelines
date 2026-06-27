from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_replacement_reviews import (
    build_live_replacement_review_from_inputs,
    discover_replacement_review_inputs,
    main,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_russell_inputs(root: Path) -> None:
    promotion_dir = root / "russell_top50_promotion_bundle_20260620_rerun"
    shadow_dir = root / "russell_top50_shadow_review_20260620"
    decay_dir = root / "live_decay_monitor_russell_top50_fixed_concentration_spa_20260620_rerun"
    promotion_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    decay_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50",
                "Candidate Role": "balanced_offensive_live_design",
                "live_gate_passed": True,
                "overfit_gate_passed": True,
                "required_gates_passed": True,
                "promotion_decision": "live_design_review_balanced_offensive",
                "recommended_action": "preferred_aggressive_live_design_review",
            }
        ]
    ).to_csv(promotion_dir / "live_promotion_review.csv", index=False)
    _write_json(
        promotion_dir / "promotion_bundle_manifest.json",
        {
            "manifest_type": "russell_top50_promotion_bundle",
            "artifact_schema_version": "russell_top50_promotion_bundle.v1",
            "candidate_runs": ["blend_top2_50_top4_50"],
            "artifacts": {
                "live_promotion_review": {"path": "live_promotion_review.csv"},
            },
        },
    )

    pd.DataFrame(
        [
            {
                "active_variant": "blend_top2_50_top4_50",
                "shadow_variant": "top4_baseline",
                "turnover_delta_vs_active": 0.1,
                "review_note": "shadow ok",
            }
        ]
    ).to_csv(shadow_dir / "russell_top50_leader_rotation_shadow_review_rows.csv", index=False)
    _write_json(
        shadow_dir / "russell_top50_leader_rotation_shadow_review_manifest.json",
        {
            "manifest_type": "shadow_review_artifact",
            "artifact_schema_version": "russell_top50_shadow_review_artifact.v1",
            "artifacts": {
                "csv": {"path": "russell_top50_leader_rotation_shadow_review_rows.csv"},
            },
        },
    )

    pd.DataFrame(
        [
            {
                "strategy": "blend_top2_50_top4_50",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    ).to_csv(decay_dir / "live_decay_strategy_summary.csv", index=False)
    _write_json(
        decay_dir / "live_decay_monitor_manifest.json",
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "input_format": "russell_daily",
            "strategies": ["blend_top2_50_top4_50"],
            "artifacts": {
                "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            },
        },
    )


def _write_global_etf_inputs(root: Path) -> None:
    research_dir = root / "global_etf_offensive_rotation_20260624"
    shadow_dir = root / "global_etf_rotation_shadow_review_20260624"
    decay_dir = root / "live_decay_monitor_global_etf_offensive_rotation_20260624"
    research_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    decay_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "Display Name": "Liveable Blend 90/10",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    ).to_csv(research_dir / "ranking.csv", index=False)
    pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    ).to_csv(research_dir / "live_readiness_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "Selected Candidate Counts": '{"liveable_blend_baseline90_fast10": 4}',
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    ).to_csv(research_dir / "walk_forward_selection_summary.csv", index=False)
    _write_json(
        research_dir / "run_manifest.json",
        {
            "research": "global_etf_offensive_rotation",
            "outputs": [
                "ranking.csv",
                "live_readiness_summary.csv",
                "walk_forward_selection_summary.csv",
            ],
        },
    )

    pd.DataFrame(
        [
            {
                "candidate": "liveable_blend_baseline90_fast10",
                "shadow_review_passed": True,
                "review_note": "shadow stable",
            }
        ]
    ).to_csv(shadow_dir / "global_etf_rotation_shadow_review_rows.csv", index=False)
    _write_json(
        shadow_dir / "global_etf_rotation_shadow_review_manifest.json",
        {
            "manifest_type": "shadow_review_artifact",
            "artifact_schema_version": "global_etf_shadow_review_artifact.v1",
            "artifacts": {
                "csv": {"path": "global_etf_rotation_shadow_review_rows.csv"},
            },
        },
    )

    pd.DataFrame(
        [
            {
                "strategy": "liveable_blend_baseline90_fast10",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    ).to_csv(decay_dir / "live_decay_strategy_summary.csv", index=False)
    _write_json(
        decay_dir / "live_decay_monitor_manifest.json",
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "input_format": "wide",
            "strategies": ["liveable_blend_baseline90_fast10", "live_global_etf_rotation_defensive_baseline"],
            "artifacts": {
                "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            },
        },
    )


def _write_global_etf_bundle_inputs(root: Path) -> None:
    _write_global_etf_inputs(root)
    bundle_dir = root / "global_etf_promotion_bundle_20260624"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        bundle_dir / "promotion_bundle_manifest.json",
        {
            "manifest_type": "global_etf_promotion_bundle",
            "artifact_schema_version": "global_etf_promotion_bundle.v1",
            "experiment_profile": "live_replacement_shortlist_v1",
            "candidate_ids": ["liveable_blend_baseline90_fast10"],
            "inputs": {
                "ranking": {"path": str(root / "global_etf_offensive_rotation_20260624" / "ranking.csv")},
                "live_readiness_summary": {
                    "path": str(root / "global_etf_offensive_rotation_20260624" / "live_readiness_summary.csv")
                },
                "walk_forward_selection_summary": {
                    "path": str(root / "global_etf_offensive_rotation_20260624" / "walk_forward_selection_summary.csv")
                },
            },
        },
    )


def _write_snapshot_inputs(root: Path) -> None:
    research_dir = root / "us_equity_strategy_candidates_20260624"
    shadow_dir = root / "snapshot_shadow_review_20260624"
    decay_dir = root / "live_decay_monitor_snapshot_20260624"
    research_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    decay_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Candidate": "opt_r1000_core_momentum_16",
                "Display Name": "R1000 Core Momentum 16",
                "Candidate Group": "optimization_variant",
                "live_gate_passed": True,
                "replacement_review_candidate": True,
                "review_action": "replacement_review_candidate",
                "gate_reason": "pass",
            }
        ]
    ).to_csv(research_dir / "ranking.csv", index=False)
    pd.DataFrame(
        [
            {"as_of": "2026-06-20", "opt_r1000_core_momentum_16": 0.01, "SPY": 0.005},
            {"as_of": "2026-06-23", "opt_r1000_core_momentum_16": 0.002, "SPY": 0.001},
        ]
    ).to_csv(research_dir / "candidate_daily_returns.csv", index=False)
    pd.DataFrame(
        [{"strategy": "opt_r1000_core_momentum_16", "expected_excess_cagr_vs_primary": 0.05}]
    ).to_csv(research_dir / "candidate_expected_excess_cagr.csv", index=False)
    pd.DataFrame(
        [
            {
                "Candidate": "opt_r1000_core_momentum_16",
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    ).to_csv(research_dir / "snapshot_walk_forward_summary.csv", index=False)
    _write_json(
        research_dir / "run_manifest.json",
        {
            "research": "us_equity_strategy_candidates",
            "outputs": [
                "ranking.csv",
                "candidate_daily_returns.csv",
                "candidate_expected_excess_cagr.csv",
                "snapshot_walk_forward_summary.csv",
            ],
        },
    )
    pd.DataFrame(
        [
            {
                "candidate": "opt_r1000_core_momentum_16",
                "shadow_review_passed": True,
                "review_note": "shadow stable",
            }
        ]
    ).to_csv(shadow_dir / "snapshot_us_equity_shadow_review_rows.csv", index=False)
    _write_json(
        shadow_dir / "snapshot_us_equity_shadow_review_manifest.json",
        {
            "manifest_type": "shadow_review_artifact",
            "artifact_schema_version": "snapshot_shadow_review_artifact.v1",
            "artifacts": {"csv": {"path": "snapshot_us_equity_shadow_review_rows.csv"}},
        },
    )
    pd.DataFrame(
        [
            {
                "strategy": "opt_r1000_core_momentum_16",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    ).to_csv(decay_dir / "live_decay_strategy_summary.csv", index=False)
    _write_json(
        decay_dir / "live_decay_monitor_manifest.json",
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "input_format": "wide",
            "strategies": ["opt_r1000_core_momentum_16", "SPY"],
            "artifacts": {"live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"}},
        },
    )


def _write_leveraged_inputs(root: Path) -> None:
    research_dir = root / "leveraged_strategy_candidates_20260624"
    research_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Candidate": "live_tqqq_dual_drive_45_45_proxy",
                "Display Name": "Live TQQQ Proxy",
                "Candidate Group": "current_live_proxy",
                "research_gate_passed": True,
                "replacement_candidate": False,
                "review_action": "keep_current_live",
                "gate_reason": "pass",
            }
        ]
    ).to_csv(research_dir / "ranking.csv", index=False)
    _write_json(
        research_dir / "run_manifest.json",
        {
            "research": "leveraged_strategy_candidates",
            "outputs": ["ranking.csv"],
        },
    )


def _write_crash_brake_review_inputs(root: Path) -> None:
    review_dir = root / "russell_top50_crash_brake_promotion_review_20260624"
    review_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "Run": "crash_brake_top2_50_floor25",
                "Candidate Role": "panic_rebound_guard_research",
                "Gate Profile": "research_only",
                "live_gate_passed": False,
                "live_gate_reason": "research_only_crash_brake_requires_live_gate_followup",
                "stress_gate_passed": False,
                "stress_gate_reason": "research_only_crash_brake_requires_stress_followup",
                "overfit_gate_passed": False,
                "overfit_gate_reason": "research_only_crash_brake_requires_overfit_followup",
                "liquidity_gate_passed": False,
                "liquidity_gate_reason": "research_only_crash_brake_requires_liquidity_followup",
                "required_gates_passed": False,
                "required_gate_reason": "live_gate;stress_gate;overfit_gate;liquidity_gate",
                "statistical_support_level": "research_only_pre_registered_experiment",
                "promotion_decision": "research_only",
                "recommended_action": "collect_live_stress_overfit_liquidity_for_crash_brake_candidate",
            }
        ]
    ).to_csv(review_dir / "live_promotion_review.csv", index=False)
    _write_json(
        review_dir / "crash_brake_promotion_review_manifest.json",
        {
            "manifest_type": "russell_top50_crash_brake_promotion_review",
            "artifact_schema_version": "russell_top50_crash_brake_promotion_review.v1",
            "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
            "candidate_runs": ["crash_brake_top2_50_floor25"],
            "artifacts": {
                "live_promotion_review": {"path": "live_promotion_review.csv"},
            },
        },
    )


def _write_crash_brake_live_decay_inputs(root: Path) -> None:
    decay_dir = root / "live_decay_monitor_crash_brake_20260624"
    decay_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "strategy": "crash_brake_top2_50_floor25",
                "overall_decay_state": "keep",
                "overall_reason": "meets benchmark and expected-edge decay gates",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    ).to_csv(decay_dir / "live_decay_strategy_summary.csv", index=False)
    _write_json(
        decay_dir / "live_decay_monitor_manifest.json",
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "input_format": "russell_daily",
            "strategies": ["crash_brake_top2_50_floor25"],
            "artifacts": {
                "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            },
        },
    )


def _write_crash_brake_shadow_inputs(root: Path) -> None:
    shadow_dir = root / "crash_brake_shadow_review_20260624"
    shadow_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "active_variant": "blend_top2_50_top4_50_no_brake",
                "shadow_variant": "crash_brake_top2_50_floor25",
                "turnover_delta_vs_active": 0.2,
                "review_note": "research-only crash-brake shadow ok",
            }
        ]
    ).to_csv(shadow_dir / "russell_top50_leader_rotation_shadow_review_rows.csv", index=False)
    _write_json(
        shadow_dir / "russell_top50_leader_rotation_shadow_review_manifest.json",
        {
            "manifest_type": "shadow_review_artifact",
            "artifact_schema_version": "russell_top50_shadow_review_artifact.v1",
            "artifacts": {
                "csv": {"path": "russell_top50_leader_rotation_shadow_review_rows.csv"},
            },
        },
    )


def test_discover_replacement_review_inputs_finds_russell_chain(tmp_path: Path) -> None:
    _write_russell_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["promotion"]["review_path"].name == "live_promotion_review.csv"
    assert discovered[0]["shadow"]["csv_path"].name.endswith("shadow_review_rows.csv")
    assert discovered[0]["live_decay"]["summary_path"].name == "live_decay_strategy_summary.csv"


def test_discover_replacement_review_inputs_finds_global_etf_chain(tmp_path: Path) -> None:
    _write_global_etf_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["group_type"] == "global_etf"
    assert discovered[0]["global_etf"]["ranking_path"].name == "ranking.csv"
    assert discovered[0]["shadow"]["csv_path"].name == "global_etf_rotation_shadow_review_rows.csv"
    assert discovered[0]["live_decay"]["summary_path"].name == "live_decay_strategy_summary.csv"


def test_discover_replacement_review_inputs_finds_snapshot_and_leveraged_runs(tmp_path: Path) -> None:
    _write_snapshot_inputs(tmp_path)
    _write_leveraged_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 2
    snapshot = next(item for item in discovered if item["group_type"] == "snapshot")
    leveraged = next(item for item in discovered if item["group_type"] == "leveraged")
    assert snapshot["snapshot"]["ranking_path"].name == "ranking.csv"
    assert snapshot["snapshot"]["walk_forward_path"].name == "snapshot_walk_forward_summary.csv"
    assert leveraged["leveraged"]["ranking_path"].name == "ranking.csv"


def test_discover_replacement_review_inputs_accepts_repo_relative_shadow_artifact_paths(tmp_path: Path, monkeypatch) -> None:
    _write_global_etf_inputs(tmp_path)
    manifest_path = tmp_path / "global_etf_rotation_shadow_review_20260624" / "global_etf_rotation_shadow_review_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    repo_relative = Path("artifacts") / "global_etf_rotation_shadow_review_rows.csv"
    artifact_path = tmp_path / repo_relative
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "global_etf_rotation_shadow_review_20260624" / "global_etf_rotation_shadow_review_rows.csv"
    artifact_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    payload["artifacts"]["csv"]["path"] = str(repo_relative)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    old_cwd = Path.cwd()
    try:
        import os

        os.chdir(tmp_path)
        discovered = discover_replacement_review_inputs(tmp_path)
    finally:
        os.chdir(old_cwd)

    assert len(discovered) == 1
    assert discovered[0]["shadow"]["csv_path"] == artifact_path.resolve()


def test_build_live_replacement_review_from_inputs_writes_manifest(tmp_path: Path) -> None:
    _write_russell_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert len(review) == 1
    assert bool(review.loc[0, "replace_live_now"]) is True
    assert manifest["manifest_type"] == "live_replacement_review"
    assert manifest["replace_live_now_count"] == 1


def test_build_live_replacement_review_from_global_etf_inputs_writes_manifest(tmp_path: Path) -> None:
    _write_global_etf_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert len(review) == 1
    assert bool(review.loc[0, "replace_live_now"]) is True
    assert manifest["manifest_type"] == "live_replacement_review"
    assert manifest["replace_live_now_count"] == 1
    assert manifest["inputs"]["global_etf_ranking"].endswith("ranking.csv")


def test_build_live_replacement_review_from_snapshot_inputs_writes_manifest(tmp_path: Path) -> None:
    _write_snapshot_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert review["candidate"].tolist() == ["opt_r1000_core_momentum_16"]
    assert bool(review.loc[0, "walk_forward_gate_passed"]) is True
    assert review.loc[0, "current_recommendation"] == "replacement_review_candidate"
    assert manifest["inputs"]["snapshot_walk_forward"].endswith("snapshot_walk_forward_summary.csv")
    assert manifest["inputs"]["snapshot_ranking"].endswith("ranking.csv")


def test_build_live_replacement_review_from_empty_snapshot_inputs_writes_empty_review(tmp_path: Path) -> None:
    research_dir = tmp_path / "us_equity_strategy_candidates_20260624"
    research_dir.mkdir(parents=True, exist_ok=True)
    (research_dir / "ranking.csv").write_text("", encoding="utf-8")
    _write_json(
        research_dir / "run_manifest.json",
        {
            "research": "us_equity_strategy_candidates",
            "outputs": ["ranking.csv"],
        },
    )
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert review.empty
    assert manifest["row_count"] == 0


def test_build_live_replacement_review_from_leveraged_inputs_writes_manifest(tmp_path: Path) -> None:
    _write_leveraged_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert review["candidate"].tolist() == ["live_tqqq_dual_drive_45_45_proxy"]
    assert review.loc[0, "current_recommendation"] == "keep_current_live"
    assert manifest["inputs"]["leveraged_ranking"].endswith("ranking.csv")


def test_main_builds_live_replacement_reviews(tmp_path: Path, monkeypatch, capsys) -> None:
    _write_russell_inputs(tmp_path)
    _write_global_etf_inputs(tmp_path)
    _write_snapshot_inputs(tmp_path)
    _write_leveraged_inputs(tmp_path)
    output_root = tmp_path / "reviews"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_replacement_reviews.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert "live_replacement_review_count=4" in captured.out
    assert any(output_root.rglob("live_replacement_manifest.json"))


def test_discover_replacement_review_inputs_prefers_global_etf_bundle_manifest(tmp_path: Path) -> None:
    _write_global_etf_bundle_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["group_type"] == "global_etf"
    assert discovered[0]["global_etf"]["manifest_path"].name == "promotion_bundle_manifest.json"
    assert discovered[0]["global_etf"]["candidate_ids"] == ("liveable_blend_baseline90_fast10",)


def test_discover_replacement_review_inputs_finds_crash_brake_review_chain(tmp_path: Path) -> None:
    _write_crash_brake_review_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["group_type"] == "russell"
    assert discovered[0]["promotion"]["manifest_path"].name == "crash_brake_promotion_review_manifest.json"
    assert discovered[0]["promotion"]["review_path"].name == "live_promotion_review.csv"


def test_build_live_replacement_review_from_crash_brake_inputs_stays_research_only(tmp_path: Path) -> None:
    _write_crash_brake_review_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert len(review) == 1
    assert review.loc[0, "candidate"] == "crash_brake_top2_50_floor25"
    assert bool(review.loc[0, "replace_live_now"]) is False
    assert review.loc[0, "current_recommendation"] == "research_only"
    assert manifest["replace_live_now_count"] == 0


def test_discover_replacement_review_inputs_links_crash_brake_live_decay(tmp_path: Path) -> None:
    _write_crash_brake_review_inputs(tmp_path)
    _write_crash_brake_live_decay_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["live_decay"]["summary_path"].name == "live_decay_strategy_summary.csv"


def test_discover_replacement_review_inputs_prefers_shadow_matching_candidate_runs(tmp_path: Path) -> None:
    _write_russell_inputs(tmp_path)
    _write_crash_brake_review_inputs(tmp_path)
    _write_crash_brake_shadow_inputs(tmp_path)

    discovered = discover_replacement_review_inputs(tmp_path)
    crash_brake_group = next(item for item in discovered if item["promotion"]["manifest_path"].name == "crash_brake_promotion_review_manifest.json")

    assert crash_brake_group["shadow"]["manifest_path"].name == "russell_top50_leader_rotation_shadow_review_manifest.json"
    shadow_rows = pd.read_csv(crash_brake_group["shadow"]["csv_path"])
    assert shadow_rows.loc[0, "shadow_variant"] == "crash_brake_top2_50_floor25"


def test_build_live_replacement_review_from_global_etf_bundle_inputs_filters_candidates(tmp_path: Path) -> None:
    _write_global_etf_bundle_inputs(tmp_path)
    group = discover_replacement_review_inputs(tmp_path)[0]

    output_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path / "out")

    review = pd.read_csv(output_dir / "live_replacement_review.csv")
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert review["candidate"].tolist() == ["liveable_blend_baseline90_fast10"]
    assert manifest["inputs"]["experiment_profile"] == "live_replacement_shortlist_v1"


def test_build_live_replacement_review_from_bundle_inputs_uses_unique_output_dirs(tmp_path: Path) -> None:
    first_root = tmp_path / "global_etf_dynamic_overlay_cap_v1_20260624"
    second_root = tmp_path / "global_etf_dynamic_overlay_cap_v1_coststress_20260624"
    _write_global_etf_bundle_inputs(first_root)
    _write_global_etf_bundle_inputs(second_root)

    discovered = discover_replacement_review_inputs(tmp_path)
    assert len(discovered) == 2

    output_root = tmp_path / "out"
    output_dirs = [build_live_replacement_review_from_inputs(group, output_root=output_root) for group in discovered]

    assert len({path.name for path in output_dirs}) == 2
    assert output_dirs[0] != output_dirs[1]
    assert any("global_etf_dynamic_overlay_cap_v1_20260624" in path.name for path in output_dirs)
    assert any("global_etf_dynamic_overlay_cap_v1_coststress_20260624" in path.name for path in output_dirs)
