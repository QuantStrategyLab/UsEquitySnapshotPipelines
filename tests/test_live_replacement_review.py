from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.live_replacement_review import (
    build_live_replacement_review,
    main,
)


def test_build_live_replacement_review_blocks_global_etf_candidate_on_walk_forward_failure() -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "Display Name": "Liveable Blend 90/10",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Selected Candidate Counts": '{"liveable_blend_baseline90_fast10": 3}',
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "worst_oos_excess_too_low",
            }
        ]
    )

    review = build_live_replacement_review(
        global_etf_ranking=ranking,
        global_etf_live_readiness=live_readiness,
        global_etf_walk_forward_summary=walk_forward,
    )
    row = review.iloc[0]

    assert row["strategy_line"] == "global_etf_rotation"
    assert bool(row["baseline_gate_passed"]) is True
    assert bool(row["walk_forward_gate_passed"]) is False
    assert bool(row["required_gates_passed"]) is False
    assert row["current_recommendation"] == "blocked_by_walk_forward_oos"
    assert row["next_action"] == "keep_current_live"
    assert row["replace_live_now_reason"] == "shadow_live_evidence_required_before_live_change"
    assert "worst_oos_excess_too_low" in row["blocking_reason"]


def test_build_live_replacement_review_blocks_global_etf_candidate_when_in_walk_forward_set_but_never_selected() -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline82_fast18",
                "Display Name": "Liveable Blend 82/18",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline82_fast18",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Candidate Set": "liveable_blend_baseline82_fast18,liveable_blend_baseline80_fast20",
                "Selected Candidate Counts": '{"liveable_blend_baseline80_fast20": 4}',
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "worst_oos_excess_too_low",
            }
        ]
    )

    review = build_live_replacement_review(
        global_etf_ranking=ranking,
        global_etf_live_readiness=live_readiness,
        global_etf_walk_forward_summary=walk_forward,
    )
    row = review.iloc[0]

    assert bool(row["baseline_gate_passed"]) is True
    assert bool(row["walk_forward_gate_passed"]) is False
    assert bool(row["required_gates_passed"]) is False
    assert row["current_recommendation"] == "blocked_by_walk_forward_oos"
    assert row["next_action"] == "keep_current_live"
    assert "not_selected_in_walk_forward" in row["blocking_reason"]


def test_build_live_replacement_review_marks_global_etf_ready_only_after_shadow_and_decay_pass() -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "Display Name": "Liveable Blend 90/10",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Selected Candidate Counts": '{"liveable_blend_baseline90_fast10": 4}',
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    )
    shadow_review = pd.DataFrame(
        [
            {
                "candidate": "liveable_blend_baseline90_fast10",
                "shadow_review_passed": True,
                "review_note": "shadow stable",
            }
        ]
    )
    live_decay = pd.DataFrame(
        [
            {
                "strategy": "liveable_blend_baseline90_fast10",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    )

    review = build_live_replacement_review(
        global_etf_ranking=ranking,
        global_etf_live_readiness=live_readiness,
        global_etf_walk_forward_summary=walk_forward,
        global_etf_shadow_review=shadow_review,
        global_etf_live_decay_summary=live_decay,
    )
    row = review.iloc[0]

    assert bool(row["required_gates_passed"]) is True
    assert bool(row["shadow_review_present"]) is True
    assert bool(row["shadow_review_passed"]) is True
    assert bool(row["live_decay_present"]) is True
    assert bool(row["live_decay_passed"]) is True
    assert row["current_recommendation"] == "candidate_for_live_promotion_review"
    assert row["next_action"] == "ready_for_live_config_change"
    assert bool(row["replace_live_now"]) is True
    assert row["replace_live_now_reason"] == "all_review_evidence_present_and_passed"


def test_build_live_replacement_review_collects_global_etf_shadow_before_live_change() -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline85_fast15",
                "Display Name": "Liveable Blend 85/15",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline85_fast15",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Selected Candidate Counts": '{"liveable_blend_baseline85_fast15": 3}',
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    )

    review = build_live_replacement_review(
        global_etf_ranking=ranking,
        global_etf_live_readiness=live_readiness,
        global_etf_walk_forward_summary=walk_forward,
    )
    row = review.iloc[0]

    assert bool(row["required_gates_passed"]) is True
    assert bool(row["replace_live_now"]) is False
    assert row["next_action"] == "collect_shadow_review_evidence"
    assert "missing_shadow_review_artifact" in row["blocking_reason"]


def test_build_live_replacement_review_marks_missing_walk_forward_evidence_for_global_etf_candidate() -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline75_fast25",
                "Display Name": "Liveable Blend 75/25",
                "Candidate Group": "liveable_candidate",
                "research_gate_passed": True,
                "review_action": "live_design_review",
            }
        ]
    )
    live_readiness = pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline75_fast25",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            }
        ]
    )

    review = build_live_replacement_review(
        global_etf_ranking=ranking,
        global_etf_live_readiness=live_readiness,
        global_etf_walk_forward_summary=pd.DataFrame(),
    )
    row = review.iloc[0]

    assert bool(row["baseline_gate_passed"]) is True
    assert bool(row["walk_forward_gate_passed"]) is False
    assert bool(row["required_gates_passed"]) is False
    assert row["next_action"] == "collect_walk_forward_evidence"
    assert "missing_walk_forward_summary" in row["blocking_reason"]


def test_build_live_replacement_review_marks_russell_ready_only_after_shadow_and_decay_pass() -> None:
    russell = pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50",
                "Candidate Role": "balanced_offensive_live_design",
                "live_gate_passed": True,
                "overfit_gate_passed": True,
                "required_gates_passed": True,
                "promotion_decision": "live_design_review_balanced_offensive",
                "recommended_action": "preferred_aggressive_live_design_review",
                "statistical_support_level": "qqq_and_spy_reality_check_and_spa",
            }
        ]
    )
    shadow_review = pd.DataFrame(
        [
            {
                "active_variant": "blend_top2_50_top4_50",
                "shadow_variant": "top4_baseline",
                "turnover_delta_vs_active": 0.12,
                "review_note": "shadow review completed",
            }
        ]
    )
    live_decay = pd.DataFrame(
        [
            {
                "strategy": "blend_top2_50_top4_50",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    )

    review = build_live_replacement_review(
        russell_promotion_review=russell,
        russell_shadow_review=shadow_review,
        russell_live_decay_summary=live_decay,
    )
    row = review.iloc[0]

    assert row["strategy_line"] == "russell_top50_leader_rotation"
    assert bool(row["required_gates_passed"]) is True
    assert bool(row["shadow_review_present"]) is True
    assert bool(row["shadow_review_passed"]) is True
    assert bool(row["live_decay_present"]) is True
    assert bool(row["live_decay_passed"]) is True
    assert row["current_recommendation"] == "live_design_review_balanced_offensive"
    assert row["next_action"] == "ready_for_live_config_change"
    assert bool(row["replace_live_now"]) is True
    assert row["replace_live_now_reason"] == "all_review_evidence_present_and_passed"


def test_build_live_replacement_review_downgrades_russell_when_shadow_or_decay_missing() -> None:
    russell = pd.DataFrame(
        [
            {
                "Run": "blend_top2_25_top4_75",
                "Candidate Role": "conservative_live_design",
                "live_gate_passed": True,
                "overfit_gate_passed": True,
                "required_gates_passed": True,
                "promotion_decision": "live_design_review_conservative",
                "recommended_action": "promote_conservative_live_design_review",
            }
        ]
    )

    review = build_live_replacement_review(russell_promotion_review=russell)
    row = review.iloc[0]

    assert bool(row["replace_live_now"]) is False
    assert row["next_action"] == "collect_shadow_review_evidence"
    assert "missing_shadow_review_artifact" in row["blocking_reason"]
    assert "missing_live_decay_artifact" in row["blocking_reason"]


def test_build_live_replacement_review_uses_decay_state_to_block_russell_live_change() -> None:
    russell = pd.DataFrame(
        [
            {
                "Run": "blend_top2_25_top4_75",
                "Candidate Role": "conservative_live_design",
                "live_gate_passed": True,
                "overfit_gate_passed": True,
                "required_gates_passed": True,
                "promotion_decision": "live_design_review_conservative",
                "recommended_action": "promote_conservative_live_design_review",
            }
        ]
    )
    shadow_review = pd.DataFrame(
        [
            {
                "active_variant": "blend_top2_25_top4_75",
                "shadow_variant": "top4_baseline",
                "turnover_delta_vs_active": 0.08,
                "review_note": "shadow ok",
            }
        ]
    )
    live_decay = pd.DataFrame(
        [
            {
                "strategy": "blend_top2_25_top4_75",
                "overall_decay_state": "watch",
                "overall_reason": "one or more windows require monitoring",
                "recommended_action": "monitor_next_cycle",
            }
        ]
    )

    review = build_live_replacement_review(
        russell_promotion_review=russell,
        russell_shadow_review=shadow_review,
        russell_live_decay_summary=live_decay,
    )
    row = review.iloc[0]

    assert bool(row["shadow_review_present"]) is True
    assert bool(row["live_decay_present"]) is True
    assert bool(row["live_decay_passed"]) is False
    assert row["next_action"] == "monitor_next_cycle"
    assert "live_decay_state_watch" in row["blocking_reason"]
    assert bool(row["replace_live_now"]) is False


def test_build_live_replacement_review_preserves_no_replacement_and_snapshot_review_states() -> None:
    leveraged = pd.DataFrame(
        [
            {
                "Candidate": "opt_tqqq_dual_drive_40_40",
                "Display Name": "Optimized TQQQ 40/40",
                "Candidate Group": "optimization_variant",
                "research_gate_passed": True,
                "replacement_candidate": False,
                "review_action": "no_replacement",
                "gate_reason": "pass",
            }
        ]
    )
    snapshot = pd.DataFrame(
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
    )

    review = build_live_replacement_review(
        leveraged_ranking=leveraged,
        snapshot_ranking=snapshot,
    )
    indexed = review.set_index("candidate")

    assert indexed.loc["opt_tqqq_dual_drive_40_40", "current_recommendation"] == "no_replacement"
    assert indexed.loc["opt_tqqq_dual_drive_40_40", "replace_live_now_reason"] == "replacement_gate_not_passed"
    assert indexed.loc["opt_tqqq_dual_drive_40_40", "blocking_reason"] == "replacement_gate_not_passed"
    assert indexed.loc["opt_r1000_core_momentum_16", "current_recommendation"] == "replacement_review_candidate"
    assert bool(indexed.loc["opt_r1000_core_momentum_16", "baseline_gate_passed"]) is True
    assert bool(indexed.loc["opt_r1000_core_momentum_16", "required_gates_passed"]) is False
    assert indexed.loc["opt_r1000_core_momentum_16", "next_action"] == "collect_walk_forward_evidence"
    assert indexed.loc["opt_r1000_core_momentum_16", "blocking_reason"] == "missing_walk_forward_summary"
    assert indexed.loc["opt_r1000_core_momentum_16", "replace_live_now_reason"] == "replacement_gate_not_passed"
    assert bool(indexed.loc["opt_r1000_core_momentum_16", "replace_live_now"]) is False


def test_live_replacement_review_cli_writes_artifacts(tmp_path) -> None:
    russell_path = tmp_path / "russell.csv"
    shadow_path = tmp_path / "shadow.csv"
    decay_path = tmp_path / "decay.csv"
    leveraged_path = tmp_path / "leveraged.csv"
    output_dir = tmp_path / "out"

    pd.DataFrame(
        [
            {
                "Run": "blend_top2_25_top4_75",
                "Candidate Role": "conservative_live_design",
                "live_gate_passed": True,
                "overfit_gate_passed": True,
                "required_gates_passed": True,
                "promotion_decision": "live_design_review_conservative",
                "recommended_action": "promote_conservative_live_design_review",
            }
        ]
    ).to_csv(russell_path, index=False)
    pd.DataFrame(
        [
            {
                "active_variant": "blend_top2_25_top4_75",
                "shadow_variant": "top4_baseline",
                "turnover_delta_vs_active": 0.1,
                "review_note": "shadow complete",
            }
        ]
    ).to_csv(shadow_path, index=False)
    pd.DataFrame(
        [
            {
                "strategy": "blend_top2_25_top4_75",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    ).to_csv(decay_path, index=False)
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
    ).to_csv(leveraged_path, index=False)

    exit_code = main(
        [
            "--russell-promotion-review",
            str(russell_path),
            "--russell-shadow-review",
            str(shadow_path),
            "--russell-live-decay",
            str(decay_path),
            "--leveraged-ranking",
            str(leveraged_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "live_replacement_review.csv").exists()
    assert (output_dir / "live_replacement_review.md").exists()
    manifest = json.loads((output_dir / "live_replacement_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "live_replacement_review"
    assert manifest["artifact_schema_version"] == "live_replacement_review.v1"
    assert manifest["row_count"] == 2


def test_build_live_replacement_review_allows_promoted_new_snapshot_strategy_into_replacement_path() -> None:
    snapshot = pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "Display Name": "Residual Strength 20",
                "Candidate Group": "new_snapshot_strategy",
                "live_gate_passed": True,
                "replacement_review_candidate": True,
                "review_action": "replacement_review_candidate",
                "gate_reason": "pass",
            }
        ]
    )

    review = build_live_replacement_review(snapshot_ranking=snapshot)
    row = review.iloc[0]

    assert row["candidate"] == "new_r1000_residual_strength_20"
    assert row["current_recommendation"] == "replacement_review_candidate"
    assert bool(row["baseline_gate_passed"]) is True
    assert bool(row["required_gates_passed"]) is False
    assert bool(row["walk_forward_gate_passed"]) is False
    assert row["next_action"] == "collect_walk_forward_evidence"
    assert row["blocking_reason"] == "missing_walk_forward_summary"


def test_build_live_replacement_review_marks_snapshot_candidate_ready_after_shadow_and_decay_pass() -> None:
    snapshot = pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "Display Name": "Residual Strength 20",
                "Candidate Group": "new_snapshot_strategy",
                "live_gate_passed": True,
                "replacement_review_candidate": True,
                "review_action": "replacement_review_candidate",
                "gate_reason": "pass",
            }
        ]
    )
    shadow = pd.DataFrame(
        [
            {
                "candidate": "new_r1000_residual_strength_20",
                "shadow_review_passed": True,
                "review_note": "recent shadow stable",
            }
        ]
    )
    decay = pd.DataFrame(
        [
            {
                "strategy": "new_r1000_residual_strength_20",
                "overall_decay_state": "keep",
                "overall_reason": "no decay gate triggered",
                "recommended_action": "continue_shadow_or_live_monitoring",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    )

    review = build_live_replacement_review(
        snapshot_ranking=snapshot,
        snapshot_walk_forward_summary=walk_forward,
        snapshot_shadow_review=shadow,
        snapshot_live_decay_summary=decay,
    )
    row = review.iloc[0]

    assert bool(row["required_gates_passed"]) is True
    assert bool(row["shadow_review_present"]) is True
    assert bool(row["live_decay_present"]) is True
    assert bool(row["replace_live_now"]) is True
    assert row["next_action"] == "ready_for_live_config_change"
    assert row["replace_live_now_reason"] == "all_review_evidence_present_and_passed"


def test_build_live_replacement_review_blocks_snapshot_candidate_on_walk_forward_failure() -> None:
    snapshot = pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "Display Name": "Residual Strength 20",
                "Candidate Group": "new_snapshot_strategy",
                "live_gate_passed": True,
                "replacement_review_candidate": True,
                "review_action": "replacement_review_candidate",
                "gate_reason": "pass",
            }
        ]
    )
    walk_forward = pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "worst_oos_excess_too_low",
            }
        ]
    )

    review = build_live_replacement_review(
        snapshot_ranking=snapshot,
        snapshot_walk_forward_summary=walk_forward,
    )
    row = review.iloc[0]

    assert bool(row["baseline_gate_passed"]) is True
    assert bool(row["walk_forward_gate_passed"]) is False
    assert bool(row["required_gates_passed"]) is False
    assert row["current_recommendation"] == "blocked_by_walk_forward_oos"
    assert row["next_action"] == "keep_current_live"
    assert "worst_oos_excess_too_low" in row["blocking_reason"]
