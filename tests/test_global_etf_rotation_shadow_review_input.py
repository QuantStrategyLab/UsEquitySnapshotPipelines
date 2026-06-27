from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.global_etf_rotation_shadow_review import build_shadow_review_artifacts
from us_equity_snapshot_pipelines.global_etf_rotation_shadow_review_input import (
    build_shadow_review_input_artifacts,
    build_shadow_review_input_payload,
    main,
)
from us_equity_snapshot_pipelines.live_replacement_review import build_live_replacement_review


def _ranking() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Candidate": "live_global_etf_rotation_defensive_baseline",
                "Display Name": "Baseline",
                "Candidate Group": "current_live_baseline",
                "median_turnover_per_year": 4.0,
                "research_gate_passed": True,
                "review_action": "keep_current_live",
            },
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "Display Name": "Blend 90/10",
                "Candidate Group": "liveable_candidate",
                "median_turnover_per_year": 4.3,
                "research_gate_passed": True,
                "review_action": "live_design_review",
            },
            {
                "Candidate": "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
                "Display Name": "Brake 85/15 Floor 0",
                "Candidate Group": "liveable_candidate",
                "median_turnover_per_year": 4.6,
                "research_gate_passed": True,
                "review_action": "live_design_review",
            },
        ]
    )


def _live_readiness() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Candidate": "liveable_blend_baseline90_fast10",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            },
            {
                "Candidate": "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
                "live_gate_passed": True,
                "live_gate_reason": "pass",
                "live_action": "candidate_for_live_promotion_review",
            },
        ]
    )


def _walk_forward() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Selected Candidate Counts": json.dumps(
                    {
                        "liveable_blend_baseline90_fast10": 4,
                        "liveable_trend_drawdown_brake_baseline85_fast15_floor0": 2,
                    },
                    sort_keys=True,
                ),
                "walk_forward_gate_passed": True,
                "walk_forward_gate_reason": "pass",
            }
        ]
    )


def _rebalance_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "candidate_id": "liveable_blend_baseline90_fast10",
                "as_of": "2026-06-27",
                "next_date": "2026-06-30",
                "overlay_weight": 0.10,
            },
            {
                "candidate_id": "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
                "as_of": "2026-06-27",
                "next_date": "2026-06-30",
                "overlay_weight": 0.0,
            },
        ]
    )


def test_build_shadow_review_input_payload_uses_research_outputs_and_approval_lists() -> None:
    payload = build_shadow_review_input_payload(
        ranking=_ranking(),
        live_readiness=_live_readiness(),
        walk_forward_summary=_walk_forward(),
        rebalance_events=_rebalance_events(),
        approved_candidates=("liveable_blend_baseline90_fast10",),
        blocked_candidates=("liveable_trend_drawdown_brake_baseline85_fast15_floor0",),
        snapshot_as_of="2026-06-30",
    )

    diagnostics = payload["diagnostics"]
    rows = diagnostics["global_etf_shadow_review_rows"]
    assert payload["snapshot_as_of"] == "2026-06-30"
    assert len(rows) == 2
    first = {row["candidate"]: row for row in rows}
    assert bool(first["liveable_blend_baseline90_fast10"]["shadow_review_passed"]) is True
    assert first["liveable_blend_baseline90_fast10"]["selected_count"] == 2
    assert float(first["liveable_blend_baseline90_fast10"]["offensive_weight"]) == 0.10
    assert bool(first["liveable_trend_drawdown_brake_baseline85_fast15_floor0"]["shadow_review_passed"]) is False
    assert first["liveable_trend_drawdown_brake_baseline85_fast15_floor0"]["selected_count"] == 0


def test_build_shadow_review_input_payload_rejects_overlapping_decisions() -> None:
    try:
        build_shadow_review_input_payload(
            ranking=_ranking(),
            live_readiness=_live_readiness(),
            walk_forward_summary=_walk_forward(),
            approved_candidates=("liveable_blend_baseline90_fast10",),
            blocked_candidates=("liveable_blend_baseline90_fast10",),
        )
    except ValueError as exc:
        assert "both approved and blocked" in str(exc)
    else:
        raise AssertionError("expected overlapping decision rejection")


def test_build_shadow_review_input_artifacts_write_json_and_markdown_and_chain_into_replacement_review(
    tmp_path: Path,
) -> None:
    ranking_path = tmp_path / "ranking.csv"
    live_path = tmp_path / "live_readiness_summary.csv"
    walk_path = tmp_path / "walk_forward_selection_summary.csv"
    rebalance_path = tmp_path / "rebalance_events.csv"
    _ranking().to_csv(ranking_path, index=False)
    _live_readiness().to_csv(live_path, index=False)
    _walk_forward().to_csv(walk_path, index=False)
    _rebalance_events().to_csv(rebalance_path, index=False)

    input_outputs = build_shadow_review_input_artifacts(
        ranking_path=ranking_path,
        live_readiness_path=live_path,
        walk_forward_path=walk_path,
        rebalance_events_path=rebalance_path,
        approved_candidates=("liveable_blend_baseline90_fast10",),
        blocked_candidates=("liveable_trend_drawdown_brake_baseline85_fast15_floor0",),
        output_dir=tmp_path / "input",
        snapshot_as_of="2026-06-30",
    )
    shadow_outputs = build_shadow_review_artifacts(
        input_outputs.json_path,
        output_dir=tmp_path / "shadow",
        profile="global_etf_rotation",
        snapshot_as_of="2026-06-30",
    )
    decay = pd.DataFrame(
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
        global_etf_ranking=_ranking().loc[lambda df: df["Candidate"].eq("liveable_blend_baseline90_fast10")],
        global_etf_live_readiness=_live_readiness().loc[lambda df: df["Candidate"].eq("liveable_blend_baseline90_fast10")],
        global_etf_walk_forward_summary=_walk_forward(),
        global_etf_shadow_review=pd.read_csv(shadow_outputs.csv_path),
        global_etf_live_decay_summary=decay,
    )
    row = review.iloc[0]

    assert input_outputs.json_path.exists()
    assert input_outputs.markdown_path.exists()
    assert shadow_outputs.csv_path.exists()
    assert bool(row["replace_live_now"]) is True
    assert row["next_action"] == "ready_for_live_config_change"


def test_shadow_review_input_cli_writes_outputs(tmp_path: Path) -> None:
    ranking_path = tmp_path / "ranking.csv"
    live_path = tmp_path / "live_readiness_summary.csv"
    walk_path = tmp_path / "walk_forward_selection_summary.csv"
    rebalance_path = tmp_path / "rebalance_events.csv"
    _ranking().to_csv(ranking_path, index=False)
    _live_readiness().to_csv(live_path, index=False)
    _walk_forward().to_csv(walk_path, index=False)
    _rebalance_events().to_csv(rebalance_path, index=False)
    output_dir = tmp_path / "out"

    exit_code = main(
        [
            "--ranking",
            str(ranking_path),
            "--live-readiness",
            str(live_path),
            "--walk-forward",
            str(walk_path),
            "--rebalance-events",
            str(rebalance_path),
            "--approved-candidates",
            "liveable_blend_baseline90_fast10",
            "--blocked-candidates",
            "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
            "--snapshot-as-of",
            "2026-06-30",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "global_etf_shadow_review_input.json").exists()
    assert (output_dir / "global_etf_shadow_review_input.md").exists()
