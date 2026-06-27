from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_promotion_readiness_report import build_readiness_summary, main


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_promotion_readiness_report_aggregates_strategy_and_plugin_rows(tmp_path: Path) -> None:
    live_dir = tmp_path / "live_replacement_global_etf"
    live_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "strategy_line": "global_etf_rotation",
                "candidate": "liveable_blend_baseline90_fast10",
                "required_gates_passed": False,
                "shadow_review_present": False,
                "shadow_review_passed": False,
                "live_decay_present": False,
                "live_decay_passed": False,
                "replace_live_now": False,
                "blocking_reason": "worst_oos_excess_too_low",
                "current_recommendation": "blocked_by_walk_forward_oos",
                "next_action": "continue_research",
            }
        ]
    ).to_csv(live_dir / "live_replacement_review.csv", index=False)
    _write_json(
        live_dir / "live_replacement_manifest.json",
        {
            "manifest_type": "live_replacement_review",
            "artifact_schema_version": "live_replacement_review.v1",
            "row_count": 1,
            "replace_live_now_count": 0,
        },
    )

    plugin_dir = tmp_path / "plugin_review_ibit"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "strategy": "ibit_smart_dca",
                "plugin": "ibit_zscore_exit",
                "plugin_role": "notification_only",
                "required_gates_passed": True,
                "shadow_review_present": False,
                "shadow_review_passed": False,
                "live_decay_present": False,
                "live_decay_passed": False,
                "replace_live_component_now": False,
                "blocking_reason": "policy_still_notification_only",
                "recommended_action": "prepare_separate_promotion_artifact",
            }
        ]
    ).to_csv(plugin_dir / "plugin_promotion_review.csv", index=False)
    _write_json(
        plugin_dir / "plugin_promotion_review_manifest.json",
        {
            "manifest_type": "strategy_plugin_promotion_review",
            "artifact_schema_version": "strategy_plugin_promotion_review.v1",
            "strategy": "ibit_smart_dca",
            "plugin": "ibit_zscore_exit",
            "row_count": 1,
            "replace_live_component_now_count": 0,
        },
    )

    output_dir = tmp_path / "readiness"
    exit_code = main(
        [
            "--artifact-root",
            str(tmp_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    summary = pd.read_csv(output_dir / "promotion_readiness_summary.csv")
    blockers = pd.read_csv(output_dir / "promotion_blocker_counts.csv")
    markdown = (output_dir / "promotion_readiness.md").read_text(encoding="utf-8")

    assert len(summary) == 2
    assert set(summary["scope_type"]) == {"strategy", "plugin"}
    assert set(blockers["blocking_reason"]) >= {"worst_oos_excess_too_low", "policy_still_notification_only"}
    assert "Promotion readiness summary" in markdown
    assert "policy_still_notification_only" in markdown


def test_build_readiness_summary_latest_only_keeps_latest_strategy_and_plugin_rows(tmp_path: Path) -> None:
    old_live_dir = tmp_path / "live_replacement_global_etf_20260620"
    new_live_dir = tmp_path / "live_replacement_global_etf_20260624"
    for directory, blocker in (
        (old_live_dir, "worst_oos_excess_too_low"),
        (new_live_dir, "not_selected_in_walk_forward"),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "strategy_line": "global_etf_rotation",
                    "candidate": "liveable_blend_baseline90_fast10",
                    "required_gates_passed": False,
                    "shadow_review_present": False,
                    "shadow_review_passed": False,
                    "live_decay_present": False,
                    "live_decay_passed": False,
                    "replace_live_now": False,
                    "blocking_reason": blocker,
                    "current_recommendation": "blocked",
                    "next_action": "continue_research",
                }
            ]
        ).to_csv(directory / "live_replacement_review.csv", index=False)
        _write_json(
            directory / "live_replacement_manifest.json",
            {
                "manifest_type": "live_replacement_review",
                "artifact_schema_version": "live_replacement_review.v1",
                "row_count": 1,
                "replace_live_now_count": 0,
            },
        )

    old_plugin_dir = tmp_path / "plugin_review_ibit_20260620"
    new_plugin_dir = tmp_path / "plugin_review_ibit_20260624"
    for directory, blocker in (
        (old_plugin_dir, "policy_still_notification_only"),
        (new_plugin_dir, "plugin_gate_failed"),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "strategy": "ibit_smart_dca",
                    "plugin": "ibit_zscore_exit",
                    "plugin_role": "notification_only",
                    "required_gates_passed": False,
                    "shadow_review_present": False,
                    "shadow_review_passed": False,
                    "live_decay_present": False,
                    "live_decay_passed": False,
                    "replace_live_component_now": False,
                    "blocking_reason": blocker,
                    "recommended_action": "continue_research",
                }
            ]
        ).to_csv(directory / "plugin_promotion_review.csv", index=False)
        _write_json(
            directory / "plugin_promotion_review_manifest.json",
            {
                "manifest_type": "strategy_plugin_promotion_review",
                "artifact_schema_version": "strategy_plugin_promotion_review.v1",
                "strategy": "ibit_smart_dca",
                "plugin": "ibit_zscore_exit",
                "row_count": 1,
                "replace_live_component_now_count": 0,
            },
        )

    all_rows = build_readiness_summary(tmp_path)
    latest_rows = build_readiness_summary(tmp_path, latest_only=True)

    assert len(all_rows) == 4
    assert len(latest_rows) == 2
    strategy_row = latest_rows.loc[latest_rows["scope_type"].eq("strategy")].iloc[0]
    plugin_row = latest_rows.loc[latest_rows["scope_type"].eq("plugin")].iloc[0]
    assert strategy_row["blocking_reason"] == "not_selected_in_walk_forward"
    assert plugin_row["blocking_reason"] == "plugin_gate_failed"


def test_promotion_readiness_report_latest_only_flag_reduces_duplicate_rows(tmp_path: Path) -> None:
    for suffix in ("20260620", "20260624"):
        live_dir = tmp_path / f"live_replacement_global_etf_{suffix}"
        live_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "strategy_line": "global_etf_rotation",
                    "candidate": "liveable_blend_baseline90_fast10",
                    "required_gates_passed": False,
                    "shadow_review_present": False,
                    "shadow_review_passed": False,
                    "live_decay_present": False,
                    "live_decay_passed": False,
                    "replace_live_now": False,
                    "blocking_reason": f"blocker_{suffix}",
                    "current_recommendation": "blocked",
                    "next_action": "continue_research",
                }
            ]
        ).to_csv(live_dir / "live_replacement_review.csv", index=False)
        _write_json(
            live_dir / "live_replacement_manifest.json",
            {
                "manifest_type": "live_replacement_review",
                "artifact_schema_version": "live_replacement_review.v1",
                "row_count": 1,
                "replace_live_now_count": 0,
            },
        )

    output_dir = tmp_path / "readiness_latest_only"
    exit_code = main(
        [
            "--artifact-root",
            str(tmp_path),
            "--output-dir",
            str(output_dir),
            "--latest-only",
        ]
    )

    assert exit_code == 0
    summary = pd.read_csv(output_dir / "promotion_readiness_summary.csv")
    markdown = (output_dir / "promotion_readiness.md").read_text(encoding="utf-8")
    assert len(summary) == 1
    assert summary.iloc[0]["blocking_reason"] == "blocker_20260624"
    assert "View mode: `latest_only`" in markdown


def test_build_readiness_summary_supports_scope_family_item_filters(tmp_path: Path) -> None:
    live_dir = tmp_path / "live_replacement_snapshot_20260624"
    live_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "strategy_line": "snapshot_us_equity",
                "candidate": "new_r1000_residual_strength_24_persistence_guard",
                "required_gates_passed": True,
                "shadow_review_present": True,
                "shadow_review_passed": True,
                "live_decay_present": True,
                "live_decay_passed": False,
                "replace_live_now": False,
                "blocking_reason": "live_decay_state_watch",
                "current_recommendation": "replacement_review_candidate",
                "next_action": "monitor_next_cycle",
            },
            {
                "strategy_line": "snapshot_us_equity",
                "candidate": "new_r1000_residual_strength_24_balance_guard",
                "required_gates_passed": True,
                "shadow_review_present": False,
                "shadow_review_passed": False,
                "live_decay_present": False,
                "live_decay_passed": False,
                "replace_live_now": False,
                "blocking_reason": "missing_shadow_review_artifact",
                "current_recommendation": "replacement_review_candidate",
                "next_action": "collect_shadow_review_evidence",
            },
        ]
    ).to_csv(live_dir / "live_replacement_review.csv", index=False)
    _write_json(
        live_dir / "live_replacement_manifest.json",
        {
            "manifest_type": "live_replacement_review",
            "artifact_schema_version": "live_replacement_review.v1",
            "row_count": 2,
            "replace_live_now_count": 0,
        },
    )

    filtered = build_readiness_summary(
        tmp_path,
        latest_only=True,
        scope_type="strategy",
        family="snapshot_us_equity",
        item_name="new_r1000_residual_strength_24_persistence_guard",
    )

    assert len(filtered) == 1
    row = filtered.iloc[0]
    assert row["family"] == "snapshot_us_equity"
    assert row["item_name"] == "new_r1000_residual_strength_24_persistence_guard"
    assert row["blocking_reason"] == "live_decay_state_watch"
