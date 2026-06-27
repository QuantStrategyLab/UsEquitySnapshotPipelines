from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.snapshot_promotion_bundle import build_snapshot_promotion_bundle, main


def _write_inputs(root: Path) -> Path:
    artifact_dir = root / "us_equity_strategy_candidates_20260624_snapshot_evidence_v4"
    review_dir = artifact_dir / "live_replacement_bundle" / f"live_replacement_review_{artifact_dir.name}"
    shadow_dir = root / "snapshot_shadow_review_20260624"
    decay_dir = root / "live_decay_monitor_snapshot_20260624"
    review_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    decay_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "Candidate": "new_r1000_residual_strength_20",
                "Display Name": "Residual Strength 20",
                "Candidate Group": "new_snapshot_strategy",
                "replacement_review_candidate": True,
            }
        ]
    ).to_csv(artifact_dir / "ranking.csv", index=False)
    pd.DataFrame(
        [
            {
                "candidate": "new_r1000_residual_strength_20",
                "required_gates_passed": True,
                "replace_live_now": True,
                "current_recommendation": "replacement_review_candidate",
                "next_action": "ready_for_live_config_change",
            }
        ]
    ).to_csv(review_dir / "live_replacement_review.csv", index=False)
    pd.DataFrame(
        [{"candidate": "new_r1000_residual_strength_20", "shadow_review_passed": True}]
    ).to_csv(shadow_dir / "snapshot_us_equity_shadow_review_rows.csv", index=False)
    pd.DataFrame(
        [{"strategy": "new_r1000_residual_strength_20", "overall_decay_state": "keep"}]
    ).to_csv(decay_dir / "live_decay_strategy_summary.csv", index=False)
    return artifact_dir


def test_build_snapshot_promotion_bundle_writes_manifest_and_summary(tmp_path: Path) -> None:
    artifact_dir = _write_inputs(tmp_path)

    result = build_snapshot_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=tmp_path / "bundle",
        candidate_ids=["new_r1000_residual_strength_20"],
    )

    manifest = json.loads((tmp_path / "bundle" / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "snapshot_us_equity_promotion_bundle"
    assert manifest["bundle_decision"] == "replace_live_now"
    assert manifest["pending_promotion_candidate_count"] == 0
    assert result["summary"]["bundle_decision"] == "replace_live_now"
    assert result["summary"]["replace_live_now_count"] == 1
    assert result["summary"]["pending_promotion_candidate_count"] == 0
    assert (tmp_path / "bundle" / "live_replacement_review.csv").exists()
    assert (tmp_path / "bundle" / "pending_promotion_summary.md").exists()
    bundle_status = (tmp_path / "bundle" / "bundle_status.md").read_text(encoding="utf-8")
    assert "Bundle decision: `replace_live_now`" in bundle_status


def test_snapshot_promotion_bundle_cli(tmp_path: Path, capsys) -> None:
    artifact_dir = _write_inputs(tmp_path)
    exit_code = main([
        "--artifact-dir", str(artifact_dir),
        "--output-dir", str(tmp_path / "bundle"),
        "--candidate-ids", "new_r1000_residual_strength_20",
    ])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "snapshot_promotion_bundle_candidates=1" in out
    assert "snapshot_promotion_bundle_pending_promotion_count=0" in out


def test_build_snapshot_promotion_bundle_prefers_matching_shadow_and_decay_artifacts(tmp_path: Path) -> None:
    artifact_dir = _write_inputs(tmp_path)
    unrelated_shadow_dir = tmp_path / "snapshot_shadow_review_older_other"
    unrelated_decay_dir = tmp_path / "live_decay_monitor_snapshot_older_other"
    unrelated_shadow_dir.mkdir(parents=True, exist_ok=True)
    unrelated_decay_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"candidate": "other_candidate", "shadow_review_passed": False}]
    ).to_csv(unrelated_shadow_dir / "snapshot_us_equity_shadow_review_rows.csv", index=False)
    pd.DataFrame(
        [{"strategy": "other_candidate", "overall_decay_state": "review"}]
    ).to_csv(unrelated_decay_dir / "live_decay_strategy_summary.csv", index=False)

    build_snapshot_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=tmp_path / "bundle",
        candidate_ids=["new_r1000_residual_strength_20"],
    )

    shadow_selected = pd.read_csv(tmp_path / "bundle" / "shadow_review_selected.csv")
    decay_selected = pd.read_csv(tmp_path / "bundle" / "live_decay_selected.csv")

    assert shadow_selected["candidate"].tolist() == ["new_r1000_residual_strength_20"]
    assert decay_selected["strategy"].tolist() == ["new_r1000_residual_strength_20"]


def test_build_snapshot_promotion_bundle_tracks_pending_promotion_candidates(tmp_path: Path) -> None:
    artifact_dir = _write_inputs(tmp_path)
    review_dir = artifact_dir / "live_replacement_bundle" / f"live_replacement_review_{artifact_dir.name}"
    pd.DataFrame(
        [
            {
                "candidate": "new_r1000_residual_strength_20",
                "required_gates_passed": True,
                "shadow_review_passed": True,
                "live_decay_present": True,
                "live_decay_passed": False,
                "replace_live_now": False,
                "current_recommendation": "replacement_review_candidate",
                "next_action": "monitor_next_cycle",
                "blocking_reason": "live_decay_state_watch",
            }
        ]
    ).to_csv(review_dir / "live_replacement_review.csv", index=False)

    result = build_snapshot_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=tmp_path / "bundle",
        candidate_ids=["new_r1000_residual_strength_20"],
    )

    summary = result["summary"]
    assert summary["bundle_decision"] == "pending_promotion"
    assert summary["replace_live_now_count"] == 0
    assert summary["pending_promotion_candidate_count"] == 1
    assert summary["pending_promotion_candidates"][0]["candidate"] == "new_r1000_residual_strength_20"
    assert summary["pending_promotion_candidates"][0]["blocking_reason"] == "live_decay_state_watch"
    manifest = json.loads((tmp_path / "bundle" / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_decision"] == "pending_promotion"
    assert manifest["pending_promotion_candidate_count"] == 1
    pending_markdown = (tmp_path / "bundle" / "pending_promotion_summary.md").read_text(encoding="utf-8")
    assert "Pending promotion candidates" in pending_markdown
    assert "Bundle decision: `pending_promotion`" in pending_markdown
    assert "new_r1000_residual_strength_20" in pending_markdown
    assert "live_decay_state_watch" in pending_markdown
    bundle_status = (tmp_path / "bundle" / "bundle_status.md").read_text(encoding="utf-8")
    assert "Bundle decision: `pending_promotion`" in bundle_status
    assert "Blocking reason: `live_decay_state_watch`" in bundle_status
