from __future__ import annotations

import json
from pathlib import Path

from scripts.build_monthly_global_etf_promotion_bundles import (
    build_global_etf_bundle_from_run,
    discover_global_etf_research_runs,
    main,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_global_etf_run(root: Path, *, experiment_profile: str = "dynamic_overlay_cap_v1") -> Path:
    run_dir = root / "global_etf_dynamic_overlay_cap_v1_coststress_20260624"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        run_dir / "run_manifest.json",
        {
            "research": "global_etf_offensive_rotation",
            "experiment_profile": experiment_profile,
        },
    )
    (run_dir / "ranking.csv").write_text(
        "Candidate,Display Name,Candidate Group,rank,research_gate_passed,review_action\n"
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8,Floor8,liveable_candidate,1,True,candidate_for_live_promotion_review\n"
        "liveable_oos_tail_guard_baseline82_fast18_floor12,Floor12,liveable_candidate,2,True,candidate_for_live_promotion_review\n"
        "liveable_oos_tail_guard_baseline82_fast18_floor8,Tail8,liveable_candidate,3,True,candidate_for_live_promotion_review\n",
        encoding="utf-8",
    )
    (run_dir / "live_readiness_summary.csv").write_text(
        "Candidate,live_gate_passed,live_gate_reason,live_action\n"
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8,True,pass,candidate_for_live_promotion_review\n"
        "liveable_oos_tail_guard_baseline82_fast18_floor12,True,pass,candidate_for_live_promotion_review\n"
        "liveable_oos_tail_guard_baseline82_fast18_floor8,True,pass,candidate_for_live_promotion_review\n",
        encoding="utf-8",
    )
    (run_dir / "walk_forward_selection_summary.csv").write_text(
        "Candidate Set,Selected Candidate Counts,walk_forward_gate_passed,walk_forward_gate_reason\n"
        "\"liveable_trend_drawdown_brake_baseline82_fast18_floor8,liveable_oos_tail_guard_baseline82_fast18_floor12,liveable_oos_tail_guard_baseline82_fast18_floor8\","
        "\"{\\\"liveable_trend_drawdown_brake_baseline82_fast18_floor8\\\": 4}\",False,worst_oos_excess_too_low\n",
        encoding="utf-8",
    )
    (run_dir / "walk_forward_selection_windows.csv").write_text(
        "Train Window,Test Window,Selected Candidate,Selection Action,Test Excess CAGR vs Baseline,Test Drawdown Delta vs Baseline\n"
        "2020-01-01_2024-12-31,2025,liveable_trend_drawdown_brake_baseline82_fast18_floor8,promote_candidate,-0.07624338688173316,-0.0013244095459483685\n",
        encoding="utf-8",
    )
    (run_dir / "portfolio_returns_with_benchmarks.csv").write_text(
        "as_of,liveable_trend_drawdown_brake_baseline82_fast18_floor8,liveable_oos_tail_guard_baseline82_fast18_floor12,liveable_oos_tail_guard_baseline82_fast18_floor8,live_global_etf_rotation_defensive_baseline,QQQ,SPY\n"
        "2025-01-31,0.01,0.009,0.008,0.009,0.02,0.015\n"
        "2025-02-28,-0.03,-0.02,-0.025,-0.01,-0.04,-0.03\n",
        encoding="utf-8",
    )
    (run_dir / "rebalance_events.csv").write_text(
        "candidate_id,as_of,next_date,signal_description,overlay_weight,base_candidate_id,overlay_candidate_id\n"
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8,2024-12-31,2025-01-31,rebalance,0.18,live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly\n",
        encoding="utf-8",
    )
    return run_dir


def test_discover_global_etf_research_runs_finds_pre_registered_runs(tmp_path: Path) -> None:
    run_dir = _write_global_etf_run(tmp_path)

    discovered = discover_global_etf_research_runs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0]["artifact_dir"] == run_dir
    assert discovered[0]["experiment_profile"] == "dynamic_overlay_cap_v1"


def test_build_global_etf_bundle_from_run_writes_bundle(tmp_path: Path) -> None:
    run_dir = _write_global_etf_run(tmp_path)
    run = discover_global_etf_research_runs(tmp_path)[0]

    output_dir = build_global_etf_bundle_from_run(run, output_root=run_dir)

    manifest = json.loads((output_dir / "promotion_bundle_manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((output_dir / "promotion_bundle_summary.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "global_etf_promotion_bundle"
    assert manifest["experiment_profile"] == "dynamic_overlay_cap_v1"
    assert summary["candidate_ids"] == [
        "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
        "liveable_oos_tail_guard_baseline82_fast18_floor12",
        "liveable_oos_tail_guard_baseline82_fast18_floor8",
    ]


def test_main_builds_global_etf_bundles(tmp_path: Path, monkeypatch, capsys) -> None:
    _write_global_etf_run(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_global_etf_promotion_bundles.py",
            "--artifact-root",
            str(tmp_path),
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert "global_etf_promotion_bundle_count=1" in captured.out
