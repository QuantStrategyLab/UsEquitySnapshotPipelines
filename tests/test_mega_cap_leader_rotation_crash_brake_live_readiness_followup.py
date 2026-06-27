from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_live_readiness_followup import (
    evaluate_crash_brake_live_readiness,
    main,
)


def _summary(*, candidate_cagr: float = 0.17, candidate_drawdown: float = -0.18) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50_no_brake",
                "Universe Lag Trading Days": 21,
                "Start": "2018-01-02",
                "End": "2026-04-01",
                "CAGR": 0.18,
                "Max Drawdown": -0.20,
                "Sharpe": 0.95,
                "Turnover/Year": 3.0,
                "Panic Brake Mode Share": 0.0,
            },
            {
                "Run": "crash_brake_top2_50_floor25",
                "Universe Lag Trading Days": 21,
                "Start": "2018-01-02",
                "End": "2026-04-01",
                "CAGR": candidate_cagr,
                "Max Drawdown": candidate_drawdown,
                "Sharpe": 0.92,
                "Turnover/Year": 3.1,
                "Panic Brake Mode Share": 0.16,
            },
        ]
    )


def _rolling(*, candidate_worst_drawdown: float = -0.19) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50_no_brake",
                "Window Years": 3,
                "Strategy Max Drawdown": -0.20,
            },
            {
                "Run": "blend_top2_50_top4_50_no_brake",
                "Window Years": 5,
                "Strategy Max Drawdown": -0.20,
            },
            {
                "Run": "crash_brake_top2_50_floor25",
                "Window Years": 3,
                "Strategy Max Drawdown": candidate_worst_drawdown,
            },
            {
                "Run": "crash_brake_top2_50_floor25",
                "Window Years": 5,
                "Strategy Max Drawdown": candidate_worst_drawdown,
            },
        ]
    )


def _mode_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Mode": ["baseline", "floor", "floor", "baseline", "baseline"],
        }
    )


def test_evaluate_crash_brake_live_readiness_passes_on_risk_benefit() -> None:
    result = evaluate_crash_brake_live_readiness(
        _summary(candidate_cagr=0.175, candidate_drawdown=-0.185),
        _rolling(candidate_worst_drawdown=-0.185),
        mode_history=_mode_history(),
    ).iloc[0]

    assert bool(result["live_gate_passed"]) is True
    assert "risk_benefit_vs_reference" in str(result["live_gate_reason"])
    assert result["recommended_action"] == "crash_brake_live_gate_passed_continue_gate_collection"


def test_evaluate_crash_brake_live_readiness_passes_on_acceptable_cagr_tradeoff() -> None:
    result = evaluate_crash_brake_live_readiness(
        _summary(candidate_cagr=0.175, candidate_drawdown=-0.20),
        _rolling(candidate_worst_drawdown=-0.20),
    ).iloc[0]

    assert bool(result["live_gate_passed"]) is True
    assert "acceptable_cagr_tradeoff_vs_reference" in str(result["live_gate_reason"])


def test_evaluate_crash_brake_live_readiness_fails_when_cagr_shortfall_and_no_risk_benefit() -> None:
    result = evaluate_crash_brake_live_readiness(
        _summary(candidate_cagr=0.14, candidate_drawdown=-0.21),
        _rolling(candidate_worst_drawdown=-0.21),
    ).iloc[0]

    assert bool(result["live_gate_passed"]) is False
    assert "cagr_shortfall_vs_reference_above_3.00%" in str(result["live_gate_reason"])
    assert "drawdown_improvement_below_0.50%" in str(result["live_gate_reason"])


def test_crash_brake_live_readiness_followup_cli_writes_manifest(tmp_path: Path) -> None:
    summary_path = tmp_path / "crash_brake_summary.csv"
    rolling_path = tmp_path / "crash_brake_rolling_summary.csv"
    mode_path = tmp_path / "crash_brake_mode_history.csv"
    manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "live_readiness"
    _summary().to_csv(summary_path, index=False)
    _rolling().to_csv(rolling_path, index=False)
    _mode_history().to_csv(mode_path, index=False)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "russell_top50_crash_brake_research",
                "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
                "candidate_runs": ["crash_brake_top2_50_floor25"],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--rolling",
            str(rolling_path),
            "--mode-history",
            str(mode_path),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    summary = pd.read_csv(output_dir / "crash_brake_live_readiness_summary.csv")
    manifest = json.loads((output_dir / "crash_brake_live_readiness_followup_manifest.json").read_text(encoding="utf-8"))
    assert summary["Run"].tolist() == ["crash_brake_top2_50_floor25"]
    assert manifest["manifest_type"] == "russell_top50_crash_brake_live_readiness_followup"
    assert manifest["reference_run"] == "blend_top2_50_top4_50_no_brake"
