from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_overfit_diagnostics import (
    build_overfit_diagnostics,
    build_overfit_promotion_gate_summary,
    main,
)


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "CAGR": 0.40,
                "Sharpe": 1.20,
                "Max Drawdown": -0.31,
                "Turnover/Year": 3.5,
            },
            {
                "Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50",
                "Variant Type": "panic_rebound_guard_fixed_blend",
                "CAGR": 0.44,
                "Sharpe": 1.28,
                "Max Drawdown": -0.31,
                "Turnover/Year": 3.8,
            },
            {
                "Run": "sector_cap1_blend_top2_50_top4_50",
                "Variant Type": "sector_capped_fixed_blend",
                "CAGR": 0.28,
                "Sharpe": 0.95,
                "Max Drawdown": -0.34,
                "Turnover/Year": 4.2,
            },
        ]
    )


def _rolling() -> pd.DataFrame:
    rows = []
    windows = [(3, 2020, 2022), (3, 2021, 2023), (3, 2022, 2024), (5, 2019, 2023)]
    values = {
        "blend_top2_50_top4_50": [0.32, 0.45, 0.38, 0.41],
        "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50": [0.20, 0.48, 0.38, 0.41],
        "sector_cap1_blend_top2_50_top4_50": [0.12, 0.22, 0.18, 0.24],
    }
    qqq = [0.25, 0.35, 0.30, 0.33]
    spy = [0.15, 0.22, 0.20, 0.21]
    for run, cagrs in values.items():
        for (years, start, end), cagr, qqq_cagr, spy_cagr in zip(windows, cagrs, qqq, spy, strict=True):
            rows.append(
                {
                    "Run": run,
                    "Variant Type": "fixed_blend" if run.startswith("blend") else "research",
                    "Window Years": years,
                    "Window Start Year": start,
                    "Window End Year": end,
                    "Strategy CAGR": cagr,
                    "Strategy Max Drawdown": -0.25,
                    "QQQ CAGR": qqq_cagr,
                    "SPY CAGR": spy_cagr,
                }
            )
    return pd.DataFrame(rows)


def _walk_forward_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Candidate Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50",
                "OOS Baseline CAGR Win Rate": 0.3333,
                "Median OOS Excess CAGR vs Baseline": 0.0,
                "Worst OOS Excess CAGR vs Baseline": 0.0,
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "oos_win_rate_below_threshold;median_oos_excess_not_positive",
            }
        ]
    )


def test_build_overfit_diagnostics_flags_full_sample_winner_with_weak_oos() -> None:
    result = build_overfit_diagnostics(_summary(), _rolling(), walk_forward_summary=_walk_forward_summary())
    diagnostics = result["overfit_candidate_diagnostics"]
    rank_windows = result["overfit_rank_windows"]

    assert not diagnostics.empty
    assert not rank_windows.empty
    assert diagnostics.iloc[0]["overfit_risk_label"] == "high"
    panic = diagnostics.set_index("Run").loc["panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50"]
    assert bool(panic["Full Sample Top Quantile"]) is True
    assert panic["Walk Forward Gate Passed"] is False
    assert panic["overfit_risk_label"] == "high"
    assert panic["recommended_action"] == "keep_research_only_oos_failed"

    fixed = diagnostics.set_index("Run").loc["blend_top2_50_top4_50"]
    assert fixed["Candidate Family"] == "fixed_blend_live_candidate"
    assert fixed["Positive QQQ Excess Rate"] == 1.0

    gate = result["overfit_promotion_gate_summary"].set_index("Run")
    assert bool(gate.loc["panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "overfit_gate_passed"]) is False
    panic_reason = gate.loc["panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "live_promotion_gate_reason"]
    assert "overfit_high_risk" in panic_reason
    assert "walk_forward_gate_failed" in panic_reason
    assert "not_promotable_candidate_family" in panic_reason
    assert bool(gate.loc["blend_top2_50_top4_50", "live_promotion_gate_passed"]) is True
    assert gate.loc["blend_top2_50_top4_50", "gate_scope"] == "blocker_only_not_positive_evidence"


def test_build_overfit_promotion_gate_summary_keeps_research_families_non_promotable() -> None:
    result = build_overfit_diagnostics(_summary(), _rolling())
    gate = build_overfit_promotion_gate_summary(result["overfit_candidate_diagnostics"]).set_index("Run")

    assert bool(gate.loc["sector_cap1_blend_top2_50_top4_50", "overfit_gate_passed"]) is False
    assert bool(gate.loc["sector_cap1_blend_top2_50_top4_50", "live_promotion_gate_passed"]) is False
    assert "not_promotable_candidate_family" in gate.loc[
        "sector_cap1_blend_top2_50_top4_50",
        "live_promotion_gate_reason",
    ]


def test_overfit_diagnostics_cli_writes_outputs(tmp_path) -> None:
    summary_path = tmp_path / "summary.csv"
    rolling_path = tmp_path / "rolling.csv"
    wf_path = tmp_path / "walk_forward.csv"
    output_dir = tmp_path / "out"
    _summary().to_csv(summary_path, index=False)
    _rolling().to_csv(rolling_path, index=False)
    _walk_forward_summary().to_csv(wf_path, index=False)

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--rolling",
            str(rolling_path),
            "--walk-forward-summary",
            str(wf_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "overfit_candidate_diagnostics.csv").exists()
    assert (output_dir / "overfit_rank_windows.csv").exists()
    assert (output_dir / "overfit_promotion_gate_summary.csv").exists()
