from __future__ import annotations

import numpy as np
import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_dsr_pbo_diagnostics import (
    build_dsr_pbo_diagnostics,
    main,
)


def _daily_returns() -> pd.DataFrame:
    rows = []
    for idx, as_of in enumerate(pd.bdate_range("2024-01-02", periods=160)):
        qqq = 0.0002 if idx % 7 else -0.0003
        spy = qqq - 0.0001
        wave = 0.0002 * np.sin(idx / 6.0)
        rows.extend(
            [
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "blend_top2_50_top4_50",
                    "Strategy Return": qqq + 0.0010 + wave,
                    "QQQ Return": qqq,
                    "SPY Return": spy,
                },
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "blend_top2_25_top4_75",
                    "Strategy Return": qqq + 0.00045 - wave,
                    "QQQ Return": qqq,
                    "SPY Return": spy,
                },
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "base_top4_cap25",
                    "Strategy Return": qqq + 0.00015 + 0.00005 * np.cos(idx / 5.0),
                    "QQQ Return": qqq,
                    "SPY Return": spy,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_build_dsr_pbo_diagnostics_reports_deflated_sharpe_and_cscv_context() -> None:
    result = build_dsr_pbo_diagnostics(
        _daily_returns(),
        benchmark_column="QQQ Return",
        candidate_runs=("base_top4_cap25", "blend_top2_25_top4_75", "blend_top2_50_top4_50"),
        cscv_groups=8,
        effective_trials=3,
        alpha=0.10,
    )

    candidate = result["dsr_pbo_candidate_summary"].set_index("Run")
    splits = result["dsr_pbo_cscv_splits"]
    global_summary = result["dsr_pbo_global_summary"].iloc[0]

    assert not splits.empty
    assert global_summary["Best Run"] == "blend_top2_50_top4_50"
    assert global_summary["CSCV Split Count"] == 70
    assert 0.0 <= global_summary["CSCV PBO Loss Rate"] <= 1.0
    assert bool(candidate.loc["blend_top2_50_top4_50", "Observed Best Candidate"]) is True
    assert candidate.loc["blend_top2_50_top4_50", "Annualized Sharpe"] > candidate.loc[
        "base_top4_cap25",
        "Annualized Sharpe",
    ]
    assert 0.0 <= candidate.loc["blend_top2_50_top4_50", "Deflated Sharpe Probability"] <= 1.0
    assert candidate.loc["blend_top2_50_top4_50", "CSCV Train Winner Count"] > 0
    assert candidate.loc["blend_top2_50_top4_50", "diagnostic_scope"] == "deflated_sharpe_and_cscv_pbo_style_not_live_gate"


def test_dsr_pbo_cli_writes_outputs(tmp_path) -> None:
    daily_path = tmp_path / "daily.csv"
    output_dir = tmp_path / "out"
    _daily_returns().to_csv(daily_path, index=False)

    exit_code = main(
        [
            "--daily-returns",
            str(daily_path),
            "--benchmark-column",
            "QQQ Return",
            "--candidate-runs",
            "base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50",
            "--cscv-groups",
            "8",
            "--effective-trials",
            "3",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "dsr_pbo_candidate_summary.csv").exists()
    assert (output_dir / "dsr_pbo_cscv_splits.csv").exists()
    assert (output_dir / "dsr_pbo_global_summary.csv").exists()
