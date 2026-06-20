from __future__ import annotations

import numpy as np
import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_mcs_diagnostics import (
    build_mcs_style_diagnostics,
    main,
)


def _daily_returns() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=120)
    rows = []
    for idx, as_of in enumerate(dates):
        noise = 0.0001 * np.sin(idx / 4.0)
        rows.extend(
            [
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "candidate_best",
                    "Variant Type": "fixed_blend",
                    "Strategy Return": 0.0015 + noise,
                    "QQQ Return": 0.0002,
                    "SPY Return": 0.0001,
                },
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "candidate_weak",
                    "Variant Type": "fixed_blend",
                    "Strategy Return": 0.0002 - noise,
                    "QQQ Return": 0.0002,
                    "SPY Return": 0.0001,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_build_mcs_style_diagnostics_excludes_dominated_candidate() -> None:
    result = build_mcs_style_diagnostics(
        _daily_returns(),
        bootstrap_iterations=99,
        block_size=5,
        random_seed=7,
        alpha=0.05,
    )
    summary = result["mcs_style_candidate_summary"].set_index("Run")
    global_summary = result["mcs_style_global_summary"].iloc[0]

    assert bool(summary.loc["candidate_best", "Observed Best Candidate"]) is True
    assert bool(summary.loc["candidate_best", "In MCS Style Confidence Set"]) is True
    assert bool(summary.loc["candidate_weak", "Dominated By Best Candidate"]) is True
    assert summary.loc["candidate_weak", "Pairwise P Value vs Best"] <= 0.05
    assert global_summary["MCS Style Confidence Set"] == "candidate_best"
    assert global_summary["diagnostic_scope"] == "mcs_style_pairwise_return_confidence_set_not_live_gate"


def test_mcs_style_cli_writes_outputs(tmp_path) -> None:
    daily_path = tmp_path / "daily.csv"
    output_dir = tmp_path / "out"
    _daily_returns().to_csv(daily_path, index=False)

    exit_code = main(
        [
            "--daily-returns",
            str(daily_path),
            "--output-dir",
            str(output_dir),
            "--candidate-runs",
            "candidate_best,candidate_weak",
            "--bootstrap-iterations",
            "99",
            "--block-size",
            "5",
            "--random-seed",
            "7",
            "--alpha",
            "0.05",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "mcs_style_candidate_summary.csv").exists()
    assert (output_dir / "mcs_style_pairwise_summary.csv").exists()
    assert (output_dir / "mcs_style_global_summary.csv").exists()
