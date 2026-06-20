from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_reality_check import (
    build_reality_check_diagnostics,
    main,
)


def _daily_returns() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows = []
    for idx, as_of in enumerate(dates):
        qqq = 0.0002 if idx % 5 else -0.0001
        rows.extend(
            [
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "candidate_strong",
                    "Variant Type": "fixed_blend",
                    "Strategy Return": qqq + 0.0010,
                    "QQQ Return": qqq,
                    "SPY Return": qqq - 0.0001,
                },
                {
                    "Date": as_of.date().isoformat(),
                    "Run": "candidate_weak",
                    "Variant Type": "fixed_blend",
                    "Strategy Return": qqq + 0.0001,
                    "QQQ Return": qqq,
                    "SPY Return": qqq - 0.0001,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_build_reality_check_diagnostics_marks_best_candidate() -> None:
    result = build_reality_check_diagnostics(
        _daily_returns(),
        bootstrap_iterations=99,
        block_size=5,
        random_seed=7,
        alpha=0.05,
    )
    candidates = result["reality_check_candidate_summary"]
    global_summary = result["reality_check_global_summary"].iloc[0]

    best = candidates.iloc[0]
    assert best["Run"] == "candidate_strong"
    assert bool(best["Observed Best Candidate"]) is True
    assert best["Reality Check P Value"] <= 0.05
    assert bool(best["Reality Check Passed"]) is True
    assert global_summary["Best Run"] == "candidate_strong"
    assert global_summary["diagnostic_scope"] == "return_panel_bootstrap_not_live_gate"


def test_reality_check_cli_writes_outputs(tmp_path) -> None:
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
            "candidate_strong,candidate_weak",
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
    assert (output_dir / "reality_check_candidate_summary.csv").exists()
    assert (output_dir / "reality_check_global_summary.csv").exists()
