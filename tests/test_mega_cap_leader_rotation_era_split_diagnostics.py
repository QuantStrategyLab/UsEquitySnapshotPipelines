from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_era_split_diagnostics import (
    build_era_split_diagnostics,
    main,
    parse_era_specs,
)


def _daily_returns() -> pd.DataFrame:
    rows = []
    for as_of in pd.bdate_range("2020-01-02", periods=40):
        rows.extend(
            [
                {"Date": as_of.date().isoformat(), "Run": "candidate_a", "Variant Type": "fixed_blend", "Strategy Return": 0.0020, "QQQ Return": 0.0010, "SPY Return": 0.0005},
                {"Date": as_of.date().isoformat(), "Run": "candidate_b", "Variant Type": "fixed_blend", "Strategy Return": 0.0010, "QQQ Return": 0.0010, "SPY Return": 0.0005},
            ]
        )
    for as_of in pd.bdate_range("2021-01-04", periods=40):
        rows.extend(
            [
                {"Date": as_of.date().isoformat(), "Run": "candidate_a", "Variant Type": "fixed_blend", "Strategy Return": 0.0015, "QQQ Return": 0.0005, "SPY Return": 0.0002},
                {"Date": as_of.date().isoformat(), "Run": "candidate_b", "Variant Type": "fixed_blend", "Strategy Return": 0.0008, "QQQ Return": 0.0005, "SPY Return": 0.0002},
            ]
        )
    return pd.DataFrame(rows)


def test_build_era_split_diagnostics_summarizes_candidate_robustness() -> None:
    result = build_era_split_diagnostics(
        _daily_returns(),
        eras=parse_era_specs("era1:2020-01-01:2020-12-31,era2:2021-01-01:2021-12-31"),
        min_observations=20,
        min_best_era_count=2,
        min_positive_qqq_excess_rate=1.0,
        min_positive_spy_excess_rate=1.0,
    )
    detail = result["era_split_candidate_summary"]
    summary = result["era_split_promotion_summary"].set_index("Run")

    assert len(detail) == 4
    assert int(summary.loc["candidate_a", "Best CAGR Era Count"]) == 2
    assert bool(summary.loc["candidate_a", "era_robustness_passed"]) is True
    assert summary.loc["candidate_a", "diagnostic_scope"] == "pre_registered_era_split_not_live_gate"
    assert bool(summary.loc["candidate_b", "era_robustness_passed"]) is False


def test_era_split_cli_writes_outputs(tmp_path) -> None:
    daily_path = tmp_path / "daily.csv"
    output_dir = tmp_path / "out"
    _daily_returns().to_csv(daily_path, index=False)

    exit_code = main(
        [
            "--daily-returns",
            str(daily_path),
            "--output-dir",
            str(output_dir),
            "--eras",
            "era1:2020-01-01:2020-12-31,era2:2021-01-01:2021-12-31",
            "--min-observations",
            "20",
            "--min-best-era-count",
            "2",
            "--min-positive-qqq-excess-rate",
            "1.0",
            "--min-positive-spy-excess-rate",
            "1.0",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "era_split_candidate_summary.csv").exists()
    assert (output_dir / "era_split_promotion_summary.csv").exists()
