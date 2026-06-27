from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.snapshot_live_decay_followup import build_snapshot_live_decay_followup


def test_build_snapshot_live_decay_followup_uses_active_baseline_as_primary(tmp_path) -> None:
    ranking = pd.DataFrame(
        [
            {
                "Candidate": "live_r1000_low_vol_momentum_24_proxy",
                "Candidate Group": "current_live_baseline",
                "replacement_review_candidate": False,
            },
            {
                "Candidate": "new_r1000_residual_strength_20",
                "Candidate Group": "new_snapshot_strategy",
                "replacement_review_candidate": True,
            },
        ]
    )
    returns = pd.DataFrame(
        {
            "as_of": pd.bdate_range("2025-01-02", periods=260),
            "live_r1000_low_vol_momentum_24_proxy": [0.0004] * 260,
            "new_r1000_residual_strength_20": [0.0008] * 260,
        }
    )
    expected = pd.DataFrame(
        [{"strategy": "new_r1000_residual_strength_20", "expected_excess_cagr_vs_primary": 0.10}]
    )
    ranking_path = tmp_path / "ranking.csv"
    returns_path = tmp_path / "returns.csv"
    expected_path = tmp_path / "expected.csv"
    ranking.to_csv(ranking_path, index=False)
    returns.to_csv(returns_path, index=False)
    expected.to_csv(expected_path, index=False)

    output_dir = build_snapshot_live_decay_followup(
        ranking_path=ranking_path,
        candidate_returns_path=returns_path,
        expected_excess_path=expected_path,
        output_dir=tmp_path / "out",
    )
    summary = pd.read_csv(output_dir / "live_decay_strategy_summary.csv")

    assert summary.iloc[0]["strategy"] == "new_r1000_residual_strength_20"
    assert summary.iloc[0]["overall_decay_state"] == "keep"
