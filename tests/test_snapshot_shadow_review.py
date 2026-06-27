from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.snapshot_shadow_review import build_snapshot_shadow_review_rows


def test_build_snapshot_shadow_review_rows_marks_candidate_pass_when_recent_excess_is_stable() -> None:
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
            "as_of": pd.bdate_range("2026-01-02", periods=260),
            "live_r1000_low_vol_momentum_24_proxy": [0.0005] * 260,
            "new_r1000_residual_strength_20": [0.0007] * 260,
        }
    )

    rows = build_snapshot_shadow_review_rows(ranking, returns)

    assert len(rows) == 1
    assert rows.iloc[0]["candidate"] == "new_r1000_residual_strength_20"
    assert bool(rows.iloc[0]["shadow_review_passed"]) is True
