from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines import live_decay_monitor as decay


def _wide_returns() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=320)
    recent_decay = [0.0010] * 220 + [-0.0010] * 100
    return pd.DataFrame(
        {
            "as_of": dates,
            "steady_winner": [0.0010] * len(dates),
            "recent_decay": recent_decay,
            "QQQ": [0.0007] * len(dates),
            "SPY": [0.0005] * len(dates),
        }
    )


def _russell_daily_returns() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for date in pd.bdate_range("2026-01-02", periods=90):
        rows.append(
            {
                "Date": date.date().isoformat(),
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Strategy Return": 0.0002,
                "QQQ Return": 0.0010,
                "SPY Return": 0.0008,
            }
        )
    return pd.DataFrame(rows)


def test_build_live_decay_monitor_flags_recent_underperformance() -> None:
    result = decay.build_live_decay_monitor(
        _wide_returns(),
        strategies=("steady_winner", "recent_decay"),
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
        windows=(63, 126, 252),
        min_observations=40,
    )

    window_summary = result["live_decay_window_summary"]
    strategy_summary = result["live_decay_strategy_summary"].set_index("strategy")

    assert strategy_summary.loc["steady_winner", "overall_decay_state"] == decay.KEEP
    assert strategy_summary.loc["recent_decay", "overall_decay_state"] == decay.REVIEW
    assert strategy_summary.loc["recent_decay", "review_window_count"] >= 1
    recent_63 = window_summary.loc[
        window_summary["strategy"].eq("recent_decay") & window_summary["window"].eq("trailing_63d")
    ].iloc[0]
    assert recent_63["decay_state"] == decay.REVIEW
    assert recent_63["excess_cagr_vs_primary"] < 0
    assert recent_63["excess_cagr_vs_secondary"] < 0


def test_russell_long_form_daily_returns_are_supported_with_expected_edge_check() -> None:
    result = decay.build_live_decay_monitor(
        _russell_daily_returns(),
        candidate_runs=("blend_top2_50_top4_50",),
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
        windows=(63,),
        min_observations=40,
        expected_excess_cagr_by_strategy={"blend_top2_50_top4_50": 0.10},
        min_realized_expected_ratio=0.50,
    )

    summary = result["live_decay_strategy_summary"].set_index("strategy")
    windows = result["live_decay_window_summary"].set_index(["strategy", "window"])

    assert summary.loc["blend_top2_50_top4_50", "overall_decay_state"] == decay.REVIEW
    assert windows.loc[("blend_top2_50_top4_50", "trailing_63d"), "realized_expected_ratio"] < 0.50
    assert "expected edge" in windows.loc[("blend_top2_50_top4_50", "trailing_63d"), "decay_reason"]


def test_live_decay_monitor_cli_writes_manifest(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns.csv"
    output_dir = tmp_path / "decay"
    _wide_returns().to_csv(returns_path, index=False)

    exit_code = decay.main(
        [
            "--returns",
            str(returns_path),
            "--strategies",
            "steady_winner,recent_decay",
            "--primary-benchmark",
            "QQQ",
            "--secondary-benchmark",
            "SPY",
            "--windows",
            "63,126,252",
            "--min-observations",
            "40",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "live_decay_window_summary.csv").exists()
    assert (output_dir / "live_decay_strategy_summary.csv").exists()
    assert (output_dir / "live_decay_report.md").exists()
    manifest = json.loads((output_dir / "live_decay_monitor_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "live_decay_monitor"
    assert manifest["primary_benchmark"] == "QQQ"
    assert manifest["row_counts"]["live_decay_strategy_summary"] == 2
