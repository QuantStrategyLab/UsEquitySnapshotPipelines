from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_capacity_stress import build_capacity_stress, main


def _shadow_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2026-01-31",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Portfolio NAV": 1_000_000,
                "Trade Count": 4,
                "Gross Turnover Weight": 1.0,
                "One Way Turnover Weight": 0.5,
                "Gross Trade Notional": 1_000_000,
                "Estimated Slippage Cost": 500,
                "Forward Window Trading Days": 21,
                "Forward Strategy Return": 0.040,
                "Forward QQQ Return": 0.020,
                "Forward SPY Return": 0.015,
                "Forward Excess Return vs QQQ": 0.020,
                "Forward Excess Return vs SPY": 0.025,
            },
            {
                "Date": "2026-02-28",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Portfolio NAV": 1_000_000,
                "Trade Count": 2,
                "Gross Turnover Weight": 0.5,
                "One Way Turnover Weight": 0.25,
                "Gross Trade Notional": 500_000,
                "Estimated Slippage Cost": 250,
                "Forward Window Trading Days": 21,
                "Forward Strategy Return": 0.010,
                "Forward QQQ Return": 0.015,
                "Forward SPY Return": 0.008,
                "Forward Excess Return vs QQQ": -0.005,
                "Forward Excess Return vs SPY": 0.002,
            },
        ]
    )


def _liquidity_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50",
                "Portfolio NAV": 1_000_000,
                "Max Participation Rate": 0.004,
                "Allowed Max Participation Rate": 0.01,
            }
        ]
    )


def test_build_capacity_stress_scales_costs_and_participation() -> None:
    result = build_capacity_stress(
        shadow_live_rebalance_summary=_shadow_summary(),
        liquidity_summary=_liquidity_summary(),
        portfolio_nav_values=(1_000_000, 10_000_000),
        slippage_bps_values=(5, 50),
        split_trade_days_values=(1, 2),
        min_median_net_excess_vs_qqq=0.0,
    )

    detail = result["capacity_stress_detail"]
    summary = result["capacity_stress_summary"]
    manifest_inputs = result["capacity_stress_manifest_inputs"]

    row = detail.loc[
        detail["Portfolio NAV"].eq(10_000_000)
        & detail["Slippage Bps"].eq(50.0)
        & detail["Split Trade Days"].eq(2)
        & detail["Date"].eq("2026-01-31")
    ].iloc[0]
    assert row["Gross Trade Notional"] == 10_000_000
    assert row["Daily Split Trade Notional"] == 5_000_000
    assert row["Estimated Slippage Cost"] == 50_000
    assert row["Net Forward Strategy Return"] == 0.035
    assert row["Estimated Max Participation Rate"] == 0.02
    assert bool(row["participation_gate_passed"]) is False

    stressed = summary.loc[
        summary["Portfolio NAV"].eq(10_000_000)
        & summary["Slippage Bps"].eq(50.0)
        & summary["Split Trade Days"].eq(2)
    ].iloc[0]
    assert bool(stressed["capacity_stress_passed"]) is False
    assert "participation_rate_above_limit" in stressed["capacity_stress_reason"]
    assert manifest_inputs["detail_rows"] == len(detail)


def test_capacity_stress_cli_writes_outputs(tmp_path: Path) -> None:
    shadow_path = tmp_path / "shadow_live_rebalance_summary.csv"
    liquidity_path = tmp_path / "liquidity_summary.csv"
    output_dir = tmp_path / "out"
    _shadow_summary().to_csv(shadow_path, index=False)
    _liquidity_summary().to_csv(liquidity_path, index=False)

    exit_code = main(
        [
            "--shadow-live-summary",
            str(shadow_path),
            "--liquidity-summary",
            str(liquidity_path),
            "--portfolio-nav-values",
            "1000000,10000000",
            "--slippage-bps-values",
            "5,50",
            "--split-trade-days-values",
            "1,2",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "capacity_stress_detail.csv").exists()
    assert (output_dir / "capacity_stress_summary.csv").exists()
    manifest = json.loads((output_dir / "capacity_stress_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "russell_top50_capacity_stress"
    assert manifest["row_counts"]["capacity_stress_summary"] > 0
