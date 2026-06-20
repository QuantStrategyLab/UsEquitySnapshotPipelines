from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_liquidity_diagnostics import (
    build_liquidity_diagnostics,
    main,
)


def _prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=30)
    rows = []
    for as_of in dates:
        rows.extend(
            [
                {"symbol": "AAPL", "as_of": as_of.date().isoformat(), "close": 100.0, "volume": 1_000_000},
                {"symbol": "MSFT", "as_of": as_of.date().isoformat(), "close": 200.0, "volume": 500_000},
                {"symbol": "BOXX", "as_of": as_of.date().isoformat(), "close": 100.0, "volume": 10_000_000},
            ]
        )
    return pd.DataFrame(rows)


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2024-01-10",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "AAPL",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2024-01-10",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "MSFT",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2024-01-10",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "BOXX",
                "Previous Weight": 1.0,
                "Target Weight": 0.5,
                "Trade Weight Delta": -0.5,
                "Abs Trade Weight Delta": 0.5,
            },
        ]
    )


def test_build_liquidity_diagnostics_flags_large_nav_participation() -> None:
    result = build_liquidity_diagnostics(
        _trades(),
        _prices(),
        portfolio_nav_values=(100_000.0, 10_000_000.0),
        adv_window=5,
        execution_days=1,
        max_participation_rate=0.01,
        exclude_symbols=("BOXX",),
    )
    summary = result["liquidity_summary"].sort_values("Portfolio NAV").reset_index(drop=True)
    assert bool(summary.loc[0, "liquidity_gate_passed"]) is True
    assert bool(summary.loc[1, "liquidity_gate_passed"]) is False
    assert summary.loc[1, "liquidity_gate_reason"] == "participation_rate_above_limit"
    detail = result["liquidity_trade_detail"]
    assert "BOXX" not in set(detail["Symbol"])


def test_liquidity_diagnostics_cli_writes_outputs(tmp_path) -> None:
    trades_path = tmp_path / "trades.csv"
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "out"
    _trades().to_csv(trades_path, index=False)
    _prices().to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--trades",
            str(trades_path),
            "--prices",
            str(prices_path),
            "--output-dir",
            str(output_dir),
            "--portfolio-nav-values",
            "100000,10000000",
            "--adv-window",
            "5",
            "--execution-days",
            "1",
            "--max-participation-rate",
            "0.01",
            "--exclude-symbols",
            "BOXX",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "liquidity_trade_detail.csv").exists()
    assert (output_dir / "liquidity_summary.csv").exists()
