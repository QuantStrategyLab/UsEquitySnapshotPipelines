from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_shadow_live_ledger import build_shadow_live_ledger, main


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2026-01-31",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "AAPL",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2026-01-31",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "MSFT",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2026-01-31",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "SGOV",
                "Previous Weight": 1.0,
                "Target Weight": 0.50,
                "Trade Weight Delta": -0.50,
                "Abs Trade Weight Delta": 0.50,
            },
            {
                "Date": "2026-02-28",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "AAPL",
                "Previous Weight": 0.25,
                "Target Weight": 0.0,
                "Trade Weight Delta": -0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2026-02-28",
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Symbol": "NVDA",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
        ]
    )


def _daily_returns() -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range("2026-01-30", periods=45)
    for idx, date in enumerate(dates):
        rows.append(
            {
                "Date": date.date().isoformat(),
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Strategy Return": 0.002 if idx % 2 == 0 else -0.0005,
                "QQQ Return": 0.001,
                "SPY Return": 0.0008,
            }
        )
    return pd.DataFrame(rows)


def _prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Date": "2026-01-31", "Symbol": "AAPL", "Close": 100.0},
            {"Date": "2026-02-02", "Symbol": "AAPL", "Close": 101.0},
            {"Date": "2026-01-31", "Symbol": "MSFT", "Close": 200.0},
            {"Date": "2026-02-02", "Symbol": "MSFT", "Close": 198.0},
            {"Date": "2026-02-28", "Symbol": "NVDA", "Close": 300.0},
            {"Date": "2026-03-02", "Symbol": "NVDA", "Close": 306.0},
        ]
    )


def test_build_shadow_live_ledger_reconstructs_holdings_and_forward_returns(tmp_path: Path) -> None:
    result = build_shadow_live_ledger(
        rebalance_trades=_trades(),
        daily_returns=_daily_returns(),
        prices=_prices(),
        output_dir=tmp_path,
        candidate_runs=("blend_top2_50_top4_50",),
        portfolio_nav=1_000_000,
        slippage_bps=5,
        forward_window_days=5,
        safe_haven="SGOV",
    )

    trades = result["shadow_live_trade_ledger"]
    holdings = result["shadow_live_holdings_ledger"]
    summary = result["shadow_live_rebalance_summary"].set_index("Date")
    manifest = json.loads((tmp_path / "shadow_live_ledger_manifest.json").read_text(encoding="utf-8"))

    first_holdings = holdings.loc[holdings["Date"].eq("2026-01-31")].set_index("Symbol")
    second_holdings = holdings.loc[holdings["Date"].eq("2026-02-28")].set_index("Symbol")

    assert first_holdings.loc["AAPL", "Target Weight"] == 0.25
    assert first_holdings.loc["MSFT", "Target Weight"] == 0.25
    assert first_holdings.loc["SGOV", "Target Weight"] == 0.50
    assert "AAPL" not in second_holdings.index
    assert second_holdings.loc["NVDA", "Target Weight"] == 0.25
    assert second_holdings.loc["MSFT", "Target Weight"] == 0.25

    assert summary.loc["2026-01-31", "Gross Trade Notional"] == 1_000_000
    assert summary.loc["2026-01-31", "Estimated Slippage Cost"] == 500
    assert summary.loc["2026-01-31", "Forward Window Trading Days"] == 5
    assert summary.loc["2026-01-31", "Forward Strategy Return"] != 0
    assert summary.loc["2026-01-31", "Forward Excess Return vs QQQ"] != 0
    assert trades.loc[trades["Symbol"].eq("AAPL"), "Next Session Price"].iloc[0] == 101.0
    assert manifest["manifest_type"] == "russell_top50_shadow_live_ledger"
    assert manifest["row_counts"]["shadow_live_trade_ledger"] == len(trades)


def test_shadow_live_ledger_cli_writes_outputs(tmp_path: Path) -> None:
    trades_path = tmp_path / "trades.csv"
    daily_path = tmp_path / "daily.csv"
    price_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "out"
    _trades().to_csv(trades_path, index=False)
    _daily_returns().to_csv(daily_path, index=False)
    _prices().to_csv(price_path, index=False)

    exit_code = main(
        [
            "--rebalance-trades",
            str(trades_path),
            "--daily-returns",
            str(daily_path),
            "--prices",
            str(price_path),
            "--candidate-runs",
            "blend_top2_50_top4_50",
            "--portfolio-nav",
            "1000000",
            "--slippage-bps",
            "5",
            "--forward-window-days",
            "5",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "shadow_live_trade_ledger.csv").exists()
    assert (output_dir / "shadow_live_holdings_ledger.csv").exists()
    assert (output_dir / "shadow_live_rebalance_summary.csv").exists()
    assert (output_dir / "shadow_live_ledger_manifest.json").exists()
