from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_liquidity_followup import main


def _prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=40)
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, close, volume in (
            ("AAPL", 190.0 + idx, 5_000_000),
            ("MSFT", 410.0 + idx, 4_000_000),
            ("NVDA", 800.0 + idx * 2, 6_000_000),
        ):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": close,
                    "volume": volume,
                }
            )
    return pd.DataFrame(rows)


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2024-01-03",
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Symbol": "AAPL",
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2024-01-03",
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Symbol": "MSFT",
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
        ]
    )


def test_crash_brake_liquidity_followup_cli_writes_manifest(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    trades_path = tmp_path / "trades.csv"
    research_manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "out"
    _prices().to_csv(prices_path, index=False)
    _trades().to_csv(trades_path, index=False)
    research_manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "russell_top50_crash_brake_research",
                "artifact_schema_version": "russell_top50_crash_brake_research.v1",
                "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--trades",
            str(trades_path),
            "--prices",
            str(prices_path),
            "--research-manifest",
            str(research_manifest_path),
            "--output-dir",
            str(output_dir),
            "--portfolio-nav-values",
            "100000,500000",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "liquidity_trade_detail.csv").exists()
    assert (output_dir / "liquidity_summary.csv").exists()
    manifest = json.loads((output_dir / "crash_brake_liquidity_followup_manifest.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(output_dir / "liquidity_summary.csv")
    assert not summary.empty
    assert manifest["manifest_type"] == "russell_top50_crash_brake_liquidity_followup"
    assert manifest["experiment_profile"] == "panic_rebound_top2_sleeve_floor_v1"
    assert manifest["row_counts"]["liquidity_summary"] > 0
