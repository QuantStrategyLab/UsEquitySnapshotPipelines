from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_shadow_review import (
    build_shadow_review_input_payload,
    main,
)


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50_no_brake", "Turnover/Year": 3.0},
            {"Run": "crash_brake_top2_50_floor25", "Turnover/Year": 3.2},
        ]
    )


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2024-01-03",
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Symbol": "AAPL",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2024-01-03",
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Symbol": "MSFT",
                "Previous Weight": 0.0,
                "Target Weight": 0.25,
                "Trade Weight Delta": 0.25,
                "Abs Trade Weight Delta": 0.25,
            },
            {
                "Date": "2024-01-03",
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Symbol": "BOXX",
                "Previous Weight": 0.0,
                "Target Weight": 0.50,
                "Trade Weight Delta": 0.50,
                "Abs Trade Weight Delta": 0.50,
            },
        ]
    )


def test_build_shadow_review_input_payload_maps_crash_brake_to_shadow_contract() -> None:
    payload = build_shadow_review_input_payload(
        summary=_summary(),
        trades=_trades(),
        candidate_runs=("crash_brake_top2_50_floor25",),
        reference_run="blend_top2_50_top4_50_no_brake",
    )

    row = payload["diagnostics"]["leader_rotation_shadow_review_rows"][0]
    assert row["active_variant"] == "blend_top2_50_top4_50_no_brake"
    assert row["shadow_variant"] == "crash_brake_top2_50_floor25"
    assert row["selected_count"] == 2
    assert row["realized_stock_weight"] == 0.5
    assert row["safe_haven_weight"] == 0.5


def test_crash_brake_shadow_review_cli_writes_outputs(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.csv"
    trades_path = tmp_path / "trades.csv"
    output_dir = tmp_path / "out"
    _summary().to_csv(summary_path, index=False)
    _trades().to_csv(trades_path, index=False)

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--trades",
            str(trades_path),
            "--output-dir",
            str(output_dir),
            "--snapshot-as-of",
            "2026-06-30",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "russell_top50_leader_rotation_shadow_review_rows.csv").exists()
    assert (output_dir / "russell_top50_leader_rotation_shadow_review_rows.json").exists()
    manifest = json.loads((output_dir / "russell_top50_leader_rotation_shadow_review_manifest.json").read_text(encoding="utf-8"))
    rows = pd.read_csv(output_dir / "russell_top50_leader_rotation_shadow_review_rows.csv")
    assert manifest["manifest_type"] == "shadow_review_artifact"
    assert manifest["snapshot_as_of"] == "2026-06-30"
    assert rows.loc[0, "shadow_variant"] == "crash_brake_top2_50_floor25"
