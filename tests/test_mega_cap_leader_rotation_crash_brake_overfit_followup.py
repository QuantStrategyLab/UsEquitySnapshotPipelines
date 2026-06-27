from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_overfit_followup import main


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "blend_top2_50_top4_50_no_brake",
                "Variant Type": "fixed_blend_no_brake",
                "CAGR": 0.18,
                "Sharpe": 0.95,
                "Max Drawdown": -0.20,
                "Turnover/Year": 3.0,
            },
            {
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "CAGR": 0.17,
                "Sharpe": 0.92,
                "Max Drawdown": -0.18,
                "Turnover/Year": 3.1,
            },
        ]
    )


def _rolling() -> pd.DataFrame:
    rows = []
    windows = [(3, 2020, 2022), (3, 2021, 2023), (5, 2019, 2023)]
    values = {
        "blend_top2_50_top4_50_no_brake": [0.16, 0.20, 0.19],
        "crash_brake_top2_50_floor25": [0.10, 0.22, 0.18],
    }
    qqq = [0.14, 0.18, 0.17]
    spy = [0.08, 0.11, 0.10]
    variant_types = {
        "blend_top2_50_top4_50_no_brake": "fixed_blend_no_brake",
        "crash_brake_top2_50_floor25": "panic_rebound_top2_sleeve_floor",
    }
    for run, cagrs in values.items():
        for (years, start, end), cagr, qqq_cagr, spy_cagr in zip(windows, cagrs, qqq, spy, strict=True):
            rows.append(
                {
                    "Run": run,
                    "Variant Type": variant_types[run],
                    "Window Years": years,
                    "Window Start Year": start,
                    "Window End Year": end,
                    "Strategy CAGR": cagr,
                    "Strategy Max Drawdown": -0.15,
                    "QQQ CAGR": qqq_cagr,
                    "SPY CAGR": spy_cagr,
                }
            )
    return pd.DataFrame(rows)


def test_crash_brake_overfit_followup_cli_writes_manifest(tmp_path: Path) -> None:
    summary_path = tmp_path / "crash_brake_summary.csv"
    rolling_path = tmp_path / "crash_brake_rolling_summary.csv"
    research_manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "out"
    _summary().to_csv(summary_path, index=False)
    _rolling().to_csv(rolling_path, index=False)
    research_manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "russell_top50_crash_brake_research",
                "artifact_schema_version": "russell_top50_crash_brake_research.v1",
                "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
                "candidate_runs": [
                    "blend_top2_50_top4_50_no_brake",
                    "crash_brake_top2_50_floor25",
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--rolling",
            str(rolling_path),
            "--research-manifest",
            str(research_manifest_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "overfit_candidate_diagnostics.csv").exists()
    assert (output_dir / "overfit_rank_windows.csv").exists()
    gate = pd.read_csv(output_dir / "overfit_promotion_gate_summary.csv").set_index("Run")
    manifest = json.loads((output_dir / "crash_brake_overfit_followup_manifest.json").read_text(encoding="utf-8"))
    assert bool(gate.loc["crash_brake_top2_50_floor25", "live_promotion_gate_passed"]) is False
    assert "not_promotable_candidate_family" in gate.loc["crash_brake_top2_50_floor25", "live_promotion_gate_reason"]
    assert manifest["manifest_type"] == "russell_top50_crash_brake_overfit_followup"
    assert manifest["experiment_profile"] == "panic_rebound_top2_sleeve_floor_v1"
