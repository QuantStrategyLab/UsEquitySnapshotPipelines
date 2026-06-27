from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_live_decay_followup import main


def _returns() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2026-01-02", periods=90)
    for date in dates:
        rows.append(
            {
                "Date": date.date().isoformat(),
                "Run": "crash_brake_top2_50_floor25",
                "Variant Type": "panic_rebound_top2_sleeve_floor",
                "Strategy Return": 0.0002,
                "QQQ Return": 0.0010,
                "SPY Return": 0.0008,
            }
        )
    return pd.DataFrame(rows)


def test_crash_brake_live_decay_followup_cli_writes_manifest(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns.csv"
    research_manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "out"
    _returns().to_csv(returns_path, index=False)
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
            "--returns",
            str(returns_path),
            "--research-manifest",
            str(research_manifest_path),
            "--candidate-runs",
            "crash_brake_top2_50_floor25",
            "--windows",
            "63",
            "--min-observations",
            "40",
            "--output-dir",
            str(output_dir),
            "--expected-excess-cagr",
            "0.10",
            "--min-realized-expected-ratio",
            "0.50",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "live_decay_window_summary.csv").exists()
    assert (output_dir / "live_decay_strategy_summary.csv").exists()
    assert (output_dir / "live_decay_report.md").exists()
    manifest = json.loads((output_dir / "live_decay_monitor_manifest.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(output_dir / "live_decay_strategy_summary.csv").set_index("strategy")
    assert manifest["manifest_type"] == "live_decay_monitor"
    assert manifest["input_format"] == "russell_daily"
    assert manifest["strategies"] == ["crash_brake_top2_50_floor25"]
    assert summary.loc["crash_brake_top2_50_floor25", "overall_decay_state"] in {"review", "watch", "keep"}
