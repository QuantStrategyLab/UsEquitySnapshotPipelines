from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_decay_monitors import (
    build_live_decay_error_report,
    build_live_decay_for_returns,
    discover_live_decay_inputs,
    main,
    resolve_russell_candidate_runs,
    resolve_wide_strategies,
)


def _russell_returns() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2026-01-02", periods=90)
    for run, daily_return in {
        "blend_top2_50_top4_50": 0.0003,
        "base_top4_cap25": 0.0008,
    }.items():
        for date in dates:
            rows.append(
                {
                    "Date": date.date().isoformat(),
                    "Run": run,
                    "Strategy Return": daily_return,
                    "QQQ Return": 0.0010,
                    "SPY Return": 0.0007,
                }
            )
    return pd.DataFrame(rows)


def _global_returns() -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-02", periods=90)
    return pd.DataFrame(
        {
            "as_of": dates,
            "liveable_blend_baseline90_fast10": [0.0009] * len(dates),
            "live_global_etf_rotation_defensive_baseline": [0.0006] * len(dates),
            "QQQ": [0.0008] * len(dates),
            "SPY": [0.0005] * len(dates),
        }
    )


def test_discover_live_decay_inputs_finds_russell_and_global_returns(tmp_path: Path) -> None:
    russell_path = tmp_path / "russell" / "concentration_variant_daily_returns.csv"
    global_path = tmp_path / "global" / "portfolio_returns_with_benchmarks.csv"
    ignored_path = tmp_path / "live_decay_monitor_old" / "portfolio_returns_with_benchmarks.csv"
    for path in (russell_path, global_path, ignored_path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")

    assert discover_live_decay_inputs(tmp_path) == [("russell_daily", russell_path), ("wide", global_path)]


def test_resolve_russell_candidate_runs_prefers_requested_intersection() -> None:
    frame = _russell_returns()

    assert resolve_russell_candidate_runs(frame, ("blend_top2_50_top4_50", "missing")) == (
        "blend_top2_50_top4_50",
    )
    assert resolve_russell_candidate_runs(frame, ("missing",)) == (
        "blend_top2_50_top4_50",
        "base_top4_cap25",
    )


def test_resolve_wide_strategies_prefers_requested_columns() -> None:
    frame = _global_returns()

    assert resolve_wide_strategies(
        frame,
        ("liveable_blend_baseline90_fast10", "missing"),
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
    ) == ("liveable_blend_baseline90_fast10",)
    assert resolve_wide_strategies(frame, ("missing",), primary_benchmark="QQQ", secondary_benchmark="SPY") == (
        "liveable_blend_baseline90_fast10",
        "live_global_etf_rotation_defensive_baseline",
    )


def test_build_live_decay_for_russell_returns_writes_manifest(tmp_path: Path) -> None:
    returns_path = tmp_path / "russell_research" / "concentration_variant_daily_returns.csv"
    returns_path.parent.mkdir()
    _russell_returns().to_csv(returns_path, index=False)

    output_dir = build_live_decay_for_returns(
        returns_path,
        input_format="russell_daily",
        output_root=tmp_path,
        windows=(63,),
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
        min_observations=40,
        russell_candidate_runs=("blend_top2_50_top4_50",),
    )

    assert output_dir == tmp_path / "live_decay_monitor_russell_research"
    manifest = json.loads((output_dir / "live_decay_monitor_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "live_decay_monitor"
    assert manifest["input_format"] == "russell_daily"
    assert manifest["strategies"] == ["blend_top2_50_top4_50"]
    assert manifest["row_counts"]["live_decay_strategy_summary"] == 1


def test_build_live_decay_for_global_returns_writes_manifest(tmp_path: Path) -> None:
    returns_path = tmp_path / "global_research" / "portfolio_returns_with_benchmarks.csv"
    returns_path.parent.mkdir()
    _global_returns().to_csv(returns_path, index=False)

    output_dir = build_live_decay_for_returns(
        returns_path,
        input_format="wide",
        output_root=tmp_path,
        windows=(63,),
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
        min_observations=40,
        global_etf_strategies=("liveable_blend_baseline90_fast10",),
    )

    assert output_dir == tmp_path / "live_decay_monitor_global_research"
    manifest = json.loads((output_dir / "live_decay_monitor_manifest.json").read_text(encoding="utf-8"))
    assert manifest["input_format"] == "wide"
    assert manifest["strategies"] == ["liveable_blend_baseline90_fast10"]


def test_build_live_decay_error_report_writes_error_artifacts(tmp_path: Path) -> None:
    returns_path = tmp_path / "bad_research" / "portfolio_returns_with_benchmarks.csv"
    returns_path.parent.mkdir()

    output_dir = build_live_decay_error_report(returns_path, output_root=tmp_path, error=ValueError("bad decay"))

    payload = json.loads((output_dir / "live_decay_monitor_error.json").read_text(encoding="utf-8"))
    assert output_dir == tmp_path / "live_decay_monitor_error_bad_research"
    assert payload["artifact_type"] == "live_decay_monitor_error"
    assert payload["error_type"] == "ValueError"
    assert payload["error_message"] == "bad decay"


def test_main_builds_monitors_and_continues_on_errors(tmp_path: Path, monkeypatch, capsys) -> None:
    good_dir = tmp_path / "global_research"
    bad_dir = tmp_path / "bad_research"
    good_dir.mkdir()
    bad_dir.mkdir()
    _global_returns().to_csv(good_dir / "portfolio_returns_with_benchmarks.csv", index=False)
    _global_returns().drop(columns=["QQQ"]).to_csv(bad_dir / "portfolio_returns_with_benchmarks.csv", index=False)
    output_root = tmp_path / "monitors"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_decay_monitors.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
            "--windows",
            "63",
            "--min-observations",
            "40",
        ],
    )

    assert main() == 0

    captured = capsys.readouterr()
    assert "live_decay_monitor_count=1" in captured.out
    assert "live_decay_monitor_error_count=1" in captured.out
    assert (output_root / "live_decay_monitor_global_research" / "live_decay_monitor_manifest.json").exists()
    assert (output_root / "live_decay_monitor_error_bad_research" / "live_decay_monitor_error.json").exists()
