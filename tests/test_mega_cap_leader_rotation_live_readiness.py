from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_live_readiness import (
    evaluate_live_readiness,
    main,
)


def _summary_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Run": "base_top2_cap50",
                "Variant Type": "base_top2",
                "Universe Lag Trading Days": 21,
                "Start": "2017-10-02",
                "End": "2026-06-18",
                "CAGR": 0.49,
                "Max Drawdown": -0.38,
                "Sharpe": 1.20,
                "Calmar": 1.30,
                "Total Return": 32.0,
                "Benchmark Total Return": 4.0,
                "Broad Benchmark Total Return": 2.0,
                "Turnover/Year": 3.5,
            },
            {
                "Run": "blend_top2_25_top4_75",
                "Variant Type": "fixed_blend",
                "Universe Lag Trading Days": 21,
                "Start": "2017-10-02",
                "End": "2026-06-18",
                "CAGR": 0.42,
                "Max Drawdown": -0.28,
                "Sharpe": 1.26,
                "Calmar": 1.50,
                "Total Return": 20.0,
                "Benchmark Total Return": 4.0,
                "Broad Benchmark Total Return": 2.0,
                "Turnover/Year": 3.5,
            },
            {
                "Run": "blend_top2_50_top4_50",
                "Variant Type": "fixed_blend",
                "Universe Lag Trading Days": 21,
                "Start": "2017-10-02",
                "End": "2026-06-18",
                "CAGR": 0.45,
                "Max Drawdown": -0.31,
                "Sharpe": 1.27,
                "Calmar": 1.47,
                "Total Return": 24.0,
                "Benchmark Total Return": 4.0,
                "Broad Benchmark Total Return": 2.0,
                "Turnover/Year": 3.5,
            },
            {
                "Run": "dynamic_top2_dd10_to_top4",
                "Variant Type": "dynamic_top2_drawdown_to_top4",
                "Universe Lag Trading Days": 21,
                "Start": "2017-10-02",
                "End": "2026-06-18",
                "CAGR": 0.43,
                "Max Drawdown": -0.30,
                "Sharpe": 1.16,
                "Calmar": 1.44,
                "Total Return": 22.0,
                "Benchmark Total Return": 4.0,
                "Broad Benchmark Total Return": 2.0,
                "Turnover/Year": 5.1,
            },
        ]
    )


def _rolling_rows() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for run, min_3y_qqq in {
        "base_top2_cap50": -0.02,
        "blend_top2_25_top4_75": -0.06,
        "blend_top2_50_top4_50": -0.04,
        "dynamic_top2_dd10_to_top4": -0.05,
    }.items():
        rows.extend(
            [
                {
                    "Run": run,
                    "Window Years": 3,
                    "Window Start Year": 2019,
                    "Window End Year": 2021,
                    "Strategy CAGR": 0.30 + min_3y_qqq,
                    "Strategy Max Drawdown": -0.28,
                    "QQQ CAGR": 0.30,
                    "SPY CAGR": 0.20,
                },
                {
                    "Run": run,
                    "Window Years": 5,
                    "Window Start Year": 2018,
                    "Window End Year": 2022,
                    "Strategy CAGR": 0.30,
                    "Strategy Max Drawdown": -0.29,
                    "QQQ CAGR": 0.18,
                    "SPY CAGR": 0.15,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_evaluate_live_readiness_promotes_fixed_blends_and_rejects_research_only() -> None:
    result = evaluate_live_readiness(_summary_rows(), _rolling_rows())
    by_run = result.set_index("Run")

    assert bool(by_run.loc["blend_top2_25_top4_75", "live_gate_passed"]) is True
    assert by_run.loc["blend_top2_25_top4_75", "Candidate Role"] == "conservative_live_design"
    assert bool(by_run.loc["blend_top2_50_top4_50", "live_gate_passed"]) is True
    assert by_run.loc["blend_top2_50_top4_50", "Candidate Role"] == "balanced_offensive_live_design"
    assert bool(by_run.loc["base_top2_cap50", "live_gate_passed"]) is False
    assert "research_only_role" in by_run.loc["base_top2_cap50", "live_gate_reason"]
    assert bool(by_run.loc["dynamic_top2_dd10_to_top4", "live_gate_passed"]) is False
    assert "dynamic_or_daily_risk_candidate" in by_run.loc["dynamic_top2_dd10_to_top4", "live_gate_reason"]


def test_evaluate_live_readiness_fails_when_five_year_qqq_excess_is_negative() -> None:
    rolling = _rolling_rows()
    mask = (rolling["Run"] == "blend_top2_25_top4_75") & (rolling["Window Years"] == 5)
    rolling.loc[mask, "Strategy CAGR"] = 0.10
    rolling.loc[mask, "QQQ CAGR"] = 0.18

    result = evaluate_live_readiness(_summary_rows(), rolling)
    row = result.loc[result["Run"].eq("blend_top2_25_top4_75")].iloc[0]

    assert bool(row["live_gate_passed"]) is False
    assert "min_5y_qqq_excess_below_0.00%" in row["live_gate_reason"]


def test_live_readiness_cli_writes_summary(tmp_path) -> None:
    summary_path = tmp_path / "summary.csv"
    rolling_path = tmp_path / "rolling.csv"
    output_dir = tmp_path / "out"
    _summary_rows().to_csv(summary_path, index=False)
    _rolling_rows().to_csv(rolling_path, index=False)

    exit_code = main(
        [
            "--summary",
            str(summary_path),
            "--rolling",
            str(rolling_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    output_path = output_dir / "live_readiness_summary.csv"
    assert output_path.exists()
    written = pd.read_csv(output_path)
    assert {"Run", "live_gate_passed", "live_gate_reason"}.issubset(written.columns)
