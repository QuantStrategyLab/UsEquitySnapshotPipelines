from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_stress_readiness import (
    build_stress_live_readiness,
    main,
    parse_csv_floats_no_percent,
    summarize_stress_live_readiness,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=900)
    trends = {
        "QQQ": 0.0006,
        "SPY": 0.0004,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0013,
        "AMZN": 0.0008,
        "LLY": 0.0011,
        "XOM": 0.0012,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (90.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 2_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    first = [
        ("NVDA", "Information Technology"),
        ("MSFT", "Information Technology"),
        ("AAPL", "Information Technology"),
        ("XOM", "Energy"),
        ("LLY", "Health Care"),
    ]
    second = [
        ("AMZN", "Consumer Discretionary"),
        ("NVDA", "Information Technology"),
        ("MSFT", "Information Technology"),
        ("LLY", "Health Care"),
        ("XOM", "Energy"),
    ]
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2021-01-29",
                "end_date": "2021-12-31",
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(first, start=1)
        ]
        + [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2022-01-31",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(second, start=1)
        ]
    )


def test_parse_csv_floats_no_percent_keeps_basis_point_values() -> None:
    assert parse_csv_floats_no_percent("5,10,25", default=(1.0,)) == (5.0, 10.0, 25.0)


def test_build_stress_live_readiness_runs_cost_and_lag_matrix() -> None:
    result = build_stress_live_readiness(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2021-02-01",
        end_date="2023-05-31",
        turnover_cost_bps_values=(0.0, 5.0),
        universe_lag_days_values=(1, 2),
        min_adv20_usd_values=(1_000_000.0,),
        rolling_window_years=(1,),
        min_history_days=100,
    )

    detail = result["stress_live_readiness_detail"]
    summary = result["stress_live_readiness_summary"]
    assert set(detail["Stress Turnover Cost Bps"]) == {0.0, 5.0}
    assert set(detail["Stress Universe Lag Trading Days"]) == {1, 2}
    assert {
        "base_top4_cap25",
        "blend_top2_25_top4_75",
        "blend_top2_50_top4_50",
    } == set(summary["Run"])
    assert set(summary["Stress Scenarios"]) == {4}
    assert "stress_gate_reason" in summary.columns


def test_build_stress_live_readiness_can_evaluate_panic_guard_metric_gate() -> None:
    candidate_run = "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50"
    result = build_stress_live_readiness(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2021-02-01",
        end_date="2023-05-31",
        turnover_cost_bps_values=(0.0,),
        universe_lag_days_values=(1,),
        min_adv20_usd_values=(1_000_000.0,),
        rolling_window_years=(1,),
        blend_top2_weights=(0.50,),
        candidate_runs=(candidate_run,),
        min_history_days=100,
        include_panic_rebound_guard_variants=True,
        panic_guard_drawdown_threshold=0.10,
        panic_guard_rebound_threshold=0.03,
        panic_guard_vol_threshold=0.25,
        panic_guard_stock_exposure=0.50,
    )

    detail = result["stress_live_readiness_detail"]
    summary = result["stress_live_readiness_summary"]
    assert set(detail["Run"]) == {candidate_run}
    assert {
        "metric_gate_passed_excluding_research_role",
        "metric_gate_reason_excluding_research_role",
    }.issubset(detail.columns)
    assert set(summary["Run"]) == {candidate_run}
    assert "all_metric_gates_passed_excluding_research_role" in summary.columns


def test_summarize_stress_live_readiness_marks_all_pass_scenarios() -> None:
    detail = pd.DataFrame(
        [
            {
                "Run": "base_top4_cap25",
                "Candidate Role": "robust_baseline",
                "Gate Profile": "fallback",
                "Stress Scenario": "a",
                "Stress Turnover Cost Bps": 5.0,
                "Stress Universe Lag Trading Days": 21,
                "Stress Min ADV20 USD": 20_000_000.0,
                "Max Drawdown": -0.20,
                "Worst Rolling Max Drawdown": -0.25,
                "Min 3Y QQQ Excess CAGR": 0.01,
                "Min 3Y SPY Excess CAGR": 0.02,
                "Min 5Y QQQ Excess CAGR": 0.03,
                "Min 5Y SPY Excess CAGR": 0.04,
                "Turnover/Year": 1.5,
                "live_gate_passed": True,
                "live_gate_reason": "pass",
            },
            {
                "Run": "base_top4_cap25",
                "Candidate Role": "robust_baseline",
                "Gate Profile": "fallback",
                "Stress Scenario": "b",
                "Stress Turnover Cost Bps": 25.0,
                "Stress Universe Lag Trading Days": 42,
                "Stress Min ADV20 USD": 50_000_000.0,
                "Max Drawdown": -0.22,
                "Worst Rolling Max Drawdown": -0.27,
                "Min 3Y QQQ Excess CAGR": 0.005,
                "Min 3Y SPY Excess CAGR": 0.01,
                "Min 5Y QQQ Excess CAGR": 0.02,
                "Min 5Y SPY Excess CAGR": 0.03,
                "Turnover/Year": 2.0,
                "live_gate_passed": True,
                "live_gate_reason": "pass",
            },
        ]
    )

    summary = summarize_stress_live_readiness(detail)
    row = summary.iloc[0]
    assert bool(row["all_stress_gates_passed"]) is True
    assert row["stress_gate_reason"] == "pass"
    assert bool(row["all_metric_gates_passed_excluding_research_role"]) is True
    assert row["metric_stress_gate_reason"] == "pass"
    assert row["Max Stress Turnover Cost Bps"] == 25.0
    assert row["Max Stress Universe Lag Trading Days"] == 42.0
    assert row["recommended_action"] == "stress_live_design_review"


def test_stress_readiness_cli_writes_outputs(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_dynamic_universe().to_csv(universe_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--universe",
            str(universe_path),
            "--output-dir",
            str(output_dir),
            "--start",
            "2021-02-01",
            "--end",
            "2023-05-31",
            "--turnover-cost-bps-values",
            "0,5",
            "--universe-lag-days-values",
            "1,2",
            "--min-adv20-usd-values",
            "1000000",
            "--rolling-window-years",
            "1",
            "--min-history-days",
            "100",
            "--include-panic-rebound-guard-variants",
        ]
    )

    assert exit_code == 0
    detail = pd.read_csv(output_dir / "stress_live_readiness_detail.csv")
    summary = pd.read_csv(output_dir / "stress_live_readiness_summary.csv")
    assert not detail.empty
    assert not summary.empty
