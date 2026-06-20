from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_walk_forward import (
    build_panic_guard_walk_forward_oos,
    main,
    summarize_walk_forward_oos,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=1250)
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
                "start_date": "2020-01-31",
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


def test_build_panic_guard_walk_forward_oos_outputs_windows_and_summary() -> None:
    result = build_panic_guard_walk_forward_oos(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2020-01-02",
        universe_lag_trading_days=1,
        turnover_cost_bps=0.0,
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        blend_top2_weights=(0.50,),
        train_years=1,
        min_oos_windows=1,
        panic_guard_drawdown_threshold=0.10,
        panic_guard_rebound_threshold=0.03,
        panic_guard_vol_threshold=0.25,
        panic_guard_stock_exposure=0.50,
    )

    windows = result["walk_forward_oos_windows"]
    summary = result["walk_forward_oos_summary"]
    assert not windows.empty
    assert not summary.empty
    assert set(windows["Baseline Run"]) == {"blend_top2_50_top4_50"}
    assert set(summary["Baseline Run"]) == {"blend_top2_50_top4_50"}
    assert "walk_forward_gate_passed" in summary.columns


def test_summarize_walk_forward_oos_marks_gate_pass() -> None:
    windows = pd.DataFrame(
        [
            {
                "Pair": "candidate_vs_baseline",
                "Baseline Run": "baseline",
                "Candidate Run": "candidate",
                "train_gate_passed": True,
                "Test Excess CAGR vs Baseline": 0.02,
                "Test Drawdown Delta vs Baseline": 0.0,
                "Test Sharpe Delta vs Baseline": 0.1,
                "Test Turnover Delta vs Baseline": 0.2,
                "oos_win_vs_baseline": True,
            },
            {
                "Pair": "candidate_vs_baseline",
                "Baseline Run": "baseline",
                "Candidate Run": "candidate",
                "train_gate_passed": True,
                "Test Excess CAGR vs Baseline": 0.01,
                "Test Drawdown Delta vs Baseline": -0.01,
                "Test Sharpe Delta vs Baseline": 0.05,
                "Test Turnover Delta vs Baseline": 0.1,
                "oos_win_vs_baseline": True,
            },
        ]
    )

    summary = summarize_walk_forward_oos(windows, train_years=1, min_oos_windows=2)
    row = summary.iloc[0]
    assert bool(row["walk_forward_gate_passed"]) is True
    assert row["walk_forward_gate_reason"] == "pass"
    assert row["recommended_action"] == "walk_forward_live_design_review"


def test_walk_forward_cli_writes_outputs(tmp_path) -> None:
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
            "2020-01-02",
            "--universe-lag-days",
            "1",
            "--turnover-cost-bps",
            "0",
            "--min-adv20-usd",
            "1000000",
            "--min-history-days",
            "100",
            "--blend-top2-weights",
            "0.5",
            "--train-years",
            "1",
            "--min-oos-windows",
            "1",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "walk_forward_oos_windows.csv").exists()
    assert (output_dir / "walk_forward_oos_summary.csv").exists()
