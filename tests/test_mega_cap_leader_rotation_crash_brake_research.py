from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research import (
    build_panic_brake_mode_history,
    main,
    run_crash_brake_research,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=900)
    trends = {
        "SPY": 0.00035,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0012,
        "AMZN": 0.0008,
        "LLY": 0.0011,
        "XOM": 0.0007,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        if idx < 500:
            qqq_multiplier = (1.0006) ** idx
        elif idx < 540:
            qqq_multiplier = (1.0006**500) * (1.0 - 0.18 * ((idx - 500) / 40.0))
        else:
            qqq_multiplier = (1.0006**500) * 0.82 * (1.003 ** (idx - 540))
        rows.append(
            {
                "symbol": "QQQ",
                "as_of": as_of.date().isoformat(),
                "close": 120.0 * qqq_multiplier,
                "volume": 3_000_000,
            }
        )
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (85.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 2_500_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2021-01-29",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("AAPL", "Information Technology"),
                    ("LLY", "Health Care"),
                    ("AMZN", "Consumer Discretionary"),
                    ("XOM", "Energy"),
                ],
                start=1,
            )
        ]
    )


def test_build_panic_brake_mode_history_triggers_floor_after_panic_rebound() -> None:
    dates = pd.bdate_range("2020-01-02", periods=620)
    closes = pd.Series(100.0, index=dates)
    closes.iloc[:450] = [100.0 * (1.0005**idx) for idx in range(450)]
    closes.iloc[450:500] = closes.iloc[449] * (1.0 - 0.30 * (pd.RangeIndex(50) / 50.0))
    closes.iloc[500:] = [closes.iloc[499] * (1.003**idx) for idx in range(120)]
    returns = closes.pct_change(fill_method=None).fillna(0.0)
    exposure = pd.DataFrame(
        [
            {"signal_date": dates[525].date().isoformat(), "effective_date": dates[526].date().isoformat()},
            {"signal_date": dates[560].date().isoformat(), "effective_date": dates[561].date().isoformat()},
        ]
    )

    weights, mode_history = build_panic_brake_mode_history(
        dates,
        benchmark_close=closes,
        benchmark_returns=returns,
        exposure_history=exposure,
        baseline_top2_weight=0.50,
        floor_top2_weight=0.25,
        drawdown_threshold=0.08,
    )

    assert not mode_history.empty
    assert "floor" in set(mode_history["Mode"])
    floor_row = mode_history.loc[mode_history["Mode"].eq("floor")].iloc[0]
    assert floor_row["Top2 Weight"] == 0.25
    assert weights.loc[pd.Timestamp(floor_row["Effective Date"])] == 0.25


def test_run_crash_brake_research_builds_fixed_and_brake_candidates() -> None:
    result = run_crash_brake_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2021-02-01",
        end_date="2023-05-31",
        universe_lag_trading_days=1,
        rolling_window_years=(1,),
        min_history_days=100,
        min_adv20_usd=1_000_000.0,
        turnover_cost_bps=0.0,
    )

    summary = result["crash_brake_summary"]
    rolling = result["crash_brake_rolling_summary"]
    mode = result["crash_brake_mode_history"]
    assert {
        "blend_top2_50_top4_50_no_brake",
        "crash_brake_top2_50_floor25",
        "blend_top2_25_top4_75_no_brake",
    } == set(summary["Run"])
    assert {"Panic Brake Mode Share", "Top2 Floor Weight"}.issubset(summary.columns)
    assert not rolling.empty
    assert not mode.empty


def test_crash_brake_research_cli_writes_outputs(tmp_path) -> None:
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
            "--universe-lag-days",
            "1",
            "--rolling-window-years",
            "1",
            "--min-history-days",
            "100",
            "--min-adv20-usd",
            "1000000",
            "--turnover-cost-bps",
            "0",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "crash_brake_summary.csv").exists()
    assert (output_dir / "crash_brake_yearly_summary.csv").exists()
    assert (output_dir / "crash_brake_rolling_summary.csv").exists()
    assert (output_dir / "crash_brake_mode_history.csv").exists()
