from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_frequency_risk_research import (
    build_rebalance_dates,
    main,
    run_frequency_risk_research,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2021-01-04", periods=760)
    trends = {
        "QQQ": 0.0007,
        "SPY": 0.0004,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0014,
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
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2022-01-31",
                "end_date": "2022-12-30",
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("AAPL", "Information Technology"),
                    ("XOM", "Energy"),
                    ("LLY", "Health Care"),
                ],
                start=1,
            )
        ]
        + [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2023-01-31",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("AMZN", "Consumer Discretionary"),
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("LLY", "Health Care"),
                    ("XOM", "Energy"),
                ],
                start=1,
            )
        ]
    )


def test_build_rebalance_dates_supports_research_frequencies() -> None:
    index = pd.bdate_range("2024-01-02", periods=45)
    monthly = build_rebalance_dates(index, "monthly")
    weekly = build_rebalance_dates(index, "weekly")
    biweekly = build_rebalance_dates(index, "biweekly")

    assert 1 <= len(monthly) < len(biweekly) < len(weekly)
    assert pd.Timestamp("2024-02-29") in monthly


def test_frequency_risk_research_builds_frequency_and_daily_risk_tables() -> None:
    result = run_frequency_risk_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        rebalance_frequencies=("monthly", "weekly"),
        daily_risk_modes=("none", "hard_cash"),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
    )

    summary = result["frequency_risk_summary"]
    yearly = result["frequency_risk_yearly_summary"]
    rolling = result["frequency_risk_rolling_summary"]
    daily_history = result["frequency_risk_daily_history"]
    assert len(summary) == 4
    assert set(summary["Rebalance Frequency"]) == {"monthly", "weekly"}
    assert set(summary["Daily Risk Mode"]) == {"none", "hard_cash"}
    assert {"Strategy Return", "QQQ Return", "SPY Return"}.issubset(yearly.columns)
    assert {"Window Years", "Strategy CAGR", "QQQ CAGR"}.issubset(rolling.columns)
    assert not rolling.empty
    assert not daily_history.empty


def test_frequency_risk_research_cli_writes_outputs(tmp_path) -> None:
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
            "2022-01-03",
            "--end",
            "2023-11-30",
            "--universe-lag-days",
            "1",
            "--rebalance-frequencies",
            "monthly,weekly",
            "--daily-risk-modes",
            "none,hard_cash",
            "--rolling-window-years",
            "1",
            "--min-adv20-usd",
            "1000000",
            "--min-history-days",
            "100",
            "--turnover-cost-bps",
            "0",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "frequency_risk_summary.csv").exists()
    assert (output_dir / "frequency_risk_yearly_summary.csv").exists()
    assert (output_dir / "frequency_risk_rolling_summary.csv").exists()
    assert (output_dir / "frequency_risk_daily_history.csv").exists()
    summary = pd.read_csv(output_dir / "frequency_risk_summary.csv")
    assert "blend50_monthly_none" in set(summary["Run"])
