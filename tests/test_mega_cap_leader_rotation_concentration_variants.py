from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants import (
    main,
    run_concentration_variant_research,
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


def test_concentration_variant_research_builds_blend_and_dynamic_tables() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(0.10,),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
    )

    summary = result["concentration_variant_summary"]
    yearly = result["concentration_variant_yearly_summary"]
    rolling = result["concentration_variant_rolling_summary"]
    mode_history = result["concentration_variant_mode_history"]
    assert {
        "base_top2_cap50",
        "base_top4_cap25",
        "blend_top2_50_top4_50",
        "dynamic_top2_dd10_to_top4",
    }.issubset(set(summary["Run"]))
    assert {"CAGR", "Max Drawdown", "Top4 Mode Share"}.issubset(summary.columns)
    assert {"Strategy Return", "QQQ Return", "SPY Return"}.issubset(yearly.columns)
    assert {"Window Years", "Strategy CAGR", "QQQ CAGR"}.issubset(rolling.columns)
    assert not rolling.empty
    assert not mode_history.empty


def test_concentration_variant_research_cli_writes_outputs(tmp_path) -> None:
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
            "--blend-top2-weights",
            "0.5",
            "--dynamic-drawdown-thresholds",
            "0.1",
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
    assert (output_dir / "concentration_variant_summary.csv").exists()
    assert (output_dir / "concentration_variant_yearly_summary.csv").exists()
    assert (output_dir / "concentration_variant_rolling_summary.csv").exists()
    assert (output_dir / "concentration_variant_mode_history.csv").exists()
    summary = pd.read_csv(output_dir / "concentration_variant_summary.csv")
    assert "blend_top2_50_top4_50" in set(summary["Run"])
