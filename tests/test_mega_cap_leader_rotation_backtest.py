from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_backtest import (
    BACKTEST_SUMMARY_COLUMNS,
    build_static_universe,
    main,
    run_backtest,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-03", periods=520)
    trends = {
        "QQQ": 0.0010,
        "SPY": 0.0006,
        "BOXX": 0.0001,
        "AAPL": 0.0011,
        "MSFT": 0.0015,
        "NVDA": 0.0030,
        "AMZN": 0.0012,
        "GOOGL": 0.0008,
        "META": 0.0020,
        "TSLA": -0.0002,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (80.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_run_backtest_builds_research_outputs() -> None:
    result = run_backtest(
        _sample_prices(),
        build_static_universe("mag7"),
        start_date="2024-06-03",
        end_date="2024-12-31",
        pool_name="mag7",
        min_adv20_usd=1_000_000.0,
        turnover_cost_bps=0.0,
    )

    summary = result["summary"]
    assert set(BACKTEST_SUMMARY_COLUMNS) <= set(summary)
    assert summary["Strategy"] == "mega_cap_leader_rotation"
    assert summary["Pool"] == "mag7"
    assert summary["Benchmark Symbol"] == "QQQ"
    assert not result["weights_history"].empty
    assert not result["candidate_scores"].empty
    assert not result["trades"].empty
    assert {"QQQ", "SPY", "equal_weight_mag7"} <= set(result["reference_returns"].columns)
    assert result["exposure_history"]["selected_symbols"].astype(str).str.contains("NVDA").any()


def test_cli_writes_backtest_artifacts(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    build_static_universe("mag7").to_csv(universe_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--universe",
            str(universe_path),
            "--pool",
            "mag7",
            "--start",
            "2024-06-03",
            "--end",
            "2024-12-31",
            "--min-adv20-usd",
            "1000000",
            "--turnover-cost-bps",
            "0",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    for name in (
        "summary.csv",
        "portfolio_returns.csv",
        "weights_history.csv",
        "turnover_history.csv",
        "candidate_scores.csv",
        "trades.csv",
        "exposure_history.csv",
        "reference_returns.csv",
    ):
        assert (output_dir / name).exists()
