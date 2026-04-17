from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_dynamic_validation import (
    ValidationConfig,
    lag_universe_history,
    main,
    run_dynamic_universe_validation,
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
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (90.0 + offset * 7.0) * ((1.0 + trend) ** idx),
                    "volume": 2_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "NVDA",
                "sector": "Information Technology",
                "start_date": "2022-01-31",
                "end_date": "2022-12-30",
                "mega_rank": 1,
            },
            {
                "symbol": "MSFT",
                "sector": "Information Technology",
                "start_date": "2022-01-31",
                "end_date": "2022-12-30",
                "mega_rank": 2,
            },
            {
                "symbol": "AAPL",
                "sector": "Information Technology",
                "start_date": "2022-01-31",
                "end_date": "2022-12-30",
                "mega_rank": 3,
            },
            {
                "symbol": "AMZN",
                "sector": "Consumer Discretionary",
                "start_date": "2023-01-31",
                "end_date": None,
                "mega_rank": 1,
            },
            {
                "symbol": "NVDA",
                "sector": "Information Technology",
                "start_date": "2023-01-31",
                "end_date": None,
                "mega_rank": 2,
            },
            {
                "symbol": "MSFT",
                "sector": "Information Technology",
                "start_date": "2023-01-31",
                "end_date": None,
                "mega_rank": 3,
            },
        ]
    )


def test_lag_universe_history_shifts_start_and_end_by_trading_sessions() -> None:
    prices = _sample_prices()
    trading_index = pd.DatetimeIndex(sorted(pd.to_datetime(prices["as_of"]).unique()))
    lagged = lag_universe_history(
        _sample_dynamic_universe(),
        lag_trading_days=1,
        trading_index=trading_index,
    )

    assert lagged["start_date"].min() == pd.Timestamp("2022-02-01")
    first_end = lagged.loc[lagged["symbol"].eq("NVDA"), "end_date"].iloc[0]
    assert first_end == pd.Timestamp("2023-01-02")


def test_run_dynamic_universe_validation_builds_lag_and_yearly_tables() -> None:
    result = run_dynamic_universe_validation(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-06-01",
        end_date="2023-11-30",
        universe_lag_trading_days=(0, 1),
        validation_configs=(
            ValidationConfig(name="top2_cap50", top_n=2, single_name_cap=0.50),
            ValidationConfig(name="top3_cap35", top_n=3, single_name_cap=0.35),
        ),
        min_adv20_usd=1_000_000.0,
        turnover_cost_bps=0.0,
    )

    summary = result["validation_summary"]
    yearly = result["yearly_validation_summary"]
    assert len(summary) == 4
    assert set(summary["Universe Lag Trading Days"]) == {0, 1}
    assert set(summary["Config"]) == {"top2_cap50", "top3_cap35"}
    assert {"Strategy Return", "QQQ Return", "SPY Return"}.issubset(yearly.columns)
    assert not yearly.empty


def test_dynamic_universe_validation_cli_writes_outputs(tmp_path) -> None:
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
            "2022-06-01",
            "--end",
            "2023-11-30",
            "--universe-lag-days",
            "0,1",
            "--strategy-configs",
            "top2_cap50:2:0.50",
            "--min-adv20-usd",
            "1000000",
            "--turnover-cost-bps",
            "0",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "validation_summary.csv").exists()
    assert (output_dir / "yearly_validation_summary.csv").exists()
    summary = pd.read_csv(output_dir / "validation_summary.csv")
    assert len(summary) == 2
