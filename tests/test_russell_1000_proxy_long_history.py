from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.russell_1000_proxy_long_history import (
    build_proxy_universe_history,
    main,
)


def _price_rows(
    symbols: dict[str, dict[str, object]],
    *,
    start: str = "2020-01-02",
    periods: int = 70,
) -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range(start, periods=periods)
    for idx, as_of in enumerate(dates):
        for symbol, config in symbols.items():
            market_value = config.get("market_value", 100.0)
            if callable(market_value):
                market_value = market_value(as_of, idx)
            rows.append(
                {
                    "symbol": symbol,
                    "sector": config.get("sector", "unknown"),
                    "as_of": as_of.date().isoformat(),
                    "close": float(config.get("close", 50.0)) + idx * 0.01,
                    "volume": int(config.get("volume", 1_000_000)),
                    "market_value": float(market_value),
                }
            )
    return pd.DataFrame(rows)


def test_proxy_universe_uses_point_in_time_market_value_without_future_leakage() -> None:
    prices = _price_rows(
        {
            "AAA": {"sector": "Technology", "market_value": 1_000.0},
            "BBB": {"sector": "Financials", "market_value": 800.0},
            "CCC": {
                "sector": "Health Care",
                "market_value": lambda as_of, _idx: 2_000.0 if as_of >= pd.Timestamp("2020-02-03") else 100.0,
            },
            "QQQ": {"sector": "benchmark", "market_value": 99_000.0},
            "SPY": {"sector": "benchmark", "market_value": 99_000.0},
            "BOXX": {"sector": "cash", "market_value": 99_000.0},
        },
        periods=70,
    )

    result = build_proxy_universe_history(
        prices,
        universe_size=2,
        min_price_usd=1.0,
        min_adv20_usd=0.0,
        min_history_days=5,
    )

    universe = result.universe_history
    first_start = universe["start_date"].min()
    first_active = universe.loc[universe["start_date"].eq(first_start), "symbol"].tolist()
    second_start = sorted(universe["start_date"].drop_duplicates())[1]
    second_active = universe.loc[universe["start_date"].eq(second_start), "symbol"].tolist()

    assert result.metadata["proxy_method"] == "point_in_time_market_value"
    assert first_start == pd.Timestamp("2020-02-03")
    assert first_active == ["AAA", "BBB"]
    assert second_active[0] == "CCC"
    assert {"QQQ", "SPY", "BOXX"}.isdisjoint(set(universe["symbol"]))
    assert (universe["start_date"] > universe["rank_as_of"]).all()


def test_proxy_universe_falls_back_to_adv20_when_market_value_is_missing() -> None:
    prices = _price_rows(
        {
            "HIGH": {"volume": 2_000_000},
            "MID": {"volume": 1_000_000},
            "LOW": {"volume": 100_000},
            "QQQ": {"volume": 5_000_000},
        },
        periods=35,
    ).drop(columns=["market_value"])

    result = build_proxy_universe_history(
        prices,
        universe_size=2,
        min_price_usd=1.0,
        min_adv20_usd=0.0,
        min_history_days=5,
        excluded_symbols=("QQQ",),
    )

    first_start = result.universe_history["start_date"].min()
    active = result.universe_history.loc[result.universe_history["start_date"].eq(first_start), "symbol"].tolist()

    assert result.metadata["proxy_method"] == "adv20_liquidity_proxy"
    assert result.metadata["ranking_column"] == "adv20_usd"
    assert active == ["HIGH", "MID"]


def test_proxy_research_cli_writes_proxy_outputs_without_validation(tmp_path) -> None:
    prices = _price_rows(
        {
            "AAA": {"market_value": 1_000.0},
            "BBB": {"market_value": 800.0},
            "QQQ": {"market_value": 99_000.0},
        },
        periods=35,
    )
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    prices.to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--output-dir",
            str(output_dir),
            "--skip-validation",
            "--universe-size",
            "2",
            "--min-price-usd",
            "1",
            "--min-adv20-usd",
            "0",
            "--min-history-days",
            "5",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "russell_1000_proxy_universe_history.csv").exists()
    assert (output_dir / "russell_1000_proxy_metadata.csv").exists()
