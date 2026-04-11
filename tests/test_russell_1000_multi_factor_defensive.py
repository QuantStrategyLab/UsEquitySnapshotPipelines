from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.russell_1000_multi_factor_defensive import build_artifacts


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=320)
    symbols = {
        "SPY": 100.0,
        "AAPL": 120.0,
        "MSFT": 110.0,
        "JPM": 80.0,
        "XOM": 75.0,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, base in symbols.items():
            multiplier = 0.0012 if symbol in {"AAPL", "MSFT"} else 0.0006 if symbol == "SPY" else 0.0003
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": base * (1.0 + idx * multiplier),
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology"},
            {"symbol": "MSFT", "sector": "Information Technology"},
            {"symbol": "JPM", "sector": "Financials"},
            {"symbol": "XOM", "sector": "Energy"},
        ]
    )


def _sample_universe_history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "sector": "Information Technology",
                "start_date": "2024-01-02",
                "end_date": None,
            },
            {
                "symbol": "MSFT",
                "sector": "Information Technology",
                "start_date": "2024-01-02",
                "end_date": None,
            },
            {
                "symbol": "JPM",
                "sector": "Financials",
                "start_date": "2024-01-02",
                "end_date": "2024-12-31",
            },
            {
                "symbol": "XOM",
                "sector": "Energy",
                "start_date": "2025-01-01",
                "end_date": None,
            },
        ]
    )


def test_builds_russell_artifact_contract(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_universe().to_csv(universe_path, index=False)

    result = build_artifacts(
        prices_path=prices_path,
        universe_path=universe_path,
        output_dir=output_dir,
        as_of_date="2025-03-24",
        min_adv20_usd=1_000_000.0,
    )

    assert result.snapshot_path.exists()
    assert result.manifest_path.exists()
    assert result.ranking_path.exists()
    assert result.release_summary_path.exists()

    snapshot = pd.read_csv(result.snapshot_path)
    ranking = pd.read_csv(result.ranking_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert set(snapshot["symbol"]) >= {"AAPL", "MSFT", "JPM", "XOM", "SPY"}
    assert not ranking.empty
    assert manifest["strategy_profile"] == "russell_1000_multi_factor_defensive"
    assert manifest["contract_version"] == "russell_1000_multi_factor_defensive.feature_snapshot.v1"
    assert manifest["row_count"] == len(snapshot)


def test_builds_russell_with_universe_history_without_explicit_as_of(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_universe_history().to_csv(universe_path, index=False)

    result = build_artifacts(
        prices_path=prices_path,
        universe_path=universe_path,
        output_dir=output_dir,
        min_adv20_usd=1_000_000.0,
    )

    assert result.snapshot_path.exists()
    snapshot = pd.read_csv(result.snapshot_path)
    assert snapshot["as_of"].nunique() == 1
    assert snapshot["as_of"].iloc[0] == "2025-03-24"
    assert "JPM" not in set(snapshot["symbol"])
    assert {"AAPL", "MSFT", "XOM", "SPY"} <= set(snapshot["symbol"])
