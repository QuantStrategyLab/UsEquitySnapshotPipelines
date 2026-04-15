from __future__ import annotations

import json

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.dynamic_mega_leveraged_pullback import build_artifacts, build_feature_snapshot


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=260)
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol in ("QQQ", "BOXX", "NVDA", "MSFT", "AAPL", "AMZN"):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": 100.0 + idx * 0.1,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1},
            {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
            {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3},
            {"symbol": "AMZN", "sector": "Consumer Discretionary", "mega_rank": 4},
        ]
    )


def _product_map() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"underlying_symbol": "NVDA", "trade_symbol": "NVDL", "product_leverage": 2.0, "product_available": True},
            {"underlying_symbol": "MSFT", "trade_symbol": "MSFU", "product_leverage": 2.0, "product_available": True},
            {"underlying_symbol": "AAPL", "trade_symbol": "AAPU", "product_leverage": 2.0, "product_available": True},
            {"underlying_symbol": "AMZN", "trade_symbol": "AMZU", "product_leverage": 2.0, "product_available": True},
        ]
    )


def test_builds_dynamic_mega_leveraged_pullback_candidate_snapshot(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    product_map_path = tmp_path / "product_map.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_universe().to_csv(universe_path, index=False)
    _product_map().to_csv(product_map_path, index=False)

    result = build_artifacts(
        prices_path=prices_path,
        universe_path=universe_path,
        product_map_path=product_map_path,
        output_dir=output_dir,
        as_of_date="2025-12-31",
        candidate_universe_size=3,
    )

    snapshot = pd.read_csv(result.snapshot_path)
    ranking = pd.read_csv(result.ranking_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    summary = json.loads(result.release_summary_path.read_text(encoding="utf-8"))

    assert list(snapshot["underlying_symbol"]) == ["NVDA", "MSFT", "AAPL"]
    assert list(snapshot["symbol"]) == ["NVDL", "MSFU", "AAPU"]
    assert snapshot["product_available"].all()
    assert list(ranking["candidate_rank"]) == [1, 2, 3]
    assert manifest["strategy_profile"] == "dynamic_mega_leveraged_pullback"
    assert manifest["contract_version"] == "dynamic_mega_leveraged_pullback.feature_snapshot.v1"
    assert manifest["config_sha256"]
    assert summary["release_status"] == "ready"


def test_dynamic_mega_leveraged_pullback_requires_product_map(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_universe().to_csv(universe_path, index=False)

    with pytest.raises(ValueError, match="product_map_path is required"):
        build_artifacts(
            prices_path=prices_path,
            universe_path=universe_path,
            output_dir=output_dir,
            as_of_date="2025-12-31",
            candidate_universe_size=3,
        )


def test_dynamic_mega_leveraged_pullback_does_not_fallback_to_underlying() -> None:
    product_map = _product_map().loc[lambda frame: frame["underlying_symbol"].ne("AAPL")]

    snapshot = build_feature_snapshot(
        price_history=_sample_prices(),
        universe_snapshot=_sample_universe(),
        product_map=product_map,
        as_of_date="2025-12-31",
        candidate_universe_size=3,
    )

    aapl = snapshot.loc[snapshot["underlying_symbol"].eq("AAPL")].iloc[0]
    assert aapl["symbol"] != "AAPL"
    assert not bool(aapl["product_available"])
    assert not bool(aapl["eligible"])


def test_dynamic_mega_leveraged_pullback_validates_two_times_product_map() -> None:
    product_map = _product_map()
    product_map.loc[product_map["underlying_symbol"].eq("NVDA"), "product_leverage"] = 1.5

    with pytest.raises(ValueError, match="2x long products"):
        build_feature_snapshot(
            price_history=_sample_prices(),
            universe_snapshot=_sample_universe(),
            product_map=product_map,
            as_of_date="2025-12-31",
            candidate_universe_size=3,
        )
