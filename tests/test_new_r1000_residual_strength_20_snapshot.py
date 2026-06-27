from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.new_r1000_residual_strength_20_snapshot import build_artifacts


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=320)
    symbols = {
        "SPY": (100.0, 0.0008),
        "BOXX": (100.0, 0.0002),
        "NVDA": (80.0, 0.0018),
        "AVGO": (70.0, 0.0016),
        "META": (90.0, 0.0013),
        "GOOGL": (95.0, 0.0011),
        "LLY": (85.0, 0.0010),
        "JPM": (75.0, 0.0009),
        "XOM": (65.0, 0.0007),
        "CAT": (60.0, 0.0008),
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, (base, slope) in symbols.items():
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": base * (1.0 + idx * slope),
                    "volume": 1_500_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology"},
            {"symbol": "AVGO", "sector": "Information Technology"},
            {"symbol": "META", "sector": "Communication Services"},
            {"symbol": "GOOGL", "sector": "Communication Services"},
            {"symbol": "LLY", "sector": "Health Care"},
            {"symbol": "JPM", "sector": "Financials"},
            {"symbol": "XOM", "sector": "Energy"},
            {"symbol": "CAT", "sector": "Industrials"},
        ]
    )


def test_builds_new_r1000_residual_strength_20_runtime_artifacts(tmp_path) -> None:
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
        current_holdings=("NVDA",),
        min_adv20_usd=1_000_000.0,
    )

    snapshot = pd.read_csv(result.snapshot_path)
    ranking = pd.read_csv(result.ranking_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    summary = json.loads(result.release_summary_path.read_text(encoding="utf-8"))

    assert result.snapshot_path.name == "new_r1000_residual_strength_20_feature_snapshot_latest.csv"
    assert manifest["strategy_profile"] == "new_r1000_residual_strength_20"
    assert manifest["contract_version"] == "new_r1000_residual_strength_20.feature_snapshot.v1"
    assert summary["strategy_profile"] == "new_r1000_residual_strength_20"
    assert summary["release_status"] == "ready"
    assert {"SPY", "BOXX", "NVDA", "META"} <= set(snapshot["symbol"])
    assert not ranking.empty
    assert "selected" in ranking.columns
    assert "target_weight" in ranking.columns
    assert ranking["selected"].astype(bool).any()
    assert len(result.selected_symbols) >= 1


def test_build_rejects_as_of_later_than_price_history(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_universe().to_csv(universe_path, index=False)

    try:
        build_artifacts(
            prices_path=prices_path,
            universe_path=universe_path,
            output_dir=tmp_path / "output",
            as_of_date="2025-03-31",
            min_adv20_usd=1_000_000.0,
        )
    except ValueError as exc:
        assert "cannot be later than latest price history row" in str(exc)
    else:
        raise AssertionError("expected ValueError")
