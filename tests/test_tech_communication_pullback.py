from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.tech_communication_pullback import build_artifacts


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=320)
    symbols = {
        "QQQ": 100.0,
        "SPY": 100.0,
        "BOXX": 100.0,
        "AAPL": 120.0,
        "MSFT": 110.0,
        "META": 90.0,
        "JPM": 80.0,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, base in symbols.items():
            trend = 1.0 + idx * (0.001 if symbol in {"AAPL", "MSFT", "META", "QQQ", "SPY"} else 0.0002)
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": base * trend,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology"},
            {"symbol": "MSFT", "sector": "Information Technology"},
            {"symbol": "META", "sector": "Communication"},
            {"symbol": "JPM", "sector": "Financials"},
        ]
    )


def test_builds_tech_pullback_artifact_contract(tmp_path) -> None:
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
        use_default_config=False,
    )

    assert result.snapshot_path.exists()
    assert result.manifest_path.exists()
    assert result.ranking_path.exists()
    assert result.release_summary_path.exists()

    snapshot = pd.read_csv(result.snapshot_path)
    ranking = pd.read_csv(result.ranking_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    summary = json.loads(result.release_summary_path.read_text(encoding="utf-8"))

    assert set(snapshot["symbol"]) >= {"AAPL", "MSFT", "META", "QQQ", "SPY", "BOXX"}
    assert "JPM" not in set(snapshot["symbol"])
    assert not ranking.empty
    assert manifest["strategy_profile"] == "tech_communication_pullback_enhancement"
    assert manifest["contract_version"] == "tech_communication_pullback_enhancement.feature_snapshot.v1"
    assert manifest["row_count"] == len(snapshot)
    assert manifest["source_project"] == "UsEquitySnapshotPipelines"
    assert summary["release_status"] == "ready"
