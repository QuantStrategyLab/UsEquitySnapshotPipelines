from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_dynamic_top20 import build_artifacts


AGGRESSIVE_PROFILE = "mega_cap_leader_rotation_aggressive"


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=320)
    symbols = {
        "QQQ": (100.0, 0.0010),
        "SPY": (100.0, 0.0007),
        "BOXX": (100.0, 0.0002),
        "NVDA": (80.0, 0.0018),
        "MSFT": (110.0, 0.0012),
        "AAPL": (120.0, 0.0010),
        "META": (90.0, 0.0014),
        "AMZN": (95.0, 0.0011),
        "TSLA": (75.0, 0.0001),
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, (base, slope) in symbols.items():
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": base * (1.0 + idx * slope),
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_ranked_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1},
            {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
            {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3},
            {"symbol": "META", "sector": "Communication Services", "mega_rank": 4},
            {"symbol": "AMZN", "sector": "Consumer Discretionary", "mega_rank": 5},
            {"symbol": "TSLA", "sector": "Consumer Discretionary", "mega_rank": 6},
        ]
    )


def test_builds_mega_cap_dynamic_top20_artifact_contract(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_ranked_universe().to_csv(universe_path, index=False)

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
    summary = json.loads(result.release_summary_path.read_text(encoding="utf-8"))

    assert {"QQQ", "SPY", "BOXX", "NVDA", "MSFT", "AAPL", "META"} <= set(snapshot["symbol"])
    assert not ranking.empty
    assert "SPY" not in set(ranking["symbol"])
    assert manifest["strategy_profile"] == "mega_cap_leader_rotation_dynamic_top20"
    assert manifest["contract_version"] == "mega_cap_leader_rotation_dynamic_top20.feature_snapshot.v1"
    assert manifest["row_count"] == len(snapshot)
    assert summary["release_status"] == "ready"


def test_builds_mega_cap_aggressive_artifact_contract(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_ranked_universe().to_csv(universe_path, index=False)

    result = build_artifacts(
        profile=AGGRESSIVE_PROFILE,
        prices_path=prices_path,
        universe_path=universe_path,
        output_dir=output_dir,
        as_of_date="2025-03-24",
        min_adv20_usd=1_000_000.0,
        dynamic_universe_size=50,
        holdings_count=4,
        soft_defense_exposure=1.0,
        hard_defense_exposure=1.0,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    summary = json.loads(result.release_summary_path.read_text(encoding="utf-8"))

    assert result.snapshot_path.name == "mega_cap_leader_rotation_aggressive_feature_snapshot_latest.csv"
    assert manifest["strategy_profile"] == AGGRESSIVE_PROFILE
    assert manifest["contract_version"] == "mega_cap_leader_rotation_aggressive.feature_snapshot.v1"
    assert summary["strategy_profile"] == AGGRESSIVE_PROFILE
    assert summary["release_status"] == "ready"


def test_build_rejects_unranked_large_universe(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    _sample_prices().to_csv(prices_path, index=False)
    pd.DataFrame(
        {"symbol": [f"S{i}" for i in range(25)], "sector": ["unknown"] * 25}
    ).to_csv(universe_path, index=False)

    try:
        build_artifacts(
            prices_path=prices_path,
            universe_path=universe_path,
            output_dir=tmp_path / "output",
            as_of_date="2025-03-24",
            min_adv20_usd=1_000_000.0,
        )
    except ValueError as exc:
        assert "requires mega_rank" in str(exc)
    else:
        raise AssertionError("expected ValueError")
