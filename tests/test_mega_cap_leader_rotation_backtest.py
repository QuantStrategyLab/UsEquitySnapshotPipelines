from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_backtest import (
    BACKTEST_SUMMARY_COLUMNS,
    _normalize_price_history,
    _precompute_symbol_feature_history,
    build_dynamic_mega_universe_history,
    build_feature_snapshot_for_backtest,
    build_static_universe,
    build_target_weights,
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


def test_build_target_weights_lowers_top_n_for_small_accounts() -> None:
    prices = _normalize_price_history(_sample_prices())
    feature_history = _precompute_symbol_feature_history(prices)
    snapshot = build_feature_snapshot_for_backtest(
        prices["as_of"].max(),
        build_static_universe("mag7"),
        feature_history,
        min_adv20_usd=1_000_000.0,
    )

    _weights, _ranked, metadata = build_target_weights(
        snapshot,
        top_n=4,
        single_name_cap=0.50,
        portfolio_total_equity=5_000.0,
        min_position_value_usd=2_000.0,
    )

    assert metadata["requested_top_n"] == 4
    assert metadata["effective_top_n"] == 2
    assert len(metadata["selected_symbols"]) == 2


def test_build_target_weights_can_limit_selected_names_per_sector() -> None:
    base = {
        "eligible": True,
        "close": 100.0,
        "adv20_usd": 100_000_000.0,
        "mom_12_1": 0.20,
        "high_252_gap": 0.0,
        "sma200_gap": 0.20,
        "vol_63": 0.30,
        "maxdd_126": -0.10,
    }
    snapshot = pd.DataFrame(
        [
            {
                **base,
                "symbol": "NVDA",
                "sector": "Information Technology",
                "mom_3m": 0.30,
                "mom_6m": 0.50,
                "rel_mom_6m_vs_benchmark": 0.20,
                "rel_mom_6m_vs_broad_benchmark": 0.25,
            },
            {
                **base,
                "symbol": "MSFT",
                "sector": "Information Technology",
                "mom_3m": 0.25,
                "mom_6m": 0.45,
                "rel_mom_6m_vs_benchmark": 0.15,
                "rel_mom_6m_vs_broad_benchmark": 0.20,
            },
            {
                **base,
                "symbol": "LLY",
                "sector": "Health Care",
                "mom_3m": 0.18,
                "mom_6m": 0.35,
                "rel_mom_6m_vs_benchmark": 0.10,
                "rel_mom_6m_vs_broad_benchmark": 0.15,
            },
            {
                **base,
                "symbol": "QQQ",
                "sector": "benchmark",
                "eligible": False,
                "mom_3m": 0.10,
                "mom_6m": 0.30,
                "rel_mom_6m_vs_benchmark": 0.0,
                "rel_mom_6m_vs_broad_benchmark": 0.0,
            },
        ]
    )

    weights, _ranked, metadata = build_target_weights(
        snapshot,
        top_n=2,
        single_name_cap=0.50,
        max_names_per_sector=1,
    )

    assert metadata["selected_symbols"] == ("NVDA", "LLY")
    assert weights["NVDA"] == 0.50
    assert weights["LLY"] == 0.50


def test_build_dynamic_mega_universe_history_ranks_each_snapshot() -> None:
    snapshots = [
        (
            pd.Timestamp("2024-01-31"),
            pd.DataFrame(
                [
                    {"symbol": "AAPL", "sector": "Information Technology", "weight": 5.0},
                    {"symbol": "GOOG", "sector": "Communication Services", "weight": 4.8},
                    {"symbol": "GOOGL", "sector": "Communication Services", "weight": 4.7},
                    {"symbol": "MSFT", "sector": "Information Technology", "weight": 4.0},
                    {"symbol": "XOM", "sector": "Energy", "weight": 1.0},
                ]
            ),
        ),
        (
            pd.Timestamp("2024-02-29"),
            pd.DataFrame(
                [
                    {"symbol": "AAPL", "sector": "Information Technology", "weight": 4.0},
                    {"symbol": "MSFT", "sector": "Information Technology", "weight": 5.0},
                    {"symbol": "XOM", "sector": "Energy", "weight": 2.0},
                ]
            ),
        ),
    ]

    history = build_dynamic_mega_universe_history(snapshots, universe_size=3)

    first = history.loc[history["start_date"] == pd.Timestamp("2024-01-31")]
    second = history.loc[history["start_date"] == pd.Timestamp("2024-02-29")]
    assert first["symbol"].tolist() == ["AAPL", "GOOG", "MSFT"]
    assert second["symbol"].tolist() == ["MSFT", "AAPL", "XOM"]
    assert "GOOGL" not in set(first["symbol"])
    assert first["end_date"].iloc[0] == pd.Timestamp("2024-02-28")
    assert pd.isna(second["end_date"].iloc[0])


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
