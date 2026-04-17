from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mag7_leveraged_pullback_backtest import (
    DYNAMIC_PROFILE,
    PROFILE,
    REBOUND_BUDGET_STRATEGY_SUFFIX,
    RETURN_MODE_MARGIN_STOCK,
    _normalize_universe_history,
    build_target_weights,
    main,
    rank_candidates,
    resolve_active_candidate_symbols,
    run_backtest,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2021-01-04", periods=760)
    trends = {
        "QQQ": 0.0007,
        "SPY": 0.0004,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0016,
        "AMZN": 0.0008,
        "GOOGL": 0.0007,
        "META": 0.0012,
        "TSLA": 0.0006,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            cyclical_pullback = 1.0 - (0.10 if idx % 90 in range(35, 45) and symbol in {"NVDA", "META"} else 0.0)
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (90.0 + offset * 7.0) * ((1.0 + trend) ** idx) * cyclical_pullback,
                    "volume": 2_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "start_date": "2022-01-31", "end_date": "2022-12-30", "mega_rank": 1},
            {"symbol": "MSFT", "start_date": "2022-01-31", "end_date": "2022-12-30", "mega_rank": 2},
            {"symbol": "AAPL", "start_date": "2022-01-31", "end_date": "2022-12-30", "mega_rank": 3},
            {"symbol": "AMZN", "start_date": "2023-01-31", "end_date": None, "mega_rank": 1},
            {"symbol": "GOOGL", "start_date": "2023-01-31", "end_date": None, "mega_rank": 2},
            {"symbol": "META", "start_date": "2023-01-31", "end_date": None, "mega_rank": 3},
        ]
    )


def test_normalizes_roundhill_mags_raw_holdings() -> None:
    raw = pd.DataFrame(
        [
            {
                "Date": "06/02/2022",
                "Account": "MAGS",
                "StockTicker": "67066G104 TRS 071426 NM",
                "SecurityName": "NVIDIA CORP SWAP",
                "Weightings": "8.00%",
            },
            {
                "Date": "06/02/2022",
                "Account": "MAGS",
                "StockTicker": "NVDA",
                "SecurityName": "NVIDIA Corp",
                "Weightings": "7.00%",
            },
            {
                "Date": "06/02/2022",
                "Account": "MAGS",
                "StockTicker": "MSFT",
                "SecurityName": "Microsoft Corp",
                "Weightings": "14.00%",
            },
            {
                "Date": "06/02/2022",
                "Account": "MAGS",
                "StockTicker": "AVGO",
                "SecurityName": "Broadcom Inc",
                "Weightings": "13.00%",
            },
            {
                "Date": "06/02/2022",
                "Account": "MAGS",
                "StockTicker": "912797TS6",
                "SecurityName": "United States Treasury Bill",
                "Weightings": "50.00%",
            },
        ]
    )

    universe = _normalize_universe_history(raw)

    assert universe["symbol"].tolist() == ["NVDA", "MSFT", "AVGO"]
    assert universe.loc[universe["symbol"].eq("NVDA"), "source_weight"].iloc[0] == 15.0
    assert universe["start_date"].iloc[0] == pd.Timestamp("2022-06-01")


def test_resolve_active_candidate_symbols_uses_point_in_time_top_names() -> None:
    universe = _sample_dynamic_universe()

    first = resolve_active_candidate_symbols(
        universe,
        pd.Timestamp("2022-06-30"),
        fallback_symbols=("AAPL", "MSFT", "NVDA"),
        candidate_universe_size=2,
        available_symbols={"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"},
    )
    second = resolve_active_candidate_symbols(
        universe,
        pd.Timestamp("2023-06-30"),
        fallback_symbols=("AAPL", "MSFT", "NVDA"),
        candidate_universe_size=2,
        available_symbols={"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"},
    )

    assert first == ("NVDA", "MSFT")
    assert second == ("AMZN", "GOOGL")


def test_rank_candidates_prefers_eligible_pullbacks() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "NVDA",
                "mom_20": -0.02,
                "mom_63": 0.15,
                "mom_126": 0.40,
                "mom_252": 0.70,
                "rel_mom_126_vs_benchmark": 0.15,
                "sma_50_gap": -0.03,
                "sma_200_gap": 0.20,
                "high_63_gap": -0.10,
                "high_252_gap": -0.08,
                "low_20_gap": 0.04,
                "vol_63": 0.35,
            },
            {
                "symbol": "TSLA",
                "mom_20": -0.10,
                "mom_63": -0.20,
                "mom_126": -0.15,
                "mom_252": 0.05,
                "rel_mom_126_vs_benchmark": -0.30,
                "sma_50_gap": -0.18,
                "sma_200_gap": -0.05,
                "high_63_gap": -0.35,
                "high_252_gap": -0.40,
                "low_20_gap": 0.01,
                "vol_63": 0.60,
            },
        ]
    )

    ranked = rank_candidates(frame)

    assert ranked["symbol"].tolist() == ["NVDA"]
    assert ranked["size_multiplier"].iloc[0] > 1.0


def test_build_target_weights_uses_two_times_product_without_borrowing() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "mom_20": -0.02,
                "mom_63": 0.12 + idx * 0.01,
                "mom_126": 0.25 + idx * 0.02,
                "mom_252": 0.45 + idx * 0.03,
                "rel_mom_126_vs_benchmark": 0.05 + idx * 0.01,
                "sma_50_gap": -0.03,
                "sma_200_gap": 0.15,
                "high_63_gap": -0.10,
                "high_252_gap": -0.08,
                "low_20_gap": 0.04,
                "vol_63": 0.30,
            }
            for idx, symbol in enumerate(["NVDA", "MSFT", "META"])
        ]
    )

    weights, _ranked, metadata = build_target_weights(
        frame,
        current_holdings=set(),
        target_product_exposure=1.0,
        top_n=3,
        hold_buffer=1,
        single_name_cap=0.45,
        safe_haven="BOXX",
        leverage_multiple=2.0,
    )

    product_exposure = sum(weight for symbol, weight in weights.items() if symbol != "BOXX")
    assert product_exposure <= 1.0
    assert metadata["product_exposure"] == product_exposure
    assert metadata["underlying_exposure"] > 1.0


def test_run_backtest_builds_research_outputs() -> None:
    result = run_backtest(
        _sample_prices(),
        start_date="2022-06-01",
        end_date="2023-11-30",
        turnover_cost_bps=0.0,
        leveraged_expense_rate=0.0,
    )

    summary = result["summary"]
    assert summary["Strategy"].iloc[0] == PROFILE
    assert {"QQQ", "SPY", "equal_weight_mag7", "equal_weight_mag7_2x"}.issubset(set(summary["Strategy"]))
    assert summary.loc[summary["Strategy"] == PROFILE, "Avg Product Exposure"].iloc[0] > 0.0
    assert summary.loc[summary["Strategy"] == PROFILE, "Avg Underlying Exposure"].iloc[0] > 0.0
    assert not result["weights_history"].empty
    assert not result["candidate_scores"].empty
    assert not result["trades"].empty
    assert not result["exposure_history"].empty


def test_run_backtest_uses_dynamic_universe_when_provided() -> None:
    result = run_backtest(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-06-01",
        end_date="2023-11-30",
        candidate_universe_size=2,
        turnover_cost_bps=0.0,
        leveraged_expense_rate=0.0,
    )

    summary = result["summary"]
    assert summary["Strategy"].iloc[0] == DYNAMIC_PROFILE
    assert {"QQQ", "SPY", "equal_weight_dynamic_pool", "equal_weight_dynamic_pool_2x"}.issubset(
        set(summary["Strategy"])
    )
    candidates = result["exposure_history"]["candidate_symbols"].astype(str)
    assert candidates.str.contains("NVDA,MSFT").any()
    assert candidates.str.contains("AMZN,GOOGL").any()


def test_run_backtest_supports_margin_stock_return_mode() -> None:
    result = run_backtest(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-06-01",
        end_date="2023-11-30",
        candidate_universe_size=2,
        turnover_cost_bps=0.0,
        return_mode=RETURN_MODE_MARGIN_STOCK,
        margin_borrow_rate=0.05,
    )

    summary = result["summary"]
    strategy_row = summary.loc[summary["Strategy"].eq(DYNAMIC_PROFILE)].iloc[0]
    assert strategy_row["Return Mode"] == RETURN_MODE_MARGIN_STOCK
    assert result["exposure_history"]["return_mode"].eq(RETURN_MODE_MARGIN_STOCK).all()
    assert result["exposure_history"]["margin_borrow_rate"].eq(0.05).all()


def test_run_backtest_applies_external_rebound_budget_to_left_side_strategy() -> None:
    signals = pd.DataFrame(
        [
            {
                "as_of": "2022-06-01",
                "active_until": "2023-11-30",
                "sleeve_suggestion": 0.10,
            }
        ]
    )

    result = run_backtest(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-06-01",
        end_date="2023-11-30",
        candidate_universe_size=2,
        turnover_cost_bps=0.0,
        leveraged_expense_rate=0.0,
        entry_line_floor=1.50,
        entry_line_cap=1.50,
        rebound_budget_signals=signals,
    )

    strategy_name = f"{DYNAMIC_PROFILE}{REBOUND_BUDGET_STRATEGY_SUFFIX}"
    exposure = result["exposure_history"]
    assert result["summary"]["Strategy"].iloc[0] == strategy_name
    assert exposure["rebound_budget_suggestion"].max() == 0.10
    assert exposure["rebound_budget_applied"].max() == 0.10
    assert exposure["target_product_exposure"].max() == 0.10


def test_run_backtest_blocks_rebound_budget_in_hard_defense_by_default() -> None:
    signals = pd.DataFrame([{"as_of": "2022-06-01", "active_until": "2022-08-31", "sleeve_suggestion": 0.10}])

    result = run_backtest(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-06-01",
        end_date="2022-08-31",
        candidate_universe_size=2,
        turnover_cost_bps=0.0,
        leveraged_expense_rate=0.0,
        exit_line_floor=1.50,
        exit_line_cap=1.50,
        rebound_budget_signals=signals,
    )

    exposure = result["exposure_history"]
    assert exposure["regime"].eq("hard_defense").any()
    assert exposure["rebound_budget_suggestion"].max() == 0.10
    assert exposure["rebound_budget_applied"].max() == 0.0
    assert exposure["target_product_exposure"].max() == 0.0


def test_cli_writes_backtest_artifacts(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--start",
            "2022-06-01",
            "--end",
            "2023-11-30",
            "--turnover-cost-bps",
            "0",
            "--leveraged-expense-rate",
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
