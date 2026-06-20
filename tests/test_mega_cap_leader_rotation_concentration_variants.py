from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants import (
    main,
    run_concentration_variant_research,
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
        "LLY": 0.0011,
        "XOM": 0.0012,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (90.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 2_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2022-01-31",
                "end_date": "2022-12-30",
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("AAPL", "Information Technology"),
                    ("XOM", "Energy"),
                    ("LLY", "Health Care"),
                ],
                start=1,
            )
        ]
        + [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2023-01-31",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("AMZN", "Consumer Discretionary"),
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("LLY", "Health Care"),
                    ("XOM", "Energy"),
                ],
                start=1,
            )
        ]
    )


def test_concentration_variant_research_builds_blend_and_dynamic_tables() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(0.10,),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
    )

    summary = result["concentration_variant_summary"]
    yearly = result["concentration_variant_yearly_summary"]
    rolling = result["concentration_variant_rolling_summary"]
    daily_returns = result["concentration_variant_daily_returns"]
    rebalance_trades = result["concentration_variant_rebalance_trades"]
    mode_history = result["concentration_variant_mode_history"]
    assert {
        "base_top2_cap50",
        "base_top4_cap25",
        "blend_top2_50_top4_50",
        "dynamic_top2_dd10_to_top4",
    }.issubset(set(summary["Run"]))
    assert {"CAGR", "Max Drawdown", "Top4 Mode Share"}.issubset(summary.columns)
    assert {"Strategy Return", "QQQ Return", "SPY Return"}.issubset(yearly.columns)
    assert {"Window Years", "Strategy CAGR", "QQQ CAGR"}.issubset(rolling.columns)
    assert {"Date", "Run", "Strategy Return", "QQQ Return", "SPY Return"}.issubset(daily_returns.columns)
    assert {"Date", "Run", "Symbol", "Trade Weight Delta", "Abs Trade Weight Delta"}.issubset(rebalance_trades.columns)
    assert {"base_top2_cap50", "blend_top2_50_top4_50"}.issubset(set(daily_returns["Run"]))
    assert {"base_top2_cap50", "blend_top2_50_top4_50"}.issubset(set(rebalance_trades["Run"]))
    assert not rolling.empty
    assert not daily_returns.empty
    assert not rebalance_trades.empty
    assert not mode_history.empty


def test_concentration_variant_research_can_build_sector_capped_variants() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
        include_sector_capped_variants=True,
        sector_cap_values=(1,),
    )

    summary = result["concentration_variant_summary"]
    assert {
        "sector_cap1_top2_cap50",
        "sector_cap1_top4_cap25",
        "sector_cap1_blend_top2_50_top4_50",
    }.issubset(set(summary["Run"]))
    sector_rows = summary.loc[summary["Run"].astype(str).str.startswith("sector_cap1_")]
    assert set(sector_rows["Max Names Per Sector"].dropna().astype(int)) == {1}
    assert set(sector_rows["Variant Type"]).issubset(
        {
            "sector_capped_base_top2",
            "sector_capped_base_top4",
            "sector_capped_fixed_blend",
        }
    )


def test_concentration_variant_research_can_build_sector_soft_penalty_variants() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
        include_sector_soft_penalty_variants=True,
        sector_score_penalty_values=(0.50,),
    )

    summary = result["concentration_variant_summary"]
    assert {
        "sector_penalty0p5_top2_cap50",
        "sector_penalty0p5_top4_cap25",
        "sector_penalty0p5_blend_top2_50_top4_50",
    }.issubset(set(summary["Run"]))
    penalty_rows = summary.loc[summary["Run"].astype(str).str.startswith("sector_penalty0p5_")]
    assert set(penalty_rows["Sector Score Penalty"].dropna().astype(float)) == {0.50}
    assert set(penalty_rows["Variant Type"]).issubset(
        {
            "sector_soft_penalty_base_top2",
            "sector_soft_penalty_base_top4",
            "sector_soft_penalty_fixed_blend",
        }
    )


def test_concentration_variant_research_can_build_residual_beta_variants() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
        include_residual_momentum_variants=True,
        residual_momentum_weights=(0.50,),
        beta_penalty_weights=(0.25,),
    )

    summary = result["concentration_variant_summary"]
    assert {
        "resid0p5_top2_cap50",
        "resid0p5_top4_cap25",
        "resid0p5_blend_top2_50_top4_50",
        "beta0p25_top2_cap50",
        "beta0p25_top4_cap25",
        "beta0p25_blend_top2_50_top4_50",
    }.issubset(set(summary["Run"]))
    residual_rows = summary.loc[summary["Run"].astype(str).str.startswith("resid0p5_")]
    beta_rows = summary.loc[summary["Run"].astype(str).str.startswith("beta0p25_")]
    assert set(residual_rows["Residual Momentum Weight"].dropna().astype(float)) == {0.50}
    assert set(beta_rows["Beta Penalty Weight"].dropna().astype(float)) == {0.25}
    assert set(residual_rows["Variant Type"]).issubset({"residual_beta_base_top2", "residual_beta_base_top4", "residual_beta_fixed_blend"})


def test_concentration_variant_research_can_build_volatility_managed_variants() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
        include_volatility_managed_variants=True,
        vol_target_values=(0.18,),
        vol_target_window=21,
        vol_target_min_stock_exposure=0.50,
    )

    summary = result["concentration_variant_summary"]
    assert {
        "voltarget18_min50_top2_cap50",
        "voltarget18_min50_top4_cap25",
        "voltarget18_min50_blend_top2_50_top4_50",
    }.issubset(set(summary["Run"]))
    vol_rows = summary.loc[summary["Run"].astype(str).str.startswith("voltarget18_min50_")]
    assert set(vol_rows["Vol Target"].dropna().astype(float)) == {0.18}
    assert set(vol_rows["Vol Target Window"].dropna().astype(int)) == {21}
    assert set(vol_rows["Min Stock Exposure"].dropna().astype(float)) == {0.50}
    assert set(vol_rows["Variant Type"]).issubset(
        {
            "volatility_managed_base_top2",
            "volatility_managed_base_top4",
            "volatility_managed_fixed_blend",
        }
    )


def test_concentration_variant_research_can_build_panic_rebound_guard_variants() -> None:
    result = run_concentration_variant_research(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2022-01-03",
        end_date="2023-11-30",
        universe_lag_trading_days=1,
        blend_top2_weights=(0.50,),
        dynamic_drawdown_thresholds=(),
        rolling_window_years=(1,),
        min_adv20_usd=1_000_000.0,
        min_history_days=100,
        turnover_cost_bps=0.0,
        include_panic_rebound_guard_variants=True,
        panic_guard_drawdown_threshold=0.10,
        panic_guard_rebound_threshold=0.03,
        panic_guard_vol_threshold=0.25,
        panic_guard_stock_exposure=0.50,
    )

    summary = result["concentration_variant_summary"]
    assert {
        "panicdd10_ret3_vol25_stock50_top2_cap50",
        "panicdd10_ret3_vol25_stock50_top4_cap25",
        "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50",
    }.issubset(set(summary["Run"]))
    panic_rows = summary.loc[summary["Run"].astype(str).str.startswith("panicdd10_ret3_vol25_stock50_")]
    assert set(panic_rows["Panic Drawdown Threshold"].dropna().astype(float)) == {0.10}
    assert set(panic_rows["Panic Rebound Threshold"].dropna().astype(float)) == {0.03}
    assert set(panic_rows["Panic Vol Threshold"].dropna().astype(float)) == {0.25}
    assert set(panic_rows["Panic Stock Exposure"].dropna().astype(float)) == {0.50}
    assert set(panic_rows["Variant Type"]).issubset(
        {
            "panic_rebound_guard_base_top2",
            "panic_rebound_guard_base_top4",
            "panic_rebound_guard_fixed_blend",
        }
    )


def test_concentration_variant_research_cli_writes_outputs(tmp_path) -> None:
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
            "2022-01-03",
            "--end",
            "2023-11-30",
            "--universe-lag-days",
            "1",
            "--blend-top2-weights",
            "0.5",
            "--dynamic-drawdown-thresholds",
            "0.1",
            "--rolling-window-years",
            "1",
            "--min-adv20-usd",
            "1000000",
            "--min-history-days",
            "100",
            "--turnover-cost-bps",
            "0",
            "--include-sector-capped-variants",
            "--sector-cap-values",
            "1",
            "--include-sector-soft-penalty-variants",
            "--sector-score-penalty-values",
            "0.5",
            "--include-residual-momentum-variants",
            "--residual-momentum-weights",
            "0.5",
            "--beta-penalty-weights",
            "0.25",
            "--include-volatility-managed-variants",
            "--vol-target-values",
            "0.18",
            "--vol-target-window",
            "21",
            "--vol-target-min-stock-exposure",
            "0.5",
            "--include-panic-rebound-guard-variants",
            "--panic-guard-drawdown-threshold",
            "0.10",
            "--panic-guard-rebound-threshold",
            "0.03",
            "--panic-guard-vol-threshold",
            "0.25",
            "--panic-guard-stock-exposure",
            "0.5",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "concentration_variant_summary.csv").exists()
    assert (output_dir / "concentration_variant_yearly_summary.csv").exists()
    assert (output_dir / "concentration_variant_rolling_summary.csv").exists()
    assert (output_dir / "concentration_variant_daily_returns.csv").exists()
    assert (output_dir / "concentration_variant_rebalance_trades.csv").exists()
    assert (output_dir / "concentration_variant_mode_history.csv").exists()
    summary = pd.read_csv(output_dir / "concentration_variant_summary.csv")
    assert "blend_top2_50_top4_50" in set(summary["Run"])
    assert "sector_cap1_blend_top2_50_top4_50" in set(summary["Run"])
    assert "sector_penalty0p5_blend_top2_50_top4_50" in set(summary["Run"])
    assert "resid0p5_blend_top2_50_top4_50" in set(summary["Run"])
    assert "beta0p25_blend_top2_50_top4_50" in set(summary["Run"])
    assert "voltarget18_min50_blend_top2_50_top4_50" in set(summary["Run"])
    assert "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50" in set(summary["Run"])
