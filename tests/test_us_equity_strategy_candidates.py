from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.us_equity_strategy_candidates import (
    ETF_CANDIDATES,
    SNAPSHOT_BASELINE_CANDIDATES,
    SNAPSHOT_CANDIDATES,
    SNAPSHOT_NEW_CANDIDATES,
    SNAPSHOT_OPTIMIZATION_CANDIDATES,
    build_ranking,
    collect_required_etf_symbols,
    main,
    run_candidate_research,
)


def _sample_prices(symbols: tuple[str, ...], *, periods: int = 900) -> pd.DataFrame:
    dates = pd.bdate_range("2021-01-04", periods=periods)
    rows = []
    for symbol_index, symbol in enumerate(symbols):
        trend = 0.0002 + (symbol_index % 7) * 0.00008
        if symbol in {"BIL", "BOXX", "IEF"}:
            trend = 0.00005
        if symbol in {"QQQ", "XLK", "MTUM"}:
            trend += 0.00035
        base = 80.0 + symbol_index * 3.0
        for idx, as_of in enumerate(dates):
            seasonal = 1.0 + 0.015 * ((idx % 63) / 63.0)
            close = base * ((1.0 + trend) ** idx) * seasonal
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": close,
                    "volume": 1_000_000 + symbol_index * 10_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_r1000_universe(symbols: tuple[str, ...]) -> pd.DataFrame:
    sectors = ["Information Technology", "Communication Services", "Health Care", "Industrials"]
    rows = []
    for idx, symbol in enumerate(symbols):
        if symbol in {"SPY", "BOXX"}:
            continue
        rows.append({"symbol": symbol, "sector": sectors[idx % len(sectors)]})
    return pd.DataFrame(rows)


def test_candidate_research_has_no_live_r1000_baseline_after_defensive_retirement() -> None:
    etf_prices = _sample_prices(collect_required_etf_symbols())
    stock_symbols = ("SPY", "BOXX", "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH")
    r1000_prices = _sample_prices(stock_symbols)
    periods = (
        ("short", "2024-01-02", "2024-06-28"),
        ("medium", "2023-01-03", "2024-06-28"),
        ("long", "2022-01-03", "2024-06-28"),
    )

    result = run_candidate_research(
        etf_price_history=etf_prices,
        periods=periods,
        r1000_price_history=r1000_prices,
        r1000_universe=_sample_r1000_universe(stock_symbols),
        turnover_cost_bps=0.0,
    )

    period_summary = result["period_summary"]
    ranking = result["ranking"]
    assert len(ETF_CANDIDATES) + len(SNAPSHOT_BASELINE_CANDIDATES) + len(SNAPSHOT_OPTIMIZATION_CANDIDATES) == 0
    assert len(SNAPSHOT_NEW_CANDIDATES) == 1
    assert SNAPSHOT_NEW_CANDIDATES[0].candidate_id == "new_r1000_residual_strength_20"
    assert not period_summary.empty
    assert not ranking.empty
    assert "new_r1000_residual_strength_20" in set(ranking["Candidate"].astype(str))


def test_build_ranking_blocks_candidates_with_missing_periods() -> None:
    period_summary = pd.DataFrame(
        [
            {
                "Period": "short",
                "Candidate": "candidate_a",
                "Display Name": "Candidate A",
                "Candidate Type": "ordinary_etf",
                "Candidate Group": "new_ordinary_strategy",
                "Trading Days": 120,
                "CAGR": 0.10,
                "Sharpe": 1.0,
                "Max Drawdown": -0.10,
                "Excess CAGR vs Benchmark": 0.02,
                "Turnover/Year": 1.0,
            }
        ]
    )

    ranking = build_ranking(period_summary)

    assert not bool(ranking.loc[0, "live_gate_passed"])
    assert not bool(ranking.loc[0, "live_enabled_candidate"])
    assert "missing_or_too_short_period" in ranking.loc[0, "gate_reason"]


def test_build_ranking_blocks_drawdown_above_30pct() -> None:
    period_summary = pd.DataFrame(
        [
            {
                "Period": period,
                "Candidate": "candidate_a",
                "Display Name": "Candidate A",
                "Candidate Type": "ordinary_etf",
                "Candidate Group": "new_ordinary_strategy",
                "Trading Days": 252,
                "CAGR": 0.10,
                "Sharpe": 1.0,
                "Max Drawdown": -0.31 if period == "long" else -0.10,
                "Excess CAGR vs Benchmark": 0.02,
                "Turnover/Year": 1.0,
            }
            for period in ("short", "medium", "long")
        ]
    )

    ranking = build_ranking(period_summary)

    assert not bool(ranking.loc[0, "live_gate_passed"])
    assert not bool(ranking.loc[0, "live_enabled_candidate"])
    assert "drawdown_above_30pct" in ranking.loc[0, "gate_reason"]


def test_cli_writes_candidate_outputs(tmp_path) -> None:
    prices_path = tmp_path / "etf_prices.csv"
    output_dir = tmp_path / "output"
    _sample_prices(collect_required_etf_symbols()).to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--etf-prices",
            str(prices_path),
            "--periods",
            "short:2024-01-02:2024-06-28,medium:2023-01-03:2024-06-28,long:2022-01-03:2024-06-28",
            "--turnover-cost-bps",
            "0",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "period_summary.csv").exists()
    assert (output_dir / "ranking.csv").exists()
