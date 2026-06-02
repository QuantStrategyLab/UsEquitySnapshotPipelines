from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.leveraged_strategy_candidates import (
    LEVERAGED_CANDIDATES,
    build_ranking,
    collect_required_symbols,
    main,
    run_candidate_research,
)


def _sample_prices(symbols: tuple[str, ...], *, periods: int = 900) -> pd.DataFrame:
    dates = pd.bdate_range("2021-01-04", periods=periods)
    rows = []
    for symbol_index, symbol in enumerate(symbols):
        trend = 0.00015 + (symbol_index % 5) * 0.00005
        if symbol in {"BIL"}:
            trend = 0.00004
        if symbol in {"QQQ", "XLK", "SOXX", "SMH"}:
            trend += 0.00025
        if symbol in {"TQQQ", "SOXL", "TECL", "UPRO", "USD"}:
            trend += 0.00045
        base = 50.0 + symbol_index * 4.0
        for idx, as_of in enumerate(dates):
            shock = 0.92 if 500 < idx < 540 and symbol not in {"BIL"} else 1.0
            close = base * ((1.0 + trend) ** idx) * shock
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": close,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_leveraged_candidate_research_runs_current_optimization_and_supplemental_groups() -> None:
    periods = (
        ("short", "2024-01-02", "2024-06-28"),
        ("medium", "2023-01-03", "2024-06-28"),
        ("long", "2022-01-03", "2024-06-28"),
    )

    result = run_candidate_research(
        price_history=_sample_prices(collect_required_symbols()),
        periods=periods,
        turnover_cost_bps=0.0,
    )

    period_summary = result["period_summary"]
    ranking = result["ranking"]
    assert len(period_summary["Candidate"].unique()) == len(LEVERAGED_CANDIDATES)
    assert set(period_summary["Period"].unique()) == {"short", "medium", "long"}
    assert {"current_live_proxy", "optimization_variant", "leveraged_supplement"} <= set(period_summary["Candidate Group"])
    assert (period_summary.drop_duplicates("Candidate")["Candidate Group"] == "leveraged_supplement").sum() == 5
    assert {"new_strategy_rank", "replacement_review_candidate", "supplemental_review_candidate", "review_action"} <= set(ranking.columns)
    assert ranking.loc[ranking["Candidate Group"].eq("optimization_variant"), "supplemental_review_candidate"].eq(False).all()


def test_drawdown_near_30_must_beat_market_for_live_gate() -> None:
    period_summary = pd.DataFrame(
        [
            {
                "Period": period,
                "Candidate": "candidate_a",
                "Display Name": "Candidate A",
                "Candidate Group": "leveraged_supplement",
                "Rule": "fixed",
                "Benchmark Symbol": "SPY",
                "Trading Days": 252,
                "CAGR": 0.08,
                "Sharpe": 0.5,
                "Max Drawdown": -0.26,
                "Excess CAGR vs Market": -0.01 if period == "long" else 0.01,
                "Turnover/Year": 1.0,
            }
            for period in ("short", "medium", "long")
        ]
    )

    ranking = build_ranking(period_summary)

    assert not bool(ranking.loc[0, "live_gate_passed"])
    assert "drawdown_near_30_without_market_outperformance" in ranking.loc[0, "gate_reason"]


def test_cli_writes_leveraged_candidate_outputs(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    _sample_prices(collect_required_symbols()).to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--prices",
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
    assert (output_dir / "portfolio_returns.csv").exists()
