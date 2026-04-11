from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.russell_1000_multi_factor_backtest import (
    BACKTEST_SUMMARY_COLUMNS,
    run_backtest,
)
from us_equity_snapshot_pipelines.russell_1000_multi_factor_defensive_snapshot import read_table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest the Russell 1000 price-only multi-factor defensive strategy.",
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input universe file")
    parser.add_argument("--start", dest="start_date", help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--output-dir", help="Optional output directory for summary/equity/weights csv")
    parser.add_argument("--benchmark-symbol", default="SPY")
    parser.add_argument("--safe-haven", default="BOXX")
    parser.add_argument("--holdings-count", type=int, default=24)
    parser.add_argument("--single-name-cap", type=float, default=0.06)
    parser.add_argument("--sector-cap", type=float, default=0.20)
    parser.add_argument("--hold-bonus", type=float, default=0.15)
    parser.add_argument("--turnover-cost-bps", type=float, default=0.0)
    return parser


def _format_summary(summary: dict[str, float | str]) -> pd.DataFrame:
    return pd.DataFrame([{column: summary.get(column) for column in BACKTEST_SUMMARY_COLUMNS}])


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    result = run_backtest(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        safe_haven=args.safe_haven,
        holdings_count=args.holdings_count,
        single_name_cap=args.single_name_cap,
        sector_cap=args.sector_cap,
        hold_bonus=args.hold_bonus,
        turnover_cost_bps=args.turnover_cost_bps,
    )

    summary_frame = _format_summary(result["summary"])
    print(summary_frame.to_string(index=False))

    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_frame.to_csv(output_dir / "summary.csv", index=False)
        result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
        result["weights_history"].to_csv(output_dir / "weights_history.csv")
        result["turnover_history"].to_csv(output_dir / "turnover_history.csv")
        print(f"wrote backtest outputs -> {output_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
