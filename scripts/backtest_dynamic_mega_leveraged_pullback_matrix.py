from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mag7_leveraged_pullback_backtest import (
    DEFAULT_ATR_ENTRY_SCALE,
    DEFAULT_ATR_EXIT_SCALE,
    DEFAULT_ATR_PERIOD,
    DEFAULT_ENTRY_LINE_CAP,
    DEFAULT_ENTRY_LINE_FLOOR,
    DEFAULT_EXIT_LINE_CAP,
    DEFAULT_EXIT_LINE_FLOOR,
    DEFAULT_MARGIN_BORROW_RATE,
    RECOMMENDED_DYNAMIC_MAX_PRODUCT_EXPOSURE,
    RECOMMENDED_DYNAMIC_HARD_PRODUCT_EXPOSURE,
    RECOMMENDED_DYNAMIC_SINGLE_NAME_CAP,
    RECOMMENDED_DYNAMIC_SOFT_PRODUCT_EXPOSURE,
    RETURN_MODE_LEVERAGED_PRODUCT,
    RETURN_MODES,
    run_backtest,
)
from us_equity_snapshot_pipelines.russell_1000_multi_factor_defensive_snapshot import read_table


def _split_ints(raw: str) -> tuple[int, ...]:
    values = []
    for value in str(raw or "").split(","):
        value = value.strip()
        if value:
            values.append(int(value))
    return tuple(values)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a parameter matrix for dynamic mega leveraged pullback.")
    parser.add_argument("--prices", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--candidate-universe-sizes", default="7,10,15,20")
    parser.add_argument("--top-n-values", default="2,3,4")
    parser.add_argument("--frequency", choices=("weekly", "monthly"), default="weekly")
    parser.add_argument("--benchmark-symbol", default="QQQ")
    parser.add_argument("--broad-benchmark-symbol", default="SPY")
    parser.add_argument("--market-trend-symbol")
    parser.add_argument("--leverage-multiple", type=float, default=2.0)
    parser.add_argument("--max-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_MAX_PRODUCT_EXPOSURE)
    parser.add_argument("--soft-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_SOFT_PRODUCT_EXPOSURE)
    parser.add_argument("--hard-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_HARD_PRODUCT_EXPOSURE)
    parser.add_argument("--single-name-cap", type=float, default=RECOMMENDED_DYNAMIC_SINGLE_NAME_CAP)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--return-mode", choices=RETURN_MODES, default=RETURN_MODE_LEVERAGED_PRODUCT)
    parser.add_argument("--leveraged-expense-rate", type=float, default=0.01)
    parser.add_argument("--margin-borrow-rate", type=float, default=DEFAULT_MARGIN_BORROW_RATE)
    parser.add_argument("--atr-period", type=int, default=DEFAULT_ATR_PERIOD)
    parser.add_argument("--atr-entry-scale", type=float, default=DEFAULT_ATR_ENTRY_SCALE)
    parser.add_argument("--entry-line-floor", type=float, default=DEFAULT_ENTRY_LINE_FLOOR)
    parser.add_argument("--entry-line-cap", type=float, default=DEFAULT_ENTRY_LINE_CAP)
    parser.add_argument("--atr-exit-scale", type=float, default=DEFAULT_ATR_EXIT_SCALE)
    parser.add_argument("--exit-line-floor", type=float, default=DEFAULT_EXIT_LINE_FLOOR)
    parser.add_argument("--exit-line-cap", type=float, default=DEFAULT_EXIT_LINE_CAP)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    prices = read_table(args.prices)
    universe = read_table(args.universe)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    for candidate_size in _split_ints(args.candidate_universe_sizes):
        for top_n in _split_ints(args.top_n_values):
            if top_n > candidate_size:
                continue
            result = run_backtest(
                prices,
                universe,
                start_date=args.start_date,
                end_date=args.end_date,
                candidate_universe_size=candidate_size,
                benchmark_symbol=args.benchmark_symbol,
                broad_benchmark_symbol=args.broad_benchmark_symbol,
                market_trend_symbol=args.market_trend_symbol,
                frequency=args.frequency,
                top_n=top_n,
                leverage_multiple=args.leverage_multiple,
                max_product_exposure=args.max_product_exposure,
                soft_product_exposure=args.soft_product_exposure,
                hard_product_exposure=args.hard_product_exposure,
                single_name_cap=args.single_name_cap,
                turnover_cost_bps=args.turnover_cost_bps,
                return_mode=args.return_mode,
                leveraged_expense_rate=args.leveraged_expense_rate,
                margin_borrow_rate=args.margin_borrow_rate,
                atr_period=args.atr_period,
                atr_entry_scale=args.atr_entry_scale,
                entry_line_floor=args.entry_line_floor,
                entry_line_cap=args.entry_line_cap,
                atr_exit_scale=args.atr_exit_scale,
                exit_line_floor=args.exit_line_floor,
                exit_line_cap=args.exit_line_cap,
            )
            summary = result["summary"].iloc[0].to_dict()
            summary["candidate_universe_size"] = candidate_size
            summary["top_n"] = top_n
            rows.append(summary)

    summary_frame = pd.DataFrame(rows)
    if summary_frame.empty:
        raise RuntimeError("No matrix rows were produced")
    sort_columns = ["Sharpe", "CAGR", "Max Drawdown", "Turnover/Year"]
    summary_frame = summary_frame.sort_values(sort_columns, ascending=[False, False, False, True]).reset_index(drop=True)
    summary_path = output_dir / "matrix_summary.csv"
    summary_frame.to_csv(summary_path, index=False)
    print(summary_frame.to_string(index=False))
    print(f"wrote matrix summary -> {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
