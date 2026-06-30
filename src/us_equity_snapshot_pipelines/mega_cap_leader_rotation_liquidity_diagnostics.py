from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_stress_readiness import parse_csv_floats_no_percent, parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_PORTFOLIO_NAV_VALUES = (100_000.0, 500_000.0, 1_000_000.0, 5_000_000.0)
DEFAULT_ADV_WINDOW = 20
DEFAULT_EXECUTION_DAYS = 1
DEFAULT_MAX_PARTICIPATION_RATE = 0.01
DEFAULT_EXCLUDE_SYMBOLS = ("BOXX", "QQQ", "SPY")
DETAIL_COLUMNS = (
    "Run",
    "Portfolio NAV",
    "Date",
    "Symbol",
    "Abs Trade Weight Delta",
    "Trade Notional USD",
    "ADV Window",
    "ADV USD",
    "Execution Days",
    "Participation Rate",
    "liquidity_gate_passed",
    "liquidity_gate_reason",
)
SUMMARY_COLUMNS = (
    "Run",
    "Portfolio NAV",
    "Trade Rows",
    "Max Trade Notional USD",
    "Max Participation Rate",
    "P95 Participation Rate",
    "Median Participation Rate",
    "Missing ADV Rows",
    "Allowed Max Participation Rate",
    "liquidity_gate_passed",
    "liquidity_gate_reason",
    "recommended_action",
)


def _normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    date_column = "as_of" if "as_of" in frame.columns else "Date" if "Date" in frame.columns else None
    if date_column is None:
        raise ValueError("price_history must include as_of or Date column")
    required = {date_column, "symbol", "close", "volume"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"price_history must include columns: {', '.join(missing)}")
    frame = frame.rename(columns={date_column: "Date", "symbol": "Symbol", "close": "Close", "volume": "Volume"})
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Symbol"] = frame["Symbol"].astype(str).str.upper()
    frame["Close"] = pd.to_numeric(frame["Close"], errors="coerce")
    frame["Volume"] = pd.to_numeric(frame["Volume"], errors="coerce")
    return frame.dropna(subset=["Date", "Symbol", "Close", "Volume"]).sort_values(["Symbol", "Date"])


def _build_adv_table(price_history: pd.DataFrame, *, adv_window: int) -> pd.DataFrame:
    prices = _normalize_price_history(price_history)
    prices["Dollar Volume"] = prices["Close"] * prices["Volume"]
    prices["ADV USD"] = prices.groupby("Symbol", sort=False)["Dollar Volume"].transform(
        lambda values: values.rolling(int(adv_window), min_periods=1).mean()
    )
    return prices.loc[:, ["Date", "Symbol", "ADV USD"]]


def _prepare_trades(rebalance_trades: pd.DataFrame, *, exclude_symbols: Iterable[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rebalance_trades).copy()
    required = {"Date", "Run", "Symbol", "Abs Trade Weight Delta"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"rebalance_trades must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Run"] = frame["Run"].astype(str)
    frame["Symbol"] = frame["Symbol"].astype(str).str.upper()
    frame["Abs Trade Weight Delta"] = pd.to_numeric(frame["Abs Trade Weight Delta"], errors="coerce")
    frame = frame.dropna(subset=["Date", "Run", "Symbol", "Abs Trade Weight Delta"])
    excluded = {str(symbol).upper() for symbol in exclude_symbols}
    if excluded:
        frame = frame.loc[~frame["Symbol"].isin(excluded)].copy()
    return frame.loc[frame["Abs Trade Weight Delta"].gt(0.0)].copy()


def _detail_for_nav(
    trades: pd.DataFrame,
    adv: pd.DataFrame,
    *,
    portfolio_nav: float,
    adv_window: int,
    execution_days: int,
    max_participation_rate: float,
) -> pd.DataFrame:
    detail = trades.merge(adv, on=["Date", "Symbol"], how="left")
    detail["Portfolio NAV"] = float(portfolio_nav)
    detail["Trade Notional USD"] = detail["Abs Trade Weight Delta"] * float(portfolio_nav)
    detail["ADV Window"] = int(adv_window)
    detail["Execution Days"] = int(execution_days)
    capacity_denominator = detail["ADV USD"] * float(execution_days)
    detail["Participation Rate"] = detail["Trade Notional USD"] / capacity_denominator.replace(0.0, np.nan)
    reasons: list[str] = []
    for _, row in detail.iterrows():
        if pd.isna(row.get("ADV USD")):
            reasons.append("missing_adv")
        elif pd.isna(row.get("Participation Rate")):
            reasons.append("invalid_participation_rate")
        elif float(row.get("Participation Rate")) > float(max_participation_rate):
            reasons.append("participation_rate_above_limit")
        else:
            reasons.append("pass")
    detail["liquidity_gate_reason"] = reasons
    detail["liquidity_gate_passed"] = detail["liquidity_gate_reason"].eq("pass")
    return detail.loc[:, [column for column in DETAIL_COLUMNS if column in detail.columns]]


def _summarize_detail(detail: pd.DataFrame, *, max_participation_rate: float) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    for (run, nav), group in detail.groupby(["Run", "Portfolio NAV"], sort=False):
        participation = pd.to_numeric(group["Participation Rate"], errors="coerce")
        passed = group["liquidity_gate_passed"].astype(bool)
        failed_reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in group.loc[~passed, "liquidity_gate_reason"].tolist()
                if str(reason) and str(reason) != "pass"
            )
        )
        all_passed = bool(len(group) > 0 and passed.all())
        rows.append(
            {
                "Run": run,
                "Portfolio NAV": float(nav),
                "Trade Rows": int(len(group)),
                "Max Trade Notional USD": float(pd.to_numeric(group["Trade Notional USD"], errors="coerce").max()),
                "Max Participation Rate": float(participation.max()) if participation.notna().any() else float("nan"),
                "P95 Participation Rate": float(participation.quantile(0.95)) if participation.notna().any() else float("nan"),
                "Median Participation Rate": float(participation.median()) if participation.notna().any() else float("nan"),
                "Missing ADV Rows": int(group["ADV USD"].isna().sum()) if "ADV USD" in group.columns else int(len(group)),
                "Allowed Max Participation Rate": float(max_participation_rate),
                "liquidity_gate_passed": all_passed,
                "liquidity_gate_reason": "pass" if all_passed else ";".join(failed_reasons) or "failed_liquidity_gate",
                "recommended_action": "liquidity_live_review" if all_passed else "reduce_nav_or_extend_execution_days",
            }
        )
    return pd.DataFrame(rows).loc[:, list(SUMMARY_COLUMNS)]


def build_liquidity_diagnostics(
    rebalance_trades: pd.DataFrame,
    price_history: pd.DataFrame,
    *,
    portfolio_nav_values: Iterable[float] = DEFAULT_PORTFOLIO_NAV_VALUES,
    adv_window: int = DEFAULT_ADV_WINDOW,
    execution_days: int = DEFAULT_EXECUTION_DAYS,
    max_participation_rate: float = DEFAULT_MAX_PARTICIPATION_RATE,
    exclude_symbols: Iterable[str] = DEFAULT_EXCLUDE_SYMBOLS,
    candidate_runs: Iterable[str] | None = None,
) -> dict[str, pd.DataFrame]:
    trades = _prepare_trades(rebalance_trades, exclude_symbols=exclude_symbols)
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        trades = trades.loc[trades["Run"].isin(runs)].copy()
    adv = _build_adv_table(price_history, adv_window=int(adv_window))
    detail_frames = [
        _detail_for_nav(
            trades,
            adv,
            portfolio_nav=float(nav),
            adv_window=int(adv_window),
            execution_days=int(execution_days),
            max_participation_rate=float(max_participation_rate),
        )
        for nav in portfolio_nav_values
    ]
    detail = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame(columns=DETAIL_COLUMNS)
    summary = _summarize_detail(detail, max_participation_rate=float(max_participation_rate))
    return {
        "liquidity_trade_detail": detail.loc[:, [column for column in DETAIL_COLUMNS if column in detail.columns]],
        "liquidity_summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estimate Russell candidate trade participation rates from rebalance trade weights and ADV."
    )
    parser.add_argument("--trades", required=True, help="Input concentration_variant_rebalance_trades.csv")
    parser.add_argument("--prices", required=True, help="Input price history CSV with close and volume")
    parser.add_argument("--output-dir", required=True, help="Directory for liquidity diagnostics outputs")
    parser.add_argument(
        "--portfolio-nav-values",
        default=",".join(str(value) for value in DEFAULT_PORTFOLIO_NAV_VALUES),
        help="Comma-separated assumed portfolio NAV values in USD. Do not pass account identifiers.",
    )
    parser.add_argument("--adv-window", type=int, default=DEFAULT_ADV_WINDOW)
    parser.add_argument("--execution-days", type=int, default=DEFAULT_EXECUTION_DAYS)
    parser.add_argument("--max-participation-rate", type=float, default=DEFAULT_MAX_PARTICIPATION_RATE)
    parser.add_argument("--exclude-symbols", default=",".join(DEFAULT_EXCLUDE_SYMBOLS))
    parser.add_argument("--candidate-runs", default="")
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_liquidity_diagnostics(
        read_table(args.trades),
        read_table(args.prices),
        portfolio_nav_values=parse_csv_floats_no_percent(
            args.portfolio_nav_values,
            default=DEFAULT_PORTFOLIO_NAV_VALUES,
        ),
        adv_window=int(args.adv_window),
        execution_days=int(args.execution_days),
        max_participation_rate=float(args.max_participation_rate),
        exclude_symbols=parse_csv_strings(args.exclude_symbols, default=DEFAULT_EXCLUDE_SYMBOLS),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=()),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    detail_path = output_dir / "liquidity_trade_detail.csv"
    summary_path = output_dir / "liquidity_summary.csv"
    result["liquidity_trade_detail"].to_csv(detail_path, index=False)
    result["liquidity_summary"].to_csv(summary_path, index=False)
    print(result["liquidity_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote liquidity trade detail -> {detail_path}")
    print(f"wrote liquidity summary -> {summary_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
