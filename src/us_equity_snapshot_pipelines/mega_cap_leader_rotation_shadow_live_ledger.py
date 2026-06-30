from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from .artifacts import sha256_file, write_json
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

SHADOW_LIVE_LEDGER_SCHEMA_VERSION = "russell_top50_shadow_live_ledger.v1"
DEFAULT_FORWARD_WINDOW_DAYS = 21
DEFAULT_SLIPPAGE_BPS = 5.0
DEFAULT_SAFE_HAVEN = "SGOV"
TRADE_LEDGER_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Symbol",
    "Previous Weight",
    "Target Weight",
    "Trade Weight Delta",
    "Abs Trade Weight Delta",
    "Trade Side",
    "Portfolio NAV",
    "Gross Trade Notional",
    "Estimated Slippage Cost",
    "Signal Price",
    "Next Session Date",
    "Next Session Price",
    "Signal To Next Session Return",
)
HOLDINGS_LEDGER_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Symbol",
    "Target Weight",
    "Portfolio NAV",
    "Target Notional",
    "is_safe_haven",
)
SUMMARY_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Portfolio NAV",
    "Trade Count",
    "Buy Trade Count",
    "Sell Trade Count",
    "Gross Turnover Weight",
    "One Way Turnover Weight",
    "Gross Trade Notional",
    "Estimated Slippage Cost",
    "Stock Weight",
    "Safe Haven Weight",
    "Selected Symbols",
    "Forward Window Trading Days",
    "Forward Strategy Return",
    "Forward QQQ Return",
    "Forward SPY Return",
    "Forward Excess Return vs QQQ",
    "Forward Excess Return vs SPY",
    "diagnostic_scope",
)
DIAGNOSTIC_SCOPE = "shadow_live_observability_not_broker_execution"


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _date_column(frame: pd.DataFrame) -> str:
    for column in ("Date", "date", "as_of", "snapshot_date"):
        if column in frame.columns:
            return column
    raise ValueError("input must include a Date/date/as_of/snapshot_date column")


def _symbol_column(frame: pd.DataFrame) -> str:
    for column in ("Symbol", "symbol", "ticker", "Ticker"):
        if column in frame.columns:
            return column
    raise ValueError("price input must include a Symbol/symbol/ticker column")


def _price_column(frame: pd.DataFrame) -> str:
    for column in ("Adj Close", "adj_close", "Close", "close", "Price", "price"):
        if column in frame.columns:
            return column
    raise ValueError("price input must include Adj Close/Close/Price")


def _prepare_trades(rebalance_trades: pd.DataFrame, *, candidate_runs: Iterable[str] | None) -> pd.DataFrame:
    frame = pd.DataFrame(rebalance_trades).copy()
    required = {
        "Date",
        "Run",
        "Variant Type",
        "Symbol",
        "Previous Weight",
        "Target Weight",
        "Trade Weight Delta",
        "Abs Trade Weight Delta",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"rebalance_trades must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Run"] = frame["Run"].astype(str)
    frame["Symbol"] = frame["Symbol"].map(_normalize_symbol)
    for column in ("Previous Weight", "Target Weight", "Trade Weight Delta", "Abs Trade Weight Delta"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        frame = frame.loc[frame["Run"].isin(runs)].copy()
    frame = frame.dropna(subset=["Date", "Run", "Symbol", "Trade Weight Delta"])
    if frame.empty:
        raise ValueError("rebalance_trades has no rows after filtering")
    return frame.sort_values(["Run", "Date", "Symbol"], kind="stable").reset_index(drop=True)


def _prepare_daily_returns(daily_returns: pd.DataFrame, *, candidate_runs: Iterable[str] | None) -> pd.DataFrame:
    frame = pd.DataFrame(daily_returns).copy()
    required = {"Date", "Run", "Strategy Return", "QQQ Return", "SPY Return"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"daily_returns must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Run"] = frame["Run"].astype(str)
    for column in ("Strategy Return", "QQQ Return", "SPY Return"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        frame = frame.loc[frame["Run"].isin(runs)].copy()
    frame = frame.dropna(subset=["Date", "Run", "Strategy Return", "QQQ Return", "SPY Return"])
    if frame.empty:
        raise ValueError("daily_returns has no rows after filtering")
    return frame.sort_values(["Run", "Date"], kind="stable").reset_index(drop=True)


def _prepare_prices(prices: pd.DataFrame | None) -> pd.DataFrame | None:
    if prices is None:
        return None
    frame = pd.DataFrame(prices).copy()
    if frame.empty:
        return None
    date_col = _date_column(frame)
    symbol_col = _symbol_column(frame)
    price_col = _price_column(frame)
    output = pd.DataFrame(
        {
            "Date": pd.to_datetime(frame[date_col], errors="coerce").dt.tz_localize(None),
            "Symbol": frame[symbol_col].map(_normalize_symbol),
            "Price": pd.to_numeric(frame[price_col], errors="coerce"),
        }
    ).dropna(subset=["Date", "Symbol", "Price"])
    if output.empty:
        return None
    return output.sort_values(["Symbol", "Date"], kind="stable").reset_index(drop=True)


def _compound(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float((1.0 + clean).prod() - 1.0)


def _forward_returns_for_date(daily: pd.DataFrame, *, run: str, date: pd.Timestamp, window_days: int) -> dict[str, float | int]:
    run_daily = daily.loc[daily["Run"].eq(str(run)) & daily["Date"].gt(date)].sort_values("Date", kind="stable")
    window = run_daily.head(max(int(window_days), 0))
    days = int(len(window))
    strategy = _compound(window["Strategy Return"]) if days else float("nan")
    qqq = _compound(window["QQQ Return"]) if days else float("nan")
    spy = _compound(window["SPY Return"]) if days else float("nan")
    return {
        "Forward Window Trading Days": days,
        "Forward Strategy Return": strategy,
        "Forward QQQ Return": qqq,
        "Forward SPY Return": spy,
        "Forward Excess Return vs QQQ": strategy - qqq if pd.notna(strategy) and pd.notna(qqq) else float("nan"),
        "Forward Excess Return vs SPY": strategy - spy if pd.notna(strategy) and pd.notna(spy) else float("nan"),
    }


def _price_lookup(prices: pd.DataFrame | None, *, symbol: str, date: pd.Timestamp) -> tuple[float, pd.Timestamp | None, float]:
    if prices is None:
        return float("nan"), None, float("nan")
    symbol_prices = prices.loc[prices["Symbol"].eq(_normalize_symbol(symbol))].sort_values("Date", kind="stable")
    if symbol_prices.empty:
        return float("nan"), None, float("nan")
    signal_candidates = symbol_prices.loc[symbol_prices["Date"].le(date)]
    signal_price = float(signal_candidates.iloc[-1]["Price"]) if not signal_candidates.empty else float("nan")
    next_candidates = symbol_prices.loc[symbol_prices["Date"].gt(date)]
    if next_candidates.empty:
        return signal_price, None, float("nan")
    next_row = next_candidates.iloc[0]
    return signal_price, pd.Timestamp(next_row["Date"]), float(next_row["Price"])


def _manifest_payload(
    *,
    output_dir: Path,
    input_paths: Mapping[str, str | Path] | None,
    portfolio_nav: float,
    slippage_bps: float,
    forward_window_days: int,
    safe_haven: str,
    frames: Mapping[str, pd.DataFrame],
) -> dict[str, object]:
    artifacts = {
        "shadow_live_trade_ledger": output_dir / "shadow_live_trade_ledger.csv",
        "shadow_live_holdings_ledger": output_dir / "shadow_live_holdings_ledger.csv",
        "shadow_live_rebalance_summary": output_dir / "shadow_live_rebalance_summary.csv",
    }
    return {
        "manifest_type": "russell_top50_shadow_live_ledger",
        "artifact_schema_version": SHADOW_LIVE_LEDGER_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostic_scope": DIAGNOSTIC_SCOPE,
        "portfolio_nav": float(portfolio_nav),
        "slippage_bps": float(slippage_bps),
        "forward_window_days": int(forward_window_days),
        "safe_haven": _normalize_symbol(safe_haven),
        "inputs": {
            name: {
                "path": str(path),
                **({"sha256": sha256_file(path)} if Path(path).exists() else {}),
            }
            for name, path in (input_paths or {}).items()
            if path
        },
        "artifacts": {name: {"path": str(path), "sha256": sha256_file(path)} for name, path in artifacts.items()},
        "row_counts": {name: int(len(frame)) for name, frame in frames.items()},
    }


def build_shadow_live_ledger(
    *,
    rebalance_trades: pd.DataFrame,
    daily_returns: pd.DataFrame,
    prices: pd.DataFrame | None = None,
    output_dir: str | Path | None = None,
    candidate_runs: Iterable[str] | None = None,
    portfolio_nav: float = 0.0,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    forward_window_days: int = DEFAULT_FORWARD_WINDOW_DAYS,
    safe_haven: str = DEFAULT_SAFE_HAVEN,
    input_paths: Mapping[str, str | Path] | None = None,
) -> dict[str, pd.DataFrame]:
    runs = tuple(str(run) for run in candidate_runs or ())
    trades = _prepare_trades(rebalance_trades, candidate_runs=runs)
    daily = _prepare_daily_returns(daily_returns, candidate_runs=runs)
    price_frame = _prepare_prices(prices)
    nav = float(portfolio_nav)
    bps = float(slippage_bps)
    safe = _normalize_symbol(safe_haven)

    trade_rows: list[dict[str, object]] = []
    holdings_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    for run, run_trades in trades.groupby("Run", sort=True):
        current_weights: dict[str, float] = {}
        variant_type = str(run_trades["Variant Type"].dropna().iloc[0]) if not run_trades.empty else ""
        for date, date_trades in run_trades.groupby("Date", sort=True):
            date_trades = date_trades.sort_values("Symbol", kind="stable")
            for _, row in date_trades.iterrows():
                symbol = _normalize_symbol(row["Symbol"])
                target_weight = float(row["Target Weight"])
                if abs(target_weight) > 1e-12:
                    current_weights[symbol] = target_weight
                else:
                    current_weights.pop(symbol, None)

            gross_trade_notional = 0.0
            estimated_slippage_cost = 0.0
            buy_count = 0
            sell_count = 0
            for _, row in date_trades.iterrows():
                symbol = _normalize_symbol(row["Symbol"])
                delta = float(row["Trade Weight Delta"])
                abs_delta = abs(delta)
                side = "buy" if delta > 0.0 else "sell" if delta < 0.0 else "hold"
                buy_count += int(side == "buy")
                sell_count += int(side == "sell")
                notional = abs_delta * nav
                slippage_cost = notional * bps / 10_000.0
                gross_trade_notional += notional
                estimated_slippage_cost += slippage_cost
                signal_price, next_date, next_price = _price_lookup(price_frame, symbol=symbol, date=pd.Timestamp(date))
                signal_to_next = (
                    (next_price / signal_price) - 1.0
                    if pd.notna(signal_price) and pd.notna(next_price) and signal_price != 0.0
                    else float("nan")
                )
                trade_rows.append(
                    {
                        "Date": pd.Timestamp(date).date().isoformat(),
                        "Run": str(run),
                        "Variant Type": str(row.get("Variant Type", variant_type)),
                        "Symbol": symbol,
                        "Previous Weight": float(row["Previous Weight"]),
                        "Target Weight": float(row["Target Weight"]),
                        "Trade Weight Delta": delta,
                        "Abs Trade Weight Delta": abs_delta,
                        "Trade Side": side,
                        "Portfolio NAV": nav,
                        "Gross Trade Notional": notional,
                        "Estimated Slippage Cost": slippage_cost,
                        "Signal Price": signal_price,
                        "Next Session Date": next_date.date().isoformat() if next_date is not None else "",
                        "Next Session Price": next_price,
                        "Signal To Next Session Return": signal_to_next,
                    }
                )

            for symbol, target_weight in sorted(current_weights.items()):
                if abs(target_weight) <= 1e-12:
                    continue
                holdings_rows.append(
                    {
                        "Date": pd.Timestamp(date).date().isoformat(),
                        "Run": str(run),
                        "Variant Type": variant_type,
                        "Symbol": symbol,
                        "Target Weight": float(target_weight),
                        "Portfolio NAV": nav,
                        "Target Notional": float(target_weight) * nav,
                        "is_safe_haven": symbol == safe,
                    }
                )

            forward = _forward_returns_for_date(
                daily,
                run=str(run),
                date=pd.Timestamp(date),
                window_days=int(forward_window_days),
            )
            safe_weight = float(current_weights.get(safe, 0.0))
            stock_weight = sum(float(weight) for symbol, weight in current_weights.items() if symbol != safe)
            selected_symbols = ",".join(
                symbol for symbol, weight in sorted(current_weights.items()) if symbol != safe and abs(float(weight)) > 1e-12
            )
            summary_rows.append(
                {
                    "Date": pd.Timestamp(date).date().isoformat(),
                    "Run": str(run),
                    "Variant Type": variant_type,
                    "Portfolio NAV": nav,
                    "Trade Count": int(len(date_trades)),
                    "Buy Trade Count": int(buy_count),
                    "Sell Trade Count": int(sell_count),
                    "Gross Turnover Weight": float(date_trades["Abs Trade Weight Delta"].sum()),
                    "One Way Turnover Weight": float(date_trades["Abs Trade Weight Delta"].sum()) / 2.0,
                    "Gross Trade Notional": gross_trade_notional,
                    "Estimated Slippage Cost": estimated_slippage_cost,
                    "Stock Weight": float(stock_weight),
                    "Safe Haven Weight": safe_weight,
                    "Selected Symbols": selected_symbols,
                    **forward,
                    "diagnostic_scope": DIAGNOSTIC_SCOPE,
                }
            )

    frames = {
        "shadow_live_trade_ledger": pd.DataFrame(trade_rows, columns=list(TRADE_LEDGER_COLUMNS)),
        "shadow_live_holdings_ledger": pd.DataFrame(holdings_rows, columns=list(HOLDINGS_LEDGER_COLUMNS)),
        "shadow_live_rebalance_summary": pd.DataFrame(summary_rows, columns=list(SUMMARY_COLUMNS)),
    }
    if output_dir is not None:
        root = Path(output_dir)
        root.mkdir(parents=True, exist_ok=True)
        for name, frame in frames.items():
            frame.to_csv(root / f"{name}.csv", index=False)
        write_json(
            root / "shadow_live_ledger_manifest.json",
            _manifest_payload(
                output_dir=root,
                input_paths=input_paths,
                portfolio_nav=nav,
                slippage_bps=bps,
                forward_window_days=int(forward_window_days),
                safe_haven=safe,
                frames=frames,
            ),
        )
    return frames


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a research-only Russell Top50 shadow-live ledger from rebalance trades and daily returns."
    )
    parser.add_argument("--rebalance-trades", required=True, help="Input concentration_variant_rebalance_trades.csv")
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--prices", help="Optional long-form price history with Date/Symbol/Close columns")
    parser.add_argument("--candidate-runs", default="", help="Optional comma-separated run filter")
    parser.add_argument("--portfolio-nav", type=float, required=True)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--forward-window-days", type=int, default=DEFAULT_FORWARD_WINDOW_DAYS)
    parser.add_argument("--safe-haven", default=DEFAULT_SAFE_HAVEN)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidates = parse_csv_strings(args.candidate_runs, default=())
    result = build_shadow_live_ledger(
        rebalance_trades=read_table(args.rebalance_trades),
        daily_returns=read_table(args.daily_returns),
        prices=read_table(args.prices) if args.prices else None,
        output_dir=args.output_dir,
        candidate_runs=candidates,
        portfolio_nav=float(args.portfolio_nav),
        slippage_bps=float(args.slippage_bps),
        forward_window_days=int(args.forward_window_days),
        safe_haven=args.safe_haven,
        input_paths={
            "rebalance_trades": args.rebalance_trades,
            "daily_returns": args.daily_returns,
            **({"prices": args.prices} if args.prices else {}),
        },
    )
    print(result["shadow_live_rebalance_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote shadow-live ledger -> {Path(args.output_dir)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
