from __future__ import annotations

import argparse
import inspect
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest
from us_equity_strategies.strategies.soxl_soxx_trend_income import build_rebalance_plan

from .artifacts import write_json
from .yfinance_prices import download_price_history

PROFILE = "soxl_soxx_trend_income"
MANAGED_SYMBOLS = ("SOXL", "SOXX", "BOXX", "QQQI", "SPYI")
DEFAULT_INITIAL_EQUITY_USD = 100_000.0
DEFAULT_PRICE_START = "2023-01-01"
DEFAULT_BACKTEST_START = "2024-01-30"
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_DOWNLOAD_SYMBOLS = list(MANAGED_SYMBOLS)
DEFAULT_OUTPUT_COLUMNS = (
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Final Equity",
)


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _build_price_frame(price_history) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.loc[frame["symbol"].ne("")].dropna(subset=["as_of", "close"])
    return (
        frame.loc[:, ["symbol", "as_of", "close"]]
        .drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def _build_close_matrix(price_history: pd.DataFrame) -> pd.DataFrame:
    close_matrix = (
        price_history.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    return close_matrix


def _build_indicator_history(close_matrix: pd.DataFrame) -> dict[str, pd.DataFrame]:
    indicators: dict[str, pd.DataFrame] = {}
    for symbol in ("SOXL", "SOXX"):
        if symbol not in close_matrix.columns:
            continue
        close = pd.to_numeric(close_matrix[symbol], errors="coerce")
        history = pd.DataFrame(
            {
                "price": close,
                "ma_trend": close.rolling(int(soxl_soxx_trend_income_manifest.default_config["trend_ma_window"])).mean(),
            },
            index=close.index,
        )
        if symbol == "SOXX":
            ma20 = close.rolling(20).mean()
            history["ma20"] = ma20
            history["ma20_slope"] = ma20.diff()
        indicators[symbol.lower()] = history
    return indicators


def _strategy_kwargs() -> dict[str, object]:
    config = soxl_soxx_trend_income_manifest.default_config
    return {
        "trend_ma_window": int(config["trend_ma_window"]),
        "cash_reserve_ratio": float(config["cash_reserve_ratio"]),
        "min_trade_ratio": float(config["min_trade_ratio"]),
        "min_trade_floor": float(config["min_trade_floor"]),
        "rebalance_threshold_ratio": float(config["rebalance_threshold_ratio"]),
        "small_account_deploy_ratio": float(config.get("small_account_deploy_ratio", 0.6)),
        "mid_account_deploy_ratio": float(config.get("mid_account_deploy_ratio", 0.57)),
        "large_account_deploy_ratio": float(config.get("large_account_deploy_ratio", 0.5)),
        "trade_layer_decay_coeff": float(config.get("trade_layer_decay_coeff", 0.04)),
        "income_layer_start_usd": float(config["income_layer_start_usd"]),
        "income_layer_max_ratio": float(config["income_layer_max_ratio"]),
        "income_layer_qqqi_weight": float(config["income_layer_qqqi_weight"]),
        "income_layer_spyi_weight": float(config["income_layer_spyi_weight"]),
        "trend_entry_buffer": float(config.get("trend_entry_buffer", 0.03)),
        "trend_mid_buffer": float(config.get("trend_mid_buffer", 0.06)),
        "trend_exit_buffer": float(config.get("trend_exit_buffer", 0.03)),
        "attack_allocation_mode": str(config.get("attack_allocation_mode", "soxx_gate_tiered_blend")),
        "blend_gate_trend_source": str(config.get("blend_gate_trend_source", "SOXX")),
        "blend_gate_soxl_weight": float(config.get("blend_gate_soxl_weight", 0.75)),
        "blend_gate_mid_soxl_weight": float(config.get("blend_gate_mid_soxl_weight", 0.65)),
        "blend_gate_active_soxx_weight": float(config.get("blend_gate_active_soxx_weight", 0.20)),
        "blend_gate_defensive_soxx_weight": float(config.get("blend_gate_defensive_soxx_weight", 0.15)),
        "blend_gate_rsi_cap_enabled": bool(config.get("blend_gate_rsi_cap_enabled", False)),
        "blend_gate_rsi_threshold": float(config.get("blend_gate_rsi_threshold", 70.0)),
        "blend_gate_bollinger_cap_enabled": bool(config.get("blend_gate_bollinger_cap_enabled", False)),
        "blend_gate_overlay_stack_triggers": bool(config.get("blend_gate_overlay_stack_triggers", False)),
    }


def _call_strategy_kwargs() -> dict[str, object]:
    kwargs = _strategy_kwargs()
    supported = set(inspect.signature(build_rebalance_plan).parameters)
    supported.discard("indicators")
    supported.discard("account_state")
    supported.discard("translator")
    return {key: value for key, value in kwargs.items() if key in supported}


def _indicator_snapshot_at(
    indicator_history: Mapping[str, pd.DataFrame],
    as_of: pd.Timestamp,
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for symbol, frame in indicator_history.items():
        if as_of not in frame.index:
            continue
        row = frame.loc[as_of]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[-1]
        payload: dict[str, float] = {}
        for key in row.index:
            value = row[key]
            if pd.notna(value):
                payload[key] = float(value)
        result[symbol] = payload
    return result


def _build_account_state(
    *,
    weights: Mapping[str, float],
    equity: float,
    close_prices: Mapping[str, float],
    cash_symbol: str = "BOXX",
) -> dict[str, object]:
    market_values = {symbol: float(equity) * float(weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
    quantities = {}
    sellable_quantities = {}
    for symbol in MANAGED_SYMBOLS:
        price = float(close_prices.get(symbol, 0.0) or 0.0)
        market_value = market_values.get(symbol, 0.0)
        qty = market_value / price if price > 0 else 0.0
        quantities[symbol] = int(round(qty))
        sellable_quantities[symbol] = int(round(qty))
    return {
        "available_cash": float(equity),
        "market_values": market_values,
        "quantities": quantities,
        "sellable_quantities": sellable_quantities,
        "total_strategy_equity": float(equity),
        "cash_sweep_symbol": cash_symbol,
    }


def _execute_rebalance(
    *,
    current_weights: Mapping[str, float],
    target_values: Mapping[str, float],
    equity: float,
    threshold_value: float,
    current_min_trade: float,
    turnover_cost_bps: float,
) -> tuple[dict[str, float], float, float]:
    current_market_values = {symbol: float(equity) * float(current_weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
    next_market_values = dict(current_market_values)

    sell_order = ("SOXL", "SOXX", "QQQI", "SPYI", "BOXX")
    buy_order = ("QQQI", "SPYI", "SOXL", "SOXX", "BOXX")

    cash = float(equity) - sum(current_market_values.values())
    if abs(cash) < 1e-9:
        cash = 0.0

    for symbol in sell_order:
        current = current_market_values.get(symbol, 0.0)
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff >= 0:
            continue
        if abs(diff) <= threshold_value or abs(diff) <= current_min_trade:
            continue
        next_market_values[symbol] = target
        cash -= diff

    for symbol in buy_order:
        current = next_market_values.get(symbol, current_market_values.get(symbol, 0.0))
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff <= 0:
            continue
        if diff <= threshold_value or diff <= current_min_trade:
            continue
        buy_value = min(diff, cash)
        if buy_value <= 0:
            continue
        next_market_values[symbol] = current + buy_value
        cash -= buy_value

    new_equity_before_cost = cash + sum(next_market_values.values())
    if new_equity_before_cost <= 0:
        return dict(current_weights), 0.0, 0.0

    turnover = 0.5 * sum(
        abs(float(next_market_values.get(symbol, 0.0)) - float(current_market_values.get(symbol, 0.0)))
        for symbol in MANAGED_SYMBOLS
    ) / float(equity)
    cost = float(equity) * turnover * (float(turnover_cost_bps) / 10_000.0)
    cash = max(0.0, cash - cost)
    new_equity = cash + sum(next_market_values.values())
    if new_equity <= 0:
        return dict(current_weights), 0.0, 0.0

    new_weights = {symbol: float(next_market_values.get(symbol, 0.0)) / new_equity for symbol in MANAGED_SYMBOLS}
    new_weights["__cash__"] = cash / new_equity
    return new_weights, turnover, new_equity


def _summarize_returns(
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
) -> dict[str, float | str]:
    returns = portfolio_returns.dropna()
    if returns.empty:
        raise RuntimeError("No portfolio returns to summarize")

    equity_curve = (1.0 + returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    sharpe = float(returns.mean() / std * np.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    changes = weights_history.fillna(0.0).diff().fillna(0.0)
    if not changes.empty:
        changes.iloc[0] = 0.0
    daily_turnover = 0.5 * changes.abs().sum(axis=1)
    rebalances_per_year = float((daily_turnover > 1e-12).sum() / years)
    turnover_per_year = float(daily_turnover.sum() / years)
    stock_columns = [column for column in weights_history.columns if column not in {"BOXX", "__cash__"}]
    avg_stock_exposure = (
        float(weights_history[stock_columns].fillna(0.0).sum(axis=1).mean()) if stock_columns else 0.0
    )

    return {
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Rebalances/Year": rebalances_per_year,
        "Turnover/Year": turnover_per_year,
        "Avg Stock Exposure": avg_stock_exposure,
        "Final Equity": float(equity_curve.iloc[-1]),
    }


def run_backtest(
    price_history,
    *,
    initial_equity: float = DEFAULT_INITIAL_EQUITY_USD,
    start_date: str = DEFAULT_BACKTEST_START,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    prices = _build_price_frame(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")

    close_matrix = _build_close_matrix(prices)
    indicator_history = _build_indicator_history(close_matrix)
    index = close_matrix.index
    index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")

    backtest_index = index[:-1]
    weights_history = pd.DataFrame(0.0, index=index, columns=[*MANAGED_SYMBOLS, "__cash__"])
    portfolio_returns = pd.Series(index=index, dtype=float, name="portfolio_return")
    turnover_history = pd.Series(index=index, dtype=float, name="turnover")
    signal_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []

    first_prices = close_matrix.loc[index[0]]
    initial_boxx_price = float(first_prices.get("BOXX", np.nan))
    if not np.isfinite(initial_boxx_price) or initial_boxx_price <= 0:
        raise RuntimeError("BOXX price unavailable at backtest start")
    initial_weights = {symbol: 0.0 for symbol in MANAGED_SYMBOLS}
    initial_weights["BOXX"] = 1.0
    current_weights = dict(initial_weights)
    current_equity = float(initial_equity)

    for as_of in backtest_index:
        next_as_of = index[index.get_loc(as_of) + 1]
        close_row = close_matrix.loc[as_of]
        next_close_row = close_matrix.loc[next_as_of]
        if pd.isna(close_row.get("SOXL")) or pd.isna(close_row.get("SOXX")):
            continue

        account_state = _build_account_state(
            weights=current_weights,
            equity=current_equity,
            close_prices={symbol: float(close_row.get(symbol, np.nan)) for symbol in MANAGED_SYMBOLS},
        )
        indicators = _indicator_snapshot_at(indicator_history, as_of)
        if "soxl" not in indicators or "soxx" not in indicators:
            continue

        try:
            plan = build_rebalance_plan(
                indicators,
                account_state,
                translator=lambda key, **kwargs: key,
                **_call_strategy_kwargs(),
            )
        except Exception:
            continue

        target_values = dict(plan["targets"])
        threshold_value = float(plan["threshold_value"])
        current_min_trade = float(plan["current_min_trade"])
        signal_rows.append(
            {
                "as_of": as_of,
                "signal_date": as_of,
                "effective_date": next_as_of,
                "blend_tier": plan.get("blend_tier"),
                "base_blend_tier": plan.get("base_blend_tier"),
                "allocation_mode": plan.get("allocation_mode"),
                "trend_symbol": plan.get("trend_symbol"),
                "trend_price": plan.get("trend_price"),
                "trend_ma": plan.get("trend_ma"),
                "trend_entry_line": plan.get("trend_entry_line"),
                "trend_mid_line": plan.get("trend_mid_line"),
                "trend_exit_line": plan.get("trend_exit_line"),
                "reserved_cash": plan.get("reserved_cash"),
                "investable_cash": plan.get("investable_cash"),
                "threshold_value": threshold_value,
                "current_min_trade": current_min_trade,
                "total_equity": current_equity,
            }
        )

        next_weights, turnover, next_equity = _execute_rebalance(
            current_weights=current_weights,
            target_values=target_values,
            equity=current_equity,
            threshold_value=threshold_value,
            current_min_trade=current_min_trade,
            turnover_cost_bps=float(turnover_cost_bps),
        )
        if turnover > 0:
            for symbol in MANAGED_SYMBOLS:
                old = float(current_weights.get(symbol, 0.0))
                new = float(next_weights.get(symbol, 0.0))
                if abs(new - old) > 1e-12:
                    trade_rows.append(
                        {
                            "signal_date": as_of,
                            "effective_date": next_as_of,
                            "symbol": symbol,
                            "old_weight": old,
                            "new_weight": new,
                            "delta_weight": new - old,
                        }
                    )
        turnover_history.at[next_as_of] = turnover
        current_weights = {symbol: float(next_weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
        current_weights["__cash__"] = float(next_weights.get("__cash__", 0.0))
        current_equity = float(next_equity)

        next_market_values = {
            symbol: float(current_equity) * float(current_weights.get(symbol, 0.0))
            for symbol in MANAGED_SYMBOLS
        }
        next_cash = float(current_equity) * float(current_weights.get("__cash__", 0.0))
        equity_after_return = next_cash + sum(
            float(next_market_values[symbol]) * (float(next_close_row.get(symbol, np.nan)) / float(close_row.get(symbol)))
            if symbol in next_close_row and pd.notna(close_row.get(symbol)) and float(close_row.get(symbol)) > 0
            else float(next_market_values[symbol])
            for symbol in MANAGED_SYMBOLS
        )
        if current_equity > 0:
            portfolio_returns.at[next_as_of] = equity_after_return / current_equity - 1.0
        for symbol in MANAGED_SYMBOLS:
            weights_history.at[next_as_of, symbol] = float(current_weights.get(symbol, 0.0))
        weights_history.at[next_as_of, "__cash__"] = float(current_weights.get("__cash__", 0.0))
        current_equity = float(equity_after_return)

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = _summarize_returns(portfolio_returns, used_weights)
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "trades": pd.DataFrame(trade_rows),
        "signal_history": pd.DataFrame(signal_rows),
    }


def _format_summary(summary: Mapping[str, float | str]) -> pd.DataFrame:
    return pd.DataFrame([{column: summary.get(column) for column in DEFAULT_OUTPUT_COLUMNS}])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtest for soxl_soxx_trend_income.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet)")
    input_group.add_argument("--download", action="store_true", help="Download price history with yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START, help="Download start date used with --download")
    parser.add_argument("--price-end", help="Download end date used with --download")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_BACKTEST_START, help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--output-dir", help="Optional output directory for research artifacts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir) if args.output_dir else None
    if args.download:
        if output_dir is None:
            raise EnvironmentError("--output-dir is required when --download is used")
        prices = download_price_history(
            DEFAULT_DOWNLOAD_SYMBOLS,
            start=args.price_start,
            end=args.price_end,
            chunk_size=25,
        )
    else:
        prices = pd.read_csv(args.prices)

    result = run_backtest(
        prices,
        initial_equity=float(args.initial_equity),
        start_date=args.start_date,
        end_date=args.end_date,
        turnover_cost_bps=float(args.turnover_cost_bps),
    )

    summary_frame = _format_summary(result["summary"])
    print(summary_frame.to_string(index=False))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        _build_price_frame(prices).to_csv(output_dir / "price_history.csv", index=False)
        summary_frame.to_csv(output_dir / "summary.csv", index=False)
        result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
        result["weights_history"].to_csv(output_dir / "weights_history.csv")
        result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
        result["trades"].to_csv(output_dir / "trades.csv", index=False)
        result["signal_history"].to_csv(output_dir / "signal_history.csv", index=False)
        write_json(output_dir / "backtest_config.json", {
            "profile": PROFILE,
            "initial_equity": float(args.initial_equity),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "turnover_cost_bps": float(args.turnover_cost_bps),
        })
        print(f"wrote research backtest outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
