from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

from us_equity_strategies.strategies import qqq_tech_enhancement as strategy

from .tech_communication_pullback_snapshot import (
    FEATURE_SNAPSHOT_COLUMNS,
    _lookup_features,
    _normalize_price_groups,
    _precompute_feature_history,
    resolve_active_universe,
    read_table,
)

DEFAULT_PERIODS = (
    ("short", "2025-06-01", None),
    ("medium", "2023-06-01", None),
    ("long", "2018-01-01", None),
)
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_MIN_PRICE_USD = 10.0
DEFAULT_MIN_HISTORY_DAYS = 252

PERIOD_SUMMARY_COLUMNS = (
    "Period",
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Final Equity",
    "Benchmark Symbol",
    "Benchmark Total Return",
    "Benchmark CAGR",
    "Benchmark Corr",
    "Broad Benchmark Symbol",
    "Broad Benchmark Total Return",
    "Broad Benchmark CAGR",
    "Broad Benchmark Corr",
)


@dataclass(frozen=True)
class TechBacktestResult:
    summary: dict[str, float | str]
    portfolio_returns: pd.Series
    weights_history: pd.DataFrame
    turnover_history: pd.Series
    rebalance_log: pd.DataFrame


@dataclass(frozen=True)
class TechBacktestContext:
    prices: pd.DataFrame
    universe: pd.DataFrame
    feature_history_by_symbol: dict[str, pd.DataFrame]
    close_matrix: pd.DataFrame
    returns_matrix: pd.DataFrame


def _normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise RuntimeError("No usable price history")
    return frame.drop_duplicates(subset=["symbol", "as_of"], keep="last").sort_values(["as_of", "symbol"])


def _normalize_universe_history(universe_snapshot: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(universe_snapshot).copy()
    required = {"symbol", "sector"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"universe_snapshot missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    for column in ("start_date", "end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.tz_localize(None).dt.normalize()
    return frame.drop_duplicates().reset_index(drop=True)


def _parse_periods(raw_periods: str | Sequence[tuple[str, str, str | None]] | None) -> tuple[tuple[str, str, str | None], ...]:
    if raw_periods is None:
        return DEFAULT_PERIODS
    if not isinstance(raw_periods, str):
        return tuple((str(name), str(start), None if end is None else str(end)) for name, start, end in raw_periods)

    periods: list[tuple[str, str, str | None]] = []
    for item in raw_periods.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) not in {2, 3}:
            raise ValueError("periods must use name:start[:end] entries")
        name = parts[0].strip()
        start = parts[1].strip()
        end = parts[2].strip() if len(parts) == 3 and parts[2].strip() else None
        if not name or not start:
            raise ValueError("period name and start date are required")
        periods.append((name, start, end))
    if not periods:
        raise ValueError("at least one period is required")
    return tuple(periods)


def build_monthly_rebalance_dates(index: pd.DatetimeIndex) -> set[pd.Timestamp]:
    series = pd.Series(index, index=index)
    grouped = series.groupby(index.to_period("M")).max()
    return set(pd.to_datetime(grouped.values))


def _compute_turnover(previous_weights: dict[str, float], new_weights: dict[str, float]) -> float:
    symbols = set(previous_weights) | set(new_weights)
    return 0.5 * sum(abs(new_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0)) for symbol in symbols)


def _load_runtime_params(config_path: str | Path | None) -> dict[str, object]:
    runtime_params = strategy.load_runtime_parameters(config_path=config_path)
    for runtime_only_key in ("execution_cash_reserve_ratio", "runtime_execution_window_trading_days", "run_as_of"):
        runtime_params.pop(runtime_only_key, None)
    return runtime_params


def build_backtest_context(price_history: pd.DataFrame, universe_snapshot: pd.DataFrame) -> TechBacktestContext:
    prices = _normalize_price_history(price_history)
    universe = _normalize_universe_history(universe_snapshot)
    max_as_of = prices["as_of"].max()
    price_groups, _ = _normalize_price_groups(prices, as_of=max_as_of)
    feature_history_by_symbol = _precompute_feature_history(price_groups)
    close_matrix = (
        prices.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    returns_matrix = close_matrix.pct_change().fillna(0.0)
    return TechBacktestContext(
        prices=prices,
        universe=universe,
        feature_history_by_symbol=feature_history_by_symbol,
        close_matrix=close_matrix,
        returns_matrix=returns_matrix,
    )


def _build_feature_snapshot_for_backtest(
    context: TechBacktestContext,
    as_of: pd.Timestamp,
    runtime_params: dict[str, object],
    *,
    min_price_usd: float,
    min_history_days: int,
) -> pd.DataFrame:
    benchmark_symbol = str(runtime_params.get("benchmark_symbol") or strategy.BENCHMARK_SYMBOL).upper()
    safe_haven = str(runtime_params.get("safe_haven") or strategy.SAFE_HAVEN).upper()
    sector_whitelist = tuple(runtime_params.get("sector_whitelist") or strategy.DEFAULT_SECTOR_WHITELIST)
    min_adv20_usd = float(runtime_params.get("min_adv20_usd", strategy.DEFAULT_MIN_ADV20_USD))

    active = resolve_active_universe(context.universe, as_of)
    if sector_whitelist:
        active = active.loc[active["sector"].isin(sector_whitelist)].copy()
    active = active.drop_duplicates(subset=["symbol"], keep="last")
    sector_map = dict(zip(active["symbol"], active["sector"]))
    symbols = active["symbol"].tolist()
    for extra in (benchmark_symbol, "SPY", safe_haven):
        if extra and extra not in symbols:
            symbols.append(extra)

    rows = [
        _lookup_features(
            symbol,
            as_of,
            context.feature_history_by_symbol,
            sector=sector_map.get(
                symbol,
                "benchmark" if symbol in {benchmark_symbol, "SPY"} else "defense" if symbol == safe_haven else "unknown",
            ),
        )
        for symbol in symbols
    ]
    frame = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
    feature_columns = [
        "mom_6_1",
        "mom_12_1",
        "sma20_gap",
        "sma50_gap",
        "sma200_gap",
        "ma50_over_ma200",
        "vol_63",
        "maxdd_126",
        "breakout_252",
        "dist_63_high",
        "dist_126_high",
        "rebound_20",
    ]
    frame["base_eligible"] = (
        ~frame["symbol"].isin([benchmark_symbol, "SPY", safe_haven])
        & frame["history_days"].ge(int(min_history_days))
        & frame["close"].gt(float(min_price_usd))
        & frame["adv20_usd"].ge(min_adv20_usd)
        & frame[feature_columns].notna().all(axis=1)
    )
    return frame.loc[:, FEATURE_SNAPSHOT_COLUMNS].reset_index(drop=True)


def _benchmark_stats(portfolio_returns: pd.Series, benchmark_returns: pd.Series | None) -> tuple[float, float, float]:
    if benchmark_returns is None or benchmark_returns.dropna().empty:
        return float("nan"), float("nan"), float("nan")
    aligned = pd.concat(
        [portfolio_returns.rename("portfolio"), benchmark_returns.rename("benchmark")],
        axis=1,
        sort=False,
    ).dropna()
    if aligned.empty:
        return float("nan"), float("nan"), float("nan")
    equity_curve = (1.0 + aligned["benchmark"]).cumprod()
    years = max((aligned.index[-1] - aligned.index[0]).days / 365.25, 1 / 365.25)
    total_return = float(equity_curve.iloc[-1] - 1.0)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    corr = float(aligned["portfolio"].corr(aligned["benchmark"]))
    return total_return, cagr, corr


def summarize_backtest(
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    *,
    benchmark_symbol: str,
    benchmark_returns: pd.Series | None,
    broad_benchmark_symbol: str,
    broad_benchmark_returns: pd.Series | None,
    safe_haven: str,
) -> dict[str, float | str]:
    returns = portfolio_returns.dropna()
    if returns.empty:
        raise RuntimeError("No portfolio returns to summarize")
    equity_curve = (1.0 + returns).cumprod()
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    benchmark_total_return, benchmark_cagr, benchmark_corr = _benchmark_stats(returns, benchmark_returns)
    broad_total_return, broad_cagr, broad_corr = _benchmark_stats(returns, broad_benchmark_returns)

    changes = weights_history.fillna(0.0).diff().fillna(0.0)
    if not changes.empty:
        changes.iloc[0] = 0.0
    daily_turnover = 0.5 * changes.abs().sum(axis=1)
    stock_columns = [column for column in weights_history.columns if column != safe_haven]

    return {
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Total Return": float(equity_curve.iloc[-1] - 1.0),
        "CAGR": float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0),
        "Max Drawdown": float(drawdown.min()),
        "Volatility": volatility,
        "Sharpe": float(returns.mean() / std * np.sqrt(252)) if std else float("nan"),
        "Rebalances/Year": float((daily_turnover > 1e-12).sum() / years),
        "Turnover/Year": float(daily_turnover.sum() / years),
        "Avg Stock Exposure": float(weights_history[stock_columns].fillna(0.0).sum(axis=1).mean()) if stock_columns else 0.0,
        "Final Equity": float(equity_curve.iloc[-1]),
        "Benchmark Symbol": benchmark_symbol,
        "Benchmark Total Return": benchmark_total_return,
        "Benchmark CAGR": benchmark_cagr,
        "Benchmark Corr": benchmark_corr,
        "Broad Benchmark Symbol": broad_benchmark_symbol,
        "Broad Benchmark Total Return": broad_total_return,
        "Broad Benchmark CAGR": broad_cagr,
        "Broad Benchmark Corr": broad_corr,
    }


def run_backtest(
    context: TechBacktestContext,
    *,
    start_date: str,
    end_date: str | None,
    runtime_params: dict[str, object],
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    min_price_usd: float = DEFAULT_MIN_PRICE_USD,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    broad_benchmark_symbol: str = "SPY",
) -> TechBacktestResult:
    benchmark_symbol = str(runtime_params.get("benchmark_symbol") or strategy.BENCHMARK_SYMBOL).upper()
    safe_haven = str(runtime_params.get("safe_haven") or strategy.SAFE_HAVEN).upper()
    if benchmark_symbol not in context.returns_matrix.columns:
        raise RuntimeError(f"{benchmark_symbol} benchmark returns are required for the backtest")
    if safe_haven not in context.returns_matrix.columns:
        context.returns_matrix[safe_haven] = 0.0
        context.close_matrix[safe_haven] = 1.0

    index = context.close_matrix.loc[context.close_matrix.index >= pd.Timestamp(start_date).normalize()].index
    if end_date:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("No usable backtest dates in selected period")

    rebalance_dates = build_monthly_rebalance_dates(index)
    symbols = sorted(set(context.close_matrix.columns) | {safe_haven})
    weights_history = pd.DataFrame(0.0, index=index, columns=symbols)
    portfolio_returns = pd.Series(0.0, index=index, name="portfolio")
    turnover_history = pd.Series(0.0, index=index, name="turnover")
    rebalance_rows: list[dict[str, object]] = []
    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()

    for idx in range(len(index) - 1):
        date = index[idx]
        next_date = index[idx + 1]
        if date in rebalance_dates:
            snapshot = _build_feature_snapshot_for_backtest(
                context,
                date,
                runtime_params,
                min_price_usd=min_price_usd,
                min_history_days=min_history_days,
            )
            target_weights, signal, metadata = strategy.build_target_weights(
                snapshot,
                current_holdings,
                **runtime_params,
            )
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_haven
            }
            rebalance_rows.append(
                {
                    "as_of": str(date.date()),
                    "effective_date": str(next_date.date()),
                    "turnover": turnover,
                    "signal": signal,
                    "regime": metadata.get("regime"),
                    "breadth_ratio": metadata.get("breadth_ratio"),
                    "target_stock_weight": metadata.get("target_stock_weight"),
                    "realized_stock_weight": metadata.get("realized_stock_weight"),
                    "safe_haven_weight": metadata.get("safe_haven_weight"),
                    "selected_count": metadata.get("selected_count"),
                    "candidate_count": metadata.get("candidate_count"),
                    "selected_symbols": "|".join(str(symbol) for symbol in metadata.get("selected_symbols", ())),
                }
            )

        for symbol, weight in current_weights.items():
            weights_history.at[date, symbol] = weight
        next_returns = context.returns_matrix.loc[next_date]
        gross_return = sum(
            weight * float(next_returns.get(symbol, 0.0))
            for symbol, weight in current_weights.items()
        )
        cost = turnover_history.at[next_date] * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    for symbol, weight in current_weights.items():
        weights_history.at[index[-1], symbol] = weight

    active_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = summarize_backtest(
        portfolio_returns,
        active_weights,
        benchmark_symbol=benchmark_symbol,
        benchmark_returns=context.returns_matrix.get(benchmark_symbol),
        broad_benchmark_symbol=broad_benchmark_symbol,
        broad_benchmark_returns=context.returns_matrix.get(broad_benchmark_symbol),
        safe_haven=safe_haven,
    )
    return TechBacktestResult(
        summary=summary,
        portfolio_returns=portfolio_returns,
        weights_history=active_weights,
        turnover_history=turnover_history,
        rebalance_log=pd.DataFrame(rebalance_rows),
    )


def run_period_backtests(
    price_history: pd.DataFrame,
    universe_snapshot: pd.DataFrame,
    *,
    periods: str | Sequence[tuple[str, str, str | None]] | None = None,
    config_path: str | Path | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    min_price_usd: float = DEFAULT_MIN_PRICE_USD,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    broad_benchmark_symbol: str = "SPY",
) -> dict[str, TechBacktestResult]:
    runtime_params = _load_runtime_params(config_path)
    context = build_backtest_context(price_history, universe_snapshot)
    return {
        name: run_backtest(
            context,
            start_date=start,
            end_date=end,
            runtime_params=dict(runtime_params),
            turnover_cost_bps=turnover_cost_bps,
            min_price_usd=min_price_usd,
            min_history_days=min_history_days,
            broad_benchmark_symbol=broad_benchmark_symbol,
        )
        for name, start, end in _parse_periods(periods)
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest tech_communication_pullback_enhancement over named periods.")
    parser.add_argument("--prices", required=True, help="Primary price history file")
    parser.add_argument(
        "--extra-prices",
        action="append",
        default=[],
        help="Optional extra price history file; can be repeated, useful when primary universe history lacks QQQ.",
    )
    parser.add_argument("--universe", required=True, help="Universe history file")
    parser.add_argument("--config-path", help="Optional runtime config path")
    parser.add_argument("--periods", help="Comma-separated name:start[:end] entries")
    parser.add_argument("--output-dir", help="Optional output directory")
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--min-price-usd", type=float, default=DEFAULT_MIN_PRICE_USD)
    parser.add_argument("--min-history-days", type=int, default=DEFAULT_MIN_HISTORY_DAYS)
    parser.add_argument("--broad-benchmark-symbol", default="SPY")
    return parser


def _write_outputs(output_dir: Path, results: dict[str, TechBacktestResult]) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    for period, result in results.items():
        summary_rows.append({"Period": period, **result.summary})
        result.portfolio_returns.rename("portfolio_return").to_csv(output_dir / f"{period}_portfolio_returns.csv")
        result.weights_history.to_csv(output_dir / f"{period}_weights_history.csv")
        result.turnover_history.rename("turnover").to_csv(output_dir / f"{period}_turnover_history.csv")
        result.rebalance_log.to_csv(output_dir / f"{period}_rebalance_log.csv", index=False)
    summary = pd.DataFrame(summary_rows)
    summary = summary.loc[:, [column for column in PERIOD_SUMMARY_COLUMNS if column in summary.columns]]
    summary.to_csv(output_dir / "period_summary.csv", index=False)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    price_frames = [read_table(args.prices), *(read_table(path) for path in args.extra_prices)]
    prices = pd.concat(price_frames, ignore_index=True)
    results = run_period_backtests(
        prices,
        read_table(args.universe),
        periods=args.periods,
        config_path=args.config_path,
        turnover_cost_bps=args.turnover_cost_bps,
        min_price_usd=args.min_price_usd,
        min_history_days=args.min_history_days,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
    )
    summary = pd.DataFrame(
        [{"Period": period, **result.summary} for period, result in results.items()]
    )
    summary = summary.loc[:, [column for column in PERIOD_SUMMARY_COLUMNS if column in summary.columns]]
    print(summary.to_string(index=False))
    if args.output_dir:
        _write_outputs(Path(args.output_dir), results)
        print(f"wrote backtest outputs -> {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
