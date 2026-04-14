from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    POOL_SYMBOLS,
    SAFE_HAVEN,
    build_static_universe,
    build_feature_snapshot_for_backtest,
    build_monthly_rebalance_dates,
    build_target_weights,
    prepare_research_input_data,
    resolve_pool_symbols,
    resolve_active_universe,
    summarize_returns,
    _build_close_and_returns,
    _compute_turnover,
    _normalize_price_history,
    _precompute_symbol_feature_history,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_POOLS = ("mag7", "expanded")
DEFAULT_TOP_N_VALUES = (3, 4, 5)
DEFAULT_SINGLE_NAME_CAP_VALUES = (0.25, 0.30, 0.35)
DEFAULT_DEFENSE_MODES = ("on", "off")
ROBUSTNESS_SUMMARY_COLUMNS = (
    "Rank",
    "Run",
    "Pool",
    "Defense Mode",
    "Top N",
    "Single Name Cap",
    "Hold Buffer",
    "Turnover Cost Bps",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Total Return",
    "Final Equity",
    "Benchmark Symbol",
    "Benchmark Total Return",
    "Benchmark Corr",
    "Broad Benchmark Symbol",
    "Broad Benchmark Total Return",
    "Equal Weight Pool Total Return",
    "Start",
    "End",
)


@dataclass(frozen=True)
class _PreparedBacktestData:
    close_matrix: pd.DataFrame
    returns_matrix: pd.DataFrame
    feature_history_by_symbol: dict[str, pd.DataFrame]
    index: pd.DatetimeIndex
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class _PoolContext:
    pool: str
    snapshots: dict[pd.Timestamp, pd.DataFrame]
    returns_matrix: pd.DataFrame
    index: pd.DatetimeIndex
    symbols: tuple[str, ...]
    benchmark_returns: pd.Series | None
    broad_benchmark_returns: pd.Series | None
    equal_weight_pool_returns: pd.Series


def parse_csv_strings(raw_value: str | Iterable[str] | None, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    normalized = tuple(dict.fromkeys(str(value).strip().lower() for value in values if str(value).strip()))
    return normalized or default


def parse_csv_ints(raw_value: str | Iterable[int] | None, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed = tuple(dict.fromkeys(int(str(value).strip()) for value in values if str(value).strip()))
    return parsed or default


def parse_csv_floats(raw_value: str | Iterable[float] | None, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed = tuple(dict.fromkeys(float(str(value).strip()) for value in values if str(value).strip()))
    return parsed or default


def _validate_defense_modes(defense_modes: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(mode).strip().lower() for mode in defense_modes if str(mode).strip()))
    invalid = sorted(set(normalized) - {"on", "off"})
    if invalid:
        invalid_text = ", ".join(invalid)
        raise ValueError(f"Unsupported defense mode(s): {invalid_text}; expected on/off")
    return normalized or DEFAULT_DEFENSE_MODES


def _collect_download_symbols(pools: Iterable[str]) -> tuple[str, ...]:
    symbols: list[str] = []
    for pool in pools:
        symbols.extend(resolve_pool_symbols(pool))
    return tuple(dict.fromkeys(symbols))


def _defense_params(
    defense_mode: str,
    *,
    risk_on_exposure: float,
    soft_defense_exposure: float,
    hard_defense_exposure: float,
) -> dict[str, float]:
    if defense_mode == "off":
        return {
            "risk_on_exposure": float(risk_on_exposure),
            "soft_defense_exposure": float(risk_on_exposure),
            "hard_defense_exposure": float(risk_on_exposure),
        }
    return {
        "risk_on_exposure": float(risk_on_exposure),
        "soft_defense_exposure": float(soft_defense_exposure),
        "hard_defense_exposure": float(hard_defense_exposure),
    }


def _run_id(*, pool: str, top_n: int, single_name_cap: float, defense_mode: str) -> str:
    cap_text = f"{single_name_cap:.2f}".replace("0.", "").replace(".", "")
    return f"{pool}_top{top_n}_cap{cap_text}_defense_{defense_mode}"


def _prepare_backtest_data(
    price_history,
    *,
    start_date: str | None,
    end_date: str | None,
    safe_haven: str,
) -> _PreparedBacktestData:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")

    feature_history_by_symbol = _precompute_symbol_feature_history(prices)
    close_matrix, returns_matrix = _build_close_and_returns(prices)
    if safe_haven not in close_matrix.columns:
        close_matrix[safe_haven] = 1.0
        returns_matrix[safe_haven] = 0.0
    index = close_matrix.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")
    return _PreparedBacktestData(
        close_matrix=close_matrix,
        returns_matrix=returns_matrix,
        feature_history_by_symbol=feature_history_by_symbol,
        index=index,
        symbols=tuple(sorted(set(close_matrix.columns) | {safe_haven})),
    )


def _build_pool_context(
    prepared: _PreparedBacktestData,
    *,
    pool: str,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    min_price_usd: float,
    min_adv20_usd: float,
    min_history_days: int,
) -> _PoolContext:
    universe = build_static_universe(pool)
    rebalance_dates = build_monthly_rebalance_dates(prepared.index)
    snapshots = {
        date: build_feature_snapshot_for_backtest(
            date,
            resolve_active_universe(universe, date),
            prepared.feature_history_by_symbol,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            min_price_usd=min_price_usd,
            min_adv20_usd=min_adv20_usd,
            min_history_days=min_history_days,
        )
        for date in sorted(rebalance_dates)
    }

    pool_symbols = tuple(dict.fromkeys(universe["symbol"].astype(str)))
    available_pool_symbols = [symbol for symbol in pool_symbols if symbol in prepared.returns_matrix.columns]
    equal_weight_pool_returns = (
        prepared.returns_matrix.loc[prepared.index, available_pool_symbols].mean(axis=1)
        if available_pool_symbols
        else pd.Series(index=prepared.index, dtype=float)
    )
    benchmark_returns = (
        prepared.returns_matrix[benchmark_symbol].reindex(prepared.index)
        if benchmark_symbol in prepared.returns_matrix
        else None
    )
    broad_benchmark_returns = (
        prepared.returns_matrix[broad_benchmark_symbol].reindex(prepared.index)
        if broad_benchmark_symbol in prepared.returns_matrix
        else None
    )
    return _PoolContext(
        pool=pool,
        snapshots=snapshots,
        returns_matrix=prepared.returns_matrix,
        index=prepared.index,
        symbols=prepared.symbols,
        benchmark_returns=benchmark_returns,
        broad_benchmark_returns=broad_benchmark_returns,
        equal_weight_pool_returns=equal_weight_pool_returns,
    )


def _run_context_backtest(
    context: _PoolContext,
    *,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    top_n: int,
    hold_buffer: int,
    single_name_cap: float,
    hold_bonus: float,
    risk_on_exposure: float,
    soft_defense_exposure: float,
    hard_defense_exposure: float,
    soft_breadth_threshold: float,
    hard_breadth_threshold: float,
    turnover_cost_bps: float,
) -> dict[str, float | str]:
    weights_history = pd.DataFrame(0.0, index=context.index, columns=context.symbols)
    portfolio_returns = pd.Series(0.0, index=context.index, name="portfolio_return")
    turnover_history = pd.Series(0.0, index=context.index, name="turnover")

    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()

    for idx in range(len(context.index) - 1):
        date = context.index[idx]
        next_date = context.index[idx + 1]

        snapshot = context.snapshots.get(date)
        if snapshot is not None:
            target_weights, _ranked, _metadata = build_target_weights(
                snapshot,
                current_holdings,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                top_n=top_n,
                hold_buffer=hold_buffer,
                single_name_cap=single_name_cap,
                hold_bonus=hold_bonus,
                risk_on_exposure=risk_on_exposure,
                soft_defense_exposure=soft_defense_exposure,
                hard_defense_exposure=hard_defense_exposure,
                soft_breadth_threshold=soft_breadth_threshold,
                hard_breadth_threshold=hard_breadth_threshold,
            )
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_haven
            }

        for symbol, weight in current_weights.items():
            weights_history.at[date, symbol] = weight

        next_returns = context.returns_matrix.loc[next_date]
        gross_return = sum(weight * float(next_returns.get(symbol, 0.0)) for symbol, weight in current_weights.items())
        cost = turnover_history.at[next_date] * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    for symbol, weight in current_weights.items():
        weights_history.at[context.index[-1], symbol] = weight

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    return summarize_returns(
        portfolio_returns,
        weights_history=used_weights,
        benchmark_returns=context.benchmark_returns,
        broad_benchmark_returns=context.broad_benchmark_returns,
        equal_weight_pool_returns=context.equal_weight_pool_returns,
        pool_name=context.pool,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
    )


def run_robustness_matrix(
    price_history,
    *,
    pools: Iterable[str] = DEFAULT_POOLS,
    top_n_values: Iterable[int] = DEFAULT_TOP_N_VALUES,
    single_name_cap_values: Iterable[float] = DEFAULT_SINGLE_NAME_CAP_VALUES,
    defense_modes: Iterable[str] = DEFAULT_DEFENSE_MODES,
    start_date: str | None = "2016-01-01",
    end_date: str | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    hold_buffer: int = 2,
    hold_bonus: float = 0.10,
    risk_on_exposure: float = 1.0,
    soft_defense_exposure: float = 0.50,
    hard_defense_exposure: float = 0.20,
    soft_breadth_threshold: float = 0.50,
    hard_breadth_threshold: float = 0.30,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
    turnover_cost_bps: float = 5.0,
) -> pd.DataFrame:
    pool_values = parse_csv_strings(tuple(pools), default=DEFAULT_POOLS)
    top_n_candidates = tuple(int(value) for value in top_n_values)
    cap_candidates = tuple(float(value) for value in single_name_cap_values)
    defense_candidates = _validate_defense_modes(defense_modes)
    prepared = _prepare_backtest_data(
        price_history,
        start_date=start_date,
        end_date=end_date,
        safe_haven=safe_haven,
    )
    contexts: dict[str, _PoolContext] = {}
    rows: list[dict[str, object]] = []

    for pool in pool_values:
        if pool not in POOL_SYMBOLS:
            known = ", ".join(sorted(POOL_SYMBOLS))
            raise ValueError(f"Unknown mega-cap pool {pool!r}; known pools: {known}")
        contexts[pool] = _build_pool_context(
            prepared,
            pool=pool,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            min_price_usd=min_price_usd,
            min_adv20_usd=min_adv20_usd,
            min_history_days=min_history_days,
        )

    for pool in pool_values:
        context = contexts[pool]
        for top_n in top_n_candidates:
            for single_name_cap in cap_candidates:
                for defense_mode in defense_candidates:
                    defense_config = _defense_params(
                        defense_mode,
                        risk_on_exposure=risk_on_exposure,
                        soft_defense_exposure=soft_defense_exposure,
                        hard_defense_exposure=hard_defense_exposure,
                    )
                    summary = _run_context_backtest(
                        context,
                        benchmark_symbol=benchmark_symbol,
                        broad_benchmark_symbol=broad_benchmark_symbol,
                        safe_haven=safe_haven,
                        top_n=top_n,
                        hold_buffer=hold_buffer,
                        single_name_cap=single_name_cap,
                        hold_bonus=hold_bonus,
                        soft_breadth_threshold=soft_breadth_threshold,
                        hard_breadth_threshold=hard_breadth_threshold,
                        turnover_cost_bps=turnover_cost_bps,
                        **defense_config,
                    )
                    summary = dict(summary)
                    summary.update(
                        {
                            "Run": _run_id(
                                pool=pool,
                                top_n=top_n,
                                single_name_cap=single_name_cap,
                                defense_mode=defense_mode,
                            ),
                            "Defense Mode": defense_mode,
                            "Top N": int(top_n),
                            "Single Name Cap": float(single_name_cap),
                            "Hold Buffer": int(hold_buffer),
                            "Turnover Cost Bps": float(turnover_cost_bps),
                        }
                    )
                    rows.append(summary)

    return pd.DataFrame(rows)


def rank_robustness_summary(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(columns=ROBUSTNESS_SUMMARY_COLUMNS)
    ranked = summary.copy()
    ranked = ranked.sort_values(
        by=["Sharpe", "CAGR", "Max Drawdown", "Turnover/Year"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    ranked.insert(0, "Rank", range(1, len(ranked) + 1))
    columns = [column for column in ROBUSTNESS_SUMMARY_COLUMNS if column in ranked.columns]
    extra_columns = [column for column in ranked.columns if column not in columns]
    return ranked.loc[:, columns + extra_columns]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a robustness matrix for mega_cap_leader_rotation research.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet)")
    input_group.add_argument("--download", action="store_true", help="Download the union pool with yfinance")
    parser.add_argument("--output-dir", required=True, help="Directory for robustness outputs")
    parser.add_argument("--pools", default=",".join(DEFAULT_POOLS), help="Comma-separated pools; default: mag7,expanded")
    parser.add_argument("--top-n-values", default="3,4,5")
    parser.add_argument("--single-name-cap-values", default="0.25,0.30,0.35")
    parser.add_argument("--defense-modes", default="on,off", help="Comma-separated on/off modes")
    parser.add_argument("--price-start", default="2015-01-01", help="Download start date used with --download")
    parser.add_argument("--price-end", help="Download end date used with --download")
    parser.add_argument("--start", dest="start_date", default="2016-01-01")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--hold-buffer", type=int, default=2)
    parser.add_argument("--hold-bonus", type=float, default=0.10)
    parser.add_argument("--risk-on-exposure", type=float, default=1.0)
    parser.add_argument("--soft-defense-exposure", type=float, default=0.50)
    parser.add_argument("--hard-defense-exposure", type=float, default=0.20)
    parser.add_argument("--soft-breadth-threshold", type=float, default=0.50)
    parser.add_argument("--hard-breadth-threshold", type=float, default=0.30)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--print-top", type=int, default=10)
    return parser


def _read_or_download_prices(args: argparse.Namespace, *, pools: tuple[str, ...], output_dir: Path) -> pd.DataFrame:
    if args.download:
        symbols = _collect_download_symbols(pools)
        prepared = prepare_research_input_data(
            output_dir=output_dir / "input",
            pool="matrix",
            symbols=symbols,
            price_start=args.price_start,
            price_end=args.price_end,
            benchmark_symbol=args.benchmark_symbol,
            broad_benchmark_symbol=args.broad_benchmark_symbol,
            safe_haven=args.safe_haven,
        )
        print(f"downloaded {prepared.price_rows} price rows -> {prepared.prices_path}")
        print(f"wrote union universe -> {prepared.universe_path}")
        return read_table(prepared.prices_path)
    return read_table(args.prices)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pools = parse_csv_strings(args.pools, default=DEFAULT_POOLS)
    top_n_values = parse_csv_ints(args.top_n_values, default=DEFAULT_TOP_N_VALUES)
    single_name_cap_values = parse_csv_floats(
        args.single_name_cap_values,
        default=DEFAULT_SINGLE_NAME_CAP_VALUES,
    )
    defense_modes = _validate_defense_modes(parse_csv_strings(args.defense_modes, default=DEFAULT_DEFENSE_MODES))
    prices = _read_or_download_prices(args, pools=pools, output_dir=output_dir)

    summary = run_robustness_matrix(
        prices,
        pools=pools,
        top_n_values=top_n_values,
        single_name_cap_values=single_name_cap_values,
        defense_modes=defense_modes,
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        hold_buffer=args.hold_buffer,
        hold_bonus=args.hold_bonus,
        risk_on_exposure=args.risk_on_exposure,
        soft_defense_exposure=args.soft_defense_exposure,
        hard_defense_exposure=args.hard_defense_exposure,
        soft_breadth_threshold=args.soft_breadth_threshold,
        hard_breadth_threshold=args.hard_breadth_threshold,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
        turnover_cost_bps=args.turnover_cost_bps,
    )
    ranked = rank_robustness_summary(summary)

    raw_path = output_dir / "robustness_summary_by_run.csv"
    ranked_path = output_dir / "robustness_summary.csv"
    summary.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)
    print(ranked.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote robustness summary -> {ranked_path}")
    print(f"wrote raw robustness rows -> {raw_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
