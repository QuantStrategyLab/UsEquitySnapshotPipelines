from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    SAFE_HAVEN,
    _build_close_and_returns,
    _compute_turnover,
    _normalize_price_history,
    _normalize_universe,
    _precompute_symbol_feature_history,
    _resolve_stock_exposure,
    build_feature_snapshot_for_backtest,
    build_monthly_rebalance_dates,
    build_target_weights,
    resolve_active_universe,
    summarize_returns,
)
from .mega_cap_leader_rotation_concentration_variants import (
    _build_rolling_rows,
    _build_yearly_rows,
    _returns_from_weights,
)
from .mega_cap_leader_rotation_dynamic_validation import (
    DEFAULT_ROLLING_WINDOW_YEARS,
    lag_universe_history,
    parse_csv_ints,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_REBALANCE_FREQUENCIES = ("monthly", "biweekly", "weekly")
DEFAULT_DAILY_RISK_MODES = ("none", "hard_cash", "partial_cash")
DEFAULT_BLEND_TOP2_WEIGHT = 0.50
SUMMARY_COLUMNS = (
    "Run",
    "Rebalance Frequency",
    "Daily Risk Mode",
    "Universe Lag Trading Days",
    "Top2 Blend Weight",
    "Top4 Blend Weight",
    "Daily Soft Exposure",
    "Daily Hard Exposure",
    "Daily Soft Share",
    "Daily Hard Share",
    "Start",
    "End",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Total Return",
    "Final Equity",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Benchmark Symbol",
    "Benchmark Total Return",
    "Benchmark Corr",
    "Broad Benchmark Symbol",
    "Broad Benchmark Total Return",
    "Equal Weight Pool Total Return",
)
DAILY_RISK_COLUMNS = (
    "Run",
    "Date",
    "Daily Risk Mode",
    "Regime",
    "Target Stock Exposure",
    "Breadth Ratio",
    "Benchmark Trend Positive",
)
DAILY_SIGNAL_COLUMNS = (
    "Date",
    "Regime",
    "Breadth Ratio",
    "Benchmark Trend Positive",
)


def parse_csv_strings(raw_value: str | Iterable[str] | None, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        parsed.append(text)
    return tuple(parsed) or default


def build_rebalance_dates(index: pd.DatetimeIndex, frequency: str) -> set[pd.Timestamp]:
    dates = pd.DatetimeIndex(index)
    if dates.empty:
        return set()
    normalized = str(frequency).strip().lower()
    if normalized == "monthly":
        return build_monthly_rebalance_dates(dates)
    if normalized == "weekly":
        grouped = pd.Series(dates, index=dates).groupby(dates.to_period("W-FRI")).max()
        return set(pd.to_datetime(grouped.values))
    if normalized == "biweekly":
        frame = pd.DataFrame({"as_of": dates, "bucket": range(len(dates))})
        frame["bucket"] = frame["bucket"] // 10
        return set(pd.to_datetime(frame.groupby("bucket")["as_of"].max().values))
    if normalized == "daily":
        return set(pd.to_datetime(dates))
    raise ValueError("rebalance frequency must be one of monthly, biweekly, weekly, or daily")


def daily_risk_mode_exposures(mode: str) -> tuple[float, float]:
    normalized = str(mode).strip().lower()
    if normalized == "none":
        return 1.0, 1.0
    if normalized == "hard_cash":
        return 1.0, 0.0
    if normalized == "partial_cash":
        return 0.5, 0.0
    raise ValueError("daily risk mode must be one of none, hard_cash, or partial_cash")


def _run_frequency_sleeve(
    price_history: pd.DataFrame,
    universe_history: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    rebalance_frequency: str,
    top_n: int,
    single_name_cap: float,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    hold_buffer: int,
    hold_bonus: float,
    min_price_usd: float,
    min_adv20_usd: float,
    min_history_days: int,
) -> dict[str, pd.DataFrame | pd.Series]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    universe = _normalize_universe(universe_history)
    feature_history_by_symbol = _precompute_symbol_feature_history(prices)
    close_matrix, _returns_matrix = _build_close_and_returns(prices)
    if safe_haven not in close_matrix.columns:
        close_matrix[safe_haven] = 1.0
    index = pd.DatetimeIndex(close_matrix.index)
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")

    rebalance_dates = build_rebalance_dates(index, rebalance_frequency)
    columns = sorted(set(close_matrix.columns) | {safe_haven})
    weights_history = pd.DataFrame(0.0, index=index, columns=columns)
    turnover_history = pd.Series(0.0, index=index, name="turnover")
    exposure_rows: list[dict[str, object]] = []
    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()

    for position, date in enumerate(index):
        if date in rebalance_dates:
            active_universe = resolve_active_universe(universe, date)
            snapshot = build_feature_snapshot_for_backtest(
                date,
                active_universe,
                feature_history_by_symbol,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                min_price_usd=float(min_price_usd),
                min_adv20_usd=float(min_adv20_usd),
                min_history_days=int(min_history_days),
            )
            target_weights, _ranked, metadata = build_target_weights(
                snapshot,
                current_holdings,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                top_n=int(top_n),
                hold_buffer=int(hold_buffer),
                single_name_cap=float(single_name_cap),
                max_names_per_sector=0,
                hold_bonus=float(hold_bonus),
                risk_on_exposure=1.0,
                soft_defense_exposure=1.0,
                hard_defense_exposure=1.0,
                min_position_value_usd=0.0,
            )
            turnover = _compute_turnover(current_weights, target_weights)
            if position + 1 < len(index):
                turnover_history.at[index[position + 1]] = turnover
            exposure_rows.append(
                {
                    "signal_date": date,
                    "effective_date": index[position + 1] if position + 1 < len(index) else date,
                    "regime": metadata.get("regime"),
                    "stock_exposure": metadata.get("stock_exposure"),
                    "safe_haven_weight": float(target_weights.get(safe_haven, 0.0)),
                    "breadth_ratio": metadata.get("breadth_ratio"),
                    "benchmark_trend_positive": metadata.get("benchmark_trend_positive"),
                    "selected_symbols": ",".join(metadata.get("selected_symbols", ())),
                    "turnover": turnover,
                }
            )
            current_weights = target_weights
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_haven
            }
        for symbol, weight in current_weights.items():
            weights_history.at[date, symbol] = weight

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    return {
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "exposure_history": pd.DataFrame(exposure_rows),
    }


def _apply_daily_risk_overlay(
    weights_history: pd.DataFrame,
    *,
    run_name: str,
    mode: str,
    safe_haven: str,
    daily_signal_history: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    soft_exposure, hard_exposure = daily_risk_mode_exposures(mode)
    if str(mode).strip().lower() == "none":
        return weights_history.copy(), pd.DataFrame(columns=DAILY_RISK_COLUMNS)

    adjusted = weights_history.copy().fillna(0.0)
    if safe_haven not in adjusted.columns:
        adjusted[safe_haven] = 0.0
    signals = daily_signal_history.set_index(pd.to_datetime(daily_signal_history["Date"]))
    risk_rows: list[dict[str, object]] = []
    stock_columns = [column for column in adjusted.columns if column != safe_haven]
    for date in pd.DatetimeIndex(adjusted.index):
        signal = signals.loc[date] if date in signals.index else None
        regime = str(signal["Regime"]) if signal is not None else "risk_on"
        if regime == "hard_defense":
            stock_exposure = float(hard_exposure)
        elif regime == "soft_defense":
            stock_exposure = float(soft_exposure)
        else:
            stock_exposure = 1.0
        current_stock_weight = float(adjusted.loc[date, stock_columns].sum()) if stock_columns else 0.0
        target_stock_weight = min(current_stock_weight, float(stock_exposure))
        if current_stock_weight > 1e-12:
            adjusted.loc[date, stock_columns] *= target_stock_weight / current_stock_weight
        adjusted.at[date, safe_haven] = max(0.0, 1.0 - target_stock_weight)
        risk_rows.append(
            {
                "Run": run_name,
                "Date": date.date().isoformat(),
                "Daily Risk Mode": mode,
                "Regime": regime,
                "Target Stock Exposure": float(stock_exposure),
                "Breadth Ratio": signal["Breadth Ratio"] if signal is not None else float("nan"),
                "Benchmark Trend Positive": (
                    signal["Benchmark Trend Positive"] if signal is not None else False
                ),
            }
        )
    return adjusted, pd.DataFrame(risk_rows, columns=DAILY_RISK_COLUMNS)


def _build_daily_signal_history(
    *,
    price_history: pd.DataFrame,
    universe_history: pd.DataFrame,
    index: pd.DatetimeIndex,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    soft_breadth_threshold: float,
    hard_breadth_threshold: float,
    min_price_usd: float,
    min_adv20_usd: float,
    min_history_days: int,
) -> pd.DataFrame:
    prices = _normalize_price_history(price_history)
    feature_history_by_symbol = _precompute_symbol_feature_history(prices)
    universe = _normalize_universe(universe_history)
    rows: list[dict[str, object]] = []
    for date in pd.DatetimeIndex(index):
        active_universe = resolve_active_universe(universe, date)
        snapshot = build_feature_snapshot_for_backtest(
            date,
            active_universe,
            feature_history_by_symbol,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            min_price_usd=float(min_price_usd),
            min_adv20_usd=float(min_adv20_usd),
            min_history_days=int(min_history_days),
        )
        _stock_exposure, regime, breadth_ratio, benchmark_trend_positive = _resolve_stock_exposure(
            snapshot,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            soft_breadth_threshold=float(soft_breadth_threshold),
            hard_breadth_threshold=float(hard_breadth_threshold),
            risk_on_exposure=1.0,
            soft_defense_exposure=1.0,
            hard_defense_exposure=1.0,
        )
        rows.append(
            {
                "Date": date.date().isoformat(),
                "Regime": regime,
                "Breadth Ratio": breadth_ratio,
                "Benchmark Trend Positive": benchmark_trend_positive,
            }
        )
    return pd.DataFrame(rows, columns=DAILY_SIGNAL_COLUMNS)


def _summary_for_variant(
    *,
    run_name: str,
    frequency: str,
    daily_risk_mode: str,
    weights: pd.DataFrame,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    universe_lag_trading_days: int,
    blend_top2_weight: float,
    daily_soft_exposure: float,
    daily_hard_exposure: float,
    risk_history: pd.DataFrame,
) -> dict[str, object]:
    equal_weight_column = next(
        (column for column in reference_returns.columns if str(column).startswith("equal_weight_")),
        None,
    )
    summary = dict(
        summarize_returns(
            portfolio_returns,
            weights_history=weights,
            benchmark_returns=(
                reference_returns[benchmark_symbol]
                if benchmark_symbol in reference_returns.columns
                else None
            ),
            broad_benchmark_returns=(
                reference_returns[broad_benchmark_symbol]
                if broad_benchmark_symbol in reference_returns.columns
                else None
            ),
            equal_weight_pool_returns=reference_returns[equal_weight_column] if equal_weight_column else None,
            pool_name="dynamic_top50_frequency_risk",
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
        )
    )
    daily_soft_share = float("nan")
    daily_hard_share = float("nan")
    if not risk_history.empty and "Regime" in risk_history.columns:
        daily_soft_share = float(risk_history["Regime"].eq("soft_defense").mean())
        daily_hard_share = float(risk_history["Regime"].eq("hard_defense").mean())
    summary.update(
        {
            "Run": run_name,
            "Rebalance Frequency": frequency,
            "Daily Risk Mode": daily_risk_mode,
            "Universe Lag Trading Days": int(universe_lag_trading_days),
            "Top2 Blend Weight": float(blend_top2_weight),
            "Top4 Blend Weight": 1.0 - float(blend_top2_weight),
            "Daily Soft Exposure": float(daily_soft_exposure),
            "Daily Hard Exposure": float(daily_hard_exposure),
            "Daily Soft Share": daily_soft_share,
            "Daily Hard Share": daily_hard_share,
        }
    )
    return summary


def run_frequency_risk_research(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    universe_lag_trading_days: int = 21,
    rebalance_frequencies: Iterable[str] = DEFAULT_REBALANCE_FREQUENCIES,
    daily_risk_modes: Iterable[str] = DEFAULT_DAILY_RISK_MODES,
    blend_top2_weight: float = DEFAULT_BLEND_TOP2_WEIGHT,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    hold_buffer: int = 2,
    hold_bonus: float = 0.10,
    soft_breadth_threshold: float = 0.50,
    hard_breadth_threshold: float = 0.30,
    turnover_cost_bps: float = 5.0,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
) -> dict[str, pd.DataFrame]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    trading_index = pd.DatetimeIndex(sorted(prices["as_of"].dropna().unique()))
    lagged_universe = lag_universe_history(
        universe_history,
        lag_trading_days=int(universe_lag_trading_days),
        trading_index=trading_index,
    )
    close_matrix, returns_matrix = _build_close_and_returns(prices)
    if safe_haven not in close_matrix.columns:
        close_matrix[safe_haven] = 1.0
        returns_matrix[safe_haven] = 0.0
    index = pd.DatetimeIndex(close_matrix.index)
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")
    returns_matrix = returns_matrix.reindex(index).fillna(0.0)
    reference_returns = pd.DataFrame(
        {
            benchmark_symbol: (
                returns_matrix[benchmark_symbol].reindex(index)
                if benchmark_symbol in returns_matrix.columns
                else pd.Series(index=index, dtype=float)
            ),
            broad_benchmark_symbol: (
                returns_matrix[broad_benchmark_symbol].reindex(index)
                if broad_benchmark_symbol in returns_matrix.columns
                else pd.Series(index=index, dtype=float)
            ),
        },
        index=index,
    )
    frequencies = parse_csv_strings(tuple(rebalance_frequencies), default=DEFAULT_REBALANCE_FREQUENCIES)
    risk_modes = parse_csv_strings(tuple(daily_risk_modes), default=DEFAULT_DAILY_RISK_MODES)
    rolling_values = parse_csv_ints(tuple(rolling_window_years), default=DEFAULT_ROLLING_WINDOW_YEARS)
    daily_signal_history = _build_daily_signal_history(
        price_history=prices,
        universe_history=lagged_universe,
        index=index,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        soft_breadth_threshold=soft_breadth_threshold,
        hard_breadth_threshold=hard_breadth_threshold,
        min_price_usd=min_price_usd,
        min_adv20_usd=min_adv20_usd,
        min_history_days=min_history_days,
    )

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []
    risk_history_rows: list[pd.DataFrame] = []

    for frequency in frequencies:
        top2 = _run_frequency_sleeve(
            prices,
            lagged_universe,
            start_date=start_date,
            end_date=end_date,
            rebalance_frequency=frequency,
            top_n=2,
            single_name_cap=0.50,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            hold_buffer=hold_buffer,
            hold_bonus=hold_bonus,
            min_price_usd=min_price_usd,
            min_adv20_usd=min_adv20_usd,
            min_history_days=min_history_days,
        )
        top4 = _run_frequency_sleeve(
            prices,
            lagged_universe,
            start_date=start_date,
            end_date=end_date,
            rebalance_frequency=frequency,
            top_n=4,
            single_name_cap=0.25,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            hold_buffer=hold_buffer,
            hold_bonus=hold_bonus,
            min_price_usd=min_price_usd,
            min_adv20_usd=min_adv20_usd,
            min_history_days=min_history_days,
        )
        columns = sorted(
            set(top2["weights_history"].columns)
            | set(top4["weights_history"].columns)
            | set(returns_matrix.columns)
            | {safe_haven}
        )
        top2_weights = top2["weights_history"].reindex(index).fillna(0.0).reindex(columns=columns, fill_value=0.0)
        top4_weights = top4["weights_history"].reindex(index).fillna(0.0).reindex(columns=columns, fill_value=0.0)
        base_weights = float(blend_top2_weight) * top2_weights + (1.0 - float(blend_top2_weight)) * top4_weights
        for risk_mode in risk_modes:
            soft_exposure, hard_exposure = daily_risk_mode_exposures(risk_mode)
            run_name = f"blend50_{frequency}_{risk_mode}"
            weights, risk_history = _apply_daily_risk_overlay(
                base_weights,
                run_name=run_name,
                mode=risk_mode,
                safe_haven=safe_haven,
                daily_signal_history=daily_signal_history,
            )
            returns = _returns_from_weights(
                weights.reindex(columns=columns, fill_value=0.0),
                returns_matrix.reindex(columns=columns, fill_value=0.0),
                turnover_cost_bps=float(turnover_cost_bps),
            )
            summary_rows.append(
                _summary_for_variant(
                    run_name=run_name,
                    frequency=frequency,
                    daily_risk_mode=risk_mode,
                    weights=weights,
                    portfolio_returns=returns,
                    reference_returns=reference_returns,
                    benchmark_symbol=benchmark_symbol,
                    broad_benchmark_symbol=broad_benchmark_symbol,
                    safe_haven=safe_haven,
                    universe_lag_trading_days=int(universe_lag_trading_days),
                    blend_top2_weight=float(blend_top2_weight),
                    daily_soft_exposure=soft_exposure,
                    daily_hard_exposure=hard_exposure,
                    risk_history=risk_history,
                )
            )
            yearly_rows.extend(
                _build_yearly_rows(
                    run_name=run_name,
                    variant_type=f"{frequency}_{risk_mode}",
                    portfolio_returns=returns,
                    reference_returns=reference_returns,
                    benchmark_symbol=benchmark_symbol,
                    broad_benchmark_symbol=broad_benchmark_symbol,
                )
            )
            rolling_rows.extend(
                _build_rolling_rows(
                    run_name=run_name,
                    variant_type=f"{frequency}_{risk_mode}",
                    portfolio_returns=returns,
                    reference_returns=reference_returns,
                    benchmark_symbol=benchmark_symbol,
                    broad_benchmark_symbol=broad_benchmark_symbol,
                    rolling_window_years=rolling_values,
                )
            )
            if not risk_history.empty:
                risk_history_rows.append(risk_history)

    return {
        "frequency_risk_summary": pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS),
        "frequency_risk_yearly_summary": pd.DataFrame(yearly_rows),
        "frequency_risk_rolling_summary": pd.DataFrame(rolling_rows),
        "frequency_risk_daily_history": (
            pd.concat(risk_history_rows, ignore_index=True)
            if risk_history_rows
            else pd.DataFrame(columns=DAILY_RISK_COLUMNS)
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research rebalance-frequency and daily-risk overlays for dynamic Top50 leader rotation."
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for research outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", type=int, default=21)
    parser.add_argument(
        "--rebalance-frequencies",
        default=",".join(DEFAULT_REBALANCE_FREQUENCIES),
        help="Comma-separated rebalance frequencies: monthly, biweekly, weekly, daily",
    )
    parser.add_argument(
        "--daily-risk-modes",
        default=",".join(DEFAULT_DAILY_RISK_MODES),
        help="Comma-separated daily risk overlays: none, hard_cash, partial_cash",
    )
    parser.add_argument("--blend-top2-weight", type=float, default=DEFAULT_BLEND_TOP2_WEIGHT)
    parser.add_argument(
        "--rolling-window-years",
        default="",
        help="Comma-separated complete-calendar-year rolling windows to summarize, for example 3,5",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--hold-buffer", type=int, default=2)
    parser.add_argument("--hold-bonus", type=float, default=0.10)
    parser.add_argument("--soft-breadth-threshold", type=float, default=0.50)
    parser.add_argument("--hard-breadth-threshold", type=float, default=0.30)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = run_frequency_risk_research(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=int(args.universe_lag_days),
        rebalance_frequencies=parse_csv_strings(
            args.rebalance_frequencies,
            default=DEFAULT_REBALANCE_FREQUENCIES,
        ),
        daily_risk_modes=parse_csv_strings(args.daily_risk_modes, default=DEFAULT_DAILY_RISK_MODES),
        blend_top2_weight=float(args.blend_top2_weight),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        hold_buffer=args.hold_buffer,
        hold_bonus=args.hold_bonus,
        soft_breadth_threshold=args.soft_breadth_threshold,
        hard_breadth_threshold=args.hard_breadth_threshold,
        turnover_cost_bps=args.turnover_cost_bps,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
    )
    summary_path = output_dir / "frequency_risk_summary.csv"
    yearly_path = output_dir / "frequency_risk_yearly_summary.csv"
    rolling_path = output_dir / "frequency_risk_rolling_summary.csv"
    daily_path = output_dir / "frequency_risk_daily_history.csv"
    result["frequency_risk_summary"].to_csv(summary_path, index=False)
    result["frequency_risk_yearly_summary"].to_csv(yearly_path, index=False)
    result["frequency_risk_rolling_summary"].to_csv(rolling_path, index=False)
    result["frequency_risk_daily_history"].to_csv(daily_path, index=False)
    print(result["frequency_risk_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote frequency risk summary -> {summary_path}")
    print(f"wrote frequency risk yearly summary -> {yearly_path}")
    print(f"wrote frequency risk rolling summary -> {rolling_path}")
    print(f"wrote frequency risk daily history -> {daily_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
