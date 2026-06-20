from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    SAFE_HAVEN,
    _build_close_and_returns,
    _normalize_price_history,
    run_backtest,
    summarize_returns,
)
from .mega_cap_leader_rotation_dynamic_validation import (
    DEFAULT_ROLLING_WINDOW_YEARS,
    _complete_calendar_years,
    _period_cagr,
    _period_max_drawdown,
    _period_return,
    lag_universe_history,
    parse_csv_ints,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_BLEND_TOP2_WEIGHTS = (0.25, 0.50, 0.75)
DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS = (0.08, 0.10, 0.12)
DEFAULT_SECTOR_CAP_VALUES = (1,)
DEFAULT_SECTOR_SCORE_PENALTY_VALUES = (0.25, 0.50)
DEFAULT_RESIDUAL_MOMENTUM_WEIGHTS = (0.25, 0.50)
DEFAULT_BETA_PENALTY_WEIGHTS = (0.25,)
DEFAULT_VOL_TARGET_VALUES = (0.18, 0.22)
DEFAULT_VOL_TARGET_WINDOW = 63
DEFAULT_VOL_TARGET_MIN_STOCK_EXPOSURE = 0.50
DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD = 0.10
DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD = 0.03
DEFAULT_PANIC_GUARD_VOL_THRESHOLD = 0.25
DEFAULT_PANIC_GUARD_STOCK_EXPOSURE = 0.50
SUMMARY_COLUMNS = (
    "Run",
    "Variant Type",
    "Universe Lag Trading Days",
    "Max Names Per Sector",
    "Sector Score Penalty",
    "Residual Momentum Weight",
    "Beta Penalty Weight",
    "Vol Target",
    "Vol Target Window",
    "Min Stock Exposure",
    "Panic Drawdown Threshold",
    "Panic Rebound Threshold",
    "Panic Vol Threshold",
    "Panic Stock Exposure",
    "Top2 Blend Weight",
    "Top4 Blend Weight",
    "Dynamic Drawdown Threshold",
    "Top4 Mode Share",
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
YEARLY_COLUMNS = (
    "Run",
    "Variant Type",
    "Year",
    "Strategy Return",
    "Strategy Max Drawdown",
    "QQQ Return",
    "QQQ Max Drawdown",
    "SPY Return",
    "SPY Max Drawdown",
)
ROLLING_COLUMNS = (
    "Run",
    "Variant Type",
    "Window Years",
    "Window Start Year",
    "Window End Year",
    "Strategy Return",
    "Strategy CAGR",
    "Strategy Max Drawdown",
    "QQQ Return",
    "QQQ CAGR",
    "QQQ Max Drawdown",
    "SPY Return",
    "SPY CAGR",
    "SPY Max Drawdown",
)
MODE_COLUMNS = (
    "Run",
    "Variant Type",
    "Signal Date",
    "Effective Date",
    "Top2 Shadow Drawdown",
    "Dynamic Drawdown Threshold",
    "Mode",
)
DAILY_RETURN_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Strategy Return",
    "QQQ Return",
    "SPY Return",
)
TRADE_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Symbol",
    "Previous Weight",
    "Target Weight",
    "Trade Weight Delta",
    "Abs Trade Weight Delta",
)


def parse_csv_floats(raw_value: str | Iterable[float] | None, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if raw_value is None:
        return default
    if isinstance(raw_value, str) and raw_value.strip().lower() in {"", "none", "off"}:
        return ()
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    if not values:
        return ()
    parsed: list[float] = []
    seen: set[float] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        number = float(text)
        if number > 1.0:
            number = number / 100.0
        if number in seen:
            continue
        seen.add(number)
        parsed.append(number)
    return tuple(parsed)


def _trading_index(price_history: pd.DataFrame) -> pd.DatetimeIndex:
    prices = _normalize_price_history(price_history)
    index = pd.DatetimeIndex(sorted(prices["as_of"].dropna().unique()))
    if index.empty:
        raise ValueError("price_history has no usable trading dates")
    return index


def _variant_name_for_blend(top2_weight: float) -> str:
    top2_label = int(round(float(top2_weight) * 100))
    top4_label = int(round((1.0 - float(top2_weight)) * 100))
    return f"blend_top2_{top2_label}_top4_{top4_label}"


def _sector_variant_name(base_name: str, max_names_per_sector: int) -> str:
    return f"sector_cap{int(max_names_per_sector)}_{base_name}"


def _penalty_variant_name(base_name: str, sector_score_penalty: float) -> str:
    label = f"{float(sector_score_penalty):.2f}".rstrip("0").rstrip(".").replace(".", "p")
    return f"sector_penalty{label}_{base_name}"


def _residual_beta_variant_name(base_name: str, *, residual_momentum_weight: float, beta_penalty_weight: float) -> str:
    parts: list[str] = []
    if float(residual_momentum_weight) > 0.0:
        residual_label = f"{float(residual_momentum_weight):.2f}".rstrip("0").rstrip(".").replace(".", "p")
        parts.append(f"resid{residual_label}")
    if float(beta_penalty_weight) > 0.0:
        beta_label = f"{float(beta_penalty_weight):.2f}".rstrip("0").rstrip(".").replace(".", "p")
        parts.append(f"beta{beta_label}")
    prefix = "_".join(parts) if parts else "raw"
    return f"{prefix}_{base_name}"


def _vol_target_variant_name(base_name: str, *, vol_target: float, min_stock_exposure: float) -> str:
    target_label = int(round(float(vol_target) * 100))
    min_label = int(round(float(min_stock_exposure) * 100))
    return f"voltarget{target_label}_min{min_label}_{base_name}"


def _panic_guard_variant_name(
    base_name: str,
    *,
    drawdown_threshold: float,
    rebound_threshold: float,
    vol_threshold: float,
    stock_exposure: float,
) -> str:
    drawdown_label = int(round(float(drawdown_threshold) * 100))
    rebound_label = int(round(float(rebound_threshold) * 100))
    vol_label = int(round(float(vol_threshold) * 100))
    exposure_label = int(round(float(stock_exposure) * 100))
    return f"panicdd{drawdown_label}_ret{rebound_label}_vol{vol_label}_stock{exposure_label}_{base_name}"


def _variant_name_for_drawdown(threshold: float) -> str:
    return f"dynamic_top2_dd{int(round(float(threshold) * 100))}_to_top4"


def _align_weights(
    weights_history: pd.DataFrame,
    *,
    index: pd.DatetimeIndex,
    columns: Iterable[str],
) -> pd.DataFrame:
    return weights_history.reindex(index).fillna(0.0).reindex(columns=columns, fill_value=0.0)


def _returns_from_weights(
    weights_history: pd.DataFrame,
    returns_matrix: pd.DataFrame,
    *,
    turnover_cost_bps: float,
) -> pd.Series:
    weights = weights_history.fillna(0.0)
    returns = returns_matrix.reindex(weights.index).reindex(columns=weights.columns, fill_value=0.0).fillna(0.0)
    gross_returns = (weights.shift(1).fillna(0.0) * returns).sum(axis=1)
    turnover = 0.5 * weights.diff().abs().sum(axis=1).shift(1).fillna(0.0)
    return gross_returns - turnover * (float(turnover_cost_bps) / 10_000.0)




def _build_rebalance_trade_rows(
    *,
    run_name: str,
    variant_type: str,
    weights: pd.DataFrame,
) -> list[dict[str, object]]:
    frame = weights.fillna(0.0).copy()
    if frame.empty:
        return []
    deltas = frame.diff().fillna(frame)
    rows: list[dict[str, object]] = []
    for as_of, delta_row in deltas.iterrows():
        changed_symbols = [symbol for symbol, value in delta_row.items() if abs(float(value)) > 1e-12]
        if not changed_symbols:
            continue
        for symbol in changed_symbols:
            target_weight = float(frame.at[as_of, symbol])
            delta = float(delta_row[symbol])
            rows.append(
                {
                    "Date": pd.Timestamp(as_of).date().isoformat(),
                    "Run": run_name,
                    "Variant Type": variant_type,
                    "Symbol": str(symbol),
                    "Previous Weight": target_weight - delta,
                    "Target Weight": target_weight,
                    "Trade Weight Delta": delta,
                    "Abs Trade Weight Delta": abs(delta),
                }
            )
    return rows


def _build_daily_return_rows(
    *,
    run_name: str,
    variant_type: str,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
) -> list[dict[str, object]]:
    strategy = pd.to_numeric(portfolio_returns, errors="coerce").dropna()
    reference = reference_returns.reindex(strategy.index)
    benchmark_returns = (
        pd.to_numeric(reference[benchmark_symbol], errors="coerce")
        if benchmark_symbol in reference.columns
        else pd.Series(index=strategy.index, dtype=float)
    )
    broad_returns = (
        pd.to_numeric(reference[broad_benchmark_symbol], errors="coerce")
        if broad_benchmark_symbol in reference.columns
        else pd.Series(index=strategy.index, dtype=float)
    )
    rows: list[dict[str, object]] = []
    for as_of, value in strategy.items():
        rows.append(
            {
                "Date": pd.Timestamp(as_of).date().isoformat(),
                "Run": run_name,
                "Variant Type": variant_type,
                "Strategy Return": float(value),
                "QQQ Return": float(benchmark_returns.get(as_of, float("nan"))),
                "SPY Return": float(broad_returns.get(as_of, float("nan"))),
            }
        )
    return rows


def _build_yearly_rows(
    *,
    run_name: str,
    variant_type: str,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for year in sorted(int(year) for year in pd.DatetimeIndex(portfolio_returns.index).year.unique()):
        mask = pd.DatetimeIndex(portfolio_returns.index).year == year
        strategy_returns = portfolio_returns.loc[mask]
        reference_slice = reference_returns.reindex(strategy_returns.index)
        benchmark_returns = (
            reference_slice[benchmark_symbol]
            if benchmark_symbol in reference_slice.columns
            else pd.Series(index=strategy_returns.index, dtype=float)
        )
        broad_returns = (
            reference_slice[broad_benchmark_symbol]
            if broad_benchmark_symbol in reference_slice.columns
            else pd.Series(index=strategy_returns.index, dtype=float)
        )
        rows.append(
            {
                "Run": run_name,
                "Variant Type": variant_type,
                "Year": int(year),
                "Strategy Return": _period_return(strategy_returns),
                "Strategy Max Drawdown": _period_max_drawdown(strategy_returns),
                "QQQ Return": _period_return(benchmark_returns),
                "QQQ Max Drawdown": _period_max_drawdown(benchmark_returns),
                "SPY Return": _period_return(broad_returns),
                "SPY Max Drawdown": _period_max_drawdown(broad_returns),
            }
        )
    return rows


def _build_rolling_rows(
    *,
    run_name: str,
    variant_type: str,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    rolling_window_years: Iterable[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    years = _complete_calendar_years(pd.DatetimeIndex(portfolio_returns.index))
    if not years:
        return rows
    reference = reference_returns.reindex(portfolio_returns.index)
    for window_years in rolling_window_years:
        window = int(window_years)
        if window <= 0:
            continue
        for start_year in years:
            end_year = start_year + window - 1
            if end_year not in years:
                continue
            mask = (pd.DatetimeIndex(portfolio_returns.index).year >= start_year) & (
                pd.DatetimeIndex(portfolio_returns.index).year <= end_year
            )
            strategy_returns = portfolio_returns.loc[mask]
            reference_slice = reference.loc[strategy_returns.index]
            benchmark_returns = (
                reference_slice[benchmark_symbol]
                if benchmark_symbol in reference_slice.columns
                else pd.Series(index=strategy_returns.index, dtype=float)
            )
            broad_returns = (
                reference_slice[broad_benchmark_symbol]
                if broad_benchmark_symbol in reference_slice.columns
                else pd.Series(index=strategy_returns.index, dtype=float)
            )
            rows.append(
                {
                    "Run": run_name,
                    "Variant Type": variant_type,
                    "Window Years": int(window),
                    "Window Start Year": int(start_year),
                    "Window End Year": int(end_year),
                    "Strategy Return": _period_return(strategy_returns),
                    "Strategy CAGR": _period_cagr(strategy_returns),
                    "Strategy Max Drawdown": _period_max_drawdown(strategy_returns),
                    "QQQ Return": _period_return(benchmark_returns),
                    "QQQ CAGR": _period_cagr(benchmark_returns),
                    "QQQ Max Drawdown": _period_max_drawdown(benchmark_returns),
                    "SPY Return": _period_return(broad_returns),
                    "SPY CAGR": _period_cagr(broad_returns),
                    "SPY Max Drawdown": _period_max_drawdown(broad_returns),
                }
            )
    return rows


def _build_dynamic_weights(
    *,
    run_name: str,
    threshold: float,
    index: pd.DatetimeIndex,
    top2_weights: pd.DataFrame,
    top4_weights: pd.DataFrame,
    top2_returns: pd.Series,
    top2_exposure_history: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, object]], float]:
    top2_equity = (1.0 + top2_returns.reindex(index).fillna(0.0)).cumprod()
    top2_drawdown = top2_equity / top2_equity.cummax() - 1.0
    mode = pd.Series("top2", index=index, dtype=object)
    mode_rows: list[dict[str, object]] = []
    exposure = top2_exposure_history.copy()
    if exposure.empty:
        return top2_weights.copy(), mode_rows, 0.0
    exposure["signal_date"] = pd.to_datetime(exposure["signal_date"], errors="coerce").dt.tz_localize(None)
    exposure["effective_date"] = pd.to_datetime(exposure["effective_date"], errors="coerce").dt.tz_localize(None)
    exposure = exposure.dropna(subset=["signal_date", "effective_date"]).sort_values("effective_date")

    for row in exposure.itertuples(index=False):
        signal_date = pd.Timestamp(getattr(row, "signal_date")).normalize()
        effective_date = pd.Timestamp(getattr(row, "effective_date")).normalize()
        drawdown_value = top2_drawdown.asof(signal_date)
        use_top4 = bool(pd.notna(drawdown_value) and float(drawdown_value) <= -float(threshold))
        selected_mode = "top4" if use_top4 else "top2"
        mode.loc[mode.index >= effective_date] = selected_mode
        mode_rows.append(
            {
                "Run": run_name,
                "Variant Type": "dynamic_top2_drawdown_to_top4",
                "Signal Date": signal_date.date().isoformat(),
                "Effective Date": effective_date.date().isoformat(),
                "Top2 Shadow Drawdown": float(drawdown_value) if pd.notna(drawdown_value) else float("nan"),
                "Dynamic Drawdown Threshold": float(threshold),
                "Mode": selected_mode,
            }
        )
    top4_share = float(mode.eq("top4").mean())
    return top2_weights.where(mode.eq("top2"), top4_weights), mode_rows, top4_share


def _apply_volatility_target_exposure(
    weights_history: pd.DataFrame,
    benchmark_returns: pd.Series,
    *,
    safe_haven: str,
    vol_target: float,
    min_stock_exposure: float,
    vol_window: int,
) -> pd.DataFrame:
    weights = weights_history.fillna(0.0).copy()
    safe_symbol = str(safe_haven).strip().upper()
    if safe_symbol not in weights.columns:
        weights[safe_symbol] = 0.0

    stock_columns = [column for column in weights.columns if str(column) != safe_symbol]
    if not stock_columns:
        weights[safe_symbol] = 1.0
        return weights

    realized_vol = (
        pd.to_numeric(benchmark_returns.reindex(weights.index), errors="coerce")
        .rolling(int(vol_window))
        .std(ddof=0)
        * np.sqrt(252)
    )
    scaler = (float(vol_target) / realized_vol).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    scaler = scaler.clip(lower=float(min_stock_exposure), upper=1.0)
    rebalance_dates = weights.loc[:, stock_columns].diff().abs().sum(axis=1).gt(1e-12)
    if not rebalance_dates.empty:
        rebalance_dates.iloc[0] = True
    scaler = scaler.where(rebalance_dates).ffill().fillna(1.0)

    output = weights.copy()
    output.loc[:, stock_columns] = output.loc[:, stock_columns].mul(scaler, axis=0)
    stock_weight = output.loc[:, stock_columns].sum(axis=1).clip(lower=0.0, upper=1.0)
    output[safe_symbol] = 1.0 - stock_weight
    return output.reindex(columns=weights_history.columns.union([safe_symbol], sort=False), fill_value=0.0)


def _apply_panic_rebound_guard(
    weights_history: pd.DataFrame,
    benchmark_returns: pd.Series,
    *,
    safe_haven: str,
    drawdown_threshold: float,
    rebound_threshold: float,
    vol_threshold: float,
    stock_exposure: float,
    drawdown_window: int = 126,
    rebound_window: int = 21,
    vol_window: int = 63,
) -> pd.DataFrame:
    weights = weights_history.fillna(0.0).copy()
    safe_symbol = str(safe_haven).strip().upper()
    if safe_symbol not in weights.columns:
        weights[safe_symbol] = 0.0

    stock_columns = [column for column in weights.columns if str(column) != safe_symbol]
    if not stock_columns:
        weights[safe_symbol] = 1.0
        return weights

    returns = pd.to_numeric(benchmark_returns.reindex(weights.index), errors="coerce").fillna(0.0)
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.rolling(int(drawdown_window), min_periods=20).max() - 1.0
    rebound = equity / equity.shift(int(rebound_window)) - 1.0
    realized_vol = returns.rolling(int(vol_window)).std(ddof=0) * np.sqrt(252)
    panic_rebound = (
        drawdown.le(-float(drawdown_threshold))
        & rebound.ge(float(rebound_threshold))
        & realized_vol.ge(float(vol_threshold))
    )
    rebalance_dates = weights.loc[:, stock_columns].diff().abs().sum(axis=1).gt(1e-12)
    if not rebalance_dates.empty:
        rebalance_dates.iloc[0] = True
    scaler = pd.Series(1.0, index=weights.index)
    scaler.loc[panic_rebound] = float(stock_exposure)
    scaler = scaler.where(rebalance_dates).ffill().fillna(1.0)

    output = weights.copy()
    output.loc[:, stock_columns] = output.loc[:, stock_columns].mul(scaler, axis=0)
    stock_weight = output.loc[:, stock_columns].sum(axis=1).clip(lower=0.0, upper=1.0)
    output[safe_symbol] = 1.0 - stock_weight
    return output.reindex(columns=weights_history.columns.union([safe_symbol], sort=False), fill_value=0.0)


def _summary_for_variant(
    *,
    run_name: str,
    variant_type: str,
    weights: pd.DataFrame,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    universe_lag_trading_days: int,
    max_names_per_sector: int | None = None,
    sector_score_penalty: float | None = None,
    residual_momentum_weight: float | None = None,
    beta_penalty_weight: float | None = None,
    vol_target: float | None = None,
    vol_target_window: int | None = None,
    min_stock_exposure: float | None = None,
    panic_drawdown_threshold: float | None = None,
    panic_rebound_threshold: float | None = None,
    panic_vol_threshold: float | None = None,
    panic_stock_exposure: float | None = None,
    top2_blend_weight: float | None = None,
    dynamic_drawdown_threshold: float | None = None,
    top4_mode_share: float | None = None,
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
            pool_name="dynamic_top50_concentration",
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
        )
    )
    summary.update(
        {
            "Run": run_name,
            "Variant Type": variant_type,
            "Universe Lag Trading Days": int(universe_lag_trading_days),
            "Max Names Per Sector": max_names_per_sector,
            "Sector Score Penalty": sector_score_penalty,
            "Residual Momentum Weight": residual_momentum_weight,
            "Beta Penalty Weight": beta_penalty_weight,
            "Vol Target": vol_target,
            "Vol Target Window": vol_target_window,
            "Min Stock Exposure": min_stock_exposure,
            "Panic Drawdown Threshold": panic_drawdown_threshold,
            "Panic Rebound Threshold": panic_rebound_threshold,
            "Panic Vol Threshold": panic_vol_threshold,
            "Panic Stock Exposure": panic_stock_exposure,
            "Top2 Blend Weight": top2_blend_weight,
            "Top4 Blend Weight": (1.0 - top2_blend_weight) if top2_blend_weight is not None else None,
            "Dynamic Drawdown Threshold": dynamic_drawdown_threshold,
            "Top4 Mode Share": top4_mode_share,
        }
    )
    return summary


def run_concentration_variant_research(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    universe_lag_trading_days: int = 21,
    blend_top2_weights: Iterable[float] = DEFAULT_BLEND_TOP2_WEIGHTS,
    dynamic_drawdown_thresholds: Iterable[float] = DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    turnover_cost_bps: float = 5.0,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
    include_sector_capped_variants: bool = False,
    sector_cap_values: Iterable[int] = DEFAULT_SECTOR_CAP_VALUES,
    include_sector_soft_penalty_variants: bool = False,
    sector_score_penalty_values: Iterable[float] = DEFAULT_SECTOR_SCORE_PENALTY_VALUES,
    include_residual_momentum_variants: bool = False,
    residual_momentum_weights: Iterable[float] = DEFAULT_RESIDUAL_MOMENTUM_WEIGHTS,
    beta_penalty_weights: Iterable[float] = DEFAULT_BETA_PENALTY_WEIGHTS,
    include_volatility_managed_variants: bool = False,
    vol_target_values: Iterable[float] = DEFAULT_VOL_TARGET_VALUES,
    vol_target_window: int = DEFAULT_VOL_TARGET_WINDOW,
    vol_target_min_stock_exposure: float = DEFAULT_VOL_TARGET_MIN_STOCK_EXPOSURE,
    include_panic_rebound_guard_variants: bool = False,
    panic_guard_drawdown_threshold: float = DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
    panic_guard_rebound_threshold: float = DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
    panic_guard_vol_threshold: float = DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
    panic_guard_stock_exposure: float = DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
) -> dict[str, pd.DataFrame]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    trading_index = _trading_index(prices)
    lagged_universe = lag_universe_history(
        universe_history,
        lag_trading_days=int(universe_lag_trading_days),
        trading_index=trading_index,
    )
    base_kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "pool_name": f"dynamic_top50_lag{int(universe_lag_trading_days)}",
        "benchmark_symbol": benchmark_symbol,
        "broad_benchmark_symbol": broad_benchmark_symbol,
        "safe_haven": safe_haven,
        "risk_on_exposure": 1.0,
        "soft_defense_exposure": 1.0,
        "hard_defense_exposure": 1.0,
        "turnover_cost_bps": float(turnover_cost_bps),
        "min_price_usd": float(min_price_usd),
        "min_adv20_usd": float(min_adv20_usd),
        "min_history_days": int(min_history_days),
    }
    top2 = run_backtest(
        prices,
        lagged_universe,
        top_n=2,
        single_name_cap=0.50,
        max_names_per_sector=0,
        **base_kwargs,
    )
    top4 = run_backtest(
        prices,
        lagged_universe,
        top_n=4,
        single_name_cap=0.25,
        max_names_per_sector=0,
        **base_kwargs,
    )

    _close_matrix, returns_matrix = _build_close_and_returns(prices)
    index = pd.DatetimeIndex(top2["portfolio_returns"].index)
    reference_returns = top2["reference_returns"].reindex(index)
    weight_columns = sorted(
        set(top2["weights_history"].columns)
        | set(top4["weights_history"].columns)
        | set(returns_matrix.columns)
    )
    returns_matrix = returns_matrix.reindex(index).reindex(columns=weight_columns, fill_value=0.0).fillna(0.0)
    top2_weights = _align_weights(top2["weights_history"], index=index, columns=weight_columns)
    top4_weights = _align_weights(top4["weights_history"], index=index, columns=weight_columns)

    variants: list[tuple[str, str, pd.DataFrame, float | None, float | None, float | None]] = [
        ("base_top2_cap50", "base_top2", top2_weights, 1.0, None, 0.0),
        ("base_top4_cap25", "base_top4", top4_weights, 0.0, None, 1.0),
    ]
    variant_sector_caps: dict[str, int | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_sector_penalties: dict[str, float | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_residual_weights: dict[str, float | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_beta_penalty_weights: dict[str, float | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_vol_targets: dict[str, float | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_vol_target_windows: dict[str, int | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_min_stock_exposures: dict[str, float | None] = {
        "base_top2_cap50": None,
        "base_top4_cap25": None,
    }
    variant_panic_drawdown_thresholds: dict[str, float | None] = {}
    variant_panic_rebound_thresholds: dict[str, float | None] = {}
    variant_panic_vol_thresholds: dict[str, float | None] = {}
    variant_panic_stock_exposures: dict[str, float | None] = {}
    for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
        if not 0.0 < top2_weight < 1.0:
            continue
        run_name = _variant_name_for_blend(top2_weight)
        weights = float(top2_weight) * top2_weights + (1.0 - float(top2_weight)) * top4_weights
        variants.append((run_name, "fixed_blend", weights, float(top2_weight), None, 1.0 - float(top2_weight)))
        variant_sector_caps[run_name] = None
        variant_sector_penalties[run_name] = None
        variant_residual_weights[run_name] = None
        variant_beta_penalty_weights[run_name] = None
        variant_vol_targets[run_name] = None
        variant_vol_target_windows[run_name] = None
        variant_min_stock_exposures[run_name] = None

    if include_volatility_managed_variants:
        benchmark_returns = reference_returns[benchmark_symbol] if benchmark_symbol in reference_returns.columns else pd.Series(index=index, dtype=float)
        base_weight_specs = [
            ("top2_cap50", "volatility_managed_base_top2", top2_weights, 1.0, 0.0),
            ("top4_cap25", "volatility_managed_base_top4", top4_weights, 0.0, 1.0),
        ]
        for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
            if not 0.0 < top2_weight < 1.0:
                continue
            blend_name = _variant_name_for_blend(top2_weight)
            blend_weights = float(top2_weight) * top2_weights + (1.0 - float(top2_weight)) * top4_weights
            base_weight_specs.append(
                (blend_name, "volatility_managed_fixed_blend", blend_weights, float(top2_weight), 1.0 - float(top2_weight))
            )
        for vol_target in parse_csv_floats(tuple(vol_target_values), default=DEFAULT_VOL_TARGET_VALUES):
            target = float(vol_target)
            if target <= 0.0:
                continue
            for base_name, variant_type, base_weights, blend_weight, top4_share in base_weight_specs:
                run_name = _vol_target_variant_name(
                    base_name,
                    vol_target=target,
                    min_stock_exposure=float(vol_target_min_stock_exposure),
                )
                weights = _apply_volatility_target_exposure(
                    base_weights,
                    benchmark_returns,
                    safe_haven=safe_haven,
                    vol_target=target,
                    min_stock_exposure=float(vol_target_min_stock_exposure),
                    vol_window=int(vol_target_window),
                )
                variants.append((run_name, variant_type, weights, blend_weight, None, top4_share))
                variant_sector_caps[run_name] = None
                variant_sector_penalties[run_name] = None
                variant_residual_weights[run_name] = None
                variant_beta_penalty_weights[run_name] = None
                variant_vol_targets[run_name] = target
                variant_vol_target_windows[run_name] = int(vol_target_window)
                variant_min_stock_exposures[run_name] = float(vol_target_min_stock_exposure)

    if include_panic_rebound_guard_variants:
        benchmark_returns = (
            reference_returns[benchmark_symbol]
            if benchmark_symbol in reference_returns.columns
            else pd.Series(index=index, dtype=float)
        )
        base_weight_specs = [
            ("top2_cap50", "panic_rebound_guard_base_top2", top2_weights, 1.0, 0.0),
            ("top4_cap25", "panic_rebound_guard_base_top4", top4_weights, 0.0, 1.0),
        ]
        for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
            if not 0.0 < top2_weight < 1.0:
                continue
            blend_name = _variant_name_for_blend(top2_weight)
            blend_weights = float(top2_weight) * top2_weights + (1.0 - float(top2_weight)) * top4_weights
            base_weight_specs.append(
                (
                    blend_name,
                    "panic_rebound_guard_fixed_blend",
                    blend_weights,
                    float(top2_weight),
                    1.0 - float(top2_weight),
                )
            )
        for base_name, variant_type, base_weights, blend_weight, top4_share in base_weight_specs:
            run_name = _panic_guard_variant_name(
                base_name,
                drawdown_threshold=float(panic_guard_drawdown_threshold),
                rebound_threshold=float(panic_guard_rebound_threshold),
                vol_threshold=float(panic_guard_vol_threshold),
                stock_exposure=float(panic_guard_stock_exposure),
            )
            weights = _apply_panic_rebound_guard(
                base_weights,
                benchmark_returns,
                safe_haven=safe_haven,
                drawdown_threshold=float(panic_guard_drawdown_threshold),
                rebound_threshold=float(panic_guard_rebound_threshold),
                vol_threshold=float(panic_guard_vol_threshold),
                stock_exposure=float(panic_guard_stock_exposure),
            )
            variants.append((run_name, variant_type, weights, blend_weight, None, top4_share))
            variant_sector_caps[run_name] = None
            variant_sector_penalties[run_name] = None
            variant_residual_weights[run_name] = None
            variant_beta_penalty_weights[run_name] = None
            variant_vol_targets[run_name] = None
            variant_vol_target_windows[run_name] = None
            variant_min_stock_exposures[run_name] = None
            variant_panic_drawdown_thresholds[run_name] = float(panic_guard_drawdown_threshold)
            variant_panic_rebound_thresholds[run_name] = float(panic_guard_rebound_threshold)
            variant_panic_vol_thresholds[run_name] = float(panic_guard_vol_threshold)
            variant_panic_stock_exposures[run_name] = float(panic_guard_stock_exposure)

    if include_sector_capped_variants:
        for sector_cap in parse_csv_ints(tuple(sector_cap_values), default=DEFAULT_SECTOR_CAP_VALUES):
            max_sector_count = int(sector_cap)
            if max_sector_count <= 0:
                continue
            sector_top2 = run_backtest(
                prices,
                lagged_universe,
                top_n=2,
                single_name_cap=0.50,
                max_names_per_sector=max_sector_count,
                **base_kwargs,
            )
            sector_top4 = run_backtest(
                prices,
                lagged_universe,
                top_n=4,
                single_name_cap=0.25,
                max_names_per_sector=max_sector_count,
                **base_kwargs,
            )
            sector_top2_weights = _align_weights(sector_top2["weights_history"], index=index, columns=weight_columns)
            sector_top4_weights = _align_weights(sector_top4["weights_history"], index=index, columns=weight_columns)
            top2_name = _sector_variant_name("top2_cap50", max_sector_count)
            top4_name = _sector_variant_name("top4_cap25", max_sector_count)
            variants.append((top2_name, "sector_capped_base_top2", sector_top2_weights, 1.0, None, 0.0))
            variants.append((top4_name, "sector_capped_base_top4", sector_top4_weights, 0.0, None, 1.0))
            variant_sector_caps[top2_name] = max_sector_count
            variant_sector_caps[top4_name] = max_sector_count
            variant_sector_penalties[top2_name] = None
            variant_sector_penalties[top4_name] = None
            variant_residual_weights[top2_name] = None
            variant_residual_weights[top4_name] = None
            variant_beta_penalty_weights[top2_name] = None
            variant_beta_penalty_weights[top4_name] = None
            variant_vol_targets[top2_name] = None
            variant_vol_targets[top4_name] = None
            variant_vol_target_windows[top2_name] = None
            variant_vol_target_windows[top4_name] = None
            variant_min_stock_exposures[top2_name] = None
            variant_min_stock_exposures[top4_name] = None
            for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
                if not 0.0 < top2_weight < 1.0:
                    continue
                base_blend_name = _variant_name_for_blend(top2_weight)
                run_name = _sector_variant_name(base_blend_name, max_sector_count)
                weights = float(top2_weight) * sector_top2_weights + (1.0 - float(top2_weight)) * sector_top4_weights
                variants.append(
                    (
                        run_name,
                        "sector_capped_fixed_blend",
                        weights,
                        float(top2_weight),
                        None,
                        1.0 - float(top2_weight),
                    )
                )
                variant_sector_caps[run_name] = max_sector_count
                variant_sector_penalties[run_name] = None
                variant_residual_weights[run_name] = None
                variant_beta_penalty_weights[run_name] = None
                variant_vol_targets[run_name] = None
                variant_vol_target_windows[run_name] = None
                variant_min_stock_exposures[run_name] = None

    if include_sector_soft_penalty_variants:
        for penalty in parse_csv_floats(
            tuple(sector_score_penalty_values),
            default=DEFAULT_SECTOR_SCORE_PENALTY_VALUES,
        ):
            sector_score_penalty = float(penalty)
            if sector_score_penalty <= 0.0:
                continue
            penalty_top2 = run_backtest(
                prices,
                lagged_universe,
                top_n=2,
                single_name_cap=0.50,
                max_names_per_sector=0,
                sector_score_penalty=sector_score_penalty,
                **base_kwargs,
            )
            penalty_top4 = run_backtest(
                prices,
                lagged_universe,
                top_n=4,
                single_name_cap=0.25,
                max_names_per_sector=0,
                sector_score_penalty=sector_score_penalty,
                **base_kwargs,
            )
            penalty_top2_weights = _align_weights(penalty_top2["weights_history"], index=index, columns=weight_columns)
            penalty_top4_weights = _align_weights(penalty_top4["weights_history"], index=index, columns=weight_columns)
            top2_name = _penalty_variant_name("top2_cap50", sector_score_penalty)
            top4_name = _penalty_variant_name("top4_cap25", sector_score_penalty)
            variants.append((top2_name, "sector_soft_penalty_base_top2", penalty_top2_weights, 1.0, None, 0.0))
            variants.append((top4_name, "sector_soft_penalty_base_top4", penalty_top4_weights, 0.0, None, 1.0))
            variant_sector_caps[top2_name] = None
            variant_sector_caps[top4_name] = None
            variant_sector_penalties[top2_name] = sector_score_penalty
            variant_sector_penalties[top4_name] = sector_score_penalty
            variant_residual_weights[top2_name] = None
            variant_residual_weights[top4_name] = None
            variant_beta_penalty_weights[top2_name] = None
            variant_beta_penalty_weights[top4_name] = None
            variant_vol_targets[top2_name] = None
            variant_vol_targets[top4_name] = None
            variant_vol_target_windows[top2_name] = None
            variant_vol_target_windows[top4_name] = None
            variant_min_stock_exposures[top2_name] = None
            variant_min_stock_exposures[top4_name] = None
            for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
                if not 0.0 < top2_weight < 1.0:
                    continue
                base_blend_name = _variant_name_for_blend(top2_weight)
                run_name = _penalty_variant_name(base_blend_name, sector_score_penalty)
                weights = float(top2_weight) * penalty_top2_weights + (1.0 - float(top2_weight)) * penalty_top4_weights
                variants.append(
                    (
                        run_name,
                        "sector_soft_penalty_fixed_blend",
                        weights,
                        float(top2_weight),
                        None,
                        1.0 - float(top2_weight),
                    )
                )
                variant_sector_caps[run_name] = None
                variant_sector_penalties[run_name] = sector_score_penalty
                variant_residual_weights[run_name] = None
                variant_beta_penalty_weights[run_name] = None
                variant_vol_targets[run_name] = None
                variant_vol_target_windows[run_name] = None
                variant_min_stock_exposures[run_name] = None

    residual_beta_specs: list[tuple[float, float]] = []
    if include_residual_momentum_variants:
        for residual_weight in parse_csv_floats(
            tuple(residual_momentum_weights),
            default=DEFAULT_RESIDUAL_MOMENTUM_WEIGHTS,
        ):
            if float(residual_weight) > 0.0:
                residual_beta_specs.append((float(residual_weight), 0.0))
        for beta_weight in parse_csv_floats(tuple(beta_penalty_weights), default=DEFAULT_BETA_PENALTY_WEIGHTS):
            if float(beta_weight) > 0.0:
                residual_beta_specs.append((0.0, float(beta_weight)))

    for residual_weight, beta_weight in residual_beta_specs:
        residual_top2 = run_backtest(
            prices,
            lagged_universe,
            top_n=2,
            single_name_cap=0.50,
            max_names_per_sector=0,
            residual_momentum_weight=residual_weight,
            beta_penalty_weight=beta_weight,
            **base_kwargs,
        )
        residual_top4 = run_backtest(
            prices,
            lagged_universe,
            top_n=4,
            single_name_cap=0.25,
            max_names_per_sector=0,
            residual_momentum_weight=residual_weight,
            beta_penalty_weight=beta_weight,
            **base_kwargs,
        )
        residual_top2_weights = _align_weights(residual_top2["weights_history"], index=index, columns=weight_columns)
        residual_top4_weights = _align_weights(residual_top4["weights_history"], index=index, columns=weight_columns)
        top2_name = _residual_beta_variant_name(
            "top2_cap50",
            residual_momentum_weight=residual_weight,
            beta_penalty_weight=beta_weight,
        )
        top4_name = _residual_beta_variant_name(
            "top4_cap25",
            residual_momentum_weight=residual_weight,
            beta_penalty_weight=beta_weight,
        )
        variants.append((top2_name, "residual_beta_base_top2", residual_top2_weights, 1.0, None, 0.0))
        variants.append((top4_name, "residual_beta_base_top4", residual_top4_weights, 0.0, None, 1.0))
        variant_sector_caps[top2_name] = None
        variant_sector_caps[top4_name] = None
        variant_sector_penalties[top2_name] = None
        variant_sector_penalties[top4_name] = None
        variant_residual_weights[top2_name] = residual_weight
        variant_residual_weights[top4_name] = residual_weight
        variant_beta_penalty_weights[top2_name] = beta_weight
        variant_beta_penalty_weights[top4_name] = beta_weight
        variant_vol_targets[top2_name] = None
        variant_vol_targets[top4_name] = None
        variant_vol_target_windows[top2_name] = None
        variant_vol_target_windows[top4_name] = None
        variant_min_stock_exposures[top2_name] = None
        variant_min_stock_exposures[top4_name] = None
        for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
            if not 0.0 < top2_weight < 1.0:
                continue
            base_blend_name = _variant_name_for_blend(top2_weight)
            run_name = _residual_beta_variant_name(
                base_blend_name,
                residual_momentum_weight=residual_weight,
                beta_penalty_weight=beta_weight,
            )
            weights = float(top2_weight) * residual_top2_weights + (1.0 - float(top2_weight)) * residual_top4_weights
            variants.append((run_name, "residual_beta_fixed_blend", weights, float(top2_weight), None, 1.0 - float(top2_weight)))
            variant_sector_caps[run_name] = None
            variant_sector_penalties[run_name] = None
            variant_residual_weights[run_name] = residual_weight
            variant_beta_penalty_weights[run_name] = beta_weight
            variant_vol_targets[run_name] = None
            variant_vol_target_windows[run_name] = None
            variant_min_stock_exposures[run_name] = None

    mode_rows: list[dict[str, object]] = []
    for threshold in parse_csv_floats(
        tuple(dynamic_drawdown_thresholds),
        default=DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
    ):
        if not 0.0 < threshold < 1.0:
            continue
        run_name = _variant_name_for_drawdown(threshold)
        weights, rows, top4_share = _build_dynamic_weights(
            run_name=run_name,
            threshold=float(threshold),
            index=index,
            top2_weights=top2_weights,
            top4_weights=top4_weights,
            top2_returns=top2["portfolio_returns"],
            top2_exposure_history=top2["exposure_history"],
        )
        mode_rows.extend(rows)
        variants.append(
            (
                run_name,
                "dynamic_top2_drawdown_to_top4",
                weights,
                None,
                float(threshold),
                top4_share,
            )
        )

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []
    daily_return_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    rolling_values = parse_csv_ints(tuple(rolling_window_years), default=DEFAULT_ROLLING_WINDOW_YEARS)
    for run_name, variant_type, weights, blend_weight, threshold, top4_share in variants:
        returns = _returns_from_weights(weights, returns_matrix, turnover_cost_bps=float(turnover_cost_bps))
        summary_rows.append(
            _summary_for_variant(
                run_name=run_name,
                variant_type=variant_type,
                weights=weights,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                universe_lag_trading_days=int(universe_lag_trading_days),
                max_names_per_sector=variant_sector_caps.get(run_name),
                sector_score_penalty=variant_sector_penalties.get(run_name),
                residual_momentum_weight=variant_residual_weights.get(run_name),
                beta_penalty_weight=variant_beta_penalty_weights.get(run_name),
                vol_target=variant_vol_targets.get(run_name),
                vol_target_window=variant_vol_target_windows.get(run_name),
                min_stock_exposure=variant_min_stock_exposures.get(run_name),
                panic_drawdown_threshold=variant_panic_drawdown_thresholds.get(run_name),
                panic_rebound_threshold=variant_panic_rebound_thresholds.get(run_name),
                panic_vol_threshold=variant_panic_vol_thresholds.get(run_name),
                panic_stock_exposure=variant_panic_stock_exposures.get(run_name),
                top2_blend_weight=blend_weight,
                dynamic_drawdown_threshold=threshold,
                top4_mode_share=top4_share,
            )
        )
        trade_rows.extend(
            _build_rebalance_trade_rows(
                run_name=run_name,
                variant_type=variant_type,
                weights=weights,
            )
        )
        daily_return_rows.extend(
            _build_daily_return_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
            )
        )
        yearly_rows.extend(
            _build_yearly_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
            )
        )
        rolling_rows.extend(
            _build_rolling_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                rolling_window_years=rolling_values,
            )
        )

    summary = pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS)
    yearly = pd.DataFrame(yearly_rows, columns=YEARLY_COLUMNS)
    rolling = pd.DataFrame(rolling_rows, columns=ROLLING_COLUMNS)
    daily_returns = pd.DataFrame(daily_return_rows, columns=DAILY_RETURN_COLUMNS)
    rebalance_trades = pd.DataFrame(trade_rows, columns=TRADE_COLUMNS)
    mode_history = pd.DataFrame(mode_rows, columns=MODE_COLUMNS)
    return {
        "concentration_variant_summary": summary,
        "concentration_variant_yearly_summary": yearly,
        "concentration_variant_rolling_summary": rolling,
        "concentration_variant_daily_returns": daily_returns,
        "concentration_variant_rebalance_trades": rebalance_trades,
        "concentration_variant_mode_history": mode_history,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research derived Top2/Top4 concentration variants for dynamic Russell Top50 leader rotation."
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for research outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", type=int, default=21)
    parser.add_argument(
        "--blend-top2-weights",
        default=",".join(str(value) for value in DEFAULT_BLEND_TOP2_WEIGHTS),
        help="Comma-separated Top2 sleeve weights for fixed Top2/Top4 blends",
    )
    parser.add_argument(
        "--dynamic-drawdown-thresholds",
        default=",".join(str(value) for value in DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS),
        help="Comma-separated Top2 shadow drawdown thresholds that switch to Top4",
    )
    parser.add_argument(
        "--rolling-window-years",
        default="",
        help="Comma-separated complete-calendar-year rolling windows to summarize, for example 3,5",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument(
        "--include-sector-capped-variants",
        action="store_true",
        help="Also test research-only Top2/Top4 blend variants with a per-sector selected-name cap.",
    )
    parser.add_argument(
        "--sector-cap-values",
        default=",".join(str(value) for value in DEFAULT_SECTOR_CAP_VALUES),
        help="Comma-separated max selected names per sector values used when sector-capped variants are enabled.",
    )
    parser.add_argument(
        "--include-sector-soft-penalty-variants",
        action="store_true",
        help="Also test research-only variants that subtract a soft score penalty for repeated sectors.",
    )
    parser.add_argument(
        "--sector-score-penalty-values",
        default=",".join(str(value) for value in DEFAULT_SECTOR_SCORE_PENALTY_VALUES),
        help="Comma-separated soft score penalties used when sector soft-penalty variants are enabled.",
    )
    parser.add_argument(
        "--include-residual-momentum-variants",
        action="store_true",
        help="Also test research-only variants using QQQ beta-adjusted residual momentum or beta penalties.",
    )
    parser.add_argument(
        "--residual-momentum-weights",
        default=",".join(str(value) for value in DEFAULT_RESIDUAL_MOMENTUM_WEIGHTS),
        help="Comma-separated residual momentum score weights used when residual variants are enabled.",
    )
    parser.add_argument(
        "--beta-penalty-weights",
        default=",".join(str(value) for value in DEFAULT_BETA_PENALTY_WEIGHTS),
        help="Comma-separated beta penalty score weights used when residual variants are enabled.",
    )
    parser.add_argument(
        "--include-volatility-managed-variants",
        action="store_true",
        help=(
            "Also test research-only variants that scale stock exposure down on base-strategy rebalance dates "
            "when benchmark volatility is high."
        ),
    )
    parser.add_argument(
        "--vol-target-values",
        default=",".join(str(value) for value in DEFAULT_VOL_TARGET_VALUES),
        help="Comma-separated annualized benchmark volatility targets used when volatility variants are enabled.",
    )
    parser.add_argument(
        "--vol-target-window",
        type=int,
        default=DEFAULT_VOL_TARGET_WINDOW,
        help="Rolling trading-day window used to estimate benchmark volatility.",
    )
    parser.add_argument(
        "--vol-target-min-stock-exposure",
        type=float,
        default=DEFAULT_VOL_TARGET_MIN_STOCK_EXPOSURE,
        help="Minimum stock exposure multiplier for volatility-managed variants.",
    )
    parser.add_argument(
        "--include-panic-rebound-guard-variants",
        action="store_true",
        help="Also test research-only variants that cut stock exposure in high-volatility rebound states.",
    )
    parser.add_argument(
        "--panic-guard-drawdown-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
        help="QQQ rolling drawdown threshold used by panic-rebound guard variants.",
    )
    parser.add_argument(
        "--panic-guard-rebound-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
        help="QQQ 21-trading-day rebound threshold used by panic-rebound guard variants.",
    )
    parser.add_argument(
        "--panic-guard-vol-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
        help="QQQ 63-trading-day annualized volatility threshold used by panic-rebound guard variants.",
    )
    parser.add_argument(
        "--panic-guard-stock-exposure",
        type=float,
        default=DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
        help="Stock exposure multiplier applied while the panic-rebound guard is active.",
    )
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = run_concentration_variant_research(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=int(args.universe_lag_days),
        blend_top2_weights=parse_csv_floats(args.blend_top2_weights, default=DEFAULT_BLEND_TOP2_WEIGHTS),
        dynamic_drawdown_thresholds=parse_csv_floats(
            args.dynamic_drawdown_thresholds,
            default=DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
        ),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        turnover_cost_bps=args.turnover_cost_bps,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
        include_sector_capped_variants=bool(args.include_sector_capped_variants),
        sector_cap_values=parse_csv_ints(args.sector_cap_values, default=DEFAULT_SECTOR_CAP_VALUES),
        include_sector_soft_penalty_variants=bool(args.include_sector_soft_penalty_variants),
        sector_score_penalty_values=parse_csv_floats(
            args.sector_score_penalty_values,
            default=DEFAULT_SECTOR_SCORE_PENALTY_VALUES,
        ),
        include_residual_momentum_variants=bool(args.include_residual_momentum_variants),
        residual_momentum_weights=parse_csv_floats(
            args.residual_momentum_weights,
            default=DEFAULT_RESIDUAL_MOMENTUM_WEIGHTS,
        ),
        beta_penalty_weights=parse_csv_floats(args.beta_penalty_weights, default=DEFAULT_BETA_PENALTY_WEIGHTS),
        include_volatility_managed_variants=bool(args.include_volatility_managed_variants),
        vol_target_values=parse_csv_floats(args.vol_target_values, default=DEFAULT_VOL_TARGET_VALUES),
        vol_target_window=int(args.vol_target_window),
        vol_target_min_stock_exposure=float(args.vol_target_min_stock_exposure),
        include_panic_rebound_guard_variants=bool(args.include_panic_rebound_guard_variants),
        panic_guard_drawdown_threshold=float(args.panic_guard_drawdown_threshold),
        panic_guard_rebound_threshold=float(args.panic_guard_rebound_threshold),
        panic_guard_vol_threshold=float(args.panic_guard_vol_threshold),
        panic_guard_stock_exposure=float(args.panic_guard_stock_exposure),
    )
    summary_path = output_dir / "concentration_variant_summary.csv"
    yearly_path = output_dir / "concentration_variant_yearly_summary.csv"
    rolling_path = output_dir / "concentration_variant_rolling_summary.csv"
    daily_returns_path = output_dir / "concentration_variant_daily_returns.csv"
    rebalance_trades_path = output_dir / "concentration_variant_rebalance_trades.csv"
    mode_path = output_dir / "concentration_variant_mode_history.csv"
    result["concentration_variant_summary"].to_csv(summary_path, index=False)
    result["concentration_variant_yearly_summary"].to_csv(yearly_path, index=False)
    result["concentration_variant_rolling_summary"].to_csv(rolling_path, index=False)
    result["concentration_variant_daily_returns"].to_csv(daily_returns_path, index=False)
    result["concentration_variant_rebalance_trades"].to_csv(rebalance_trades_path, index=False)
    result["concentration_variant_mode_history"].to_csv(mode_path, index=False)
    print(result["concentration_variant_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote concentration variant summary -> {summary_path}")
    print(f"wrote concentration variant yearly summary -> {yearly_path}")
    print(f"wrote concentration variant rolling summary -> {rolling_path}")
    print(f"wrote concentration variant daily returns -> {daily_returns_path}")
    print(f"wrote concentration variant rebalance trades -> {rebalance_trades_path}")
    print(f"wrote concentration variant mode history -> {mode_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
