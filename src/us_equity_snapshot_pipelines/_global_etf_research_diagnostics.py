"""Robustness, liquidity, walk-forward, and live-readiness diagnostics."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

from ._global_etf_research_metrics import (
    _normalize_price_history,
    _benchmark_summary,
    summarize_returns,
)
from ._global_etf_research_models import (
    DEFAULT_PERIODS,
    MIN_TRADING_DAYS_PER_PERIOD,
    DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW,
    DEFAULT_LIVE_BASELINE_CANDIDATE,
    DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME,
    DEFAULT_PRIMARY_BENCHMARK,
    DEFAULT_ROBUSTNESS_CANDIDATES,
    DEFAULT_ROLLING_ROBUSTNESS_YEARS,
    DEFAULT_SECONDARY_BENCHMARK,
    DEFAULT_WALK_FORWARD_CANDIDATES,
    DEFAULT_WALK_FORWARD_MIN_TEST_DAYS,
    DEFAULT_WALK_FORWARD_MIN_TRAIN_DAYS_PER_YEAR,
    DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR,
    DEFAULT_WALK_FORWARD_TRAIN_YEARS,
    DRAWNDOWN_ADVANTAGE_VS_QQQ,
    LIVE_MAX_LONG_DRAWDOWN_DEGRADATION_VS_BASELINE,
    LIVE_MAX_MEDIAN_TURNOVER_INCREASE_VS_BASELINE,
    LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE,
    LIVE_MIN_CALENDAR_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_LONG_EXCESS_CAGR_VS_BASELINE,
    LIVE_MIN_ROLLING_3Y_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_ROLLING_5Y_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_WORST_ROLLING_EXCESS_CAGR_VS_BASELINE,
    SAFE_LIKE_SYMBOLS,
    WALK_FORWARD_MAX_WORST_OOS_DRAWDOWN_DEGRADATION,
    WALK_FORWARD_MIN_OOS_MEDIAN_EXCESS_CAGR,
    WALK_FORWARD_MIN_OOS_WIN_RATE,
    WALK_FORWARD_MIN_WINDOW_COUNT,
    WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR,
    DynamicExecutionCostConfig,
)

def _gate_reason(
    *,
    all_periods_available: bool,
    positive_return_all_periods: bool,
    positive_sharpe_all_periods: bool,
    long_spy_gate: bool,
    median_spy_gate: bool,
    qqq_tradeoff_gate: bool,
) -> str:
    reasons: list[str] = []
    if not all_periods_available:
        reasons.append("missing_or_too_short_period")
    if not positive_return_all_periods:
        reasons.append("non_positive_cagr_period")
    if not positive_sharpe_all_periods:
        reasons.append("non_positive_sharpe_period")
    if not long_spy_gate:
        reasons.append("long_cagr_not_above_spy")
    if not median_spy_gate:
        reasons.append("median_cagr_not_above_spy")
    if not qqq_tradeoff_gate:
        reasons.append("not_qqq_competitive_on_return_or_drawdown")
    return "pass" if not reasons else ";".join(reasons)


def _summarize_candidate_window(
    *,
    candidate_id: str,
    window_type: str,
    window: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
    secondary_benchmark_returns: pd.Series,
    min_trading_days: int,
    weights_history: pd.DataFrame | None = None,
) -> dict[str, object] | None:
    subset = pd.Series(portfolio_returns).loc[start:end].dropna()
    if len(subset) < int(min_trading_days):
        return None
    weights_subset = None
    if weights_history is not None and not weights_history.empty:
        weights = pd.DataFrame(weights_history).copy()
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights_subset = weights.loc[start:end]
    summary = summarize_returns(
        subset,
        weights_history=weights_subset,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start.date().isoformat(),
        end_date=end.date().isoformat(),
    )
    summary["Candidate"] = candidate_id
    summary["Window Type"] = window_type
    summary["Window"] = window
    summary["Requested Start"] = start.date().isoformat()
    summary["Requested End"] = end.date().isoformat()
    return summary


def build_candidate_robustness_diagnostics(
    *,
    price_history: pd.DataFrame,
    portfolio_returns: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame] | None = None,
    candidate_ids: Sequence[str] = DEFAULT_ROBUSTNESS_CANDIDATES,
    rolling_years: Sequence[int] = DEFAULT_ROLLING_ROBUSTNESS_YEARS,
    primary_benchmark_symbol: str = DEFAULT_PRIMARY_BENCHMARK,
    secondary_benchmark_symbol: str = DEFAULT_SECONDARY_BENCHMARK,
    min_calendar_trading_days: int = 120,
    min_rolling_trading_days_per_year: int = 180,
) -> dict[str, pd.DataFrame]:
    close = _normalize_price_history(price_history)
    daily_returns = close.pct_change(fill_method=None).fillna(0.0)
    primary_symbol = str(primary_benchmark_symbol).strip().upper()
    secondary_symbol = str(secondary_benchmark_symbol).strip().upper()
    primary_returns = daily_returns.get(primary_symbol, pd.Series(index=daily_returns.index, dtype=float))
    secondary_returns = daily_returns.get(secondary_symbol, pd.Series(index=daily_returns.index, dtype=float))
    weights_map = dict(weights_by_candidate or {})
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if portfolio.empty:
        return {"robustness_windows": pd.DataFrame(), "robustness_summary": pd.DataFrame()}
    portfolio.index = pd.to_datetime(portfolio.index).tz_localize(None).normalize()

    rows: list[dict[str, object]] = []
    for candidate_id in tuple(dict.fromkeys(str(item).strip() for item in candidate_ids if str(item).strip())):
        if candidate_id not in portfolio.columns:
            continue
        candidate_returns = pd.to_numeric(portfolio[candidate_id], errors="coerce").dropna()
        if candidate_returns.empty:
            continue
        candidate_weights = weights_map.get(candidate_id)
        for year, year_returns in candidate_returns.groupby(candidate_returns.index.year):
            start = pd.Timestamp(year_returns.index.min()).normalize()
            end = pd.Timestamp(year_returns.index.max()).normalize()
            row = _summarize_candidate_window(
                candidate_id=candidate_id,
                window_type="calendar_year",
                window=str(int(year)),
                start=start,
                end=end,
                portfolio_returns=candidate_returns,
                benchmark_returns=primary_returns,
                secondary_benchmark_returns=secondary_returns,
                min_trading_days=min_calendar_trading_days,
                weights_history=candidate_weights,
            )
            if row is not None:
                rows.append(row)

        year_end_dates = (
            pd.Series(candidate_returns.index, index=candidate_returns.index)
            .groupby(candidate_returns.index.to_period("Y"))
            .tail(1)
        )
        for years in tuple(int(value) for value in rolling_years):
            min_days = int(years) * int(min_rolling_trading_days_per_year)
            for end_value in year_end_dates:
                end = pd.Timestamp(end_value).normalize()
                start = pd.Timestamp(end - pd.DateOffset(years=int(years)) + pd.DateOffset(days=1)).normalize()
                row = _summarize_candidate_window(
                    candidate_id=candidate_id,
                    window_type=f"rolling_{years}y",
                    window=f"{start.date().isoformat()}_{end.date().isoformat()}",
                    start=start,
                    end=end,
                    portfolio_returns=candidate_returns,
                    benchmark_returns=primary_returns,
                    secondary_benchmark_returns=secondary_returns,
                    min_trading_days=min_days,
                    weights_history=candidate_weights,
                )
                if row is not None:
                    rows.append(row)

    windows = pd.DataFrame(rows)
    if windows.empty:
        return {"robustness_windows": windows, "robustness_summary": pd.DataFrame()}
    for column in (
        "CAGR",
        "Sharpe",
        "Max Drawdown",
        "Excess CAGR vs Benchmark",
        "Excess CAGR vs Secondary Benchmark",
        "Secondary Benchmark Max Drawdown",
        "Turnover/Year",
    ):
        windows[column] = pd.to_numeric(windows.get(column), errors="coerce")
    windows["Beats SPY CAGR"] = windows["Excess CAGR vs Benchmark"].gt(0.0)
    windows["Beats QQQ CAGR"] = windows["Excess CAGR vs Secondary Benchmark"].gt(0.0)
    windows["QQQ Drawdown Advantage"] = windows["Max Drawdown"] >= (
        windows["Secondary Benchmark Max Drawdown"] + DRAWNDOWN_ADVANTAGE_VS_QQQ
    )
    windows["QQQ Competitive"] = windows["Beats QQQ CAGR"] | windows["QQQ Drawdown Advantage"]

    summary_rows: list[dict[str, object]] = []
    for candidate_id, frame in windows.groupby("Candidate", sort=False):
        for window_type, subset in frame.groupby("Window Type", sort=False):
            count = int(len(subset))
            summary_rows.append(
                {
                    "Candidate": candidate_id,
                    "Window Type": window_type,
                    "Window Count": count,
                    "SPY CAGR Win Rate": float(subset["Beats SPY CAGR"].mean()) if count else float("nan"),
                    "QQQ CAGR Win Rate": float(subset["Beats QQQ CAGR"].mean()) if count else float("nan"),
                    "QQQ Competitive Rate": float(subset["QQQ Competitive"].mean()) if count else float("nan"),
                    "Median Excess CAGR vs SPY": float(subset["Excess CAGR vs Benchmark"].median()),
                    "Worst Excess CAGR vs SPY": float(subset["Excess CAGR vs Benchmark"].min()),
                    "Median Excess CAGR vs QQQ": float(subset["Excess CAGR vs Secondary Benchmark"].median()),
                    "Worst Excess CAGR vs QQQ": float(subset["Excess CAGR vs Secondary Benchmark"].min()),
                    "Worst Drawdown": float(subset["Max Drawdown"].min()),
                    "Median Sharpe": float(subset["Sharpe"].median()),
                    "Median Turnover/Year": float(subset["Turnover/Year"].median()),
                }
            )
    return {
        "robustness_windows": windows.sort_values(["Candidate", "Window Type", "Start"]).reset_index(drop=True),
        "robustness_summary": pd.DataFrame(summary_rows),
    }


def _rolling_dollar_volume(price_history: pd.DataFrame, *, window: int) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        return pd.DataFrame()
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close", "volume"])
    if frame.empty:
        return pd.DataFrame()
    frame["dollar_volume"] = frame["close"] * frame["volume"]
    dollar_volume = frame.pivot_table(index="as_of", columns="symbol", values="dollar_volume", aggfunc="last")
    dollar_volume = dollar_volume.sort_index()
    dollar_volume.columns = dollar_volume.columns.map(str).str.upper()
    return dollar_volume.rolling(int(window), min_periods=1).median()


def _risk_asset_columns(columns: Sequence[str]) -> list[str]:
    return [str(column).strip().upper() for column in columns if str(column).strip().upper() not in SAFE_LIKE_SYMBOLS]


def build_candidate_liquidity_diagnostics(
    *,
    price_history: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame],
    candidate_ids: Sequence[str],
    dollar_volume_window: int = DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW,
    low_liquidity_dollar_volume: float = DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME,
) -> dict[str, pd.DataFrame]:
    rolling_dollar_volume = _rolling_dollar_volume(price_history, window=int(dollar_volume_window))
    if rolling_dollar_volume.empty:
        return {"liquidity_summary": pd.DataFrame(), "liquidity_symbol_summary": pd.DataFrame()}

    summary_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    threshold = float(low_liquidity_dollar_volume)
    for candidate_id in tuple(dict.fromkeys(str(item).strip() for item in candidate_ids if str(item).strip())):
        if candidate_id not in weights_by_candidate:
            continue
        weights = pd.DataFrame(weights_by_candidate[candidate_id]).copy()
        if weights.empty:
            continue
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights.columns = [str(column).strip().upper() for column in weights.columns]
        weights = weights.apply(pd.to_numeric, errors="coerce").fillna(0.0)
        common_index = weights.index.intersection(rolling_dollar_volume.index)
        common_columns = [column for column in weights.columns if column in rolling_dollar_volume.columns]
        if len(common_index) == 0 or not common_columns:
            continue
        weights = weights.reindex(index=common_index, columns=common_columns, fill_value=0.0)
        dollar_volume = rolling_dollar_volume.reindex(index=common_index, columns=common_columns)
        held_mask = weights.gt(1e-9)
        weighted_dollar_volume = weights.mul(dollar_volume, axis=0).where(held_mask).sum(axis=1, min_count=1)
        held_dollar_volume = dollar_volume.where(held_mask)
        min_held_dollar_volume = held_dollar_volume.min(axis=1, skipna=True)
        low_liquidity_weight = weights.where(dollar_volume.lt(threshold), 0.0).sum(axis=1)
        risk_columns = _risk_asset_columns(common_columns)
        risk_low_liquidity_weight = (
            weights[risk_columns].where(dollar_volume[risk_columns].lt(threshold), 0.0).sum(axis=1)
            if risk_columns
            else pd.Series(0.0, index=weights.index)
        )
        risk_min_held_dollar_volume = (
            dollar_volume[risk_columns].where(weights[risk_columns].gt(1e-9)).min(axis=1, skipna=True)
            if risk_columns
            else pd.Series(float("nan"), index=weights.index)
        )
        max_position_weight = weights.max(axis=1)
        held_symbol_count = held_mask.sum(axis=1)

        summary_rows.append(
            {
                "Candidate": candidate_id,
                "Trading Days": int(len(common_index)),
                "Dollar Volume Window": int(dollar_volume_window),
                "Low Liquidity Dollar Volume Threshold": threshold,
                "Median Weighted Dollar Volume": float(weighted_dollar_volume.median()),
                "Worst Held Dollar Volume": float(min_held_dollar_volume.min()),
                "Median Low Liquidity Weight": float(low_liquidity_weight.median()),
                "Max Low Liquidity Weight": float(low_liquidity_weight.max()),
                "Worst Risk Held Dollar Volume": float(risk_min_held_dollar_volume.min()),
                "Median Risk Low Liquidity Weight": float(risk_low_liquidity_weight.median()),
                "Max Risk Low Liquidity Weight": float(risk_low_liquidity_weight.max()),
                "Median Max Position Weight": float(max_position_weight.median()),
                "Max Position Weight": float(max_position_weight.max()),
                "Median Held Symbol Count": float(held_symbol_count.median()),
            }
        )

        for symbol in common_columns:
            symbol_weight = weights[symbol]
            symbol_held = symbol_weight.gt(1e-9)
            if not bool(symbol_held.any()):
                continue
            symbol_dollar_volume = dollar_volume[symbol].where(symbol_held)
            symbol_rows.append(
                {
                    "Candidate": candidate_id,
                    "Symbol": symbol,
                    "Safe Like": symbol in SAFE_LIKE_SYMBOLS,
                    "Held Days": int(symbol_held.sum()),
                    "Held Day Rate": float(symbol_held.mean()),
                    "Average Weight When Held": float(symbol_weight.loc[symbol_held].mean()),
                    "Max Weight": float(symbol_weight.max()),
                    "Median Dollar Volume": float(symbol_dollar_volume.median()),
                    "Worst Dollar Volume": float(symbol_dollar_volume.min()),
                    "Low Liquidity Day Rate": float(symbol_dollar_volume.lt(threshold).sum() / int(symbol_held.sum())),
                }
            )

    return {
        "liquidity_summary": pd.DataFrame(summary_rows),
        "liquidity_symbol_summary": pd.DataFrame(symbol_rows),
    }


def _safe_positive(value: float, *, default: float = 0.0) -> float:
    numeric = float(value)
    return numeric if math.isfinite(numeric) and numeric > 0.0 else float(default)


def _dynamic_low_liquidity_penalty_bps(
    dollar_volume: pd.DataFrame,
    *,
    threshold: float,
    penalty_bps: float,
    max_penalty_bps: float,
) -> pd.DataFrame:
    if dollar_volume.empty:
        return pd.DataFrame(index=dollar_volume.index, columns=dollar_volume.columns, dtype=float)
    safe_threshold = _safe_positive(threshold)
    if safe_threshold <= 0.0 or float(penalty_bps) <= 0.0 or float(max_penalty_bps) <= 0.0:
        return pd.DataFrame(0.0, index=dollar_volume.index, columns=dollar_volume.columns)
    volume = pd.DataFrame(dollar_volume).apply(pd.to_numeric, errors="coerce")
    ratio = safe_threshold / volume.where(volume.gt(0.0))
    penalty = float(penalty_bps) * (np.sqrt(ratio) - 1.0)
    return penalty.clip(lower=0.0, upper=float(max_penalty_bps)).fillna(float(max_penalty_bps))


def _dynamic_participation_penalty_bps(
    trade_weight_delta: pd.DataFrame,
    dollar_volume: pd.DataFrame,
    *,
    estimated_portfolio_nav: float,
    participation_rate_threshold: float,
    penalty_bps: float,
    max_penalty_bps: float,
) -> pd.DataFrame:
    nav = _safe_positive(estimated_portfolio_nav)
    threshold = _safe_positive(participation_rate_threshold)
    if nav <= 0.0 or threshold <= 0.0 or float(penalty_bps) <= 0.0 or float(max_penalty_bps) <= 0.0:
        return pd.DataFrame(0.0, index=trade_weight_delta.index, columns=trade_weight_delta.columns)
    volume = pd.DataFrame(dollar_volume).apply(pd.to_numeric, errors="coerce")
    trade_dollars = pd.DataFrame(trade_weight_delta).abs().mul(nav)
    participation = trade_dollars / volume.where(volume.gt(0.0))
    penalty = float(penalty_bps) * (np.sqrt(participation / threshold) - 1.0)
    return penalty.clip(lower=0.0, upper=float(max_penalty_bps)).fillna(float(max_penalty_bps))


def _dynamic_cost_for_weights(
    *,
    weights: pd.DataFrame,
    asset_returns: pd.DataFrame,
    rolling_dollar_volume: pd.DataFrame,
    config: DynamicExecutionCostConfig,
) -> dict[str, object]:
    prepared_weights = pd.DataFrame(weights).copy()
    if prepared_weights.empty:
        return {"returns": pd.Series(dtype=float), "costs": pd.Series(dtype=float), "summary": {}}
    prepared_weights.index = pd.to_datetime(prepared_weights.index).tz_localize(None).normalize()
    prepared_weights.columns = [str(column).strip().upper() for column in prepared_weights.columns]
    prepared_weights = prepared_weights.apply(pd.to_numeric, errors="coerce").fillna(0.0)

    common_index = prepared_weights.index.intersection(asset_returns.index).intersection(rolling_dollar_volume.index)
    common_columns = [column for column in prepared_weights.columns if column in asset_returns.columns]
    if len(common_index) == 0 or not common_columns:
        return {"returns": pd.Series(dtype=float), "costs": pd.Series(dtype=float), "summary": {}}

    prepared_weights = prepared_weights.reindex(index=common_index, columns=common_columns, fill_value=0.0)
    returns = asset_returns.reindex(index=common_index, columns=common_columns).fillna(0.0)
    dollar_volume = rolling_dollar_volume.reindex(index=common_index, columns=common_columns)
    gross_returns = prepared_weights.mul(returns, axis=0).sum(axis=1)
    trade_delta = prepared_weights.diff().abs().fillna(0.0)
    turnover = 0.5 * trade_delta.sum(axis=1)

    low_liquidity_penalty = _dynamic_low_liquidity_penalty_bps(
        dollar_volume,
        threshold=float(config.low_liquidity_dollar_volume),
        penalty_bps=float(config.low_liquidity_penalty_bps),
        max_penalty_bps=float(config.max_low_liquidity_penalty_bps),
    )
    participation_penalty = _dynamic_participation_penalty_bps(
        trade_delta,
        dollar_volume,
        estimated_portfolio_nav=float(config.estimated_portfolio_nav),
        participation_rate_threshold=float(config.participation_rate_threshold),
        penalty_bps=float(config.participation_penalty_bps),
        max_penalty_bps=float(config.max_participation_penalty_bps),
    )
    effective_cost_bps = (
        float(config.base_cost_bps)
        + low_liquidity_penalty.reindex_like(trade_delta).fillna(float(config.max_low_liquidity_penalty_bps))
        + participation_penalty.reindex_like(trade_delta).fillna(0.0)
    )
    daily_cost = 0.5 * trade_delta.mul(effective_cost_bps / 10_000.0, axis=0).sum(axis=1)
    adjusted_returns = gross_returns - daily_cost
    effective_daily_bps = daily_cost.div(turnover.where(turnover.gt(0.0))) * 10_000.0
    years = max((common_index[-1] - common_index[0]).days / 365.25, 1 / 365.25)
    traded_mask = trade_delta.gt(1e-12)
    low_liquidity_trade_weight = trade_delta.where(
        dollar_volume.lt(float(config.low_liquidity_dollar_volume)), 0.0
    ).sum(axis=1)
    participation_rate = (
        trade_delta.mul(float(config.estimated_portfolio_nav)).div(dollar_volume.where(dollar_volume.gt(0.0)))
        if float(config.estimated_portfolio_nav) > 0.0
        else pd.DataFrame(0.0, index=trade_delta.index, columns=trade_delta.columns)
    )
    summary = {
        "Trading Days": int(len(common_index)),
        "Trade Days": int(turnover.gt(1e-12).sum()),
        "Estimated Portfolio NAV": float(config.estimated_portfolio_nav),
        "Base Cost Bps": float(config.base_cost_bps),
        "Dollar Volume Window": int(config.dollar_volume_window),
        "Low Liquidity Dollar Volume Threshold": float(config.low_liquidity_dollar_volume),
        "Low Liquidity Penalty Bps": float(config.low_liquidity_penalty_bps),
        "Max Low Liquidity Penalty Bps": float(config.max_low_liquidity_penalty_bps),
        "Participation Rate Threshold": float(config.participation_rate_threshold),
        "Participation Penalty Bps": float(config.participation_penalty_bps),
        "Max Participation Penalty Bps": float(config.max_participation_penalty_bps),
        "Turnover/Year": float(turnover.sum() / years),
        "Annualized Cost Drag": float(daily_cost.sum() / years),
        "Total Cost Drag": float(daily_cost.sum()),
        "Median Effective Cost Bps On Trade Days": float(effective_daily_bps.dropna().median()),
        "Max Effective Cost Bps On Trade Days": float(effective_daily_bps.dropna().max()),
        "Max Daily Cost Drag": float(daily_cost.max()),
        "Median Low Liquidity Trade Weight": float(low_liquidity_trade_weight.where(turnover.gt(1e-12)).median()),
        "Max Low Liquidity Trade Weight": float(low_liquidity_trade_weight.max()),
        "Max Participation Rate": float(participation_rate.where(traded_mask).max().max()),
    }
    return {"returns": adjusted_returns, "costs": daily_cost, "summary": summary}


def build_dynamic_execution_cost_adjusted_returns(
    *,
    price_history: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame],
    candidate_ids: Sequence[str],
    config: DynamicExecutionCostConfig,
) -> dict[str, pd.DataFrame]:
    close = _normalize_price_history(price_history)
    asset_returns = close.pct_change(fill_method=None).fillna(0.0)
    rolling_dollar_volume = _rolling_dollar_volume(price_history, window=int(config.dollar_volume_window))
    if rolling_dollar_volume.empty:
        return {
            "portfolio_returns": pd.DataFrame(),
            "daily_costs": pd.DataFrame(),
            "dynamic_cost_summary": pd.DataFrame(),
        }

    returns_by_candidate: dict[str, pd.Series] = {}
    costs_by_candidate: dict[str, pd.Series] = {}
    summary_rows: list[dict[str, object]] = []
    for candidate_id in tuple(dict.fromkeys(str(item).strip() for item in candidate_ids if str(item).strip())):
        if candidate_id not in weights_by_candidate:
            continue
        result = _dynamic_cost_for_weights(
            weights=pd.DataFrame(weights_by_candidate[candidate_id]),
            asset_returns=asset_returns,
            rolling_dollar_volume=rolling_dollar_volume,
            config=config,
        )
        returns = pd.Series(result["returns"], dtype=float)
        if returns.empty:
            continue
        returns.name = candidate_id
        returns_by_candidate[candidate_id] = returns
        costs = pd.Series(result["costs"], dtype=float)
        costs.name = candidate_id
        costs_by_candidate[candidate_id] = costs
        summary_rows.append({"Candidate": candidate_id, **dict(result["summary"])})

    return {
        "portfolio_returns": pd.concat(returns_by_candidate.values(), axis=1)
        if returns_by_candidate
        else pd.DataFrame(),
        "daily_costs": pd.concat(costs_by_candidate.values(), axis=1) if costs_by_candidate else pd.DataFrame(),
        "dynamic_cost_summary": pd.DataFrame(summary_rows),
    }


def _metadata_from_period_summary(period_summary: pd.DataFrame) -> dict[str, dict[str, object]]:
    metadata_columns = (
        "Candidate",
        "Display Name",
        "Candidate Group",
        "Rule",
        "Primary Benchmark Symbol",
        "Secondary Benchmark Symbol",
        "Safe Haven",
        "Top N",
        "Rebalance Months",
        "SMA Period",
        "Canary Bad Threshold",
        "Score Mode",
        "Canary Mode",
        "Safe Fraction Per Bad Canary",
        "Confidence Weighting",
        "Confidence Volatility Gate",
        "Ranking Pool Size",
        "Ranking Pool",
        "Notes",
    )
    frame = pd.DataFrame(period_summary).copy()
    metadata: dict[str, dict[str, object]] = {}
    if frame.empty:
        return metadata
    for candidate_id, subset in frame.groupby("Candidate", sort=False):
        row = subset.iloc[0]
        metadata[str(candidate_id)] = {column: row.get(column) for column in metadata_columns if column in row.index}
    return metadata


def build_period_summary_from_portfolio_returns(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]],
    portfolio_returns: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame],
    source_period_summary: pd.DataFrame,
) -> pd.DataFrame:
    close = _normalize_price_history(price_history)
    asset_returns = close.pct_change(fill_method=None).fillna(0.0)
    benchmark_returns = asset_returns.get(DEFAULT_PRIMARY_BENCHMARK, pd.Series(index=asset_returns.index, dtype=float))
    secondary_benchmark_returns = asset_returns.get(
        DEFAULT_SECONDARY_BENCHMARK, pd.Series(index=asset_returns.index, dtype=float)
    )
    metadata = _metadata_from_period_summary(source_period_summary)
    rows: list[dict[str, object]] = []
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if portfolio.empty:
        return pd.DataFrame()
    portfolio.index = pd.to_datetime(portfolio.index).tz_localize(None).normalize()
    for candidate_id in portfolio.columns:
        candidate_id = str(candidate_id)
        candidate_returns = pd.to_numeric(portfolio[candidate_id], errors="coerce").dropna()
        if candidate_returns.empty:
            continue
        weights = pd.DataFrame(weights_by_candidate.get(candidate_id, pd.DataFrame())).copy()
        if not weights.empty:
            weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        for period_name, start_date, end_date in periods:
            start_ts = pd.Timestamp(start_date).normalize()
            end_ts = pd.Timestamp(end_date).normalize() if end_date else candidate_returns.index.max()
            period_returns = candidate_returns.loc[
                (candidate_returns.index >= start_ts) & (candidate_returns.index <= end_ts)
            ]
            if period_returns.empty:
                continue
            period_weights = weights.loc[start_ts:end_ts] if not weights.empty else pd.DataFrame()
            summary = summarize_returns(
                period_returns,
                weights_history=period_weights,
                benchmark_returns=benchmark_returns,
                secondary_benchmark_returns=secondary_benchmark_returns,
                start_date=start_date,
                end_date=end_date,
            )
            summary.update(metadata.get(candidate_id, {"Candidate": candidate_id}))
            rows.append({"Period": period_name, **summary})
    return pd.DataFrame(rows)


def _candidate_period_vs_baseline_summary(
    *,
    candidate_id: str,
    baseline_candidate: str,
    portfolio_returns: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
    min_trading_days: int,
) -> dict[str, object] | None:
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if candidate_id not in portfolio.columns or baseline_candidate not in portfolio.columns:
        return None
    portfolio.index = pd.to_datetime(portfolio.index).tz_localize(None).normalize()
    candidate_returns = pd.to_numeric(portfolio[candidate_id], errors="coerce").loc[start:end].dropna()
    baseline_returns = pd.to_numeric(portfolio[baseline_candidate], errors="coerce").loc[start:end].dropna()
    common_index = candidate_returns.index.intersection(baseline_returns.index)
    if len(common_index) < int(min_trading_days):
        return None
    candidate_returns = candidate_returns.reindex(common_index)
    baseline_returns = baseline_returns.reindex(common_index)
    weights = pd.DataFrame(weights_by_candidate.get(candidate_id, pd.DataFrame())).copy()
    if not weights.empty:
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights = weights.loc[start:end]
    summary = summarize_returns(
        candidate_returns,
        weights_history=weights,
        benchmark_returns=baseline_returns,
        start_date=start.date().isoformat(),
        end_date=end.date().isoformat(),
    )
    baseline_summary = _benchmark_summary(baseline_returns, pd.DatetimeIndex(common_index))
    return {
        "Candidate": candidate_id,
        "Start": summary["Start"],
        "End": summary["End"],
        "Trading Days": summary["Trading Days"],
        "CAGR": float(summary["CAGR"]),
        "Baseline CAGR": float(baseline_summary["cagr"]),
        "Excess CAGR vs Baseline": float(summary["CAGR"]) - float(baseline_summary["cagr"]),
        "Max Drawdown": float(summary["Max Drawdown"]),
        "Baseline Max Drawdown": float(baseline_summary["max_drawdown"]),
        "Drawdown Delta vs Baseline": float(summary["Max Drawdown"]) - float(baseline_summary["max_drawdown"]),
        "Turnover/Year": float(summary["Turnover/Year"]),
    }


def _walk_forward_gate_reason(
    *,
    window_count_gate: bool,
    win_rate_gate: bool,
    median_excess_gate: bool,
    worst_excess_gate: bool,
    worst_drawdown_gate: bool,
) -> str:
    reasons: list[str] = []
    if not window_count_gate:
        reasons.append("not_enough_oos_windows")
    if not win_rate_gate:
        reasons.append("oos_win_rate_below_50pct")
    if not median_excess_gate:
        reasons.append("median_oos_excess_not_positive")
    if not worst_excess_gate:
        reasons.append("worst_oos_excess_too_low")
    if not worst_drawdown_gate:
        reasons.append("worst_oos_drawdown_worse_than_baseline")
    return "pass" if not reasons else ";".join(reasons)


def build_walk_forward_selection_diagnostics(
    *,
    portfolio_returns: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame],
    candidate_ids: Sequence[str] = DEFAULT_WALK_FORWARD_CANDIDATES,
    baseline_candidate: str = DEFAULT_LIVE_BASELINE_CANDIDATE,
    train_years: int = DEFAULT_WALK_FORWARD_TRAIN_YEARS,
    min_train_days_per_year: int = DEFAULT_WALK_FORWARD_MIN_TRAIN_DAYS_PER_YEAR,
    min_test_days: int = DEFAULT_WALK_FORWARD_MIN_TEST_DAYS,
    min_train_excess_cagr_vs_baseline: float = DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR,
) -> dict[str, pd.DataFrame]:
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if portfolio.empty or baseline_candidate not in portfolio.columns:
        return {"walk_forward_windows": pd.DataFrame(), "walk_forward_summary": pd.DataFrame()}
    portfolio.index = pd.to_datetime(portfolio.index).tz_localize(None).normalize()
    portfolio = portfolio.sort_index()
    candidate_list = tuple(
        dict.fromkeys(
            str(candidate_id).strip()
            for candidate_id in candidate_ids
            if str(candidate_id).strip() and str(candidate_id).strip() in portfolio.columns
        )
    )
    if not candidate_list:
        return {"walk_forward_windows": pd.DataFrame(), "walk_forward_summary": pd.DataFrame()}

    min_year = int(portfolio.index.min().year)
    max_year = int(portfolio.index.max().year)
    rows: list[dict[str, object]] = []
    min_train_days = int(train_years) * int(min_train_days_per_year)
    for test_year in range(min_year + int(train_years), max_year + 1):
        train_start = pd.Timestamp(year=test_year - int(train_years), month=1, day=1)
        train_end = pd.Timestamp(year=test_year - 1, month=12, day=31)
        test_start = pd.Timestamp(year=test_year, month=1, day=1)
        test_end = pd.Timestamp(year=test_year, month=12, day=31)
        train_rows: list[dict[str, object]] = []
        for candidate_id in candidate_list:
            train_summary = _candidate_period_vs_baseline_summary(
                candidate_id=candidate_id,
                baseline_candidate=baseline_candidate,
                portfolio_returns=portfolio,
                weights_by_candidate=weights_by_candidate,
                start=train_start,
                end=train_end,
                min_trading_days=min_train_days,
            )
            if train_summary is not None:
                train_rows.append(train_summary)
        if not train_rows:
            continue
        train_frame = pd.DataFrame(train_rows)
        for column in ("Excess CAGR vs Baseline", "Drawdown Delta vs Baseline", "Turnover/Year"):
            train_frame[column] = pd.to_numeric(train_frame[column], errors="coerce")
        train_frame = train_frame.dropna(subset=["Excess CAGR vs Baseline"])
        if train_frame.empty:
            continue
        min_train_excess = float(min_train_excess_cagr_vs_baseline)
        excess_gate = (
            train_frame["Excess CAGR vs Baseline"].ge(min_train_excess)
            if min_train_excess > 0.0
            else train_frame["Excess CAGR vs Baseline"].gt(0.0)
        )
        eligible = train_frame.loc[
            excess_gate
            & train_frame["Drawdown Delta vs Baseline"].ge(-LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE)
        ].copy()
        if eligible.empty:
            rows.append(
                {
                    "Train Window": f"{train_start.date().isoformat()}_{train_end.date().isoformat()}",
                    "Test Window": str(test_year),
                    "Selected Candidate": baseline_candidate,
                    "Selection Action": "keep_baseline",
                    "Train Candidate Count": int(len(train_frame)),
                    "Min Train Excess CAGR vs Baseline": min_train_excess,
                    "Train Selected Excess CAGR vs Baseline": 0.0,
                    "Train Selected Drawdown Delta vs Baseline": 0.0,
                    "Train Selected Turnover/Year": 0.0,
                    "Test Trading Days": 0,
                    "Test CAGR": float("nan"),
                    "Test Baseline CAGR": float("nan"),
                    "Test Excess CAGR vs Baseline": 0.0,
                    "Test Drawdown Delta vs Baseline": 0.0,
                    "Test Turnover/Year": 0.0,
                }
            )
            continue
        selected = (
            eligible.sort_values(
                ["Excess CAGR vs Baseline", "Drawdown Delta vs Baseline", "Turnover/Year"],
                ascending=[False, False, True],
            )
            .iloc[0]
            .copy()
        )
        selected_candidate = str(selected["Candidate"])
        test_summary = _candidate_period_vs_baseline_summary(
            candidate_id=selected_candidate,
            baseline_candidate=baseline_candidate,
            portfolio_returns=portfolio,
            weights_by_candidate=weights_by_candidate,
            start=test_start,
            end=test_end,
            min_trading_days=min_test_days,
        )
        if test_summary is None:
            continue
        rows.append(
            {
                "Train Window": f"{train_start.date().isoformat()}_{train_end.date().isoformat()}",
                "Test Window": str(test_year),
                "Selected Candidate": selected_candidate,
                "Selection Action": "promote_candidate",
                "Train Candidate Count": int(len(train_frame)),
                "Min Train Excess CAGR vs Baseline": min_train_excess,
                "Train Selected Excess CAGR vs Baseline": float(selected["Excess CAGR vs Baseline"]),
                "Train Selected Drawdown Delta vs Baseline": float(selected["Drawdown Delta vs Baseline"]),
                "Train Selected Turnover/Year": float(selected["Turnover/Year"]),
                "Test Trading Days": int(test_summary["Trading Days"]),
                "Test CAGR": float(test_summary["CAGR"]),
                "Test Baseline CAGR": float(test_summary["Baseline CAGR"]),
                "Test Excess CAGR vs Baseline": float(test_summary["Excess CAGR vs Baseline"]),
                "Test Drawdown Delta vs Baseline": float(test_summary["Drawdown Delta vs Baseline"]),
                "Test Turnover/Year": float(test_summary["Turnover/Year"]),
            }
        )
    windows = pd.DataFrame(rows)
    if windows.empty:
        return {"walk_forward_windows": windows, "walk_forward_summary": pd.DataFrame()}
    for column in (
        "Train Selected Excess CAGR vs Baseline",
        "Test Excess CAGR vs Baseline",
        "Test Drawdown Delta vs Baseline",
        "Test Turnover/Year",
    ):
        windows[column] = pd.to_numeric(windows[column], errors="coerce")
    promoted = windows.loc[windows["Selection Action"].eq("promote_candidate")].copy()
    selected_counts = promoted["Selected Candidate"].value_counts().sort_index().to_dict() if not promoted.empty else {}
    test_excess = promoted["Test Excess CAGR vs Baseline"] if not promoted.empty else pd.Series(dtype=float)
    test_drawdown_delta = promoted["Test Drawdown Delta vs Baseline"] if not promoted.empty else pd.Series(dtype=float)
    window_count = int(len(promoted))
    win_rate = float(test_excess.gt(0.0).mean()) if window_count else float("nan")
    median_excess = float(test_excess.median()) if window_count else float("nan")
    worst_excess = float(test_excess.min()) if window_count else float("nan")
    worst_drawdown_delta = float(test_drawdown_delta.min()) if window_count else float("nan")
    window_count_gate = window_count >= WALK_FORWARD_MIN_WINDOW_COUNT
    win_rate_gate = not pd.isna(win_rate) and win_rate >= WALK_FORWARD_MIN_OOS_WIN_RATE
    median_excess_gate = not pd.isna(median_excess) and median_excess > WALK_FORWARD_MIN_OOS_MEDIAN_EXCESS_CAGR
    worst_excess_gate = not pd.isna(worst_excess) and worst_excess >= WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR
    worst_drawdown_gate = (
        not pd.isna(worst_drawdown_delta) and worst_drawdown_delta >= -WALK_FORWARD_MAX_WORST_OOS_DRAWDOWN_DEGRADATION
    )
    summary = pd.DataFrame(
        [
            {
                "Baseline Candidate": baseline_candidate,
                "Candidate Set": ",".join(candidate_list),
                "Train Years": int(train_years),
                "Min Train Excess CAGR vs Baseline": float(min_train_excess_cagr_vs_baseline),
                "Total OOS Windows": int(len(windows)),
                "Promotion OOS Windows": window_count,
                "Keep Baseline Windows": int(windows["Selection Action"].eq("keep_baseline").sum()),
                "OOS Baseline CAGR Win Rate": win_rate,
                "Median OOS Excess CAGR vs Baseline": median_excess,
                "Worst OOS Excess CAGR vs Baseline": worst_excess,
                "Worst OOS Drawdown Delta vs Baseline": worst_drawdown_delta,
                "Median OOS Turnover/Year": float(promoted["Test Turnover/Year"].median())
                if window_count
                else float("nan"),
                "Selected Candidate Counts": json.dumps(selected_counts, sort_keys=True),
                "walk_forward_gate_passed": bool(
                    window_count_gate
                    and win_rate_gate
                    and median_excess_gate
                    and worst_excess_gate
                    and worst_drawdown_gate
                ),
                "walk_forward_gate_reason": _walk_forward_gate_reason(
                    window_count_gate=window_count_gate,
                    win_rate_gate=win_rate_gate,
                    median_excess_gate=median_excess_gate,
                    worst_excess_gate=worst_excess_gate,
                    worst_drawdown_gate=worst_drawdown_gate,
                ),
            }
        ]
    )
    return {
        "walk_forward_windows": windows.reset_index(drop=True),
        "walk_forward_summary": summary,
    }


def _numeric_value(row: pd.Series, column: str) -> float:
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else float("nan")


def _live_readiness_gate_reason(
    *,
    research_gate_passed: bool,
    long_cagr_gate: bool,
    long_drawdown_gate: bool,
    turnover_gate: bool,
    calendar_win_gate: bool,
    rolling_3y_win_gate: bool,
    rolling_5y_win_gate: bool,
    worst_rolling_excess_gate: bool,
    worst_window_drawdown_gate: bool,
) -> str:
    reasons: list[str] = []
    if not research_gate_passed:
        reasons.append("research_gate_not_passed")
    if not long_cagr_gate:
        reasons.append("long_cagr_not_above_baseline")
    if not long_drawdown_gate:
        reasons.append("long_drawdown_worse_than_baseline")
    if not turnover_gate:
        reasons.append("turnover_increase_too_high")
    if not calendar_win_gate:
        reasons.append("calendar_baseline_win_rate_below_50pct")
    if not rolling_3y_win_gate:
        reasons.append("rolling_3y_baseline_win_rate_below_50pct")
    if not rolling_5y_win_gate:
        reasons.append("rolling_5y_baseline_win_rate_below_60pct")
    if not worst_rolling_excess_gate:
        reasons.append("worst_rolling_excess_vs_baseline_too_low")
    if not worst_window_drawdown_gate:
        reasons.append("worst_window_drawdown_worse_than_baseline")
    return "pass" if not reasons else ";".join(reasons)


def _baseline_relative_window_metrics(
    windows: pd.DataFrame,
    *,
    candidate_id: str,
    baseline_candidate: str,
    window_type: str,
) -> dict[str, float | int]:
    if windows.empty:
        return {
            "count": 0,
            "baseline_cagr_win_rate": float("nan"),
            "median_excess_cagr_vs_baseline": float("nan"),
            "worst_excess_cagr_vs_baseline": float("nan"),
            "worst_drawdown_delta_vs_baseline": float("nan"),
            "median_turnover_delta_vs_baseline": float("nan"),
        }
    typed = windows.loc[windows["Window Type"].eq(window_type)].copy()
    candidate = typed.loc[typed["Candidate"].eq(candidate_id)].copy()
    baseline = typed.loc[typed["Candidate"].eq(baseline_candidate)].copy()
    if candidate.empty or baseline.empty:
        return {
            "count": 0,
            "baseline_cagr_win_rate": float("nan"),
            "median_excess_cagr_vs_baseline": float("nan"),
            "worst_excess_cagr_vs_baseline": float("nan"),
            "worst_drawdown_delta_vs_baseline": float("nan"),
            "median_turnover_delta_vs_baseline": float("nan"),
        }
    columns = ["Window", "CAGR", "Max Drawdown", "Turnover/Year"]
    merged = candidate[columns].merge(baseline[columns], on="Window", suffixes=("_candidate", "_baseline"))
    if merged.empty:
        return {
            "count": 0,
            "baseline_cagr_win_rate": float("nan"),
            "median_excess_cagr_vs_baseline": float("nan"),
            "worst_excess_cagr_vs_baseline": float("nan"),
            "worst_drawdown_delta_vs_baseline": float("nan"),
            "median_turnover_delta_vs_baseline": float("nan"),
        }
    for column in (
        "CAGR_candidate",
        "CAGR_baseline",
        "Max Drawdown_candidate",
        "Max Drawdown_baseline",
        "Turnover/Year_candidate",
        "Turnover/Year_baseline",
    ):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    excess_cagr = merged["CAGR_candidate"] - merged["CAGR_baseline"]
    drawdown_delta = merged["Max Drawdown_candidate"] - merged["Max Drawdown_baseline"]
    turnover_delta = merged["Turnover/Year_candidate"] - merged["Turnover/Year_baseline"]
    return {
        "count": int(len(merged)),
        "baseline_cagr_win_rate": float(excess_cagr.gt(0.0).mean()),
        "median_excess_cagr_vs_baseline": float(excess_cagr.median()),
        "worst_excess_cagr_vs_baseline": float(excess_cagr.min()),
        "worst_drawdown_delta_vs_baseline": float(drawdown_delta.min()),
        "median_turnover_delta_vs_baseline": float(turnover_delta.median()),
    }


def build_live_readiness_summary(
    *,
    period_summary: pd.DataFrame,
    ranking: pd.DataFrame,
    robustness_windows: pd.DataFrame,
    baseline_candidate: str = DEFAULT_LIVE_BASELINE_CANDIDATE,
) -> pd.DataFrame:
    periods = pd.DataFrame(period_summary).copy()
    rank = pd.DataFrame(ranking).copy()
    windows = pd.DataFrame(robustness_windows).copy()
    if periods.empty or rank.empty:
        return pd.DataFrame()

    long_rows = periods.loc[periods["Period"].eq("long")].copy()
    baseline_long = long_rows.loc[long_rows["Candidate"].eq(baseline_candidate)]
    baseline_rank = rank.loc[rank["Candidate"].eq(baseline_candidate)]
    if baseline_long.empty or baseline_rank.empty:
        return pd.DataFrame()

    baseline_long_row = baseline_long.iloc[0]
    baseline_rank_row = baseline_rank.iloc[0]
    baseline_long_cagr = _numeric_value(baseline_long_row, "CAGR")
    baseline_long_drawdown = _numeric_value(baseline_long_row, "Max Drawdown")
    baseline_median_turnover = _numeric_value(baseline_rank_row, "median_turnover_per_year")

    candidates = rank.loc[rank["Candidate Group"].eq("liveable_candidate")].copy()
    if candidates.empty:
        return pd.DataFrame()

    for column in ("CAGR", "Max Drawdown", "Turnover/Year"):
        if column in windows.columns:
            windows[column] = pd.to_numeric(windows[column], errors="coerce")

    rows: list[dict[str, object]] = []
    for _idx, candidate_rank in candidates.iterrows():
        candidate_id = str(candidate_rank.get("Candidate"))
        candidate_long = long_rows.loc[long_rows["Candidate"].eq(candidate_id)]
        if candidate_long.empty:
            continue
        candidate_long_row = candidate_long.iloc[0]
        candidate_long_cagr = _numeric_value(candidate_long_row, "CAGR")
        candidate_long_drawdown = _numeric_value(candidate_long_row, "Max Drawdown")
        candidate_median_turnover = _numeric_value(candidate_rank, "median_turnover_per_year")
        long_excess_cagr = candidate_long_cagr - baseline_long_cagr
        long_drawdown_delta = candidate_long_drawdown - baseline_long_drawdown
        median_turnover_delta = candidate_median_turnover - baseline_median_turnover

        window_metrics = {
            window_type: _baseline_relative_window_metrics(
                windows,
                candidate_id=candidate_id,
                baseline_candidate=baseline_candidate,
                window_type=window_type,
            )
            for window_type in ("calendar_year", "rolling_3y", "rolling_5y")
        }
        rolling_worst_excess = (
            min(
                float(window_metrics[window_type]["worst_excess_cagr_vs_baseline"])
                for window_type in ("rolling_3y", "rolling_5y")
                if not pd.isna(window_metrics[window_type]["worst_excess_cagr_vs_baseline"])
            )
            if any(
                not pd.isna(window_metrics[window_type]["worst_excess_cagr_vs_baseline"])
                for window_type in ("rolling_3y", "rolling_5y")
            )
            else float("nan")
        )
        worst_drawdown_delta = (
            min(
                float(window_metrics[window_type]["worst_drawdown_delta_vs_baseline"])
                for window_type in window_metrics
                if not pd.isna(window_metrics[window_type]["worst_drawdown_delta_vs_baseline"])
            )
            if any(
                not pd.isna(window_metrics[window_type]["worst_drawdown_delta_vs_baseline"])
                for window_type in window_metrics
            )
            else float("nan")
        )

        research_gate_passed = bool(candidate_rank.get("research_gate_passed", False))
        long_cagr_gate = not pd.isna(long_excess_cagr) and long_excess_cagr >= LIVE_MIN_LONG_EXCESS_CAGR_VS_BASELINE
        long_drawdown_gate = (
            not pd.isna(long_drawdown_delta) and long_drawdown_delta >= -LIVE_MAX_LONG_DRAWDOWN_DEGRADATION_VS_BASELINE
        )
        turnover_gate = (
            not pd.isna(median_turnover_delta)
            and median_turnover_delta <= LIVE_MAX_MEDIAN_TURNOVER_INCREASE_VS_BASELINE
        )
        calendar_win_rate = float(window_metrics["calendar_year"]["baseline_cagr_win_rate"])
        rolling_3y_win_rate = float(window_metrics["rolling_3y"]["baseline_cagr_win_rate"])
        rolling_5y_win_rate = float(window_metrics["rolling_5y"]["baseline_cagr_win_rate"])
        calendar_win_gate = (
            not pd.isna(calendar_win_rate) and calendar_win_rate >= LIVE_MIN_CALENDAR_BASELINE_CAGR_WIN_RATE
        )
        rolling_3y_win_gate = (
            not pd.isna(rolling_3y_win_rate) and rolling_3y_win_rate >= LIVE_MIN_ROLLING_3Y_BASELINE_CAGR_WIN_RATE
        )
        rolling_5y_win_gate = (
            not pd.isna(rolling_5y_win_rate) and rolling_5y_win_rate >= LIVE_MIN_ROLLING_5Y_BASELINE_CAGR_WIN_RATE
        )
        worst_rolling_excess_gate = (
            not pd.isna(rolling_worst_excess) and rolling_worst_excess >= LIVE_MIN_WORST_ROLLING_EXCESS_CAGR_VS_BASELINE
        )
        worst_window_drawdown_gate = (
            not pd.isna(worst_drawdown_delta)
            and worst_drawdown_delta >= -LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE
        )
        live_gate_passed = bool(
            research_gate_passed
            and long_cagr_gate
            and long_drawdown_gate
            and turnover_gate
            and calendar_win_gate
            and rolling_3y_win_gate
            and rolling_5y_win_gate
            and worst_rolling_excess_gate
            and worst_window_drawdown_gate
        )
        rows.append(
            {
                "Candidate": candidate_id,
                "Display Name": candidate_rank.get("Display Name"),
                "Candidate Group": candidate_rank.get("Candidate Group"),
                "Rule": candidate_rank.get("Rule"),
                "Baseline Candidate": baseline_candidate,
                "research_gate_passed": research_gate_passed,
                "long_excess_cagr_vs_baseline": long_excess_cagr,
                "long_drawdown_delta_vs_baseline": long_drawdown_delta,
                "median_turnover_delta_vs_baseline": median_turnover_delta,
                "calendar_window_count": int(window_metrics["calendar_year"]["count"]),
                "calendar_baseline_cagr_win_rate": calendar_win_rate,
                "calendar_median_excess_cagr_vs_baseline": float(
                    window_metrics["calendar_year"]["median_excess_cagr_vs_baseline"]
                ),
                "rolling_3y_window_count": int(window_metrics["rolling_3y"]["count"]),
                "rolling_3y_baseline_cagr_win_rate": rolling_3y_win_rate,
                "rolling_3y_median_excess_cagr_vs_baseline": float(
                    window_metrics["rolling_3y"]["median_excess_cagr_vs_baseline"]
                ),
                "rolling_5y_window_count": int(window_metrics["rolling_5y"]["count"]),
                "rolling_5y_baseline_cagr_win_rate": rolling_5y_win_rate,
                "rolling_5y_median_excess_cagr_vs_baseline": float(
                    window_metrics["rolling_5y"]["median_excess_cagr_vs_baseline"]
                ),
                "worst_rolling_excess_cagr_vs_baseline": rolling_worst_excess,
                "worst_window_drawdown_delta_vs_baseline": worst_drawdown_delta,
                "live_gate_passed": live_gate_passed,
                "live_gate_reason": _live_readiness_gate_reason(
                    research_gate_passed=research_gate_passed,
                    long_cagr_gate=long_cagr_gate,
                    long_drawdown_gate=long_drawdown_gate,
                    turnover_gate=turnover_gate,
                    calendar_win_gate=calendar_win_gate,
                    rolling_3y_win_gate=rolling_3y_win_gate,
                    rolling_5y_win_gate=rolling_5y_win_gate,
                    worst_rolling_excess_gate=worst_rolling_excess_gate,
                    worst_window_drawdown_gate=worst_window_drawdown_gate,
                ),
                "live_action": "candidate_for_live_promotion_review" if live_gate_passed else "continue_research",
            }
        )
    if not rows:
        return pd.DataFrame()
    return (
        pd.DataFrame(rows)
        .sort_values(["live_gate_passed", "long_excess_cagr_vs_baseline"], ascending=[False, False])
        .reset_index(drop=True)
    )


def _weights_by_candidate_from_result(result: Mapping[str, object]) -> dict[str, pd.DataFrame]:
    return {key.removeprefix("weights_"): value for key, value in result.items() if key.startswith("weights_")}


def build_ranking(period_summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate, frame in period_summary.groupby("Candidate", sort=False):
        numeric = frame.copy()
        for column in (
            "CAGR",
            "Sharpe",
            "Max Drawdown",
            "Excess CAGR vs Benchmark",
            "Excess CAGR vs Secondary Benchmark",
            "Benchmark Max Drawdown",
            "Secondary Benchmark Max Drawdown",
            "Turnover/Year",
        ):
            numeric[column] = pd.to_numeric(numeric.get(column), errors="coerce")

        long_rows = numeric.loc[numeric["Period"].eq("long")]
        long_excess_spy = float(long_rows["Excess CAGR vs Benchmark"].iloc[0]) if not long_rows.empty else float("nan")
        long_excess_qqq = (
            float(long_rows["Excess CAGR vs Secondary Benchmark"].iloc[0]) if not long_rows.empty else float("nan")
        )
        min_sharpe = float(numeric["Sharpe"].min())
        median_sharpe = float(numeric["Sharpe"].median())
        worst_drawdown = float(numeric["Max Drawdown"].min())
        worst_qqq_drawdown = float(numeric["Secondary Benchmark Max Drawdown"].min())
        median_excess_spy = float(numeric["Excess CAGR vs Benchmark"].median())
        median_excess_qqq = float(numeric["Excess CAGR vs Secondary Benchmark"].median())
        median_turnover = float(numeric["Turnover/Year"].median())
        all_periods_available = (
            len(numeric) >= len(DEFAULT_PERIODS)
            and numeric["Trading Days"].fillna(0).ge(MIN_TRADING_DAYS_PER_PERIOD).all()
        )
        positive_return_all_periods = numeric["CAGR"].gt(0.0).all()
        positive_sharpe_all_periods = numeric["Sharpe"].gt(0.0).all()
        long_spy_gate = not pd.isna(long_excess_spy) and long_excess_spy > 0.0
        median_spy_gate = not pd.isna(median_excess_spy) and median_excess_spy > 0.0
        qqq_tradeoff_gate = bool(
            (not pd.isna(long_excess_qqq) and long_excess_qqq > 0.0)
            or (
                not pd.isna(worst_drawdown)
                and not pd.isna(worst_qqq_drawdown)
                and worst_drawdown >= worst_qqq_drawdown + DRAWNDOWN_ADVANTAGE_VS_QQQ
            )
        )
        research_gate_passed = bool(
            all_periods_available
            and positive_return_all_periods
            and positive_sharpe_all_periods
            and long_spy_gate
            and median_spy_gate
            and qqq_tradeoff_gate
        )
        first = frame.iloc[0]
        score = (
            min_sharpe
            + 0.50 * median_sharpe
            + 6.0 * long_excess_spy
            + 3.0 * median_excess_spy
            + 1.0 * median_excess_qqq
            + 0.75 * worst_drawdown
            - 0.03 * median_turnover
        )
        rows.append(
            {
                "Candidate": candidate,
                "Display Name": first.get("Display Name"),
                "Candidate Group": first.get("Candidate Group"),
                "Rule": first.get("Rule"),
                "Primary Benchmark Symbol": first.get("Primary Benchmark Symbol"),
                "Secondary Benchmark Symbol": first.get("Secondary Benchmark Symbol"),
                "min_sharpe": min_sharpe,
                "median_sharpe": median_sharpe,
                "median_excess_cagr_vs_spy": median_excess_spy,
                "long_excess_cagr_vs_spy": long_excess_spy,
                "median_excess_cagr_vs_qqq": median_excess_qqq,
                "long_excess_cagr_vs_qqq": long_excess_qqq,
                "worst_drawdown": worst_drawdown,
                "worst_qqq_drawdown": worst_qqq_drawdown,
                "median_turnover_per_year": median_turnover,
                "robustness_score": score,
                "research_gate_passed": research_gate_passed,
                "gate_reason": _gate_reason(
                    all_periods_available=all_periods_available,
                    positive_return_all_periods=positive_return_all_periods,
                    positive_sharpe_all_periods=positive_sharpe_all_periods,
                    long_spy_gate=long_spy_gate,
                    median_spy_gate=median_spy_gate,
                    qqq_tradeoff_gate=qqq_tradeoff_gate,
                ),
                "Top N": first.get("Top N"),
                "Rebalance Months": first.get("Rebalance Months"),
                "Canary Bad Threshold": first.get("Canary Bad Threshold"),
                "Score Mode": first.get("Score Mode"),
                "Canary Mode": first.get("Canary Mode"),
                "Safe Fraction Per Bad Canary": first.get("Safe Fraction Per Bad Canary"),
                "Confidence Weighting": first.get("Confidence Weighting"),
                "Notes": first.get("Notes"),
            }
        )

    ranking = (
        pd.DataFrame(rows)
        .sort_values(["research_gate_passed", "robustness_score"], ascending=[False, False])
        .reset_index(drop=True)
    )
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    ranking["paper_review_candidate"] = ranking["research_gate_passed"] & ranking["Candidate Group"].eq(
        "offensive_candidate"
    )
    ranking["live_review_candidate"] = ranking["research_gate_passed"] & ranking["Candidate Group"].eq(
        "liveable_candidate"
    )
    ranking["review_action"] = "reject"
    ranking.loc[ranking["Candidate Group"].eq("current_live_baseline"), "review_action"] = "keep_current_live"
    ranking.loc[ranking["paper_review_candidate"], "review_action"] = "paper_review_only"
    ranking.loc[ranking["live_review_candidate"], "review_action"] = "live_design_review"
    return ranking
