"""Backtest engines for Global ETF offensive rotation variants and liveable composites."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd


from ._global_etf_research_metrics import (
    _normalize_price_history,
    _compute_turnover,
    _normalize_weights,
    summarize_returns,
)
from ._global_etf_research_models import (
    DEFAULT_MONTHLY_REBALANCE_MONTHS,
    _normalize_symbols,
    DEFAULT_PRIMARY_BENCHMARK,
    DEFAULT_SAFE_HAVEN,
    DEFAULT_SECONDARY_BENCHMARK,
    DEFAULT_TURNOVER_COST_BPS,
    GlobalEtfLiveableCompositeSpec,
    GlobalEtfOffensiveVariantSpec,
    _period_end,
    _period_start,
)

@dataclass(frozen=True)
class IndicatorContext:
    close: pd.DataFrame
    returns: pd.DataFrame
    momentum: pd.DataFrame
    fast_momentum: pd.DataFrame
    sma_pass_by_period: Mapping[int, pd.DataFrame]
    volatility_by_window: Mapping[int, pd.DataFrame]


def _compute_13612w_momentum_series(series: pd.Series) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    output = pd.Series(float("nan"), index=series.index, dtype=float)
    if len(clean) < 253:
        return output
    monthly_last = clean.groupby(clean.index.to_period("M")).last()
    for as_of, current in clean.items():
        current_period = pd.Timestamp(as_of).to_period("M")
        values: list[float] = []
        for months, weight in ((1, 12), (3, 4), (6, 2), (12, 1)):
            prior = monthly_last.get(current_period - months, float("nan"))
            if pd.isna(prior) or float(prior) == 0.0:
                values = []
                break
            values.append(float(weight) * (float(current) / float(prior) - 1.0))
        if values:
            output.at[as_of] = float(sum(values) / 19.0)
    return output


def _build_indicator_context(
    close: pd.DataFrame,
    *,
    variants: Sequence[GlobalEtfOffensiveVariantSpec],
) -> IndicatorContext:
    sma_periods = sorted({int(variant.sma_period) for variant in variants})
    volatility_windows = sorted(
        {int(variant.confidence_volatility_window) for variant in variants}
        | {int(variant.score_volatility_window) for variant in variants}
    )
    returns = close.pct_change(fill_method=None).fillna(0.0)
    momentum = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for symbol in close.columns:
        momentum[symbol] = _compute_13612w_momentum_series(close[symbol])
    fast_momentum = (
        12.0 * (close / close.shift(21) - 1.0)
        + 4.0 * (close / close.shift(63) - 1.0)
        + 2.0 * (close / close.shift(126) - 1.0)
    ) / 18.0
    sma_pass_by_period = {
        period: close.gt(close.rolling(int(period), min_periods=int(period)).mean()) for period in sma_periods
    }
    volatility_by_window = {
        window: returns.rolling(int(window), min_periods=int(window)).std(ddof=0) * math.sqrt(252.0)
        for window in volatility_windows
    }
    return IndicatorContext(
        close=close,
        returns=returns,
        momentum=momentum,
        fast_momentum=fast_momentum,
        sma_pass_by_period=sma_pass_by_period,
        volatility_by_window=volatility_by_window,
    )


def _rebalance_dates(index: pd.DatetimeIndex, months: Sequence[int]) -> set[pd.Timestamp]:
    allowed_months = {int(month) for month in months}
    frame = pd.DataFrame({"as_of": index}, index=index)
    month_ends = frame.groupby(index.to_period("M"))["as_of"].tail(1)
    return {pd.Timestamp(value).normalize() for value in month_ends if pd.Timestamp(value).month in allowed_months}


def _indicator_value(frame: pd.DataFrame, as_of: pd.Timestamp, symbol: str) -> float:
    symbol = str(symbol).strip().upper()
    if symbol not in frame.columns or as_of not in frame.index:
        return float("nan")
    value = frame.at[as_of, symbol]
    return float(value) if pd.notna(value) else float("nan")


def _zscore_mapping(values: Mapping[str, float]) -> dict[str, float]:
    clean = {symbol: float(value) for symbol, value in values.items() if pd.notna(value)}
    if not clean:
        return {}
    series = pd.Series(clean, dtype=float)
    std = float(series.std(ddof=0))
    if std <= 0.0 or pd.isna(std):
        return {symbol: 0.0 for symbol in clean}
    return ((series - float(series.mean())) / std).to_dict()


def _pool_correlation_scores(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
) -> dict[str, float]:
    window = max(2, int(spec.score_correlation_window))
    pool = [symbol for symbol in _normalize_symbols(spec.ranking_pool) if symbol in context.returns.columns]
    frame = context.returns.loc[:as_of, pool].tail(window).dropna(axis=1, how="all")
    if len(frame) < max(2, window // 2) or frame.shape[1] < 2:
        return {}
    pool_index = frame.mean(axis=1)
    correlations: dict[str, float] = {}
    pool_std = float(pool_index.std(ddof=0))
    for symbol in frame.columns:
        symbol_std = float(frame[symbol].std(ddof=0))
        if pool_std <= 0.0 or symbol_std <= 0.0 or pd.isna(pool_std) or pd.isna(symbol_std):
            correlations[str(symbol)] = 0.0
            continue
        corr = frame[symbol].corr(pool_index)
        correlations[str(symbol)] = float(corr) if pd.notna(corr) else 0.0
    return correlations


def _candidate_scores(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
    current_holdings: Sequence[str],
) -> dict[str, float]:
    sma_pass = context.sma_pass_by_period[int(spec.sma_period)]
    current_set = {str(symbol).strip().upper() for symbol in current_holdings}
    rows: dict[str, dict[str, float]] = {}
    for ticker in spec.ranking_pool:
        symbol = str(ticker).strip().upper()
        momentum = _indicator_value(context.momentum, as_of, symbol)
        fast_momentum = _indicator_value(context.fast_momentum, as_of, symbol)
        if bool(_indicator_value(sma_pass, as_of, symbol)) is False:
            continue
        score_mode = str(spec.score_mode)
        if score_mode == "runtime_13612w":
            if pd.isna(momentum):
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
        elif score_mode == "fast_136w":
            if pd.isna(fast_momentum) or fast_momentum <= 0.0:
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
        elif score_mode == "dual_momentum_13612w":
            if pd.isna(momentum) or momentum <= 0.0:
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
        else:
            if pd.isna(momentum) or momentum <= 0.0:
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
    if not rows:
        return {}

    if str(spec.score_mode) == "runtime_13612w":
        return {
            symbol: values["momentum"] + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            for symbol, values in rows.items()
        }
    if str(spec.score_mode) == "fast_136w":
        return {
            symbol: values["fast_momentum"] + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            for symbol, values in rows.items()
        }
    if str(spec.score_mode) == "dual_momentum_13612w":
        return {
            symbol: values["momentum"] + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            for symbol, values in rows.items()
        }
    if str(spec.score_mode) == "eaa_generalized":
        momentum_z = _zscore_mapping({symbol: values["momentum"] for symbol, values in rows.items()})
        volatility = context.volatility_by_window[int(spec.score_volatility_window)]
        vol_z = _zscore_mapping({symbol: _indicator_value(volatility, as_of, symbol) for symbol in rows})
        corr_z = _zscore_mapping(_pool_correlation_scores(context, spec, as_of=as_of))
        return {
            symbol: (
                momentum_z.get(symbol, 0.0)
                - 0.50 * vol_z.get(symbol, 0.0)
                - 0.25 * corr_z.get(symbol, 0.0)
                + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            )
            for symbol in rows
        }
    raise ValueError(f"unsupported score_mode: {spec.score_mode}")


def _resolve_variant_target_weights(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
    current_holdings: Sequence[str],
    rebalance_dates: set[pd.Timestamp],
) -> tuple[dict[str, float] | None, str, bool, str]:
    safe_haven = spec.safe_haven.upper()
    n_bad = 0
    canary_details: list[str] = []
    for ticker in spec.canary_assets:
        symbol = str(ticker).strip().upper()
        momentum = _indicator_value(context.momentum, as_of, symbol)
        if pd.isna(momentum) or momentum < 0.0:
            n_bad += 1
            canary_details.append(f"{symbol}:❌({momentum:.3f})" if not pd.isna(momentum) else f"{symbol}:❌(nan)")
        else:
            canary_details.append(f"{symbol}:✅({momentum:.3f})")
    canary_str = ", ".join(canary_details)
    canary_mode = str(spec.canary_mode).strip().lower()
    if canary_mode == "threshold" and n_bad >= int(spec.canary_bad_threshold):
        return {safe_haven: 1.0}, "emergency", True, canary_str

    if pd.Timestamp(as_of).normalize() not in rebalance_dates:
        return None, "daily_check", False, canary_str

    scores = _candidate_scores(context, spec, as_of=as_of, current_holdings=current_holdings)
    sorted_tickers = sorted(scores.items(), key=lambda item: -item[1])
    top = sorted_tickers[: max(1, int(spec.top_n))]
    if not top:
        return {safe_haven: 1.0}, "emergency", False, canary_str

    per_weight = 1.0 / float(max(1, int(spec.top_n)))
    weights = {symbol: per_weight for symbol, _score in top}
    if spec.confidence_weighting_enabled and int(spec.top_n) == 2 and len(top) == 2:
        score_values = [score for _symbol, score in sorted_tickers]
        dispersion = float(np.nanstd(score_values))
        confidence = (
            float("nan") if dispersion <= 0.0 or np.isnan(dispersion) else float((top[0][1] - top[1][1]) / dispersion)
        )
        use_confidence_weight = not pd.isna(confidence) and confidence >= float(spec.confidence_threshold)
        if use_confidence_weight and spec.confidence_volatility_gate_enabled:
            volatility = context.volatility_by_window[int(spec.confidence_volatility_window)]
            top1_vol = _indicator_value(volatility, as_of, top[0][0])
            top2_vol = _indicator_value(volatility, as_of, top[1][0])
            use_confidence_weight = (
                not pd.isna(top1_vol)
                and not pd.isna(top2_vol)
                and top2_vol > 0.0
                and top1_vol <= top2_vol * float(spec.confidence_volatility_max_ratio)
            )
        if use_confidence_weight:
            top1_weight = min(1.0, max(per_weight, float(spec.confidence_top1_weight)))
            weights = {top[0][0]: top1_weight, top[1][0]: 1.0 - top1_weight}

    if canary_mode == "cash_fraction":
        canary_count = max(1, len(tuple(spec.canary_assets)))
        safe_fraction = min(float(spec.max_safe_fraction), float(spec.safe_fraction_per_bad_canary) * float(n_bad))
        if n_bad >= canary_count:
            safe_fraction = float(spec.max_safe_fraction)
        safe_fraction = max(0.0, min(1.0, safe_fraction))
        if safe_fraction > 0.0:
            risk_fraction = 1.0 - safe_fraction
            weights = {symbol: weight * risk_fraction for symbol, weight in weights.items()}
            weights[safe_haven] = weights.get(safe_haven, 0.0) + safe_fraction
    elif canary_mode != "threshold":
        raise ValueError(f"unsupported canary_mode: {spec.canary_mode}")
    return weights, "rebalance", False, canary_str


def run_variant_backtest(
    price_history: pd.DataFrame,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    indicator_context: IndicatorContext | None = None,
) -> dict[str, object]:
    close = _normalize_price_history(price_history) if indicator_context is None else indicator_context.close
    full_start = pd.Timestamp(start_date).normalize() if start_date else close.index.min()
    full_end = pd.Timestamp(end_date).normalize() if end_date else close.index.max()
    close = close.loc[close.index <= full_end].copy()
    context = indicator_context or _build_indicator_context(close, variants=(spec,))
    if context.close.index[-1] > full_end:
        context = IndicatorContext(
            close=context.close.loc[:full_end].copy(),
            returns=context.returns.loc[:full_end].copy(),
            momentum=context.momentum.loc[:full_end].copy(),
            fast_momentum=context.fast_momentum.loc[:full_end].copy(),
            sma_pass_by_period={key: value.loc[:full_end].copy() for key, value in context.sma_pass_by_period.items()},
            volatility_by_window={
                key: value.loc[:full_end].copy() for key, value in context.volatility_by_window.items()
            },
        )
    active_index = context.close.index[context.close.index >= full_start]
    if len(active_index) < 2:
        raise ValueError(f"not enough price history for {spec.candidate_id}")

    safe_haven = spec.safe_haven.upper()
    symbols = tuple(
        dict.fromkeys(
            (
                *tuple(symbol.upper() for symbol in spec.ranking_pool),
                *tuple(symbol.upper() for symbol in spec.canary_assets),
                spec.primary_benchmark_symbol.upper(),
                spec.secondary_benchmark_symbol.upper(),
                safe_haven,
            )
        )
    )
    portfolio_returns = pd.Series(0.0, index=active_index[1:], name=spec.candidate_id)
    turnover_history = pd.Series(0.0, index=active_index[1:], name="turnover")
    weights_history = pd.DataFrame(0.0, index=active_index[1:], columns=symbols)
    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: tuple[str, ...] = ()
    signal_rows: list[dict[str, object]] = []
    rebalance_dates = _rebalance_dates(active_index, spec.rebalance_months)

    for pos, as_of in enumerate(active_index[:-1]):
        next_date = active_index[pos + 1]
        weights, signal_description, is_emergency, canary = _resolve_variant_target_weights(
            context,
            spec,
            as_of=as_of,
            current_holdings=current_holdings,
            rebalance_dates=rebalance_dates,
        )

        if weights is not None:
            target_weights = _normalize_weights(weights, safe_haven=safe_haven)
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = tuple(
                symbol for symbol, weight in current_weights.items() if symbol != safe_haven and float(weight) > 0.0
            )
            signal_rows.append(
                {
                    "candidate_id": spec.candidate_id,
                    "as_of": as_of,
                    "next_date": next_date,
                    "signal_description": signal_description,
                    "is_emergency": bool(is_emergency),
                    "canary": canary,
                    "turnover": turnover,
                    **{f"weight_{symbol}": weight for symbol, weight in current_weights.items()},
                }
            )

        for symbol, weight in current_weights.items():
            if symbol in weights_history.columns:
                weights_history.at[next_date, symbol] = float(weight)
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol in context.returns.columns:
                value = context.returns.at[next_date, symbol]
                if pd.notna(value):
                    gross_return += float(weight) * float(value)
        cost = float(turnover_history.at[next_date]) * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    benchmark_returns = context.returns.get(
        spec.primary_benchmark_symbol.upper(), pd.Series(index=portfolio_returns.index, dtype=float)
    )
    secondary_benchmark_returns = context.returns.get(
        spec.secondary_benchmark_symbol.upper(), pd.Series(index=portfolio_returns.index, dtype=float)
    )
    summary = summarize_returns(
        portfolio_returns,
        weights_history=weights_history,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Group": spec.candidate_group,
            "Rule": spec.rule,
            "Primary Benchmark Symbol": spec.primary_benchmark_symbol.upper(),
            "Secondary Benchmark Symbol": spec.secondary_benchmark_symbol.upper(),
            "Safe Haven": safe_haven,
            "Top N": int(spec.top_n),
            "Rebalance Months": ",".join(str(month) for month in spec.rebalance_months),
            "SMA Period": int(spec.sma_period),
            "Canary Bad Threshold": int(spec.canary_bad_threshold),
            "Score Mode": str(spec.score_mode),
            "Canary Mode": str(spec.canary_mode),
            "Safe Fraction Per Bad Canary": float(spec.safe_fraction_per_bad_canary),
            "Confidence Weighting": bool(spec.confidence_weighting_enabled),
            "Confidence Volatility Gate": bool(spec.confidence_volatility_gate_enabled),
            "Ranking Pool Size": int(len(spec.ranking_pool)),
            "Ranking Pool": ",".join(spec.ranking_pool),
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history,
        "signal_history": pd.DataFrame(signal_rows),
        "benchmark_returns": benchmark_returns,
        "secondary_benchmark_returns": secondary_benchmark_returns,
    }


def _period_summary_from_result(
    result: Mapping[str, object],
    *,
    period_name: str,
    start_date: str,
    end_date: str | None,
) -> dict[str, object]:
    returns = pd.Series(result["portfolio_returns"]).copy()
    returns.index = pd.to_datetime(returns.index).tz_localize(None).normalize()
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else returns.index.max()
    period_returns = returns.loc[(returns.index >= start_ts) & (returns.index <= end_ts)]

    weights = pd.DataFrame(result.get("weights_history", pd.DataFrame())).copy()
    if not weights.empty:
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights = weights.loc[(weights.index >= start_ts) & (weights.index <= end_ts)]

    benchmark_returns = pd.Series(result.get("benchmark_returns", pd.Series(dtype=float))).copy()
    if not benchmark_returns.empty:
        benchmark_returns.index = pd.to_datetime(benchmark_returns.index).tz_localize(None).normalize()
    secondary_benchmark_returns = pd.Series(result.get("secondary_benchmark_returns", pd.Series(dtype=float))).copy()
    if not secondary_benchmark_returns.empty:
        secondary_benchmark_returns.index = (
            pd.to_datetime(secondary_benchmark_returns.index).tz_localize(None).normalize()
        )

    summary = summarize_returns(
        period_returns,
        weights_history=weights,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    base = dict(result["summary"])
    for key in (
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
    ):
        summary[key] = base.get(key)
    return {"Period": period_name, **summary}


def _weight_frame_for_index(weights_history: pd.DataFrame, index: pd.DatetimeIndex) -> pd.DataFrame:
    weights = pd.DataFrame(weights_history).copy()
    if weights.empty:
        return pd.DataFrame(index=index)
    weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
    weights = weights.sort_index()
    weights.columns = [str(column).strip().upper() for column in weights.columns]
    weights = weights.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return weights.reindex(index).ffill().fillna(0.0)


def _monthly_applied_overlay_weight(
    raw_weight: pd.Series,
    *,
    target_index: pd.DatetimeIndex,
    rebalance_months: Sequence[int] = DEFAULT_MONTHLY_REBALANCE_MONTHS,
) -> pd.Series:
    raw = pd.to_numeric(pd.Series(raw_weight), errors="coerce").fillna(0.0)
    raw.index = pd.to_datetime(raw.index).tz_localize(None).normalize()
    decisions = pd.Series(float("nan"), index=raw.index, dtype=float)
    rebalance_dates = _rebalance_dates(pd.DatetimeIndex(raw.index), rebalance_months)
    if rebalance_dates:
        dates = [date for date in sorted(rebalance_dates) if date in raw.index]
        decisions.loc[dates] = raw.loc[dates].astype(float)
    applied = decisions.ffill().shift(1)
    applied = applied.reindex(target_index).ffill().fillna(0.0)
    return applied.clip(lower=0.0, upper=1.0)


def _gross_returns_from_weights(
    context: IndicatorContext,
    weights: pd.DataFrame,
    *,
    target_index: pd.DatetimeIndex,
) -> pd.Series:
    weight_frame = _weight_frame_for_index(weights, target_index)
    asset_returns = context.returns.reindex(target_index).fillna(0.0)
    tradable_columns = [column for column in weight_frame.columns if column in asset_returns.columns]
    if not tradable_columns:
        return pd.Series(0.0, index=target_index, dtype=float)
    return (
        weight_frame.reindex(columns=tradable_columns, fill_value=0.0)
        .mul(asset_returns.reindex(columns=tradable_columns), axis=0)
        .sum(axis=1)
    )


def _rolling_total_return(returns: pd.Series, window: int) -> pd.Series:
    series = pd.to_numeric(pd.Series(returns), errors="coerce").fillna(0.0)
    return (1.0 + series).rolling(int(window), min_periods=int(window)).apply(np.prod, raw=True).sub(1.0)


def _composite_trend_gate(context: IndicatorContext, spec: GlobalEtfLiveableCompositeSpec) -> pd.Series:
    symbol = str(spec.regime_symbol).strip().upper()
    if symbol not in context.close.columns:
        return pd.Series(False, index=context.close.index)
    close = pd.to_numeric(context.close[symbol], errors="coerce")
    sma = close.rolling(int(spec.trend_sma_period), min_periods=int(spec.trend_sma_period)).mean()
    eligible = close.gt(sma)
    if spec.trend_fast_momentum_required and symbol in context.fast_momentum.columns:
        eligible = eligible & pd.to_numeric(context.fast_momentum[symbol], errors="coerce").gt(0.0)
    return eligible.fillna(False)


def _build_composite_overlay_weight(
    context: IndicatorContext,
    spec: GlobalEtfLiveableCompositeSpec,
    *,
    target_index: pd.DatetimeIndex,
    base_weights: pd.DataFrame | None = None,
    overlay_weights: pd.DataFrame | None = None,
) -> pd.Series:
    max_weight = max(0.0, min(1.0, float(spec.overlay_weight)))
    rule = str(spec.rule).strip().lower()
    if rule.startswith("static_blend"):
        return pd.Series(max_weight, index=target_index, dtype=float)

    trend_gate = _composite_trend_gate(context, spec)
    if rule.startswith("qqq_trend_overlay"):
        raw_weight = pd.Series(0.0, index=context.close.index, dtype=float)
        raw_weight.loc[trend_gate] = max_weight
        return _monthly_applied_overlay_weight(raw_weight, target_index=target_index)

    if rule.startswith("trend_drawdown_brake"):
        symbol = str(spec.regime_symbol).strip().upper()
        min_weight = max(0.0, min(max_weight, float(spec.min_overlay_weight)))
        raw_weight = pd.Series(min_weight, index=context.close.index, dtype=float)
        close = pd.to_numeric(context.close.get(symbol, pd.Series(index=context.close.index)), errors="coerce")
        rolling_peak = close.rolling(int(spec.drawdown_window), min_periods=int(spec.drawdown_window)).max()
        drawdown = close.div(rolling_peak).sub(1.0)
        drawdown_ok = drawdown.ge(float(spec.drawdown_threshold)).fillna(False)
        risk_on = trend_gate & drawdown_ok
        raw_weight.loc[risk_on] = max_weight
        return _monthly_applied_overlay_weight(raw_weight, target_index=target_index)

    if rule.startswith("baseline_relative_decay_brake"):
        if base_weights is None or overlay_weights is None:
            raise ValueError("baseline-relative decay brake requires base and overlay weights")
        min_weight = max(0.0, min(max_weight, float(spec.min_overlay_weight)))
        base_returns = _gross_returns_from_weights(context, base_weights, target_index=target_index)
        overlay_returns = _gross_returns_from_weights(context, overlay_weights, target_index=target_index)
        fast_window = int(spec.relative_decay_fast_window)
        slow_window = int(spec.relative_decay_slow_window)
        fast_excess = _rolling_total_return(overlay_returns, fast_window).sub(
            _rolling_total_return(base_returns, fast_window)
        )
        slow_excess = _rolling_total_return(overlay_returns, slow_window).sub(
            _rolling_total_return(base_returns, slow_window)
        )
        brake = fast_excess.lt(float(spec.relative_decay_fast_threshold)) & slow_excess.lt(
            float(spec.relative_decay_slow_threshold)
        )
        raw_weight = pd.Series(max_weight, index=target_index, dtype=float)
        raw_weight.loc[brake.fillna(False)] = min_weight
        return _monthly_applied_overlay_weight(raw_weight, target_index=target_index)

    if rule.startswith("volatility_managed_overlay"):
        symbol = str(spec.regime_symbol).strip().upper()
        returns = pd.to_numeric(context.returns.get(symbol, pd.Series(index=context.returns.index)), errors="coerce")
        realized_vol = returns.rolling(int(spec.volatility_window), min_periods=int(spec.volatility_window)).std(
            ddof=0
        ) * math.sqrt(252.0)
        scale = (float(spec.target_volatility) / realized_vol).replace([np.inf, -np.inf], np.nan)
        raw_weight = max_weight * scale.clip(lower=0.0, upper=1.0).fillna(0.0)
        if float(spec.min_overlay_weight) > 0.0:
            raw_weight = raw_weight.where(raw_weight.le(0.0), raw_weight.clip(lower=float(spec.min_overlay_weight)))
        raw_weight = raw_weight.reindex(context.close.index).fillna(0.0)
        raw_weight.loc[~trend_gate] = 0.0
        return _monthly_applied_overlay_weight(raw_weight, target_index=target_index)

    raise ValueError(f"unsupported liveable composite rule: {spec.rule}")


def _combine_composite_weights(
    *,
    base_weights: pd.DataFrame,
    overlay_weights: pd.DataFrame,
    overlay_weight: pd.Series,
) -> pd.DataFrame:
    index = pd.DatetimeIndex(overlay_weight.index)
    base = _weight_frame_for_index(base_weights, index)
    overlay = _weight_frame_for_index(overlay_weights, index)
    columns = sorted(set(base.columns) | set(overlay.columns))
    base = base.reindex(columns=columns, fill_value=0.0)
    overlay = overlay.reindex(columns=columns, fill_value=0.0)
    sleeve = pd.to_numeric(overlay_weight, errors="coerce").reindex(index).fillna(0.0).clip(lower=0.0, upper=1.0)
    combined = base.mul(1.0 - sleeve, axis=0).add(overlay.mul(sleeve, axis=0), fill_value=0.0)
    row_sum = combined.sum(axis=1)
    valid = row_sum.gt(0.0)
    combined.loc[valid] = combined.loc[valid].div(row_sum.loc[valid], axis=0)
    return combined.fillna(0.0)


def _composite_signal_history(
    spec: GlobalEtfLiveableCompositeSpec,
    *,
    overlay_weight: pd.Series,
) -> pd.DataFrame:
    if overlay_weight.empty:
        return pd.DataFrame()
    weight = pd.to_numeric(pd.Series(overlay_weight), errors="coerce").fillna(0.0)
    changed = weight.diff().abs().gt(1e-9)
    changed.iloc[0] = bool(abs(float(weight.iloc[0])) > 1e-9)
    rows: list[dict[str, object]] = []
    for next_date, sleeve_weight in weight.loc[changed].items():
        pos = weight.index.get_loc(next_date)
        as_of = weight.index[max(0, int(pos) - 1)]
        rows.append(
            {
                "candidate_id": spec.candidate_id,
                "as_of": as_of,
                "next_date": next_date,
                "signal_description": spec.rule,
                "is_emergency": False,
                "base_candidate_id": spec.base_candidate_id,
                "overlay_candidate_id": spec.overlay_candidate_id,
                "overlay_weight": float(sleeve_weight),
            }
        )
    return pd.DataFrame(rows)


def run_liveable_composite_backtest(
    *,
    spec: GlobalEtfLiveableCompositeSpec,
    context: IndicatorContext,
    base_weights: pd.DataFrame,
    overlay_weights: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    full_start = pd.Timestamp(start_date).normalize() if start_date else context.close.index.min()
    full_end = pd.Timestamp(end_date).normalize() if end_date else context.close.index.max()
    target_index = context.returns.index[(context.returns.index >= full_start) & (context.returns.index <= full_end)]
    target_index = target_index.intersection(pd.DatetimeIndex(base_weights.index)).intersection(
        pd.DatetimeIndex(overlay_weights.index)
    )
    if len(target_index) < 2:
        raise ValueError(f"not enough child weight history for {spec.candidate_id}")

    overlay_weight = _build_composite_overlay_weight(
        context,
        spec,
        target_index=target_index,
        base_weights=base_weights,
        overlay_weights=overlay_weights,
    )
    combined_weights = _combine_composite_weights(
        base_weights=base_weights,
        overlay_weights=overlay_weights,
        overlay_weight=overlay_weight,
    )
    asset_returns = context.returns.reindex(target_index).fillna(0.0)
    tradable_columns = [column for column in combined_weights.columns if column in asset_returns.columns]
    gross_returns = (
        combined_weights.reindex(columns=tradable_columns, fill_value=0.0)
        .mul(asset_returns.reindex(columns=tradable_columns), axis=0)
        .sum(axis=1)
    )
    turnover = 0.5 * combined_weights.diff().abs().sum(axis=1).fillna(0.0)
    portfolio_returns = gross_returns - turnover * (float(turnover_cost_bps) / 10_000.0)
    portfolio_returns.name = spec.candidate_id

    benchmark_returns = context.returns.get(DEFAULT_PRIMARY_BENCHMARK, pd.Series(index=portfolio_returns.index))
    secondary_benchmark_returns = context.returns.get(
        DEFAULT_SECONDARY_BENCHMARK, pd.Series(index=portfolio_returns.index)
    )
    summary = summarize_returns(
        portfolio_returns,
        weights_history=combined_weights,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Group": "liveable_candidate",
            "Rule": spec.rule,
            "Primary Benchmark Symbol": DEFAULT_PRIMARY_BENCHMARK,
            "Secondary Benchmark Symbol": DEFAULT_SECONDARY_BENCHMARK,
            "Safe Haven": DEFAULT_SAFE_HAVEN,
            "Top N": "",
            "Rebalance Months": ",".join(str(month) for month in DEFAULT_MONTHLY_REBALANCE_MONTHS),
            "SMA Period": int(spec.trend_sma_period),
            "Canary Bad Threshold": "",
            "Score Mode": "composite",
            "Canary Mode": "child_strategy",
            "Safe Fraction Per Bad Canary": "",
            "Confidence Weighting": "",
            "Confidence Volatility Gate": "",
            "Ranking Pool Size": "",
            "Ranking Pool": f"{spec.base_candidate_id},{spec.overlay_candidate_id}",
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": combined_weights,
        "turnover_history": turnover,
        "signal_history": _composite_signal_history(spec, overlay_weight=overlay_weight),
        "benchmark_returns": benchmark_returns,
        "secondary_benchmark_returns": secondary_benchmark_returns,
    }


def build_liveable_composite_results(
    *,
    context: IndicatorContext,
    specs: Sequence[GlobalEtfLiveableCompositeSpec],
    periods: Sequence[tuple[str, str, str | None]],
    weights_by_candidate: Mapping[str, pd.DataFrame],
    turnover_cost_bps: float,
) -> dict[str, object]:
    full_start = _period_start(periods)
    full_end = _period_end(periods)
    rows: list[dict[str, object]] = []
    returns_by_candidate: dict[str, pd.Series] = {}
    weights_by_composite: dict[str, pd.DataFrame] = {}
    signal_frames: list[pd.DataFrame] = []
    for spec in specs:
        if spec.base_candidate_id not in weights_by_candidate or spec.overlay_candidate_id not in weights_by_candidate:
            continue
        result = run_liveable_composite_backtest(
            spec=spec,
            context=context,
            base_weights=weights_by_candidate[spec.base_candidate_id],
            overlay_weights=weights_by_candidate[spec.overlay_candidate_id],
            start_date=full_start,
            end_date=full_end,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_candidate[spec.candidate_id] = pd.Series(result["portfolio_returns"], name=spec.candidate_id)
        weights_by_composite[spec.candidate_id] = pd.DataFrame(result["weights_history"])
        signal_history = pd.DataFrame(result.get("signal_history", pd.DataFrame()))
        if not signal_history.empty:
            signal_frames.append(signal_history)
        for period_name, start_date, end_date in periods:
            rows.append(
                _period_summary_from_result(
                    result,
                    period_name=period_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    return {
        "period_rows": rows,
        "returns_by_candidate": returns_by_candidate,
        "weights_by_candidate": weights_by_composite,
        "signal_history": pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame(),
    }
