from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from .crisis_regime_guard_research import (
    DEFAULT_BUBBLE_LOOKBACK_DAYS,
    DEFAULT_BUBBLE_PERSISTENCE_DAYS,
    DEFAULT_BUBBLE_RETURN_THRESHOLD,
    DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
    DEFAULT_FINANCIAL_SYMBOL,
    DEFAULT_MARKET_SYMBOL,
    apply_context_gate_to_signal,
    build_ai_crisis_opinions,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_backtest import DEFAULT_TURNOVER_COST_BPS
from .taco_panic_rebound_overlay_compare import (
    DEFAULT_ATTACK_SYMBOL,
    DEFAULT_BENCHMARK_SYMBOL,
    DEFAULT_CASH_SYMBOL,
    DEFAULT_COMPARISON_PERIODS,
    DEFAULT_OVERLAY_SLEEVE_RATIOS,
    DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    DEFAULT_SAFE_SYMBOL,
    DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    add_synthetic_attack_close,
    apply_price_crisis_guard_to_weights,
    build_crisis_guard_diagnostics,
    build_deltas_vs_base,
    build_diagnostics,
    build_period_summary,
    build_price_crisis_guard_signal,
    build_price_stress_scan,
    build_tqqq_growth_income_base_weights,
    close_matrix_to_price_history,
    filter_events_by_price_stress,
    _add_overlay_strategy_returns,
    _format_percent_columns,
    _integrate_overlay_weights,
    _next_index_date,
    _parse_float_tuple,
    _parse_str_tuple,
    _run_taco_overlay_backtest,
    _weights_to_returns,
)
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
    TRADE_WAR_EVENT_SETS,
    TRADE_WAR_EVENTS_2018_TO_PRESENT,
    TradeWarEvent,
    events_to_frame,
    price_history_to_close_matrix,
    resolve_trade_war_event_set,
)
from .yfinance_prices import download_price_history

DEFAULT_START_DATE = "1999-03-10"
DEFAULT_PRICE_START_DATE = "1999-03-10"
DEFAULT_RESPONSE_CRISIS_RISK_MULTIPLIER = 0.25
DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS = 5
DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN = -0.08
DEFAULT_BUBBLE_FRAGILITY_MA_DAYS = 100
DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS = 20
DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS = 5
DEFAULT_SYNTHETIC_ATTACK_MULTIPLE = 3.0
DEFAULT_RESPONSE_SAFE_SYMBOL = "SHY"
ROUTE_TACO = "taco_fake_crisis"
ROUTE_TRUE_CRISIS = "true_crisis"
ROUTE_NO_ACTION = "no_action"
CRISIS_CONTEXT_MODE_V1_AI_RUBRIC = "v1_ai_rubric"
CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK = "v2_context_pack"
CRISIS_CONTEXT_MODES = (CRISIS_CONTEXT_MODE_V1_AI_RUBRIC, CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK)
DEFAULT_CONTEXT_FINANCIAL_SYMBOLS = ("XLF", "KRE")
DEFAULT_CONTEXT_CREDIT_PAIRS = (("HYG", "IEF"), ("LQD", "IEF"))
DEFAULT_CONTEXT_RATE_SYMBOLS = ("IEF", "TLT")
DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD = 60.0
DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD = 45.0
DEFAULT_EXTERNAL_CAPE_THRESHOLD = 45.0
DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD = 0.35
EXTERNAL_VALUATION_MODE_OFF = "off"
EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL = "price_or_external"
EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL = "price_and_external"
EXTERNAL_VALUATION_MODE_EXTERNAL_ONLY = "external_only"
EXTERNAL_VALUATION_MODES = (
    EXTERNAL_VALUATION_MODE_OFF,
    EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL,
    EXTERNAL_VALUATION_MODE_EXTERNAL_ONLY,
)
SEVERE_CRISIS_CONTEXT_EXTERNAL_VALUATION = "external_valuation"
SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE = "valuation_bubble"
SEVERE_CRISIS_CONTEXTS = (
    SEVERE_CRISIS_CONTEXT_EXTERNAL_VALUATION,
    SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE,
)
DEFAULT_SEVERE_CRISIS_CONTEXT = SEVERE_CRISIS_CONTEXT_EXTERNAL_VALUATION
FRAGILITY_CONTEXT_EXTERNAL_VALUATION = "external_valuation"
FRAGILITY_CONTEXT_VALUATION_BUBBLE = "valuation_bubble"
FRAGILITY_CONTEXTS = (
    FRAGILITY_CONTEXT_EXTERNAL_VALUATION,
    FRAGILITY_CONTEXT_VALUATION_BUBBLE,
)
DEFAULT_FRAGILITY_CONTEXT = FRAGILITY_CONTEXT_EXTERNAL_VALUATION


def _normalize_close(close: pd.DataFrame) -> pd.DataFrame:
    frame = close.copy().sort_index()
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame.columns = frame.columns.astype(str).str.upper().str.strip()
    return frame


def _apply_confirm_days(signal: pd.Series, confirm_days: int) -> pd.Series:
    raw = pd.Series(signal).fillna(False).astype(bool)
    if int(confirm_days) <= 1:
        return raw
    confirmed = raw.rolling(int(confirm_days), min_periods=int(confirm_days)).sum().ge(int(confirm_days))
    return confirmed.fillna(False).rename(raw.name)


def _series_from_ai_allowed(ai_opinions: pd.DataFrame, index: pd.DatetimeIndex) -> pd.Series:
    output = pd.Series(False, index=index, name="ai_true_crisis_allowed")
    if ai_opinions.empty:
        return output
    opinion_dates = pd.to_datetime(ai_opinions["as_of"]).dt.normalize()
    output.loc[opinion_dates] = ai_opinions["final_context_allowed"].to_numpy(dtype=bool)
    return output


def _series_from_ai_bool_column(
    ai_opinions: pd.DataFrame,
    index: pd.DatetimeIndex,
    column: str,
    *,
    require_final_allowed: bool = True,
) -> pd.Series:
    output = pd.Series(False, index=index, name=str(column))
    if ai_opinions.empty or column not in ai_opinions.columns:
        return output

    frame = ai_opinions.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"])
    if frame.empty:
        return output

    values = frame[column].fillna(False).astype(bool)
    if require_final_allowed and "final_context_allowed" in frame.columns:
        values = values & frame["final_context_allowed"].fillna(False).astype(bool)
    daily_values = pd.Series(values.to_numpy(dtype=bool), index=frame["as_of"]).groupby(level=0).max()
    aligned = daily_values.reindex(output.index).fillna(False).astype(bool)
    output.loc[aligned.index] = aligned.to_numpy(dtype=bool)
    return output


def _series_from_ai_valuation_bubble(ai_opinions: pd.DataFrame, index: pd.DatetimeIndex) -> pd.Series:
    output = pd.Series(False, index=index, name="valuation_bubble_context")
    if ai_opinions.empty:
        return output

    frame = ai_opinions.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"])
    if frame.empty:
        return output

    if "crisis_type" in frame.columns:
        values = frame["crisis_type"].fillna("").astype(str).str.lower().eq("valuation_bubble")
    elif "bubble_context" in frame.columns:
        values = frame["bubble_context"].fillna(False).astype(bool)
    else:
        return output
    if "final_context_allowed" in frame.columns:
        values = values & frame["final_context_allowed"].fillna(False).astype(bool)

    daily_values = pd.Series(values.to_numpy(dtype=bool), index=frame["as_of"]).groupby(level=0).max()
    aligned = daily_values.reindex(output.index).fillna(False).astype(bool)
    output.loc[aligned.index] = aligned.to_numpy(dtype=bool)
    return output


def _series_from_context_features_bool_column(
    context_features: pd.DataFrame,
    index: pd.DatetimeIndex,
    column: str,
) -> pd.Series:
    output = pd.Series(False, index=index, name=str(column))
    if context_features.empty or column not in context_features.columns:
        return output

    frame = context_features.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"])
    if frame.empty:
        return output

    daily_values = pd.Series(
        frame[column].fillna(False).astype(bool).to_numpy(dtype=bool),
        index=frame["as_of"],
    ).groupby(level=0).max()
    aligned = daily_values.reindex(output.index).ffill().fillna(False).astype(bool)
    output.loc[aligned.index] = aligned.to_numpy(dtype=bool)
    return output


def _series_from_context_features_valuation_bubble(
    context_features: pd.DataFrame,
    index: pd.DatetimeIndex,
) -> pd.Series:
    output = pd.Series(False, index=index, name="valuation_bubble_context")
    if context_features.empty:
        return output

    frame = context_features.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"])
    if frame.empty:
        return output

    if "suggested_context_label" in frame.columns:
        values = frame["suggested_context_label"].fillna("").astype(str).str.lower().eq("valuation_bubble")
    elif "bubble_context" in frame.columns:
        values = frame["bubble_context"].fillna(False).astype(bool)
    else:
        return output
    daily_values = pd.Series(values.to_numpy(dtype=bool), index=frame["as_of"]).groupby(level=0).max()
    aligned = daily_values.reindex(output.index).ffill().fillna(False).astype(bool)
    output.loc[aligned.index] = aligned.to_numpy(dtype=bool)
    return output


def _build_bubble_fragility_signal(
    close: pd.DataFrame,
    context_features: pd.DataFrame,
    *,
    index: pd.DatetimeIndex,
    benchmark_symbol: str,
    fragility_context: str,
    drawdown_threshold: float,
    ma_days: int,
    ma_slope_days: int,
    confirm_days: int,
) -> pd.Series:
    output = pd.Series(False, index=index, name="bubble_fragility")
    if context_features.empty:
        return output

    context_name = str(fragility_context).strip().lower()
    if context_name not in FRAGILITY_CONTEXTS:
        raise ValueError(f"Unsupported fragility_context: {fragility_context!r}")
    if context_name == FRAGILITY_CONTEXT_VALUATION_BUBBLE:
        valuation_context = _series_from_context_features_valuation_bubble(context_features, index)
    else:
        valuation_context = _series_from_context_features_bool_column(
            context_features,
            index,
            "external_valuation_context",
        )

    benchmark = pd.to_numeric(close[str(benchmark_symbol).strip().upper()], errors="coerce").reindex(index).ffill()
    high_252 = benchmark.rolling(252, min_periods=63).max()
    drawdown = benchmark / high_252 - 1.0
    ma_window = max(2, int(ma_days))
    slope_window = max(1, int(ma_slope_days))
    ma = benchmark.rolling(ma_window, min_periods=min(63, ma_window)).mean()
    ma_slope = ma.diff(slope_window)
    deteriorating_price = drawdown.le(float(drawdown_threshold)) & (benchmark.lt(ma) | ma_slope.lt(0.0))
    raw = (valuation_context & deteriorating_price.fillna(False)).rename("bubble_fragility")
    return _apply_confirm_days(raw, int(confirm_days)).fillna(False).rename("bubble_fragility")


def _parse_credit_pairs(raw: str | Sequence[str | Sequence[str]]) -> tuple[tuple[str, str], ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    pairs: list[tuple[str, str]] = []
    for value in values:
        parts = value.replace("/", ":").split(":") if isinstance(value, str) else list(value)
        if len(parts) != 2:
            raise ValueError(f"Credit pair must use NUMERATOR:DENOMINATOR syntax: {value!r}")
        numerator = str(parts[0]).strip().upper()
        denominator = str(parts[1]).strip().upper()
        if numerator and denominator and (numerator, denominator) not in pairs:
            pairs.append((numerator, denominator))
    return tuple(pairs)


def _parse_upper_str_tuple(raw: str | Sequence[str]) -> tuple[str, ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    output: list[str] = []
    for value in values:
        text = str(value).strip().upper()
        if text and text not in output:
            output.append(text)
    return tuple(output)


def _build_v2_context_opinions(
    close: pd.DataFrame,
    price_signal: pd.Series,
    *,
    events: Sequence[TradeWarEvent],
    external_context: pd.DataFrame | None,
    start_date: str,
    end_date: str | None,
    benchmark_symbol: str,
    market_symbol: str,
    financial_symbols: Sequence[str],
    credit_pairs: Sequence[tuple[str, str]],
    rate_symbols: Sequence[str],
    external_valuation_mode: str,
    external_trailing_pe_threshold: float,
    external_forward_pe_threshold: float,
    external_cape_threshold: float,
    external_unprofitable_growth_threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    from .crisis_context_research import build_crisis_context_features

    context_features = build_crisis_context_features(
        close,
        events=events,
        external_context=external_context,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        market_symbol=market_symbol,
        financial_symbols=financial_symbols,
        credit_pairs=credit_pairs,
        rate_symbols=rate_symbols,
        external_valuation_mode=external_valuation_mode,
        external_trailing_pe_threshold=float(external_trailing_pe_threshold),
        external_forward_pe_threshold=float(external_forward_pe_threshold),
        external_cape_threshold=float(external_cape_threshold),
        external_unprofitable_growth_threshold=float(external_unprofitable_growth_threshold),
    )
    columns = [
        "strategy",
        "as_of",
        "price_crisis_confirmed",
        "suggested_route",
        "price_bubble_context",
        "external_valuation_context",
        "external_trailing_pe_extreme_context",
        "external_forward_pe_extreme_context",
        "external_cape_extreme_context",
        "external_speculative_quality_context",
        "bubble_context",
        "financial_context",
        "credit_context",
        "financial_system_context",
        "systemic_financial_crisis_context",
        "rate_context",
        "policy_context",
        "exogenous_context",
        "policy_rescue_context",
        "exogenous_policy_rescue_context",
        "proposer_verdict",
        "auditor_verdict",
        "crisis_type",
        "final_context_allowed",
        "confidence",
        "reason",
    ]
    if context_features.empty:
        return pd.DataFrame(columns=columns), context_features

    price = pd.Series(price_signal).fillna(False).astype(bool).copy()
    price.index = pd.to_datetime(price.index).tz_localize(None).normalize()
    features = context_features.copy()
    features["as_of"] = pd.to_datetime(features["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    features = features.dropna(subset=["as_of"]).set_index("as_of").sort_index()
    index = pd.DatetimeIndex(features.index)
    price = price.reindex(index).ffill().fillna(False)
    opinion_index = index[price]

    rows: list[dict[str, object]] = []
    for date in opinion_index:
        feature_row = features.loc[date]
        route = str(feature_row.get("suggested_route", ROUTE_NO_ACTION))
        final_allowed = route == ROUTE_TRUE_CRISIS
        if final_allowed:
            proposer_verdict = "allow_guard"
            auditor_verdict = "approve"
            confidence = 0.84
        elif route == ROUTE_TACO:
            proposer_verdict = "watch_only"
            auditor_verdict = "veto_policy_fake_crisis_context"
            confidence = 0.76
        else:
            proposer_verdict = "watch_only"
            auditor_verdict = "veto_v2_context_route"
            confidence = 0.72
        rows.append(
            {
                "strategy": "unified_crisis_response_v2_context",
                "as_of": pd.Timestamp(date).date().isoformat(),
                "price_crisis_confirmed": True,
                "suggested_route": route,
                "price_bubble_context": bool(feature_row.get("price_bubble_context", False)),
                "external_valuation_context": bool(feature_row.get("external_valuation_context", False)),
                "external_trailing_pe_extreme_context": bool(
                    feature_row.get("external_trailing_pe_extreme_context", False)
                ),
                "external_forward_pe_extreme_context": bool(
                    feature_row.get("external_forward_pe_extreme_context", False)
                ),
                "external_cape_extreme_context": bool(feature_row.get("external_cape_extreme_context", False)),
                "external_speculative_quality_context": bool(
                    feature_row.get("external_speculative_quality_context", False)
                ),
                "bubble_context": bool(feature_row.get("bubble_context", False)),
                "financial_context": bool(feature_row.get("financial_context", False)),
                "credit_context": bool(feature_row.get("credit_context", False)),
                "financial_system_context": bool(feature_row.get("financial_system_context", False)),
                "systemic_financial_crisis_context": bool(
                    feature_row.get("systemic_financial_crisis_context", False)
                ),
                "rate_context": bool(feature_row.get("rate_context", False)),
                "policy_context": bool(feature_row.get("policy_context", False)),
                "exogenous_context": bool(feature_row.get("exogenous_context", False)),
                "policy_rescue_context": bool(feature_row.get("policy_rescue_context", False)),
                "exogenous_policy_rescue_context": bool(
                    feature_row.get("exogenous_policy_rescue_context", False)
                ),
                "proposer_verdict": proposer_verdict,
                "auditor_verdict": auditor_verdict,
                "crisis_type": feature_row.get("suggested_context_label"),
                "final_context_allowed": final_allowed,
                "confidence": confidence,
                "reason": feature_row.get("suggested_reason"),
            }
        )
    return pd.DataFrame(rows, columns=columns), context_features


def build_event_response_decisions(
    recognized_events: Sequence[TradeWarEvent],
    scan_days: pd.Series,
    crisis_signal: pd.Series,
    ai_opinions: pd.DataFrame,
) -> pd.DataFrame:
    scan = pd.Series(scan_days).fillna(False).astype(bool).copy()
    scan.index = pd.to_datetime(scan.index).tz_localize(None).normalize()
    index = pd.DatetimeIndex(scan.index).sort_values()
    crisis = pd.Series(crisis_signal).fillna(False).astype(bool).copy()
    crisis.index = pd.to_datetime(crisis.index).tz_localize(None).normalize()
    crisis = crisis.reindex(index).ffill().fillna(False)

    rows: list[dict[str, object]] = []
    for event in sorted(recognized_events, key=lambda item: item.event_date):
        signal_date = _next_index_date(index, event.event_date)
        if signal_date is None:
            continue
        true_crisis = bool(crisis.loc[signal_date])
        rows.append(
            {
                "source": "taco_event",
                "as_of": signal_date.date().isoformat(),
                "event_id": event.event_id,
                "event_kind": event.kind,
                "title": event.title,
                "route": ROUTE_TRUE_CRISIS if true_crisis else ROUTE_TACO,
                "action": "suppress_taco" if true_crisis else "run_taco",
                "auditor_verdict": "veto_taco_during_true_crisis" if true_crisis else "approve_taco",
                "reason": "TACO candidate overlaps active true-crisis guard"
                if true_crisis
                else "price stress plus policy/trade-war event without true-crisis guard",
            }
        )

    if not ai_opinions.empty:
        for row in ai_opinions.to_dict("records"):
            final_allowed = bool(row.get("final_context_allowed", False))
            rows.append(
                {
                    "source": "crisis_ai",
                    "as_of": row.get("as_of"),
                    "event_id": "",
                    "event_kind": row.get("crisis_type"),
                    "title": row.get("reason"),
                    "route": ROUTE_TRUE_CRISIS if final_allowed else ROUTE_NO_ACTION,
                    "action": "activate_crisis_guard" if final_allowed else "no_action",
                    "auditor_verdict": row.get("auditor_verdict"),
                    "reason": row.get("reason"),
                }
            )
    return pd.DataFrame(rows)


def _filter_taco_events_from_decisions(
    recognized_events: Sequence[TradeWarEvent],
    decisions: pd.DataFrame,
) -> tuple[TradeWarEvent, ...]:
    if decisions.empty:
        return tuple(recognized_events)
    approved_ids = set(
        decisions.loc[
            decisions["source"].eq("taco_event") & decisions["action"].eq("run_taco"),
            "event_id",
        ].astype(str)
    )
    return tuple(event for event in recognized_events if event.event_id in approved_ids)


def _add_unified_response_returns(
    *,
    scenario_prefix: str,
    taco_result: Mapping[str, object],
    returns: pd.DataFrame,
    base_weights: pd.DataFrame,
    crisis_weights: pd.DataFrame,
    crisis_signal: pd.Series,
    fragility_weights: pd.DataFrame | None = None,
    fragility_signal: pd.Series | None = None,
    returns_by_strategy: dict[str, pd.Series],
    weights_by_strategy: dict[str, pd.DataFrame],
    trades_by_strategy: dict[str, pd.DataFrame],
    index: pd.DatetimeIndex,
    overlay_sleeve_ratios: Sequence[float],
    attack_symbol: str,
    safe_symbol: str,
    cash_symbol: str,
    turnover_cost_bps: float,
) -> None:
    overlay_weights = taco_result["weights_history"].copy()
    if not isinstance(overlay_weights, pd.DataFrame):
        raise TypeError("taco_result['weights_history'] must be a DataFrame")
    overlay_weights.index = pd.to_datetime(overlay_weights.index).tz_localize(None).normalize()
    overlay_weights = overlay_weights.reindex(index[:-1]).ffill().fillna({cash_symbol: 1.0}).fillna(0.0)
    if attack_symbol not in overlay_weights.columns:
        overlay_weights[attack_symbol] = 0.0
    if cash_symbol not in overlay_weights.columns:
        overlay_weights[cash_symbol] = 1.0 - overlay_weights[attack_symbol]

    crisis = pd.Series(crisis_signal).fillna(False).astype(bool).copy()
    crisis.index = pd.to_datetime(crisis.index).tz_localize(None).normalize()
    crisis = crisis.reindex(index[:-1]).ffill().fillna(False)

    if fragility_signal is None or fragility_weights is None:
        fragility = pd.Series(False, index=index[:-1], name="bubble_fragility")
        fragility_weights = base_weights
    else:
        fragility = pd.Series(fragility_signal).fillna(False).astype(bool).copy()
        fragility.index = pd.to_datetime(fragility.index).tz_localize(None).normalize()
        fragility = fragility.reindex(index[:-1]).ffill().fillna(False)
        fragility_weights = fragility_weights.reindex(index[:-1]).ffill().fillna(0.0)

    for sleeve_ratio in overlay_sleeve_ratios:
        strategy_name = f"{scenario_prefix}_{int(float(sleeve_ratio) * 100)}pct"
        rows: list[dict[str, object]] = []
        for date in index[:-1]:
            if bool(crisis.loc[date]):
                weights = crisis_weights.loc[date].to_dict()
            elif bool(fragility.loc[date]):
                weights = fragility_weights.loc[date].to_dict()
            else:
                weights = _integrate_overlay_weights(
                    base_weights.loc[date].to_dict(),
                    overlay_weights.loc[date].to_dict(),
                    sleeve_ratio=float(sleeve_ratio),
                    attack_symbol=attack_symbol,
                    safe_symbol=safe_symbol,
                    cash_symbol=cash_symbol,
                )
            rows.append({"as_of": date, **weights})
        combined_weights = pd.DataFrame(rows).set_index("as_of").reindex(index[:-1]).fillna(0.0)
        strategy_returns, strategy_weights = _weights_to_returns(
            returns,
            combined_weights,
            strategy_name=strategy_name,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_strategy[strategy_name] = strategy_returns
        weights_by_strategy[strategy_name] = strategy_weights
        trades = taco_result.get("trades")
        if isinstance(trades, pd.DataFrame):
            trades_by_strategy[strategy_name] = trades


def run_crisis_response_research(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    overlay_sleeve_ratios: Sequence[float] = DEFAULT_OVERLAY_SLEEVE_RATIOS,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    safe_symbol: str = DEFAULT_RESPONSE_SAFE_SYMBOL,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    synthetic_attack_from: str | None = DEFAULT_BENCHMARK_SYMBOL,
    synthetic_attack_multiple: float = DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
    synthetic_attack_expense_rate: float = DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    crisis_drawdown: float = DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    crisis_risk_multiplier: float = DEFAULT_RESPONSE_CRISIS_RISK_MULTIPLIER,
    severe_crisis_risk_multiplier: float | None = None,
    severe_crisis_context: str = DEFAULT_SEVERE_CRISIS_CONTEXT,
    bubble_fragility_risk_multiplier: float | None = None,
    bubble_fragility_context: str = DEFAULT_FRAGILITY_CONTEXT,
    bubble_fragility_drawdown: float = DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN,
    bubble_fragility_ma_days: int = DEFAULT_BUBBLE_FRAGILITY_MA_DAYS,
    bubble_fragility_ma_slope_days: int = DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS,
    bubble_fragility_confirm_days: int = DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS,
    crisis_confirm_days: int = DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS,
    ma_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    ma_slope_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    financial_symbol: str = DEFAULT_FINANCIAL_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    crisis_context_mode: str = CRISIS_CONTEXT_MODE_V1_AI_RUBRIC,
    external_context: pd.DataFrame | None = None,
    context_financial_symbols: Sequence[str] = DEFAULT_CONTEXT_FINANCIAL_SYMBOLS,
    context_credit_pairs: Sequence[tuple[str, str]] = DEFAULT_CONTEXT_CREDIT_PAIRS,
    context_rate_symbols: Sequence[str] = DEFAULT_CONTEXT_RATE_SYMBOLS,
    external_valuation_mode: str = EXTERNAL_VALUATION_MODE_OFF,
    external_trailing_pe_threshold: float = DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD,
    external_forward_pe_threshold: float = DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD,
    external_cape_threshold: float = DEFAULT_EXTERNAL_CAPE_THRESHOLD,
    external_unprofitable_growth_threshold: float = DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    close = price_history_to_close_matrix(price_history)
    close = _normalize_close(close)
    if float(synthetic_attack_multiple) > 0.0:
        close = add_synthetic_attack_close(
            close,
            attack_symbol=attack_symbol,
            source_symbol=synthetic_attack_from or benchmark_symbol,
            multiple=float(synthetic_attack_multiple),
            annual_expense_rate=float(synthetic_attack_expense_rate),
        )
    if end_date is not None:
        close = close.loc[close.index <= pd.Timestamp(end_date).normalize()].copy()
    index = close.index[close.index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough trading days for crisis response research")

    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    comparison_price_history = close_matrix_to_price_history(close)

    scan_days = build_price_stress_scan(
        close,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
    )
    recognized_events = filter_events_by_price_stress(events, scan_days)

    raw_crisis_signal = build_price_crisis_guard_signal(
        close,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        drawdown_threshold=float(crisis_drawdown),
        ma_days=int(ma_days),
        ma_slope_days=int(ma_slope_days),
    )
    confirmed_crisis_signal = _apply_confirm_days(raw_crisis_signal, int(crisis_confirm_days))
    crisis_context_mode = str(crisis_context_mode).strip().lower()
    if crisis_context_mode not in CRISIS_CONTEXT_MODES:
        raise ValueError(f"Unsupported crisis_context_mode: {crisis_context_mode!r}")
    if crisis_context_mode == CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK:
        ai_opinions, crisis_context_features = _build_v2_context_opinions(
            close,
            confirmed_crisis_signal,
            events=events,
            external_context=external_context,
            start_date=start_date,
            end_date=end_date,
            benchmark_symbol=benchmark_symbol,
            market_symbol=market_symbol,
            financial_symbols=_parse_upper_str_tuple(context_financial_symbols),
            credit_pairs=_parse_credit_pairs(context_credit_pairs),
            rate_symbols=_parse_upper_str_tuple(context_rate_symbols),
            external_valuation_mode=external_valuation_mode,
            external_trailing_pe_threshold=float(external_trailing_pe_threshold),
            external_forward_pe_threshold=float(external_forward_pe_threshold),
            external_cape_threshold=float(external_cape_threshold),
            external_unprofitable_growth_threshold=float(external_unprofitable_growth_threshold),
        )
    else:
        ai_opinions = build_ai_crisis_opinions(
            close,
            confirmed_crisis_signal,
            strategy_name="unified_crisis_response_ai",
            start_date=start_date,
            end_date=end_date,
            benchmark_symbol=benchmark_symbol,
            financial_symbol=financial_symbol,
            market_symbol=market_symbol,
            trigger_only=True,
        )
        crisis_context_features = pd.DataFrame()
    ai_context = _series_from_ai_allowed(ai_opinions, confirmed_crisis_signal.index)
    true_crisis_signal = apply_context_gate_to_signal(confirmed_crisis_signal, ai_context)
    severe_crisis_context = str(severe_crisis_context).strip().lower()
    if severe_crisis_context not in SEVERE_CRISIS_CONTEXTS:
        raise ValueError(f"Unsupported severe_crisis_context: {severe_crisis_context!r}")
    if severe_crisis_risk_multiplier is None:
        severe_crisis_signal = pd.Series(False, index=true_crisis_signal.index, name="severe_crisis")
    else:
        if severe_crisis_context == SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE:
            severe_context = _series_from_ai_valuation_bubble(ai_opinions, confirmed_crisis_signal.index)
        else:
            severe_context = _series_from_ai_bool_column(
                ai_opinions,
                confirmed_crisis_signal.index,
                "external_valuation_context",
                require_final_allowed=True,
            )
        severe_crisis_signal = apply_context_gate_to_signal(true_crisis_signal, severe_context).rename("severe_crisis")

    if bubble_fragility_risk_multiplier is None:
        bubble_fragility_signal = pd.Series(False, index=true_crisis_signal.index, name="bubble_fragility")
    else:
        bubble_fragility_signal = _build_bubble_fragility_signal(
            close,
            crisis_context_features,
            index=true_crisis_signal.index,
            benchmark_symbol=benchmark_symbol,
            fragility_context=bubble_fragility_context,
            drawdown_threshold=float(bubble_fragility_drawdown),
            ma_days=int(bubble_fragility_ma_days),
            ma_slope_days=int(bubble_fragility_ma_slope_days),
            confirm_days=int(bubble_fragility_confirm_days),
        )

    decisions = build_event_response_decisions(
        recognized_events,
        scan_days,
        true_crisis_signal,
        ai_opinions,
    )
    taco_events = _filter_taco_events_from_decisions(recognized_events, decisions)

    taco_only_result = _run_taco_overlay_backtest(
        comparison_price_history,
        recognized_events=recognized_events,
        start_date=start_date,
        end_date=end_date,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )
    unified_taco_result = _run_taco_overlay_backtest(
        comparison_price_history,
        recognized_events=taco_events,
        start_date=start_date,
        end_date=end_date,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    returns = close.pct_change(fill_method=None).fillna(0.0).reindex(index).fillna(0.0)
    for symbol in {benchmark_symbol, attack_symbol, safe_symbol}:
        if symbol not in returns.columns:
            returns[symbol] = 0.0

    base_weights = build_tqqq_growth_income_base_weights(
        close,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
    ).reindex(index[:-1]).ffill().fillna(0.0)
    base_returns, base_weights_history = _weights_to_returns(
        returns,
        base_weights,
        strategy_name="base",
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )
    crisis_weights = apply_price_crisis_guard_to_weights(
        base_weights,
        true_crisis_signal,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        risk_multiplier=float(crisis_risk_multiplier),
    ).reindex(index[:-1]).ffill().fillna(0.0)
    if severe_crisis_risk_multiplier is not None and bool(severe_crisis_signal.any()):
        severe_weights = apply_price_crisis_guard_to_weights(
            base_weights,
            severe_crisis_signal,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            risk_multiplier=float(severe_crisis_risk_multiplier),
        ).reindex(index[:-1]).ffill().fillna(0.0)
        severe_mask = severe_crisis_signal.reindex(index[:-1]).ffill().fillna(False).astype(bool)
        crisis_weights.loc[severe_mask] = severe_weights.loc[severe_mask]
    fragility_weights = None
    if bubble_fragility_risk_multiplier is not None and bool(bubble_fragility_signal.any()):
        fragility_weights = apply_price_crisis_guard_to_weights(
            base_weights,
            bubble_fragility_signal,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            risk_multiplier=float(bubble_fragility_risk_multiplier),
        ).reindex(index[:-1]).ffill().fillna(0.0)
    crisis_returns, crisis_weights_history = _weights_to_returns(
        returns,
        crisis_weights,
        strategy_name="true_crisis_guard_base",
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    returns_by_strategy: dict[str, pd.Series] = {
        "base": base_returns,
        "true_crisis_guard_base": crisis_returns,
    }
    weights_by_strategy: dict[str, pd.DataFrame] = {
        "base": base_weights_history,
        "true_crisis_guard_base": crisis_weights_history,
    }
    if fragility_weights is not None:
        fragility_returns, fragility_weights_history = _weights_to_returns(
            returns,
            fragility_weights,
            strategy_name="bubble_fragility_guard_base",
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_strategy["bubble_fragility_guard_base"] = fragility_returns
        weights_by_strategy["bubble_fragility_guard_base"] = fragility_weights_history
    trades_by_strategy: dict[str, pd.DataFrame] = {}
    _add_overlay_strategy_returns(
        scenario_prefix="taco_only",
        taco_result=taco_only_result,
        returns=returns,
        base_weights=base_weights,
        returns_by_strategy=returns_by_strategy,
        weights_by_strategy=weights_by_strategy,
        trades_by_strategy=trades_by_strategy,
        index=index,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )
    _add_unified_response_returns(
        scenario_prefix="unified_response",
        taco_result=unified_taco_result,
        returns=returns,
        base_weights=base_weights,
        crisis_weights=crisis_weights,
        crisis_signal=true_crisis_signal,
        fragility_weights=fragility_weights,
        fragility_signal=bubble_fragility_signal,
        returns_by_strategy=returns_by_strategy,
        weights_by_strategy=weights_by_strategy,
        trades_by_strategy=trades_by_strategy,
        index=index,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    summary = build_period_summary(returns_by_strategy, trades_by_strategy=trades_by_strategy)
    deltas = build_deltas_vs_base(summary)
    diagnostics = build_diagnostics(scan_days=scan_days, recognized_events=recognized_events, trades=taco_only_result["trades"])
    crisis_diagnostics = build_crisis_guard_diagnostics(true_crisis_signal)
    return {
        "summary": summary,
        "deltas_vs_base": deltas,
        "diagnostics": diagnostics,
        "crisis_guard_diagnostics": crisis_diagnostics,
        "response_decisions": decisions,
        "ai_opinions": ai_opinions,
        "crisis_context_features": crisis_context_features,
        "scan_days": scan_days,
        "confirmed_crisis_signal": confirmed_crisis_signal,
        "true_crisis_signal": true_crisis_signal,
        "severe_crisis_signal": severe_crisis_signal,
        "bubble_fragility_signal": bubble_fragility_signal,
        "recognized_events": events_to_frame(recognized_events),
        "taco_events": events_to_frame(taco_events),
        "returns_by_strategy": returns_by_strategy,
        "weights_by_strategy": weights_by_strategy,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research unified TACO-vs-true-crisis response variants.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument(
        "--external-context",
        default=None,
        help="Optional point-in-time context CSV with an as_of column for V2 context research",
    )
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--download-proxy", default=None, help="Optional yfinance proxy URL; YFINANCE_PROXY also works")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--overlay-sleeve-ratios", default=",".join(str(value) for value in DEFAULT_OVERLAY_SLEEVE_RATIOS))
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--attack-symbol", default=DEFAULT_ATTACK_SYMBOL)
    parser.add_argument("--safe-symbol", default=DEFAULT_RESPONSE_SAFE_SYMBOL)
    parser.add_argument("--synthetic-attack-from", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--synthetic-attack-multiple", type=float, default=DEFAULT_SYNTHETIC_ATTACK_MULTIPLE)
    parser.add_argument("--synthetic-attack-expense-rate", type=float, default=DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE)
    parser.add_argument("--crisis-drawdown", type=float, default=DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN)
    parser.add_argument("--crisis-risk-multiplier", type=float, default=DEFAULT_RESPONSE_CRISIS_RISK_MULTIPLIER)
    parser.add_argument(
        "--severe-crisis-risk-multiplier",
        type=float,
        default=None,
        help=(
            "Optional research-only risk multiplier for selected severe true-crisis days; "
            "disabled by default"
        ),
    )
    parser.add_argument(
        "--severe-crisis-context",
        choices=SEVERE_CRISIS_CONTEXTS,
        default=DEFAULT_SEVERE_CRISIS_CONTEXT,
        help="Context subset eligible for --severe-crisis-risk-multiplier",
    )
    parser.add_argument(
        "--bubble-fragility-risk-multiplier",
        type=float,
        default=None,
        help="Optional research-only pre-crisis risk multiplier for valuation-bubble fragility days",
    )
    parser.add_argument(
        "--bubble-fragility-context",
        choices=FRAGILITY_CONTEXTS,
        default=DEFAULT_FRAGILITY_CONTEXT,
        help="Valuation context subset eligible for the bubble fragility pre-crisis guard",
    )
    parser.add_argument(
        "--bubble-fragility-drawdown",
        type=float,
        default=DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN,
        help="252-day drawdown threshold for the bubble fragility price deterioration gate",
    )
    parser.add_argument("--bubble-fragility-ma-days", type=int, default=DEFAULT_BUBBLE_FRAGILITY_MA_DAYS)
    parser.add_argument(
        "--bubble-fragility-ma-slope-days",
        type=int,
        default=DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS,
    )
    parser.add_argument("--bubble-fragility-confirm-days", type=int, default=DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS)
    parser.add_argument("--crisis-confirm-days", type=int, default=DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS)
    parser.add_argument("--financial-symbol", default=DEFAULT_FINANCIAL_SYMBOL)
    parser.add_argument("--market-symbol", default=DEFAULT_MARKET_SYMBOL)
    parser.add_argument(
        "--crisis-context-mode",
        choices=CRISIS_CONTEXT_MODES,
        default=CRISIS_CONTEXT_MODE_V1_AI_RUBRIC,
        help="Research context used after the confirmed crisis-price scanner opens",
    )
    parser.add_argument("--context-financial-symbols", default=",".join(DEFAULT_CONTEXT_FINANCIAL_SYMBOLS))
    parser.add_argument(
        "--context-credit-pairs",
        default=",".join(f"{numerator}:{denominator}" for numerator, denominator in DEFAULT_CONTEXT_CREDIT_PAIRS),
    )
    parser.add_argument("--context-rate-symbols", default=",".join(DEFAULT_CONTEXT_RATE_SYMBOLS))
    parser.add_argument(
        "--external-valuation-mode",
        choices=EXTERNAL_VALUATION_MODES,
        default=EXTERNAL_VALUATION_MODE_OFF,
        help="How optional external valuation context participates in the V2 research bubble route",
    )
    parser.add_argument("--external-trailing-pe-threshold", type=float, default=DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD)
    parser.add_argument("--external-forward-pe-threshold", type=float, default=DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD)
    parser.add_argument("--external-cape-threshold", type=float, default=DEFAULT_EXTERNAL_CAPE_THRESHOLD)
    parser.add_argument(
        "--external-unprofitable-growth-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD,
    )
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        symbols = [args.benchmark_symbol, args.safe_symbol, args.financial_symbol, args.market_symbol]
        context_financial_symbols = _parse_upper_str_tuple(args.context_financial_symbols)
        context_credit_pairs = _parse_credit_pairs(args.context_credit_pairs)
        context_rate_symbols = _parse_upper_str_tuple(args.context_rate_symbols)
        if args.crisis_context_mode == CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK:
            symbols.extend([*context_financial_symbols, *context_rate_symbols])
            for numerator, denominator in context_credit_pairs:
                symbols.extend([numerator, denominator])
        if float(args.synthetic_attack_multiple) > 0.0:
            symbols.append(args.synthetic_attack_from)
        else:
            symbols.append(args.attack_symbol)
        symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        price_history = download_price_history(
            symbols,
            start=args.price_start,
            end=args.price_end,
            proxy=args.download_proxy,
        )
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "crisis_response_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)
    external_context = read_table(args.external_context) if args.external_context else None

    result = run_crisis_response_research(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        start_date=args.start_date,
        end_date=args.end_date,
        overlay_sleeve_ratios=_parse_float_tuple(args.overlay_sleeve_ratios),
        benchmark_symbol=args.benchmark_symbol,
        attack_symbol=args.attack_symbol,
        safe_symbol=args.safe_symbol,
        synthetic_attack_from=args.synthetic_attack_from,
        synthetic_attack_multiple=float(args.synthetic_attack_multiple),
        synthetic_attack_expense_rate=float(args.synthetic_attack_expense_rate),
        crisis_drawdown=float(args.crisis_drawdown),
        crisis_risk_multiplier=float(args.crisis_risk_multiplier),
        severe_crisis_risk_multiplier=args.severe_crisis_risk_multiplier,
        severe_crisis_context=args.severe_crisis_context,
        bubble_fragility_risk_multiplier=args.bubble_fragility_risk_multiplier,
        bubble_fragility_context=args.bubble_fragility_context,
        bubble_fragility_drawdown=float(args.bubble_fragility_drawdown),
        bubble_fragility_ma_days=int(args.bubble_fragility_ma_days),
        bubble_fragility_ma_slope_days=int(args.bubble_fragility_ma_slope_days),
        bubble_fragility_confirm_days=int(args.bubble_fragility_confirm_days),
        crisis_confirm_days=int(args.crisis_confirm_days),
        financial_symbol=args.financial_symbol,
        market_symbol=args.market_symbol,
        crisis_context_mode=args.crisis_context_mode,
        external_context=external_context,
        context_financial_symbols=_parse_upper_str_tuple(args.context_financial_symbols),
        context_credit_pairs=_parse_credit_pairs(args.context_credit_pairs),
        context_rate_symbols=_parse_upper_str_tuple(args.context_rate_symbols),
        external_valuation_mode=args.external_valuation_mode,
        external_trailing_pe_threshold=float(args.external_trailing_pe_threshold),
        external_forward_pe_threshold=float(args.external_forward_pe_threshold),
        external_cape_threshold=float(args.external_cape_threshold),
        external_unprofitable_growth_threshold=float(args.external_unprofitable_growth_threshold),
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    print("\nSummary:")
    print(_format_percent_columns(result["summary"]).to_string(index=False))
    print("\nDeltas vs base:")
    print(_format_percent_columns(result["deltas_vs_base"]).to_string(index=False))
    print("\nResponse decision counts:")
    decisions = result["response_decisions"]
    if isinstance(decisions, pd.DataFrame) and not decisions.empty:
        print(decisions.groupby(["source", "route", "action"]).size().reset_index(name="count").to_string(index=False))

    result["summary"].to_csv(output_dir / "summary.csv", index=False)
    result["deltas_vs_base"].to_csv(output_dir / "deltas_vs_base.csv", index=False)
    result["diagnostics"].to_csv(output_dir / "diagnostics.csv", index=False)
    result["crisis_guard_diagnostics"].to_csv(output_dir / "crisis_guard_diagnostics.csv", index=False)
    result["response_decisions"].to_csv(output_dir / "response_decisions.csv", index=False)
    result["ai_opinions"].to_csv(output_dir / "ai_opinions.csv", index=False)
    context_features = result["crisis_context_features"]
    if isinstance(context_features, pd.DataFrame) and not context_features.empty:
        context_features.to_csv(output_dir / "crisis_context_features.csv", index=False)
    result["recognized_events"].to_csv(output_dir / "recognized_event_calendar.csv", index=False)
    result["taco_events"].to_csv(output_dir / "taco_event_calendar.csv", index=False)
    result["scan_days"].rename("price_stress_scan").to_csv(output_dir / "price_stress_scan_days.csv")
    result["confirmed_crisis_signal"].rename("confirmed_crisis").to_csv(output_dir / "confirmed_crisis_signal.csv")
    result["true_crisis_signal"].rename("true_crisis").to_csv(output_dir / "true_crisis_signal.csv")
    result["severe_crisis_signal"].rename("severe_crisis").to_csv(output_dir / "severe_crisis_signal.csv")
    result["bubble_fragility_signal"].rename("bubble_fragility").to_csv(output_dir / "bubble_fragility_signal.csv")
    returns_dir = output_dir / "returns"
    weights_dir = output_dir / "weights"
    returns_dir.mkdir(exist_ok=True)
    weights_dir.mkdir(exist_ok=True)
    for strategy, returns in result["returns_by_strategy"].items():
        returns.rename("return").to_csv(returns_dir / f"{strategy}.csv")
    for strategy, weights in result["weights_by_strategy"].items():
        weights.to_csv(weights_dir / f"{strategy}.csv")
    print(f"wrote crisis response research outputs -> {output_dir}")
    return 0


__all__ = [
    "CRISIS_CONTEXT_MODE_V1_AI_RUBRIC",
    "CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK",
    "CRISIS_CONTEXT_MODES",
    "EXTERNAL_VALUATION_MODE_EXTERNAL_ONLY",
    "EXTERNAL_VALUATION_MODE_OFF",
    "EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL",
    "EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL",
    "EXTERNAL_VALUATION_MODES",
    "FRAGILITY_CONTEXT_EXTERNAL_VALUATION",
    "FRAGILITY_CONTEXT_VALUATION_BUBBLE",
    "FRAGILITY_CONTEXTS",
    "ROUTE_NO_ACTION",
    "ROUTE_TACO",
    "ROUTE_TRUE_CRISIS",
    "SEVERE_CRISIS_CONTEXT_EXTERNAL_VALUATION",
    "SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE",
    "SEVERE_CRISIS_CONTEXTS",
    "build_event_response_decisions",
    "main",
    "run_crisis_response_research",
]
