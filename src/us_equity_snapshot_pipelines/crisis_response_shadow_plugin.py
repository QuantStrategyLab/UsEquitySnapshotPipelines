from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .artifacts import write_json
from .crisis_context_research import (
    CONTEXT_LABEL_FINANCIAL_CRISIS,
    CONTEXT_LABEL_NORMAL,
    CONTEXT_LABEL_POLICY_SHOCK,
    CONTEXT_LABEL_RATE_BEAR,
    CONTEXT_LABEL_VALUATION_BUBBLE,
    DEFAULT_MARKET_SYMBOL,
    EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    EXTERNAL_VALUATION_MODES,
    build_crisis_context_features,
)
from .crisis_response_research import (
    DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS,
    DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN,
    DEFAULT_BUBBLE_FRAGILITY_MA_DAYS,
    DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS,
    DEFAULT_CONTEXT_CREDIT_PAIRS,
    DEFAULT_CONTEXT_FINANCIAL_SYMBOLS,
    DEFAULT_CONTEXT_RATE_SYMBOLS,
    DEFAULT_EXTERNAL_ADVANCE_DECLINE_DRAWDOWN_THRESHOLD,
    DEFAULT_EXTERNAL_CAPE_THRESHOLD,
    DEFAULT_EXTERNAL_EARNINGS_REVISION_3M_THRESHOLD,
    DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD,
    DEFAULT_EXTERNAL_MARGIN_REVISION_3M_THRESHOLD,
    DEFAULT_EXTERNAL_NEGATIVE_EARNINGS_SHARE_THRESHOLD,
    DEFAULT_EXTERNAL_NEW_HIGH_NEW_LOW_SPREAD_THRESHOLD,
    DEFAULT_EXTERNAL_PCT_ABOVE_50D_THRESHOLD,
    DEFAULT_EXTERNAL_PCT_ABOVE_200D_THRESHOLD,
    DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD,
    DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD,
    DEFAULT_FRAGILITY_CONTEXT,
    DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS,
    DEFAULT_START_DATE,
    DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
    ROUTE_NO_ACTION,
    ROUTE_TRUE_CRISIS,
    SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE,
    _apply_confirm_days,
    _build_bubble_fragility_signal,
    _parse_credit_pairs,
    _parse_upper_str_tuple,
)
from .plugin_signal_utils import bool_at, flatten_for_csv, json_scalar, normalize_close, resolve_signal_date
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_overlay_compare import (
    add_synthetic_attack_close,
    build_price_crisis_guard_signal,
)
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
    TRADE_WAR_EVENT_SETS,
    TradeWarEvent,
    resolve_trade_war_event_set,
)
from .yfinance_prices import download_price_history

SCHEMA_VERSION = "crisis_response_shadow.v1"
SHADOW_MODE = "shadow"
SHADOW_PROFILE = "crisis_response_shadow"
DEFAULT_BENCHMARK_SYMBOL = "QQQ"
DEFAULT_ATTACK_SYMBOL = "TQQQ"
DEFAULT_OUTPUT_DIR = "data/output/crisis_response_shadow"
DEFAULT_MAX_PRICE_AGE_DAYS = 4
DEFAULT_MAX_EXTERNAL_CONTEXT_AGE_DAYS = 45
DEFAULT_SHADOW_CRISIS_RISK_MULTIPLIER = 0.0
DEFAULT_SHADOW_SEVERE_CRISIS_RISK_MULTIPLIER = 0.0
DEFAULT_SHADOW_BUBBLE_FRAGILITY_RISK_MULTIPLIER = 0.0

ACTION_BLOCKED = "blocked"
ACTION_DEFEND = "defend"
ACTION_NO_ACTION = "no_action"
ACTION_WATCH_ONLY = "watch_only"


def _feature_row_at(context_features: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    if context_features.empty:
        return pd.Series(dtype=object)
    frame = context_features.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"]).set_index("as_of").sort_index()
    if frame.empty:
        return pd.Series(dtype=object)
    aligned = frame.reindex(frame.index.union(pd.DatetimeIndex([date]))).sort_index().ffill()
    if date not in aligned.index:
        return pd.Series(dtype=object)
    return aligned.loc[date]


def _field_bool(row: pd.Series, field: str) -> bool:
    if row.empty or field not in row:
        return False
    value = row.get(field, False)
    return False if pd.isna(value) else bool(value)


def _field_float(row: pd.Series, field: str) -> float | None:
    if row.empty or field not in row:
        return None
    value = pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0]
    return None if pd.isna(value) else float(value)


def _latest_external_as_of(external_context: pd.DataFrame | None, signal_date: pd.Timestamp) -> pd.Timestamp | None:
    if external_context is None or external_context.empty or "as_of" not in external_context.columns:
        return None
    dates = pd.to_datetime(external_context["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    dates = dates.dropna()
    dates = dates.loc[dates <= signal_date]
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def _route_watch_label(feature_row: pd.Series, bubble_fragility_active: bool) -> str:
    if bubble_fragility_active:
        return "valuation_fragility"
    label = str(feature_row.get("suggested_context_label", CONTEXT_LABEL_NORMAL) if not feature_row.empty else "")
    if label == CONTEXT_LABEL_FINANCIAL_CRISIS:
        return "systemic_stress_watch"
    if label == CONTEXT_LABEL_RATE_BEAR:
        return "rate_bear"
    if label == CONTEXT_LABEL_POLICY_SHOCK:
        return "policy_shock_watch"
    if label == CONTEXT_LABEL_VALUATION_BUBBLE:
        return "valuation_bubble_watch"
    return "" if label == CONTEXT_LABEL_NORMAL else label


def _build_evidence(feature_row: pd.Series) -> dict[str, Any]:
    valuation_context = (
        _field_bool(feature_row, "bubble_context")
        or _field_bool(feature_row, "external_valuation_context")
        or str(feature_row.get("suggested_context_label", "")) == CONTEXT_LABEL_VALUATION_BUBBLE
    )
    return {
        "valuation_context": bool(valuation_context),
        "breadth_quality_context": _field_bool(feature_row, "external_breadth_or_quality_context"),
        "financial_context": _field_bool(feature_row, "financial_context"),
        "credit_context": _field_bool(feature_row, "credit_context"),
        "combined_financial_credit_context": _field_bool(feature_row, "combined_financial_credit_context"),
        "rate_context": _field_bool(feature_row, "rate_context"),
        "policy_context": _field_bool(feature_row, "policy_context"),
        "exogenous_context": _field_bool(feature_row, "exogenous_context"),
        "policy_rescue_context": _field_bool(feature_row, "policy_rescue_context"),
        "metrics": {
            "qqq_drawdown_252d": _field_float(feature_row, "QQQ_drawdown_252d"),
            "financial_drawdown_min_252d": _field_float(feature_row, "financial_drawdown_min_252d"),
            "financial_relative_return_min_126d": _field_float(feature_row, "financial_relative_return_min_126d"),
            "credit_relative_return_min_63d": _field_float(feature_row, "credit_relative_return_min_63d"),
            "rate_proxy_return_min_126d": _field_float(feature_row, "rate_proxy_return_min_126d"),
            "external_trailing_pe": _field_float(feature_row, "external_nasdaq_100_trailing_pe"),
            "external_forward_pe": _field_float(feature_row, "external_nasdaq_100_forward_pe"),
            "external_cape_proxy": _field_float(feature_row, "external_nasdaq_100_cape_proxy"),
            "external_pct_above_200d": _field_float(feature_row, "external_nasdaq_100_pct_above_200d"),
            "external_pct_above_50d": _field_float(feature_row, "external_nasdaq_100_pct_above_50d"),
            "policy_event_ids": str(feature_row.get("policy_event_ids", "") if not feature_row.empty else ""),
            "exogenous_event_ids": str(feature_row.get("exogenous_event_ids", "") if not feature_row.empty else ""),
            "policy_rescue_event_ids": str(
                feature_row.get("policy_rescue_event_ids", "") if not feature_row.empty else ""
            ),
        },
    }


def build_crisis_response_shadow_signal(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = (),
    external_context: pd.DataFrame | None = None,
    as_of: str | None = None,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    financial_symbols: Sequence[str] = DEFAULT_CONTEXT_FINANCIAL_SYMBOLS,
    credit_pairs: Sequence[tuple[str, str]] = DEFAULT_CONTEXT_CREDIT_PAIRS,
    rate_symbols: Sequence[str] = DEFAULT_CONTEXT_RATE_SYMBOLS,
    synthetic_attack_from: str | None = None,
    synthetic_attack_multiple: float = DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
    synthetic_attack_expense_rate: float = DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    crisis_drawdown: float = DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    crisis_confirm_days: int = DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS,
    crisis_risk_multiplier: float = DEFAULT_SHADOW_CRISIS_RISK_MULTIPLIER,
    severe_crisis_context: str = SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE,
    severe_crisis_risk_multiplier: float = DEFAULT_SHADOW_SEVERE_CRISIS_RISK_MULTIPLIER,
    bubble_fragility_context: str = DEFAULT_FRAGILITY_CONTEXT,
    bubble_fragility_drawdown: float = DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN,
    bubble_fragility_ma_days: int = DEFAULT_BUBBLE_FRAGILITY_MA_DAYS,
    bubble_fragility_ma_slope_days: int = DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS,
    bubble_fragility_confirm_days: int = DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS,
    bubble_fragility_risk_multiplier: float = DEFAULT_SHADOW_BUBBLE_FRAGILITY_RISK_MULTIPLIER,
    ma_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    ma_slope_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    external_valuation_mode: str = EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    external_trailing_pe_threshold: float = DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD,
    external_forward_pe_threshold: float = DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD,
    external_cape_threshold: float = DEFAULT_EXTERNAL_CAPE_THRESHOLD,
    external_unprofitable_growth_threshold: float = DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD,
    external_pct_above_200d_threshold: float = DEFAULT_EXTERNAL_PCT_ABOVE_200D_THRESHOLD,
    external_pct_above_50d_threshold: float = DEFAULT_EXTERNAL_PCT_ABOVE_50D_THRESHOLD,
    external_new_high_new_low_spread_threshold: float = DEFAULT_EXTERNAL_NEW_HIGH_NEW_LOW_SPREAD_THRESHOLD,
    external_advance_decline_drawdown_threshold: float = DEFAULT_EXTERNAL_ADVANCE_DECLINE_DRAWDOWN_THRESHOLD,
    external_negative_earnings_share_threshold: float = DEFAULT_EXTERNAL_NEGATIVE_EARNINGS_SHARE_THRESHOLD,
    external_earnings_revision_3m_threshold: float = DEFAULT_EXTERNAL_EARNINGS_REVISION_3M_THRESHOLD,
    external_margin_revision_3m_threshold: float = DEFAULT_EXTERNAL_MARGIN_REVISION_3M_THRESHOLD,
    max_price_age_days: int = DEFAULT_MAX_PRICE_AGE_DAYS,
    max_external_context_age_days: int = DEFAULT_MAX_EXTERNAL_CONTEXT_AGE_DAYS,
) -> dict[str, Any]:
    close = normalize_close(price_history)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    market_symbol = str(market_symbol).strip().upper()
    financial_symbols = _parse_upper_str_tuple(financial_symbols)
    credit_pairs = _parse_credit_pairs(credit_pairs)
    rate_symbols = _parse_upper_str_tuple(rate_symbols)

    if float(synthetic_attack_multiple) > 0.0 and attack_symbol not in close.columns:
        close = add_synthetic_attack_close(
            close,
            attack_symbol=attack_symbol,
            source_symbol=synthetic_attack_from or benchmark_symbol,
            multiple=float(synthetic_attack_multiple),
            annual_expense_rate=float(synthetic_attack_expense_rate),
        )
    if end_date is not None:
        close = close.loc[close.index <= pd.Timestamp(end_date).tz_localize(None).normalize()].copy()
    requested_date, signal_date = resolve_signal_date(close, as_of)
    signal_iso = signal_date.date().isoformat()
    latest_price_date = pd.Timestamp(close.index.max()).normalize()
    price_age_days = int((requested_date - signal_date).days)

    kill_reasons: list[str] = []
    if benchmark_symbol not in close.columns:
        kill_reasons.append(f"missing benchmark price data: {benchmark_symbol}")
    if price_age_days > int(max_price_age_days):
        kill_reasons.append(
            f"price data stale: signal_as_of={signal_iso}, requested_as_of={requested_date.date().isoformat()}"
        )

    price_scanner_active = False
    bubble_fragility_active = False
    context_features = pd.DataFrame()
    feature_row = pd.Series(dtype=object)
    if benchmark_symbol in close.columns:
        raw_crisis = build_price_crisis_guard_signal(
            close,
            start_date=start_date,
            end_date=signal_iso,
            benchmark_symbol=benchmark_symbol,
            drawdown_threshold=float(crisis_drawdown),
            ma_days=int(ma_days),
            ma_slope_days=int(ma_slope_days),
        )
        confirmed_crisis = _apply_confirm_days(raw_crisis, int(crisis_confirm_days))
        price_scanner_active = bool_at(confirmed_crisis, signal_date)
    if benchmark_symbol in close.columns:
        context_features = build_crisis_context_features(
            close,
            events=events,
            external_context=external_context,
            start_date=start_date,
            end_date=signal_iso,
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
            external_pct_above_200d_threshold=float(external_pct_above_200d_threshold),
            external_pct_above_50d_threshold=float(external_pct_above_50d_threshold),
            external_new_high_new_low_spread_threshold=float(external_new_high_new_low_spread_threshold),
            external_advance_decline_drawdown_threshold=float(external_advance_decline_drawdown_threshold),
            external_negative_earnings_share_threshold=float(external_negative_earnings_share_threshold),
            external_earnings_revision_3m_threshold=float(external_earnings_revision_3m_threshold),
            external_margin_revision_3m_threshold=float(external_margin_revision_3m_threshold),
        )
        feature_row = _feature_row_at(context_features, signal_date)
        bubble_fragility_signal = _build_bubble_fragility_signal(
            close,
            context_features,
            index=pd.DatetimeIndex(close.index[close.index >= pd.Timestamp(start_date).normalize()]),
            benchmark_symbol=benchmark_symbol,
            fragility_context=bubble_fragility_context,
            drawdown_threshold=float(bubble_fragility_drawdown),
            ma_days=int(bubble_fragility_ma_days),
            ma_slope_days=int(bubble_fragility_ma_slope_days),
            confirm_days=int(bubble_fragility_confirm_days),
        )
        bubble_fragility_active = bool_at(bubble_fragility_signal, signal_date)

    raw_proposer_route = str(
        feature_row.get("suggested_route", ROUTE_NO_ACTION) if not feature_row.empty else ROUTE_NO_ACTION
    )
    proposer_route = ROUTE_TRUE_CRISIS if raw_proposer_route == ROUTE_TRUE_CRISIS else ROUTE_NO_ACTION
    proposer_label = str(
        feature_row.get("suggested_context_label", CONTEXT_LABEL_NORMAL)
        if not feature_row.empty
        else CONTEXT_LABEL_NORMAL
    )
    non_crisis_context_active = raw_proposer_route != ROUTE_NO_ACTION or proposer_label != CONTEXT_LABEL_NORMAL
    proposer_reason = str(
        feature_row.get("suggested_reason", "no context features available") if not feature_row.empty else ""
    )

    external_as_of = _latest_external_as_of(external_context, signal_date)
    external_age_days = int((signal_date - external_as_of).days) if external_as_of is not None else None
    external_dependency_active = (
        _field_bool(feature_row, "external_valuation_context")
        or _field_bool(feature_row, "external_confirmed_bubble_fragility_context")
        or (
            bubble_fragility_active
            and str(bubble_fragility_context).strip().lower() == str(DEFAULT_FRAGILITY_CONTEXT).strip().lower()
        )
    )
    if external_dependency_active and external_as_of is None:
        kill_reasons.append("external context missing for external valuation or fragility route")
    if (
        external_dependency_active
        and external_age_days is not None
        and external_age_days > int(max_external_context_age_days)
    ):
        kill_reasons.append(
            f"external context stale: external_context_as_of={external_as_of.date().isoformat()}"
        )

    canonical_route = ROUTE_NO_ACTION
    suggested_action = ACTION_NO_ACTION
    risk_multiplier_suggestion: float | None = None
    would_trade_if_enabled = False
    watch_label = _route_watch_label(feature_row, bubble_fragility_active)

    if bubble_fragility_active:
        canonical_route = ROUTE_TRUE_CRISIS
        suggested_action = ACTION_DEFEND
        risk_multiplier_suggestion = float(bubble_fragility_risk_multiplier)
        would_trade_if_enabled = True
    elif price_scanner_active and proposer_route == ROUTE_TRUE_CRISIS:
        canonical_route = ROUTE_TRUE_CRISIS
        suggested_action = ACTION_DEFEND
        severe_context_active = proposer_label == CONTEXT_LABEL_VALUATION_BUBBLE and (
            str(severe_crisis_context).strip().lower() == SEVERE_CRISIS_CONTEXT_VALUATION_BUBBLE
        )
        risk_multiplier_suggestion = (
            float(severe_crisis_risk_multiplier) if severe_context_active else float(crisis_risk_multiplier)
        )
        would_trade_if_enabled = True
    elif non_crisis_context_active or price_scanner_active:
        canonical_route = ROUTE_NO_ACTION
        suggested_action = ACTION_WATCH_ONLY
        risk_multiplier_suggestion = None
        would_trade_if_enabled = False

    kill_switch_active = bool(kill_reasons)
    if kill_switch_active:
        canonical_route = ROUTE_NO_ACTION
        suggested_action = ACTION_BLOCKED
        risk_multiplier_suggestion = None
        would_trade_if_enabled = False

    evidence = _build_evidence(feature_row)
    generated_at = datetime.now(timezone.utc).isoformat()
    data_freshness = {
        "requested_as_of": requested_date.date().isoformat(),
        "signal_as_of": signal_iso,
        "prices_as_of": latest_price_date.date().isoformat(),
        "price_age_days": price_age_days,
        "max_price_age_days": int(max_price_age_days),
        "external_context_as_of": external_as_of.date().isoformat() if external_as_of is not None else None,
        "external_context_age_days": external_age_days,
        "max_external_context_age_days": int(max_external_context_age_days),
        "events_as_of": signal_iso,
    }
    auditor_verdict = "block_kill_switch" if kill_switch_active else "approve_shadow_route"
    if not kill_switch_active and suggested_action == ACTION_WATCH_ONLY:
        auditor_verdict = "approve_watch_only"
    elif not kill_switch_active and suggested_action == ACTION_NO_ACTION:
        auditor_verdict = "approve_no_action"

    return json_scalar(
        {
            "as_of": signal_iso,
            "mode": SHADOW_MODE,
            "schema_version": SCHEMA_VERSION,
            "profile": SHADOW_PROFILE,
            "canonical_route": canonical_route,
            "watch_label": (
                ""
                if canonical_route != ROUTE_NO_ACTION and watch_label == "valuation_bubble_watch"
                else watch_label
            ),
            "suggested_action": suggested_action,
            "risk_multiplier_suggestion": risk_multiplier_suggestion,
            "would_trade_if_enabled": would_trade_if_enabled,
            "price_scanner_active": price_scanner_active,
            "bubble_fragility_active": bubble_fragility_active,
            "kill_switch_active": kill_switch_active,
            "kill_switch_reason": "; ".join(kill_reasons),
            "data_freshness": data_freshness,
            "evidence": evidence,
            "audit_summary": {
                "proposer_route": proposer_route,
                "proposer_context_label": proposer_label,
                "auditor_verdict": auditor_verdict,
                "final_route": canonical_route,
                "reason": proposer_reason,
            },
            "execution_controls": {
                "capital_impact": "none",
                "broker_order_allowed": False,
                "live_allocation_mutation_allowed": False,
                "log_namespace": SHADOW_PROFILE,
                "notification_profile": "shadow_only",
                "intended_strategy_role": "black_swan_defense",
                "defensive_destination": "cash_or_money_market",
            },
            "generated_at": generated_at,
        }
    )


def write_crisis_response_shadow_outputs(payload: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
    output_root = Path(output_dir)
    signal_date = str(payload["as_of"])
    signal_dir = output_root / "signals"
    audit_dir = output_root / "audit"
    latest_path = output_root / "latest_signal.json"
    dated_json_path = signal_dir / f"{signal_date}.json"
    dated_csv_path = signal_dir / f"{signal_date}.csv"
    evidence_csv_path = audit_dir / f"{signal_date}_evidence.csv"

    write_json(latest_path, payload)
    write_json(dated_json_path, payload)
    signal_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([flatten_for_csv(payload)]).to_csv(dated_csv_path, index=False)

    evidence_payload = {
        "as_of": payload.get("as_of"),
        "canonical_route": payload.get("canonical_route"),
        "suggested_action": payload.get("suggested_action"),
        "watch_label": payload.get("watch_label"),
        **flatten_for_csv(payload.get("data_freshness", {})),
        **flatten_for_csv(payload.get("evidence", {})),
        **flatten_for_csv(payload.get("audit_summary", {})),
    }
    pd.DataFrame([evidence_payload]).to_csv(evidence_csv_path, index=False)
    return {
        "latest_signal": latest_path,
        "signal_json": dated_json_path,
        "signal_csv": dated_csv_path,
        "evidence_csv": evidence_csv_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the log-only Crisis Response shadow signal.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--mode", choices=(SHADOW_MODE,), default=SHADOW_MODE)
    parser.add_argument(
        "--external-context",
        default=None,
        help="Optional point-in-time context CSV with an as_of column",
    )
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--as-of", default=None, help="Requested signal date; defaults to the latest price date")
    parser.add_argument("--price-start", default=DEFAULT_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--download-proxy", default=None, help="Optional yfinance proxy URL; YFINANCE_PROXY also works")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--attack-symbol", default=DEFAULT_ATTACK_SYMBOL)
    parser.add_argument("--market-symbol", default=DEFAULT_MARKET_SYMBOL)
    parser.add_argument("--financial-symbols", default=",".join(DEFAULT_CONTEXT_FINANCIAL_SYMBOLS))
    parser.add_argument(
        "--credit-pairs",
        default=",".join(f"{numerator}:{denominator}" for numerator, denominator in DEFAULT_CONTEXT_CREDIT_PAIRS),
    )
    parser.add_argument("--rate-symbols", default=",".join(DEFAULT_CONTEXT_RATE_SYMBOLS))
    parser.add_argument("--synthetic-attack-from", default=None)
    parser.add_argument("--synthetic-attack-multiple", type=float, default=DEFAULT_SYNTHETIC_ATTACK_MULTIPLE)
    parser.add_argument(
        "--synthetic-attack-expense-rate",
        type=float,
        default=DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    )
    parser.add_argument("--crisis-drawdown", type=float, default=DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN)
    parser.add_argument("--crisis-confirm-days", type=int, default=DEFAULT_RESPONSE_CRISIS_CONFIRM_DAYS)
    parser.add_argument("--crisis-risk-multiplier", type=float, default=DEFAULT_SHADOW_CRISIS_RISK_MULTIPLIER)
    parser.add_argument(
        "--severe-crisis-risk-multiplier",
        type=float,
        default=DEFAULT_SHADOW_SEVERE_CRISIS_RISK_MULTIPLIER,
    )
    parser.add_argument(
        "--bubble-fragility-risk-multiplier",
        type=float,
        default=DEFAULT_SHADOW_BUBBLE_FRAGILITY_RISK_MULTIPLIER,
    )
    parser.add_argument("--bubble-fragility-drawdown", type=float, default=DEFAULT_BUBBLE_FRAGILITY_DRAWDOWN)
    parser.add_argument("--bubble-fragility-ma-days", type=int, default=DEFAULT_BUBBLE_FRAGILITY_MA_DAYS)
    parser.add_argument("--bubble-fragility-ma-slope-days", type=int, default=DEFAULT_BUBBLE_FRAGILITY_MA_SLOPE_DAYS)
    parser.add_argument("--bubble-fragility-confirm-days", type=int, default=DEFAULT_BUBBLE_FRAGILITY_CONFIRM_DAYS)
    parser.add_argument(
        "--external-valuation-mode",
        choices=EXTERNAL_VALUATION_MODES,
        default=EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    )
    parser.add_argument("--external-trailing-pe-threshold", type=float, default=DEFAULT_EXTERNAL_TRAILING_PE_THRESHOLD)
    parser.add_argument("--external-forward-pe-threshold", type=float, default=DEFAULT_EXTERNAL_FORWARD_PE_THRESHOLD)
    parser.add_argument("--external-cape-threshold", type=float, default=DEFAULT_EXTERNAL_CAPE_THRESHOLD)
    parser.add_argument(
        "--external-unprofitable-growth-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_UNPROFITABLE_GROWTH_THRESHOLD,
    )
    parser.add_argument(
        "--external-pct-above-200d-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_PCT_ABOVE_200D_THRESHOLD,
    )
    parser.add_argument(
        "--external-pct-above-50d-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_PCT_ABOVE_50D_THRESHOLD,
    )
    parser.add_argument(
        "--external-new-high-new-low-spread-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_NEW_HIGH_NEW_LOW_SPREAD_THRESHOLD,
    )
    parser.add_argument(
        "--external-advance-decline-drawdown-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_ADVANCE_DECLINE_DRAWDOWN_THRESHOLD,
    )
    parser.add_argument(
        "--external-negative-earnings-share-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_NEGATIVE_EARNINGS_SHARE_THRESHOLD,
    )
    parser.add_argument(
        "--external-earnings-revision-3m-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_EARNINGS_REVISION_3M_THRESHOLD,
    )
    parser.add_argument(
        "--external-margin-revision-3m-threshold",
        type=float,
        default=DEFAULT_EXTERNAL_MARGIN_REVISION_3M_THRESHOLD,
    )
    parser.add_argument("--max-price-age-days", type=int, default=DEFAULT_MAX_PRICE_AGE_DAYS)
    parser.add_argument("--max-external-context-age-days", type=int, default=DEFAULT_MAX_EXTERNAL_CONTEXT_AGE_DAYS)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    financial_symbols = _parse_upper_str_tuple(args.financial_symbols)
    credit_pairs = _parse_credit_pairs(args.credit_pairs)
    rate_symbols = _parse_upper_str_tuple(args.rate_symbols)

    if args.download:
        symbols = [
            args.benchmark_symbol,
            args.attack_symbol,
            args.market_symbol,
            *financial_symbols,
            *rate_symbols,
        ]
        for numerator, denominator in credit_pairs:
            symbols.extend([numerator, denominator])
        symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        price_history = download_price_history(
            symbols,
            start=args.price_start,
            end=args.price_end,
            proxy=args.download_proxy,
        )
        input_dir = Path(args.output_dir) / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "crisis_response_shadow_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    external_context = read_table(args.external_context) if args.external_context else None
    payload = build_crisis_response_shadow_signal(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        external_context=external_context,
        as_of=args.as_of,
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        attack_symbol=args.attack_symbol,
        market_symbol=args.market_symbol,
        financial_symbols=financial_symbols,
        credit_pairs=credit_pairs,
        rate_symbols=rate_symbols,
        synthetic_attack_from=args.synthetic_attack_from,
        synthetic_attack_multiple=float(args.synthetic_attack_multiple),
        synthetic_attack_expense_rate=float(args.synthetic_attack_expense_rate),
        crisis_drawdown=float(args.crisis_drawdown),
        crisis_confirm_days=int(args.crisis_confirm_days),
        crisis_risk_multiplier=float(args.crisis_risk_multiplier),
        severe_crisis_risk_multiplier=float(args.severe_crisis_risk_multiplier),
        bubble_fragility_risk_multiplier=float(args.bubble_fragility_risk_multiplier),
        bubble_fragility_drawdown=float(args.bubble_fragility_drawdown),
        bubble_fragility_ma_days=int(args.bubble_fragility_ma_days),
        bubble_fragility_ma_slope_days=int(args.bubble_fragility_ma_slope_days),
        bubble_fragility_confirm_days=int(args.bubble_fragility_confirm_days),
        external_valuation_mode=args.external_valuation_mode,
        external_trailing_pe_threshold=float(args.external_trailing_pe_threshold),
        external_forward_pe_threshold=float(args.external_forward_pe_threshold),
        external_cape_threshold=float(args.external_cape_threshold),
        external_unprofitable_growth_threshold=float(args.external_unprofitable_growth_threshold),
        external_pct_above_200d_threshold=float(args.external_pct_above_200d_threshold),
        external_pct_above_50d_threshold=float(args.external_pct_above_50d_threshold),
        external_new_high_new_low_spread_threshold=float(args.external_new_high_new_low_spread_threshold),
        external_advance_decline_drawdown_threshold=float(args.external_advance_decline_drawdown_threshold),
        external_negative_earnings_share_threshold=float(args.external_negative_earnings_share_threshold),
        external_earnings_revision_3m_threshold=float(args.external_earnings_revision_3m_threshold),
        external_margin_revision_3m_threshold=float(args.external_margin_revision_3m_threshold),
        max_price_age_days=int(args.max_price_age_days),
        max_external_context_age_days=int(args.max_external_context_age_days),
    )
    paths = write_crisis_response_shadow_outputs(payload, args.output_dir)
    print(
        "wrote crisis response shadow signal "
        f"{payload['as_of']} route={payload['canonical_route']} action={payload['suggested_action']} "
        f"-> {paths['latest_signal']}"
    )
    return 0


__all__ = [
    "SCHEMA_VERSION",
    "SHADOW_MODE",
    "SHADOW_PROFILE",
    "build_crisis_response_shadow_signal",
    "write_crisis_response_shadow_outputs",
]
