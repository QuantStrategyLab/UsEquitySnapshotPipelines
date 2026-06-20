"""Publish high-signal strategy plugin alerts from workflow artifacts."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from quant_platform_kit.common.notification_localization import STRATEGY_PLUGIN_I18N
from quant_platform_kit.common.strategy_plugins import (
    StrategyPluginAlertMessage,
    build_strategy_plugin_alert_key,
    load_strategy_plugin_signal,
)
from quant_platform_kit.notifications import (
    strategy_plugin_email,
    strategy_plugin_push,
    strategy_plugin_sms,
    strategy_plugin_telegram,
)
from quant_platform_kit.notifications.strategy_plugin_alerts import (
    StrategyPluginAlertStateSettings,
    publish_strategy_plugin_alerts,
)


HIGH_VALUE_UNIFIED_MARKET_REGIME_ROUTES = frozenset(
    {
        "true_crisis",
        "crisis",
        "risk_off",
        "opportunity_watch",
        "panic_reversal",
        "taco_rebound",
    }
)
LOW_VALUE_UNIFIED_MARKET_REGIME_ROUTES = frozenset(
    {
        "no_action",
        "watch",
        "risk_reduced",
        "delever",
        "blocked",
    }
)

UNIFIED_MARKET_REGIME_ALERT_I18N: dict[str, dict[str, str]] = {
    "zh": {
        "strategy_plugin_alert_context_strategy_plugin_publish": "插件发布 / {target}",
    },
    "en": {
        "strategy_plugin_alert_context_strategy_plugin_publish": "plugin publish / {target}",
    },
}


@dataclass(frozen=True)
class UnifiedMarketRegimeAlertGateDecision:
    should_publish: bool
    reason: str
    route: str
    action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "should_publish": self.should_publish,
            "reason": self.reason,
            "canonical_route": self.route,
            "suggested_action": self.action,
        }


def _normalize_signal_field(value: object) -> str:
    return str(value or "").strip().lower() or "unknown"


def _sequence_has_items(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and bool(value)


def _opportunity_vetoed_by_risk(signal: object) -> bool:
    payload = _as_mapping(getattr(signal, "payload", None))
    notification = _as_mapping(payload.get("notification"))
    position_control = _as_mapping(payload.get("position_control"))
    if _as_bool(notification.get("opportunity_vetoed_should_notify")):
        return True
    if _sequence_has_items(notification.get("vetoed_opportunities")):
        return True
    vetoes = (*_iter_reason_codes(notification.get("vetoes")), *_iter_reason_codes(position_control.get("vetoes")))
    return any("blocks_taco" in veto or "blocks_panic_reversal" in veto for veto in vetoes)


def evaluate_unified_market_regime_alert_gate(signal: object) -> UnifiedMarketRegimeAlertGateDecision:
    """Keep unified market-regime alerts sparse and high-signal."""

    route = _normalize_signal_field(getattr(signal, "canonical_route", None))
    action = _normalize_signal_field(getattr(signal, "suggested_action", None))
    if route in HIGH_VALUE_UNIFIED_MARKET_REGIME_ROUTES:
        return UnifiedMarketRegimeAlertGateDecision(
            should_publish=True,
            reason="high_value_market_event",
            route=route,
            action=action,
        )
    if route in {"risk_reduced", "delever"} and _opportunity_vetoed_by_risk(signal):
        return UnifiedMarketRegimeAlertGateDecision(
            should_publish=True,
            reason="opportunity_vetoed_by_risk",
            route=route,
            action=action,
        )
    if route in LOW_VALUE_UNIFIED_MARKET_REGIME_ROUTES:
        return UnifiedMarketRegimeAlertGateDecision(
            should_publish=False,
            reason="low_priority_market_state",
            route=route,
            action=action,
        )
    return UnifiedMarketRegimeAlertGateDecision(
        should_publish=False,
        reason="unrecognized_market_state",
        route=route,
        action=action,
    )


def _has_panic_reversal_signal(signal: object) -> bool:
    payload = _as_mapping(getattr(signal, "payload", None))
    panic = _as_mapping(_as_mapping(payload.get("component_signals")).get("panic_reversal"))
    return _as_bool(panic.get("manual_review_required")) or _as_bool(panic.get("panic_reversal_context_active"))


def _alert_priority(signal: object, decision: UnifiedMarketRegimeAlertGateDecision) -> int:
    if not decision.should_publish:
        return -1
    if decision.route in {"true_crisis", "crisis", "risk_off"}:
        return 400
    if decision.reason == "opportunity_vetoed_by_risk":
        return 300
    if decision.route == "panic_reversal" or _has_panic_reversal_signal(signal):
        return 220
    if decision.route in {"opportunity_watch", "taco_rebound"}:
        return 200
    return 100


def _select_unified_market_regime_alert_signals(
    signals: Sequence[object],
    decisions: Sequence[UnifiedMarketRegimeAlertGateDecision],
) -> tuple[object, ...]:
    ranked = [
        (index, _alert_priority(signal, decision), signal)
        for index, (signal, decision) in enumerate(zip(signals, decisions))
        if decision.should_publish
    ]
    if not ranked:
        return ()
    _index, _priority, signal = max(ranked, key=lambda item: (item[1], -item[0]))
    return (signal,)


def build_unified_market_regime_alert_translator(lang: str | None) -> Callable[..., str]:
    normalized = str(lang or "zh").strip().lower()
    locale = "zh" if normalized.startswith("zh") else "en"
    table = {
        **STRATEGY_PLUGIN_I18N[locale],
        **UNIFIED_MARKET_REGIME_ALERT_I18N[locale],
    }

    def translate(key: str, **kwargs: Any) -> str:
        template = table.get(key, key)
        return template.format(**kwargs) if kwargs else template

    return translate


def _translator_uses_zh(translator: Callable[..., str] | None) -> bool:
    if translator is None:
        return True
    try:
        locale = str(translator("strategy_plugin_alert_locale"))
    except Exception:
        locale = ""
    if locale.strip().lower().startswith("zh"):
        return True
    return any("\u4e00" <= char <= "\u9fff" for char in locale)


def _market_status_label(route: str, *, use_zh: bool) -> str:
    if route in {"true_crisis", "crisis", "risk_off"}:
        return "黑天鹅" if use_zh else "black swan risk"
    if route in {"opportunity_watch", "panic_reversal", "taco_rebound"}:
        return "抄底机会" if use_zh else "dip-buy opportunity"
    if route in {"risk_reduced", "delever"}:
        return "机会被否决" if use_zh else "opportunity vetoed"
    return "需要复核" if use_zh else "manual review"


def _as_mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_scalar_value(value: object) -> bool:
    return isinstance(value, (str, int, float, bool))


def _strip_terminal_punctuation(value: str) -> str:
    return value.strip().rstrip("。.!；; ")


def _strip_background_lead_in(value: str) -> str:
    text = value.strip()
    for prefix in (
        "当时可见的风险线索是",
        "当时可见的机会线索是",
        "当时可见的线索是",
        "当时可见线索是",
        "可见风险线索是",
        "可见机会线索是",
        "可见线索是",
        "Visible risk evidence:",
        "Visible opportunity evidence:",
        "Visible evidence:",
    ):
        if text.startswith(prefix):
            return text[len(prefix) :].lstrip(" ：:，,")
    return text


def _shorten_text(value: str, *, max_chars: int = 96) -> str:
    text = _strip_background_lead_in(_strip_terminal_punctuation(value))
    if len(text) <= max_chars:
        return text
    for delimiter in ("。", "；", ";", "."):
        head = _strip_terminal_punctuation(text.split(delimiter, 1)[0])
        if 8 <= len(head) <= max_chars:
            return head
    return text[: max_chars - 1].rstrip() + "…"


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_ratio(value: float) -> str:
    return f"{value * 100:.0f}%" if abs(value) in {0.0, 1.0} else f"{value * 100:.1f}%"


def _metric_label(key: str, *, use_zh: bool) -> str:
    labels_zh = {
        "risk_budget_scalar": "风险预算",
        "leverage_scalar": "杠杆",
        "risk_asset_scalar": "风险资产",
        "taco_size_scalar": "TACO机会仓位",
        "panic_reversal_size_scalar": "恐慌反转仓位",
        "crisis_defense_required": "危机防守",
        "local_delever_veto_allowed": "机会可覆盖本地降杠杆",
        "hard_risk": "极端风险",
        "soft_risk": "风险降档",
        "rebound_confirm": "反弹确认",
        "price_rebound_candidate": "价格反弹候选",
        "macro_watch": "宏观观察",
        "actionable_score": "可执行评分",
        "total_score": "总分",
        "risk_multiplier_suggestion": "风险倍数建议",
        "manual_review_required": "需要人工复核",
        "rebound_context_active": "反弹背景",
        "event_context_active": "事件背景",
        "panic_reversal_context_active": "恐慌反转背景",
        "price_crisis_guard_active": "价格危机保护",
        "confirmed": "确认",
        "reason": "原因",
        "lookback_days": "回看天数",
        "trading_days_after_event": "事件后交易日",
        "min_trading_days_after_event": "最少事件后交易日",
        "benchmark_symbol": "基准",
        "attack_symbol": "进攻标的",
        "benchmark_rebound_from_recent_low": "基准低点反弹",
        "attack_rebound_from_recent_low": "进攻标的低点反弹",
        "benchmark_3d_return": "基准3日收益",
        "min_benchmark_rebound_from_low": "基准低点反弹阈值",
        "min_attack_rebound_from_low": "进攻标的低点反弹阈值",
        "min_benchmark_3d_return": "基准3日收益阈值",
        "vix": "VIX",
        "vix_previous": "VIX前值",
        "vix_lookback_high": "VIX回看高点",
        "vix_pullback_from_high": "VIX高点回落",
        "vix3m": "VIX3M",
        "vix_vix3m_ratio": "VIX/VIX3M",
        "quality_score": "质量分",
        "price_age_days": "价格延迟天数",
        "max_price_age_days": "价格最大延迟",
        "external_context_age_days": "外部背景延迟天数",
        "max_external_context_age_days": "外部背景最大延迟",
    }
    if use_zh:
        return labels_zh.get(key, key)
    return key.replace("_", " ")


def _component_label(key: str, *, use_zh: bool) -> str:
    labels_zh = {
        "crisis": "危机",
        "macro": "宏观",
        "taco": "TACO",
        "panic_reversal": "恐慌反转",
    }
    return labels_zh.get(key, key) if use_zh else key.replace("_", " ")


def _format_metric_value(key: str, value: object, *, use_zh: bool) -> str:
    if isinstance(value, bool):
        return ("是" if value else "否") if use_zh else ("yes" if value else "no")
    if isinstance(value, (int, float)):
        number = float(value)
        if any(token in key for token in ("scalar", "ratio", "budget", "retention")):
            return _format_ratio(number)
        if any(token in key for token in ("return", "drawdown", "volatility", "rebound")) and abs(number) <= 2:
            return _format_ratio(number)
        return _format_number(number)
    return str(value).strip()


def _format_metric_item(label: str, key: str, value: object, *, use_zh: bool) -> str:
    return f"{label}{_metric_label(key, use_zh=use_zh)}={_format_metric_value(key, value, use_zh=use_zh)}"


def _append_scalar_metrics(
    items: list[str],
    mapping: Mapping[str, Any],
    keys: Sequence[str],
    *,
    prefix: str = "",
    use_zh: bool,
    max_items: int,
) -> None:
    for key in keys:
        if len(items) >= max_items:
            return
        value = mapping.get(key)
        if value in (None, "", (), []):
            continue
        if _is_scalar_value(value):
            items.append(_format_metric_item(prefix, key, value, use_zh=use_zh))


def _append_nested_metrics(
    items: list[str],
    mapping: Mapping[str, Any],
    keys: Sequence[str],
    *,
    prefix: str = "",
    use_zh: bool,
    max_items: int,
) -> None:
    for key in keys:
        nested = mapping.get(key)
        if not isinstance(nested, Mapping):
            continue
        for nested_key, value in nested.items():
            if len(items) >= max_items:
                return
            if value in (None, "", (), []):
                continue
            if _is_scalar_value(value):
                items.append(_format_metric_item(prefix, str(nested_key), value, use_zh=use_zh))


def _component_metric_order(route: str) -> tuple[str, ...]:
    if route == "panic_reversal":
        return ("panic_reversal", "taco", "crisis", "macro")
    if route in {"opportunity_watch", "taco_rebound"}:
        return ("taco", "panic_reversal", "crisis", "macro")
    if route in {"true_crisis", "crisis", "risk_off"}:
        return ("crisis", "macro", "panic_reversal", "taco")
    return ("crisis", "macro", "taco", "panic_reversal")


def _indicator_summary(signal: object, *, use_zh: bool, max_items: int = 16) -> str:
    payload = _as_mapping(getattr(signal, "payload", None))
    position_control = _as_mapping(payload.get("position_control"))
    volatility_context = _as_mapping(position_control.get("volatility_delever_context"))
    route = _normalize_signal_field(getattr(signal, "canonical_route", None))
    items: list[str] = []
    _append_scalar_metrics(
        items,
        position_control,
        (
            "risk_budget_scalar",
            "leverage_scalar",
            "risk_asset_scalar",
            "taco_size_scalar",
            "panic_reversal_size_scalar",
            "crisis_defense_required",
        ),
        use_zh=use_zh,
        max_items=max_items,
    )
    _append_scalar_metrics(
        items,
        volatility_context,
        (
            "hard_risk",
            "soft_risk",
            "rebound_confirm",
            "price_rebound_candidate",
            "macro_watch",
        ),
        use_zh=use_zh,
        max_items=max_items,
    )

    component_signals = _as_mapping(payload.get("component_signals"))
    ordered_components = tuple(
        dict.fromkeys((*_component_metric_order(route), *(str(component) for component in component_signals)))
    )
    for component in ordered_components:
        if len(items) >= max_items:
            break
        component_payload = component_signals.get(component)
        component_mapping = _as_mapping(component_payload)
        if not _as_bool(component_mapping.get("available")):
            continue
        label = f"{_component_label(str(component), use_zh=use_zh)}."
        _append_scalar_metrics(
            items,
            component_mapping,
            (
                "actionable_score",
                "total_score",
                "leverage_scalar",
                "risk_asset_scalar",
                "risk_multiplier_suggestion",
                "manual_review_required",
                "rebound_context_active",
                "event_context_active",
                "panic_reversal_context_active",
                "price_crisis_guard_active",
            ),
            prefix=label,
            use_zh=use_zh,
            max_items=max_items,
        )
        _append_nested_metrics(
            items,
            component_mapping,
            (
                "metrics",
                "rebound_confirmation",
                "reversal_confirmation",
                "data_quality",
                "event_quality",
                "panic_reversal_quality",
            ),
            prefix=label,
            use_zh=use_zh,
            max_items=max_items,
        )
    return "；".join(items)


def _ai_analysis_text(signal: object, *, use_zh: bool) -> str:
    payload = _as_mapping(getattr(signal, "payload", None))
    ai_audit = _as_mapping(payload.get("ai_audit"))
    if not ai_audit or not _as_bool(ai_audit.get("enabled")):
        return ""
    status = str(ai_audit.get("status") or "").strip()
    verdict = str(ai_audit.get("verdict") or "").strip()
    assessment = str(ai_audit.get("route_assessment") or "").strip()
    summary = _strip_background_lead_in(_strip_terminal_punctuation(str(ai_audit.get("summary") or "")))
    verdict_labels = {
        "agree": "同意",
        "support": "支持",
        "supported": "支持",
        "review": "需要复核",
        "neutral": "中性",
        "caution": "谨慎",
        "cautious": "谨慎",
        "data_insufficient": "数据不足",
        "oppose": "不支持",
        "opposed": "不支持",
        "reject": "不支持",
    }
    assessment_labels = {
        "consistent": "一致",
        "aligned": "一致",
        "mixed": "有分歧",
        "inconsistent": "不一致",
        "conflict": "不一致",
    }
    if status.lower() == "ok" and (summary or verdict or assessment):
        prefix_parts = []
        if verdict:
            verdict_text = verdict_labels.get(verdict.lower(), verdict) if use_zh else verdict
            prefix_parts.append(f"AI评估{verdict_text}" if use_zh else f"verdict {verdict_text}")
        if assessment:
            assessment_text = assessment_labels.get(assessment.lower(), assessment) if use_zh else assessment
            prefix_parts.append(f"路线{assessment_text}" if use_zh else f"route assessment {assessment_text}")
        prefix = "；".join(prefix_parts) if use_zh else "; ".join(prefix_parts)
        if prefix and summary:
            return f"{prefix}；{summary}" if use_zh else f"{prefix}; {summary}"
        return prefix or summary
    reason = str(ai_audit.get("skip_reason") or ai_audit.get("error") or "").strip()
    if not reason:
        return ""
    return f"AI未完成：{reason}" if use_zh else f"AI unavailable: {reason}"


def _ai_background_text(route: str, signal: object, *, use_zh: bool) -> str:
    payload = _as_mapping(getattr(signal, "payload", None))
    ai_audit = _as_mapping(payload.get("ai_audit"))
    if not ai_audit or not _as_bool(ai_audit.get("enabled")):
        return ""
    status = str(ai_audit.get("status") or "").strip().lower()
    summary = _shorten_text(str(ai_audit.get("summary") or ""))
    if status != "ok" or not summary:
        return ""
    if use_zh:
        return f"{summary}。"
    return f"{summary}."


def _iter_reason_codes(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, Sequence):
        return ()
    return tuple(str(item).strip().lower() for item in value if str(item or "").strip())


def _collect_reason_codes(signal: object) -> tuple[str, ...]:
    payload = _as_mapping(getattr(signal, "payload", None))
    codes: list[str] = []
    for key in ("reason_codes",):
        codes.extend(_iter_reason_codes(payload.get(key)))
    for key in ("audit_summary", "position_control", "notification"):
        codes.extend(_iter_reason_codes(_as_mapping(payload.get(key)).get("reason_codes")))
    for component_payload in _as_mapping(payload.get("component_signals")).values():
        component = _as_mapping(component_payload)
        codes.extend(_iter_reason_codes(component.get("reason_codes")))
        codes.extend(_iter_reason_codes(_as_mapping(component.get("audit_summary")).get("reason_codes")))
    return tuple(dict.fromkeys(code for code in codes if code))


def _has_reason_code(codes: Sequence[str], *needles: str) -> bool:
    return any(any(needle in code for needle in needles) for code in codes)


def _visible_risk_evidence_points(signal: object, *, use_zh: bool) -> tuple[str, ...]:
    payload = _as_mapping(getattr(signal, "payload", None))
    position_control = _as_mapping(payload.get("position_control"))
    volatility_context = _as_mapping(position_control.get("volatility_delever_context"))
    component_signals = _as_mapping(payload.get("component_signals"))
    crisis = _as_mapping(component_signals.get("crisis"))
    macro = _as_mapping(component_signals.get("macro"))
    codes = _collect_reason_codes(signal)
    points: list[str] = []
    if use_zh:
        if _has_reason_code(codes, "credit") or "credit" in str(crisis.get("audit_summary") or "").lower():
            points.append("信用压力在扩大")
        if _has_reason_code(codes, "financial") or "financial" in str(crisis.get("audit_summary") or "").lower():
            points.append("金融板块承压")
        if _has_reason_code(codes, "benchmark_below_ma"):
            points.append("指数跌破中长期趋势")
        if _has_reason_code(codes, "drawdown_crisis", "drawdown_watch"):
            points.append("回撤扩大到危机观察区")
        if _as_bool(position_control.get("crisis_defense_required")) or _as_bool(volatility_context.get("hard_risk")):
            points.append("风险预算被压到防守档")
        if _as_bool(volatility_context.get("macro_watch")) or _as_bool(macro.get("available")):
            points.append("宏观压力没有解除")
        return tuple(dict.fromkeys(points))
    if _has_reason_code(codes, "credit") or "credit" in str(crisis.get("audit_summary") or "").lower():
        points.append("credit stress is widening")
    if _has_reason_code(codes, "financial") or "financial" in str(crisis.get("audit_summary") or "").lower():
        points.append("financials are under pressure")
    if _has_reason_code(codes, "benchmark_below_ma"):
        points.append("the index is below its medium-term trend")
    if _has_reason_code(codes, "drawdown_crisis", "drawdown_watch"):
        points.append("drawdown has reached a crisis-watch zone")
    if _as_bool(position_control.get("crisis_defense_required")) or _as_bool(volatility_context.get("hard_risk")):
        points.append("risk budget has moved to defensive mode")
    if _as_bool(volatility_context.get("macro_watch")) or _as_bool(macro.get("available")):
        points.append("macro pressure has not cleared")
    return tuple(dict.fromkeys(points))


def _visible_opportunity_evidence_points(signal: object, *, use_zh: bool) -> tuple[str, ...]:
    payload = _as_mapping(getattr(signal, "payload", None))
    position_control = _as_mapping(payload.get("position_control"))
    volatility_context = _as_mapping(position_control.get("volatility_delever_context"))
    component_signals = _as_mapping(payload.get("component_signals"))
    taco = _as_mapping(component_signals.get("taco"))
    panic = _as_mapping(component_signals.get("panic_reversal"))
    codes = _collect_reason_codes(signal)
    points: list[str] = []
    if use_zh:
        if _as_bool(taco.get("event_context_active")) or _has_reason_code(codes, "taco"):
            points.append("事件压力有缓和迹象")
        if _as_bool(volatility_context.get("rebound_confirm")) or _has_reason_code(codes, "price_rebound"):
            points.append("价格反弹开始确认")
        if _as_bool(volatility_context.get("price_rebound_candidate")):
            points.append("标的从近期低点修复")
        if _has_reason_code(codes, "vix_panic_reversal"):
            points.append("VIX恐慌波动从高位回落")
        elif _as_bool(panic.get("panic_reversal_context_active")) or _has_reason_code(codes, "panic_reversal"):
            points.append("恐慌波动从高位回落")
        if _as_bool(position_control.get("crisis_defense_required")) or _as_bool(volatility_context.get("hard_risk")):
            points.append("但风险保护还没有完全解除")
        return tuple(dict.fromkeys(points))
    if _as_bool(taco.get("event_context_active")) or _has_reason_code(codes, "taco"):
        points.append("event pressure is easing")
    if _as_bool(volatility_context.get("rebound_confirm")) or _has_reason_code(codes, "price_rebound"):
        points.append("price rebound is starting to confirm")
    if _as_bool(volatility_context.get("price_rebound_candidate")):
        points.append("the asset is recovering from a recent low")
    if _has_reason_code(codes, "vix_panic_reversal"):
        points.append("VIX panic volatility is easing from high levels")
    elif _as_bool(panic.get("panic_reversal_context_active")) or _has_reason_code(codes, "panic_reversal"):
        points.append("panic volatility is easing from high levels")
    if _as_bool(position_control.get("crisis_defense_required")) or _as_bool(volatility_context.get("hard_risk")):
        points.append("risk protection has not fully cleared")
    return tuple(dict.fromkeys(points))


def _deterministic_background_text(route: str, signal: object, *, use_zh: bool) -> str:
    payload = _as_mapping(getattr(signal, "payload", None))
    position_control = _as_mapping(payload.get("position_control"))
    volatility_context = _as_mapping(position_control.get("volatility_delever_context"))
    component_signals = _as_mapping(payload.get("component_signals"))
    taco_component = _as_mapping(component_signals.get("taco"))
    panic_component = _as_mapping(component_signals.get("panic_reversal"))
    crisis_defense = _as_bool(position_control.get("crisis_defense_required"))
    hard_risk = _as_bool(volatility_context.get("hard_risk"))
    soft_risk = _as_bool(volatility_context.get("soft_risk"))
    macro_watch = _as_bool(volatility_context.get("macro_watch"))
    rebound_confirm = _as_bool(volatility_context.get("rebound_confirm"))
    price_rebound_candidate = _as_bool(volatility_context.get("price_rebound_candidate"))
    taco_active = route == "taco_rebound" or _as_bool(taco_component.get("manual_review_required"))
    panic_active = route == "panic_reversal" or _as_bool(panic_component.get("manual_review_required"))

    if use_zh:
        if route in {"risk_reduced", "delever"} and _opportunity_vetoed_by_risk(signal):
            risk_points = _visible_risk_evidence_points(signal, use_zh=use_zh)
            opportunity_points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
            if risk_points and opportunity_points:
                return (
                    f"{'、'.join(opportunity_points[:2])}，但{'、'.join(risk_points[:3])}；"
                    "仲裁先按降风险处理，暂不抄底。"
                )
            if risk_points:
                return f"{'、'.join(risk_points[:3])}；机会信号被风控否决，暂不抄底。"
            return "机会信号被宏观或波动风控否决，暂不抄底。"
        if route in {"true_crisis", "crisis", "risk_off"}:
            points = _visible_risk_evidence_points(signal, use_zh=use_zh)
            if points:
                return f"{'、'.join(points[:4])}，说明流动性和风险偏好正在恶化。"
            if crisis_defense or hard_risk:
                return "风险线索已经进入防守区；反弹即使出现，也先按风险控制处理。"
            if soft_risk or macro_watch:
                return "波动或宏观压力偏高，风险偏好还没有恢复。"
            return "风险信号偏防守，需要人工确认是否降杠杆或清仓。"
        if panic_active:
            points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
            if points:
                return f"{'、'.join(points[:4])}；这只是反转窗口，不是自动买入信号。"
            return "恐慌波动回落、价格开始修复；这只是反转窗口，不是自动买入。"
        if taco_active:
            points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
            if points:
                return f"{'、'.join(points[:4])}；只能按小仓位候选复核。"
            return "事件压力或价格走势开始改善；只能按小仓位候选复核。"
        if rebound_confirm or price_rebound_candidate:
            return "价格开始从低位修复，但还需要确认反弹质量和失效位。"
        return "线索不足以支持自动交易，只适合作为人工复核提醒。"
    if route in {"true_crisis", "crisis", "risk_off"}:
        points = _visible_risk_evidence_points(signal, use_zh=use_zh)
        if points:
            return f"{', '.join(points[:4])}; liquidity and risk appetite are weakening."
        if crisis_defense or hard_risk:
            return "Risk evidence has moved into a defensive zone; treat any rebound as secondary."
        if soft_risk or macro_watch:
            return "Volatility or macro pressure is elevated; risk appetite has not recovered."
        return "Risk evidence is defensive; confirm whether de-risking or exiting is needed."
    if route in {"risk_reduced", "delever"} and _opportunity_vetoed_by_risk(signal):
        risk_points = _visible_risk_evidence_points(signal, use_zh=use_zh)
        opportunity_points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
        if risk_points and opportunity_points:
            return (
                f"{', '.join(opportunity_points[:2])}, but {', '.join(risk_points[:3])}; "
                "the arbiter keeps risk reduced and vetoes the dip-buy."
            )
        if risk_points:
            return f"{', '.join(risk_points[:3])}; risk control vetoes the opportunity signal."
        return "Macro or volatility risk control vetoes the opportunity signal."
    if panic_active:
        points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
        if points:
            return f"{', '.join(points[:4])}; this is not an auto-buy signal."
        return "Panic volatility is easing and price is starting to recover; this is not an auto-buy signal."
    if taco_active:
        points = _visible_opportunity_evidence_points(signal, use_zh=use_zh)
        if points:
            return f"{', '.join(points[:4])}; review it as a small-size candidate only."
        return "Event pressure or price action is improving; review it as a small-size candidate only."
    if rebound_confirm or price_rebound_candidate:
        return "Price is starting to recover from lows, but rebound quality and invalidation still need review."
    return "Evidence is not strong enough for automation; treat this as a manual-review reminder."


def _background_text(route: str, signal: object, *, use_zh: bool) -> str:
    return _ai_background_text(route, signal, use_zh=use_zh) or _deterministic_background_text(
        route,
        signal,
        use_zh=use_zh,
    )


def _recommended_action(route: str, *, use_zh: bool) -> str:
    if use_zh:
        if route in {"true_crisis", "crisis", "risk_off"}:
            return "先不要抄底；检查杠杆和风险仓位，必要时按策略结果降杠杆或清仓。"
        if route in {"opportunity_watch", "panic_reversal", "taco_rebound"}:
            return "不要自动买；确认反弹质量、失效位和仓位上限后，再考虑小仓位抄底。"
        if route in {"risk_reduced", "delever"}:
            return "先不要抄底；等宏观或波动压力解除，再重新评估小仓位机会。"
        return "先人工复核，不把这条消息当成下单指令。"
    if route in {"true_crisis", "crisis", "risk_off"}:
        return (
            "Do not buy the dip first; check leverage and risk exposure, then de-lever or exit "
            "if the strategy run confirms it."
        )
    if route in {"opportunity_watch", "panic_reversal", "taco_rebound"}:
        return (
            "Do not auto-buy; confirm rebound quality, invalidation level, and size cap before "
            "any small manual dip-buy."
        )
    if route in {"risk_reduced", "delever"}:
        return "Do not buy the dip yet; wait for macro or volatility pressure to clear before reassessing."
    return "Review manually first; do not treat this message as an order instruction."


def build_unified_market_regime_alert_messages(
    signals,
    *,
    translator: Callable[..., str] | None = None,
    strategy_label: str | None = None,
    context_label: str | None = None,
    alert_namespace: str = "strategy_plugin_alert",
) -> tuple[StrategyPluginAlertMessage, ...]:
    messages: list[StrategyPluginAlertMessage] = []
    use_zh = _translator_uses_zh(translator)
    for signal in signals:
        decision = evaluate_unified_market_regime_alert_gate(signal)
        if not decision.should_publish:
            continue
        as_of = str(getattr(signal, "as_of", None) or ("未知" if use_zh else "unknown")).strip()
        body = "\n".join(
            (
                f"{'日期' if use_zh else 'Date'}：{as_of}",
                f"{'市场状态' if use_zh else 'Market state'}：{_market_status_label(decision.route, use_zh=use_zh)}",
                f"{'背景情况' if use_zh else 'Background'}：{_background_text(decision.route, signal, use_zh=use_zh)}",
                f"{'建议操作' if use_zh else 'Suggested action'}：{_recommended_action(decision.route, use_zh=use_zh)}",
            )
        )
        target_label = (
            str(getattr(signal, "strategy", None) or "").strip()
            or str(getattr(signal, "notification_target", None) or "").strip()
            or str(strategy_label or "").strip()
            or "unknown"
        )
        messages.append(
            StrategyPluginAlertMessage(
                subject="",
                body=body,
                alert_key=build_strategy_plugin_alert_key(
                    signal,
                    strategy_label=target_label,
                    context_label=context_label,
                    namespace=alert_namespace,
                ),
                metadata={
                    "target": target_label,
                    "plugin": getattr(signal, "plugin", None),
                    "as_of": getattr(signal, "as_of", None),
                    "canonical_route": decision.route,
                    "suggested_action": decision.action,
                    "market_status_label": _market_status_label(decision.route, use_zh=use_zh),
                    "indicator_summary": _indicator_summary(signal, use_zh=use_zh),
                    "ai_analysis": _ai_analysis_text(signal, use_zh=use_zh),
                },
            )
        )
    return tuple(messages)


@contextmanager
def _use_unified_market_regime_alert_messages():
    modules = (
        strategy_plugin_email,
        strategy_plugin_push,
        strategy_plugin_sms,
        strategy_plugin_telegram,
    )
    originals = {
        module: module.build_strategy_plugin_alert_messages
        for module in modules
    }
    try:
        for module in modules:
            module.build_strategy_plugin_alert_messages = build_unified_market_regime_alert_messages
        yield
    finally:
        for module, original in originals.items():
            module.build_strategy_plugin_alert_messages = original


def _setting(env: Mapping[str, str], name: str) -> str | None:
    value = env.get(name)
    return value or None


def build_strategy_plugin_alert_settings_from_env(env: Mapping[str, str]) -> SimpleNamespace:
    return SimpleNamespace(
        strategy_plugin_alert_channels=_setting(env, "STRATEGY_PLUGIN_ALERT_CHANNELS") or "telegram",
        strategy_plugin_alert_email_recipients=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_RECIPIENTS"),
        strategy_plugin_alert_email_sender_email=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_EMAIL"),
        strategy_plugin_alert_email_sender_password=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_SENDER_PASSWORD"),
        strategy_plugin_alert_email_smtp_host=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_HOST"),
        strategy_plugin_alert_email_smtp_port=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_PORT"),
        strategy_plugin_alert_email_smtp_security=_setting(env, "STRATEGY_PLUGIN_ALERT_EMAIL_SMTP_SECURITY"),
        strategy_plugin_alert_sms_recipients=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_RECIPIENTS"),
        strategy_plugin_alert_sms_provider=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_PROVIDER"),
        strategy_plugin_alert_sms_account_id=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_ACCOUNT_ID"),
        strategy_plugin_alert_sms_auth_token=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_AUTH_TOKEN"),
        strategy_plugin_alert_sms_sender=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_SENDER"),
        strategy_plugin_alert_sms_messaging_service_id=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_MESSAGING_SERVICE_ID"),
        strategy_plugin_alert_sms_api_base_url=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_API_BASE_URL"),
        strategy_plugin_alert_sms_body_max_chars=_setting(env, "STRATEGY_PLUGIN_ALERT_SMS_BODY_MAX_CHARS"),
        strategy_plugin_alert_push_recipients=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_RECIPIENTS"),
        strategy_plugin_alert_push_provider=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_PROVIDER"),
        strategy_plugin_alert_push_app_token=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_APP_TOKEN"),
        strategy_plugin_alert_push_access_token=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_ACCESS_TOKEN"),
        strategy_plugin_alert_push_api_base_url=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_API_BASE_URL"),
        strategy_plugin_alert_push_device=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_DEVICE"),
        strategy_plugin_alert_push_priority=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_PRIORITY"),
        strategy_plugin_alert_push_tags=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_TAGS"),
        strategy_plugin_alert_push_body_max_chars=_setting(env, "STRATEGY_PLUGIN_ALERT_PUSH_BODY_MAX_CHARS"),
        strategy_plugin_alert_telegram_chat_ids=_setting(env, "STRATEGY_PLUGIN_ALERT_TELEGRAM_CHAT_IDS"),
        strategy_plugin_alert_telegram_bot_token=_setting(env, "STRATEGY_PLUGIN_ALERT_TELEGRAM_BOT_TOKEN"),
        strategy_plugin_alert_telegram_api_base_url=_setting(env, "STRATEGY_PLUGIN_ALERT_TELEGRAM_API_BASE_URL"),
        strategy_plugin_alert_telegram_parse_mode=_setting(env, "STRATEGY_PLUGIN_ALERT_TELEGRAM_PARSE_MODE"),
        strategy_plugin_alert_telegram_disable_web_page_preview=_setting(
            env,
            "STRATEGY_PLUGIN_ALERT_TELEGRAM_DISABLE_WEB_PAGE_PREVIEW",
        ),
        strategy_plugin_alert_telegram_body_max_chars=_setting(env, "STRATEGY_PLUGIN_ALERT_TELEGRAM_BODY_MAX_CHARS"),
    )


def publish_unified_market_regime_alerts(
    signals: Sequence[object],
    *,
    notification_settings: object,
    translator: Callable[..., str],
    context_label: str,
    state_settings: StrategyPluginAlertStateSettings,
    publish_alerts_fn: Callable[..., Any] = publish_strategy_plugin_alerts,
    log_message: Callable[[str], Any] = print,
) -> tuple[Any, tuple[UnifiedMarketRegimeAlertGateDecision, ...]]:
    decisions = tuple(evaluate_unified_market_regime_alert_gate(signal) for signal in signals)
    publishable_signals = _select_unified_market_regime_alert_signals(signals, decisions)
    selected_ids = {id(signal) for signal in publishable_signals}
    for signal, decision in zip(signals, decisions):
        if not decision.should_publish:
            log_message(
                "unified market-regime alert skipped "
                f"reason={decision.reason} route={decision.route} action={decision.action}"
            )
        elif id(signal) not in selected_ids:
            log_message(
                "unified market-regime alert suppressed "
                f"reason=lower_priority_unified_state route={decision.route} action={decision.action}"
            )
    with _use_unified_market_regime_alert_messages():
        result = publish_alerts_fn(
            publishable_signals,
            notification_settings=notification_settings,
            translator=translator,
            context_label=context_label,
            state_settings=state_settings,
            log_message=log_message,
        )
    return result, decisions


def publish_unified_market_regime_alert(
    signal: object,
    *,
    notification_settings: object,
    translator: Callable[..., str],
    context_label: str,
    state_settings: StrategyPluginAlertStateSettings,
    publish_alerts_fn: Callable[..., Any] = publish_strategy_plugin_alerts,
    log_message: Callable[[str], Any] = print,
) -> tuple[Any, UnifiedMarketRegimeAlertGateDecision]:
    result, decisions = publish_unified_market_regime_alerts(
        [signal],
        notification_settings=notification_settings,
        translator=translator,
        context_label=context_label,
        state_settings=state_settings,
        publish_alerts_fn=publish_alerts_fn,
        log_message=log_message,
    )
    return result, decisions[0]


def build_unified_alert_report_fields(
    result: object,
    decisions: UnifiedMarketRegimeAlertGateDecision | Sequence[UnifiedMarketRegimeAlertGateDecision],
) -> dict[str, Any]:
    fields = dict(result.to_report_fields())
    decision_list = list(decisions) if isinstance(decisions, Sequence) else [decisions]
    fields["unified_alert_gates"] = [decision.to_dict() for decision in decision_list]
    if len(decision_list) == 1:
        fields["unified_alert_gate"] = decision_list[0].to_dict()
    return fields


def _split_env_paths(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    parts: list[str] = []
    for chunk in str(value).replace("\n", ",").split(","):
        text = chunk.strip()
        if text:
            parts.append(text)
    return tuple(parts)


def _resolve_alert_signal_paths(env: Mapping[str, str], output_dir: Path) -> tuple[str, ...]:
    explicit_paths = _split_env_paths(env.get("PLUGIN_ALERT_SIGNAL_PATHS"))
    if explicit_paths:
        return tuple(dict.fromkeys(explicit_paths))
    pattern = str(env.get("PLUGIN_ALERT_SIGNAL_GLOB") or "").strip()
    if pattern:
        return tuple(dict.fromkeys(sorted(glob(pattern, recursive=True))))
    return (str(output_dir / "latest_signal.json"),)


def load_unified_market_regime_alert_signals(
    paths: Sequence[str],
    *,
    expected_plugin: str,
    expected_schema_version: str = "market_regime_control.v1",
    expected_notification_target: str | None = None,
) -> tuple[object, ...]:
    signals = []
    for path in paths:
        signals.append(
            load_strategy_plugin_signal(
                str(path),
                expected_notification_target=expected_notification_target,
                expected_plugin=expected_plugin,
                expected_schema_version=expected_schema_version,
            )
        )
    return tuple(signals)


def main(env: Mapping[str, str] | None = None) -> int:
    resolved_env = os.environ if env is None else env
    output_dir = Path(resolved_env.get("PLUGIN_ALERT_OUTPUT_DIR") or resolved_env["PLUGIN_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)
    signal_paths = _resolve_alert_signal_paths(resolved_env, output_dir)
    if not signal_paths:
        raise SystemExit("no market-regime latest_signal.json files matched alert input")
    multi_signal_mode = bool(resolved_env.get("PLUGIN_ALERT_SIGNAL_PATHS") or resolved_env.get("PLUGIN_ALERT_SIGNAL_GLOB"))
    signals = load_unified_market_regime_alert_signals(
        signal_paths,
        expected_notification_target=None if multi_signal_mode else resolved_env.get("PLUGIN_NOTIFICATION_TARGET"),
        expected_plugin=resolved_env["PLUGIN_NAME"],
        expected_schema_version="market_regime_control.v1",
    )
    context_label = (
        resolved_env.get("STRATEGY_PLUGIN_ALERT_CONTEXT_LABEL")
        or (
            "strategy-plugin-publish / unified-market-regime"
            if multi_signal_mode
            else f"strategy-plugin-publish / {resolved_env['PLUGIN_NOTIFICATION_TARGET']}"
        )
    )
    result, decisions = publish_unified_market_regime_alerts(
        signals,
        notification_settings=build_strategy_plugin_alert_settings_from_env(resolved_env),
        translator=build_unified_market_regime_alert_translator(resolved_env.get("STRATEGY_PLUGIN_ALERT_LANG", "zh")),
        context_label=context_label,
        state_settings=StrategyPluginAlertStateSettings.from_env(
            gcp_project_id=resolved_env.get("GCP_PROJECT_ID"),
        ),
        log_message=print,
    )
    output_path = output_dir / "unified_alert_result.json"
    output_path.write_text(
        json.dumps(build_unified_alert_report_fields(result, decisions), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"wrote unified alert result -> {output_path}")
    print(
        "unified_alert_result "
        f"attempted={result.attempted_count} sent={result.sent_count} "
        f"skipped={result.skipped_count} failed={result.failed_count} "
        f"gate_should_publish={any(decision.should_publish for decision in decisions)} "
        f"gate_count={len(decisions)}"
    )
    if result.failed_count:
        raise SystemExit("unified market-regime alert failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
