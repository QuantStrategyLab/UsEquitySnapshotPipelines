from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from us_equity_snapshot_pipelines.strategy_plugin_alerts import (
    build_unified_market_regime_alert_messages,
    build_unified_market_regime_alert_translator,
    evaluate_unified_market_regime_alert_gate,
    publish_unified_market_regime_alert,
    publish_unified_market_regime_alerts,
)


def _signal(
    route: str,
    action: str = "watch_only",
    *,
    ai_summary: str | None = "可见线索是价格从近期低点修复，但宏观和波动压力还没有完全解除，只能先按小仓位候选复核。",
) -> SimpleNamespace:
    payload: dict[str, Any] = {
        "position_control": {
            "risk_budget_scalar": 1.0,
            "leverage_scalar": 1.0,
            "risk_asset_scalar": 1.0,
            "taco_size_scalar": 0.12,
            "panic_reversal_size_scalar": 0.0,
            "crisis_defense_required": False,
            "volatility_delever_context": {
                "hard_risk": False,
                "soft_risk": False,
                "rebound_confirm": True,
                "price_rebound_candidate": True,
                "macro_watch": False,
            },
        },
        "component_signals": {
            "taco": {
                "available": True,
                "actionable_score": 0.82,
                "total_score": 4.2,
                "manual_review_required": True,
                "metrics": {
                    "benchmark_3d_return": 0.052,
                    "benchmark_rebound_from_recent_low": 0.08,
                    "attack_rebound_from_recent_low": 0.11,
                },
            },
        },
        "localized_messages": {
            "default_locale": "zh-CN",
            "labels": {
                "reason_codes": {
                    "zh-CN": ["市场压力进入触发区", "反弹窗口待确认"],
                    "en-US": ["market stress entered trigger zone", "rebound window needs confirmation"],
                }
            },
        },
    }
    if ai_summary is not None:
        payload["ai_audit"] = {
            "enabled": True,
            "status": "ok",
            "verdict": "support",
            "route_assessment": "consistent",
            "summary": ai_summary,
        }
    return SimpleNamespace(
        strategy="",
        target_type="notification_target",
        notification_target="market_regime_notification",
        plugin="market_regime_control",
        effective_mode="shadow",
        canonical_route=route,
        suggested_action=action,
        would_trade_if_enabled=False,
        as_of="2026-06-19",
        execution_controls={"notification_profile": "shadow_only"},
        payload=payload,
    )


def test_unified_market_regime_alert_gate_skips_low_value_volatility_states() -> None:
    for route in ("no_action", "watch", "risk_reduced", "delever", "blocked"):
        decision = evaluate_unified_market_regime_alert_gate(_signal(route))

        assert decision.should_publish is False
        assert decision.reason == "low_priority_market_state"


def test_unified_market_regime_alert_gate_allows_rare_crisis_and_opportunity_states() -> None:
    allowed = {
        "true_crisis": "defend",
        "crisis": "defend",
        "risk_off": "defend",
        "opportunity_watch": "notify_manual_review",
        "panic_reversal": "notify_manual_review",
        "taco_rebound": "notify_manual_review",
    }

    for route, action in allowed.items():
        decision = evaluate_unified_market_regime_alert_gate(_signal(route, action))

        assert decision.should_publish is True
        assert decision.reason == "high_value_market_event"


def test_unified_market_regime_alert_gate_allows_risk_reduced_when_opportunity_is_vetoed() -> None:
    signal = _signal("risk_reduced", "delever", ai_summary=None)
    signal.payload["notification"] = {
        "opportunity_vetoed_should_notify": True,
        "vetoes": ["macro_delever_blocks_taco"],
        "vetoed_opportunities": [{"component": "taco"}],
    }

    decision = evaluate_unified_market_regime_alert_gate(signal)

    assert decision.should_publish is True
    assert decision.reason == "opportunity_vetoed_by_risk"


def test_unified_market_regime_alert_renders_minimal_chinese_message() -> None:
    translate = build_unified_market_regime_alert_translator("zh")

    messages = build_unified_market_regime_alert_messages(
        [_signal("opportunity_watch", "notify_manual_review")],
        translator=translate,
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert len(messages) == 1
    assert messages[0].subject == ""
    assert messages[0].body == "\n".join(
        (
            "日期：2026-06-19",
            "市场状态：抄底机会",
            "背景情况：价格从近期低点修复，但宏观和波动压力还没有完全解除，只能先按小仓位候选复核。",
            "建议操作：不要自动买；确认反弹质量、失效位和仓位上限后，再考虑小仓位抄底。",
        )
    )
    assert "可见线索是" not in messages[0].body
    assert "当时可见" not in messages[0].body
    assert "触发项" not in messages[0].body
    assert "指标：" not in messages[0].body
    assert "AI分析：" not in messages[0].body
    assert messages[0].metadata["indicator_summary"]
    assert (
        messages[0].metadata["ai_analysis"]
        == "AI评估支持；路线一致；价格从近期低点修复，但宏观和波动压力还没有完全解除，只能先按小仓位候选复核"
    )
    assert "插件发布" not in messages[0].body
    assert "统一市场状态通知" not in messages[0].body
    assert "插件：" not in messages[0].body
    assert "模式：" not in messages[0].body
    assert "自动化：" not in messages[0].body
    assert "当前情况：" not in messages[0].body
    assert "建议处理：" not in messages[0].body
    assert "动作边界：" not in messages[0].body


def test_unified_market_regime_alert_renders_minimal_black_swan_message() -> None:
    messages = build_unified_market_regime_alert_messages(
        [
            _signal(
                "risk_off",
                "defend",
                ai_summary="可见线索是信用压力扩大、金融板块承压，指数也跌破中长期趋势，说明流动性和风险偏好正在恶化。",
            )
        ],
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert messages[0].body == "\n".join(
        (
            "日期：2026-06-19",
            "市场状态：黑天鹅",
            "背景情况：信用压力扩大、金融板块承压，指数也跌破中长期趋势，说明流动性和风险偏好正在恶化。",
            "建议操作：先不要抄底；检查杠杆和风险仓位，必要时按策略结果降杠杆或清仓。",
        )
    )
    assert "可见线索是" not in messages[0].body
    assert "触发项" not in messages[0].body
    assert "指标：" not in messages[0].body


def test_unified_market_regime_alert_renders_vetoed_opportunity_message() -> None:
    signal = _signal("risk_reduced", "delever", ai_summary=None)
    signal.payload["notification"] = {
        "opportunity_vetoed_should_notify": True,
        "vetoes": ["macro_delever_blocks_taco"],
        "vetoed_opportunities": [{"component": "taco"}],
    }
    signal.payload["position_control"]["volatility_delever_context"]["macro_watch"] = True
    signal.payload["component_signals"]["macro"] = {
        "available": True,
        "canonical_route": "delever",
        "reason_codes": ["benchmark_below_ma", "credit_pair_stress"],
    }

    messages = build_unified_market_regime_alert_messages(
        [signal],
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert messages[0].body == "\n".join(
        (
            "日期：2026-06-19",
            "市场状态：机会被否决",
            "背景情况：价格反弹开始确认、标的从近期低点修复，但信用压力在扩大、指数跌破中长期趋势、"
            "宏观压力没有解除；仲裁先按降风险处理，暂不抄底。",
            "建议操作：先不要抄底；等宏观或波动压力解除，再重新评估小仓位机会。",
        )
    )
    assert "指标：" not in messages[0].body
    assert "可见线索是" not in messages[0].body


def test_unified_market_regime_alert_mentions_vix_panic_reversal_component() -> None:
    signal = _signal("opportunity_watch", "notify_manual_review", ai_summary=None)
    signal.payload["position_control"]["volatility_delever_context"]["rebound_confirm"] = False
    signal.payload["position_control"]["volatility_delever_context"]["price_rebound_candidate"] = False
    signal.payload["component_signals"]["taco"]["manual_review_required"] = False
    signal.payload["component_signals"]["panic_reversal"] = {
        "available": True,
        "manual_review_required": True,
        "panic_reversal_context_active": True,
        "reason_codes": ["vix_panic_reversal", "price_rebound_confirmation"],
    }

    messages = build_unified_market_regime_alert_messages(
        [signal],
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert "VIX恐慌波动从高位回落" in messages[0].body
    assert "市场状态：抄底机会" in messages[0].body


def test_unified_market_regime_alert_uses_concise_deterministic_background_without_ai() -> None:
    messages = build_unified_market_regime_alert_messages(
        [_signal("opportunity_watch", "notify_manual_review", ai_summary=None)],
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert "背景情况：价格反弹开始确认、标的从近期低点修复；只能按小仓位候选复核。" in messages[0].body
    assert "当时可见" not in messages[0].body
    assert "指标：" not in messages[0].body
    assert "AI分析：" not in messages[0].body
    assert messages[0].metadata["indicator_summary"]
    assert messages[0].metadata["ai_analysis"] == ""


def test_unified_market_regime_alert_explains_visible_risk_evidence_without_ai() -> None:
    signal = _signal("risk_off", "defend", ai_summary=None)
    signal.payload["position_control"]["crisis_defense_required"] = True
    signal.payload["position_control"]["volatility_delever_context"]["hard_risk"] = True
    signal.payload["component_signals"]["crisis"] = {
        "available": True,
        "canonical_route": "true_crisis",
        "reason_codes": ["true_crisis"],
        "audit_summary": {
            "reason": "severe or jointly confirmed financial-sector / credit-stress context is active",
        },
    }
    signal.payload["component_signals"]["macro"] = {
        "available": True,
        "canonical_route": "crisis",
        "reason_codes": ["benchmark_below_ma", "benchmark_drawdown_crisis", "credit_pair_stress"],
    }

    messages = build_unified_market_regime_alert_messages(
        [signal],
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
    )

    assert (
        "背景情况：信用压力在扩大、金融板块承压、指数跌破中长期趋势、"
        "回撤扩大到危机观察区，说明流动性和风险偏好正在恶化。"
    ) in messages[0].body
    assert "当时可见" not in messages[0].body
    assert "金融危机" not in messages[0].body
    assert "指标：" not in messages[0].body


def test_publish_unified_market_regime_alert_passes_empty_signal_list_when_gate_skips() -> None:
    observed = {}

    class FakeResult:
        attempted_count = 0
        sent_count = 0
        skipped_count = 0
        failed_count = 0

        def to_report_fields(self):
            return {"strategy_plugin_alert_attempted_count": 0}

    def fake_publish(signals, **kwargs):
        observed["signals"] = tuple(signals)
        observed["kwargs"] = kwargs
        return FakeResult()

    result, decision = publish_unified_market_regime_alert(
        _signal("watch"),
        notification_settings=SimpleNamespace(),
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / market_regime_notification",
        state_settings=SimpleNamespace(),
        publish_alerts_fn=fake_publish,
        log_message=lambda _message: None,
    )

    assert result.attempted_count == 0
    assert decision.should_publish is False
    assert observed["signals"] == ()


def test_publish_unified_market_regime_alerts_selects_one_highest_priority_signal() -> None:
    observed = {}
    watch = _signal("watch")
    opportunity = _signal("opportunity_watch", "notify_manual_review")
    crisis = _signal("risk_off", "defend")

    class FakeResult:
        attempted_count = 1
        sent_count = 1
        skipped_count = 0
        failed_count = 0

        def to_report_fields(self):
            return {"strategy_plugin_alert_attempted_count": 1}

    def fake_publish(signals, **kwargs):
        observed["signals"] = tuple(signals)
        observed["kwargs"] = kwargs
        return FakeResult()

    result, decisions = publish_unified_market_regime_alerts(
        [watch, opportunity, crisis],
        notification_settings=SimpleNamespace(),
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / unified-market-regime",
        state_settings=SimpleNamespace(),
        publish_alerts_fn=fake_publish,
        log_message=lambda _message: None,
    )

    assert result.sent_count == 1
    assert [decision.should_publish for decision in decisions] == [False, True, True]
    assert observed["signals"] == (crisis,)


def test_publish_unified_market_regime_alerts_prefers_vix_panic_reversal_opportunity() -> None:
    observed = {}
    taco = _signal("opportunity_watch", "notify_manual_review", ai_summary=None)
    panic = _signal("opportunity_watch", "notify_manual_review", ai_summary=None)
    panic.payload["component_signals"]["taco"]["manual_review_required"] = False
    panic.payload["component_signals"]["panic_reversal"] = {
        "available": True,
        "manual_review_required": True,
        "panic_reversal_context_active": True,
        "reason_codes": ["vix_panic_reversal"],
    }

    class FakeResult:
        attempted_count = 1
        sent_count = 1
        skipped_count = 0
        failed_count = 0

        def to_report_fields(self):
            return {"strategy_plugin_alert_attempted_count": 1}

    def fake_publish(signals, **kwargs):
        observed["signals"] = tuple(signals)
        return FakeResult()

    publish_unified_market_regime_alerts(
        [taco, panic],
        notification_settings=SimpleNamespace(),
        translator=build_unified_market_regime_alert_translator("zh"),
        context_label="strategy-plugin-publish / unified-market-regime",
        state_settings=SimpleNamespace(),
        publish_alerts_fn=fake_publish,
        log_message=lambda _message: None,
    )

    assert observed["signals"] == (panic,)
