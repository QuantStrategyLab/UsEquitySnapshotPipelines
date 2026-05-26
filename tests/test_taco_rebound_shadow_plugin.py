from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.taco_panic_rebound_research import EVENT_KIND_SOFTENING, TradeWarEvent
from us_equity_snapshot_pipelines.taco_rebound_shadow_plugin import (
    ACTION_NOTIFY_MANUAL_REVIEW,
    ROUTE_TACO_REBOUND,
    build_taco_rebound_shadow_signal,
    write_taco_rebound_shadow_outputs,
)


def _panic_rebound_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2026-03-20", periods=12)
    qqq_path = [100.0, 98.0, 96.0, 94.0, 95.0, 99.0, 101.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    tqqq_path = [100.0, 94.0, 88.0, 82.0, 85.0, 96.0, 102.0, 108.0, 111.0, 114.0, 117.0, 120.0]
    rows = []
    for idx, as_of in enumerate(dates):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_path[idx], "volume": 1_000_000})
        rows.append({"symbol": "TQQQ", "as_of": as_of, "close": tqqq_path[idx], "volume": 1_000_000})
    return pd.DataFrame(rows)


def test_taco_rebound_shadow_routes_geopolitical_deescalation_to_manual_review_notice() -> None:
    prices = _panic_rebound_prices()
    dates = pd.bdate_range("2026-03-20", periods=12)
    event = TradeWarEvent(
        event_id="iran-ceasefire",
        event_date=str(dates[4].date()),
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="Ceasefire talks",
        source="test",
        source_url="https://example.test/ceasefire",
    )

    payload = build_taco_rebound_shadow_signal(
        prices,
        events=(event,),
        as_of=str(dates[6].date()),
        start_date=str(dates[0].date()),
    )

    assert payload["canonical_route"] == ROUTE_TACO_REBOUND
    assert payload["suggested_action"] == ACTION_NOTIFY_MANUAL_REVIEW
    assert payload["manual_review_required"] is True
    assert payload["notification_reason"] == "event rebound context confirmed"
    assert payload["rebound_context_active"] is True
    assert payload["event_context_active"] is True
    assert payload["rebound_confirmation"]["confirmed"] is True
    assert payload["would_trade_if_enabled"] is False
    assert "sleeve_suggestion" not in payload
    assert "allow_hard_defense" not in payload
    assert payload["event_rebound_break_bear"] is True
    assert payload["selected_event"]["event_id"] == "iran-ceasefire"
    assert payload["execution_controls"]["intended_strategy_role"] == "event_rebound_notification"
    assert payload["execution_controls"]["selection_allowed"] is False
    assert payload["execution_controls"]["position_sizing_allowed"] is False
    assert payload["execution_controls"]["allocation_recommendation_allowed"] is False
    assert payload["execution_controls"]["hard_defense_override_signal_allowed"] is False


def test_taco_rebound_shadow_waits_for_rebound_confirmation_before_manual_review() -> None:
    prices = _panic_rebound_prices()
    dates = pd.bdate_range("2026-03-20", periods=12)
    event = TradeWarEvent(
        event_id="iran-ceasefire",
        event_date=str(dates[3].date()),
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="Ceasefire talks",
        source="test",
        source_url="https://example.test/ceasefire",
    )

    payload = build_taco_rebound_shadow_signal(
        prices,
        events=(event,),
        as_of=str(dates[3].date()),
        start_date=str(dates[0].date()),
    )

    assert payload["canonical_route"] == "no_action"
    assert payload["suggested_action"] == "watch_only"
    assert payload["manual_review_required"] is False
    assert payload["event_context_active"] is True
    assert payload["rebound_context_active"] is False
    assert payload["rebound_confirmation"]["confirmed"] is False
    assert payload["suppression_reason"] == "rebound confirmation pending"
    assert "post-event trading confirmation" in payload["rebound_confirmation"]["reason"]


def test_taco_rebound_shadow_writes_artifacts(tmp_path) -> None:
    prices = _panic_rebound_prices()
    dates = pd.bdate_range("2026-03-20", periods=12)
    event = TradeWarEvent(
        event_id="tariff-softening",
        event_date=str(dates[4].date()),
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="Tariff softening",
        source="test",
        source_url="https://example.test/tariff",
    )
    payload = build_taco_rebound_shadow_signal(
        prices,
        events=(event,),
        as_of=str(dates[6].date()),
        start_date=str(dates[0].date()),
    )

    paths = write_taco_rebound_shadow_outputs(payload, tmp_path)

    assert paths["latest_signal"].exists()
    assert paths["signal_json"].exists()
    assert paths["signal_csv"].exists()
    assert paths["evidence_csv"].exists()
    latest = json.loads(paths["latest_signal"].read_text(encoding="utf-8"))
    assert latest["manual_review_required"] is True
    assert latest["rebound_confirmation"]["confirmed"] is True
    assert "sleeve_suggestion" not in latest
    assert "allow_hard_defense" not in latest
    assert latest["event_rebound_break_bear"] is False
