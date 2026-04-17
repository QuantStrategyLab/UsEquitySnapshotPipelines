from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.taco_panic_rebound_research import EVENT_KIND_SOFTENING, TradeWarEvent
from us_equity_snapshot_pipelines.taco_rebound_shadow_plugin import (
    ACTION_INCREASE_REBOUND_BUDGET,
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


def test_taco_rebound_shadow_routes_geopolitical_deescalation_to_left_side_budget() -> None:
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
    assert payload["suggested_action"] == ACTION_INCREASE_REBOUND_BUDGET
    assert payload["sleeve_suggestion"] == 0.10
    assert payload["selected_event"]["event_id"] == "iran-ceasefire"
    assert payload["execution_controls"]["intended_strategy_role"] == "left_side_rebound_budget_modifier"
    assert payload["execution_controls"]["selection_allowed"] is False


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
    assert latest["sleeve_suggestion"] == 0.05
