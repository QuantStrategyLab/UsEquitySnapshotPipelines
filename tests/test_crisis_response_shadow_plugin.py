from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.crisis_response_research import ROUTE_NO_ACTION, ROUTE_TRUE_CRISIS
from us_equity_snapshot_pipelines.crisis_response_shadow_plugin import (
    SCHEMA_VERSION,
    build_crisis_response_shadow_signal,
    main,
    write_crisis_response_shadow_outputs,
)
from us_equity_snapshot_pipelines.taco_panic_rebound_research import EVENT_KIND_SHOCK, TradeWarEvent


def _financial_crisis_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2007-01-02", periods=310)
    rows: list[dict[str, object]] = []
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[245:] = pd.Series(
        [100.0 - idx * (35.0 / (len(dates) - 245 - 1)) for idx in range(len(dates) - 245)],
        index=dates[245:],
    )
    tqqq = pd.Series(100.0, index=dates)
    tqqq.iloc[245:] = pd.Series(
        [100.0 - idx * (70.0 / (len(dates) - 245 - 1)) for idx in range(len(dates) - 245)],
        index=dates[245:],
    )
    xlf = pd.Series(100.0, index=dates)
    xlf.iloc[220:] = pd.Series(
        [100.0 - idx * (55.0 / (len(dates) - 220 - 1)) for idx in range(len(dates) - 220)],
        index=dates[220:],
    )
    hyg = pd.Series(100.0, index=dates)
    hyg.iloc[235:] = pd.Series(
        [100.0 - idx * (18.0 / (len(dates) - 235 - 1)) for idx in range(len(dates) - 235)],
        index=dates[235:],
    )
    prices = {
        "QQQ": qqq,
        "TQQQ": tqqq,
        "SPY": pd.Series(100.0, index=dates),
        "XLF": xlf,
        "HYG": hyg,
        "IEF": pd.Series(100.0, index=dates),
    }
    for symbol, series in prices.items():
        for as_of, close in series.items():
            rows.append({"symbol": symbol, "as_of": as_of, "close": close, "volume": 1_000_000})
    return pd.DataFrame(rows)


def test_shadow_signal_routes_financial_credit_crisis_without_live_execution() -> None:
    prices = _financial_crisis_prices()
    as_of = str(pd.to_datetime(prices["as_of"]).max().date())

    payload = build_crisis_response_shadow_signal(
        prices,
        events=(),
        as_of=as_of,
        start_date="2007-01-02",
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
    )

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["mode"] == "shadow"
    assert payload["canonical_route"] == ROUTE_TRUE_CRISIS
    assert payload["suggested_action"] == "defend"
    assert payload["would_trade_if_enabled"] is True
    assert payload["risk_multiplier_suggestion"] == 0.0
    assert payload["price_scanner_active"] is True
    assert payload["kill_switch_active"] is False
    assert payload["evidence"]["financial_context"] is True
    assert payload["evidence"]["credit_context"] is True
    assert payload["evidence"]["combined_financial_credit_context"] is True
    assert payload["execution_controls"]["capital_impact"] == "none"
    assert payload["execution_controls"]["broker_order_allowed"] is False
    assert payload["execution_controls"]["live_allocation_mutation_allowed"] is False


def test_shadow_signal_writes_daily_json_csv_and_evidence(tmp_path) -> None:
    prices = _financial_crisis_prices()
    as_of = str(pd.to_datetime(prices["as_of"]).max().date())
    payload = build_crisis_response_shadow_signal(
        prices,
        events=(),
        as_of=as_of,
        start_date="2007-01-02",
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
    )

    paths = write_crisis_response_shadow_outputs(payload, tmp_path)

    assert paths["latest_signal"].exists()
    assert paths["signal_json"].exists()
    assert paths["signal_csv"].exists()
    assert paths["evidence_csv"].exists()
    latest = json.loads(paths["latest_signal"].read_text(encoding="utf-8"))
    assert latest["as_of"] == as_of
    assert latest["canonical_route"] == ROUTE_TRUE_CRISIS


def test_shadow_signal_blocks_stale_price_data() -> None:
    prices = _financial_crisis_prices()
    last_date = pd.to_datetime(prices["as_of"]).max().normalize()
    requested_as_of = (last_date + pd.Timedelta(days=10)).date().isoformat()

    payload = build_crisis_response_shadow_signal(
        prices,
        events=(),
        as_of=requested_as_of,
        start_date="2007-01-02",
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
        max_price_age_days=2,
    )

    assert payload["canonical_route"] == ROUTE_NO_ACTION
    assert payload["suggested_action"] == "blocked"
    assert payload["would_trade_if_enabled"] is False
    assert payload["kill_switch_active"] is True
    assert "price data stale" in payload["kill_switch_reason"]


def test_policy_context_without_price_stress_stays_watch_only() -> None:
    dates = pd.bdate_range("2025-01-02", periods=230)
    rows = []
    for symbol in ("QQQ", "TQQQ", "SPY"):
        for as_of in dates:
            rows.append({"symbol": symbol, "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    event = TradeWarEvent(
        event_id="tariff-watch",
        event_date=str(dates[-1].date()),
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Tariff shock",
        source="test",
        source_url="https://example.test/tariff",
    )

    payload = build_crisis_response_shadow_signal(
        pd.DataFrame(rows),
        events=(event,),
        as_of=str(dates[-1].date()),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
    )

    assert payload["audit_summary"]["proposer_route"] == ROUTE_NO_ACTION
    assert payload["audit_summary"]["proposer_context_label"] == "policy_shock"
    assert payload["canonical_route"] == ROUTE_NO_ACTION
    assert payload["suggested_action"] == "watch_only"
    assert payload["evidence"]["policy_context"] is True
    assert payload["would_trade_if_enabled"] is False
    assert "price_stress_scan_active" not in payload
    assert "taco_sleeve_suggestion" not in payload


def test_shadow_signal_maps_policy_context_to_watch_only_after_split() -> None:
    dates = pd.bdate_range("2026-03-20", periods=12)
    qqq_path = [100.0, 98.0, 96.0, 94.0, 95.0, 99.0, 101.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    tqqq_path = [100.0, 94.0, 88.0, 82.0, 85.0, 96.0, 102.0, 108.0, 111.0, 114.0, 117.0, 120.0]
    rows = []
    for idx, as_of in enumerate(dates):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_path[idx], "volume": 1_000_000})
        rows.append({"symbol": "TQQQ", "as_of": as_of, "close": tqqq_path[idx], "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": qqq_path[idx], "volume": 1_000_000})
    event = TradeWarEvent(
        event_id="war-deescalation",
        event_date=str(dates[4].date()),
        kind=EVENT_KIND_SHOCK,
        region="iran_middle_east",
        title="War deescalation watch",
        source="test",
        source_url="https://example.test/deescalation",
    )

    payload = build_crisis_response_shadow_signal(
        pd.DataFrame(rows),
        events=(event,),
        as_of=str(dates[-1].date()),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
    )

    assert payload["audit_summary"]["proposer_route"] == ROUTE_NO_ACTION
    assert payload["audit_summary"]["proposer_context_label"] == "exogenous_shock"
    assert payload["canonical_route"] == ROUTE_NO_ACTION
    assert payload["suggested_action"] == "watch_only"
    assert payload["evidence"]["exogenous_context"] is True
    assert "taco_sleeve_suggestion" not in payload
    assert "taco_routing_allowed" not in payload["execution_controls"]
    assert payload["execution_controls"]["defensive_destination"] == "cash_or_money_market"
    assert payload["execution_controls"]["intended_strategy_role"] == "black_swan_defense"


def test_shadow_cli_writes_artifacts(tmp_path) -> None:
    prices = _financial_crisis_prices()
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "shadow"
    prices.to_csv(prices_path, index=False)
    as_of = str(pd.to_datetime(prices["as_of"]).max().date())

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--as-of",
            as_of,
            "--start",
            "2007-01-02",
            "--financial-symbols",
            "XLF",
            "--credit-pairs",
            "HYG:IEF",
            "--rate-symbols",
            "",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "latest_signal.json").exists()
    latest = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert latest["mode"] == "shadow"
