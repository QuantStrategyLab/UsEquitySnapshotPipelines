from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.crisis_response_research import (
    CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK,
    EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    ROUTE_NO_ACTION,
    ROUTE_SYSTEMIC_STRESS_WATCH,
    ROUTE_TACO,
    ROUTE_TRUE_CRISIS,
    build_route_audit_effectiveness_reports,
    build_event_response_decisions,
    run_crisis_response_research,
)
from us_equity_snapshot_pipelines.crisis_regime_guard_research import (
    CONTEXT_GATE_RUBRIC,
    CONTEXT_GATE_BUBBLE,
    CONTEXT_GATE_BUBBLE_OR_FINANCIAL,
    apply_context_gate_to_signal,
    build_crisis_context_opinions,
    build_bubble_context_gate,
    build_crisis_guard_specs,
    build_financial_context_gate,
    build_guard_transition_events,
    run_crisis_guard_research,
)
from us_equity_snapshot_pipelines.taco_panic_rebound_research import (
    EVENT_KIND_SHOCK,
    TRADE_WAR_EVENTS_2018_TO_PRESENT,
    TradeWarEvent,
    analyze_event_windows,
    price_history_to_close_matrix,
    resolve_trade_war_event_set,
    summarize_symbol_windows,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2019-05-03", periods=80)
    rows = []
    qqq_path = [100, 95, 90, 92, 94, 98, 103, 106, 108, 110]
    tqqq_path = [100, 86, 74, 80, 88, 100, 115, 126, 135, 145]
    for idx, as_of in enumerate(dates):
        qqq_close = qqq_path[idx] if idx < len(qqq_path) else 110 + idx * 0.3
        tqqq_close = tqqq_path[idx] if idx < len(tqqq_path) else 145 + idx * 0.8
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "TQQQ", "as_of": as_of, "close": tqqq_close, "volume": 1_000_000})
    return pd.DataFrame(rows)


def test_price_history_to_close_matrix_pivots_long_history() -> None:
    close = price_history_to_close_matrix(_sample_prices())

    assert list(close.columns) == ["QQQ", "TQQQ"]
    assert close.loc[pd.Timestamp("2019-05-03"), "QQQ"] == 100


def test_analyze_event_windows_uses_next_trading_day_and_trough_rebound() -> None:
    close = price_history_to_close_matrix(_sample_prices())
    events = (
        TradeWarEvent(
            event_id="weekend-shock",
            event_date="2019-05-05",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Weekend shock",
            source="test",
            source_url="https://example.test",
        ),
    )

    windows = analyze_event_windows(close, events=events, horizons=(5,), trough_window_days=4)
    tqqq = windows.loc[windows["symbol"].eq("TQQQ")].iloc[0]

    assert tqqq["signal_date"] == "2019-05-06"
    assert tqqq["trough_date"] == "2019-05-07"
    assert tqqq["trough_days_from_signal"] == 1
    assert round(float(tqqq["trough_return_from_signal"]), 4) == round(74 / 86 - 1, 4)
    assert round(float(tqqq["return_from_trough_5d"]), 4) == round(126 / 74 - 1, 4)


def test_summarize_symbol_windows_ranks_larger_rebound() -> None:
    close = price_history_to_close_matrix(_sample_prices())
    events = (
        TradeWarEvent(
            event_id="shock",
            event_date="2019-05-06",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Shock",
            source="test",
            source_url="https://example.test",
        ),
    )
    windows = analyze_event_windows(close, events=events, horizons=(5,), trough_window_days=4)

    summary = summarize_symbol_windows(windows, kind=EVENT_KIND_SHOCK, ranking_horizon=5)

    assert summary["symbol"].iloc[0] == "TQQQ"
    assert summary.loc[summary["symbol"].eq("TQQQ"), "hit_rate_max_rebound_gt_20pct_5d"].iloc[0] == 1.0

from us_equity_snapshot_pipelines.taco_panic_rebound_backtest import run_backtest as run_portfolio_backtest
from us_equity_snapshot_pipelines.taco_panic_rebound_overlay_compare import (
    AUDIT_MODE_CRISIS_VETO,
    add_synthetic_attack_close,
    apply_price_crisis_guard_to_weights,
    build_dual_audit_decisions,
    build_price_crisis_guard_signal,
    build_price_stress_scan,
    filter_events_by_price_stress,
    run_overlay_comparison,
)


def test_full_event_set_includes_presidential_period_events() -> None:
    full = resolve_trade_war_event_set("full")

    assert full == TRADE_WAR_EVENTS_2018_TO_PRESENT
    assert any(event.event_date.startswith("2018-") for event in full)
    assert any(event.event_date.startswith("2024-") for event in full)
    assert any(event.event_date.startswith("2025-") for event in full)


def test_price_stress_scan_filters_event_calendar() -> None:
    close = price_history_to_close_matrix(_sample_prices())
    events = (
        TradeWarEvent(
            event_id="stress-shock",
            event_date="2019-05-05",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Stress shock",
            source="test",
            source_url="https://example.test",
        ),
        TradeWarEvent(
            event_id="quiet-shock",
            event_date="2019-07-01",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Quiet shock",
            source="test",
            source_url="https://example.test",
        ),
    )

    scan_days = build_price_stress_scan(close, start_date="2019-05-03")
    filtered = filter_events_by_price_stress(events, scan_days)

    assert scan_days.loc[pd.Timestamp("2019-05-06")]
    assert tuple(event.event_id for event in filtered) == ("stress-shock",)


def test_overlay_comparison_keeps_base_and_adds_price_stress_scenario() -> None:
    prices = _sample_prices()
    events = (
        TradeWarEvent(
            event_id="stress-shock",
            event_date="2019-05-05",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Stress shock",
            source="test",
            source_url="https://example.test",
        ),
    )

    result = run_overlay_comparison(
        prices,
        events=events,
        start_date="2019-05-03",
        overlay_sleeve_ratios=(0.05,),
        turnover_cost_bps=0.0,
    )

    summary = result["summary"]
    diagnostics = result["diagnostics"]
    recognized_events = result["recognized_events"]

    assert set(summary["Strategy"]) == {
        "base",
        "price_stress_taco_5pct",
        "dual_audit_crisis_veto_taco_5pct",
    }
    assert not recognized_events.empty
    assert recognized_events["event_id"].tolist() == ["stress-shock"]
    assert diagnostics["Recognized Events"].sum() >= 1
    assert not result["audit_decisions"].empty


def test_dual_audit_proxy_can_veto_recognized_taco_event() -> None:
    close = price_history_to_close_matrix(_sample_prices())
    events = (
        TradeWarEvent(
            event_id="stress-shock",
            event_date="2019-05-05",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Stress shock",
            source="test",
            source_url="https://example.test",
        ),
    )

    scan_days = build_price_stress_scan(close, start_date="2019-05-03")
    recognized = filter_events_by_price_stress(events, scan_days)
    audited_events, audit_decisions = build_dual_audit_decisions(
        recognized,
        scan_days,
        audit_mode=AUDIT_MODE_CRISIS_VETO,
        veto_event_ids=("stress-shock",),
    )

    assert audited_events == ()
    assert audit_decisions["auditor_verdict"].tolist() == ["veto"]
    assert audit_decisions["final_event_included"].tolist() == [False]


def test_synthetic_attack_close_extends_pre_inception_leveraged_proxy() -> None:
    close = price_history_to_close_matrix(_sample_prices())

    synthetic = add_synthetic_attack_close(
        close[["QQQ"]],
        attack_symbol="SYNTH_TQQQ",
        source_symbol="QQQ",
        multiple=3.0,
        annual_expense_rate=0.0,
    )

    qqq_return = close["QQQ"].pct_change(fill_method=None).iloc[1]
    synthetic_return = synthetic["SYNTH_TQQQ"].pct_change(fill_method=None).iloc[1]
    assert round(float(synthetic_return), 6) == round(float(qqq_return) * 3.0, 6)
    assert synthetic["SYNTH_TQQQ"].notna().all()


def test_price_crisis_guard_proxy_can_reduce_growth_exposure() -> None:
    dates = pd.bdate_range("2008-01-02", periods=90)
    qqq = [100.0] * 65 + [98.0, 95.0, 92.0, 88.0, 84.0, 80.0, 78.0, 76.0, 74.0, 72.0]
    qqq.extend([70.0] * (len(dates) - len(qqq)))
    close = pd.DataFrame({"QQQ": qqq, "TQQQ": qqq}, index=dates)
    crisis = build_price_crisis_guard_signal(
        close,
        start_date="2008-01-02",
        benchmark_symbol="QQQ",
        drawdown_threshold=-0.20,
        ma_days=5,
        ma_slope_days=1,
    )
    assert crisis.any()

    base_weights = pd.DataFrame(
        {
            "QQQ": 0.45,
            "TQQQ": 0.45,
            "SHY": 0.08,
            "CASH": 0.02,
        },
        index=dates,
    )
    guarded = apply_price_crisis_guard_to_weights(
        base_weights,
        crisis,
        safe_symbol="SHY",
        risk_multiplier=0.0,
    )
    crisis_date = crisis.loc[crisis].index[0]

    assert guarded.fillna(0.0).loc[crisis_date, "QQQ"] == 0.0
    assert guarded.fillna(0.0).loc[crisis_date, "TQQQ"] == 0.0
    assert guarded.loc[crisis_date, "SHY"] > 0.9


def test_crisis_guard_research_builds_matrix_and_events() -> None:
    dates = pd.bdate_range("2008-01-02", periods=260)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 80:
            qqq.append(100.0)
        elif idx < 150:
            qqq.append(100.0 - (idx - 79) * 0.45)
        else:
            qqq.append(68.0 + (idx - 150) * 0.2)
    rows = []
    for as_of, close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})

    result = run_crisis_guard_research(
        pd.DataFrame(rows),
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        attack_symbol="SYNTH_TQQQ",
        safe_symbol="SHY",
        drawdown_thresholds=(-0.20,),
        risk_multipliers=(0.0, 0.5),
        ma_days=20,
        ma_slope_days=3,
        turnover_cost_bps=0.0,
    )

    summary = result["summary"]
    diagnostics = result["guard_diagnostics"]
    events = result["guard_events"]

    assert {"base", "crisis_guard_dd20_risk0", "crisis_guard_dd20_risk50"}.issubset(set(summary["Strategy"]))
    assert not diagnostics.empty
    assert not events.empty


def test_crisis_guard_specs_name_threshold_and_multiplier() -> None:
    specs = build_crisis_guard_specs(drawdown_thresholds=(-0.25,), risk_multipliers=(0.5,), confirm_days=3)

    assert specs[0].name == "crisis_guard_dd25_risk50_confirm3"
    assert specs[0].drawdown_threshold == -0.25
    assert specs[0].risk_multiplier == 0.5


def test_crisis_guard_specs_can_add_context_gate_label() -> None:
    specs = build_crisis_guard_specs(
        drawdown_thresholds=(-0.20,),
        risk_multipliers=(0.5,),
        confirm_days=5,
        context_gates=(CONTEXT_GATE_BUBBLE_OR_FINANCIAL,),
    )

    assert specs[0].name == "crisis_guard_bubble_or_financial_dd20_risk50_confirm5"
    assert specs[0].context_gate == CONTEXT_GATE_BUBBLE_OR_FINANCIAL


def test_bubble_context_gate_uses_as_of_trailing_return() -> None:
    dates = pd.bdate_range("1999-01-04", periods=280)
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[252:] = 180.0
    close = pd.DataFrame({"QQQ": qqq}, index=dates)

    gate = build_bubble_context_gate(
        close,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        lookback_days=252,
        return_threshold=0.75,
    )

    assert not bool(gate.iloc[251])
    assert bool(gate.iloc[252])


def test_financial_context_gate_detects_financial_relative_stress() -> None:
    dates = pd.bdate_range("2008-01-02", periods=80)
    xlf = [100.0] * 30 + [99.0 - idx for idx in range(50)]
    spy = [100.0] * len(dates)
    close = pd.DataFrame({"XLF": xlf, "SPY": spy}, index=dates)

    gate = build_financial_context_gate(
        close,
        start_date=str(dates[0].date()),
        financial_symbol="XLF",
        market_symbol="SPY",
        drawdown_threshold=-0.20,
        relative_lookback_days=20,
        relative_return_threshold=-0.10,
    )

    assert gate.any()


def test_context_gate_latches_after_contextual_entry_until_price_signal_off() -> None:
    dates = pd.bdate_range("2020-01-02", periods=6)
    price_signal = pd.Series([False, True, True, True, False, True], index=dates)
    context_signal = pd.Series([False, True, False, False, False, False], index=dates)

    gated = apply_context_gate_to_signal(price_signal, context_signal)

    assert gated.tolist() == [False, True, True, True, False, False]


def test_crisis_context_opinion_proxy_approves_bubble_and_vetoes_plain_price_crisis() -> None:
    dates = pd.bdate_range("1999-01-04", periods=270)
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[252:] = 180.0
    close = pd.DataFrame(
        {
            "QQQ": qqq,
            "XLF": 100.0,
            "SPY": 100.0,
        },
        index=dates,
    )
    price_signal = pd.Series(False, index=dates)
    price_signal.iloc[260] = True

    opinions = build_crisis_context_opinions(
        close,
        price_signal,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        financial_symbol="XLF",
        market_symbol="SPY",
    )
    row = opinions.loc[opinions["as_of"].eq(dates[260].date().isoformat())].iloc[0]

    assert row["proposer_verdict"] == "allow_guard"
    assert row["auditor_verdict"] == "approve"
    assert row["crisis_type"] == "bubble_burst_risk"
    assert bool(row["final_context_allowed"])

    plain_close = pd.DataFrame({"QQQ": 100.0, "XLF": 100.0, "SPY": 100.0}, index=dates)
    plain_opinions = build_crisis_context_opinions(
        plain_close,
        price_signal,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        financial_symbol="XLF",
        market_symbol="SPY",
    )
    plain_row = plain_opinions.loc[plain_opinions["as_of"].eq(dates[260].date().isoformat())].iloc[0]

    assert plain_row["proposer_verdict"] == "watch_only"
    assert plain_row["auditor_verdict"] == "veto_missing_bubble_or_financial_context"
    assert not bool(plain_row["final_context_allowed"])


def test_rubric_context_gate_writes_opinion_rows() -> None:
    dates = pd.bdate_range("1999-01-04", periods=320)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 252:
            qqq.append(100.0)
        elif idx < 270:
            qqq.append(180.0)
        else:
            qqq.append(180.0 - (idx - 269) * 1.5)
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})

    result = run_crisis_guard_research(
        pd.DataFrame(rows),
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        attack_symbol="SYNTH_TQQQ",
        safe_symbol="SHY",
        context_gates=(CONTEXT_GATE_RUBRIC,),
        drawdown_thresholds=(-0.20,),
        risk_multipliers=(0.25,),
        ma_days=20,
        ma_slope_days=3,
        turnover_cost_bps=0.0,
    )

    assert "crisis_guard_rubric_dd20_risk25" in set(result["summary"]["Strategy"])
    assert not result["context_opinions"].empty
    assert result["context_opinions"]["final_context_allowed"].any()


def test_event_response_decisions_route_taco_or_true_crisis() -> None:
    dates = pd.bdate_range("2025-04-01", periods=8)
    scan = pd.Series(True, index=dates)
    crisis = pd.Series([False, False, False, True, True, False, False, False], index=dates)
    events = (
        TradeWarEvent(
            event_id="fake-crisis",
            event_date=str(dates[1].date()),
            kind=EVENT_KIND_SHOCK,
            region="us",
            title="Fake crisis",
            source="test",
            source_url="https://example.test/fake",
        ),
        TradeWarEvent(
            event_id="true-crisis",
            event_date=str(dates[3].date()),
            kind=EVENT_KIND_SHOCK,
            region="us",
            title="True crisis",
            source="test",
            source_url="https://example.test/true",
        ),
    )

    decisions = build_event_response_decisions(events, scan, crisis, pd.DataFrame())

    routes = decisions.set_index("event_id")["route"].to_dict()
    assert routes["fake-crisis"] == ROUTE_TACO
    assert routes["true-crisis"] == ROUTE_TRUE_CRISIS


def test_unified_crisis_response_combines_taco_and_true_crisis_routes() -> None:
    dates = pd.bdate_range("1999-01-04", periods=460)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 252:
            qqq.append(100.0)
        elif idx < 270:
            qqq.append(180.0)
        elif idx < 330:
            qqq.append(180.0 - (idx - 269) * 1.2)
        elif idx == 420:
            qqq.append(100.0)
        elif idx == 421:
            qqq.append(96.0)
        else:
            qqq.append(100.0)
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    events = (
        TradeWarEvent(
            event_id="taco-shock",
            event_date=str(dates[421].date()),
            kind=EVENT_KIND_SHOCK,
            region="us",
            title="Policy shock",
            source="test",
            source_url="https://example.test/taco",
        ),
    )

    result = run_crisis_response_research(
        pd.DataFrame(rows),
        events=events,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        attack_symbol="SYNTH_TQQQ",
        safe_symbol="SHY",
        overlay_sleeve_ratios=(0.05,),
        crisis_drawdown=-0.20,
        crisis_risk_multiplier=0.25,
        crisis_confirm_days=3,
        ma_days=20,
        ma_slope_days=3,
        turnover_cost_bps=0.0,
    )

    assert "unified_response_5pct" in set(result["summary"]["Strategy"])
    assert {ROUTE_TACO, ROUTE_TRUE_CRISIS}.issubset(set(result["response_decisions"]["route"]))
    assert not result["context_opinions"].empty


def test_unified_crisis_response_can_use_v2_context_pack() -> None:
    dates = pd.bdate_range("1999-01-04", periods=460)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 252:
            qqq.append(100.0)
        elif idx < 270:
            qqq.append(180.0)
        elif idx < 330:
            qqq.append(180.0 - (idx - 269) * 1.2)
        elif idx == 420:
            qqq.append(100.0)
        elif idx == 421:
            qqq.append(96.0)
        else:
            qqq.append(100.0)
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    events = (
        TradeWarEvent(
            event_id="tariff-shock",
            event_date=str(dates[421].date()),
            kind=EVENT_KIND_SHOCK,
            region="us",
            title="Tariff policy shock",
            source="test",
            source_url="https://example.test/tariff",
        ),
    )

    result = run_crisis_response_research(
        pd.DataFrame(rows),
        events=events,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        attack_symbol="SYNTH_TQQQ",
        safe_symbol="SHY",
        overlay_sleeve_ratios=(0.05,),
        crisis_context_mode=CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK,
        crisis_drawdown=-0.20,
        crisis_risk_multiplier=0.25,
        crisis_confirm_days=3,
        ma_days=20,
        ma_slope_days=3,
        turnover_cost_bps=0.0,
    )

    assert "unified_response_5pct" in set(result["summary"]["Strategy"])
    assert not result["crisis_context_features"].empty
    assert "suggested_route" in result["context_opinions"].columns
    assert result["context_opinions"]["final_context_allowed"].any()
    assert {ROUTE_TACO, ROUTE_TRUE_CRISIS}.issubset(set(result["response_decisions"]["route"]))


def test_unified_crisis_response_can_use_external_valuation_context() -> None:
    dates = pd.bdate_range("2000-01-03", periods=120)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 60:
            qqq.append(100.0)
        else:
            qqq.append(max(60.0, 100.0 - (idx - 59) * 0.8))
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    external = pd.DataFrame(
        {
            "as_of": [dates[55]],
            "nasdaq_100_trailing_pe": [82.0],
            "unprofitable_growth_proxy": [0.45],
        }
    )

    result = run_crisis_response_research(
        pd.DataFrame(rows),
        events=(),
        external_context=external,
        start_date=str(dates[0].date()),
        benchmark_symbol="QQQ",
        attack_symbol="SYNTH_TQQQ",
        safe_symbol="SHY",
        overlay_sleeve_ratios=(0.05,),
        crisis_context_mode=CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK,
        external_valuation_mode=EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
        crisis_drawdown=-0.20,
        crisis_risk_multiplier=0.25,
        crisis_confirm_days=3,
        ma_days=20,
        ma_slope_days=3,
        turnover_cost_bps=0.0,
    )

    opinions = result["context_opinions"]
    assert not opinions.empty
    assert opinions["final_context_allowed"].any()
    assert opinions["external_valuation_context"].any()
    assert opinions["external_trailing_pe_extreme_context"].any()
    assert result["true_crisis_signal"].any()


def test_unified_crisis_response_can_tighten_severe_external_valuation_crisis() -> None:
    dates = pd.bdate_range("2000-01-03", periods=330)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 260:
            qqq.append(100.0 + idx * 0.7)
        elif idx < 276:
            qqq.append(100.0 + 259 * 0.7 - (idx - 259) * 4.0)
        else:
            qqq.append(218.0)
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    external = pd.DataFrame(
        {
            "as_of": [dates[250]],
            "nasdaq_100_trailing_pe": [82.0],
            "unprofitable_growth_proxy": [0.45],
        }
    )
    common_kwargs = {
        "events": (),
        "external_context": external,
        "start_date": str(dates[0].date()),
        "benchmark_symbol": "QQQ",
        "attack_symbol": "SYNTH_TQQQ",
        "safe_symbol": "SHY",
        "overlay_sleeve_ratios": (0.05,),
        "crisis_context_mode": CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK,
        "external_valuation_mode": EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
        "crisis_drawdown": -0.20,
        "crisis_risk_multiplier": 0.25,
        "crisis_confirm_days": 3,
        "ma_days": 20,
        "ma_slope_days": 3,
        "turnover_cost_bps": 0.0,
    }

    normal = run_crisis_response_research(pd.DataFrame(rows), **common_kwargs)
    severe = run_crisis_response_research(
        pd.DataFrame(rows),
        severe_crisis_risk_multiplier=0.10,
        **common_kwargs,
    )
    severe_bubble = run_crisis_response_research(
        pd.DataFrame(rows),
        severe_crisis_risk_multiplier=0.10,
        severe_crisis_context="valuation_bubble",
        **common_kwargs,
    )

    severe_signal = severe["severe_crisis_signal"].astype(bool)
    assert severe_signal.any()
    assert severe_bubble["severe_crisis_signal"].astype(bool).any()
    normal_weights = normal["weights_by_strategy"]["true_crisis_guard_base"]
    severe_weights = severe["weights_by_strategy"]["true_crisis_guard_base"]
    signal_date = next(date for date in severe_signal.index[severe_signal] if date in severe_weights.index)
    normal_risk = normal_weights.reindex(columns=["QQQ", "SYNTH_TQQQ"], fill_value=0.0).loc[signal_date].sum()
    severe_risk = severe_weights.reindex(columns=["QQQ", "SYNTH_TQQQ"], fill_value=0.0).loc[signal_date].sum()
    assert severe_risk < normal_risk


def test_unified_crisis_response_can_reduce_bubble_fragility_before_true_crisis() -> None:
    dates = pd.bdate_range("2000-01-03", periods=330)
    qqq = []
    for idx, _date in enumerate(dates):
        if idx < 260:
            qqq.append(100.0 + idx * 0.7)
        elif idx < 290:
            qqq.append(100.0 + 259 * 0.7 - (idx - 259) * 3.0)
        else:
            qqq.append(191.0)
    rows = []
    for as_of, qqq_close in zip(dates, qqq, strict=True):
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "SHY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "XLF", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
        rows.append({"symbol": "SPY", "as_of": as_of, "close": 100.0, "volume": 1_000_000})
    external = pd.DataFrame(
        {
            "as_of": [dates[245]],
            "nasdaq_100_trailing_pe": [82.0],
            "unprofitable_growth_proxy": [0.45],
        }
    )
    external_pe_only = pd.DataFrame({"as_of": [dates[245]], "nasdaq_100_trailing_pe": [82.0]})
    common_kwargs = {
        "events": (),
        "external_context": external,
        "start_date": str(dates[0].date()),
        "benchmark_symbol": "QQQ",
        "attack_symbol": "SYNTH_TQQQ",
        "safe_symbol": "SHY",
        "overlay_sleeve_ratios": (0.05,),
        "crisis_context_mode": CRISIS_CONTEXT_MODE_V2_CONTEXT_PACK,
        "external_valuation_mode": EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
        "crisis_drawdown": -0.20,
        "crisis_risk_multiplier": 0.25,
        "crisis_confirm_days": 3,
        "ma_days": 20,
        "ma_slope_days": 3,
        "turnover_cost_bps": 0.0,
    }

    normal = run_crisis_response_research(pd.DataFrame(rows), **common_kwargs)
    fragility = run_crisis_response_research(
        pd.DataFrame(rows),
        bubble_fragility_risk_multiplier=0.50,
        bubble_fragility_drawdown=-0.05,
        bubble_fragility_ma_days=100,
        bubble_fragility_ma_slope_days=5,
        bubble_fragility_confirm_days=3,
        **common_kwargs,
    )
    confirmed_without_quality = run_crisis_response_research(
        pd.DataFrame(rows),
        external_context=external_pe_only,
        bubble_fragility_risk_multiplier=0.50,
        bubble_fragility_context="external_breadth_or_quality",
        bubble_fragility_drawdown=-0.05,
        bubble_fragility_ma_days=100,
        bubble_fragility_ma_slope_days=5,
        bubble_fragility_confirm_days=3,
        **{key: value for key, value in common_kwargs.items() if key != "external_context"},
    )
    confirmed_with_quality = run_crisis_response_research(
        pd.DataFrame(rows),
        bubble_fragility_risk_multiplier=0.50,
        bubble_fragility_context="external_breadth_or_quality",
        bubble_fragility_drawdown=-0.05,
        bubble_fragility_ma_days=100,
        bubble_fragility_ma_slope_days=5,
        bubble_fragility_confirm_days=3,
        **common_kwargs,
    )

    fragility_signal = fragility["bubble_fragility_signal"].astype(bool)
    early_fragility = fragility_signal & ~fragility["true_crisis_signal"].astype(bool)
    assert early_fragility.any()
    assert not confirmed_without_quality["bubble_fragility_signal"].astype(bool).any()
    assert confirmed_with_quality["bubble_fragility_signal"].astype(bool).any()
    signal_date = next(date for date in early_fragility.index[early_fragility] if date in normal["weights_by_strategy"]["unified_response_5pct"].index)
    normal_weights = normal["weights_by_strategy"]["unified_response_5pct"]
    fragility_weights = fragility["weights_by_strategy"]["unified_response_5pct"]
    normal_risk = normal_weights.reindex(columns=["QQQ", "SYNTH_TQQQ"], fill_value=0.0).loc[signal_date].sum()
    fragility_risk = fragility_weights.reindex(columns=["QQQ", "SYNTH_TQQQ"], fill_value=0.0).loc[signal_date].sum()
    assert fragility_risk < normal_risk


def test_route_audit_effectiveness_keeps_2022_rate_bear_out_of_true_crisis() -> None:
    dates = pd.bdate_range("2022-01-03", periods=6)
    features = pd.DataFrame(
        {
            "as_of": [date.date().isoformat() for date in dates],
            "suggested_route": [ROUTE_NO_ACTION] * len(dates),
            "suggested_context_label": ["rate_bear"] * len(dates),
            "suggested_reason": ["rate bear without financial-system stress"] * len(dates),
        }
    )
    confirmed = pd.Series([True] * len(dates), index=dates)
    true_crisis = pd.Series([False] * len(dates), index=dates)
    base_returns = pd.Series([0.0, -0.02, 0.01, -0.01, 0.02, -0.01], index=dates)
    strategy_returns = pd.Series([0.0, -0.02, 0.01, -0.01, 0.02, -0.01], index=dates)

    reports = build_route_audit_effectiveness_reports(
        features,
        confirmed_crisis_signal=confirmed,
        true_crisis_signal=true_crisis,
        returns_by_strategy={"base": base_returns, "unified_response_5pct": strategy_returns},
        route_expectations=(
            ("biden_2022_bear", "2022-01-03", "2022-01-10", ROUTE_NO_ACTION, ROUTE_NO_ACTION),
        ),
    )
    effectiveness = reports["route_audit_effectiveness"].iloc[0]

    assert effectiveness["Status"] == "pass"
    assert effectiveness["False Positive True Crisis Days"] == 0
    assert effectiveness["Confirmed Price Crisis Days"] == len(dates)
    assert reports["route_audit_false_positive_true_crisis"].empty


def test_route_audit_effectiveness_flags_true_crisis_vetoes_as_false_negatives() -> None:
    dates = pd.bdate_range("2000-03-24", periods=5)
    features = pd.DataFrame(
        {
            "as_of": [date.date().isoformat() for date in dates],
            "suggested_route": [
                ROUTE_TRUE_CRISIS,
                ROUTE_TRUE_CRISIS,
                ROUTE_NO_ACTION,
                ROUTE_TRUE_CRISIS,
                ROUTE_TRUE_CRISIS,
            ],
            "suggested_context_label": [
                "valuation_bubble",
                "valuation_bubble",
                "normal",
                "valuation_bubble",
                "valuation_bubble",
            ],
            "suggested_reason": [
                "bubble",
                "bubble",
                "missing context",
                "bubble",
                "bubble",
            ],
        }
    )
    confirmed = pd.Series([True] * len(dates), index=dates)
    true_crisis = pd.Series([True, True, False, True, True], index=dates)

    reports = build_route_audit_effectiveness_reports(
        features,
        confirmed_crisis_signal=confirmed,
        true_crisis_signal=true_crisis,
        route_expectations=(
            ("dotcom_bubble_burst", "2000-03-24", "2000-03-30", ROUTE_TRUE_CRISIS, ROUTE_TRUE_CRISIS),
        ),
    )
    effectiveness = reports["route_audit_effectiveness"].iloc[0]
    false_negatives = reports["route_audit_false_negative_true_crisis"]

    assert effectiveness["False Negative True Crisis Days"] == 1
    assert false_negatives["as_of"].tolist() == ["2000-03-28"]
    assert false_negatives["Suggested Route"].tolist() == [ROUTE_NO_ACTION]


def test_route_audit_effectiveness_treats_2011_as_stress_watch_until_price_confirms() -> None:
    dates = pd.bdate_range("2011-07-22", periods=5)
    features = pd.DataFrame(
        {
            "as_of": [date.date().isoformat() for date in dates],
            "suggested_route": [
                ROUTE_NO_ACTION,
                ROUTE_TRUE_CRISIS,
                ROUTE_TRUE_CRISIS,
                ROUTE_NO_ACTION,
                ROUTE_TRUE_CRISIS,
            ],
            "suggested_context_label": [
                "normal",
                "financial_crisis",
                "financial_crisis",
                "normal",
                "financial_crisis",
            ],
            "suggested_reason": [
                "no active historical-crisis context",
                "joint financial and credit stress",
                "joint financial and credit stress",
                "no active historical-crisis context",
                "joint financial and credit stress",
            ],
        }
    )
    confirmed = pd.Series([False] * len(dates), index=dates)
    true_crisis = pd.Series([False] * len(dates), index=dates)

    reports = build_route_audit_effectiveness_reports(
        features,
        confirmed_crisis_signal=confirmed,
        true_crisis_signal=true_crisis,
        route_expectations=(
            (
                "2011_debt_euro_stress",
                "2011-07-22",
                "2011-07-28",
                ROUTE_SYSTEMIC_STRESS_WATCH,
                f"{ROUTE_TRUE_CRISIS},{ROUTE_NO_ACTION}",
            ),
        ),
    )
    effectiveness = reports["route_audit_effectiveness"].iloc[0]

    assert effectiveness["Status"] == "pass"
    assert effectiveness["Expected Route"] == ROUTE_SYSTEMIC_STRESS_WATCH
    assert effectiveness["Suggested Acceptable Days"] == len(dates)
    assert effectiveness["False Positive True Crisis Days"] == 0
    assert reports["route_audit_false_positive_true_crisis"].empty


def test_guard_transition_events_records_on_off_edges() -> None:
    dates = pd.bdate_range("2020-01-02", periods=5)
    signal = pd.Series([False, True, True, False, True], index=dates)

    events = build_guard_transition_events(signal, strategy_name="guard")

    assert events["reason"].tolist() == ["crisis_guard_on", "crisis_guard_off", "crisis_guard_on"]


def test_portfolio_backtest_trades_shock_ladder_with_sleeve_overlay() -> None:
    prices = _sample_prices()
    events = (
        TradeWarEvent(
            event_id="weekend-shock",
            event_date="2019-05-05",
            kind=EVENT_KIND_SHOCK,
            region="china",
            title="Weekend shock",
            source="test",
            source_url="https://example.test",
        ),
    )

    result = run_portfolio_backtest(
        prices,
        events=events,
        basket_weights={"TQQQ": 1.0},
        start_date="2019-05-03",
        end_date="2019-08-15",
        turnover_cost_bps=0.0,
        account_sleeve_ratio=0.10,
    )

    trades = result["trades"]
    summary = result["summary"]
    period_summary = result["period_summary"]

    assert not trades.empty
    assert "shock_ladder_entry" in set(trades["reason"])
    assert trades["new_exposure"].max() > 0.0
    assert not result["weights_history"].empty
    assert not period_summary.empty
    assert "Period" in period_summary.columns
    assert set(period_summary["Period"]) == {"trump_1"}
    assert summary["Strategy"].iloc[0] == "taco_panic_rebound_steady"
    assert summary.loc[summary["Strategy"].str.contains("account_overlay"), "Final Equity"].iloc[0] > 1.0
