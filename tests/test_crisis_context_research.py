from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.crisis_context_research import (
    CONTEXT_LABEL_FINANCIAL_CRISIS,
    CONTEXT_LABEL_POLICY_SHOCK,
    CONTEXT_LABEL_RATE_BEAR,
    CONTEXT_LABEL_VALUATION_BUBBLE,
    build_context_diagnostics,
    build_crisis_context_features,
)
from us_equity_snapshot_pipelines.crisis_response_research import ROUTE_NO_ACTION, ROUTE_TACO, ROUTE_TRUE_CRISIS
from us_equity_snapshot_pipelines.taco_panic_rebound_research import EVENT_KIND_SHOCK, TradeWarEvent


def test_crisis_context_features_detect_valuation_bubble_route() -> None:
    dates = pd.bdate_range("1999-01-04", periods=270)
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[252:] = 180.0
    close = pd.DataFrame({"QQQ": qqq, "SPY": 100.0}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
    )
    row = features.loc[pd.to_datetime(features["as_of"]).eq(dates[260])].iloc[0]

    assert bool(row["bubble_context"])
    assert row["suggested_context_label"] == CONTEXT_LABEL_VALUATION_BUBBLE
    assert row["suggested_route"] == ROUTE_TRUE_CRISIS


def test_crisis_context_features_detect_financial_and_credit_stress() -> None:
    dates = pd.bdate_range("2008-01-02", periods=90)
    xlf = [100.0] * 30 + [99.0 - idx for idx in range(60)]
    spy = [100.0] * len(dates)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": spy, "XLF": xlf}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        start_date=str(dates[0].date()),
        financial_symbols=("XLF",),
        credit_pairs=(),
        rate_symbols=(),
        financial_drawdown_threshold=-0.20,
        financial_relative_lookback_days=20,
        financial_relative_return_threshold=-0.10,
    )
    row = features.iloc[-1]

    assert bool(row["financial_context"])
    assert bool(row["financial_system_context"])
    assert row["suggested_context_label"] == CONTEXT_LABEL_FINANCIAL_CRISIS
    assert row["suggested_route"] == ROUTE_TRUE_CRISIS


def test_crisis_context_features_keeps_rate_bear_as_no_action() -> None:
    dates = pd.bdate_range("2022-01-03", periods=160)
    ief = pd.Series(100.0, index=dates)
    ief.iloc[126:] = 89.0
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0, "IEF": ief}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=("IEF",),
        rate_lookback_days=126,
        rate_return_threshold=-0.08,
    )
    row = features.iloc[-1]

    assert bool(row["rate_context"])
    assert row["suggested_context_label"] == CONTEXT_LABEL_RATE_BEAR
    assert row["suggested_route"] == ROUTE_NO_ACTION


def test_crisis_context_features_routes_policy_shock_to_taco_context() -> None:
    dates = pd.bdate_range("2025-04-01", periods=15)
    event = TradeWarEvent(
        event_id="tariff-shock",
        event_date="2025-04-05",
        kind=EVENT_KIND_SHOCK,
        region="global",
        title="Tariff shock",
        source="test",
        source_url="https://example.test/tariff",
    )
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(event,),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
        policy_event_window_days=3,
    )
    policy_rows = features.loc[features["policy_context"]]

    assert policy_rows["policy_event_ids"].str.contains("tariff-shock").any()
    assert set(policy_rows["suggested_context_label"]) == {CONTEXT_LABEL_POLICY_SHOCK}
    assert set(policy_rows["suggested_route"]) == {ROUTE_TACO}


def test_external_context_columns_are_point_in_time_forward_filled() -> None:
    dates = pd.bdate_range("2000-01-03", periods=5)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0}, index=dates)
    external = pd.DataFrame(
        {
            "as_of": [dates[1]],
            "nasdaq_100_trailing_pe": [82.0],
        }
    )

    features = build_crisis_context_features(
        close,
        events=(),
        external_context=external,
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
    )

    assert pd.isna(features.loc[0, "external_nasdaq_100_trailing_pe"])
    assert features.loc[2, "external_nasdaq_100_trailing_pe"] == 82.0


def test_context_diagnostics_counts_routes_and_context_days() -> None:
    dates = pd.bdate_range("2025-04-01", periods=5)
    features = pd.DataFrame(
        {
            "as_of": [date.date().isoformat() for date in dates],
            "bubble_context": [False, True, True, False, False],
            "financial_context": [False] * 5,
            "credit_context": [False] * 5,
            "financial_system_context": [False] * 5,
            "rate_context": [False] * 5,
            "policy_context": [False, False, True, False, False],
            "exogenous_context": [False] * 5,
            "suggested_context_label": ["normal", "valuation_bubble", "valuation_bubble", "normal", "normal"],
            "suggested_route": [
                ROUTE_NO_ACTION,
                ROUTE_TRUE_CRISIS,
                ROUTE_TRUE_CRISIS,
                ROUTE_NO_ACTION,
                ROUTE_NO_ACTION,
            ],
        }
    )

    diagnostics = build_context_diagnostics(
        features,
        periods=(("sample", dates[0].date().isoformat(), dates[-1].date().isoformat()),),
    )

    bubble = diagnostics.loc[diagnostics["Metric"].eq("bubble_context")].iloc[0]
    true_crisis = diagnostics.loc[diagnostics["Metric"].eq(f"suggested_route:{ROUTE_TRUE_CRISIS}")].iloc[0]

    assert bubble["Value"] == 2
    assert true_crisis["Value"] == 2
