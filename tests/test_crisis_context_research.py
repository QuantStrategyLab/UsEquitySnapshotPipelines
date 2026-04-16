from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.crisis_context_research import (
    CONTEXT_LABEL_EXOGENOUS_POLICY_RESCUE,
    CONTEXT_LABEL_EXOGENOUS_SHOCK,
    CONTEXT_LABEL_FINANCIAL_CRISIS,
    CONTEXT_LABEL_POLICY_RESCUE,
    CONTEXT_LABEL_POLICY_SHOCK,
    CONTEXT_LABEL_RATE_BEAR,
    CONTEXT_LABEL_VALUATION_BUBBLE,
    EVENT_KIND_EXOGENOUS_SHOCK,
    EVENT_KIND_POLICY_RESCUE,
    EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL,
    EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
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


def test_crisis_context_features_persists_bubble_context_after_raw_return_fades() -> None:
    dates = pd.bdate_range("1999-01-04", periods=270)
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[252] = 180.0
    qqq.iloc[253:] = 120.0
    close = pd.DataFrame({"QQQ": qqq, "SPY": 100.0}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
        bubble_persistence_days=10,
    )
    persisted_row = features.loc[pd.to_datetime(features["as_of"]).eq(dates[260])].iloc[0]

    assert bool(persisted_row["bubble_context"])
    assert persisted_row["suggested_route"] == ROUTE_TRUE_CRISIS


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


def test_moderate_financial_context_is_audit_only_until_systemic_threshold() -> None:
    dates = pd.bdate_range("2018-09-03", periods=90)
    xlf = [100.0] * 65 + [74.0] * 25
    spy = [100.0] * len(dates)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": spy, "XLF": xlf}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        context_events=(),
        start_date=str(dates[0].date()),
        financial_symbols=("XLF",),
        credit_pairs=(),
        rate_symbols=(),
        financial_drawdown_threshold=-0.20,
        financial_relative_lookback_days=5,
        financial_relative_return_threshold=-0.10,
    )
    row = features.loc[features["financial_system_context"]].iloc[0]

    assert bool(row["financial_system_context"])
    assert not bool(row["systemic_financial_crisis_context"])
    assert row["suggested_route"] == ROUTE_NO_ACTION


def test_combined_financial_and_credit_context_routes_to_true_crisis() -> None:
    dates = pd.bdate_range("2008-01-02", periods=80)
    xlf = [100.0] * 65 + [74.0 - idx * 0.1 for idx in range(15)]
    hyg = [100.0] * 65 + [91.0 - idx * 0.1 for idx in range(15)]
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0, "XLF": xlf, "HYG": hyg, "IEF": 100.0}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(),
        context_events=(),
        start_date=str(dates[0].date()),
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
        financial_drawdown_threshold=-0.20,
        financial_relative_lookback_days=20,
        financial_relative_return_threshold=-0.10,
        credit_relative_lookback_days=20,
        credit_relative_return_threshold=-0.08,
        systemic_financial_drawdown_threshold=-0.35,
        systemic_credit_relative_return_threshold=-0.12,
    )
    row = features.iloc[-1]

    assert bool(row["financial_context"])
    assert bool(row["credit_context"])
    assert bool(row["combined_financial_credit_context"])
    assert not bool(row["systemic_financial_context"])
    assert not bool(row["systemic_credit_context"])
    assert bool(row["systemic_financial_crisis_context"])
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


def test_policy_context_keeps_tariff_window_from_becoming_true_crisis_on_financial_noise() -> None:
    dates = pd.bdate_range("2018-08-01", periods=100)
    xlf = [100.0] * 70 + [76.0 - idx * 0.2 for idx in range(30)]
    hyg = [100.0] * 70 + [96.0 - idx * 0.2 for idx in range(30)]
    spy = [100.0] * len(dates)
    event = TradeWarEvent(
        event_id="tariff-window",
        event_date=str(dates[70].date()),
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Tariff escalation",
        source="test",
        source_url="https://example.test/tariff",
    )
    close = pd.DataFrame({"QQQ": 100.0, "SPY": spy, "XLF": xlf, "HYG": hyg, "IEF": 100.0}, index=dates)

    features = build_crisis_context_features(
        close,
        events=(event,),
        context_events=(),
        start_date=str(dates[0].date()),
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
        policy_event_window_days=20,
        financial_drawdown_threshold=-0.20,
        financial_relative_lookback_days=5,
        financial_relative_return_threshold=-0.01,
        credit_relative_lookback_days=5,
        credit_relative_return_threshold=-0.01,
    )
    noisy_policy_rows = features.loc[features["policy_context"] & features["combined_financial_credit_context"]]

    assert not noisy_policy_rows.empty
    assert set(noisy_policy_rows["suggested_context_label"]) == {CONTEXT_LABEL_POLICY_SHOCK}
    assert set(noisy_policy_rows["suggested_route"]) == {ROUTE_TACO}


def test_exogenous_and_policy_rescue_context_suppresses_covid_like_financial_false_positive() -> None:
    dates = pd.bdate_range("2020-02-18", "2020-04-30")
    xlf = []
    hyg = []
    spy = []
    ief = []
    for idx, _date in enumerate(dates):
        spy.append(100.0)
        ief.append(100.0)
        if idx < 10:
            xlf.append(100.0)
            hyg.append(100.0)
        else:
            xlf.append(88.0 - (idx - 10) * 0.7)
            hyg.append(96.0 - (idx - 10) * 0.35)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": spy, "XLF": xlf, "HYG": hyg, "IEF": ief}, index=dates)
    events = (
        TradeWarEvent(
            event_id="covid-shock",
            event_date="2020-02-24",
            kind=EVENT_KIND_EXOGENOUS_SHOCK,
            region="global",
            title="COVID pandemic sudden-stop shock",
            source="test",
            source_url="https://example.test/covid",
        ),
        TradeWarEvent(
            event_id="fed-rescue",
            event_date="2020-03-23",
            kind=EVENT_KIND_POLICY_RESCUE,
            region="us",
            title="Federal Reserve policy rescue",
            source="test",
            source_url="https://example.test/fed",
        ),
    )

    features = build_crisis_context_features(
        close,
        events=(),
        context_events=events,
        start_date=str(dates[0].date()),
        financial_symbols=("XLF",),
        credit_pairs=(("HYG", "IEF"),),
        rate_symbols=(),
        financial_drawdown_threshold=-0.20,
        financial_relative_lookback_days=5,
        financial_relative_return_threshold=-0.02,
        credit_relative_lookback_days=5,
        credit_relative_return_threshold=-0.01,
        exogenous_event_window_days=21,
        policy_rescue_event_window_days=63,
    )
    stressed = features.loc[features["financial_system_context"]]

    assert not stressed.empty
    assert set(stressed["suggested_route"]) == {ROUTE_NO_ACTION}
    assert CONTEXT_LABEL_EXOGENOUS_SHOCK in set(stressed["suggested_context_label"])
    assert CONTEXT_LABEL_EXOGENOUS_POLICY_RESCUE in set(stressed["suggested_context_label"])
    assert CONTEXT_LABEL_POLICY_RESCUE in set(stressed["suggested_context_label"])
    assert stressed["policy_rescue_event_ids"].str.contains("fed-rescue").any()


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
    assert not bool(features.loc[2, "bubble_context"])
    assert bool(features.loc[2, "external_valuation_context"])


def test_external_valuation_mode_can_route_extreme_pe_to_bubble_context() -> None:
    dates = pd.bdate_range("2000-01-03", periods=5)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0}, index=dates)
    external = pd.DataFrame(
        {
            "as_of": [dates[1]],
            "nasdaq_100_trailing_pe": [82.0],
            "unprofitable_growth_proxy": [0.42],
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
        external_valuation_mode=EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    )
    routed = features.loc[pd.to_datetime(features["as_of"]).eq(dates[2])].iloc[0]

    assert not bool(routed["price_bubble_context"])
    assert bool(routed["external_trailing_pe_extreme_context"])
    assert bool(routed["external_speculative_quality_context"])
    assert bool(routed["bubble_context"])
    assert routed["suggested_context_label"] == CONTEXT_LABEL_VALUATION_BUBBLE
    assert routed["suggested_route"] == ROUTE_TRUE_CRISIS


def test_external_breadth_and_earnings_quality_can_confirm_fragility() -> None:
    dates = pd.bdate_range("2000-01-03", periods=5)
    close = pd.DataFrame({"QQQ": 100.0, "SPY": 100.0}, index=dates)
    external = pd.DataFrame(
        {
            "as_of": [dates[1]],
            "nasdaq_100_trailing_pe": [82.0],
            "nasdaq_100_pct_above_200d": [0.30],
            "nasdaq_100_negative_earnings_share": [0.32],
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
        external_valuation_mode=EXTERNAL_VALUATION_MODE_PRICE_OR_EXTERNAL,
    )
    routed = features.loc[pd.to_datetime(features["as_of"]).eq(dates[2])].iloc[0]

    assert bool(routed["external_valuation_context"])
    assert bool(routed["external_breadth_weak_context"])
    assert bool(routed["external_earnings_quality_weak_context"])
    assert bool(routed["external_breadth_or_quality_context"])
    assert bool(routed["external_confirmed_bubble_fragility_context"])


def test_external_valuation_mode_can_require_price_and_external_confirmation() -> None:
    dates = pd.bdate_range("1999-01-04", periods=270)
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[252:] = 180.0
    close = pd.DataFrame({"QQQ": qqq, "SPY": 100.0}, index=dates)

    without_external = build_crisis_context_features(
        close,
        events=(),
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
        external_valuation_mode=EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL,
    )
    assert without_external["price_bubble_context"].any()
    assert not without_external["bubble_context"].any()

    external = pd.DataFrame({"as_of": [dates[260]], "nasdaq_100_forward_pe": [50.0]})
    with_external = build_crisis_context_features(
        close,
        events=(),
        external_context=external,
        start_date=str(dates[0].date()),
        financial_symbols=(),
        credit_pairs=(),
        rate_symbols=(),
        external_valuation_mode=EXTERNAL_VALUATION_MODE_PRICE_AND_EXTERNAL,
    )
    row = with_external.loc[pd.to_datetime(with_external["as_of"]).eq(dates[261])].iloc[0]

    assert bool(row["price_bubble_context"])
    assert bool(row["external_forward_pe_extreme_context"])
    assert bool(row["bubble_context"])
    assert row["suggested_route"] == ROUTE_TRUE_CRISIS


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
            "policy_rescue_context": [False] * 5,
            "exogenous_policy_rescue_context": [False] * 5,
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
