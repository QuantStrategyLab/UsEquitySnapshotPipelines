from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .artifacts import write_json
from .plugin_signal_utils import bool_at, flatten_for_csv, json_scalar, normalize_close, resolve_signal_date
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_overlay_compare import (
    DEFAULT_ATTACK_SYMBOL,
    DEFAULT_BENCHMARK_SYMBOL,
    DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    build_price_crisis_guard_signal,
    build_price_stress_scan,
    filter_events_by_price_stress,
)
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
    EVENT_KIND_SHOCK,
    EVENT_KIND_SOFTENING,
    TRADE_WAR_EVENT_SETS,
    TradeWarEvent,
    resolve_trade_war_event_set,
)
from .yfinance_prices import download_price_history

SCHEMA_VERSION = "taco_rebound_shadow.v1"
SHADOW_MODE = "shadow"
TACO_REBOUND_PROFILE = "taco_rebound_shadow"
ROUTE_TACO_REBOUND = "taco_rebound"
ACTION_INCREASE_REBOUND_BUDGET = "increase_rebound_budget"
ACTION_WATCH_ONLY = "watch_only"
ACTION_NO_ACTION = "no_action"
ACTION_BLOCKED = "blocked"
DEFAULT_OUTPUT_DIR = "data/output/taco_rebound_shadow"
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_MAX_PRICE_AGE_DAYS = 4
DEFAULT_ACTIVE_SIGNAL_DAYS = 10
DEFAULT_TARIFF_SOFTENING_SLEEVE = 0.05
DEFAULT_GEOPOLITICAL_DEESCALATION_SLEEVE = 0.10
DEFAULT_SHOCK_SLEEVE = 0.0
DEFAULT_MAX_SLEEVE = 0.10
HARD_DEFENSE_BREAK_BEAR_REGIONS = frozenset({"iran_middle_east"})


def _next_index_date(index: pd.DatetimeIndex, raw_date: str | pd.Timestamp) -> pd.Timestamp | None:
    date = pd.Timestamp(raw_date).tz_localize(None).normalize()
    candidates = index[index >= date]
    if candidates.empty:
        return None
    return pd.Timestamp(candidates[0]).normalize()


def _event_sleeve(
    event: TradeWarEvent,
    *,
    tariff_softening_sleeve: float,
    geopolitical_deescalation_sleeve: float,
    shock_sleeve: float,
    max_sleeve: float,
) -> float:
    if event.kind == EVENT_KIND_SHOCK:
        sleeve = float(shock_sleeve)
    elif str(event.region).strip().lower() == "iran_middle_east":
        sleeve = float(geopolitical_deescalation_sleeve)
    else:
        sleeve = float(tariff_softening_sleeve)
    return max(0.0, min(float(max_sleeve), sleeve))


def _event_allows_hard_defense(event: TradeWarEvent | None) -> bool:
    if event is None or event.kind != EVENT_KIND_SOFTENING:
        return False
    return str(event.region).strip().lower() in HARD_DEFENSE_BREAK_BEAR_REGIONS


def _active_recognized_events(
    events: Sequence[TradeWarEvent],
    *,
    index: pd.DatetimeIndex,
    signal_date: pd.Timestamp,
    active_signal_days: int,
) -> tuple[tuple[TradeWarEvent, pd.Timestamp], ...]:
    active: list[tuple[TradeWarEvent, pd.Timestamp]] = []
    for event in sorted(events, key=lambda item: item.event_date):
        event_signal_date = _next_index_date(index, event.event_date)
        if event_signal_date is None:
            continue
        if event_signal_date <= signal_date <= event_signal_date + pd.Timedelta(days=int(active_signal_days)):
            active.append((event, event_signal_date))
    return tuple(active)


def build_taco_rebound_shadow_signal(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = (),
    as_of: str | None = None,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    active_signal_days: int = DEFAULT_ACTIVE_SIGNAL_DAYS,
    tariff_softening_sleeve: float = DEFAULT_TARIFF_SOFTENING_SLEEVE,
    geopolitical_deescalation_sleeve: float = DEFAULT_GEOPOLITICAL_DEESCALATION_SLEEVE,
    shock_sleeve: float = DEFAULT_SHOCK_SLEEVE,
    max_sleeve: float = DEFAULT_MAX_SLEEVE,
    suppress_when_price_crisis_guard_active: bool = True,
    crisis_guard_drawdown: float = DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    crisis_guard_ma_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    crisis_guard_ma_slope_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    max_price_age_days: int = DEFAULT_MAX_PRICE_AGE_DAYS,
) -> dict[str, Any]:
    close = normalize_close(price_history)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    if end_date is not None:
        close = close.loc[close.index <= pd.Timestamp(end_date).tz_localize(None).normalize()].copy()
    requested_date, signal_date = resolve_signal_date(close, as_of)
    signal_iso = signal_date.date().isoformat()
    latest_price_date = pd.Timestamp(close.index.max()).normalize()
    price_age_days = int((requested_date - signal_date).days)

    kill_reasons: list[str] = []
    if benchmark_symbol not in close.columns:
        kill_reasons.append(f"missing benchmark price data: {benchmark_symbol}")
    if attack_symbol not in close.columns:
        kill_reasons.append(f"missing attack price data: {attack_symbol}")
    if price_age_days > int(max_price_age_days):
        kill_reasons.append(
            f"price data stale: signal_as_of={signal_iso}, requested_as_of={requested_date.date().isoformat()}"
        )

    scan_active = False
    crisis_guard_active = False
    recognized_events: tuple[TradeWarEvent, ...] = ()
    active_events: tuple[tuple[TradeWarEvent, pd.Timestamp], ...] = ()
    if not kill_reasons:
        scan_days = build_price_stress_scan(
            close,
            start_date=start_date,
            end_date=signal_iso,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
        )
        scan_active = bool_at(scan_days, signal_date)
        recognized_events = filter_events_by_price_stress(events, scan_days)
        active_events = _active_recognized_events(
            recognized_events,
            index=pd.DatetimeIndex(scan_days.index),
            signal_date=signal_date,
            active_signal_days=int(active_signal_days),
        )
        if bool(suppress_when_price_crisis_guard_active):
            crisis_guard = build_price_crisis_guard_signal(
                close,
                start_date=start_date,
                end_date=signal_iso,
                benchmark_symbol=benchmark_symbol,
                drawdown_threshold=float(crisis_guard_drawdown),
                ma_days=int(crisis_guard_ma_days),
                ma_slope_days=int(crisis_guard_ma_slope_days),
            )
            crisis_guard_active = bool_at(crisis_guard, signal_date)

    selected_event: TradeWarEvent | None = None
    selected_event_signal_date: pd.Timestamp | None = None
    sleeve = 0.0
    for event, event_signal_date in active_events:
        candidate_sleeve = _event_sleeve(
            event,
            tariff_softening_sleeve=float(tariff_softening_sleeve),
            geopolitical_deescalation_sleeve=float(geopolitical_deescalation_sleeve),
            shock_sleeve=float(shock_sleeve),
            max_sleeve=float(max_sleeve),
        )
        if candidate_sleeve >= sleeve:
            selected_event = event
            selected_event_signal_date = event_signal_date
            sleeve = candidate_sleeve

    canonical_route = ROUTE_TACO_REBOUND if sleeve > 0.0 else "no_action"
    suggested_action = ACTION_INCREASE_REBOUND_BUDGET if sleeve > 0.0 else ACTION_NO_ACTION
    would_trade_if_enabled = sleeve > 0.0
    allow_hard_defense = bool(would_trade_if_enabled and _event_allows_hard_defense(selected_event))
    suppression_reason = ""
    if active_events and sleeve <= 0.0:
        suggested_action = ACTION_WATCH_ONLY
        suppression_reason = "active event has zero configured sleeve"
        allow_hard_defense = False
    if crisis_guard_active:
        canonical_route = "no_action"
        suggested_action = ACTION_BLOCKED
        would_trade_if_enabled = False
        sleeve = 0.0
        allow_hard_defense = False
        suppression_reason = "price crisis guard active"
    if kill_reasons:
        canonical_route = "no_action"
        suggested_action = ACTION_BLOCKED
        would_trade_if_enabled = False
        sleeve = 0.0
        allow_hard_defense = False
        suppression_reason = "; ".join(kill_reasons)

    generated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "as_of": signal_iso,
        "mode": SHADOW_MODE,
        "schema_version": SCHEMA_VERSION,
        "profile": TACO_REBOUND_PROFILE,
        "canonical_route": canonical_route,
        "suggested_action": suggested_action,
        "sleeve_suggestion": sleeve if sleeve > 0.0 else None,
        "allow_hard_defense": allow_hard_defense,
        "event_rebound_break_bear": allow_hard_defense,
        "would_trade_if_enabled": would_trade_if_enabled,
        "price_stress_scan_active": scan_active,
        "price_crisis_guard_active": crisis_guard_active,
        "active_signal_days": int(active_signal_days),
        "suppression_reason": suppression_reason,
        "selected_event": (
            {
                "event_id": selected_event.event_id,
                "event_date": selected_event.event_date,
                "signal_date": selected_event_signal_date.date().isoformat()
                if selected_event_signal_date is not None
                else None,
                "kind": selected_event.kind,
                "region": selected_event.region,
                "title": selected_event.title,
                "source": selected_event.source,
                "source_url": selected_event.source_url,
            }
            if selected_event is not None
            else None
        ),
        "recognized_event_ids": [event.event_id for event in recognized_events],
        "active_event_ids": [event.event_id for event, _signal_date in active_events],
        "data_freshness": {
            "requested_as_of": requested_date.date().isoformat(),
            "signal_as_of": signal_iso,
            "prices_as_of": latest_price_date.date().isoformat(),
            "price_age_days": price_age_days,
            "max_price_age_days": int(max_price_age_days),
        },
        "execution_controls": {
            "capital_impact": "none",
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "log_namespace": TACO_REBOUND_PROFILE,
            "notification_profile": "shadow_only",
            "intended_strategy_role": "left_side_rebound_budget_modifier",
            "selection_allowed": False,
            "hard_defense_override_signal_allowed": allow_hard_defense,
        },
        "generated_at": generated_at,
    }
    return json_scalar(payload)


def write_taco_rebound_shadow_outputs(payload: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
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
        "sleeve_suggestion": payload.get("sleeve_suggestion"),
        "allow_hard_defense": payload.get("allow_hard_defense"),
        "event_rebound_break_bear": payload.get("event_rebound_break_bear"),
        **flatten_for_csv(payload.get("data_freshness", {})),
        **flatten_for_csv(payload.get("selected_event") or {}),
    }
    pd.DataFrame([evidence_payload]).to_csv(evidence_csv_path, index=False)
    return {
        "latest_signal": latest_path,
        "signal_json": dated_json_path,
        "signal_csv": dated_csv_path,
        "evidence_csv": evidence_csv_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the log-only TACO rebound shadow signal.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--mode", choices=(SHADOW_MODE,), default=SHADOW_MODE)
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--as-of", default=None, help="Requested signal date; defaults to the latest price date")
    parser.add_argument("--price-start", default=DEFAULT_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--download-proxy", default=None, help="Optional yfinance proxy URL; YFINANCE_PROXY also works")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--attack-symbol", default=DEFAULT_ATTACK_SYMBOL)
    parser.add_argument("--active-signal-days", type=int, default=DEFAULT_ACTIVE_SIGNAL_DAYS)
    parser.add_argument("--tariff-softening-sleeve", type=float, default=DEFAULT_TARIFF_SOFTENING_SLEEVE)
    parser.add_argument(
        "--geopolitical-deescalation-sleeve",
        type=float,
        default=DEFAULT_GEOPOLITICAL_DEESCALATION_SLEEVE,
    )
    parser.add_argument("--shock-sleeve", type=float, default=DEFAULT_SHOCK_SLEEVE)
    parser.add_argument("--max-sleeve", type=float, default=DEFAULT_MAX_SLEEVE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.download:
        input_dir = Path(args.output_dir) / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "taco_rebound_shadow_price_history.csv"
        prices = download_price_history(
            [args.benchmark_symbol, args.attack_symbol],
            start=args.price_start,
            end=args.price_end,
            proxy=args.download_proxy,
        )
        prices.to_csv(prices_path, index=False)
        price_history = prices
    else:
        price_history = read_table(args.prices)

    payload = build_taco_rebound_shadow_signal(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        as_of=args.as_of,
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        attack_symbol=args.attack_symbol,
        active_signal_days=args.active_signal_days,
        tariff_softening_sleeve=args.tariff_softening_sleeve,
        geopolitical_deescalation_sleeve=args.geopolitical_deescalation_sleeve,
        shock_sleeve=args.shock_sleeve,
        max_sleeve=args.max_sleeve,
    )
    paths = write_taco_rebound_shadow_outputs(payload, args.output_dir)
    print(
        "wrote TACO rebound shadow signal "
        f"as_of={payload['as_of']} route={payload['canonical_route']} "
        f"action={payload['suggested_action']} latest={paths['latest_signal']}"
    )
    return 0


__all__ = [
    "ACTION_INCREASE_REBOUND_BUDGET",
    "DEFAULT_GEOPOLITICAL_DEESCALATION_SLEEVE",
    "DEFAULT_MAX_SLEEVE",
    "DEFAULT_TARIFF_SOFTENING_SLEEVE",
    "ROUTE_TACO_REBOUND",
    "SCHEMA_VERSION",
    "TACO_REBOUND_PROFILE",
    "build_taco_rebound_shadow_signal",
    "main",
    "write_taco_rebound_shadow_outputs",
]
