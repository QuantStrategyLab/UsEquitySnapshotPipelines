"""Normalize a verified archive for the existing offline B1/B2 harness.

This module builds inputs only. Calendar and event-coverage declarations are
caller supplied and do not establish historical PIT or authorize real replay.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Mapping, Sequence

from input_qualification import (Event, PROFILE_SYMBOLS, RESEARCH_SYMBOLS,
                                 qualify_historical_input, select_profiles, validate_historical_members)
from us_equity_strategies.research.kelly_core_return_generator import CorporateActions, RawBar
from us_equity_strategies.research.kelly_private_runner import load_accounting_archive


@dataclass(frozen=True)
class NormalizedInput:
    sessions: tuple[date, ...]
    core_sessions: dict[str, tuple[date, ...]]
    core_prices: dict[str, tuple[dict[str, float], ...]]
    opening_prices: dict[date, dict[str, float]]
    core_events: dict
    replay_bounds: dict[str, tuple[int, int]]
    prices: tuple[dict[str, float], ...]
    events: dict
    all_events: tuple[Event, ...]
    bars: dict
    actions: dict
    receipt: dict


def read_archive_members(root: Path, manifest_sha256: str, *, symbols=RESEARCH_SYMBOLS):
    """Read hash-bound raw SIP archive members and preserve event accounting dates."""
    symbols = tuple(sorted(symbols))
    manifest = json.loads((root / "manifest.json").read_bytes())
    declarations = {"schema_version": "qsl.research.raw_sip_input.v1",
                    "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
                    "feed": "sip", "price_adjustment": "raw"}
    if any(manifest.get(key) != value for key, value in declarations.items()):
        raise ValueError("RAW_SIP_DECLARATIONS_MISMATCH")
    bars, actions, payments = load_accounting_archive(root, symbols, manifest_sha256)
    for symbol in symbols:
        for kind, count in (("bars", len(bars[symbol])),
                            ("actions", len(actions[symbol].splits) + len(actions[symbol].dividends))):
            item = next(item for item in manifest["inputs"]
                        if item["symbol"] == symbol and item["kind"] == kind)
            if type(item.get("count")) is not int or item["count"] != count:
                raise ValueError(f"ARCHIVE_DECLARED_COUNT_MISMATCH:{symbol}:{kind}")
    events = []
    for symbol in symbols:
        action = actions[symbol]
        if {day for day, _ in action.splits} & {day for day, _ in action.dividends}:
            raise ValueError("SPLIT_AND_DIVIDEND_SAME_DAY_NEEDS_REVIEW")
        # Check the accounting view against the legacy view, preserving duplicates.
        if Counter((ex, rate) for ex, _, rate in payments[symbol]) != Counter(action.dividends):
            raise ValueError("DIVIDEND_ACCOUNTING_MISMATCH")
        events.extend(Event(symbol, ex, "split", ratio) for ex, ratio in action.splits)
        events.extend(Event(symbol, ex, "dividend", rate, pay_session=pay)
                      for ex, pay, rate in payments[symbol])
    return bars, actions, events


def normalize_archive(root: Path, manifest_sha256: str, *,
                      expected_sessions: Mapping[str, Sequence[date]],
                      event_coverage: set[str],
                      profiles=None) -> NormalizedInput:
    """Load verified pages and map them; this does not execute a strategy.

    The expected sessions must come from a separately selected calendar and
    coverage evidence. event_coverage is a caller declaration, not verification.
    """
    profiles = select_profiles(profiles)
    symbols = tuple(sorted(set().union(*(PROFILE_SYMBOLS[p] for p in profiles))))
    if event_coverage is not None and event_coverage != set(symbols):
        raise ValueError("CORPORATE_ACTION_COVERAGE_UNVERIFIED")
    if set(expected_sessions) != set(symbols):
        raise ValueError("RESEARCH_SYMBOLS_INCOMPLETE")
    bars, actions, events = read_archive_members(root, manifest_sha256, symbols=symbols)
    closes = {symbol: {bar.session: bar.close for bar in bars[symbol]} for symbol in symbols}
    if event_coverage is None:
        qualified, receipt = validate_historical_members(expected_sessions, closes, events, symbols=symbols)
        receipt["qualified_symbols"] = tuple(sorted(symbols))  # Member checks only; not event completeness.
    else:
        qualified, receipt = qualify_historical_input(
            expected_sessions, closes, events, event_coverage=event_coverage, symbols=symbols,
        )
    start = max(expected_sessions[s][0] for s in symbols)
    end = min(expected_sessions[s][-1] for s in symbols)
    common = tuple(day for day in expected_sessions[symbols[0]] if start <= day <= end)
    if not common or any(tuple(d for d in expected_sessions[s] if start <= d <= end) != common
                         for s in symbols):
        raise ValueError("COMMON_SESSIONS_NOT_ALIGNED")
    core_sessions = {}
    for profile, required in (("TQQQ", ("QQQ", "TQQQ")), ("SOXL", ("SOXL", "SOXX"))):
        if profile not in profiles:
            continue
        first = max(expected_sessions[s][0] for s in required)
        last = min(expected_sessions[s][-1] for s in required)
        days = tuple(d for d in expected_sessions[required[0]] if first <= d <= last)
        if not days or any(tuple(d for d in expected_sessions[s] if first <= d <= last) != days
                           for s in required):
            raise ValueError(f"CORE_SESSIONS_NOT_ALIGNED:{profile}")
        if common[0] not in days or common[-1] not in days:
            raise ValueError(f"CORE_REPLAY_WINDOW_MISMATCH:{profile}")
        core_sessions[profile] = days
    session_set = set(common)
    window_events = {day: values for day, values in qualified.items() if day in session_set}
    receipt.update({
        "manifest_sha256": manifest_sha256,
        "calendar_and_event_coverage": ("INDEPENDENT_SPLIT_COVERAGE_UNPROVEN" if event_coverage is None
                                        else "CALLER_DECLARED_NOT_AUTHENTICATED"),
        "common_window": (start, end),
        "events_outside_common_window": sum(event.ex_session not in session_set for event in events),
        "real_replay_authorized": False,
    })
    opening_prices = defaultdict(dict)
    for symbol, rows in bars.items():
        for bar in rows:
            opening_prices[bar.session][symbol] = bar.open
    core_bars = {s: [RawBar(bar.session, bar.open, bar.close) for bar in bars[s]] for s in symbols}
    core_actions = {s: CorporateActions(splits=actions[s].splits, dividends=actions[s].dividends)
                    for s in symbols}
    core_prices = {profile: tuple({s: closes[s][day] for s in symbols if day in closes[s]}
                                  for day in days)
                   for profile, days in core_sessions.items()}
    replay_bounds = {profile: (days.index(common[0]), days.index(common[-1]) + 1)
                     for profile, days in core_sessions.items()}
    return NormalizedInput(common, core_sessions, core_prices, dict(opening_prices), qualified, replay_bounds, tuple({s: closes[s][d] for s in symbols} for d in common),
                           window_events, tuple(events), core_bars, core_actions, receipt)
