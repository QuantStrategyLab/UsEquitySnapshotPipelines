"""Input checks for the research bridge, using normalized data supplied by its owner."""
from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Mapping, Sequence


@dataclass(frozen=True)
class Event:
    symbol: str
    ex_session: date
    kind: str
    value: float
    pay_session: date | None = None
    first_available: date | None = None


PROFILE_SYMBOLS = {
    "TQQQ": frozenset({"QQQ", "TQQQ", "QQQM", "BOXX"}),
    "SOXL": frozenset({"SOXL", "SOXX", "BOXX"}),
}
PROFILES = tuple(PROFILE_SYMBOLS)
RESEARCH_SYMBOLS = set().union(*PROFILE_SYMBOLS.values())


def select_profiles(profiles):
    """Return a nonempty, unique selection in canonical order."""
    if profiles is None:
        return PROFILES
    profiles = tuple(profiles)
    if not profiles or len(set(profiles)) != len(profiles) or any(p not in PROFILE_SYMBOLS for p in profiles):
        raise ValueError("INVALID_PROFILES")
    return tuple(p for p in PROFILES if p in profiles)


def qualify_historical_input(
    expected: Mapping[str, Sequence[date]],
    bars: Mapping[str, Mapping[date, float]],
    events: Sequence[Event],
    *,
    event_coverage: set[str],
    symbols=RESEARCH_SYMBOLS,
):
    """Validate inputs for the bridge when the caller declares event coverage."""
    if event_coverage != set(symbols):
        raise ValueError("CORPORATE_ACTION_COVERAGE_UNVERIFIED")
    events_by_day, receipt = validate_historical_members(expected, bars, events, symbols=symbols)
    receipt["qualified_symbols"] = tuple(sorted(symbols))
    return events_by_day, receipt


def validate_historical_members(
    expected: Mapping[str, Sequence[date]],
    bars: Mapping[str, Mapping[date, float]],
    events: Sequence[Event],
    *,
    symbols=RESEARCH_SYMBOLS,
):
    """Verify observed member dates and values without asserting coverage of events."""
    if set(expected) != set(symbols) or set(bars) != set(symbols):
        raise ValueError("RESEARCH_SYMBOLS_INCOMPLETE")
    for symbol, sessions in expected.items():
        if not sessions or any(right <= left for left, right in zip(sessions, sessions[1:])):
            raise ValueError(f"EXPECTED_SESSIONS_INVALID:{symbol}")
        if set(bars[symbol]) != set(sessions):
            raise ValueError(f"BAR_COVERAGE_MISMATCH:{symbol}")
        if any(not isfinite(price) or price <= 0 for price in bars[symbol].values()):
            raise ValueError(f"RAW_CLOSE_INVALID:{symbol}")

    splits = {}
    dividends = {}
    availability_gaps = 0
    for event in events:
        if event.symbol not in symbols or event.ex_session not in bars[event.symbol]:
            raise ValueError("CORPORATE_ACTION_OUTSIDE_BAR_COVERAGE")
        if event.first_available is None or event.first_available > event.ex_session:
            availability_gaps += 1
        if not isfinite(event.value) or event.value <= 0:
            raise ValueError("CORPORATE_ACTION_VALUE_INVALID")
        if event.kind == "split":
            group = splits.setdefault(event.ex_session, {})
            if event.symbol in group:
                raise ValueError("DUPLICATE_SPLIT")
            group[event.symbol] = event.value
        elif event.kind == "dividend":
            if event.pay_session is None or event.pay_session < event.ex_session:
                raise ValueError("DIVIDEND_PAYMENT_SESSION_UNVERIFIED")
            dividends.setdefault(event.ex_session, []).append(
                (event.symbol, event.value, event.pay_session)
            )
        else:
            raise ValueError("CORPORATE_ACTION_TYPE_UNSUPPORTED")

    bridge_events = {
        session: (splits.get(session, {}), tuple(dividends.get(session, ())))
        for session in sorted(set(splits) | set(dividends))
    }
    receipt = {
        "per_symbol_sessions": {s: len(expected[s]) for s in sorted(expected)},
        "present_cash_event_dates_valid": True,
        "event_availability_gaps": availability_gaps,
        # Matched dates do not prove PIT for raw bars, revisions, or the vendor's full event stream.
        "strict_pit_verified": False,
    }
    return bridge_events, receipt
