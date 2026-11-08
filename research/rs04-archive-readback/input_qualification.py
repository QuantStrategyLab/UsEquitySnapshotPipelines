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


RESEARCH_SYMBOLS = {"TQQQ", "QQQ", "QQQM", "SOXL", "SOXX", "BOXX"}


def qualify_historical_input(
    expected: Mapping[str, Sequence[date]],
    bars: Mapping[str, Mapping[date, float]],
    events: Sequence[Event],
    *,
    event_coverage: set[str],
):
    """Validate inputs for the bridge when the caller declares event coverage."""
    if event_coverage != RESEARCH_SYMBOLS:
        raise ValueError("CORPORATE_ACTION_COVERAGE_UNVERIFIED")
    return validate_historical_members(expected, bars, events)


def validate_historical_members(
    expected: Mapping[str, Sequence[date]],
    bars: Mapping[str, Mapping[date, float]],
    events: Sequence[Event],
):
    """Verify observed member dates and values without asserting coverage of events."""
    if set(expected) != RESEARCH_SYMBOLS or set(bars) != RESEARCH_SYMBOLS:
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
        if event.symbol not in RESEARCH_SYMBOLS or event.ex_session not in bars[event.symbol]:
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
