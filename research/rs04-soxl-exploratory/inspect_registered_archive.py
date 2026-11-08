"""Read already-accessible RS-04 members and emit only structural evidence."""
import argparse
from collections import Counter
from datetime import date
import json
from math import isfinite
from pathlib import Path
import re

from archive_bridge import read_archive_members
from input_qualification import RESEARCH_SYMBOLS, validate_historical_members


END_SESSION = date(2026, 8, 25)


# Issuer evidence is a cross-check only. It is never replay input.
TQQQ_SPLIT_SAMPLES = (
    (date(2022, 1, 13), date(2021, 12, 22), 2.0),
    (date(2025, 11, 20), date(2025, 11, 4), 2.0),
)
SOXL_DIVIDEND_SAMPLES = (
    (date(2025, 3, 25), date(2025, 4, 1)),
    (date(2025, 6, 24), date(2025, 7, 1)),
    (date(2025, 9, 23), date(2025, 9, 30)),
)


def _issuer_sample_checks(events):
    splits, dividends = {}, {}
    for event in events:
        if event.symbol == "TQQQ" and event.kind == "split":
            splits[event.ex_session] = event.value
        if event.symbol == "SOXL" and event.kind == "dividend":
            dividends.setdefault(event.ex_session, set()).add(event.pay_session)
    results = []
    for day, published, ratio in TQQQ_SPLIT_SAMPLES:
        status = "MISSING" if day not in splits else "MATCHED" if splits[day] == ratio else "DIFFERENT"
        results.append({"symbol": "TQQQ", "kind": "split", "effective_date": day.isoformat(),
                        "issuer_published_on": published.isoformat(), "status": status})
    for day, payment in SOXL_DIVIDEND_SAMPLES:
        status = "MISSING" if day not in dividends else "MATCHED" if payment in dividends[day] else "DIFFERENT"
        results.append({"symbol": "SOXL", "kind": "dividend", "ex_date": day.isoformat(),
                        "issuer_payable_date": payment.isoformat(), "status": status})
    return results


def registered_calendar():
    import exchange_calendars

    calendar = exchange_calendars.get_calendar("XNYS")
    expected = {}
    for symbol in sorted(RESEARCH_SYMBOLS):
        start, count = (date(2022, 12, 28), 917) if symbol == "BOXX" else (date(2021, 12, 1), 1187)
        sessions = tuple(session.date() for session in calendar.sessions_in_range(start, END_SESSION))
        if (len(sessions), sessions[0], sessions[-1]) != (count, start, END_SESSION):
            raise ValueError("REGISTERED_CALENDAR_MISMATCH")
        expected[symbol] = sessions
    return expected, exchange_calendars.__version__


def inspect_members(root, manifest_sha256, expected):
    bars, _, events = read_archive_members(root, manifest_sha256)
    closes = {symbol: {bar.session: bar.close for bar in rows} for symbol, rows in bars.items()}
    _, receipt = validate_historical_members(expected, closes, events)
    event_counts = Counter((event.symbol, event.kind) for event in events)
    return {
        "status": "STRUCTURE_CHECKED",
        "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1",
        "feed": "sip", "price_adjustment": "raw", "manifest_sha256": manifest_sha256,
        "symbols": {symbol: {
            "first_session": rows[0].session.isoformat(), "last_session": rows[-1].session.isoformat(),
            "sessions": receipt["per_symbol_sessions"][symbol],
            "invalid_open_rows": sum(not isfinite(bar.open) or bar.open <= 0 for bar in rows),
            "present_dividend_rows": event_counts[symbol, "dividend"],
            "present_split_rows": event_counts[symbol, "split"],
        } for symbol, rows in sorted(bars.items())},
        "present_payment_dates": "PARSED_NOT_INDEPENDENTLY_VERIFIED",
        "issuer_sample_checks": _issuer_sample_checks(events),
        "present_event_availability_unknown_or_late": receipt["event_availability_gaps"],
        "event_coverage": "UNVERIFIED",
        "strict_pit_verified": False,
        "effective_configuration": "UNVERIFIED",
        "real_replay_authorized": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        expected, calendar_version = registered_calendar()
        if not re.fullmatch(r"[0-9a-f]{64}", args.manifest_sha256):
            raise ValueError("MANIFEST_MISMATCH")
        result = inspect_members(args.archive_dir, args.manifest_sha256, expected)
        result.update(calendar="XNYS", calendar_version=calendar_version)
    except ImportError:
        result = {"status": "PARKED", "reason_code": "XNYS_CALENDAR_UNAVAILABLE"}
    except (KeyError, OSError, TypeError, ValueError, ZeroDivisionError) as exc:
        code, _, detail = str(exc).partition(":")
        result = {"status": "PARKED", "reason_code": (
            code if re.fullmatch(r"[A-Z][A-Z0-9_]*", code) else "ARCHIVE_MEMBER_INVALID"
        )}
        symbol = detail.partition(":")[0]
        if symbol in RESEARCH_SYMBOLS:
            result["symbol"] = symbol
    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0 if result["status"] == "STRUCTURE_CHECKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
