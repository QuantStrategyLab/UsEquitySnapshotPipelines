"""Emit a redacted R3-v2 free-source full-OHLCV assurance diagnostic.

Research-only. Separate from private R3-v1 joint evidence. Never publishes a
research input, never auto-promotes, and never accesses brokers or orders.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date

import exchange_calendars as xcals
import pandas as pd
from quant_platform_kit.data.multisource_assurance import (
    DATA_ASSURANCE_STATUS_PARKED,
    DATA_ASSURANCE_STATUS_VERIFIED,
    MultiSourceDailyBarPolicy,
    assess_multisource_daily_bars,
)

from us_equity_snapshot_pipelines.twelve_data_daily import (
    TWELVE_DATA_ADJUSTMENT_BASIS,
    TWELVE_DATA_DAILY_SOURCE_ID,
    observe_twelve_data_adjusted_daily_bars,
)
from us_equity_snapshot_pipelines.yahoo_finance_daily import (
    YAHOO_FINANCE_ADJUSTMENT_BASIS,
    YAHOO_FINANCE_DAILY_SOURCE_ID,
    observe_yahoo_finance_adjusted_daily_bars,
)

_START_DATES = {
    "QQQ": "2022-01-03",
    "TQQQ": "2022-01-03",
    "SOXX": "2022-01-03",
    "SOXL": "2022-01-03",
}
_COVERAGE_SAMPLE_LIMIT = 3
_CALENDAR_ID = "XNYS"
_COVERAGE_INCOMPLETE = "xnys_session_coverage_incomplete"


def _date_cutoff(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date_cutoff must be YYYY-MM-DD") from exc


def _expected_xnys_sessions(*, start_date: str, date_cutoff: str) -> tuple[str, ...]:
    """Return inclusive XNYS completed-session dates for one symbol window."""

    start = date.fromisoformat(start_date)
    cutoff = date.fromisoformat(date_cutoff)
    if start > cutoff:
        return ()
    calendar = xcals.get_calendar(_CALENDAR_ID)
    labels = calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(cutoff))
    sessions = tuple(label.date().isoformat() for label in labels)
    if not sessions or sessions[-1] != date_cutoff:
        raise ValueError("date_cutoff must be a completed XNYS session")
    return sessions


def _redacted_session_coverage(
    *,
    observations: tuple[object, ...],
    start_date: str,
    date_cutoff: str,
) -> dict[str, object]:
    """Return bounded session-coverage evidence without emitting market bars."""

    expected = set(_expected_xnys_sessions(start_date=start_date, date_cutoff=date_cutoff))
    coverage: dict[str, object] = {
        "calendar_id": _CALENDAR_ID,
        "expected_session_count": len(expected),
        "coverage_complete": False,
        "sources": {},
    }
    sources = coverage["sources"]
    assert isinstance(sources, dict)
    complete = True
    for observation in observations:
        source_id = getattr(observation, "source_id", None)
        status = getattr(observation, "status", None)
        snapshot = getattr(observation, "snapshot", None)
        if not isinstance(source_id, str) or not isinstance(status, str):
            complete = False
            continue
        source_coverage: dict[str, object] = {"status": status}
        if snapshot is None:
            complete = False
        else:
            observed = {bar.session_date for bar in snapshot.bars}
            missing = sorted(expected - observed)
            unexpected = sorted(observed - expected)
            if missing or unexpected:
                complete = False
            source_coverage.update(
                {
                    "observed_session_count": len(observed),
                    "first_observed_session": min(observed) if observed else None,
                    "last_observed_session": max(observed) if observed else None,
                    "missing_session_count": len(missing),
                    "unexpected_session_count": len(unexpected),
                    "missing_session_samples": missing[:_COVERAGE_SAMPLE_LIMIT],
                    "unexpected_session_samples": unexpected[:_COVERAGE_SAMPLE_LIMIT],
                }
            )
        sources[source_id] = source_coverage
    coverage["coverage_complete"] = complete and set(sources) == {
        TWELVE_DATA_DAILY_SOURCE_ID,
        YAHOO_FINANCE_DAILY_SOURCE_ID,
    }
    return coverage


def _redacted_ohlcv_agreement(
    *,
    observations: tuple[object, ...],
    price_relative_tolerance: float,
    volume_relative_tolerance: float,
    compare_volume: bool,
) -> dict[str, object]:
    """Report bounded OHLCV comparison metadata without emitting OHLCV values."""

    snapshots = [
        (observation.source_id, observation.snapshot)
        for observation in observations
        if getattr(observation, "snapshot", None) is not None
    ]
    if len(snapshots) != 2:
        return {"status": "NOT_COMPARABLE", "compare_volume": compare_volume}
    (_left_source, left), (_right_source, right) = snapshots
    assert left is not None and right is not None
    left_by_session = {bar.session_date: bar for bar in left.bars}
    right_by_session = {bar.session_date: bar for bar in right.bars}
    if set(left_by_session) != set(right_by_session):
        return {"status": "SESSION_COVERAGE_MISMATCH", "compare_volume": compare_volume}

    max_price_relative_delta = 0.0
    max_volume_relative_delta = 0.0
    first_divergent_session: str | None = None
    divergent_fields: set[str] = set()
    for session_date in sorted(left_by_session):
        left_bar = left_by_session[session_date]
        right_bar = right_by_session[session_date]
        for field_name in ("open", "high", "low", "close"):
            left_value = float(getattr(left_bar, field_name))
            right_value = float(getattr(right_bar, field_name))
            relative_delta = abs(left_value - right_value) / max(abs(left_value), abs(right_value), 1e-12)
            max_price_relative_delta = max(max_price_relative_delta, relative_delta)
            if relative_delta > price_relative_tolerance:
                divergent_fields.add(field_name)
                if first_divergent_session is None:
                    first_divergent_session = session_date
        if compare_volume:
            volume_delta = abs(float(left_bar.volume) - float(right_bar.volume)) / max(
                abs(float(left_bar.volume)), abs(float(right_bar.volume)), 1e-12
            )
            max_volume_relative_delta = max(max_volume_relative_delta, volume_delta)
            if volume_delta > volume_relative_tolerance:
                divergent_fields.add("volume")
                if first_divergent_session is None:
                    first_divergent_session = session_date
    return {
        "status": "COMPARED",
        "compare_volume": compare_volume,
        "price_relative_tolerance": price_relative_tolerance,
        "volume_relative_tolerance": volume_relative_tolerance,
        "max_price_relative_delta": max_price_relative_delta,
        "max_volume_relative_delta": max_volume_relative_delta,
        "first_ohlcv_divergent_session": first_divergent_session,
        "ohlcv_divergent_fields": sorted(divergent_fields),
    }


def _apply_coverage_gate(report: dict[str, object], *, coverage_complete: bool) -> dict[str, object]:
    if coverage_complete:
        return report
    findings = list(report.get("findings") or [])
    if _COVERAGE_INCOMPLETE not in findings:
        findings.append(_COVERAGE_INCOMPLETE)
    report["findings"] = findings
    report["can_publish_research_input"] = False
    if report.get("status") == DATA_ASSURANCE_STATUS_VERIFIED:
        report["status"] = "DEGRADED"
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-cutoff", required=True, type=_date_cutoff)
    args = parser.parse_args(argv)

    if TWELVE_DATA_ADJUSTMENT_BASIS != YAHOO_FINANCE_ADJUSTMENT_BASIS:
        raise RuntimeError("configured daily sources do not share an adjustment basis")

    # Fail closed before provider calls when the cutoff is not a completed XNYS session.
    _expected_xnys_sessions(start_date=_START_DATES["QQQ"], date_cutoff=args.date_cutoff)

    reports: dict[str, dict[str, object]] = {}
    for symbol, start_date in _START_DATES.items():
        observations = (
            observe_twelve_data_adjusted_daily_bars(
                api_key=os.environ.get("TWELVE_DATA_API_KEY"),
                symbol=symbol,
                start_date=start_date,
                date_cutoff=args.date_cutoff,
            ),
            observe_yahoo_finance_adjusted_daily_bars(
                symbol=symbol,
                start_date=start_date,
                date_cutoff=args.date_cutoff,
            ),
        )
        # QPK defaults already gate full OHLC plus volume; keep them explicit.
        policy = MultiSourceDailyBarPolicy(
            scope_id=f"r3_v2_free_source_{symbol.lower()}",
            symbol=symbol,
            date_cutoff=args.date_cutoff,
            adjustment_basis=TWELVE_DATA_ADJUSTMENT_BASIS,
            required_source_ids=(TWELVE_DATA_DAILY_SOURCE_ID, YAHOO_FINANCE_DAILY_SOURCE_ID),
            required_price_fields=("open", "high", "low", "close"),
            compare_volume=True,
        )
        report = assess_multisource_daily_bars(policy, observations).to_diagnostic()
        coverage = _redacted_session_coverage(
            observations=observations,
            start_date=start_date,
            date_cutoff=args.date_cutoff,
        )
        report = _apply_coverage_gate(report, coverage_complete=bool(coverage["coverage_complete"]))
        report["session_coverage"] = coverage
        report["ohlcv_agreement"] = _redacted_ohlcv_agreement(
            observations=observations,
            price_relative_tolerance=policy.price_relative_tolerance,
            volume_relative_tolerance=policy.volume_relative_tolerance,
            compare_volume=policy.compare_volume,
        )
        reports[symbol] = report

    if all(
        report["status"] == DATA_ASSURANCE_STATUS_VERIFIED and report["can_publish_research_input"]
        for report in reports.values()
    ):
        status = "VERIFIED"
    elif all(report["status"] == DATA_ASSURANCE_STATUS_PARKED for report in reports.values()):
        status = "PARKED"
    else:
        status = "NOT_VERIFIED"

    diagnostic = {
        "schema_version": "qsl.r3_v2_free_source_assurance_diagnostic.v1",
        "date_cutoff": args.date_cutoff,
        "status": status,
        "can_promote": False,
        "auto_promote": False,
        "adjustment_basis": TWELVE_DATA_ADJUSTMENT_BASIS,
        "required_source_ids": [
            TWELVE_DATA_DAILY_SOURCE_ID,
            YAHOO_FINANCE_DAILY_SOURCE_ID,
        ],
        "required_price_fields": ["open", "high", "low", "close"],
        "compare_volume": True,
        "reports": reports,
    }
    print(
        "R3_V2_FREE_SOURCE_ASSURANCE_DIAGNOSTIC="
        + json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
