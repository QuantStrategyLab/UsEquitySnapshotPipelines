"""Emit a redacted, non-publishing daily multi-source assurance diagnostic."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date

from quant_platform_kit.data.multisource_assurance import (
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
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import (
    expected_soxl_core_only_sessions,
)

_START_DATES = {"SOXL": "2022-01-03", "SOXX": "2022-01-03", "BOXX": "2022-12-28"}
_COVERAGE_SAMPLE_LIMIT = 3


def _date_cutoff(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date_cutoff must be YYYY-MM-DD") from exc


def _redacted_session_coverage(*, observations: tuple[object, ...], symbol: str, date_cutoff: str) -> dict[str, object]:
    """Return bounded session-coverage evidence without emitting market bars.

    An assurance finding deliberately names only the failure class.  This
    companion is for diagnosing a source/calendar disagreement safely: it
    contains session dates and counts only, never prices, volumes, response
    payloads, credentials, or source URLs.
    """

    expected = {session.isoformat() for session in expected_soxl_core_only_sessions(date_cutoff)[symbol]}
    coverage: dict[str, object] = {"expected_session_count": len(expected), "sources": {}}
    sources = coverage["sources"]
    assert isinstance(sources, dict)
    for observation in observations:
        source_id = getattr(observation, "source_id", None)
        status = getattr(observation, "status", None)
        snapshot = getattr(observation, "snapshot", None)
        if not isinstance(source_id, str) or not isinstance(status, str):
            continue
        source_coverage: dict[str, object] = {"status": status}
        if snapshot is not None:
            observed = {bar.session_date for bar in snapshot.bars}
            missing = sorted(expected - observed)
            unexpected = sorted(observed - expected)
            source_coverage.update(
                {
                    "observed_session_count": len(observed),
                    "first_observed_session": min(observed),
                    "last_observed_session": max(observed),
                    "missing_session_count": len(missing),
                    "unexpected_session_count": len(unexpected),
                    "missing_session_samples": missing[:_COVERAGE_SAMPLE_LIMIT],
                    "unexpected_session_samples": unexpected[:_COVERAGE_SAMPLE_LIMIT],
                }
            )
        sources[source_id] = source_coverage
    return coverage


def _redacted_price_agreement(
    *, observations: tuple[object, ...], price_relative_tolerance: float
) -> dict[str, object]:
    """Report bounded comparison metadata without emitting OHLCV values."""

    snapshots = [
        (observation.source_id, observation.snapshot)
        for observation in observations
        if getattr(observation, "snapshot", None) is not None
    ]
    if len(snapshots) != 2:
        return {"status": "NOT_COMPARABLE"}
    (_left_source, left), (_right_source, right) = snapshots
    assert left is not None and right is not None
    left_by_session = {bar.session_date: bar for bar in left.bars}
    right_by_session = {bar.session_date: bar for bar in right.bars}
    if set(left_by_session) != set(right_by_session):
        return {"status": "SESSION_COVERAGE_MISMATCH"}

    max_relative_delta = 0.0
    first_divergent_session: str | None = None
    divergent_fields: set[str] = set()
    for session_date in sorted(left_by_session):
        left_bar = left_by_session[session_date]
        right_bar = right_by_session[session_date]
        for field_name in ("open", "high", "low", "close"):
            left_value = float(getattr(left_bar, field_name))
            right_value = float(getattr(right_bar, field_name))
            relative_delta = abs(left_value - right_value) / max(abs(left_value), abs(right_value), 1e-12)
            max_relative_delta = max(max_relative_delta, relative_delta)
            if relative_delta > price_relative_tolerance:
                divergent_fields.add(field_name)
                if first_divergent_session is None:
                    first_divergent_session = session_date
    return {
        "status": "COMPARED",
        "price_relative_tolerance": price_relative_tolerance,
        "max_price_relative_delta": max_relative_delta,
        "first_price_divergent_session": first_divergent_session,
        "price_divergent_fields": sorted(divergent_fields),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-cutoff", required=True, type=_date_cutoff)
    args = parser.parse_args(argv)

    if TWELVE_DATA_ADJUSTMENT_BASIS != YAHOO_FINANCE_ADJUSTMENT_BASIS:
        raise RuntimeError("configured daily sources do not share an adjustment basis")

    reports = {}
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
        policy = MultiSourceDailyBarPolicy(
            scope_id=f"uesp_shadow_daily_{symbol.lower()}",
            symbol=symbol,
            date_cutoff=args.date_cutoff,
            adjustment_basis=TWELVE_DATA_ADJUSTMENT_BASIS,
            required_source_ids=(TWELVE_DATA_DAILY_SOURCE_ID, YAHOO_FINANCE_DAILY_SOURCE_ID),
        )
        report = assess_multisource_daily_bars(policy, observations).to_diagnostic()
        report["session_coverage"] = _redacted_session_coverage(
            observations=observations,
            symbol=symbol,
            date_cutoff=args.date_cutoff,
        )
        report["price_agreement"] = _redacted_price_agreement(
            observations=observations,
            price_relative_tolerance=policy.price_relative_tolerance,
        )
        reports[symbol] = report

    status = (
        "MULTISOURCE_DAILY_ASSURANCE_VERIFIED"
        if all(report["status"] == DATA_ASSURANCE_STATUS_VERIFIED for report in reports.values())
        else "MULTISOURCE_DAILY_ASSURANCE_NOT_VERIFIED"
    )
    diagnostic = {
        "schema_version": "qsl.multisource_daily_assurance_diagnostic.v1",
        "date_cutoff": args.date_cutoff,
        "status": status,
        "reports": reports,
    }
    print("MULTISOURCE_DAILY_ASSURANCE_DIAGNOSTIC=" + json.dumps(diagnostic, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
