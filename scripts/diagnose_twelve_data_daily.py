"""Emit a redacted availability report for Twelve Data adjusted daily bars."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date

from us_equity_snapshot_pipelines.twelve_data_daily import (
    SOURCE_OBSERVATION_READY,
    observe_twelve_data_adjusted_daily_bars,
)

_START_DATES = {"SOXL": "2022-01-03", "SOXX": "2022-01-03", "BOXX": "2022-12-28"}


def _date_cutoff(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date_cutoff must be YYYY-MM-DD") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-cutoff", required=True, type=_date_cutoff)
    args = parser.parse_args(argv)

    observations = {
        symbol: observe_twelve_data_adjusted_daily_bars(
            api_key=os.environ.get("TWELVE_DATA_API_KEY"),
            symbol=symbol,
            start_date=start_date,
            date_cutoff=args.date_cutoff,
        )
        for symbol, start_date in _START_DATES.items()
    }
    statuses = {symbol: observation.status for symbol, observation in observations.items()}
    if all(status == SOURCE_OBSERVATION_READY for status in statuses.values()):
        status = "TWELVE_DATA_DAILY_ACCESS_OK"
    elif any(status == SOURCE_OBSERVATION_READY for status in statuses.values()):
        status = "TWELVE_DATA_DAILY_ACCESS_PARTIALLY_AVAILABLE"
    else:
        status = "TWELVE_DATA_DAILY_ACCESS_UNAVAILABLE"
    diagnostic = {
        "schema_version": "qsl.twelve_data_daily_access_diagnostic.v1",
        "date_cutoff": args.date_cutoff,
        "status": status,
        "symbol_statuses": statuses,
        "symbol_reason_codes": {
            symbol: list(observation.reason_codes)
            for symbol, observation in observations.items()
            if observation.reason_codes
        },
        "symbol_snapshot_sha256": {
            symbol: observation.snapshot.snapshot_sha256
            for symbol, observation in observations.items()
            if observation.snapshot is not None
        },
    }
    print("TWELVE_DATA_DAILY_DIAGNOSTIC=" + json.dumps(diagnostic, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
