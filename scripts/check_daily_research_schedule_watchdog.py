#!/usr/bin/env python3
"""Check only GitHub Actions schedule availability for daily research."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.daily_research_schedule_watchdog import (
    DailyResearchScheduleWatchdogError,
    WATCHDOG_OBSERVED,
    build_daily_research_schedule_watchdog_summary,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid_arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--expected-utc-date", required=True)
    parser.add_argument("--tqqq-workflow-runs", type=Path, required=True)
    parser.add_argument("--soxl-workflow-runs", type=Path, required=True)
    return parser.parse_args(argv)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise DailyResearchScheduleWatchdogError("invalid_watchdog_input") from exc


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        summary = build_daily_research_schedule_watchdog_summary(
            expected_utc_date=args.expected_utc_date,
            tqqq_workflow_runs_response=_read_json(args.tqqq_workflow_runs),
            soxl_workflow_runs_response=_read_json(args.soxl_workflow_runs),
        )
    except (DailyResearchScheduleWatchdogError, ValueError):
        print(
            json.dumps(
                {
                    "schema_version": "qsl.daily-research-schedule-watchdog.v1",
                    "status": "PARKED",
                    "reason_codes": ["WATCHDOG_INPUT_INVALID"],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0 if summary["status"] == WATCHDOG_OBSERVED else 1


if __name__ == "__main__":
    raise SystemExit(main())
