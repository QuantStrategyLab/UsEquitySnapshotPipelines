#!/usr/bin/env python3
"""Write a sanitized, bounded P3 replay-recovery plan from daily metadata."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_recovery import (
    TqqqP3RecoveryError,
    build_tqqq_p3_recovery_plan,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _read_json(path: Path) -> object:
    def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicates)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise TqqqP3RecoveryError("invalid daily research status") from exc


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--daily-research-status", type=Path, required=True)
    parser.add_argument("--recovery-record-exists", choices=("true", "false"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        plan = build_tqqq_p3_recovery_plan(
            daily_research_status=_read_json(args.daily_research_status),
            recovery_record_exists=args.recovery_record_exists == "true",
        )
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    except (OSError, TqqqP3RecoveryError, ValueError):
        print('{"reason_code":"RECOVERY_METADATA_INVALID","status":"PARKED"}')
        return 2
    print(json.dumps({"reason_code": plan["reason_code"], "status": plan["status"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
