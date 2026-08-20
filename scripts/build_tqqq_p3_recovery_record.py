#!/usr/bin/env python3
"""Create one validated, create-only terminal record for a P3 recovery replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_recovery import (
    TqqqP3RecoveryError,
    build_tqqq_p3_recovery_record,
    validate_tqqq_p3_terminal,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _read_json(path: Path, label: str) -> object:
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
        raise TqqqP3RecoveryError(f"invalid {label}") from exc


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--p3-result", type=Path, required=True)
    parser.add_argument("--p3-exit-code", required=True)
    parser.add_argument("--produced-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        exit_code = int(args.p3_exit_code)
        if not 0 <= exit_code <= 255:
            raise ValueError("invalid P3 exit code")
        terminal = validate_tqqq_p3_terminal(_read_json(args.p3_result, "P3 result"))
        if (exit_code == 0) != (terminal["status"] == "EVIDENCE_V2_COMPLETE"):
            raise TqqqP3RecoveryError("P3 exit and terminal status disagree")
        record = build_tqqq_p3_recovery_record(
            plan=_read_json(args.plan, "recovery plan"),
            p3_terminal=terminal,
            produced_at=args.produced_at,
        )
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    except (OSError, TqqqP3RecoveryError, ValueError):
        print('{"status":"PARKED","stage":"recovery_record"}')
        return 2
    print(
        json.dumps(
            {
                "failure_class": record["p3_terminal"].get("failure_class", ""),
                "p3_status": record["p3_terminal"]["status"],
                "recovery_record_sha256": record["recovery_record_sha256"],
                "status": "P3_RECOVERY_RECORDED",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
