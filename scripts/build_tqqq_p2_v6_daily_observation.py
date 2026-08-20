#!/usr/bin/env python3
"""Write a redacted, short-lived v6 observation only after a completed v5 P3."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p2_v6_daily_observation import (
    TqqqP2V6DailyObservationError,
    build_tqqq_p2_v6_daily_observation,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--daily-research-status", type=Path, required=True)
    parser.add_argument("--forward-observation", type=Path, required=True)
    parser.add_argument("--qsp-revision", required=True)
    parser.add_argument("--produced-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise TqqqP2V6DailyObservationError("invalid_artifact_input") from exc


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        status = _read_json(args.daily_research_status)
        forward = _read_json(args.forward_observation)
        if not isinstance(status, dict) or not isinstance(forward, dict):
            raise TqqqP2V6DailyObservationError("invalid_artifact_input")
        observation = build_tqqq_p2_v6_daily_observation(
            snapshot_root=args.snapshot_root,
            daily_research_status=status,
            forward_observation=forward,
            qsp_revision=args.qsp_revision,
            produced_at=args.produced_at,
        )
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(observation, sort_keys=True, separators=(",", ":")))
    except (OSError, TqqqP2V6DailyObservationError, ValueError):
        print('{"stage":"v6_plugin_observation","status":"PARKED"}')
        return 2
    print(
        json.dumps(
            {
                "observation_sha256": observation["observation_sha256"],
                "status": "OBSERVATION_RECORDED",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
