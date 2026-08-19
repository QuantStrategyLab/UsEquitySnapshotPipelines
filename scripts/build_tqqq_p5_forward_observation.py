#!/usr/bin/env python3
"""Create one bounded P5-ready forward observation from daily research inputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p5_forward_observation import (
    TqqqP5ForwardObservationError,
    build_tqqq_p5_forward_observation,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _read_json(path: Path, label: str) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TqqqP5ForwardObservationError(f"invalid {label}") from exc


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--daily-research-status", type=Path, required=True)
    parser.add_argument("--producer-revision", required=True)
    parser.add_argument("--produced-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        observation = build_tqqq_p5_forward_observation(
            snapshot_root=args.snapshot_root,
            config_payload=_read_json(args.config, "frozen config"),
            daily_research_status=_read_json(args.daily_research_status, "daily research status"),
            producer_revision=args.producer_revision,
            produced_at=args.produced_at,
        )
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(observation, sort_keys=True, separators=(",", ":")))
    except (OSError, TqqqP5ForwardObservationError, ValueError):
        print('{"status":"PARKED","stage":"forward_observation"}')
        return 2
    print(
        json.dumps(
            {
                "forward_observation_sha256": observation["forward_observation_sha256"],
                "status": "FORWARD_OBSERVATION_RECORDED",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
