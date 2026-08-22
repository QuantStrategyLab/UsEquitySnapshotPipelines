#!/usr/bin/env python3
"""Build a sanitized portfolio research-readiness record from terminal artifacts."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from us_equity_snapshot_pipelines.research.portfolio_candidate_readiness import (
    build_component_observation,
    build_portfolio_candidate_readiness,
)
from us_equity_snapshot_pipelines.research.strategy_candidate_registry import (
    SOXL_SOXX_CORE_ONLY_P2_V3,
    TQQQ_CORE_ONLY_P2_V5,
)


def _json_file(value: str) -> dict[str, object]:
    try:
        loaded = json.loads(Path(value).read_bytes())
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise argparse.ArgumentTypeError("terminal artifact must be a JSON object") from exc
    if not isinstance(loaded, dict):
        raise argparse.ArgumentTypeError("terminal artifact must be a JSON object")
    return loaded


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tqqq-p1", required=True, type=_json_file)
    parser.add_argument("--tqqq-p3", type=_json_file)
    parser.add_argument("--soxl-p1", required=True, type=_json_file)
    parser.add_argument("--soxl-p3", type=_json_file)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = build_portfolio_candidate_readiness(
        components=(
            build_component_observation(
                candidate=TQQQ_CORE_ONLY_P2_V5,
                p1_terminal=args.tqqq_p1,
                p3_terminal=args.tqqq_p3,
            ),
            build_component_observation(
                candidate=SOXL_SOXX_CORE_ONLY_P2_V3,
                p1_terminal=args.soxl_p1,
                p3_terminal=args.soxl_p3,
            ),
        ),
        observed_at=args.observed_at,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(
        json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )
    print(json.dumps({"status": result["status"], "readiness_sha256": result["readiness_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
