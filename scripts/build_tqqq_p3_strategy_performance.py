#!/usr/bin/env python3
"""Write the sanitized P3 performance observation for research automation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_strategy_performance import (
    build_tqqq_p3_strategy_performance,
    canonical_tqqq_p3_strategy_performance_bytes,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-package", required=True, type=Path)
    parser.add_argument("--expected-evidence-sha256", required=True)
    parser.add_argument("--producer-revision", required=True)
    parser.add_argument("--computed-at", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    try:
        evidence = json.loads(args.evidence_package.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid P3 evidence package") from exc
    performance = build_tqqq_p3_strategy_performance(
        evidence_package=evidence,
        expected_evidence_sha256=args.expected_evidence_sha256,
        producer_revision=args.producer_revision,
        computed_at=args.computed_at,
    )
    args.output.write_bytes(canonical_tqqq_p3_strategy_performance_bytes(performance))


if __name__ == "__main__":
    main()
