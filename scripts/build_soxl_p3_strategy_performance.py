"""Write the sanitized SOXL P3 performance observation for research automation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_p3_strategy_performance import (
    build_soxl_p3_strategy_performance,
    canonical_soxl_p3_strategy_performance_bytes,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-summary", required=True, type=Path)
    parser.add_argument("--expected-evidence-sha256", required=True)
    parser.add_argument("--producer-revision", required=True)
    parser.add_argument("--computed-at", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    try:
        summary = json.loads(args.evidence_summary.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid SOXL P3 evidence summary") from exc
    performance = build_soxl_p3_strategy_performance(
        evidence_summary=summary,
        expected_evidence_sha256=args.expected_evidence_sha256,
        producer_revision=args.producer_revision,
        computed_at=args.computed_at,
    )
    args.output.write_bytes(canonical_soxl_p3_strategy_performance_bytes(performance))


if __name__ == "__main__":
    main()
