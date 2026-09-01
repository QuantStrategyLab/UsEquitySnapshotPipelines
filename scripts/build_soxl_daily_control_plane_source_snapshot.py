"""Write the bounded SOXL P1/P3 source snapshot for the control plane."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_daily_control_plane_source import (
    build_soxl_daily_control_plane_source_snapshot,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--computed-at", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--p1-status", required=True)
    parser.add_argument("--p1-reason-code", default="")
    parser.add_argument("--p1-manifest-sha256", default="")
    parser.add_argument("--p2-config-sha256", required=True)
    parser.add_argument("--p3-status", default="")
    parser.add_argument("--p3-evidence-sha256", default="")
    parser.add_argument("--p3-failure-class", default="")
    parser.add_argument("--decision-projection-status", default="NOT_ATTEMPTED")
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    snapshot = build_soxl_daily_control_plane_source_snapshot(
        computed_at=args.computed_at,
        source_revision=args.source_revision,
        p1_status=args.p1_status,
        p1_reason_code=args.p1_reason_code,
        p1_manifest_sha256=args.p1_manifest_sha256,
        p2_config_sha256=args.p2_config_sha256,
        p3_status=args.p3_status,
        p3_evidence_sha256=args.p3_evidence_sha256,
        p3_failure_class=args.p3_failure_class,
        decision_projection_status=args.decision_projection_status,
    )
    args.output.write_text(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
