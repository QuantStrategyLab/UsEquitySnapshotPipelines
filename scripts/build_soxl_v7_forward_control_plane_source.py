#!/usr/bin/env python3
"""Project one existing SOXL V7 non-live record for the control-plane sync API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation import (
    SoxlV7NonliveForwardObservationError,
    build_soxl_v7_forward_control_plane_source,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--generated-at", required=True)
    args = parser.parse_args()
    try:
        record = json.loads(args.record.read_text(encoding="utf-8"))
        snapshot = build_soxl_v7_forward_control_plane_source(
            record,
            generated_at=args.generated_at,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, SoxlV7NonliveForwardObservationError):
        raise SystemExit("invalid SOXL V7 forward control-plane source") from None
    args.output.write_text(
        json.dumps(
            snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
