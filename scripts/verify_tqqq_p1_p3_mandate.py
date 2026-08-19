#!/usr/bin/env python3
"""Resolve one checked-in TQQQ P1/P3 non-live scope record without exposing its contents."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p1_p3_mandate import (
    TqqqP1P3MandateError,
    load_tqqq_p1_p3_mandate,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--mandates-root", required=True, type=Path)
    parser.add_argument("--mandate-id", required=True)
    try:
        args = parser.parse_args(argv)
        mandate, receipt_sha256 = load_tqqq_p1_p3_mandate(args.mandates_root, args.mandate_id)
    except (SystemExit, TqqqP1P3MandateError):
        print(json.dumps({"status": "PARKED", "reason": "nonlive_scope_record_unavailable"}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "mandate_id": mandate["mandate_id"],
                "mandate_receipt_sha256": receipt_sha256,
                "status": "NONLIVE_SCOPE_RECORD_VERIFIED",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
