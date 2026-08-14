#!/usr/bin/env python3
"""Run the direct, offline-only TQQQ P3 evidence entry point."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_direct import TqqqP3ContractError, run_tqqq_p3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-root", required=True)
    parser.add_argument("--authority-json", required=True)
    parser.add_argument("--output-parent", required=True)
    args = parser.parse_args(argv)
    try:
        authority = json.loads(Path(args.authority_json).read_text(encoding="utf-8"))
        root = run_tqqq_p3(args.snapshot_root, authority, args.output_parent)
    except (OSError, ValueError, TqqqP3ContractError):
        print('{"status":"PARKED"}')
        return 2
    print(json.dumps({"status": "EVIDENCE_V2_COMPLETE", "evidence_root": str(root)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
