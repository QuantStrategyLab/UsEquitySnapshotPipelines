#!/usr/bin/env python3
"""Run the TQQQ P2 v2 adapter contract proof on synthetic input only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_p2_v2_synthetic_evidence import (
    run_synthetic_p2_v2_evidence,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = {
        "input_manifest": json.loads((args.snapshot_root / "manifest.json").read_bytes()),
        "binding": json.loads((args.snapshot_root / "binding.json").read_bytes()),
        "bars": json.loads((args.snapshot_root / "bars.json").read_bytes()),
    }
    candidate = json.loads(args.config.read_bytes())
    result = run_synthetic_p2_v2_evidence(
        input_payload=payload, candidate=candidate, output_dir=args.output_dir
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
