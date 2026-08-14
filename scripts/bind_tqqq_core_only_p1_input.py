"""Publish the static P1 data-only binding for the frozen TQQQ core candidate."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    CANDIDATE_ID,
    binding_sha256,
    build_tqqq_core_only_p1_binding,
    canonical_binding_bytes,
)
from us_equity_snapshot_pipelines.tqqq_r1_snapshot import _publish_noreplace


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a static TQQQ core-only P1 data binding.")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    binding = build_tqqq_core_only_p1_binding()
    raw = canonical_binding_bytes(binding)
    destination = args.output
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".tqqq-core-only-p1-") as temporary_dir:
            temporary = Path(temporary_dir)
            (temporary / "binding.json").write_bytes(raw)
            _publish_noreplace(temporary, destination)
    except OSError:
        print('{"status":"PARKED"}')
        return 2
    print(
        json.dumps(
            {
                "binding_sha256": binding_sha256(binding),
                "candidate_id": CANDIDATE_ID,
                "status": "P1_DATA_ONLY_BINDING_COMPLETE",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
