"""Data-only publisher for injected TQQQ core-only IBKR historical-bars providers."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    CANDIDATE_ID,
    TqqqCoreOnlyHistoricalBarsProvider,
    TqqqCoreOnlyP1BindingError,
    publish_tqqq_core_only_p1_inputs as _publish,
)


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish injected TQQQ core-only P1 data-only inputs.")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    return parser.parse_args(argv)


def publish_tqqq_core_only_p1_inputs(
    provider: TqqqCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Run one four-call transaction through the only accepted injected provider port."""
    return _publish(provider, output_root=output_root, observed_at=observed_at, producer=producer)


def main(
    argv: list[str] | None = None,
    *,
    provider: TqqqCoreOnlyHistoricalBarsProvider | None = None,
    producer: Mapping[str, object] | None = None,
) -> int:
    args = _arguments(argv)
    if provider is None or producer is None:
        print('{"status":"PARKED"}')
        return 2
    try:
        result = publish_tqqq_core_only_p1_inputs(
            provider,
            output_root=args.output_root,
            observed_at=args.observed_at,
            producer=producer,
        )
    except TqqqCoreOnlyP1BindingError:
        print('{"status":"PARKED"}')
        return 2
    print(
        json.dumps(
            {
                "candidate_id": CANDIDATE_ID,
                "manifest_sha256": result["manifest_sha256"],
                "status": result["status"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
