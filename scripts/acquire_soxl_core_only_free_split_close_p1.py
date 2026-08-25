"""Publish injected two-source SOXL v4 split-adjusted-close P1 inputs.

The executable has no default credentials or environment lookup.  A future
non-live controller must explicitly inject both the source observer and
producer metadata.  Without them it emits a sanitized PARKED result and makes
no provider, storage, broker, or strategy call.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import DailyBarSourceObservation

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p1 import (
    SoxlCoreOnlyFreeSplitCloseObserver,
    SoxlCoreOnlyFreeSplitCloseP1Error,
    SoxlCoreOnlyFreeSplitCloseP1UnavailableError,
    publish_soxl_core_only_free_split_close_p1_inputs as _publish,
)
from us_equity_snapshot_pipelines.twelve_data_daily import (
    TWELVE_DATA_DAILY_SOURCE_ID,
    observe_twelve_data_adjusted_daily_bars,
)
from us_equity_snapshot_pipelines.yahoo_finance_daily import (
    YAHOO_FINANCE_DAILY_SOURCE_ID,
    observe_yahoo_finance_adjusted_daily_bars,
)


class TwelveYahooSplitAdjustedCloseObserver:
    """Concrete two-source adapter with a fixed canonical/verifier pairing."""

    def __init__(self, twelve_data_api_key: str) -> None:
        self._twelve_data_api_key = twelve_data_api_key

    def observe_daily_bars(
        self,
        *,
        source_id: str,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> DailyBarSourceObservation:
        if source_id == TWELVE_DATA_DAILY_SOURCE_ID:
            return observe_twelve_data_adjusted_daily_bars(
                api_key=self._twelve_data_api_key,
                symbol=symbol,
                start_date=start_date,
                date_cutoff=date_cutoff,
            )
        if source_id == YAHOO_FINANCE_DAILY_SOURCE_ID:
            return observe_yahoo_finance_adjusted_daily_bars(
                symbol=symbol,
                start_date=start_date,
                date_cutoff=date_cutoff,
            )
        raise SoxlCoreOnlyFreeSplitCloseP1Error("data-only source observation failed")


def publish_soxl_core_only_free_split_close_p1_inputs(
    observer: SoxlCoreOnlyFreeSplitCloseObserver,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    date_cutoff: str,
) -> dict[str, object]:
    """Publish only after the v4 P1 layer verifies both mandatory sources."""
    return _publish(
        observer,
        output_root=output_root,
        observed_at=observed_at,
        producer=producer,
        date_cutoff=date_cutoff,
    )


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--date-cutoff", required=True)
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    observer: SoxlCoreOnlyFreeSplitCloseObserver | None = None,
    producer: Mapping[str, object] | None = None,
) -> int:
    args = _arguments(argv)
    if observer is None or producer is None:
        print(json.dumps({"status": "PARKED"}, sort_keys=True))
        return 2
    try:
        result = publish_soxl_core_only_free_split_close_p1_inputs(
            observer,
            output_root=args.output_root,
            observed_at=args.observed_at,
            producer=producer,
            date_cutoff=args.date_cutoff,
        )
    except SoxlCoreOnlyFreeSplitCloseP1UnavailableError:
        result = {"reason": "INPUT_UNAVAILABLE", "status": "PARKED", "verdict": "INCONCLUSIVE"}
    except SoxlCoreOnlyFreeSplitCloseP1Error:
        result = {"status": "PARKED"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == "P1_FREE_SPLIT_CLOSE_INPUTS_PUBLISHED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
