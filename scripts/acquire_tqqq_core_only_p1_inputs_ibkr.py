"""Data-only publisher for injected TQQQ core-only IBKR historical-bars providers."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    build_request_bound_ibkr_app,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    CANDIDATE_ID,
    TqqqCoreOnlyHistoricalBarsProvider,
    TqqqCoreOnlyP1BindingError,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    publish_tqqq_core_only_p1_inputs as _publish,
)


_FAILURE_CLASSES = frozenset({"data_only_acquisition_failed"})
_SOURCE_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class _SanitizedLifecycleProvider:
    """Track only closed request lifecycle fields for terminal CLI output."""

    def __init__(self, provider: TqqqCoreOnlyHistoricalBarsProvider) -> None:
        self._provider = provider
        self._count = 0
        self._submitted = False
        self._completed = False

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> dict[str, object]:
        self._count += 1
        self._submitted = True
        self._completed = False
        response = self._provider.fetch_historical_bars(
            symbol=symbol,
            calendar_id=calendar_id,
            timezone=timezone,
            adjustment_policy=adjustment_policy,
            feed=feed,
            date_cutoff=date_cutoff,
        )
        self._completed = True
        return response

    def failure_payload(self, producer: Mapping[str, object]) -> dict[str, object]:
        failure_class = "data_only_acquisition_failed"
        if failure_class not in _FAILURE_CLASSES:
            raise ValueError("invalid sanitized failure class")
        source_commit = producer.get("commit_sha")
        return {
            "candidate_id": CANDIDATE_ID,
            "failure_class": failure_class,
            "request_id": None,
            "event_type": "historical_bars",
            "submitted": self._submitted,
            "completed": self._completed,
            "count": self._count,
            "source_commit": (
                source_commit
                if isinstance(source_commit, str) and _SOURCE_COMMIT.fullmatch(source_commit)
                else None
            ),
            "status": "PARKED",
        }


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


def build_tqqq_core_only_ibkr_callback_app(
    *,
    client_type: type[Any],
    wrapper_type: type[Any],
    contract_type: type[Any],
) -> Any:
    """Build the official IBKR callback boundary without connecting or requesting data."""
    return build_request_bound_ibkr_app(
        client_type=client_type,
        wrapper_type=wrapper_type,
        contract_type=contract_type,
    )


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
    lifecycle_provider = _SanitizedLifecycleProvider(provider)
    try:
        result = publish_tqqq_core_only_p1_inputs(
            lifecycle_provider,
            output_root=args.output_root,
            observed_at=args.observed_at,
            producer=producer,
        )
    except TqqqCoreOnlyP1BindingError:
        print(json.dumps(lifecycle_provider.failure_payload(producer), sort_keys=True, separators=(",", ":")))
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
