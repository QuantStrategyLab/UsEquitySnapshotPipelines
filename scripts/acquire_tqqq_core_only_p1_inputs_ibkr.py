"""Data-only publisher for injected TQQQ core-only IBKR historical-bars providers."""

from __future__ import annotations

import argparse
import json
import math
import re
import threading
import time
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

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
_HISTORICAL_DURATION = "9 Y"
_HISTORICAL_BAR_SIZE = "1 day"
_HISTORICAL_END_SUFFIX = "23:59:59 America/New_York"
_FROZEN_START_DATES = {
    "QQQ": "2018-01-02",
    "TQQQ": "2018-01-02",
    "QQQM": "2020-10-13",
    "BOXX": "2022-12-28",
}


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
    history_watchdog_seconds: float = 240.0,
    request_id_start: int = 1_000_000,
) -> Any:
    """Build the concrete P1 port without connecting or requesting data.

    The returned callback app implements ``TqqqCoreOnlyHistoricalBarsProvider``.
    It only retains normalized bars and fixed terminal classes; raw provider error
    strings and callback payloads are intentionally discarded.
    """
    if (
        not isinstance(client_type, type)
        or not isinstance(wrapper_type, type)
        or not isinstance(contract_type, type)
        or isinstance(history_watchdog_seconds, bool)
        or not isinstance(history_watchdog_seconds, (int, float))
        or history_watchdog_seconds <= 0
        or isinstance(request_id_start, bool)
        or not isinstance(request_id_start, int)
        or request_id_start < 0
    ):
        raise TqqqCoreOnlyP1BindingError("invalid data-only acquisition configuration")

    class TqqqCoreOnlyIbkrCallbackApp(wrapper_type, client_type):
        def __init__(self) -> None:
            wrapper_type.__init__(self)
            client_type.__init__(self, self)
            self._condition = threading.Condition()
            self._next_request_id = request_id_start
            self._active_request_id: int | None = None
            self._terminal = "IDLE"
            self._bars: list[dict[str, object]] = []
            self.last_tqqq_core_only_contract: Any | None = None
            self.last_tqqq_core_only_request_envelope: dict[str, object] | None = None

        def error(
            self,
            reqId: int,
            errorTime: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str,
        ) -> None:
            del errorTime, errorCode, errorString, advancedOrderRejectJson
            with self._condition:
                if self._active_request_id is not None and reqId in {-1, self._active_request_id}:
                    self._terminal = "PROVIDER_ERROR"
                    self._condition.notify_all()

        def connectionClosed(self) -> None:
            with self._condition:
                if self._active_request_id is not None:
                    self._terminal = "TRANSPORT_ERROR"
                    self._condition.notify_all()

        def historicalData(self, reqId: int, bar: Any) -> None:
            with self._condition:
                if reqId != self._active_request_id or self._terminal != "PENDING":
                    return
                try:
                    normalized = _normalize_historical_bar(bar)
                except (TypeError, ValueError):
                    self._terminal = "INVALID_RESPONSE"
                    self._condition.notify_all()
                    return
                start_date = date.fromisoformat(str(self.last_tqqq_core_only_request_envelope["start_date"]))
                cutoff = date.fromisoformat(str(self.last_tqqq_core_only_request_envelope["date_cutoff"]))
                session = date.fromisoformat(str(normalized["date"]))
                if start_date <= session <= cutoff:
                    self._bars.append(normalized)

        def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
            del start, end
            with self._condition:
                if reqId == self._active_request_id and self._terminal == "PENDING":
                    self._terminal = "COMPLETED"
                    self._condition.notify_all()

        def tqqq_core_only_terminal_state(self) -> dict[str, object]:
            with self._condition:
                return {
                    "active_request_id": self._active_request_id,
                    "terminal": self._terminal,
                }

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
            envelope = _frozen_request_envelope(
                symbol=symbol,
                calendar_id=calendar_id,
                timezone=timezone,
                adjustment_policy=adjustment_policy,
                feed=feed,
                date_cutoff=date_cutoff,
            )
            try:
                if not self.isConnected():
                    raise ValueError
                contract = contract_type()
                contract.symbol = symbol
                contract.secType = "STK"
                contract.exchange = "SMART"
                contract.currency = "USD"
                with self._condition:
                    if self._active_request_id is not None:
                        raise ValueError
                    request_id = self._next_request_id
                    self._next_request_id += 1
                    self._active_request_id = request_id
                    self._terminal = "PENDING"
                    self._bars = []
                    self.last_tqqq_core_only_contract = contract
                    self.last_tqqq_core_only_request_envelope = envelope
                self.reqHistoricalData(
                    request_id,
                    contract,
                    envelope["endDateTime"],
                    envelope["durationStr"],
                    envelope["barSizeSetting"],
                    envelope["whatToShow"],
                    envelope["useRTH"],
                    1,
                    False,
                    [],
                )
                deadline = time.monotonic() + history_watchdog_seconds
                with self._condition:
                    while self._terminal == "PENDING":
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._terminal = "COMPLETION_NOT_OBSERVED"
                            break
                        self._condition.wait(remaining)
                    if self._terminal != "COMPLETED":
                        raise ValueError
                    return {"bars": list(self._bars)}
            except Exception:
                raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None
            finally:
                with self._condition:
                    self._active_request_id = None

    TqqqCoreOnlyIbkrCallbackApp.__name__ = "TqqqCoreOnlyIbkrCallbackApp"
    return TqqqCoreOnlyIbkrCallbackApp()


def _frozen_request_envelope(
    *,
    symbol: str,
    calendar_id: str,
    timezone: str,
    adjustment_policy: str,
    feed: str,
    date_cutoff: str,
) -> dict[str, object]:
    if (
        symbol not in _FROZEN_START_DATES
        or calendar_id != "XNYS"
        or timezone != "America/New_York"
        or adjustment_policy != "total_return_adjusted"
        or feed != "ADJUSTED_LAST"
        or date_cutoff != "2026-07-31"
    ):
        raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
    return {
        "symbol": symbol,
        "start_date": _FROZEN_START_DATES[symbol],
        "date_cutoff": date_cutoff,
        "endDateTime": f"{date_cutoff.replace('-', '')} {_HISTORICAL_END_SUFFIX}",
        "durationStr": _HISTORICAL_DURATION,
        "barSizeSetting": _HISTORICAL_BAR_SIZE,
        "whatToShow": "ADJUSTED_LAST",
        "useRTH": 1,
        "calendar_id": calendar_id,
        "timezone": timezone,
    }


def _normalize_historical_bar(bar: Any) -> dict[str, object]:
    raw_date = str(getattr(bar, "date"))
    session = (
        date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:]))
        if len(raw_date) == 8 and raw_date.isdigit()
        else date.fromisoformat(raw_date)
    )
    result: dict[str, object] = {"date": session.isoformat()}
    for field in ("open", "high", "low", "close", "volume"):
        value = float(getattr(bar, field))
        if not math.isfinite(value):
            raise ValueError
        result[field] = value
    return result


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
