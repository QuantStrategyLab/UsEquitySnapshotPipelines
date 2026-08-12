"""Strict SOXL adjusted-history wrapper and sanitized failure packager."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections import Counter
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from quant_platform_kit.ibkr import (
    StrictAdjustedHistoryError,
    StrictAdjustedHistoryRequestOutcome,
    StrictAdjustedHistoryResult,
    fetch_strict_adjusted_historical_price_candles,
)


_CLASSIFICATIONS = frozenset(
    {
        "transport_error",
        "provider_error",
        "completion_not_observed",
        "empty_response",
        "session_contract_mismatch",
    }
)
_COUNT_KEYS = frozenset(
    {
        "expected_count",
        "observed_in_window_count",
        "missing_count",
        "extra_count",
        "duplicate_count",
    }
)
_COMMITMENT_KEYS = frozenset(
    {
        "algorithm",
        "canonicalization",
        "missing_sessions_sha256",
        "extra_sessions_sha256",
        "duplicate_sessions_sha256",
    }
)
_REQUEST_VALIDATION_321_CAUSES = frozenset(
    {
        "request_id_mismatch",
        "invalid_end_datetime",
        "invalid_duration",
        "invalid_what_to_show",
        "unknown_321",
    }
)
_REQUEST_VALIDATION_321_COUNTS = (
    "matching_error_count",
    "mismatching_error_count",
    "matching_completion_count",
    "mismatching_completion_count",
    "expected_session_count",
    "observed_session_count",
)
_RESPONSE_CALLBACK_TERMINAL_TRIGGER = "response_callback"
_TRANSPORT_TERMINAL_TRIGGERS = frozenset(
    {"timeout", "connection_closed", "reader_stopped", "transport_stopped"}
)
_PROVIDER_MESSAGE_CAUSE_PATTERNS = (
    (
        re.compile(
            r"(?<![a-z0-9])invalid\s+end(?:datetime|\s+date(?:\s*/\s*|\s+)time)(?![a-z0-9])"
        ),
        "invalid_end_datetime",
    ),
    (
        re.compile(r"(?<![a-z0-9])invalid\s+duration(?![a-z0-9])"),
        "invalid_duration",
    ),
    (
        re.compile(
            r"(?<![a-z0-9])invalid\s+what(?:toshow|\s+to\s+show)(?![a-z0-9])"
        ),
        "invalid_what_to_show",
    ),
)
_PROVIDER_MESSAGE_FIELD_PATTERNS = (
    (
        re.compile(
            r"(?<![a-z0-9])end(?:datetime|\s+date(?:\s*/\s*|\s+)time)(?![a-z0-9])"
        ),
        "invalid_end_datetime",
    ),
    (
        re.compile(r"(?<![a-z0-9])duration(?![a-z0-9])"),
        "invalid_duration",
    ),
    (
        re.compile(r"(?<![a-z0-9])what(?:toshow|\s+to\s+show)(?![a-z0-9])"),
        "invalid_what_to_show",
    ),
)
_REQUEST_ENVELOPE_COMMITMENT_DOMAIN = b"qsl.soxl.request-envelope.v1"
_PROVIDER_MESSAGE_COMMITMENT_DOMAIN = b"qsl.soxl.provider-message.v1"
_CONNECTIVITY_TRANSITION_CODES = frozenset(
    {1100, 1101, 1102, 1300, 2104, 2105, 2106, 2107, 2108, 2158}
)
_TRANSPORT_FAILURE_CODES = frozenset({1100, 1101, 1300, 2105})
_INFORMATIONAL_ERROR_CODES = frozenset({1102, 2104, 2106, 2107, 2108, 2158})


class SoxlAdjustedLastDiagnosticError(ValueError):
    """The sanitized adjusted-history diagnostic violated its closed contract."""


@dataclass(frozen=True)
class RequestValidation321Diagnostic:
    """Sanitized request-bound classification without raw provider surfaces."""

    cause: str
    numeric_code: int
    request_envelope_sha256: str
    provider_message_sha256: str
    request_completion_observed: bool
    counts: tuple[tuple[str, int], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "cause": self.cause,
            "numeric_code": self.numeric_code,
            "request_envelope_sha256": self.request_envelope_sha256,
            "provider_message_sha256": self.provider_message_sha256,
            "request_completion_observed": self.request_completion_observed,
            "counts": dict(self.counts),
        }


@dataclass(frozen=True)
class RequestBoundAdjustedHistoryOutcome:
    """Strict history outcome plus sanitized counts for unrelated callbacks."""

    history_outcome: StrictAdjustedHistoryRequestOutcome
    foreign_error_code_counts: tuple[tuple[int, int], ...]
    matching_historical_data_end_count: int
    foreign_historical_data_end_count: int
    request_validation_321_diagnostics: tuple[RequestValidation321Diagnostic, ...]


def bind_strict_adjusted_history_request(
    *,
    active_request_id: int,
    terminal_trigger: str,
    bars: Sequence[Any],
    error_events: Sequence[tuple[int, int, str | None]],
    historical_data_end_request_ids: Sequence[int],
    informational_error_codes: Collection[int],
    request_envelope: bytes,
    expected_session_count: int,
) -> RequestBoundAdjustedHistoryOutcome:
    """Bind errors and completion to one exact IBKR historical request id.

    Completion is derived only from a matching ``historicalDataEnd`` callback;
    returning bars from a blocking request does not imply completion.
    """
    try:
        normalized_bars = tuple(bars)
        normalized_errors = tuple(error_events)
        completion_ids = tuple(historical_data_end_request_ids)
        informational_codes = frozenset(informational_error_codes)
    except TypeError:
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-bound history input"
        ) from None

    def valid_int(value: object) -> bool:
        return not isinstance(value, bool) and isinstance(value, int)

    if (
        not valid_int(active_request_id)
        or active_request_id < 0
        or not isinstance(terminal_trigger, str)
        or terminal_trigger
        not in _TRANSPORT_TERMINAL_TRIGGERS | {_RESPONSE_CALLBACK_TERMINAL_TRIGGER}
        or not isinstance(request_envelope, bytes)
        or not request_envelope
        or not valid_int(expected_session_count)
        or expected_session_count < 0
        or any(
            not isinstance(event, (tuple, list))
            or len(event) != 3
            or not valid_int(event[0])
            or not valid_int(event[1])
            or (event[2] is not None and not isinstance(event[2], str))
            for event in normalized_errors
        )
        or any(not valid_int(request_id) for request_id in completion_ids)
        or any(not valid_int(code) for code in informational_codes)
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid request-bound history input")
    if terminal_trigger in _TRANSPORT_TERMINAL_TRIGGERS:
        raise SoxlAdjustedLastDiagnosticError(
            f"transport terminal trigger:{terminal_trigger}"
        )

    matching_error_codes = tuple(
        code
        for request_id, code, _message in normalized_errors
        if request_id == active_request_id and code not in informational_codes
    )
    foreign_error_counts = Counter(
        code
        for request_id, code, _message in normalized_errors
        if request_id != active_request_id and code not in informational_codes
    )
    matching_completion_count = sum(
        request_id == active_request_id for request_id in completion_ids
    )
    foreign_completion_count = len(completion_ids) - matching_completion_count
    request_validation_diagnostics = tuple(
        classify_request_validation_321(
            active_request_id=active_request_id,
            error_request_id=request_id,
            error_code=code,
            provider_message=message,
            request_envelope=request_envelope,
            completion_request_ids=completion_ids,
            expected_session_count=expected_session_count,
            observed_session_count=len(normalized_bars),
        )
        for request_id, code, message in normalized_errors
        if code == 321 and code not in informational_codes
    )
    return RequestBoundAdjustedHistoryOutcome(
        history_outcome=StrictAdjustedHistoryRequestOutcome(
            bars=normalized_bars,
            completion_observed=matching_completion_count > 0,
            provider_error_codes=matching_error_codes,
        ),
        foreign_error_code_counts=tuple(sorted(foreign_error_counts.items())),
        matching_historical_data_end_count=matching_completion_count,
        foreign_historical_data_end_count=foreign_completion_count,
        request_validation_321_diagnostics=request_validation_diagnostics,
    )


def build_request_bound_ibkr_app(
    *,
    client_type: type[Any],
    wrapper_type: type[Any],
    contract_type: type[Any],
    qualification_watchdog_seconds: float = 30.0,
    history_watchdog_seconds: float = 240.0,
    wait_poll_seconds: float = 0.1,
    request_id_start: int = 1_000_000,
) -> Any:
    """Build the concrete IBKR callback boundary for the frozen acquisition CLI.

    ``ibapi`` remains a runtime-provided integration: the repository does not add
    a production dependency, and tests inject minimal EClient/EWrapper doubles.
    """
    if (
        not isinstance(client_type, type)
        or not isinstance(wrapper_type, type)
        or not isinstance(contract_type, type)
        or isinstance(request_id_start, bool)
        or not isinstance(request_id_start, int)
        or request_id_start < 0
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value <= 0
            for value in (
                qualification_watchdog_seconds,
                history_watchdog_seconds,
                wait_poll_seconds,
            )
        )
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid IBKR lifecycle configuration")

    class RequestBoundIbkrApp(wrapper_type, client_type):
        def __init__(self) -> None:
            wrapper_type.__init__(self)
            client_type.__init__(self, self)
            self._condition = threading.Condition()
            self._handshake = threading.Event()
            self._next_request_id = request_id_start
            self._reader_thread: threading.Thread | None = None
            self._reader_stopped = False
            self._connection_closed = False
            self._transport_terminal: str | None = None
            self._hmds_ready = False
            self._pending_transition_counts: Counter[int] = Counter()
            self._lifecycle: list[dict[str, Any]] = []
            self._active_request_id: int | None = None
            self._phase: str | None = None
            self._terminal_trigger: str | None = None
            self._errors: list[tuple[int, int, str | None]] = []
            self._completion_ids: list[int] = []
            self._contract_details: list[Any] = []
            self._bars: list[Any] = []
            self._matching_callback_count = 0
            self._foreign_callback_count = 0
            self._transition_counts: Counter[int] = Counter()
            self._readiness_or_progress_observed = False
            self._cancellation_count = 0
            self._request_started_monotonic: float | None = None

        def nextValidId(self, orderId: int) -> None:
            del orderId
            self._handshake.set()

        def wait_for_handshake(self) -> bool:
            return self._handshake.wait(15.0)

        def start_reader(self) -> None:
            with self._condition:
                if self._reader_thread is not None:
                    raise SoxlAdjustedLastDiagnosticError(
                        "IBKR reader thread already started"
                    )
                reader = threading.Thread(
                    target=self.run_until_stopped,
                    name="soxl-tqqq-ibkr-reader",
                    daemon=True,
                )
                self._reader_thread = reader
            reader.start()

        def run_until_stopped(self) -> None:
            try:
                client_type.run(self)
            except Exception:  # noqa: BLE001 - provider text must not reach stderr
                return
            finally:
                with self._condition:
                    self._reader_stopped = True
                    self._transport_terminal = "reader_stopped"
                    if (
                        self._active_request_id is not None
                        and self._terminal_trigger is None
                    ):
                        self._terminal_trigger = "reader_stopped"
                    self._condition.notify_all()

        def connectionClosed(self) -> None:
            with self._condition:
                self._connection_closed = True
                self._transport_terminal = "connection_closed"
                if (
                    self._active_request_id is not None
                    and self._terminal_trigger is None
                ):
                    self._terminal_trigger = "connection_closed"
                self._condition.notify_all()

        def error(
            self,
            reqId: int,
            errorTime: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str = "",
        ) -> None:
            del errorTime, advancedOrderRejectJson
            try:
                request_id = int(reqId)
                code = int(errorCode)
            except (TypeError, ValueError):
                return
            with self._condition:
                if code in _CONNECTIVITY_TRANSITION_CODES:
                    counts = (
                        self._transition_counts
                        if self._active_request_id is not None
                        else self._pending_transition_counts
                    )
                    counts[code] += 1
                    if code == 2106:
                        self._hmds_ready = True
                        if self._active_request_id is not None:
                            self._readiness_or_progress_observed = True
                    elif code in {2105, 2107}:
                        self._hmds_ready = False
                    if code in _TRANSPORT_FAILURE_CODES:
                        self._transport_terminal = "transport_stopped"
                        if (
                            self._active_request_id is not None
                            and self._terminal_trigger is None
                        ):
                            self._terminal_trigger = "transport_stopped"
                    self._condition.notify_all()
                    return
                if self._active_request_id is None:
                    return
                self._errors.append(
                    (
                        request_id,
                        code,
                        str(errorString) if errorString is not None else None,
                    )
                )
                if request_id == self._active_request_id:
                    self._matching_callback_count += 1
                    if self._terminal_trigger is None:
                        self._terminal_trigger = "response_callback"
                else:
                    self._foreign_callback_count += 1
                self._condition.notify_all()

        def contractDetails(self, reqId: int, contractDetails: Any) -> None:
            with self._condition:
                if self._phase == "qualification" and reqId == self._active_request_id:
                    self._contract_details.append(contractDetails)
                    self._matching_callback_count += 1
                else:
                    self._foreign_callback_count += 1
                self._condition.notify_all()

        def contractDetailsEnd(self, reqId: int) -> None:
            with self._condition:
                if self._phase == "qualification" and reqId == self._active_request_id:
                    self._completion_ids.append(reqId)
                    self._matching_callback_count += 1
                    if self._terminal_trigger is None:
                        self._terminal_trigger = "response_callback"
                else:
                    self._foreign_callback_count += 1
                self._condition.notify_all()

        def historicalData(self, reqId: int, bar: Any) -> None:
            with self._condition:
                if self._phase == "history" and reqId == self._active_request_id:
                    self._bars.append(bar)
                    self._matching_callback_count += 1
                    self._readiness_or_progress_observed = True
                else:
                    self._foreign_callback_count += 1
                self._condition.notify_all()

        def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
            del start, end
            with self._condition:
                if self._phase == "history" and reqId == self._active_request_id:
                    self._completion_ids.append(reqId)
                    self._matching_callback_count += 1
                    self._readiness_or_progress_observed = True
                    if self._terminal_trigger is None:
                        self._terminal_trigger = "response_callback"
                else:
                    self._foreign_callback_count += 1
                self._condition.notify_all()

        def _begin_request(self, phase: str) -> int:
            with self._condition:
                if self._active_request_id is not None:
                    raise SoxlAdjustedLastDiagnosticError(
                        "concurrent IBKR request is forbidden"
                    )
                if not self._handshake.is_set():
                    raise SoxlAdjustedLastDiagnosticError(
                        "IBKR handshake unavailable"
                    )
                self._next_request_id += 1
                self._active_request_id = self._next_request_id
                self._phase = phase
                self._terminal_trigger = self._transport_terminal
                self._errors = []
                self._completion_ids = []
                self._contract_details = []
                self._bars = []
                self._matching_callback_count = 0
                self._foreign_callback_count = 0
                self._transition_counts = Counter(self._pending_transition_counts)
                self._pending_transition_counts.clear()
                self._readiness_or_progress_observed = self._hmds_ready
                self._cancellation_count = 0
                self._request_started_monotonic = time.monotonic()
                return self._active_request_id

        def _wait_for_terminal(self, timeout_seconds: float) -> str:
            deadline = time.monotonic() + timeout_seconds
            with self._condition:
                while self._terminal_trigger is None:
                    if self._connection_closed:
                        self._terminal_trigger = "connection_closed"
                        break
                    if self._reader_stopped:
                        self._terminal_trigger = "reader_stopped"
                        break
                    if not self.isConnected():
                        self._terminal_trigger = "transport_stopped"
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._terminal_trigger = "timeout"
                        break
                    self._condition.wait(min(wait_poll_seconds, remaining))
                return self._terminal_trigger

        def _finish_request(
            self,
            *,
            status: str,
            request_envelope_sha256: str | None = None,
        ) -> None:
            with self._condition:
                elapsed_monotonic_ms = (
                    max(
                        0,
                        int(
                            (time.monotonic() - self._request_started_monotonic)
                            * 1_000
                        ),
                    )
                    if self._request_started_monotonic is not None
                    else 0
                )
                payload: dict[str, Any] = {
                    "phase": self._phase,
                    "status": status,
                    "terminal_trigger": self._terminal_trigger,
                    "readiness_or_progress_observed": self._readiness_or_progress_observed,
                    "matching_callback_count": self._matching_callback_count,
                    "foreign_callback_count": self._foreign_callback_count,
                    "matching_completion_count": sum(
                        request_id == self._active_request_id
                        for request_id in self._completion_ids
                    ),
                    "transition_code_counts": {
                        str(code): count
                        for code, count in sorted(self._transition_counts.items())
                    },
                    "cancellation_count": self._cancellation_count,
                    "elapsed_monotonic_ms": elapsed_monotonic_ms,
                }
                if request_envelope_sha256 is not None:
                    payload["request_envelope_sha256"] = request_envelope_sha256
                self._lifecycle.append(payload)
                self._active_request_id = None
                self._phase = None
                self._request_started_monotonic = None

        def sanitized_lifecycle(self) -> tuple[dict[str, Any], ...]:
            with self._condition:
                return tuple(
                    {
                        **item,
                        "transition_code_counts": dict(
                            item["transition_code_counts"]
                        ),
                    }
                    for item in self._lifecycle
                )

        def qualifyContracts(self, template: Any) -> tuple[Any, ...]:
            request_id = self._begin_request("qualification")
            status = "FAILED_MATERIAL"
            try:
                if self._terminal_trigger is not None or not self.isConnected():
                    raise SoxlAdjustedLastDiagnosticError(
                        "transport terminal trigger:transport_stopped"
                    )
                contract = contract_type()
                contract.symbol = getattr(template, "symbol", None)
                contract.secType = "STK"
                contract.exchange = "SMART"
                contract.currency = "USD"
                self.reqContractDetails(request_id, contract)
                terminal = self._wait_for_terminal(
                    qualification_watchdog_seconds
                )
                if terminal != "response_callback":
                    raise SoxlAdjustedLastDiagnosticError(
                        f"transport terminal trigger:{terminal}"
                    )
                matching_errors = tuple(
                    code for event_id, code, _message in self._errors
                    if event_id == request_id
                )
                matching_completions = self._completion_ids.count(request_id)
                if (
                    matching_errors
                    or matching_completions != 1
                    or len(self._contract_details) != 1
                ):
                    raise SoxlAdjustedLastDiagnosticError(
                        "IBKR contract qualification failed"
                    )
                qualified = self._contract_details[0].contract
                if (
                    getattr(qualified, "symbol", None) != contract.symbol
                    or getattr(qualified, "secType", None) != "STK"
                    or getattr(qualified, "exchange", None) != "SMART"
                    or getattr(qualified, "currency", None) != "USD"
                    or isinstance(getattr(qualified, "conId", None), bool)
                    or not isinstance(getattr(qualified, "conId", None), int)
                    or qualified.conId <= 0
                ):
                    raise SoxlAdjustedLastDiagnosticError(
                        "IBKR qualified contract identity mismatch"
                    )
                status = "SUCCESS"
                return (qualified,)
            finally:
                self._finish_request(status=status)

        def request_adjusted_history(
            self,
            symbol: str,
            contract: Any,
            *,
            expected_session_count: int,
            expected_duration: str,
            expected_end_datetime: str = "",
            **request_kwargs: Any,
        ) -> StrictAdjustedHistoryRequestOutcome:
            exact_request = {
                "endDateTime": expected_end_datetime,
                "durationStr": expected_duration,
                "barSizeSetting": "1 day",
                "whatToShow": "ADJUSTED_LAST",
                "useRTH": True,
                "formatDate": 1,
                "keepUpToDate": False,
            }
            if (
                not isinstance(symbol, str)
                or not symbol
                or isinstance(expected_session_count, bool)
                or not isinstance(expected_session_count, int)
                or expected_session_count <= 0
                or not isinstance(expected_duration, str)
                or not isinstance(expected_end_datetime, str)
                or request_kwargs != exact_request
            ):
                raise SoxlAdjustedLastDiagnosticError(
                    "IBKR historical request contract mismatch"
                )
            envelope = json.dumps(
                {"symbol": symbol, "request": exact_request},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            envelope_sha256 = hashlib.sha256(envelope).hexdigest()
            request_id = self._begin_request("history")
            status = "FAILED_MATERIAL"
            try:
                if self._terminal_trigger is not None or not self.isConnected():
                    raise SoxlAdjustedLastDiagnosticError(
                        "transport terminal trigger:transport_stopped"
                    )
                self.reqHistoricalData(
                    request_id,
                    contract,
                    expected_end_datetime,
                    expected_duration,
                    "1 day",
                    "ADJUSTED_LAST",
                    1,
                    1,
                    False,
                    [],
                )
                terminal = self._wait_for_terminal(history_watchdog_seconds)
                if terminal == "timeout":
                    self._cancellation_count += 1
                    self.cancelHistoricalData(request_id)
                with self._condition:
                    bars = tuple(self._bars)
                    errors = tuple(self._errors)
                    completion_ids = tuple(self._completion_ids)
                bound = bind_strict_adjusted_history_request(
                    active_request_id=request_id,
                    terminal_trigger=terminal,
                    bars=bars,
                    error_events=errors,
                    historical_data_end_request_ids=completion_ids,
                    informational_error_codes=_INFORMATIONAL_ERROR_CODES,
                    request_envelope=envelope,
                    expected_session_count=expected_session_count,
                )
                completion_count = bound.matching_historical_data_end_count
                matching_errors = tuple(bound.history_outcome.provider_error_codes)
                status = (
                    "SUCCESS"
                    if not matching_errors and completion_count == 1
                    else "FAILED_MATERIAL"
                )
                if completion_count == 1:
                    return bound.history_outcome
                return StrictAdjustedHistoryRequestOutcome(
                    bars=bound.history_outcome.bars,
                    completion_observed=False,
                    provider_error_codes=bound.history_outcome.provider_error_codes,
                )
            finally:
                self._finish_request(
                    status=status,
                    request_envelope_sha256=envelope_sha256,
                )

    RequestBoundIbkrApp.__name__ = "RequestBoundIbkrApp"
    return RequestBoundIbkrApp()


def acquire_strict_adjusted_last(
    ib: Any,
    symbol: str,
    *,
    end_datetime: datetime,
    duration: str,
    expected_sessions: Sequence[date],
    provider_end_datetime: str = "",
    stock_factory: Callable[..., Any] | None = None,
    requester: Callable[..., StrictAdjustedHistoryRequestOutcome],
) -> StrictAdjustedHistoryResult:
    """Request current adjusted history, then enforce the frozen session contract."""
    frozen_sessions = tuple(expected_sessions)

    def request_frozen_sessions(
        contract: Any,
        **request_kwargs: Any,
    ) -> StrictAdjustedHistoryRequestOutcome:
        outcome = requester(
            contract,
            **{**request_kwargs, "endDateTime": provider_end_datetime},
        )
        if not isinstance(outcome, StrictAdjustedHistoryRequestOutcome):
            return outcome
        try:
            bars = tuple(outcome.bars)
        except TypeError:
            return outcome

        filtered_bars = []
        for bar in bars:
            raw_session = getattr(bar, "date", None)
            try:
                if isinstance(raw_session, datetime):
                    session = raw_session.date()
                elif isinstance(raw_session, date):
                    session = raw_session
                else:
                    session = datetime.fromisoformat(str(raw_session)).date()
            except (TypeError, ValueError):
                filtered_bars.append(bar)
                continue
            if frozen_sessions[0] <= session and (
                session <= end_datetime.date()
                if provider_end_datetime
                else session < end_datetime.date()
            ):
                filtered_bars.append(bar)

        return StrictAdjustedHistoryRequestOutcome(
            bars=tuple(filtered_bars),
            completion_observed=outcome.completion_observed,
            provider_error_codes=outcome.provider_error_codes,
        )

    return fetch_strict_adjusted_historical_price_candles(
        ib,
        symbol,
        end_datetime=end_datetime,
        duration=duration,
        expected_sessions=frozen_sessions,
        stock_factory=stock_factory,
        requester=request_frozen_sessions,
    )


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _domain_separated_sha256(domain: bytes, value: bytes) -> str:
    return hashlib.sha256(domain + b"\x00" + value).hexdigest()


def _nonnegative_count(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def classify_request_validation_321(
    *,
    active_request_id: int,
    error_request_id: int,
    error_code: int,
    provider_message: str | None,
    request_envelope: bytes,
    completion_request_ids: Sequence[int],
    expected_session_count: int,
    observed_session_count: int,
) -> RequestValidation321Diagnostic:
    """Correlate one synthetic 321 event and retain only closed aggregates."""
    if (
        isinstance(active_request_id, bool)
        or not isinstance(active_request_id, int)
        or active_request_id < 0
        or isinstance(error_request_id, bool)
        or not isinstance(error_request_id, int)
        or isinstance(error_code, bool)
        or error_code != 321
        or (provider_message is not None and not isinstance(provider_message, str))
        or not isinstance(request_envelope, bytes)
        or not request_envelope
        or not _nonnegative_count(expected_session_count)
        or not _nonnegative_count(observed_session_count)
    ):
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic input"
        )
    try:
        completion_ids = tuple(completion_request_ids)
    except TypeError:
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic input"
        ) from None
    if any(
        isinstance(request_id, bool) or not isinstance(request_id, int)
        for request_id in completion_ids
    ):
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic input"
        )

    error_matches = error_request_id == active_request_id
    matching_completion_count = sum(
        request_id == active_request_id for request_id in completion_ids
    )
    if error_matches:
        normalized_message = (
            " ".join(provider_message.split()).casefold()
            if provider_message is not None
            else ""
        )
        matched_causes = {
            candidate
            for pattern, candidate in _PROVIDER_MESSAGE_CAUSE_PATTERNS
            if pattern.search(normalized_message) is not None
        }
        mentioned_causes = {
            candidate
            for pattern, candidate in _PROVIDER_MESSAGE_FIELD_PATTERNS
            if pattern.search(normalized_message) is not None
        }
        cause = (
            matched_causes.pop()
            if len(matched_causes) == 1 and matched_causes == mentioned_causes
            else "unknown_321"
        )
    else:
        cause = "request_id_mismatch"

    message_bytes = (
        provider_message.encode("utf-8") if provider_message is not None else b""
    )
    counts = {
        "matching_error_count": int(error_matches),
        "mismatching_error_count": int(not error_matches),
        "matching_completion_count": matching_completion_count,
        "mismatching_completion_count": len(completion_ids)
        - matching_completion_count,
        "expected_session_count": expected_session_count,
        "observed_session_count": observed_session_count,
    }
    return RequestValidation321Diagnostic(
        cause=cause,
        numeric_code=error_code,
        request_envelope_sha256=_domain_separated_sha256(
            _REQUEST_ENVELOPE_COMMITMENT_DOMAIN,
            request_envelope,
        ),
        provider_message_sha256=_domain_separated_sha256(
            _PROVIDER_MESSAGE_COMMITMENT_DOMAIN,
            message_bytes,
        ),
        request_completion_observed=matching_completion_count > 0,
        counts=tuple((key, counts[key]) for key in _REQUEST_VALIDATION_321_COUNTS),
    )


def _sanitized_payload(error: StrictAdjustedHistoryError) -> dict[str, Any]:
    diagnostic = error.diagnostic
    if diagnostic is None:
        raise SoxlAdjustedLastDiagnosticError("missing sanitized diagnostic")
    payload = diagnostic.to_dict()
    if set(payload) != {
        "schema_version",
        "classification",
        "request_completion_observed",
        "counts",
        "commitments",
        "provider_error_code_counts",
    }:
        raise SoxlAdjustedLastDiagnosticError("invalid sanitized diagnostic")
    if (
        payload["schema_version"] != "strict_adjusted_history_diagnostic.v1"
        or payload["classification"] not in _CLASSIFICATIONS
        or not isinstance(payload["request_completion_observed"], bool)
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid sanitized diagnostic")

    counts = payload["counts"]
    if (
        not isinstance(counts, dict)
        or set(counts) != _COUNT_KEYS
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts.values()
        )
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid sanitized diagnostic")

    commitments = payload["commitments"]
    if (
        not isinstance(commitments, dict)
        or set(commitments) != _COMMITMENT_KEYS
        or commitments["algorithm"] != "sha256"
        or commitments["canonicalization"]
        != "sorted_unique_iso_sessions_json_utf8.v1"
        or any(
            not _is_sha256(commitments[key])
            for key in (
                "missing_sessions_sha256",
                "extra_sessions_sha256",
                "duplicate_sessions_sha256",
            )
        )
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid sanitized diagnostic")

    errors = payload["provider_error_code_counts"]
    if not isinstance(errors, dict) or any(
        not isinstance(code, str)
        or not code.isdigit()
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count <= 0
        for code, count in errors.items()
    ):
        raise SoxlAdjustedLastDiagnosticError("invalid sanitized diagnostic")
    return payload


def _request_validation_321_payload(
    diagnostic: RequestValidation321Diagnostic,
) -> dict[str, Any]:
    if not isinstance(diagnostic, RequestValidation321Diagnostic):
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic"
        )
    payload = diagnostic.to_dict()
    if (
        set(payload)
        != {
            "cause",
            "numeric_code",
            "request_envelope_sha256",
            "provider_message_sha256",
            "request_completion_observed",
            "counts",
        }
        or payload["cause"] not in _REQUEST_VALIDATION_321_CAUSES
        or payload["numeric_code"] != 321
        or not _is_sha256(payload["request_envelope_sha256"])
        or not _is_sha256(payload["provider_message_sha256"])
        or not isinstance(payload["request_completion_observed"], bool)
    ):
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic"
        )
    counts = payload["counts"]
    if (
        not isinstance(counts, dict)
        or tuple(counts) != _REQUEST_VALIDATION_321_COUNTS
        or any(not _nonnegative_count(value) for value in counts.values())
        or counts["matching_error_count"] + counts["mismatching_error_count"]
        != 1
        or (payload["cause"] == "request_id_mismatch")
        != (counts["mismatching_error_count"] == 1)
        or payload["request_completion_observed"]
        != (counts["matching_completion_count"] > 0)
    ):
        raise SoxlAdjustedLastDiagnosticError(
            "invalid request-validation 321 diagnostic"
        )
    return payload


def _write_exclusive_mode_0600_json(
    destination: str | Path,
    payload: dict[str, Any],
) -> None:
    path = Path(destination)
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)


def write_sanitized_adjusted_last_diagnostic(
    destination: str | Path,
    error: StrictAdjustedHistoryError,
) -> None:
    """Create one exclusive mode-0600 JSON diagnostic without raw market data."""
    _write_exclusive_mode_0600_json(destination, _sanitized_payload(error))


def write_sanitized_request_validation_321_diagnostic(
    destination: str | Path,
    diagnostic: RequestValidation321Diagnostic,
) -> None:
    """Create one exclusive mode-0600 request-bound 321 diagnostic."""
    _write_exclusive_mode_0600_json(
        destination,
        _request_validation_321_payload(diagnostic),
    )
