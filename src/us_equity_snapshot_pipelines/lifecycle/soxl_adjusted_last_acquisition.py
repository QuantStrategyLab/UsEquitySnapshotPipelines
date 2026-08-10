"""Strict SOXL adjusted-history wrapper and sanitized failure packager."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import re
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


def acquire_strict_adjusted_last(
    ib: Any,
    symbol: str,
    *,
    end_datetime: datetime,
    duration: str,
    expected_sessions: Sequence[date],
    stock_factory: Callable[..., Any] | None = None,
    requester: Callable[..., StrictAdjustedHistoryRequestOutcome],
) -> StrictAdjustedHistoryResult:
    """Request current adjusted history, then enforce the frozen session contract."""
    frozen_sessions = tuple(expected_sessions)

    def request_frozen_sessions(
        contract: Any,
        **request_kwargs: Any,
    ) -> StrictAdjustedHistoryRequestOutcome:
        outcome = requester(contract, **{**request_kwargs, "endDateTime": ""})
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
            if frozen_sessions[0] <= session < end_datetime.date():
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
