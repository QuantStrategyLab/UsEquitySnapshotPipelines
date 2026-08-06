"""Strict SOXL adjusted-history wrapper and sanitized failure packager."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime
import json
import os
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


class SoxlAdjustedLastDiagnosticError(ValueError):
    """The sanitized adjusted-history diagnostic violated its closed contract."""


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
    """Run the QPK strict contract with explicit completion and error state."""
    return fetch_strict_adjusted_historical_price_candles(
        ib,
        symbol,
        end_datetime=end_datetime,
        duration=duration,
        expected_sessions=expected_sessions,
        stock_factory=stock_factory,
        requester=requester,
    )


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
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


def write_sanitized_adjusted_last_diagnostic(
    destination: str | Path,
    error: StrictAdjustedHistoryError,
) -> None:
    """Create one exclusive mode-0600 JSON diagnostic without raw market data."""
    path = Path(destination)
    payload = json.dumps(
        _sanitized_payload(error),
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
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
