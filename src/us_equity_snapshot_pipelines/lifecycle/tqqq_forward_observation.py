"""Portable core for one frozen TQQQ forward-learning observation.

The core owns the frozen calendar, attempt-ledger contract, source contract,
and sanitized result. Schedulers, provider transports, and storage are thin
injected adapters. A future platform adapter route must re-freeze the forward clock
before calling this same core. QuantConnect/LEAN may only use a separate learning or
backtest adapter: its data, optimizer results, calendar, adjustment, and fill
model cannot be joined to this encrypted forward holdout or promotion proof.
Infrastructure portability is not evidence equivalence: provider, source,
calendar, adjustment, fill, runtime, storage, or retention changes require a
new candidate/evidence identity. Future adapters may consume only sanitized
strategy or development inputs and must not read the encrypted forward payload.

The pinned QPK ObjectStore/StatePort contracts do not provide the exclusive
attempt-lock and atomic no-replace semantics required here. The core therefore
defines only its candidate-specific ledger operations; concrete persistence
stays in the outer adapter. Any reusable storage expansion must be QPK-first.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, NoReturn, Protocol
from zoneinfo import ZoneInfo

from quant_platform_kit.ibkr import StrictAdjustedHistoryResult

from .soxl_pit_input_packager import _xnys_holidays

PLAN_ID = "QSL-P3-TQQQ-FORWARD-OBS-20260813-V1"
PLAN_SHA256 = "2cb898cea13f8b9f55be52a974f6d93ceadd011284a100c2fb1ee10e720c271a"
SESSIONS_SHA256 = "f3d8729d7862198f5aa830108d11c3a743d219eeed54a2484b8822cd32c783d8"
CANDIDATE_CONTRACT_SHA256 = "fa75dccbfbd56e255448e4ccb144e07def1fa8b19800432dea9ad02848473eb2"
SOURCE_CONTRACT_SHA256 = "382efce9a88a45d3ed9667516d8fe14f7eaa5eec539c02d5c725569e7f480816"
RETENTION_EXPIRES_AT = "2028-02-16T00:00:00Z"
ORDERED_SYMBOLS = ("QQQ", "TQQQ", "QQQM", "BOXX")
APPLICATION_CALL_CEILING = len(ORDERED_SYMBOLS) * 2

_START_SESSION = date(2026, 8, 13)
_END_SESSION_EXCLUSIVE = date(2027, 8, 16)
_EARLY_CLOSE_SESSIONS = frozenset({date(2026, 11, 27), date(2026, 12, 24)})
_NEW_YORK = ZoneInfo("America/New_York")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_EMPTY_SESSIONS_SHA256 = hashlib.sha256(b"[]").hexdigest()


class ForwardObservationError(ValueError):
    """Sanitized fail-closed forward-observation failure."""


@dataclass(frozen=True)
class CollectionResult:
    status: str
    provider_application_calls: int
    observation_sha256: str | None


class ForwardObservationLedger(Protocol):
    """Candidate-specific persistence operations required by the portable core."""

    def completed_sessions(self) -> tuple[date, ...]: ...

    def start_session(self, session: date) -> None: ...

    def publish_observation(self, payload: bytes) -> str: ...

    def complete_session(self, session: date, observation_sha256: str) -> None: ...

    def invalidate(self, reason_code: str) -> NoReturn: ...


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _session_labels(start: date, end_exclusive: date) -> tuple[date, ...]:
    holidays = set().union(
        *(_xnys_holidays(year) for year in range(start.year, end_exclusive.year + 1))
    )
    sessions: list[date] = []
    current = start
    while current < end_exclusive:
        if current.weekday() < 5 and current not in holidays:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


_FROZEN_SESSIONS = _session_labels(_START_SESSION, _END_SESSION_EXCLUSIVE)
if hashlib.sha256(
    canonical_json([session.isoformat() for session in _FROZEN_SESSIONS])
).hexdigest() != SESSIONS_SHA256:
    raise RuntimeError("frozen forward calendar identity mismatch")


def frozen_sessions() -> tuple[date, ...]:
    """Return the exact frozen XNYS labels without contacting a provider."""
    return _FROZEN_SESSIONS


def _session_close(session: date) -> datetime:
    close_time = time(13 if session in _EARLY_CLOSE_SESSIONS else 16)
    return datetime.combine(session, close_time, _NEW_YORK).astimezone(UTC)


def _next_session_open(session: date) -> datetime:
    candidates = _session_labels(session + timedelta(days=1), session + timedelta(days=12))
    if not candidates:
        raise ForwardObservationError("plan invalid")
    return datetime.combine(candidates[0], time(9, 30), _NEW_YORK).astimezone(UTC)


def validate_authority_contract(
    authority_receipt: Mapping[str, Any],
    authority_receipt_sha256: str,
    *,
    plan_receipt: Mapping[str, Any],
    plan_receipt_sha256: str,
    runtime_commit: str,
    now: datetime,
) -> None:
    """Validate the fresh exact-plan authority after adapter-level loading."""
    if (
        not _REVISION.fullmatch(runtime_commit)
        or not _DIGEST.fullmatch(authority_receipt_sha256)
        or not _DIGEST.fullmatch(plan_receipt_sha256)
        or not isinstance(now, datetime)
        or now.tzinfo is None
        or not isinstance(authority_receipt, Mapping)
        or not isinstance(plan_receipt, Mapping)
    ):
        raise ForwardObservationError("authority binding invalid")
    plan = plan_receipt.get("plan")
    if not isinstance(plan, Mapping):
        raise ForwardObservationError("plan receipt binding invalid")
    plan_contract = plan.get("contract")
    if (
        not isinstance(plan_contract, Mapping)
        or plan.get("plan_sha256") != PLAN_SHA256
        or plan_contract.get("plan_id") != PLAN_ID
    ):
        raise ForwardObservationError("plan receipt binding invalid")
    receipt = authority_receipt
    provider_identity = receipt.get("provider_identity")
    scheduling = receipt.get("scheduling")
    entitlement = receipt.get("entitlement_receipt")
    license_terms = receipt.get("license_source_terms_receipt")
    if (
        receipt.get("authority_scope") != "RESEARCH_ONLY"
        or receipt.get("candidate_contract_sha256") != CANDIDATE_CONTRACT_SHA256
        or receipt.get("collector_commit") != runtime_commit
        or receipt.get("live_ready") is not False
        or receipt.get("no_order") is not True
        or receipt.get("plan_id") != PLAN_ID
        or receipt.get("plan_sha256") != PLAN_SHA256
        or receipt.get("plan_receipt_sha256") != plan_receipt_sha256
        or receipt.get("promotion_eligible") is not False
        or receipt.get("retention_expires_at") != RETENTION_EXPIRES_AT
        or receipt.get("sessions_sha256") != SESSIONS_SHA256
        or receipt.get("size_zero_required") is not True
        or receipt.get("source_contract_sha256") != SOURCE_CONTRACT_SHA256
        or not isinstance(provider_identity, Mapping)
        or set(provider_identity)
        != {
            "application_call_ceiling",
            "deploy_target",
            "ordered_symbols",
            "provider_kind",
            "session_class",
            "source_identity_sha256",
        }
        or provider_identity.get("application_call_ceiling") != APPLICATION_CALL_CEILING
        or not isinstance(provider_identity.get("deploy_target"), str)
        or not provider_identity.get("deploy_target")
        or provider_identity.get("ordered_symbols") != list(ORDERED_SYMBOLS)
        or provider_identity.get("provider_kind") != "ibkr_gateway"
        or provider_identity.get("session_class") != "live-data-only"
        or provider_identity.get("source_identity_sha256") != SOURCE_CONTRACT_SHA256
        or scheduling
        != {
            "gateway_availability_confirmed": True,
            "per_session_once_guaranteed": True,
        }
        or not isinstance(entitlement, Mapping)
        or not isinstance(license_terms, Mapping)
    ):
        raise ForwardObservationError("authority binding invalid")
    expiry = datetime.fromisoformat(RETENTION_EXPIRES_AT)
    if now.astimezone(UTC) >= expiry:
        raise ForwardObservationError("authority binding expired")
    for binding in (entitlement, license_terms):
        digest = binding.get("sha256")
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise ForwardObservationError("authority binding invalid")


def _observation_payload(
    results: Mapping[str, StrictAdjustedHistoryResult],
    *,
    session: date,
    runtime_commit: str,
    authority_receipt_sha256: str,
    provider_identity: Mapping[str, Any],
) -> bytes:
    if tuple(results) != ORDERED_SYMBOLS:
        raise ForwardObservationError("strict daily observation invalid")
    observations: list[dict[str, Any]] = []
    for symbol in ORDERED_SYMBOLS:
        result = results[symbol]
        if not isinstance(result, StrictAdjustedHistoryResult) or len(result.candles) != 1:
            raise ForwardObservationError("strict daily observation invalid")
        candle = result.candles[0]
        provenance = result.provenance
        diagnostic = result.diagnostic
        if (
            candle.session != session
            or provenance.symbol != symbol
            or provenance.exchange != "SMART"
            or provenance.currency != "USD"
            or provenance.duration != "1 D"
            or provenance.bar_size != "1 day"
            or provenance.what_to_show != "ADJUSTED_LAST"
            or provenance.use_rth is not True
            or provenance.format_date != 1
            or provenance.keep_up_to_date is not False
            or provenance.returned_row_count != 1
            or diagnostic.classification != "exact_match"
            or diagnostic.completion_observed is not True
            or diagnostic.expected_count != 1
            or diagnostic.observed_in_window_count != 1
            or diagnostic.missing_count != 0
            or diagnostic.extra_count != 0
            or diagnostic.duplicate_count != 0
            or diagnostic.missing_sessions_sha256 != _EMPTY_SESSIONS_SHA256
            or diagnostic.extra_sessions_sha256 != _EMPTY_SESSIONS_SHA256
            or diagnostic.duplicate_sessions_sha256 != _EMPTY_SESSIONS_SHA256
            or diagnostic.provider_error_code_counts
        ):
            raise ForwardObservationError("strict daily observation invalid")
        observations.append(
            {
                "symbol": symbol,
                "session": candle.session.isoformat(),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "provenance": {
                    "end_datetime": provenance.end_datetime,
                    "duration": provenance.duration,
                    "bar_size": provenance.bar_size,
                    "what_to_show": provenance.what_to_show,
                    "use_rth": provenance.use_rth,
                    "format_date": provenance.format_date,
                    "keep_up_to_date": provenance.keep_up_to_date,
                },
            }
        )
    return canonical_json(
        {
            "schema_version": "tqqq_forward_daily_observation.v1",
            "plan_id": PLAN_ID,
            "plan_sha256": PLAN_SHA256,
            "candidate_contract_sha256": CANDIDATE_CONTRACT_SHA256,
            "source_contract_sha256": SOURCE_CONTRACT_SHA256,
            "sessions_sha256": SESSIONS_SHA256,
            "collector_commit": runtime_commit,
            "authority_receipt_sha256": authority_receipt_sha256,
            "provider_identity": dict(provider_identity),
            "learning_only": True,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
            "observations": observations,
        }
    )


def _invalidate(ledger: ForwardObservationLedger, reason_code: str) -> NoReturn:
    ledger.invalidate(reason_code)
    raise ForwardObservationError("plan invalid")


def collect_once(
    *,
    ledger: ForwardObservationLedger,
    authority_receipt: Mapping[str, Any],
    authority_receipt_sha256: str,
    plan_receipt: Mapping[str, Any],
    plan_receipt_sha256: str,
    runtime_commit: str,
    acquire_symbol: Callable[[str, date, datetime], StrictAdjustedHistoryResult],
    now: datetime | None = None,
) -> CollectionResult:
    """Collect the next exact session once, or make zero provider calls."""
    observed_now = datetime.now(UTC) if now is None else now
    if not isinstance(observed_now, datetime) or observed_now.tzinfo is None:
        raise ForwardObservationError("authority binding invalid")
    observed_now = observed_now.astimezone(UTC)
    validate_authority_contract(
        authority_receipt,
        authority_receipt_sha256,
        plan_receipt=plan_receipt,
        plan_receipt_sha256=plan_receipt_sha256,
        runtime_commit=runtime_commit,
        now=observed_now,
    )
    if not callable(acquire_symbol):
        raise ForwardObservationError("provider adapter invalid")
    completed = ledger.completed_sessions()
    if (
        not isinstance(completed, tuple)
        or len(completed) > len(_FROZEN_SESSIONS)
        or completed != _FROZEN_SESSIONS[: len(completed)]
    ):
        _invalidate(ledger, "ATTEMPT_LEDGER_MISSING_OR_DUPLICATE")
    if len(completed) == len(_FROZEN_SESSIONS):
        return CollectionResult("FROZEN_PLAN_COMPLETE", 0, None)
    session = _FROZEN_SESSIONS[len(completed)]
    close = _session_close(session)
    deadline = _next_session_open(session)
    if observed_now < close:
        return CollectionResult("NO_FROZEN_SESSION_READY", 0, None)
    if observed_now >= deadline:
        _invalidate(ledger, "FROZEN_SESSION_MISSED")

    try:
        ledger.start_session(session)
    except Exception:  # noqa: BLE001 - any failed exclusive lock invalidates
        _invalidate(ledger, "ATTEMPT_LOCK_FAILED")

    results: dict[str, StrictAdjustedHistoryResult] = {}
    try:
        request_end = close + timedelta(minutes=1)
        for symbol in ORDERED_SYMBOLS:
            results[symbol] = acquire_symbol(symbol, session, request_end)
        payload = _observation_payload(
            results,
            session=session,
            runtime_commit=runtime_commit,
            authority_receipt_sha256=authority_receipt_sha256,
            provider_identity=authority_receipt["provider_identity"],
        )
        digest = ledger.publish_observation(payload)
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise ForwardObservationError("strict daily observation invalid")
        ledger.complete_session(session, digest)
    except Exception:  # noqa: BLE001 - injected provider failures must invalidate
        _invalidate(ledger, "MATERIAL_COLLECTION_FAILURE")
    return CollectionResult("COLLECTED", APPLICATION_CALL_CEILING, digest)
