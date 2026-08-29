"""Publish and verify the isolated SOXL free-source, close-only P1 input.

This module is a research-only P1 boundary for P2 v4.  It never contacts a
provider by itself: callers inject two independently acquired observations.
Twelve Data becomes canonical only after the pinned QPK assurance policy
verifies it against Yahoo Finance.  There is no fallback, averaging, source
substitution, strategy execution, scheduling, or storage access here.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, cast

import exchange_calendars as xcals
import pandas as pd
from quant_platform_kit.data.multisource_assurance import (
    DATA_ASSURANCE_STATUS_VERIFIED,
    SOURCE_OBSERVATION_READY,
    DailyBarSourceObservation,
    MultiSourceDailyBarPolicy,
    assess_multisource_daily_bars,
)
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

from ..twelve_data_daily import TWELVE_DATA_DAILY_SOURCE_ID
from ..yahoo_finance_daily import YAHOO_FINANCE_DAILY_SOURCE_ID
from .soxl_core_only_p1_binding import expected_soxl_core_only_sessions
from .soxl_core_only_p2_v4_free_split_close_contract import (
    P2_V4_FREE_SPLIT_CLOSE_CONTRACT,
)

_INPUT_SCHEMA = "qsl.soxl-soxx-core-only-free-split-close-p1-binding.v1"
_CLOSES_SCHEMA = "qsl.soxl-soxx-core-only-split-adjusted-close-series.v1"
_ASSURANCE_SCHEMA = "qsl.soxl-soxx-core-only-free-split-close-assurance.v1"
_UNIVERSE = ("SOXL", "SOXX", "BOXX")
_CANONICAL_SOURCE_ID = TWELVE_DATA_DAILY_SOURCE_ID
_VERIFIER_SOURCE_ID = YAHOO_FINANCE_DAILY_SOURCE_ID
_ADJUSTMENT_BASIS = "split_adjusted"
_CALENDAR = {
    "calendar_id": "XNYS",
    "timezone": "America/New_York",
    "source": "exchange_calendars:4.13.2:XNYS",
}
_V4_POLICY_SCOPE_PREFIX = "soxl_core_only_p2_v4_free_split_close"
_PRICE_RELATIVE_TOLERANCE = 0.0001
_OUTPUT_FILENAMES = frozenset({"closes.json", "assurance.json", "binding.json", "manifest.json"})
_DIGEST = re.compile(r"^[0-9a-f]{64}$")

SOXL_FREE_SOURCE_REASON_XNYS_SESSION_NOT_COMPLETE = "XNYS_SESSION_NOT_COMPLETE"
SOXL_FREE_SOURCE_REASON_YAHOO_SETTLEMENT_LAG = "YAHOO_SETTLEMENT_LAG"
SOXL_FREE_SOURCE_REASON_PROVIDER_UNAVAILABLE = "FREE_SOURCE_PROVIDER_UNAVAILABLE"
SOXL_FREE_SOURCE_REASON_SESSION_COVERAGE_MISMATCH = "FREE_SOURCE_SESSION_COVERAGE_MISMATCH"
SOXL_FREE_SOURCE_REASON_PRICE_AGREEMENT_NOT_VERIFIED = "FREE_SOURCE_PRICE_AGREEMENT_NOT_VERIFIED"
SOXL_FREE_SOURCE_REASON_ASSURANCE_NOT_VERIFIED = "FREE_SOURCE_ASSURANCE_NOT_VERIFIED"


class SoxlCoreOnlyFreeSplitCloseP1Error(ValueError):
    """Sanitized failure for a v4 P1 identity or immutable input root."""


class SoxlCoreOnlyFreeSplitCloseP1UnavailableError(SoxlCoreOnlyFreeSplitCloseP1Error):
    """Both mandatory sources did not produce an assured P1 input."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = SOXL_FREE_SOURCE_REASON_ASSURANCE_NOT_VERIFIED,
    ) -> None:
        self.reason_code = reason_code
        super().__init__(message)


class SoxlCoreOnlyFreeSplitCloseObserver(Protocol):
    """Injected source-observation port; this P1 layer owns no credentials."""

    def observe_daily_bars(
        self,
        *,
        source_id: str,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> DailyBarSourceObservation: ...


class _P2Contract(Protocol):
    """Minimal immutable P2 identity accepted by this source-only P1 boundary."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


def _resolve_p2_contract(value: object | None) -> _P2Contract:
    contract = P2_V4_FREE_SPLIT_CLOSE_CONTRACT if value is None else value
    required = (
        "candidate_id",
        "config_sha256",
        "ues_revision",
        "qpk_revision",
        "input_contract_id",
    )
    if any(not isinstance(getattr(contract, field, None), str) or not getattr(contract, field) for field in required):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source P2 contract")
    if not _DIGEST.fullmatch(getattr(contract, "config_sha256")):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source P2 contract")
    return cast(_P2Contract, contract)


def _policy_scope_prefix(contract: _P2Contract) -> str:
    if contract.candidate_id == P2_V4_FREE_SPLIT_CLOSE_CONTRACT.candidate_id:
        return _V4_POLICY_SCOPE_PREFIX
    return contract.candidate_id


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _qpk_canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _date_cutoff(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source date cutoff")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source date cutoff") from exc
    expected = expected_soxl_core_only_sessions(parsed.isoformat())["SOXL"]
    if parsed.isoformat() != value or not expected or expected[-1] != parsed:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source date cutoff")
    return value


def validate_soxl_core_only_free_split_close_completed_session(
    *, date_cutoff: object, observed_at: object
) -> str:
    """Require a completed XNYS session before a daily P1 can acquire data.

    A calendar-valid date alone is insufficient: before that session closes,
    both providers can legitimately expose an in-progress daily bar.  P1 must
    park before any source request in that case, rather than comparing or
    materializing provisional prices.
    """
    cutoff = _date_cutoff(date_cutoff)
    if not isinstance(observed_at, str):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source observed time")
    try:
        observed = datetime.fromisoformat(observed_at)
    except ValueError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source observed time") from exc
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source observed time")
    try:
        closing = xcals.get_calendar("XNYS").session_close(pd.Timestamp(cutoff))
    except (AttributeError, TypeError, ValueError) as exc:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("XNYS calendar is unavailable") from exc
    if pd.Timestamp(observed) < closing:
        raise SoxlCoreOnlyFreeSplitCloseP1UnavailableError(
            "SOXL free-source session is not complete",
            reason_code=SOXL_FREE_SOURCE_REASON_XNYS_SESSION_NOT_COMPLETE,
        )
    return cutoff


def _policy(
    *, symbol: str, date_cutoff: str, p2_contract: _P2Contract
) -> MultiSourceDailyBarPolicy:
    return MultiSourceDailyBarPolicy(
        scope_id=f"{_policy_scope_prefix(p2_contract)}:{symbol.lower()}",
        symbol=symbol,
        date_cutoff=date_cutoff,
        adjustment_basis=_ADJUSTMENT_BASIS,
        required_source_ids=(_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID),
        required_price_fields=("close",),
        compare_volume=False,
        price_relative_tolerance=_PRICE_RELATIVE_TOLERANCE,
    )


def build_soxl_core_only_free_split_close_p1_binding(
    *, date_cutoff: object, p2_contract: object | None = None
) -> dict[str, object]:
    """Build the exact candidate-bound P1 identity without acquiring market data."""
    cutoff = _date_cutoff(date_cutoff)
    contract = _resolve_p2_contract(p2_contract)
    return {
        "schema_version": _INPUT_SCHEMA,
        "candidate": {
            "candidate_id": contract.candidate_id,
            "config_sha256": contract.config_sha256,
        },
        "source": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": contract.ues_revision,
        },
        "data_identity": {
            "calendar": _CALENDAR,
            "adjustment": {
                "policy": _ADJUSTMENT_BASIS,
                "source": "independently assured Twelve Data and Yahoo Finance split-adjusted daily bars",
            },
            "assurance": {
                "canonical_source_id": _CANONICAL_SOURCE_ID,
                "verifier_source_id": _VERIFIER_SOURCE_ID,
                "scope_id_prefix": _policy_scope_prefix(contract),
                "required_price_fields": ["close"],
                "compare_volume": False,
                "price_relative_tolerance": _PRICE_RELATIVE_TOLERANCE,
            },
            "universe": list(_UNIVERSE),
            "date_cutoff": cutoff,
        },
    }


def validate_soxl_core_only_free_split_close_p1_binding(
    value: Mapping[str, object], *, p2_contract: object | None = None
) -> dict[str, object]:
    """Reject an input binding that drifts from the candidate's data contract."""
    try:
        identity = value["data_identity"]
        if not isinstance(identity, Mapping):
            raise TypeError
        cutoff = identity["date_cutoff"]
    except (KeyError, TypeError):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source P1 binding") from None
    expected = build_soxl_core_only_free_split_close_p1_binding(
        date_cutoff=cutoff,
        p2_contract=p2_contract,
    )
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source P1 binding")
    return expected


def canonical_soxl_core_only_free_split_close_p1_binding_bytes(
    value: Mapping[str, object], *, p2_contract: object | None = None
) -> bytes:
    return _canonical(validate_soxl_core_only_free_split_close_p1_binding(value, p2_contract=p2_contract))


def soxl_core_only_free_split_close_p1_binding_sha256(
    value: Mapping[str, object], *, p2_contract: object | None = None
) -> str:
    return hashlib.sha256(
        canonical_soxl_core_only_free_split_close_p1_binding_bytes(value, p2_contract=p2_contract)
    ).hexdigest()


def canonical_soxl_core_only_free_split_close_series_bytes(*, symbol: object, series: object) -> bytes:
    """Canonically encode the v4 canonical-source close series for one asset."""
    if not isinstance(symbol, str) or symbol not in _UNIVERSE or not isinstance(series, list) or not series:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
    normalized: list[dict[str, object]] = []
    previous: date | None = None
    for raw in series:
        if not isinstance(raw, Mapping) or set(raw) != {"session_date", "close"}:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
        session_date = raw["session_date"]
        close = raw["close"]
        if not isinstance(session_date, str) or isinstance(close, bool) or not isinstance(close, (int, float)):
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
        try:
            parsed = date.fromisoformat(session_date)
        except ValueError as exc:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series") from exc
        number = float(close)
        if parsed.isoformat() != session_date or (previous is not None and parsed <= previous):
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
        if not math.isfinite(number) or number <= 0.0:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
        previous = parsed
        normalized.append({"session_date": session_date, "close": number})
    return _canonical({"schema_version": _CLOSES_SCHEMA, "symbol": symbol, "sessions": normalized})


def _close_series_from_snapshot(
    *, symbol: str, observation: DailyBarSourceObservation, expected_sessions: tuple[date, ...]
) -> tuple[list[dict[str, object]], str]:
    if observation.status != SOURCE_OBSERVATION_READY or observation.snapshot is None:
        raise SoxlCoreOnlyFreeSplitCloseP1UnavailableError("SOXL free-source assurance not verified")
    snapshot = observation.snapshot
    observed_dates = tuple(date.fromisoformat(bar.session_date) for bar in snapshot.bars)
    if observed_dates != expected_sessions:
        raise SoxlCoreOnlyFreeSplitCloseP1UnavailableError("SOXL free-source coverage not verified")
    series = [{"session_date": bar.session_date, "close": bar.close} for bar in snapshot.bars]
    canonical = canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=series)
    try:
        normalized = json.loads(canonical)["sessions"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series") from exc
    if not isinstance(normalized, list):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
    return normalized, hashlib.sha256(canonical).hexdigest()


def _source_snapshot_sessions(observation: DailyBarSourceObservation) -> tuple[date, ...] | None:
    if observation.status != SOURCE_OBSERVATION_READY or observation.snapshot is None:
        return None
    try:
        return tuple(date.fromisoformat(bar.session_date) for bar in observation.snapshot.bars)
    except (TypeError, ValueError):
        return None


def classify_soxl_core_only_free_split_close_unavailability(
    *,
    observations: tuple[DailyBarSourceObservation, DailyBarSourceObservation],
    expected_sessions: tuple[date, ...],
) -> str:
    """Classify a rejected two-source P1 without disclosing provider payloads.

    A verifier that has every expected session except the completed cutoff is
    the ordinary settlement-delay case.  It remains non-publishable, but is
    safe for the scheduler to retry on its next planned cycle.  Every other
    failure stays in a broader, fail-closed class; this helper never changes
    the required sources, price tolerance, or publication decision.
    """

    if not expected_sessions:
        return SOXL_FREE_SOURCE_REASON_ASSURANCE_NOT_VERIFIED
    by_source = {observation.source_id: observation for observation in observations}
    canonical = by_source.get(_CANONICAL_SOURCE_ID)
    verifier = by_source.get(_VERIFIER_SOURCE_ID)
    if canonical is None or verifier is None:
        return SOXL_FREE_SOURCE_REASON_ASSURANCE_NOT_VERIFIED
    canonical_sessions = _source_snapshot_sessions(canonical)
    verifier_sessions = _source_snapshot_sessions(verifier)
    if canonical_sessions is None or verifier_sessions is None:
        return SOXL_FREE_SOURCE_REASON_PROVIDER_UNAVAILABLE
    if canonical_sessions == expected_sessions and verifier_sessions == expected_sessions[:-1]:
        return SOXL_FREE_SOURCE_REASON_YAHOO_SETTLEMENT_LAG
    if canonical_sessions != expected_sessions or verifier_sessions != expected_sessions:
        return SOXL_FREE_SOURCE_REASON_SESSION_COVERAGE_MISMATCH
    return SOXL_FREE_SOURCE_REASON_PRICE_AGREEMENT_NOT_VERIFIED


def _closes_bytes(series: Mapping[str, list[dict[str, object]]]) -> bytes:
    if set(series) != set(_UNIVERSE):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source close series")
    return _canonical({"schema_version": _CLOSES_SCHEMA, "series": {symbol: series[symbol] for symbol in _UNIVERSE}})


def _assurance_member(
    *,
    date_cutoff: str,
    reports: Mapping[str, object],
    canonical_close_sha256: Mapping[str, str],
    p2_contract: _P2Contract,
) -> dict[str, object]:
    if set(reports) != set(_UNIVERSE) or set(canonical_close_sha256) != set(_UNIVERSE):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
    assurances: dict[str, object] = {}
    for symbol in _UNIVERSE:
        report = reports[symbol]
        diagnostic = getattr(report, "to_diagnostic", lambda: None)()
        report_sha256 = getattr(report, "report_sha256", None)
        policy = _policy(symbol=symbol, date_cutoff=date_cutoff, p2_contract=p2_contract)
        if (
            getattr(report, "status", None) != DATA_ASSURANCE_STATUS_VERIFIED
            or getattr(report, "can_publish_research_input", False) is not True
            or not isinstance(diagnostic, Mapping)
            or report_sha256 != _qpk_canonical_sha256(diagnostic)
            or diagnostic.get("policy_sha256") != policy.policy_sha256
        ):
            raise SoxlCoreOnlyFreeSplitCloseP1UnavailableError("SOXL free-source assurance not verified")
        assurances[symbol] = {
            "assurance_report_sha256": report_sha256,
            "canonical_close_series_sha256": canonical_close_sha256[symbol],
            "diagnostic": dict(diagnostic),
        }
    value = {"schema_version": _ASSURANCE_SCHEMA, "date_cutoff": date_cutoff, "assurances": assurances}
    validate_soxl_core_only_free_split_close_assurance_member(
        value,
        date_cutoff=date_cutoff,
        canonical_close_sha256=canonical_close_sha256,
        p2_contract=p2_contract,
    )
    return value


def validate_soxl_core_only_free_split_close_assurance_member(
    value: object,
    *,
    date_cutoff: str,
    canonical_close_sha256: Mapping[str, str],
    p2_contract: object | None = None,
) -> dict[str, dict[str, str]]:
    contract = _resolve_p2_contract(p2_contract)
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema_version", "date_cutoff", "assurances"}
        or value.get("schema_version") != _ASSURANCE_SCHEMA
        or value.get("date_cutoff") != date_cutoff
        or not isinstance(value.get("assurances"), Mapping)
        or set(value["assurances"]) != set(_UNIVERSE)
    ):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
    snapshots: dict[str, dict[str, str]] = {}
    for symbol in _UNIVERSE:
        assurance = value["assurances"][symbol]
        if not isinstance(assurance, Mapping) or set(assurance) != {
            "assurance_report_sha256",
            "canonical_close_series_sha256",
            "diagnostic",
        }:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
        diagnostic = assurance["diagnostic"]
        policy = _policy(symbol=symbol, date_cutoff=date_cutoff, p2_contract=contract)
        expected_diagnostic = {
            "schema_version": "qpk.multisource_daily_bar_assurance.v1",
            "policy_sha256": policy.policy_sha256,
            "scope_id": policy.scope_id,
            "symbol": symbol,
            "date_cutoff": date_cutoff,
            "status": DATA_ASSURANCE_STATUS_VERIFIED,
            "can_publish_research_input": True,
            "source_statuses": {
                _CANONICAL_SOURCE_ID: SOURCE_OBSERVATION_READY,
                _VERIFIER_SOURCE_ID: SOURCE_OBSERVATION_READY,
            },
            "source_reason_codes": {},
            "findings": [],
        }
        if not isinstance(diagnostic, Mapping) or set(diagnostic) != {
            *expected_diagnostic,
            "source_snapshot_sha256",
        }:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
        for field, expected in expected_diagnostic.items():
            if diagnostic.get(field) != expected:
                raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
        source_snapshots = diagnostic.get("source_snapshot_sha256")
        if (
            not isinstance(source_snapshots, Mapping)
            or set(source_snapshots) != {_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID}
            or any(not isinstance(digest, str) or not _DIGEST.fullmatch(digest) for digest in source_snapshots.values())
            or assurance["assurance_report_sha256"] != _qpk_canonical_sha256(diagnostic)
            or assurance["canonical_close_series_sha256"] != canonical_close_sha256.get(symbol)
        ):
            raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
        snapshots[symbol] = {source_id: source_snapshots[source_id] for source_id in sorted(source_snapshots)}
    return snapshots


def build_soxl_core_only_free_split_close_input_manifest(
    binding: Mapping[str, object],
    *,
    observed_at: str,
    producer: Mapping[str, object],
    closes_bytes: bytes,
    assurance_bytes: bytes,
    p2_contract: object | None = None,
) -> dict[str, object]:
    """Build the candidate-bound manifest after closes and assurance are frozen."""
    contract = _resolve_p2_contract(p2_contract)
    frozen = validate_soxl_core_only_free_split_close_p1_binding(binding, p2_contract=contract)
    if not isinstance(closes_bytes, bytes) or not isinstance(assurance_bytes, bytes):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input member")
    series = _verified_close_series(closes_bytes, frozen["data_identity"]["date_cutoff"])
    close_digests = {
        symbol: hashlib.sha256(
            canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=series[symbol])
        ).hexdigest()
        for symbol in _UNIVERSE
    }
    try:
        assurance = json.loads(assurance_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance") from None
    source_snapshots = validate_soxl_core_only_free_split_close_assurance_member(
        assurance,
        date_cutoff=frozen["data_identity"]["date_cutoff"],
        canonical_close_sha256=close_digests,
        p2_contract=contract,
    )
    if assurance_bytes != _canonical(assurance):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source assurance")
    binding_digest = soxl_core_only_free_split_close_p1_binding_sha256(frozen, p2_contract=contract)
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"soxl-free-split-close-{binding_digest[:24]}-{hashlib.sha256(closes_bytes).hexdigest()[:24]}",
            "research_input_contract_id": contract.input_contract_id,
            "domain": "us_equity",
            "profile": contract.candidate_id,
            "artifact_type": "immutable_assured_split_adjusted_close_etf_only",
            "observed_at": observed_at,
            "effective_at": observed_at,
            "as_of": observed_at,
            "producer": dict(producer),
            "calendar": {
                **_CALENDAR,
                "session_date": frozen["data_identity"]["date_cutoff"],
                "source_revision": binding_digest,
            },
            "adjustment": {
                **frozen["data_identity"]["adjustment"],
                "source_revision": binding_digest,
            },
            "sources": sorted(
                (
                    {
                        "source_id": f"{source_id}:{symbol}",
                        "revision": binding_digest,
                        "observed_at": observed_at,
                        "content_sha256": source_snapshots[symbol][source_id],
                    }
                    for symbol in _UNIVERSE
                    for source_id in (_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID)
                ),
                key=lambda source: str(source["source_id"]),
            ),
            "members": [
                {
                    "path": "assurance.json",
                    "media_type": "application/json",
                    "size_bytes": len(assurance_bytes),
                    "sha256": hashlib.sha256(assurance_bytes).hexdigest(),
                },
                {
                    "path": "closes.json",
                    "media_type": "application/json",
                    "size_bytes": len(closes_bytes),
                    "sha256": hashlib.sha256(closes_bytes).hexdigest(),
                },
            ],
        }
    )
    validate_soxl_core_only_free_split_close_input_manifest(manifest, frozen, p2_contract=contract)
    return manifest


def validate_soxl_core_only_free_split_close_input_manifest(
    manifest: Mapping[str, object],
    binding: Mapping[str, object],
    *,
    p2_contract: object | None = None,
) -> str:
    """Validate candidate-bound manifest metadata before P3 consumes it."""
    contract = _resolve_p2_contract(p2_contract)
    frozen = validate_soxl_core_only_free_split_close_p1_binding(binding, p2_contract=contract)
    try:
        validated = validate_research_input_manifest(manifest)
    except ValueError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input manifest") from exc
    binding_digest = soxl_core_only_free_split_close_p1_binding_sha256(frozen, p2_contract=contract)
    identity = frozen["data_identity"]
    expected_sources = {
        f"{source_id}:{symbol}"
        for symbol in _UNIVERSE
        for source_id in (_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID)
    }
    if (
        validated["research_input_contract_id"] != contract.input_contract_id
        or validated["domain"] != "us_equity"
        or validated["profile"] != contract.candidate_id
        or validated["artifact_type"] != "immutable_assured_split_adjusted_close_etf_only"
        or validated["calendar"]
        != {**_CALENDAR, "session_date": identity["date_cutoff"], "source_revision": binding_digest}
        or validated["adjustment"]
        != {**identity["adjustment"], "source_revision": binding_digest}
        or {source["source_id"] for source in validated["sources"]} != expected_sources
        or {source["revision"] for source in validated["sources"]} != {binding_digest}
    ):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("SOXL free-source input binding mismatch")
    return research_input_manifest_sha256(validated)


def _verified_close_series(member_bytes: bytes, date_cutoff: object) -> dict[str, list[dict[str, object]]]:
    try:
        payload = json.loads(member_bytes)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input root") from None
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"schema_version", "series"}
        or payload["schema_version"] != _CLOSES_SCHEMA
        or not isinstance(payload["series"], Mapping)
        or set(payload["series"]) != set(_UNIVERSE)
    ):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input root")
    expected = expected_soxl_core_only_sessions(date_cutoff)
    normalized: dict[str, list[dict[str, object]]] = {}
    for symbol in _UNIVERSE:
        canonical = canonical_soxl_core_only_free_split_close_series_bytes(
            symbol=symbol, series=payload["series"][symbol]
        )
        source = json.loads(canonical)
        sessions = source["sessions"]
        actual = tuple(date.fromisoformat(row["session_date"]) for row in sessions)
        if actual != expected[symbol]:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("incomplete SOXL free-source historical coverage")
        normalized[symbol] = sessions
    if _closes_bytes(normalized) != member_bytes:
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input root")
    return normalized


def _require_new_private_output_root(output_root: str | Path) -> Path:
    destination = Path(output_root)
    if destination.exists() or destination.is_symlink():
        raise SoxlCoreOnlyFreeSplitCloseP1Error("immutable output already exists")
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise SoxlCoreOnlyFreeSplitCloseP1Error("output parent is unavailable")
    return destination


def _publish_noreplace(source: Path, destination: Path) -> None:
    if not sys.platform.startswith("linux"):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("unsupported platform for atomic no-clobber publish")
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("required no-clobber capability unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    parent_fd = os.open(destination.parent, flags)
    try:
        result = renameat2(parent_fd, source.name.encode(), parent_fd, destination.name.encode(), 1)
    finally:
        os.close(parent_fd)
    if result != 0:
        if ctypes.get_errno() == errno.EEXIST:
            raise SoxlCoreOnlyFreeSplitCloseP1Error("immutable output already exists")
        raise SoxlCoreOnlyFreeSplitCloseP1Error("atomic no-clobber publish failed")


def publish_soxl_core_only_free_split_close_p1_inputs(
    observer: SoxlCoreOnlyFreeSplitCloseObserver,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    date_cutoff: str,
    p2_contract: object | None = None,
) -> dict[str, object]:
    """Publish a verified candidate-bound P1 root, or fail closed without a root."""
    contract = _resolve_p2_contract(p2_contract)
    destination = _require_new_private_output_root(output_root)
    validate_soxl_core_only_free_split_close_completed_session(
        date_cutoff=date_cutoff,
        observed_at=observed_at,
    )
    binding = build_soxl_core_only_free_split_close_p1_binding(
        date_cutoff=date_cutoff,
        p2_contract=contract,
    )
    expected = expected_soxl_core_only_sessions(date_cutoff)
    canonical_series: dict[str, list[dict[str, object]]] = {}
    close_digests: dict[str, str] = {}
    reports: dict[str, object] = {}
    for symbol in _UNIVERSE:
        start_date = expected[symbol][0].isoformat()
        try:
            observations = tuple(
                observer.observe_daily_bars(
                    source_id=source_id,
                    symbol=symbol,
                    start_date=start_date,
                    date_cutoff=date_cutoff,
                )
                for source_id in (_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID)
            )
            report = assess_multisource_daily_bars(
                _policy(symbol=symbol, date_cutoff=date_cutoff, p2_contract=contract), observations
            )
        except SoxlCoreOnlyFreeSplitCloseP1Error:
            raise
        except Exception:  # noqa: BLE001 - never leak transport/provider detail from the P1 boundary
            raise SoxlCoreOnlyFreeSplitCloseP1Error("data-only source observation failed") from None
        if report.status != DATA_ASSURANCE_STATUS_VERIFIED or not report.can_publish_research_input:
            raise SoxlCoreOnlyFreeSplitCloseP1UnavailableError(
                "SOXL free-source assurance not verified",
                reason_code=classify_soxl_core_only_free_split_close_unavailability(
                    observations=observations,
                    expected_sessions=expected[symbol],
                ),
            )
        canonical_observation = observations[0]
        series, digest = _close_series_from_snapshot(
            symbol=symbol,
            observation=canonical_observation,
            expected_sessions=expected[symbol],
        )
        canonical_series[symbol] = series
        close_digests[symbol] = digest
        reports[symbol] = report
    closes_bytes = _closes_bytes(canonical_series)
    assurance_bytes = _canonical(
        _assurance_member(
            date_cutoff=date_cutoff,
            reports=reports,
            canonical_close_sha256=close_digests,
            p2_contract=contract,
        )
    )
    manifest = build_soxl_core_only_free_split_close_input_manifest(
        binding,
        observed_at=observed_at,
        producer=producer,
        closes_bytes=closes_bytes,
        assurance_bytes=assurance_bytes,
        p2_contract=contract,
    )
    manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / "binding.json").write_bytes(
            canonical_soxl_core_only_free_split_close_p1_binding_bytes(binding, p2_contract=contract)
        )
        (temporary / "closes.json").write_bytes(closes_bytes)
        (temporary / "assurance.json").write_bytes(assurance_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        manifest_sha256 = verify_soxl_core_only_free_split_close_p1_input_root(
            temporary,
            p2_contract=contract,
        )
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"manifest_sha256": manifest_sha256, "status": "P1_FREE_SPLIT_CLOSE_INPUTS_PUBLISHED"}


def verify_soxl_core_only_free_split_close_p1_input_root(
    output_root: str | Path, *, p2_contract: object | None = None
) -> str:
    """Verify an immutable candidate-bound P1 root without provider or network access."""
    contract = _resolve_p2_contract(p2_contract)
    root = Path(output_root)
    try:
        root_stat = root.lstat()
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise ValueError
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise ValueError
        if {entry.name for entry in root.iterdir()} != _OUTPUT_FILENAMES:
            raise ValueError
        binding_bytes = (root / "binding.json").read_bytes()
        closes_bytes = (root / "closes.json").read_bytes()
        assurance_bytes = (root / "assurance.json").read_bytes()
        manifest_bytes = (root / "manifest.json").read_bytes()
        binding = json.loads(binding_bytes)
        if binding_bytes != canonical_soxl_core_only_free_split_close_p1_binding_bytes(
            binding,
            p2_contract=contract,
        ):
            raise ValueError
        manifest = json.loads(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            raise ValueError
        manifest_sha256 = validate_soxl_core_only_free_split_close_input_manifest(
            manifest,
            binding,
            p2_contract=contract,
        )
        series = _verified_close_series(closes_bytes, binding["data_identity"]["date_cutoff"])
        close_digests = {
            symbol: hashlib.sha256(
                canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=series[symbol])
            ).hexdigest()
            for symbol in _UNIVERSE
        }
        assurance = json.loads(assurance_bytes)
        source_snapshots = validate_soxl_core_only_free_split_close_assurance_member(
            assurance,
            date_cutoff=binding["data_identity"]["date_cutoff"],
            canonical_close_sha256=close_digests,
            p2_contract=contract,
        )
        if assurance_bytes != _canonical(assurance):
            raise ValueError
        members = {member["path"]: member for member in manifest["members"]}
        expected_members = {
            "assurance.json": assurance_bytes,
            "closes.json": closes_bytes,
        }
        if set(members) != set(expected_members) or any(
            members[path]
            != {
                "path": path,
                "media_type": "application/json",
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for path, content in expected_members.items()
        ):
            raise ValueError
        expected_sources = {
            f"{source_id}:{symbol}": source_snapshots[symbol][source_id]
            for symbol in _UNIVERSE
            for source_id in (_CANONICAL_SOURCE_ID, _VERIFIER_SOURCE_ID)
        }
        actual_sources = {source["source_id"]: source["content_sha256"] for source in manifest["sources"]}
        if actual_sources != expected_sources:
            raise ValueError
        return manifest_sha256
    except (
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
        SoxlCoreOnlyFreeSplitCloseP1Error,
    ):
        raise SoxlCoreOnlyFreeSplitCloseP1Error("invalid SOXL free-source input root") from None


__all__ = [
    "SOXL_FREE_SOURCE_REASON_ASSURANCE_NOT_VERIFIED",
    "SOXL_FREE_SOURCE_REASON_PRICE_AGREEMENT_NOT_VERIFIED",
    "SOXL_FREE_SOURCE_REASON_PROVIDER_UNAVAILABLE",
    "SOXL_FREE_SOURCE_REASON_SESSION_COVERAGE_MISMATCH",
    "SOXL_FREE_SOURCE_REASON_XNYS_SESSION_NOT_COMPLETE",
    "SOXL_FREE_SOURCE_REASON_YAHOO_SETTLEMENT_LAG",
    "SoxlCoreOnlyFreeSplitCloseObserver",
    "SoxlCoreOnlyFreeSplitCloseP1Error",
    "SoxlCoreOnlyFreeSplitCloseP1UnavailableError",
    "build_soxl_core_only_free_split_close_input_manifest",
    "build_soxl_core_only_free_split_close_p1_binding",
    "canonical_soxl_core_only_free_split_close_p1_binding_bytes",
    "canonical_soxl_core_only_free_split_close_series_bytes",
    "classify_soxl_core_only_free_split_close_unavailability",
    "publish_soxl_core_only_free_split_close_p1_inputs",
    "soxl_core_only_free_split_close_p1_binding_sha256",
    "validate_soxl_core_only_free_split_close_assurance_member",
    "validate_soxl_core_only_free_split_close_completed_session",
    "validate_soxl_core_only_free_split_close_input_manifest",
    "validate_soxl_core_only_free_split_close_p1_binding",
    "verify_soxl_core_only_free_split_close_p1_input_root",
]
