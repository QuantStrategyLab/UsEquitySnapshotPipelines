"""Data-only P1 identity for the frozen SOXL/SOXX core-only P2 v2 candidate.

This module defines and validates provenance plus the authoritative XNYS
session identity for an immutable input.  It does not call a market-data
provider, read credentials, write a snapshot, schedule work, replay a
strategy, or create an order.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import date

import exchange_calendars as xcals
import pandas as pd
from quant_platform_kit.data.research_input import (
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

from .soxl_core_only_p2_v2_contract import (
    FUTURE_INPUT_CONTRACT_ID,
    P2_V2_CONTRACT,
)

INPUT_CONTRACT_ID = FUTURE_INPUT_CONTRACT_ID
_INPUT_SCHEMA = "qsl.soxl_soxx_core_only_p1_data_binding.v1"
_UNIVERSE = ("SOXL", "SOXX", "BOXX")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SOXL_SOXX_FIRST_SESSION = date(2022, 1, 3)
_FIRST_ELIGIBLE_SESSION = {
    "SOXL": _SOXL_SOXX_FIRST_SESSION,
    "SOXX": _SOXL_SOXX_FIRST_SESSION,
    "BOXX": date(2022, 12, 28),
}
_CALENDAR_SOURCE = "exchange_calendars:4.13.2:XNYS"
BARS_SCHEMA = "qsl.soxl-soxx-core-only-adjusted-ohlcv.v2"
SOURCE_SERIES_SCHEMA = "qsl.soxl-soxx-core-only-adjusted-ohlcv-source.v1"


class SoxlCoreOnlyP1BindingError(ValueError):
    """Sanitized failure for an invalid SOXL core-only P1 identity."""


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _date_cutoff(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only date cutoff")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only date cutoff") from exc
    sessions = _expected_xnys_sessions(_SOXL_SOXX_FIRST_SESSION, parsed)
    if parsed.isoformat() != value or not sessions or sessions[-1] != parsed:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only date cutoff")
    return value


def _expected_xnys_sessions(start: date, cutoff: date) -> tuple[date, ...]:
    """Resolve calendar sessions from the versioned XNYS calendar dependency."""
    if start > cutoff:
        return ()
    try:
        calendar = xcals.get_calendar("XNYS")
        labels = calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(cutoff))
        return tuple(label.date() for label in labels)
    except (AttributeError, TypeError, ValueError) as exc:
        raise SoxlCoreOnlyP1BindingError("XNYS calendar is unavailable") from exc


def expected_soxl_core_only_sessions(date_cutoff: object) -> dict[str, tuple[date, ...]]:
    """Return the exact per-symbol P1 XNYS coverage for one completed cutoff.

    The SOXL/SOXX history begins well before the fixed 252-session indicator
    warm-up.  BOXX begins at its eligible ETF session.  This is calendar-only:
    it neither requests nor retains market data.
    """
    cutoff = _date_cutoff(date_cutoff)
    resolved_cutoff = date.fromisoformat(cutoff)
    return {
        symbol: _expected_xnys_sessions(first_session, resolved_cutoff)
        for symbol, first_session in _FIRST_ELIGIBLE_SESSION.items()
    }


def _normalized_source_bar(value: object) -> dict[str, float]:
    if not isinstance(value, Mapping) or set(value) != {"open", "high", "low", "close", "volume"}:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
    normalized: dict[str, float] = {}
    for field in ("open", "high", "low", "close", "volume"):
        raw = value[field]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
        number = float(raw)
        if not math.isfinite(number) or (field != "volume" and number <= 0.0) or (
            field == "volume" and number < 0.0
        ):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
        normalized[field] = 0.0 if number == 0.0 else number
    if (
        normalized["low"] > min(normalized["open"], normalized["close"])
        or normalized["high"] < max(normalized["open"], normalized["close"])
        or normalized["high"] < normalized["low"]
    ):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
    return normalized


def canonical_soxl_core_only_source_series_bytes(
    *, symbol: object, series: object
) -> bytes:
    """Canonically encode one provider-normalized series for P1 source hashing."""
    if not isinstance(symbol, str) or symbol not in _UNIVERSE or not isinstance(series, list):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
    normalized: list[dict[str, object]] = []
    previous: date | None = None
    for raw_session in series:
        if not isinstance(raw_session, Mapping) or set(raw_session) != {"session_date", "bar"}:
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
        raw_date = raw_session["session_date"]
        if not isinstance(raw_date, str):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
        try:
            parsed = date.fromisoformat(raw_date)
        except ValueError as exc:
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series") from exc
        if parsed.isoformat() != raw_date or (previous is not None and parsed <= previous):
            raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
        previous = parsed
        normalized.append({"session_date": raw_date, "bar": _normalized_source_bar(raw_session["bar"])})
    if not normalized:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only source series")
    return _canonical(
        {
            "schema_version": SOURCE_SERIES_SCHEMA,
            "symbol": symbol,
            "sessions": normalized,
        }
    )


def build_soxl_core_only_p1_binding(*, date_cutoff: object) -> dict[str, object]:
    """Build one exact data identity without acquiring market data.

    ``date_cutoff`` is a completed XNYS session.  The publisher separately
    proves complete XNYS-session coverage before it writes an input root.
    """
    cutoff = _date_cutoff(date_cutoff)
    return {
        "schema_version": _INPUT_SCHEMA,
        "candidate": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "source": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": P2_V2_CONTRACT.ues_revision,
        },
        "data_identity": {
            "provider": "ALPACA_MARKET_DATA",
            "feed": "SIP",
            "calendar": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "source": _CALENDAR_SOURCE,
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
            },
            "universe": list(_UNIVERSE),
            "date_cutoff": cutoff,
        },
    }


def validate_soxl_core_only_p1_binding(value: Mapping[str, object]) -> dict[str, object]:
    """Reject any P1 binding other than this candidate's exact data identity."""
    try:
        identity = value["data_identity"]
        if not isinstance(identity, Mapping):
            raise TypeError
        cutoff = identity["date_cutoff"]
    except (KeyError, TypeError):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only P1 binding") from None
    expected = build_soxl_core_only_p1_binding(date_cutoff=cutoff)
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only P1 binding")
    return expected


def canonical_soxl_core_only_p1_binding_bytes(value: Mapping[str, object]) -> bytes:
    """Encode a validated binding deterministically for a P1/P3 manifest."""
    return _canonical(validate_soxl_core_only_p1_binding(value))


def soxl_core_only_p1_binding_sha256(value: Mapping[str, object]) -> str:
    """Return the digest carried by the immutable-input manifest provenance."""
    return hashlib.sha256(canonical_soxl_core_only_p1_binding_bytes(value)).hexdigest()


def build_soxl_core_only_input_manifest(
    binding: Mapping[str, object],
    *,
    observed_at: str,
    producer: Mapping[str, object],
    member_bytes: bytes,
    source_content_sha256: Mapping[str, str],
) -> dict[str, object]:
    """Build a manifest for one already-collected private daily-bar member.

    The caller supplies only opaque bytes and member digests.  This function
    neither inspects the bars nor accesses an external provider.
    """
    frozen = validate_soxl_core_only_p1_binding(binding)
    if (
        not isinstance(member_bytes, bytes)
        or set(source_content_sha256) != set(_UNIVERSE)
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in source_content_sha256.values()
        )
    ):
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input member")
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = soxl_core_only_p1_binding_sha256(frozen)
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"soxl-core-only-{binding_digest[:24]}-{hashlib.sha256(member_bytes).hexdigest()[:24]}",
            "research_input_contract_id": INPUT_CONTRACT_ID,
            "domain": "us_equity",
            "profile": P2_V2_CONTRACT.candidate_id,
            "artifact_type": "immutable_adjusted_ohlcv_etf_only",
            "observed_at": observed_at,
            "effective_at": observed_at,
            "as_of": observed_at,
            "producer": dict(producer),
            "calendar": {
                **identity["calendar"],
                "session_date": identity["date_cutoff"],
                "source_revision": binding_digest,
            },
            "adjustment": {
                **identity["adjustment"],
                "source_revision": binding_digest,
            },
            "sources": [
                {
                    "source_id": f"alpaca_sip_1day_adjustment_all:{symbol}",
                    "revision": binding_digest,
                    "observed_at": observed_at,
                    "content_sha256": source_content_sha256[symbol],
                }
                for symbol in sorted(_UNIVERSE)
            ],
            "members": [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(member_bytes),
                    "sha256": hashlib.sha256(member_bytes).hexdigest(),
                }
            ],
        }
    )
    validate_soxl_core_only_input_manifest(manifest, frozen)
    return manifest


def validate_soxl_core_only_input_manifest(
    manifest: Mapping[str, object],
    binding: Mapping[str, object],
) -> str:
    """Validate immutable-input provenance before a future P3 verifier runs."""
    frozen = validate_soxl_core_only_p1_binding(binding)
    try:
        validated = validate_research_input_manifest(manifest)
    except ValueError as exc:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only input manifest") from exc
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = soxl_core_only_p1_binding_sha256(frozen)
    if (
        validated["research_input_contract_id"] != INPUT_CONTRACT_ID
        or validated["domain"] != "us_equity"
        or validated["profile"] != P2_V2_CONTRACT.candidate_id
        or validated["artifact_type"] != "immutable_adjusted_ohlcv_etf_only"
        or validated["calendar"]
        != {
            **identity["calendar"],
            "session_date": identity["date_cutoff"],
            "source_revision": binding_digest,
        }
        or validated["adjustment"]
        != {
            **identity["adjustment"],
            "source_revision": binding_digest,
        }
        or {source["source_id"] for source in validated["sources"]}
        != {f"alpaca_sip_1day_adjustment_all:{symbol}" for symbol in _UNIVERSE}
        or {source["revision"] for source in validated["sources"]} != {binding_digest}
    ):
        raise SoxlCoreOnlyP1BindingError("SOXL core-only input binding mismatch")
    return research_input_manifest_sha256(validated)
