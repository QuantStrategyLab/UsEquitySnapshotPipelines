"""Data-only P1 identity for the frozen SOXL/SOXX core-only P2 v2 candidate.

This module defines and validates provenance for a future immutable input.  It
does not call a provider, read credentials, write a snapshot, schedule work,
replay a strategy, or create an order.  Those actions require a later P1
publisher and P3 verifier bound to this exact contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date

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


class SoxlCoreOnlyP1BindingError(ValueError):
    """Sanitized failure for an invalid SOXL core-only P1 identity."""


def _canonical(value: Mapping[str, object]) -> bytes:
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
    if parsed.isoformat() != value or parsed.weekday() >= 5:
        raise SoxlCoreOnlyP1BindingError("invalid SOXL core-only date cutoff")
    return value


def build_soxl_core_only_p1_binding(*, date_cutoff: object) -> dict[str, object]:
    """Build one exact data identity without acquiring market data.

    ``date_cutoff`` is a completed weekday supplied by a future P1 publisher.
    The publisher and P3 verifier must additionally prove complete XNYS-session
    coverage; this pure binding deliberately has no provider side effects.
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
                "source": "exchange_calendars",
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
