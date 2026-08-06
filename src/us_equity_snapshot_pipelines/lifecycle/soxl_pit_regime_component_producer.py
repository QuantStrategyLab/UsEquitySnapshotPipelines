"""Pure offline producer for the identity-bound SOXL core-only PIT contract."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_ID = "SOXL_P3_CORE_ONLY_9_INPUT_V1"
SOURCE_CONTRACT_SCHEMA = "qsl.soxl_core_only_9_input_source_contract.v1"
PRODUCER_RECEIPT_SCHEMA = "qsl.soxl_core_only_unavailable_components_receipt.v1"
MARKET_REGIME_SCHEMA = "soxl_core_only_market_regime_unavailable.v1"
FIXED_CUTOFF = "2026-08-05T03:59:59Z"
ALL_LOGICAL_INPUTS = (
    "SOXL",
    "SOXX",
    "BOXX",
    "SCHD",
    "DGRO",
    "SGOV",
    "SPYI",
    "QQQI",
    "QQQ",
)
UNAVAILABLE_COMPONENTS = (
    "crisis",
    "macro",
    "taco",
    "panic_reversal",
    "volatility_delever_price_rebound",
)
_FIRST_LOGICAL_INPUT_SESSION = {
    **{symbol: "2018-08-03" for symbol in ALL_LOGICAL_INPUTS},
    "SGOV": "2020-05-26",
    "SPYI": "2022-08-29",
    "BOXX": "2022-12-27",
    "QQQI": "2024-01-29",
}
FROZEN_CORE_ONLY_CONTRACT = {
    "schema_version": "soxl_core_only_9_input_contract.v1",
    "candidate_id": CANDIDATE_ID,
    "ordered_logical_inputs": list(ALL_LOGICAL_INPUTS),
    "market_regime_control_enabled": False,
    "unavailable_components": {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    },
    "benchmark_symbol": "SOXX",
    "substitution_policy": "none_no_proxy_no_alias",
    "position_control_allowed": False,
    "promotion_eligible": False,
    "live_ready": False,
    "size_zero_required": True,
    "no_order": True,
}

_SOURCE_CONTRACT_KEYS = frozenset(
    {
        "schema_version",
        "data_class",
        "observed_at",
        "effective_at",
        "as_of",
        "fixed_cutoff",
        "input_content_sha256",
        "calendar",
        "producer",
        "candidate_id",
        "candidate_contract_sha256",
        "market_regime_control_enabled",
        "unavailable_components",
        "logical_inputs",
    }
)
_LOGICAL_INPUT_KEYS = frozenset(
    {
        "logical_input_id",
        "provider_instrument_id",
        "instrument_type",
        "venue",
        "currency",
        "provider_id",
        "source_revision",
        "field",
        "frequency",
        "timezone",
        "calendar",
        "adjustment_contract",
        "corporate_action_basis",
        "missing_value_policy",
        "data_origin",
        "substitution_policy",
        "entitlement_receipt_sha256",
        "license_or_usage_receipt_sha256",
        "retention_scope",
        "retention_expires_at",
        "request_sha256",
        "observed_at",
        "effective_at",
        "fixed_cutoff",
        "content_sha256",
        "row_count",
        "first_date",
        "last_date",
        "no_future_rows",
    }
)
_SENSITIVE_MARKERS = (
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
    "cookie",
    "jwt",
    "apikey",
    "headers",
    "rawresponse",
    "responsebody",
)


class SoxlPITRegimeProducerError(ValueError):
    """Fail-closed producer error without source, credential, or data detail."""


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlPITRegimeProducerError("invalid canonical input") from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _git_blob_sha(value: bytes) -> str:
    return hashlib.sha1(f"blob {len(value)}\0".encode() + value).hexdigest()


CORE_ONLY_CONFIG_SHA256 = _sha256(canonical_json_bytes(FROZEN_CORE_ONLY_CONTRACT))


def _exact_mapping(value: object, keys: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _nonblank(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    return value


def _digest(value: object, label: str) -> str:
    result = _nonblank(value, label)
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    return result


def _revision(value: object, label: str) -> str:
    result = _nonblank(value, label)
    if len(result) != 40 or any(character not in "0123456789abcdef" for character in result):
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    return result


def _timestamp(value: object, label: str) -> datetime:
    result = _nonblank(value, label)
    if not result.endswith("Z"):
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    try:
        parsed = datetime.fromisoformat(result.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise SoxlPITRegimeProducerError(f"invalid {label}") from exc
    if parsed.tzinfo != timezone.utc:
        raise SoxlPITRegimeProducerError(f"invalid {label}")
    return parsed


def _reject_sensitive(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = "".join(character for character in str(key).lower() if character.isalnum())
            if any(marker in normalized for marker in _SENSITIVE_MARKERS):
                raise SoxlPITRegimeProducerError("sensitive input is forbidden")
            _reject_sensitive(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _reject_sensitive(item)


def runtime_producer_source_identity(*, commit_sha: str, tree_sha: str) -> dict[str, str]:
    """Return local code identity; callers still supply the trusted commit and tree anchors."""
    payload = Path(__file__).read_bytes()
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": _revision(commit_sha, "producer commit"),
        "tree_sha": _revision(tree_sha, "producer tree"),
        "module_blob_sha": _git_blob_sha(payload),
        "module_content_sha256": _sha256(payload),
        "tool": "soxl_pit_regime_component_producer",
        "tool_version": "2",
    }


def _logical_payload(sessions: Sequence[Mapping[str, Any]], logical_input_id: str) -> list[dict[str, Any]]:
    return [
        {"date": session["date"], **session["bars"][logical_input_id]}
        for session in sessions
        if logical_input_id in session["bars"]
    ]


@dataclass(frozen=True)
class ValidatedSoxlPITRegimeSource:
    contract: dict[str, Any]
    source_contract_sha256: str
    prefix_input_sha256: tuple[str, ...]
    synthetic_fixture: bool


def validate_soxl_pit_regime_source_contract(
    sessions: Sequence[Mapping[str, Any]],
    source_contract: Mapping[str, Any],
    *,
    expected_sessions: Sequence[str],
    trusted_source_contract_sha256: str | None = None,
) -> ValidatedSoxlPITRegimeSource:
    """Validate exactly nine price inputs and their source/license/content identities."""
    if len(sessions) != len(expected_sessions):
        raise SoxlPITRegimeProducerError("exact XNYS sessions are required")
    contract = _exact_mapping(source_contract, _SOURCE_CONTRACT_KEYS, "regime source contract")
    _reject_sensitive(contract)
    if contract["schema_version"] != SOURCE_CONTRACT_SCHEMA:
        raise SoxlPITRegimeProducerError("invalid regime source schema")
    if contract["data_class"] not in {"provider_observed", "synthetic_fixture"}:
        raise SoxlPITRegimeProducerError("invalid source data class")
    synthetic_fixture = contract["data_class"] == "synthetic_fixture"
    for session, expected_date in zip(sessions, expected_sessions, strict=True):
        if set(session) != {"date", "bars"} or session["date"] != expected_date:
            raise SoxlPITRegimeProducerError("exact prefix session is required")
        expected_bars = {
            symbol
            for symbol in ALL_LOGICAL_INPUTS
            if expected_date >= _FIRST_LOGICAL_INPUT_SESSION[symbol]
        }
        if not isinstance(session["bars"], Mapping) or set(session["bars"]) != expected_bars:
            raise SoxlPITRegimeProducerError("exact eligible input set is required")
    if contract["fixed_cutoff"] != FIXED_CUTOFF:
        raise SoxlPITRegimeProducerError("invalid fixed cutoff")
    effective = _timestamp(contract["effective_at"], "effective_at")
    observed = _timestamp(contract["observed_at"], "observed_at")
    as_of = _timestamp(contract["as_of"], "as_of")
    cutoff = _timestamp(contract["fixed_cutoff"], "fixed cutoff")
    if effective > observed or cutoff > observed or observed > as_of:
        raise SoxlPITRegimeProducerError("invalid source timestamp ordering")
    if contract["input_content_sha256"] != _sha256(canonical_json_bytes(sessions)):
        raise SoxlPITRegimeProducerError("input content digest mismatch")

    calendar = _exact_mapping(
        contract["calendar"],
        frozenset({"calendar_id", "timezone", "source", "source_revision", "first_session", "last_session", "session_count", "sessions_sha256"}),
        "calendar identity",
    )
    expected_calendar = {
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "source": "exchange_calendars",
        "source_revision": "4.13.2",
        "first_session": expected_sessions[0],
        "last_session": expected_sessions[-1],
        "session_count": len(expected_sessions),
        "sessions_sha256": _sha256(canonical_json_bytes(list(expected_sessions))),
    }
    if calendar != expected_calendar:
        raise SoxlPITRegimeProducerError("calendar identity mismatch")

    producer = _exact_mapping(
        contract["producer"],
        frozenset({"repository", "commit_sha", "tree_sha", "module_blob_sha", "module_content_sha256", "tool", "tool_version"}),
        "producer identity",
    )
    expected_producer = runtime_producer_source_identity(
        commit_sha=producer["commit_sha"], tree_sha=producer["tree_sha"]
    )
    if producer != expected_producer:
        raise SoxlPITRegimeProducerError("producer source identity mismatch")
    contract["producer"] = producer
    if contract["candidate_id"] != CANDIDATE_ID:
        raise SoxlPITRegimeProducerError("candidate identity mismatch")
    if (
        contract["candidate_contract_sha256"] != CORE_ONLY_CONFIG_SHA256
        or _sha256(canonical_json_bytes(FROZEN_CORE_ONLY_CONTRACT)) != CORE_ONLY_CONFIG_SHA256
    ):
        raise SoxlPITRegimeProducerError("candidate contract identity mismatch")
    if contract["market_regime_control_enabled"] is not False:
        raise SoxlPITRegimeProducerError("market regime control identity mismatch")
    unavailable = {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    if contract["unavailable_components"] != unavailable:
        raise SoxlPITRegimeProducerError("unavailable component identity mismatch")

    entries = contract["logical_inputs"]
    if not isinstance(entries, list) or [entry.get("logical_input_id") for entry in entries if isinstance(entry, Mapping)] != list(ALL_LOGICAL_INPUTS):
        raise SoxlPITRegimeProducerError("exact 9 logical input contract is required")
    canonical_entries: list[dict[str, Any]] = []
    provider_instruments: set[tuple[str, str]] = set()
    for logical_input_id, raw_entry in zip(ALL_LOGICAL_INPUTS, entries, strict=True):
        entry = _exact_mapping(raw_entry, _LOGICAL_INPUT_KEYS, "logical input source")
        if entry["logical_input_id"] != logical_input_id:
            raise SoxlPITRegimeProducerError("logical input identity mismatch")
        for field in (
            "provider_instrument_id",
            "venue",
            "provider_id",
            "source_revision",
            "retention_scope",
        ):
            _nonblank(entry[field], field)
        provider_instrument = (entry["provider_id"], entry["provider_instrument_id"])
        if provider_instrument in provider_instruments:
            raise SoxlPITRegimeProducerError("provider instrument alias collision")
        provider_instruments.add(provider_instrument)
        if entry["currency"] != "USD" or entry["frequency"] != "1d":
            raise SoxlPITRegimeProducerError("logical input market identity mismatch")
        if entry["timezone"] != "America/New_York" or entry["calendar"] != "XNYS":
            raise SoxlPITRegimeProducerError("logical input calendar mismatch")
        if (
            entry["instrument_type"] != "etf"
            or entry["field"] != "adjusted_ohlcv"
            or entry["adjustment_contract"] != "total_return_adjusted"
            or entry["corporate_action_basis"] != "provider_adjusted"
            or entry["missing_value_policy"] != "reject"
            or entry["substitution_policy"] != "none"
            or entry["data_origin"] != contract["data_class"]
        ):
            raise SoxlPITRegimeProducerError("logical input source semantics mismatch")
        if not synthetic_fixture and any(
            marker in str(entry[field]).lower()
            for field in ("provider_id", "provider_instrument_id")
            for marker in ("synthetic", "proxy")
        ):
            raise SoxlPITRegimeProducerError("synthetic or proxy substitution is forbidden")
        for field in (
            "entitlement_receipt_sha256",
            "license_or_usage_receipt_sha256",
            "request_sha256",
            "content_sha256",
        ):
            _digest(entry[field], field)
        if (
            entry["observed_at"] != contract["observed_at"]
            or entry["effective_at"] != contract["effective_at"]
            or entry["fixed_cutoff"] != FIXED_CUTOFF
            or _timestamp(entry["retention_expires_at"], "retention expiry") < as_of
            or entry["no_future_rows"] is not True
        ):
            raise SoxlPITRegimeProducerError("logical input source timing mismatch")
        payload = _logical_payload(sessions, logical_input_id)
        if not payload:
            raise SoxlPITRegimeProducerError("missing logical input")
        expected_dates = [
            session_date
            for session_date in expected_sessions
            if session_date >= _FIRST_LOGICAL_INPUT_SESSION[logical_input_id]
        ]
        if (
            entry["content_sha256"] != _sha256(canonical_json_bytes(payload))
            or entry["row_count"] != len(payload)
            or entry["first_date"] != payload[0]["date"]
            or entry["last_date"] != expected_sessions[-1]
            or [item["date"] for item in payload] != expected_dates
        ):
            raise SoxlPITRegimeProducerError("logical input content identity mismatch")
        canonical_entries.append(entry)
    contract["logical_inputs"] = canonical_entries

    source_contract_sha256 = _sha256(canonical_json_bytes(contract))
    if synthetic_fixture:
        if trusted_source_contract_sha256 is not None:
            raise SoxlPITRegimeProducerError("synthetic fixture cannot claim trusted source authority")
    elif trusted_source_contract_sha256 is None:
        raise SoxlPITRegimeProducerError("trusted source contract identity is required")
    elif _digest(trusted_source_contract_sha256, "trusted source contract digest") != source_contract_sha256:
        raise SoxlPITRegimeProducerError("trusted source contract identity mismatch")
    prefix_sha = source_contract_sha256
    prefix_digests: list[str] = []
    for session, expected_date in zip(sessions, expected_sessions, strict=True):
        prefix_row = {
            "date": expected_date,
            "bars": {
                symbol: copy.deepcopy(session["bars"][symbol])
                for symbol in ALL_LOGICAL_INPUTS
                if symbol in session["bars"]
            },
        }
        prefix_sha = _sha256(
            canonical_json_bytes({"previous_sha256": prefix_sha, "session": prefix_row})
        )
        prefix_digests.append(prefix_sha)
    return ValidatedSoxlPITRegimeSource(
        contract=contract,
        source_contract_sha256=source_contract_sha256,
        prefix_input_sha256=tuple(prefix_digests),
        synthetic_fixture=synthetic_fixture,
    )


def _prefix_digest(
    sessions: Sequence[Mapping[str, Any]], *, end_index: int, source_contract_sha256: str
) -> str:
    digest = source_contract_sha256
    for session in sessions[: end_index + 1]:
        prefix_row = {
            "date": session["date"],
            "bars": {
                symbol: copy.deepcopy(session["bars"][symbol])
                for symbol in ALL_LOGICAL_INPUTS
                if symbol in session["bars"]
            },
        }
        digest = _sha256(
            canonical_json_bytes({"previous_sha256": digest, "session": prefix_row})
        )
    return digest


def _produce_soxl_pit_regime_component_receipt(
    sessions: Sequence[Mapping[str, Any]],
    source: ValidatedSoxlPITRegimeSource,
    *,
    session_index: int,
    verify_prefix_identity: bool = True,
) -> dict[str, Any]:
    if not isinstance(source, ValidatedSoxlPITRegimeSource):
        raise SoxlPITRegimeProducerError("validated source contract is required")
    if _sha256(canonical_json_bytes(FROZEN_CORE_ONLY_CONTRACT)) != CORE_ONLY_CONFIG_SHA256:
        raise SoxlPITRegimeProducerError("frozen core-only contract identity mismatch")
    if isinstance(session_index, bool) or not 0 <= session_index < len(sessions):
        raise SoxlPITRegimeProducerError("invalid prefix end")
    if verify_prefix_identity and _prefix_digest(
        sessions,
        end_index=session_index,
        source_contract_sha256=source.source_contract_sha256,
    ) != source.prefix_input_sha256[session_index]:
        raise SoxlPITRegimeProducerError("prefix input identity mismatch")
    session_date = str(sessions[session_index]["date"])
    unavailable = {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    receipt = {
        "schema_version": PRODUCER_RECEIPT_SCHEMA,
        "candidate_id": CANDIDATE_ID,
        "as_of": session_date,
        "evidence_class": "synthetic_fixture" if source.synthetic_fixture else "provider_observed",
        "real_producer": not source.synthetic_fixture,
        "market_regime_control_enabled": False,
        "unavailable_components": unavailable,
        "position_control_allowed": False,
        "provenance": {
            "source_contract_sha256": source.source_contract_sha256,
            "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
            "prefix_input_manifest_sha256": source.prefix_input_sha256[session_index],
            "prefix_session_count": session_index + 1,
            "prefix_end": session_date,
            "future_sessions_exposed": False,
            "logical_input_ids": list(ALL_LOGICAL_INPUTS),
            "raw_series_persisted": False,
        },
    }
    receipt["receipt_sha256"] = _sha256(canonical_json_bytes(receipt))
    _reject_sensitive(receipt)
    canonical_json_bytes(receipt)
    return receipt


def produce_soxl_pit_regime_component_receipt(
    sessions: Sequence[Mapping[str, Any]],
    source: ValidatedSoxlPITRegimeSource,
    *,
    session_index: int,
) -> dict[str, Any]:
    """Return honest unavailable declarations and digest-only prefix provenance."""
    if not isinstance(source, ValidatedSoxlPITRegimeSource):
        raise SoxlPITRegimeProducerError("validated source contract is required")
    return _produce_soxl_pit_regime_component_receipt(
        sessions,
        source,
        session_index=session_index,
    )


def produce_soxl_pit_regime_component_receipts(
    sessions: Sequence[Mapping[str, Any]], source: ValidatedSoxlPITRegimeSource
) -> tuple[dict[str, Any], ...]:
    """Produce every frozen session after one exact source identity verification."""
    if not isinstance(source, ValidatedSoxlPITRegimeSource):
        raise SoxlPITRegimeProducerError("validated source contract is required")
    if not sessions or _prefix_digest(
        sessions,
        end_index=len(sessions) - 1,
        source_contract_sha256=source.source_contract_sha256,
    ) != source.prefix_input_sha256[-1]:
        raise SoxlPITRegimeProducerError("prefix input identity mismatch")
    return tuple(
        _produce_soxl_pit_regime_component_receipt(
            sessions,
            source,
            session_index=index,
            verify_prefix_identity=False,
        )
        for index in range(len(sessions))
    )


__all__ = [
    "ALL_LOGICAL_INPUTS",
    "CANDIDATE_ID",
    "CORE_ONLY_CONFIG_SHA256",
    "FIXED_CUTOFF",
    "FROZEN_CORE_ONLY_CONTRACT",
    "MARKET_REGIME_SCHEMA",
    "PRODUCER_RECEIPT_SCHEMA",
    "SOURCE_CONTRACT_SCHEMA",
    "UNAVAILABLE_COMPONENTS",
    "SoxlPITRegimeProducerError",
    "ValidatedSoxlPITRegimeSource",
    "produce_soxl_pit_regime_component_receipt",
    "produce_soxl_pit_regime_component_receipts",
    "runtime_producer_source_identity",
    "validate_soxl_pit_regime_source_contract",
]
