"""Pure offline producer for identity-bound SOXL PIT market-regime evidence."""

from __future__ import annotations

import copy
import hashlib
import inspect
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd
from quant_strategy_plugins import (
    build_crisis_response_shadow_signal,
    build_macro_risk_governor_signal,
    build_market_regime_control_signal,
)
from quant_strategy_plugins.taco_panic_rebound_research import resolve_trade_war_event_set
from quant_strategy_plugins.volatility_delever_price_rebound import (
    build_volatility_delever_price_rebound_context,
)


SOURCE_CONTRACT_SCHEMA = "qsl.soxl_pit_regime_component_source_contract.v1"
PRODUCER_RECEIPT_SCHEMA = "qsl.soxl_pit_regime_component_receipt.v1"
QSP_REVISION = "1f3a27b8fd83d71b583f4f5160a748e95fbefaa1"
QSP_TREE_SHA = "c13e196ba7649343f49eda71365d2b192c8aa7a3"
FIXED_CUTOFF = "2026-08-05T03:59:59Z"
ACTIVE_COMPONENTS = ("crisis", "macro")
DISABLED_COMPONENTS = ("taco", "panic_reversal")
REGIME_LOGICAL_INPUTS = (
    "SOXL",
    "SOXX",
    "SPY",
    "XLF",
    "KRE",
    "HYG",
    "IEF",
    "LQD",
    "TLT",
    "VIX",
)
ADDITIONAL_REGIME_INPUTS = REGIME_LOGICAL_INPUTS[2:]
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
    *ADDITIONAL_REGIME_INPUTS,
)
_FIRST_LOGICAL_INPUT_SESSION = {
    **{symbol: "2018-08-03" for symbol in ALL_LOGICAL_INPUTS},
    "SGOV": "2020-05-26",
    "SPYI": "2022-08-29",
    "BOXX": "2022-12-27",
    "QQQI": "2024-01-29",
}
QSP_SOURCE_IDENTITIES = {
    "crisis": {
        "blob_sha": "be809b893b03c1ca39930b5e741d4d796940e818",
        "content_sha256": "1b15e127fa5213306da063b9d7aa40ebfcc6cdded872d34187f087cb4434ee8c",
    },
    "macro": {
        "blob_sha": "9f801c8b0580af482be93339e6f4abba01812281",
        "content_sha256": "780f4a31fef6e232c1c171d2acc48023eab5e65d26bc5f1063acd98778dbaa38",
    },
    "arbiter": {
        "blob_sha": "1e23204dd7b1824553f70b9106e3564b661801ef",
        "content_sha256": "e69b11a15a08e5a7553397f7d63212c411184101ad3c97e25882949c783c6088",
    },
    "price_rebound": {
        "blob_sha": "1c5e53c309f47afd534e604a59c5737285fcec25",
        "content_sha256": "2f3fd969839a2c6144eb3648434238608bf8a12899d846257aba9264f22adcf9",
    },
    "event_catalog": {
        "blob_sha": "9ef85b20fc946278567b07ae4bbcd71e1028c945",
        "content_sha256": "374c2f42d73b9017c3a3c1f058478340c4a6d91a36aa6aa2636a4dd6479f35a8",
    },
}
_QSP_SOURCE_IDENTITIES_SHA256 = "17888a16e714b85299f82f1710872615c0519b64a8bfa6efbff86b24cc5c1076"
EVENT_CATALOG_SHA256 = "9fbf2d4aa19ac7429218344659eb6bfa54e8fab38b44a6b8f6064ef1f21f0431"
FROZEN_REGIME_CONFIG = {
    "schema_version": "soxl_pit_market_regime_config.v2",
    "active_components": list(ACTIVE_COMPONENTS),
    "disabled_components": list(DISABLED_COMPONENTS),
    "crisis": {
        "event_set": "full",
        "benchmark_symbol": "SOXX",
        "attack_symbol": "SOXL",
        "market_symbol": "SPY",
        "financial_symbols": ["XLF", "KRE"],
        "credit_pairs": [["HYG", "IEF"], ["LQD", "IEF"]],
        "rate_symbols": ["IEF", "TLT"],
        "synthetic_attack_multiple": 0.0,
        "ai_audit_enabled": False,
    },
    "macro": {
        "benchmark_symbol": "SOXX",
        "attack_symbol": "SOXL",
        "vix_symbols": ["VIX"],
        "vix3m_symbols": [],
        "credit_pairs": [["HYG", "IEF"], ["LQD", "IEF"]],
        "external_stress_actionable": False,
        "delever_risk_asset_scalar": 0.0,
    },
    "price_rebound": {
        "enabled": True,
        "benchmark_symbol": "SOXX",
        "vix_symbols": ["VIX"],
        "credit_pairs": ["HYG:IEF", "LQD:IEF"],
        "financial_symbols": ["XLF", "KRE"],
    },
    "arbiter": {
        "strategy_policy": "levered_growth_income_v1",
        "taco_opportunity_size_scalar": 0.0,
    },
    "position_control_authority": "none_static_research_only",
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
        "qsp",
        "config_sha256",
        "event_catalog_sha256",
        "active_components",
        "disabled_components",
        "auxiliary_contexts",
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


MARKET_REGIME_CONFIG_SHA256 = _sha256(canonical_json_bytes(FROZEN_REGIME_CONFIG))


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


def _finite(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SoxlPITRegimeProducerError("invalid regime input")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise SoxlPITRegimeProducerError("invalid regime input")
    return result


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


def _source_file_identity(obj: Callable[..., Any]) -> dict[str, str]:
    source_path = inspect.getsourcefile(obj)
    try:
        payload = Path(source_path).read_bytes() if source_path else b""
    except OSError as exc:
        raise SoxlPITRegimeProducerError("QSP source identity unavailable") from exc
    return {"blob_sha": _git_blob_sha(payload), "content_sha256": _sha256(payload)}


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
        "tool_version": "1",
    }


def _verify_qsp_identity(qsp: object) -> dict[str, Any]:
    if _sha256(canonical_json_bytes(QSP_SOURCE_IDENTITIES)) != _QSP_SOURCE_IDENTITIES_SHA256:
        raise SoxlPITRegimeProducerError("frozen QSP source identity mismatch")
    value = _exact_mapping(qsp, frozenset({"revision", "tree_sha", "sources"}), "QSP identity")
    if value["revision"] != QSP_REVISION or value["tree_sha"] != QSP_TREE_SHA:
        raise SoxlPITRegimeProducerError("QSP revision identity mismatch")
    if value["sources"] != QSP_SOURCE_IDENTITIES:
        raise SoxlPITRegimeProducerError("QSP source identity mismatch")
    observed = {
        "crisis": _source_file_identity(build_crisis_response_shadow_signal),
        "macro": _source_file_identity(build_macro_risk_governor_signal),
        "arbiter": _source_file_identity(build_market_regime_control_signal),
        "price_rebound": _source_file_identity(build_volatility_delever_price_rebound_context),
        "event_catalog": _source_file_identity(resolve_trade_war_event_set),
    }
    if observed != QSP_SOURCE_IDENTITIES:
        raise SoxlPITRegimeProducerError("QSP source identity mismatch")
    return value


def _logical_payload(sessions: Sequence[Mapping[str, Any]], logical_input_id: str) -> list[dict[str, Any]]:
    if logical_input_id in ALL_LOGICAL_INPUTS[:9]:
        return [
            {"date": session["date"], **session["bars"][logical_input_id]}
            for session in sessions
            if logical_input_id in session["bars"]
        ]
    return [
        {"date": session["date"], "value": session["regime_inputs"][logical_input_id]}
        for session in sessions
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
    """Validate 17 logical inputs and their source/license/content identities."""
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
        if set(session) != {"date", "bars", "regime_inputs"} or session["date"] != expected_date:
            raise SoxlPITRegimeProducerError("exact prefix session is required")
        expected_bars = {
            symbol
            for symbol in ALL_LOGICAL_INPUTS[:9]
            if expected_date >= _FIRST_LOGICAL_INPUT_SESSION[symbol]
        }
        if not isinstance(session["bars"], Mapping) or set(session["bars"]) != expected_bars:
            raise SoxlPITRegimeProducerError("exact eligible input set is required")
        regime_inputs = session["regime_inputs"]
        if not isinstance(regime_inputs, Mapping) or set(regime_inputs) != set(ADDITIONAL_REGIME_INPUTS):
            raise SoxlPITRegimeProducerError("exact regime input set is required")
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
    contract["qsp"] = _verify_qsp_identity(contract["qsp"])
    if (
        contract["config_sha256"] != MARKET_REGIME_CONFIG_SHA256
        or _sha256(canonical_json_bytes(FROZEN_REGIME_CONFIG)) != MARKET_REGIME_CONFIG_SHA256
    ):
        raise SoxlPITRegimeProducerError("regime config identity mismatch")
    events = [asdict(event) for event in resolve_trade_war_event_set("full")]
    if contract["event_catalog_sha256"] != EVENT_CATALOG_SHA256 or _sha256(
        canonical_json_bytes(events)
    ) != EVENT_CATALOG_SHA256:
        raise SoxlPITRegimeProducerError("event catalog identity mismatch")
    if contract["active_components"] != list(ACTIVE_COMPONENTS):
        raise SoxlPITRegimeProducerError("active component identity mismatch")
    if contract["disabled_components"] != {
        component: {"enabled": False, "available": False} for component in DISABLED_COMPONENTS
    }:
        raise SoxlPITRegimeProducerError("disabled component identity mismatch")
    if contract["auxiliary_contexts"] != {"volatility_delever_price_rebound": {"enabled": True}}:
        raise SoxlPITRegimeProducerError("price rebound identity mismatch")

    entries = contract["logical_inputs"]
    if not isinstance(entries, list) or [entry.get("logical_input_id") for entry in entries if isinstance(entry, Mapping)] != list(ALL_LOGICAL_INPUTS):
        raise SoxlPITRegimeProducerError("exact 17 logical input contract is required")
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
        is_vix = logical_input_id == "VIX"
        expected_field = "unadjusted_index_close" if is_vix else (
            "adjusted_ohlcv" if logical_input_id in ALL_LOGICAL_INPUTS[:9] else "adjusted_close"
        )
        expected_adjustment = "unadjusted_index" if is_vix else "total_return_adjusted"
        expected_basis = "not_applicable" if is_vix else "provider_adjusted"
        if (
            entry["instrument_type"] != ("index" if is_vix else "etf")
            or entry["field"] != expected_field
            or entry["adjustment_contract"] != expected_adjustment
            or entry["corporate_action_basis"] != expected_basis
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
        regime_inputs = session["regime_inputs"]
        prefix_row = {
            "date": expected_date,
            "values": {
                "SOXL": _finite(session["bars"]["SOXL"]["close"]),
                "SOXX": _finite(session["bars"]["SOXX"]["close"]),
                **{symbol: _finite(regime_inputs[symbol]) for symbol in ADDITIONAL_REGIME_INPUTS},
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


def _without_generated_at(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _without_generated_at(item)
            for key, item in value.items()
            if key != "generated_at"
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_without_generated_at(item) for item in value]
    return copy.deepcopy(value)


def _active_component(payload: object, *, profile: str, as_of: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise SoxlPITRegimeProducerError("component production mismatch")
    result = _without_generated_at(payload)
    if not isinstance(result, dict):
        raise SoxlPITRegimeProducerError("component production mismatch")
    observed_profile = str(result.get("profile") or result.get("plugin") or "")
    if (
        profile not in observed_profile
        or result.get("as_of") != as_of
        or not isinstance(result.get("schema_version"), str)
        or not str(result.get("canonical_route") or "").strip()
        or not str(result.get("suggested_action") or "").strip()
    ):
        raise SoxlPITRegimeProducerError("component production mismatch")
    allowed = {
        "profile",
        "plugin",
        "schema_version",
        "as_of",
        "canonical_route",
        "suggested_action",
        "would_trade_if_enabled",
        "kill_switch_active",
        "reason_codes",
        "watch_label",
        "risk_multiplier_suggestion",
        "leverage_scalar",
        "risk_asset_scalar",
    }
    sanitized = {key: copy.deepcopy(value) for key, value in result.items() if key in allowed}
    _reject_sensitive(sanitized)
    canonical_json_bytes(sanitized)
    return sanitized


def _price_rebound_receipt(payload: object, *, as_of: str) -> dict[str, Any]:
    result = _without_generated_at(payload)
    required = {
        "schema_version",
        "enabled",
        "confirmed",
        "as_of",
        "benchmark_symbol",
        "vix_symbol",
        "reason_codes",
        "trend_ok",
        "slope_ok",
        "constructive",
        "hard_filter",
        "soft_filter",
        "volatility_triggered",
        "rebound_1d",
        "rebound_nd",
        "metrics",
    }
    if (
        not isinstance(result, Mapping)
        or set(result) != required
        or result.get("schema_version") != "volatility_delever_price_rebound_context.v1"
        or result.get("enabled") is not True
        or result.get("as_of") != as_of
        or not isinstance(result.get("metrics"), Mapping)
    ):
        raise SoxlPITRegimeProducerError("price rebound production mismatch")
    sanitized = {key: copy.deepcopy(value) for key, value in result.items() if key != "metrics"}
    _reject_sensitive(sanitized)
    canonical_json_bytes(sanitized)
    return sanitized


def _price_history(sessions: Sequence[Mapping[str, Any]], end_index: int) -> pd.DataFrame:
    rows = []
    dates = []
    for session in sessions[: end_index + 1]:
        rows.append(
            {
                "SOXL": session["bars"]["SOXL"]["close"],
                "SOXX": session["bars"]["SOXX"]["close"],
                **session["regime_inputs"],
            }
        )
        dates.append(session["date"])
    return pd.DataFrame(rows, index=pd.to_datetime(dates), columns=list(REGIME_LOGICAL_INPUTS))


def _prefix_digest(
    sessions: Sequence[Mapping[str, Any]], *, end_index: int, source_contract_sha256: str
) -> str:
    digest = source_contract_sha256
    for session in sessions[: end_index + 1]:
        prefix_row = {
            "date": session["date"],
            "values": {
                "SOXL": _finite(session["bars"]["SOXL"]["close"]),
                "SOXX": _finite(session["bars"]["SOXX"]["close"]),
                **{
                    symbol: _finite(session["regime_inputs"][symbol])
                    for symbol in ADDITIONAL_REGIME_INPUTS
                },
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
    if _sha256(canonical_json_bytes(FROZEN_REGIME_CONFIG)) != MARKET_REGIME_CONFIG_SHA256:
        raise SoxlPITRegimeProducerError("frozen regime config identity mismatch")
    if isinstance(session_index, bool) or not 0 <= session_index < len(sessions):
        raise SoxlPITRegimeProducerError("invalid prefix end")
    if verify_prefix_identity and _prefix_digest(
        sessions, end_index=session_index, source_contract_sha256=source.source_contract_sha256
    ) != source.prefix_input_sha256[session_index]:
        raise SoxlPITRegimeProducerError("prefix input identity mismatch")
    session_date = str(sessions[session_index]["date"])
    prices = _price_history(sessions, session_index)
    if prices.empty or prices.index[-1].date().isoformat() != session_date:
        raise SoxlPITRegimeProducerError("invalid prefix end")
    events = resolve_trade_war_event_set("full")
    crisis_config = cast(Mapping[str, Any], FROZEN_REGIME_CONFIG["crisis"])
    macro_config = cast(Mapping[str, Any], FROZEN_REGIME_CONFIG["macro"])
    rebound_config = {
        "strategy": "soxl_soxx_trend_income",
        "as_of": session_date,
        "volatility_delever_price_rebound_enabled": True,
        "benchmark_symbol": "SOXX",
        "vix_symbols": ("VIX",),
        "credit_pairs": ("HYG:IEF", "LQD:IEF"),
        "financial_symbols": ("XLF", "KRE"),
    }
    try:
        crisis = build_crisis_response_shadow_signal(
            prices,
            events=events,
            external_context=None,
            as_of=session_date,
            start_date=str(sessions[0]["date"]),
            benchmark_symbol=str(crisis_config["benchmark_symbol"]),
            attack_symbol=str(crisis_config["attack_symbol"]),
            market_symbol=str(crisis_config["market_symbol"]),
            financial_symbols=tuple(crisis_config["financial_symbols"]),
            credit_pairs=tuple(tuple(pair) for pair in crisis_config["credit_pairs"]),
            rate_symbols=tuple(crisis_config["rate_symbols"]),
            synthetic_attack_multiple=0.0,
            ai_audit_enabled=False,
        )
        macro = build_macro_risk_governor_signal(
            prices,
            external_context=None,
            as_of=session_date,
            benchmark_symbol=str(macro_config["benchmark_symbol"]),
            attack_symbol=str(macro_config["attack_symbol"]),
            vix_symbols=tuple(macro_config["vix_symbols"]),
            vix3m_symbols=(),
            credit_pairs=tuple(tuple(pair) for pair in macro_config["credit_pairs"]),
            external_stress_actionable=False,
            delever_risk_asset_scalar=0.0,
        )
        price_rebound = build_volatility_delever_price_rebound_context(prices, rebound_config)
    except Exception as exc:
        raise SoxlPITRegimeProducerError("component production failed") from exc
    components = {
        "crisis": _active_component(crisis, profile="crisis_response", as_of=session_date),
        "macro": _active_component(macro, profile="macro_risk_governor", as_of=session_date),
    }
    price_rebound = _price_rebound_receipt(price_rebound, as_of=session_date)
    receipt = {
        "schema_version": PRODUCER_RECEIPT_SCHEMA,
        "as_of": session_date,
        "evidence_class": "synthetic_fixture" if source.synthetic_fixture else "provider_observed",
        "real_producer": not source.synthetic_fixture,
        "active_components": components,
        "disabled_components": {
            component: {"enabled": False, "available": False} for component in DISABLED_COMPONENTS
        },
        "price_rebound_context": price_rebound,
        "provenance": {
            "source_contract_sha256": source.source_contract_sha256,
            "prefix_input_manifest_sha256": source.prefix_input_sha256[session_index],
            "prefix_session_count": session_index + 1,
            "prefix_end": session_date,
            "future_sessions_exposed": False,
            "config_sha256": MARKET_REGIME_CONFIG_SHA256,
            "event_catalog_sha256": EVENT_CATALOG_SHA256,
            "qsp_revision": QSP_REVISION,
            "qsp_tree_sha": QSP_TREE_SHA,
            "qsp_sources": copy.deepcopy(QSP_SOURCE_IDENTITIES),
            "component_output_sha256": {
                name: _sha256(canonical_json_bytes(payload)) for name, payload in components.items()
            },
            "price_rebound_output_sha256": _sha256(canonical_json_bytes(price_rebound)),
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
    """Invoke pinned QSP builders on one exact prefix and return digest-only provenance."""
    if not isinstance(source, ValidatedSoxlPITRegimeSource):
        raise SoxlPITRegimeProducerError("validated source contract is required")
    _verify_qsp_identity(source.contract["qsp"])
    return _produce_soxl_pit_regime_component_receipt(
        sessions,
        source,
        session_index=session_index,
    )


def produce_soxl_pit_regime_component_receipts(
    sessions: Sequence[Mapping[str, Any]], source: ValidatedSoxlPITRegimeSource
) -> tuple[dict[str, Any], ...]:
    """Produce every frozen session after one exact runtime identity verification."""
    if not isinstance(source, ValidatedSoxlPITRegimeSource):
        raise SoxlPITRegimeProducerError("validated source contract is required")
    _verify_qsp_identity(source.contract["qsp"])
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
    "ACTIVE_COMPONENTS",
    "ADDITIONAL_REGIME_INPUTS",
    "ALL_LOGICAL_INPUTS",
    "DISABLED_COMPONENTS",
    "EVENT_CATALOG_SHA256",
    "FIXED_CUTOFF",
    "FROZEN_REGIME_CONFIG",
    "MARKET_REGIME_CONFIG_SHA256",
    "PRODUCER_RECEIPT_SCHEMA",
    "QSP_REVISION",
    "QSP_SOURCE_IDENTITIES",
    "QSP_TREE_SHA",
    "REGIME_LOGICAL_INPUTS",
    "SOURCE_CONTRACT_SCHEMA",
    "SoxlPITRegimeProducerError",
    "ValidatedSoxlPITRegimeSource",
    "produce_soxl_pit_regime_component_receipt",
    "produce_soxl_pit_regime_component_receipts",
    "runtime_producer_source_identity",
    "validate_soxl_pit_regime_source_contract",
]
