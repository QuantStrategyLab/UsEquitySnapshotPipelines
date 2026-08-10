"""Deterministic offline packager for the frozen SOXL promotion-research input contract."""

from __future__ import annotations

import copy
import hashlib
import inspect
import json
import math
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity

from .soxl_pit_regime_component_producer import (
    CANDIDATE_ID,
    CORE_ONLY_CONFIG_SHA256,
    MARKET_REGIME_SCHEMA,
    PRODUCER_RECEIPT_SCHEMA,
    SOURCE_CONTRACT_SCHEMA,
    UNAVAILABLE_COMPONENTS,
    SoxlPITRegimeProducerError,
    ValidatedSoxlPITRegimeSource,
    produce_soxl_pit_regime_component_receipts,
    validate_soxl_pit_regime_source_contract,
)


SOXL_PROMOTION_ASSETS = (
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
FIRST_ELIGIBLE_SESSION = {
    "SGOV": "2020-05-28",
    "SPYI": "2022-08-29",
    "BOXX": "2022-12-28",
    "QQQI": "2024-01-29",
}
QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
INPUT_CONTRACT_ID = "soxl_p3_core_only_9_input.v1"
MANDATE_ID = "soxl_p3_core_only_9_input_research_v1"
_FROZEN_CALENDAR_SHA256 = "6e3bf4713cca22264987c583cf4c5c94923850de4a3d18e76f66f42e719f2290"
_QPK_RESEARCH_INPUT_SOURCE_SHA256 = "1b6c5413242dc8c3d9879f65d2bc0d9b4bbd8f886b7a901406259ce9abe9a544"
_QPK_RISK_CONTRACTS_SOURCE_SHA256 = "fca206ab1f25bbadfec6d56db08d378572e45aed1e622388f273187c6fc5ae6b"
_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "password",
        "secret",
        "token",
        "cookie",
        "jwt",
        "api_key",
        "access_token",
        "refresh_token",
        "request_headers",
        "response_headers",
        "headers",
        "raw_response",
        "response_body",
        "provider_response",
    }
)
_SENSITIVE_KEY_MARKERS = tuple("".join(character for character in key if character.isalnum()) for key in _SENSITIVE_KEYS)
_BINDING_KEYS = frozenset(
    {
        "strategy_profile",
        "account_mode",
        "strategy_revision",
        "runner_revision",
        "qpk_revision",
        "config_sha256",
        "input_manifest_sha256",
        "authority_receipt_sha256",
        "candidate_identity_sha256",
        "mandate_id",
        "mandate_digest_sha256",
        "candidate_id",
        "input_contract_id",
        "source_contract_schema",
        "source_contract_sha256",
        "candidate_contract_sha256",
    }
)


class SoxlPITPackagerError(ValueError):
    """Fail-closed error without provider, credential, or private-input detail."""


def canonical_json_bytes(value: Any) -> bytes:
    """Return deterministic canonical JSON bytes."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlPITPackagerError("invalid canonical input") from exc


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _verify_dependency_sources() -> None:
    expected = (
        (validate_research_input_manifest, _QPK_RESEARCH_INPUT_SOURCE_SHA256),
        (CandidateRiskIdentity, _QPK_RISK_CONTRACTS_SOURCE_SHA256),
    )
    for dependency_object, expected_sha256 in expected:
        source_path = inspect.getsourcefile(dependency_object)
        try:
            payload = Path(source_path).read_bytes() if source_path else b""
        except OSError as exc:
            raise SoxlPITPackagerError("dependency source identity is unavailable") from exc
        if _sha256_bytes(payload) != expected_sha256:
            raise SoxlPITPackagerError("dependency source identity mismatch")


def _observed_fixed_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    current = date(year, month, 1)
    return current + timedelta(days=(weekday - current.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    length = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * length) // 451
    month = (h + length - 7 * m + 114) // 31
    day = (h + length - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _xnys_holidays(year: int) -> set[date]:
    new_year = date(year, 1, 1)
    if new_year.weekday() == 6:
        new_year += timedelta(days=1)
    holidays = {
        new_year,
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(date(year, 6, 19)))
    holidays.update({date(2018, 12, 5), date(2025, 1, 9)})
    return holidays


def _frozen_xnys_sessions() -> tuple[str, ...]:
    start = date(2018, 8, 3)
    end = date(2026, 8, 4)
    holidays = set().union(*(_xnys_holidays(year) for year in range(start.year, end.year + 1)))
    sessions: list[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            sessions.append(current.isoformat())
        current += timedelta(days=1)
    result = tuple(sessions)
    if _sha256_bytes(canonical_json_bytes(list(result))) != _FROZEN_CALENDAR_SHA256:
        raise RuntimeError("frozen XNYS calendar contract is inconsistent")
    return result


FROZEN_XNYS_SESSIONS = _frozen_xnys_sessions()
@dataclass(frozen=True)
class PreparedSoxlPITInput:
    """Immutable canonical bytes prepared before identity-bound atomic publication."""

    sessions_bytes: bytes
    input_manifest_bytes: bytes
    input_bytes: bytes
    contract_bytes: bytes
    source_contract_bytes: bytes
    input_manifest_sha256: str
    source_contract_sha256: str
    producer_commit_sha: str


def _exact_mapping(value: object, expected: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise SoxlPITPackagerError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _nonblank(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SoxlPITPackagerError(f"invalid {label}")
    return value


def _digest(value: object, label: str) -> str:
    value = _nonblank(value, label)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise SoxlPITPackagerError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    value = _nonblank(value, label)
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise SoxlPITPackagerError(f"invalid {label}")
    return value


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SoxlPITPackagerError("invalid bar")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0.0) or (nonnegative and number < 0.0):
        raise SoxlPITPackagerError("invalid bar")
    return number


def _reject_sensitive(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = (
                "".join(character for character in key.strip().lower() if character.isalnum())
                if isinstance(key, str)
                else ""
            )
            if not normalized_key or any(marker in normalized_key for marker in _SENSITIVE_KEY_MARKERS):
                raise SoxlPITPackagerError("sensitive input is forbidden")
            _reject_sensitive(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _reject_sensitive(item)


def _eligible_assets(session_date: str) -> tuple[str, ...]:
    return tuple(
        symbol
        for symbol in SOXL_PROMOTION_ASSETS
        if symbol not in FIRST_ELIGIBLE_SESSION
        or session_date >= FIRST_ELIGIBLE_SESSION[symbol]
    )


def _validate_raw_session(raw_session: object, expected_date: str) -> dict[str, Any]:
    session = _exact_mapping(
        raw_session,
        frozenset({"date", "bars"}),
        "raw session",
    )
    if session["date"] != expected_date:
        raise SoxlPITPackagerError("exact XNYS sessions are required")
    eligible = _eligible_assets(expected_date)
    bars = session["bars"]
    if not isinstance(bars, Mapping) or set(bars) != set(eligible):
        raise SoxlPITPackagerError("exact eligible bar set is required")
    canonical_bars: dict[str, dict[str, float]] = {}
    for symbol in eligible:
        bar = _exact_mapping(
            bars[symbol],
            frozenset({"open", "high", "low", "close", "volume"}),
            "bar",
        )
        open_price = _finite(bar["open"], positive=True)
        high = _finite(bar["high"], positive=True)
        low = _finite(bar["low"], positive=True)
        close = _finite(bar["close"], positive=True)
        volume = _finite(bar["volume"], nonnegative=True)
        if low > min(open_price, close) or high < max(open_price, close) or high < low:
            raise SoxlPITPackagerError("invalid bar")
        canonical_bars[symbol] = {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    return {"date": expected_date, "bars": canonical_bars}


def _sanitized_regime(
    producer_receipt: Mapping[str, Any], *, session_date: str, prefix_session_count: int
) -> dict[str, Any]:
    receipt = _exact_mapping(
        producer_receipt,
        frozenset(
            {
                "schema_version",
                "candidate_id",
                "as_of",
                "evidence_class",
                "real_producer",
                "market_regime_control_enabled",
                "unavailable_components",
                "position_control_allowed",
                "provenance",
                "receipt_sha256",
            }
        ),
        "producer receipt",
    )
    receipt_sha256 = receipt.pop("receipt_sha256")
    unavailable = {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    if (
        receipt["schema_version"] != PRODUCER_RECEIPT_SCHEMA
        or receipt["candidate_id"] != CANDIDATE_ID
        or receipt["as_of"] != session_date
        or receipt["market_regime_control_enabled"] is not False
        or receipt["unavailable_components"] != unavailable
        or receipt["position_control_allowed"] is not False
        or receipt_sha256 != _sha256_bytes(canonical_json_bytes(receipt))
    ):
        raise SoxlPITPackagerError("producer receipt identity mismatch")
    provenance = _exact_mapping(
        receipt["provenance"],
        frozenset(
            {
                "source_contract_sha256",
                "candidate_contract_sha256",
                "prefix_input_manifest_sha256",
                "prefix_session_count",
                "prefix_end",
                "future_sessions_exposed",
                "logical_input_ids",
                "raw_series_persisted",
            }
        ),
        "producer provenance",
    )
    if (
        provenance["candidate_contract_sha256"] != CORE_ONLY_CONFIG_SHA256
        or provenance["prefix_session_count"] != prefix_session_count
        or provenance["prefix_end"] != session_date
        or provenance["future_sessions_exposed"] is not False
        or provenance["logical_input_ids"] != list(SOXL_PROMOTION_ASSETS)
        or provenance["raw_series_persisted"] is not False
    ):
        raise SoxlPITPackagerError("producer provenance identity mismatch")
    result = {
        "schema_version": MARKET_REGIME_SCHEMA,
        "profile": "market_regime_control",
        "candidate_id": CANDIDATE_ID,
        "as_of": session_date,
        "market_regime_control_enabled": False,
        "component_signals": unavailable,
        "execution_controls": {
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "repository_broker_write_allowed": False,
            "repository_allocation_mutation_allowed": False,
            "position_control_allowed": False,
            "consumption_evidence_status": "static_research_only",
        },
        "pit_provenance": {
            "source_contract_sha256": provenance["source_contract_sha256"],
            "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
            "producer_receipt_sha256": receipt_sha256,
            "prefix_input_manifest_sha256": provenance[
                "prefix_input_manifest_sha256"
            ],
            "logical_input_ids": list(SOXL_PROMOTION_ASSETS),
            "evidence_class": receipt["evidence_class"],
            "real_producer": receipt["real_producer"],
            "prefix_session_count": prefix_session_count,
            "prefix_end": session_date,
            "future_sessions_exposed": False,
            "raw_series_persisted": False,
        },
    }
    _reject_sensitive(result)
    canonical_json_bytes(result)
    return result


def _package_contract(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "soxl_core_only_9_input_packager_contract.v1",
        "candidate_id": CANDIDATE_ID,
        "input_contract_id": INPUT_CONTRACT_ID,
        "assets": list(SOXL_PROMOTION_ASSETS),
        "availability": {
            "always_eligible": ["SOXL", "SOXX", "SCHD", "DGRO", "QQQ"],
            "first_eligible_session": dict(FIRST_ELIGIBLE_SESSION),
            "preinception_policy": {
                "QQQI": "independent_QQQ_fallback_no_splice_or_rename",
                "SGOV": "cash_without_renormalization",
                "SPYI": "cash_without_renormalization",
                "BOXX": "cash_without_renormalization",
            },
        },
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "start": FROZEN_XNYS_SESSIONS[0],
            "end": FROZEN_XNYS_SESSIONS[-1],
            "session_count": len(FROZEN_XNYS_SESSIONS),
            "sessions_sha256": _FROZEN_CALENDAR_SHA256,
            "source": source["calendar"]["source"],
            "source_revision": source["calendar"]["source_revision"],
            "fixed_cutoff": source["fixed_cutoff"],
        },
        "windows": {
            "folds": [
                {
                    "train_sessions": 420,
                    "purge_sessions": 20,
                    "test_sessions": 126,
                    "embargo_sessions": 20,
                }
                for _ in range(3)
            ],
            "boundaries": [
                {
                    "train_start": "2018-08-03",
                    "train_end": "2020-04-03",
                    "test_start": "2020-05-05",
                    "test_end": "2020-10-30",
                },
                {
                    "train_start": "2020-12-01",
                    "train_end": "2022-08-02",
                    "test_start": "2022-08-31",
                    "test_end": "2023-03-02",
                },
                {
                    "train_start": "2023-03-31",
                    "train_end": "2024-11-29",
                    "test_start": "2024-12-31",
                    "test_end": "2025-07-03",
                },
            ],
            "final_oos": {
                "start": "2025-08-04",
                "end": "2026-08-04",
                "sessions": 252,
                "minimum_calendar_months": 12,
                "actual_nine_assets_only": True,
            },
        },
        "execution": {
            "signal_timing": "close_t",
            "execution_timing": "open_t_plus_1",
            "continuous_state": [
                "cash",
                "holdings",
                "lots",
                "hysteresis",
                "income",
                "executable_5pct_stop",
                "drawdown",
                "strategy_breaker",
                "account_breaker",
            ],
        },
        "cost_model_bps": [5, 10, 25],
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "per_logical_input_source_contract",
            "source_revision": source["schema_version"],
        },
        "market_regime": {
            "control_enabled": False,
            "unavailable_components": {
                component: {"enabled": False, "available": False}
                for component in UNAVAILABLE_COMPONENTS
            },
            "logical_input_count": 9,
            "component_as_of_policy": "exact_session_close",
            "missing_input_policy": "fail_closed",
            "prefix_only": True,
            "source_contract_schema": SOURCE_CONTRACT_SCHEMA,
            "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
            "substitution_policy": "none_no_proxy_no_alias",
            "position_control_allowed": False,
        },
        "dependency_sources": {
            "qpk_research_input_sha256": _QPK_RESEARCH_INPUT_SOURCE_SHA256,
            "qpk_risk_contracts_sha256": _QPK_RISK_CONTRACTS_SOURCE_SHA256,
        },
        "package": {
            "deterministic": True,
            "atomic_directory_publish": True,
            "recursive_sensitive_field_policy": "reject",
            "forbidden_content": [
                "raw-provider-payload",
                "request-metadata",
                "secret-material",
            ],
        },
    }


def _produce_regimes(
    validated: Sequence[Mapping[str, Any]],
    regime_source: ValidatedSoxlPITRegimeSource,
) -> tuple[dict[str, Any], ...]:
    try:
        return produce_soxl_pit_regime_component_receipts(
            validated,
            regime_source,
        )
    except SoxlPITRegimeProducerError as exc:
        raise SoxlPITPackagerError(str(exc)) from exc


def prepare_soxl_pit_input(
    raw_sessions: Sequence[Mapping[str, Any]],
    source_contract: Mapping[str, Any],
    *,
    trusted_regime_source_contract_sha256: str | None = None,
) -> PreparedSoxlPITInput:
    """Validate synthetic/offline inputs and prepare deterministic canonical package bytes."""
    if not isinstance(raw_sessions, Sequence) or isinstance(raw_sessions, (str, bytes, bytearray)):
        raise SoxlPITPackagerError("exact XNYS sessions are required")
    _verify_dependency_sources()
    if len(raw_sessions) != len(FROZEN_XNYS_SESSIONS):
        raise SoxlPITPackagerError("exact XNYS sessions are required")
    _reject_sensitive(raw_sessions)
    validated = [
        _validate_raw_session(raw_session, expected_date)
        for raw_session, expected_date in zip(raw_sessions, FROZEN_XNYS_SESSIONS, strict=True)
    ]
    try:
        regime_source = validate_soxl_pit_regime_source_contract(
            validated,
            source_contract,
            expected_sessions=FROZEN_XNYS_SESSIONS,
            trusted_source_contract_sha256=trusted_regime_source_contract_sha256,
        )
    except SoxlPITRegimeProducerError as exc:
        raise SoxlPITPackagerError(str(exc)) from exc
    source = regime_source.contract
    regime_receipts = _produce_regimes(validated, regime_source)
    sessions = [
        {
            "date": session["date"],
            "bars": session["bars"],
            "eligible_assets": list(_eligible_assets(session["date"])),
            "market_regime": _sanitized_regime(
                regime_receipt,
                session_date=session["date"],
                prefix_session_count=index + 1,
            ),
        }
        for index, (session, regime_receipt) in enumerate(
            zip(validated, regime_receipts, strict=True)
        )
    ]
    sessions_bytes = canonical_json_bytes(sessions)
    manifest = {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": f"soxl-pit-20180803-20260804-{_sha256_bytes(sessions_bytes)[:12]}",
        "research_input_contract_id": INPUT_CONTRACT_ID,
        "domain": "us_equity",
        "profile": "soxl_soxx_trend_income",
        "artifact_type": "immutable_adjusted_ohlcv_core_only",
        "observed_at": source["observed_at"],
        "effective_at": source["effective_at"],
        "as_of": source["as_of"],
        "producer": {
            key: source["producer"][key]
            for key in ("repository", "commit_sha", "tree_sha", "tool", "tool_version")
        },
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_date": FROZEN_XNYS_SESSIONS[-1],
            "source": source["calendar"]["source"],
            "source_revision": source["calendar"]["source_revision"],
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "per_logical_input_source_contract",
            "source_revision": source["schema_version"],
        },
        "sources": [
            {
                "source_id": item["logical_input_id"],
                "revision": item["source_revision"],
                "observed_at": item["observed_at"],
                "content_sha256": item["content_sha256"],
            }
            for item in sorted(source["logical_inputs"], key=lambda item: item["logical_input_id"])
        ],
        "members": [
            {
                "path": "sessions.json",
                "media_type": "application/json",
                "size_bytes": len(sessions_bytes),
                "sha256": _sha256_bytes(sessions_bytes),
            }
        ],
    }
    try:
        manifest = validate_research_input_manifest(manifest)
        manifest_bytes = canonical_research_input_manifest_bytes(manifest)
        manifest_sha256 = research_input_manifest_sha256(manifest)
    except ValueError as exc:
        raise SoxlPITPackagerError("input manifest validation failed") from exc
    input_bytes = canonical_json_bytes(
        {
            "schema_version": INPUT_CONTRACT_ID,
            "input_manifest": manifest,
            "sessions": sessions,
        }
    )
    source_summary = source
    contract = _package_contract(source)
    _reject_sensitive(source_summary)
    _reject_sensitive(contract)
    return PreparedSoxlPITInput(
        sessions_bytes=sessions_bytes,
        input_manifest_bytes=manifest_bytes,
        input_bytes=input_bytes,
        contract_bytes=canonical_json_bytes(contract),
        source_contract_bytes=canonical_json_bytes(source_summary),
        input_manifest_sha256=manifest_sha256,
        source_contract_sha256=regime_source.source_contract_sha256,
        producer_commit_sha=source["producer"]["commit_sha"],
    )


def _validate_binding(prepared: PreparedSoxlPITInput, identity_binding: Mapping[str, Any]) -> dict[str, Any]:
    binding = _exact_mapping(identity_binding, _BINDING_KEYS, "identity binding")
    _reject_sensitive(binding)
    if binding["strategy_profile"] != "soxl_soxx_trend_income":
        raise SoxlPITPackagerError("invalid strategy identity")
    if binding["account_mode"] != "single_strategy":
        raise SoxlPITPackagerError("invalid account identity")
    _revision(binding["strategy_revision"], "strategy revision")
    runner_revision = _revision(binding["runner_revision"], "runner revision")
    if runner_revision != prepared.producer_commit_sha:
        raise SoxlPITPackagerError("producer and runner revision mismatch")
    if binding["qpk_revision"] != QPK_REVISION:
        raise SoxlPITPackagerError("QPK revision mismatch")
    if binding["candidate_id"] != CANDIDATE_ID:
        raise SoxlPITPackagerError("candidate identity mismatch")
    if binding["input_contract_id"] != INPUT_CONTRACT_ID:
        raise SoxlPITPackagerError("input contract identity mismatch")
    if binding["source_contract_schema"] != SOURCE_CONTRACT_SCHEMA:
        raise SoxlPITPackagerError("source contract identity mismatch")
    if binding["source_contract_sha256"] != prepared.source_contract_sha256:
        raise SoxlPITPackagerError("source contract identity mismatch")
    if binding["candidate_contract_sha256"] != CORE_ONLY_CONFIG_SHA256:
        raise SoxlPITPackagerError("candidate contract identity mismatch")
    if binding["input_manifest_sha256"] != prepared.input_manifest_sha256:
        raise SoxlPITPackagerError("input manifest identity mismatch")
    for field in (
        "config_sha256",
        "input_manifest_sha256",
        "authority_receipt_sha256",
        "candidate_identity_sha256",
        "mandate_digest_sha256",
        "source_contract_sha256",
        "candidate_contract_sha256",
    ):
        _digest(binding[field], field)
    if binding["mandate_id"] != MANDATE_ID:
        raise SoxlPITPackagerError("mandate identity mismatch")
    try:
        candidate = CandidateRiskIdentity(
            strategy_profile=binding["strategy_profile"],
            account_mode=binding["account_mode"],
            strategy_revision=binding["strategy_revision"],
            runner_revision=binding["runner_revision"],
            config_sha256=binding["config_sha256"],
            input_manifest_sha256=binding["input_manifest_sha256"],
            authority_receipt_sha256=binding["authority_receipt_sha256"],
        )
    except (TypeError, ValueError) as exc:
        raise SoxlPITPackagerError("candidate identity mismatch") from exc
    if candidate.candidate_sha256 != binding["candidate_identity_sha256"]:
        raise SoxlPITPackagerError("candidate identity mismatch")
    return binding


def _write_private_file(root: Path, relative_path: str, payload: bytes) -> None:
    destination = root / relative_path
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)


def _reject_symlink_ancestors(directory: Path) -> None:
    current = directory
    while True:
        if current.is_symlink():
            raise SoxlPITPackagerError("output parent symlink is forbidden")
        if current == current.parent:
            return
        current = current.parent


def publish_soxl_pit_input(
    prepared: PreparedSoxlPITInput,
    identity_binding: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, str]:
    """Atomically publish the deterministic, identity-bound private package."""
    if not isinstance(prepared, PreparedSoxlPITInput):
        raise SoxlPITPackagerError("invalid prepared package")
    _verify_dependency_sources()
    binding = _validate_binding(prepared, identity_binding)
    output_root = Path(output_dir)
    if output_root.exists() or output_root.is_symlink():
        raise SoxlPITPackagerError("output directory must not exist")
    parent = output_root.parent
    try:
        parent.lstat()
    except OSError as exc:
        raise SoxlPITPackagerError("output parent is unavailable") from exc
    _reject_symlink_ancestors(parent)
    if not parent.is_dir() or not output_root.name or output_root.name in {".", ".."}:
        raise SoxlPITPackagerError("invalid output directory")
    files = {
        "input-manifest.json": prepared.input_manifest_bytes,
        "input.json": prepared.input_bytes,
        "sessions.json": prepared.sessions_bytes,
    }
    members = [
        {
            "path": filename,
            "media_type": "application/json",
            "size_bytes": len(payload),
            "sha256": _sha256_bytes(payload),
        }
        for filename, payload in sorted(files.items())
    ]
    source_contract = json.loads(prepared.source_contract_bytes)
    manifest = {
        "schema_version": "soxl_core_only_9_input_package_manifest.v1",
        "package_type": "promotion_research_input_static_only",
        "candidate_id": CANDIDATE_ID,
        "input_contract_id": INPUT_CONTRACT_ID,
        "input_manifest_sha256": prepared.input_manifest_sha256,
        "source_contract": source_contract,
        "identity": binding,
        "contract": json.loads(prepared.contract_bytes),
        "members": members,
        "lifecycle_claims": {
            "promotion_eligible": False,
            "live_ready": False,
            "paper_authority": False,
            "shadow_authority": False,
            "live_authority": False,
            "order_authority": False,
            "position_control_allowed": False,
            "size_zero_required": True,
            "no_order": True,
            "real_producer": source_contract["data_class"] == "provider_observed",
            "synthetic_fixture": source_contract["data_class"] == "synthetic_fixture",
            "real_backtest_executed": False,
        },
    }
    _reject_sensitive(manifest)
    manifest_bytes = canonical_json_bytes(manifest)
    files["package-manifest.json"] = manifest_bytes
    temporary_root: Path | None = None
    try:
        temporary_root = Path(
            tempfile.mkdtemp(prefix=f".{output_root.name}.tmp-", dir=parent)
        )
        os.chmod(temporary_root, 0o700)
        for filename, payload in files.items():
            _write_private_file(temporary_root, filename, payload)
        directory_descriptor = os.open(temporary_root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        os.replace(temporary_root, output_root)
        temporary_root = None
        parent_descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    except OSError as exc:
        if temporary_root is not None:
            shutil.rmtree(temporary_root, ignore_errors=True)
        raise SoxlPITPackagerError("atomic package publication failed") from exc
    return {
        "input_manifest_sha256": prepared.input_manifest_sha256,
        "sessions_sha256": _sha256_bytes(prepared.sessions_bytes),
        "input_sha256": _sha256_bytes(prepared.input_bytes),
        "package_manifest_sha256": _sha256_bytes(manifest_bytes),
    }


__all__ = [
    "CANDIDATE_ID",
    "CORE_ONLY_CONFIG_SHA256",
    "FIRST_ELIGIBLE_SESSION",
    "FROZEN_XNYS_SESSIONS",
    "INPUT_CONTRACT_ID",
    "QPK_REVISION",
    "SOURCE_CONTRACT_SCHEMA",
    "SOXL_PROMOTION_ASSETS",
    "PreparedSoxlPITInput",
    "SoxlPITPackagerError",
    "canonical_json_bytes",
    "prepare_soxl_pit_input",
    "publish_soxl_pit_input",
]
