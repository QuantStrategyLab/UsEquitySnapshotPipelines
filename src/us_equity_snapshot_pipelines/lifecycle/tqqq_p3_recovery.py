"""Bounded recovery metadata for one parked TQQQ P3 replay.

The daily P1/P3 controller deliberately does not retry provider acquisition or
change an accepted input.  This module covers the narrower operational case:
an already verified, create-only P1 root reached P3, but the offline replay
ended in a sanitized runtime failure.  It permits one later replay of that
same root while it is still available.  It never contacts a provider, reads a
credential, schedules work, writes storage, or grants P4--P6 authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from .tqqq_core_only_p1_binding import P2_V5_CONTRACT
from .tqqq_p3_evidence_index import P3_STATUS, validate_tqqq_p3_result

DAILY_STATUS_SCHEMA = "qsl.tqqq-daily-research-status.v1"
RECOVERY_PLAN_SCHEMA = "qsl.tqqq-p3-recovery-plan.v1"
RECOVERY_RECORD_SCHEMA = "qsl.tqqq-p3-recovery-record.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_CANDIDATE_FIELDS = frozenset({"candidate_id", "config_sha256"})
_DAILY_STATUS_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "date_cutoff",
        "input_manifest_sha256",
        "p1_health_sha256",
        "p3_terminal",
    }
)
_SUCCESS_TERMINAL_FIELDS = frozenset({"evidence_sha256", "status", "verdict"})
_PARKED_TERMINAL_FIELDS = frozenset(
    {"complete_evidence", "failure_class", "replay_started", "source_commit", "stage", "status"}
)
_FAILURE_STAGES = {
    "input_validation_failure": "input_validation",
    "config_contract_failure": "config_contract",
    "orchestrator_contract_failure": "orchestrator_contract",
    "risk_contract_failure": "risk_contract",
    "evidence_validation_failure": "evidence_validation",
    "runtime_internal_failure": "runtime_internal",
}
_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "reason_code",
        "candidate",
        "date_cutoff",
        "input_manifest_sha256",
        "daily_status_sha256",
        "recovery_attempt_limit",
    }
)
_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "produced_at",
        "candidate",
        "date_cutoff",
        "input_manifest_sha256",
        "daily_status_sha256",
        "recovery_attempt",
        "p3_terminal",
        "recovery_record_sha256",
    }
)


class TqqqP3RecoveryError(ValueError):
    """Raised when recovery metadata is malformed or exceeds its boundary."""


def _fail(message: str) -> None:
    raise TqqqP3RecoveryError(message)


def _mapping(value: Any, fields: frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(f"invalid {label}")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(f"invalid {label}")
    return value


def _canonical_json(value: Mapping[str, Any], *, omitted: str | None = None) -> bytes:
    material = dict(value)
    if omitted is not None:
        material.pop(omitted, None)
    try:
        return json.dumps(
            material, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TqqqP3RecoveryError("invalid recovery metadata") from exc


def _candidate(value: Any) -> dict[str, str]:
    candidate = _mapping(value, _CANDIDATE_FIELDS, "candidate")
    expected = {
        "candidate_id": P2_V5_CONTRACT.candidate_id,
        "config_sha256": P2_V5_CONTRACT.config_sha256,
    }
    if dict(candidate) != expected:
        _fail("unexpected candidate")
    return expected


def _date(value: Any, label: str) -> str:
    if not isinstance(value, str):
        _fail(f"invalid {label}")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise TqqqP3RecoveryError(f"invalid {label}") from exc


def _timestamp(value: Any) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        _fail("invalid recovery timestamp")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise TqqqP3RecoveryError("invalid recovery timestamp") from exc
    return value


def validate_tqqq_p3_terminal(value: Any) -> dict[str, object]:
    """Validate the sanitized P3 terminal result without accepting raw detail."""
    if isinstance(value, Mapping) and set(value) == _SUCCESS_TERMINAL_FIELDS:
        try:
            return validate_tqqq_p3_result(value)
        except ValueError as exc:
            raise TqqqP3RecoveryError("invalid completed P3 terminal") from exc

    terminal = _mapping(value, _PARKED_TERMINAL_FIELDS, "parked P3 terminal")
    failure_class = terminal["failure_class"]
    if (
        terminal["status"] != "PARKED"
        or terminal["complete_evidence"] is not False
        or not isinstance(failure_class, str)
        or terminal["stage"] != _FAILURE_STAGES.get(failure_class)
        or not isinstance(terminal["replay_started"], bool)
        or not isinstance(terminal["source_commit"], str)
        or not _REVISION.fullmatch(terminal["source_commit"])
    ):
        _fail("invalid parked P3 terminal")
    return {
        "complete_evidence": False,
        "failure_class": failure_class,
        "replay_started": terminal["replay_started"],
        "source_commit": terminal["source_commit"],
        "stage": terminal["stage"],
        "status": "PARKED",
    }


def _daily_status(value: Any) -> tuple[dict[str, object], dict[str, object], str]:
    status = _mapping(value, _DAILY_STATUS_FIELDS, "daily research status")
    if status["schema_version"] != DAILY_STATUS_SCHEMA:
        _fail("invalid daily research status")
    normalized: dict[str, object] = {
        "schema_version": DAILY_STATUS_SCHEMA,
        "candidate": _candidate(status["candidate"]),
        "date_cutoff": _date(status["date_cutoff"], "date cutoff"),
        "input_manifest_sha256": _digest(status["input_manifest_sha256"], "input manifest digest"),
        "p1_health_sha256": _digest(status["p1_health_sha256"], "P1 health digest"),
        "p3_terminal": validate_tqqq_p3_terminal(status["p3_terminal"]),
    }
    daily_digest = hashlib.sha256(_canonical_json(normalized)).hexdigest()
    return normalized, normalized["p3_terminal"], daily_digest


def build_tqqq_p3_recovery_plan(
    *, daily_research_status: Any, recovery_record_exists: object
) -> dict[str, object]:
    """Plan at most one retry of a previously started runtime-only P3 replay."""
    if not isinstance(recovery_record_exists, bool):
        _fail("invalid recovery-record existence")
    status, terminal, daily_digest = _daily_status(daily_research_status)
    if terminal["status"] == P3_STATUS:
        plan_status, reason = "PARKED", "P3_ALREADY_COMPLETE"
    elif recovery_record_exists:
        plan_status, reason = "PARKED", "RECOVERY_ALREADY_RECORDED"
    elif (
        terminal["failure_class"] == "runtime_internal_failure"
        and terminal["stage"] == "runtime_internal"
        and terminal["replay_started"] is True
    ):
        plan_status, reason = "REPLAY_ONCE", "RUNTIME_REPLAY_RECOVERY"
    else:
        plan_status, reason = "PARKED", "P3_FAILURE_NOT_RETRIABLE"
    return {
        "schema_version": RECOVERY_PLAN_SCHEMA,
        "status": plan_status,
        "reason_code": reason,
        "candidate": status["candidate"],
        "date_cutoff": status["date_cutoff"],
        "input_manifest_sha256": status["input_manifest_sha256"],
        "daily_status_sha256": daily_digest,
        "recovery_attempt_limit": 1,
    }


def validate_tqqq_p3_recovery_plan(value: Any) -> dict[str, object]:
    """Validate a metadata-only recovery plan emitted by this module."""
    plan = _mapping(value, _PLAN_FIELDS, "P3 recovery plan")
    if plan["schema_version"] != RECOVERY_PLAN_SCHEMA:
        _fail("invalid P3 recovery plan")
    status = plan["status"]
    reason = plan["reason_code"]
    allowed = {
        ("REPLAY_ONCE", "RUNTIME_REPLAY_RECOVERY"),
        ("PARKED", "P3_ALREADY_COMPLETE"),
        ("PARKED", "RECOVERY_ALREADY_RECORDED"),
        ("PARKED", "P3_FAILURE_NOT_RETRIABLE"),
    }
    if (status, reason) not in allowed or plan["recovery_attempt_limit"] != 1:
        _fail("invalid P3 recovery plan")
    return {
        "schema_version": RECOVERY_PLAN_SCHEMA,
        "status": status,
        "reason_code": reason,
        "candidate": _candidate(plan["candidate"]),
        "date_cutoff": _date(plan["date_cutoff"], "date cutoff"),
        "input_manifest_sha256": _digest(plan["input_manifest_sha256"], "input manifest digest"),
        "daily_status_sha256": _digest(plan["daily_status_sha256"], "daily status digest"),
        "recovery_attempt_limit": 1,
    }


def calculate_tqqq_p3_recovery_record_sha256(value: Mapping[str, Any]) -> str:
    """Calculate the self-digest for one create-only recovery record."""
    return hashlib.sha256(_canonical_json(value, omitted="recovery_record_sha256")).hexdigest()


def build_tqqq_p3_recovery_record(
    *, plan: Any, p3_terminal: Any, produced_at: object
) -> dict[str, object]:
    """Bind one terminal replay outcome to its eligible one-shot recovery plan."""
    validated_plan = validate_tqqq_p3_recovery_plan(plan)
    if validated_plan["status"] != "REPLAY_ONCE":
        _fail("recovery plan does not permit replay")
    record: dict[str, object] = {
        "schema_version": RECOVERY_RECORD_SCHEMA,
        "produced_at": _timestamp(produced_at),
        "candidate": validated_plan["candidate"],
        "date_cutoff": validated_plan["date_cutoff"],
        "input_manifest_sha256": validated_plan["input_manifest_sha256"],
        "daily_status_sha256": validated_plan["daily_status_sha256"],
        "recovery_attempt": 1,
        "p3_terminal": validate_tqqq_p3_terminal(p3_terminal),
        "recovery_record_sha256": "",
    }
    record["recovery_record_sha256"] = calculate_tqqq_p3_recovery_record_sha256(record)
    return validate_tqqq_p3_recovery_record(record)


def validate_tqqq_p3_recovery_record(value: Any) -> dict[str, object]:
    """Validate a create-only recovery outcome without granting downstream authority."""
    record = _mapping(value, _RECORD_FIELDS, "P3 recovery record")
    if record["schema_version"] != RECOVERY_RECORD_SCHEMA or record["recovery_attempt"] != 1:
        _fail("invalid P3 recovery record")
    normalized: dict[str, object] = {
        "schema_version": RECOVERY_RECORD_SCHEMA,
        "produced_at": _timestamp(record["produced_at"]),
        "candidate": _candidate(record["candidate"]),
        "date_cutoff": _date(record["date_cutoff"], "date cutoff"),
        "input_manifest_sha256": _digest(record["input_manifest_sha256"], "input manifest digest"),
        "daily_status_sha256": _digest(record["daily_status_sha256"], "daily status digest"),
        "recovery_attempt": 1,
        "p3_terminal": validate_tqqq_p3_terminal(record["p3_terminal"]),
        "recovery_record_sha256": _digest(record["recovery_record_sha256"], "recovery record digest"),
    }
    if normalized["recovery_record_sha256"] != calculate_tqqq_p3_recovery_record_sha256(normalized):
        _fail("invalid P3 recovery record digest")
    return normalized


__all__ = [
    "DAILY_STATUS_SCHEMA",
    "RECOVERY_PLAN_SCHEMA",
    "RECOVERY_RECORD_SCHEMA",
    "TqqqP3RecoveryError",
    "build_tqqq_p3_recovery_plan",
    "build_tqqq_p3_recovery_record",
    "calculate_tqqq_p3_recovery_record_sha256",
    "validate_tqqq_p3_recovery_plan",
    "validate_tqqq_p3_recovery_record",
    "validate_tqqq_p3_terminal",
]
