"""Build a low-noise, research-only readiness signal for a future portfolio.

The current registry contains separate TQQQ and SOXL research candidates.  This
module may only summarize their already-sanitized P1/P3 terminal records.  It
does not choose portfolio weights, freeze a P2 portfolio candidate, read bars,
call a provider, or create any P4--P6 authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from .strategy_candidate_registry import (
    SINGLE_STRATEGY,
    SOXL_SOXX_CORE_ONLY_P2_V3,
    TQQQ_CORE_ONLY_P2_V5,
    StrategyCandidate,
)

SCHEMA_VERSION = "qsl.portfolio-candidate-readiness.v1"
PARKED_STATUS = "PARKED"
READY_STATUS = "AI_RESEARCH_PROPOSAL_READY"
P1_ACCEPTED = "ACCEPTED"
P3_COMPLETE = "COMPLETE"
P3_NOT_RUN = "NOT_RUN"
P3_PARKED = "PARKED"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_P1_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "reason_code",
        "provider_retry_state",
        "date_cutoff",
        "candidate",
        "input_manifest_sha256",
    }
)
_COMPONENT_FIELDS = frozenset(
    {
        "candidate_id",
        "candidate_sha256",
        "config_sha256",
        "p1_status",
        "p3_status",
        "date_cutoff",
        "p1_manifest_sha256",
        "p3_evidence_sha256",
    }
)
_ROOT_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "execution_authorized",
        "observed_at",
        "status",
        "reason_codes",
        "proposal",
        "components",
        "readiness_sha256",
    }
)


class PortfolioCandidateReadinessError(ValueError):
    """Raised when a terminal record is not a safe portfolio readiness input."""


def _fail(message: str) -> None:
    raise PortfolioCandidateReadinessError(message)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PortfolioCandidateReadinessError("invalid portfolio readiness record") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(f"invalid {label}")
    return dict(value)


def _digest(value: object, label: str, *, allow_empty: bool = False) -> str:
    if allow_empty and value == "":
        return ""
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(f"invalid {label}")
    return value


def _date(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        _fail(f"invalid {label}")
    try:
        if datetime.fromisoformat(value).date().isoformat() != value:
            _fail(f"invalid {label}")
    except ValueError as exc:
        raise PortfolioCandidateReadinessError(f"invalid {label}") from exc
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        _fail("invalid observed timestamp")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise PortfolioCandidateReadinessError("invalid observed timestamp") from exc
    return parsed.isoformat().replace("+00:00", "Z")


def _candidate_from_terminal(value: object, candidate: StrategyCandidate) -> None:
    terminal_candidate = _mapping(value, frozenset({"candidate_id", "config_sha256"}), "terminal candidate")
    if terminal_candidate != {
        "candidate_id": candidate.candidate_id,
        "config_sha256": candidate.config_sha256,
    }:
        _fail("terminal candidate does not match registered candidate")


def _p1_terminal(value: object, candidate: StrategyCandidate) -> dict[str, str]:
    terminal = dict(value) if isinstance(value, Mapping) else None
    if terminal is None:
        _fail("invalid P1 terminal")
    schema = terminal.get("schema_version")
    expected_fields = _P1_COMMON_FIELDS
    health_sha256 = ""
    if schema == "qsl.tqqq-core-only-daily-p1-status.v1":
        expected_fields = _P1_COMMON_FIELDS | {"p1_health_sha256"}
        health_sha256 = _digest(terminal.get("p1_health_sha256"), "TQQQ P1 health digest")
    elif schema != "qsl.soxl-soxx-core-only-daily-p1-status.v1":
        _fail("unsupported P1 terminal schema")
    terminal = _mapping(terminal, expected_fields, "P1 terminal")
    _candidate_from_terminal(terminal["candidate"], candidate)
    status = terminal["status"]
    if status not in {"ACCEPTED", "DEFERRED", "QUARANTINED", "PARKED"}:
        _fail("invalid P1 terminal status")
    manifest = _digest(terminal["input_manifest_sha256"], "P1 manifest digest", allow_empty=True)
    if (status == P1_ACCEPTED) != bool(manifest):
        _fail("P1 terminal manifest does not match status")
    result = {
        "status": str(status),
        "date_cutoff": _date(terminal["date_cutoff"], "P1 cutoff"),
        "manifest_sha256": manifest,
    }
    if health_sha256:
        result["health_sha256"] = health_sha256
    return result


def _tqqq_p3_terminal(value: object, candidate: StrategyCandidate, p1: Mapping[str, str]) -> dict[str, str]:
    terminal = _mapping(
        value,
        frozenset(
            {
                "schema_version",
                "candidate",
                "date_cutoff",
                "input_manifest_sha256",
                "p1_health_sha256",
                "p3_terminal",
            }
        ),
        "TQQQ P3 terminal",
    )
    if terminal["schema_version"] != "qsl.tqqq-daily-research-status.v1":
        _fail("invalid TQQQ P3 terminal schema")
    _candidate_from_terminal(terminal["candidate"], candidate)
    if _date(terminal["date_cutoff"], "TQQQ P3 cutoff") != p1["date_cutoff"]:
        _fail("TQQQ P1/P3 cutoff mismatch")
    if _digest(terminal["input_manifest_sha256"], "TQQQ P3 manifest digest") != p1["manifest_sha256"]:
        _fail("TQQQ P1/P3 manifest mismatch")
    if _digest(terminal["p1_health_sha256"], "TQQQ P3 health digest") != p1.get("health_sha256"):
        _fail("TQQQ P1/P3 health mismatch")
    nested = terminal["p3_terminal"]
    if not isinstance(nested, Mapping):
        _fail("invalid TQQQ P3 result")
    status = nested.get("status")
    if status == "EVIDENCE_V2_COMPLETE":
        return {"status": P3_COMPLETE, "evidence_sha256": _digest(nested.get("evidence_sha256"), "TQQQ P3 evidence digest")}
    if status == "PARKED":
        return {"status": P3_PARKED, "evidence_sha256": ""}
    _fail("invalid TQQQ P3 result status")


def _soxl_p3_terminal(
    value: object, candidate: StrategyCandidate, p1: Mapping[str, str]
) -> dict[str, str]:
    terminal = dict(value) if isinstance(value, Mapping) else None
    if terminal is None:
        _fail("invalid SOXL P3 terminal")
    if terminal.get("schema_version") == "qsl.soxl-soxx-core-only-p3-evidence-summary.v1":
        expected_fields = {
            "schema_version",
            "status",
            "p1_identity",
            "p2_identity",
            "materialized_input_sha256",
            "evidence_plan_sha256",
            "execution_identity",
            "runs",
            "evidence_summary_sha256",
        }
        claimed_digest = terminal.pop("evidence_summary_sha256", None)
        if set(terminal) != expected_fields - {"evidence_summary_sha256"} or terminal.get("status") != "SUCCESS":
            _fail("invalid SOXL P3 success terminal")
        if _digest(claimed_digest, "SOXL P3 evidence digest") != _sha256(terminal):
            _fail("invalid SOXL P3 evidence digest")
        p2_identity = terminal.get("p2_identity")
        if not isinstance(p2_identity, Mapping) or p2_identity != {
            "candidate_id": candidate.candidate_id,
            "config_sha256": candidate.config_sha256,
        }:
            _fail("SOXL P3 candidate does not match registered candidate")
        p1_identity = _mapping(
            terminal.get("p1_identity"),
            frozenset({"input_manifest_sha256", "binding_sha256", "bars_member_sha256", "date_cutoff"}),
            "SOXL P3 P1 identity",
        )
        if (
            _digest(p1_identity["input_manifest_sha256"], "SOXL P3 manifest digest")
            != p1["manifest_sha256"]
            or _date(p1_identity["date_cutoff"], "SOXL P3 cutoff") != p1["date_cutoff"]
        ):
            _fail("SOXL P1/P3 identity mismatch")
        _digest(p1_identity["binding_sha256"], "SOXL P3 binding digest")
        _digest(p1_identity["bars_member_sha256"], "SOXL P3 bars digest")
        return {
            "status": P3_COMPLETE,
            "evidence_sha256": _digest(claimed_digest, "SOXL P3 evidence digest"),
        }
    if set(terminal) == {"schema_version", "status", "failure_class"} and terminal.get("schema_version") == "qsl.soxl-soxx-core-only-p3-offline-run.v1" and terminal.get("status") == "PARKED":
        return {"status": P3_PARKED, "evidence_sha256": ""}
    _fail("invalid SOXL P3 terminal")


def build_component_observation(
    *, candidate: StrategyCandidate, p1_terminal: Mapping[str, object], p3_terminal: Mapping[str, object] | None
) -> dict[str, str]:
    """Normalize one existing candidate's sanitized P1/P3 terminal records."""
    if type(candidate) is not StrategyCandidate or candidate.kind != SINGLE_STRATEGY:
        _fail("component must be a registered single-strategy candidate")
    p1 = _p1_terminal(p1_terminal, candidate)
    if p3_terminal is None:
        p3 = {"status": P3_NOT_RUN, "evidence_sha256": ""}
    elif candidate == TQQQ_CORE_ONLY_P2_V5:
        p3 = _tqqq_p3_terminal(p3_terminal, candidate, p1)
    elif candidate == SOXL_SOXX_CORE_ONLY_P2_V3:
        p3 = _soxl_p3_terminal(p3_terminal, candidate, p1)
    else:
        _fail("unsupported registered candidate route")
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_sha256": candidate.candidate_sha256,
        "config_sha256": candidate.config_sha256,
        "p1_status": p1["status"],
        "p3_status": p3["status"],
        "date_cutoff": p1["date_cutoff"],
        "p1_manifest_sha256": p1["manifest_sha256"],
        "p3_evidence_sha256": p3["evidence_sha256"],
    }


def _component(value: object) -> dict[str, str]:
    component = _mapping(value, _COMPONENT_FIELDS, "component observation")
    if component["p1_status"] not in {"ACCEPTED", "DEFERRED", "QUARANTINED", "PARKED"}:
        _fail("invalid component P1 status")
    if component["p3_status"] not in {P3_COMPLETE, P3_NOT_RUN, P3_PARKED}:
        _fail("invalid component P3 status")
    manifest = _digest(component["p1_manifest_sha256"], "component P1 manifest", allow_empty=True)
    evidence = _digest(component["p3_evidence_sha256"], "component P3 evidence", allow_empty=True)
    if (component["p1_status"] == P1_ACCEPTED) != bool(manifest):
        _fail("component P1 manifest does not match status")
    if (component["p3_status"] == P3_COMPLETE) != bool(evidence):
        _fail("component P3 evidence does not match status")
    return {
        "candidate_id": str(component["candidate_id"]),
        "candidate_sha256": _digest(component["candidate_sha256"], "component candidate digest"),
        "config_sha256": _digest(component["config_sha256"], "component config digest"),
        "p1_status": str(component["p1_status"]),
        "p3_status": str(component["p3_status"]),
        "date_cutoff": _date(component["date_cutoff"], "component cutoff"),
        "p1_manifest_sha256": manifest,
        "p3_evidence_sha256": evidence,
    }


def _state_payload(value: Mapping[str, object]) -> dict[str, object]:
    return {key: item for key, item in value.items() if key not in {"observed_at", "readiness_sha256"}}


def build_portfolio_candidate_readiness(
    *, components: Sequence[Mapping[str, object]], observed_at: object
) -> dict[str, object]:
    """Return one proposal readiness record for the current two research routes.

    A ``READY`` result is only a prompt for a future AI research proposal.  It
    is intentionally not a P2 candidate, a P1 root, a P3 result, or execution
    authorization.
    """
    if not isinstance(components, Sequence) or isinstance(components, (str, bytes)):
        _fail("component observations must be a sequence")
    normalized = [_component(item) for item in components]
    expected = {
        TQQQ_CORE_ONLY_P2_V5.candidate_id: TQQQ_CORE_ONLY_P2_V5,
        SOXL_SOXX_CORE_ONLY_P2_V3.candidate_id: SOXL_SOXX_CORE_ONLY_P2_V3,
    }
    if {item["candidate_id"] for item in normalized} != set(expected) or len(normalized) != len(expected):
        _fail("component observations must exactly cover the current two research candidates")
    normalized.sort(key=lambda item: item["candidate_id"])
    for item in normalized:
        candidate = expected[item["candidate_id"]]
        if item["candidate_sha256"] != candidate.candidate_sha256 or item["config_sha256"] != candidate.config_sha256:
            _fail("component observation does not match registered candidate")

    reason_codes: list[str] = []
    for item in normalized:
        prefix = "TQQQ" if item["candidate_id"] == TQQQ_CORE_ONLY_P2_V5.candidate_id else "SOXL"
        if item["p1_status"] != P1_ACCEPTED:
            reason_codes.append(f"{prefix}_P1_NOT_ACCEPTED")
        if item["p3_status"] != P3_COMPLETE:
            reason_codes.append(f"{prefix}_P3_EVIDENCE_INCOMPLETE")
    if len({item["date_cutoff"] for item in normalized}) != 1:
        reason_codes.append("COMPONENT_CUTOFF_MISMATCH")

    component_ids = [item["candidate_id"] for item in normalized]
    proposal = {
        "proposal_id": "portfolio-research-" + _sha256(component_ids)[:16],
        "component_candidate_ids": component_ids,
        "p2_freeze_authorized": False,
        "p1_publish_authorized": False,
        "p3_replay_authorized": False,
        "p4_paper_authorized": False,
        "p5_shadow_authorized": False,
        "p6_live_authorized": False,
    }
    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "execution_authorized": False,
        "observed_at": _timestamp(observed_at),
        "status": READY_STATUS if not reason_codes else PARKED_STATUS,
        "reason_codes": sorted(reason_codes),
        "proposal": proposal,
        "components": normalized,
        "readiness_sha256": "",
    }
    result["readiness_sha256"] = _sha256(_state_payload(result))
    return result


def validate_portfolio_candidate_readiness(value: Mapping[str, object]) -> dict[str, object]:
    """Validate one safe, deduplicable portfolio research proposal signal."""
    record = _mapping(value, _ROOT_FIELDS, "portfolio readiness")
    if record["schema_version"] != SCHEMA_VERSION or record["research_only"] is not True or record["execution_authorized"] is not False:
        _fail("invalid portfolio readiness boundary")
    if record["status"] not in {PARKED_STATUS, READY_STATUS}:
        _fail("invalid portfolio readiness status")
    reasons = record["reason_codes"]
    if not isinstance(reasons, list) or any(not isinstance(item, str) or not item for item in reasons) or reasons != sorted(set(reasons)):
        _fail("invalid portfolio readiness reasons")
    components = record["components"]
    if not isinstance(components, list):
        _fail("invalid portfolio readiness components")
    normalized_components = [_component(item) for item in components]
    rebuilt = build_portfolio_candidate_readiness(
        components=normalized_components, observed_at=record["observed_at"]
    )
    if record["status"] != rebuilt["status"] or record["reason_codes"] != rebuilt["reason_codes"] or record["proposal"] != rebuilt["proposal"]:
        _fail("portfolio readiness state is inconsistent")
    digest = _digest(record["readiness_sha256"], "portfolio readiness digest")
    if digest != rebuilt["readiness_sha256"]:
        _fail("portfolio readiness digest mismatch")
    return rebuilt


__all__ = [
    "P3_COMPLETE",
    "P3_NOT_RUN",
    "P3_PARKED",
    "PARKED_STATUS",
    "READY_STATUS",
    "SCHEMA_VERSION",
    "PortfolioCandidateReadinessError",
    "build_component_observation",
    "build_portfolio_candidate_readiness",
    "validate_portfolio_candidate_readiness",
]
