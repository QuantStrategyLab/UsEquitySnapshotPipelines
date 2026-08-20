"""Build the bounded control-plane source record for the daily TQQQ P1/P3 lane.

This module is intentionally pure: it does not contact a provider, read a
credential, access cloud storage, or make a network request.  The workflow
supplies only terminal status and immutable digests after its own P1/P3 checks
have completed.
"""

from __future__ import annotations

import re
from datetime import datetime

from .tqqq_core_only_p1_binding import P2_V5_CONTRACT
from .tqqq_p3_evidence_index import P3_STATUS

SOURCE_ID = "uesp.tqqq_daily_research"
SOURCE_SCHEMA_VERSION = "qsl_control_plane_source_snapshot.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_P1_STATUSES = frozenset({"ACCEPTED", "DEFERRED", "QUARANTINED"})
_P1_DEFERRED_REASON_CODES = frozenset(
    {
        "INPUT_UNAVAILABLE",
        "MISSING_SESSIONS",
        "ALPACA_RATE_LIMITED",
        "ALPACA_SERVICE_UNAVAILABLE",
        "ALPACA_TRANSPORT_UNAVAILABLE",
        "ALPACA_AUTH_OR_ENTITLEMENT",
        "ALPACA_REQUEST_REJECTED",
    }
)
_P1_OPERATOR_ATTENTION_REASON_CODES = frozenset(
    {"ALPACA_AUTH_OR_ENTITLEMENT", "ALPACA_REQUEST_REJECTED"}
)
_P1_QUARANTINED_REASON_CODES = frozenset({"P1_CONTRACT_FAILURE"})
_P3_FAILURE_CLASSES = frozenset(
    {
        "input_validation_failure",
        "config_contract_failure",
        "orchestrator_contract_failure",
        "risk_contract_failure",
        "evidence_validation_failure",
        "runtime_internal_failure",
    }
)


class TqqqDailyControlPlaneSourceError(ValueError):
    """Raised when a workflow terminal state cannot be represented faithfully."""


def _required_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqDailyControlPlaneSourceError(f"invalid {label}")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value in (None, ""):
        return None
    return _required_digest(value, label)


def _revision(value: object) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise TqqqDailyControlPlaneSourceError("invalid source revision")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise TqqqDailyControlPlaneSourceError("invalid computed timestamp")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TqqqDailyControlPlaneSourceError("invalid computed timestamp") from exc
    return value


def _p1_reason_code(value: object, *, allowed: frozenset[str], label: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise TqqqDailyControlPlaneSourceError(f"invalid {label}")
    return value


def _candidate(
    *,
    lifecycle: dict[str, str],
    recommendation: dict[str, str],
    p1_manifest_sha256: str | None,
    p2_config_sha256: str,
    p3_evidence_sha256: str | None,
    source_revision: str,
) -> dict[str, object]:
    return {
        "candidate_id": P2_V5_CONTRACT.candidate_id,
        "candidate_kind": "individual",
        "domain": "us_equity",
        "lifecycle": lifecycle,
        "evidence": {
            "p1_input_digest": p1_manifest_sha256,
            "p2_config_digest": p2_config_sha256,
            "p3_evidence_id": p3_evidence_sha256,
            "source_revision": source_revision,
        },
        "recommendation": recommendation,
        "freshness": {"status": "fresh", "age_seconds": 0},
    }


def build_tqqq_daily_control_plane_source_snapshot(
    *,
    computed_at: object,
    source_revision: object,
    p1_status: object,
    p1_reason_code: object = None,
    p1_manifest_sha256: object,
    p2_config_sha256: object,
    p3_status: object = None,
    p3_evidence_sha256: object = None,
    p3_failure_class: object = None,
) -> dict[str, object]:
    """Return one sanitized, non-execution source snapshot.

    P1 is authoritative for whether P3 may have run.  A deferred or
    quarantined input remains visible rather than being converted into a fake
    successful evidence record.  P3 success stays research-only; no outcome
    can grant P4, P5, or P6 authority.
    """
    timestamp = _timestamp(computed_at)
    revision = _revision(source_revision)
    config_digest = _required_digest(p2_config_sha256, "P2 config digest")
    if config_digest != P2_V5_CONTRACT.config_sha256:
        raise TqqqDailyControlPlaneSourceError("unexpected P2 v5 config digest")
    manifest_digest = _optional_digest(p1_manifest_sha256, "P1 manifest digest")
    evidence_digest = _optional_digest(p3_evidence_sha256, "P3 evidence digest")
    p1 = str(p1_status or "").strip().upper()
    p1_reason = str(p1_reason_code or "").strip().upper()
    p3 = str(p3_status or "").strip()
    failure_class = str(p3_failure_class or "").strip()

    if p1 not in _P1_STATUSES:
        return {
            "schema_version": SOURCE_SCHEMA_VERSION,
            "source_id": SOURCE_ID,
            "generated_at": timestamp,
            "computed_at": timestamp,
            "data_status": "ready",
            "candidates": [
                _candidate(
                    lifecycle={"stage": "P1", "status": "parked"},
                    recommendation={"code": "park", "reason": "P1 did not produce a terminal status."},
                    p1_manifest_sha256=None,
                    p2_config_sha256=config_digest,
                    p3_evidence_sha256=None,
                    source_revision=revision,
                )
            ],
            "errors": ["p1_terminal_missing"],
        }

    if p1 == "DEFERRED":
        if manifest_digest is not None or p3 or evidence_digest is not None or failure_class:
            raise TqqqDailyControlPlaneSourceError("deferred P1 cannot carry a P3 result")
        deferred_reason = _p1_reason_code(
            p1_reason, allowed=_P1_DEFERRED_REASON_CODES, label="deferred P1 reason code"
        )
        rendered_reason = deferred_reason.lower()
        needs_operator_attention = deferred_reason in _P1_OPERATOR_ATTENTION_REASON_CODES
        recommendation = (
            {
                "code": "defer",
                "reason": (
                    f"P1 deferred: {rendered_reason}; inspect Alpaca account or request configuration."
                ),
            }
            if needs_operator_attention
            else {
                "code": "defer",
                "reason": f"P1 deferred: {rendered_reason}; retry on the next scheduled session.",
            }
        )
        return {
            "schema_version": SOURCE_SCHEMA_VERSION,
            "source_id": SOURCE_ID,
            "generated_at": timestamp,
            "computed_at": timestamp,
            "data_status": "ready",
            "candidates": [
                _candidate(
                    lifecycle={"stage": "P1", "status": "deferred"},
                    recommendation=recommendation,
                    p1_manifest_sha256=None,
                    p2_config_sha256=config_digest,
                    p3_evidence_sha256=None,
                    source_revision=revision,
                )
            ],
            "errors": [
                "p1_deferred_"
                f"{'operator_attention_' if needs_operator_attention else ''}{rendered_reason}"
            ],
        }

    if p1 == "QUARANTINED":
        if manifest_digest is not None or p3 or evidence_digest is not None or failure_class:
            raise TqqqDailyControlPlaneSourceError("quarantined P1 cannot carry a P3 result")
        quarantined_reason = _p1_reason_code(
            p1_reason, allowed=_P1_QUARANTINED_REASON_CODES, label="quarantined P1 reason code"
        )
        rendered_reason = quarantined_reason.lower()
        return {
            "schema_version": SOURCE_SCHEMA_VERSION,
            "source_id": SOURCE_ID,
            "generated_at": timestamp,
            "computed_at": timestamp,
            "data_status": "ready",
            "candidates": [
                _candidate(
                    lifecycle={"stage": "P1", "status": "parked"},
                    recommendation={"code": "park", "reason": f"P1 quarantined: {rendered_reason}."},
                    p1_manifest_sha256=None,
                    p2_config_sha256=config_digest,
                    p3_evidence_sha256=None,
                    source_revision=revision,
                )
            ],
            "errors": [f"p1_quarantined_{rendered_reason}"],
        }

    if manifest_digest is None:
        raise TqqqDailyControlPlaneSourceError("accepted P1 requires a manifest digest")
    if p3 == P3_STATUS:
        if evidence_digest is None or failure_class:
            raise TqqqDailyControlPlaneSourceError("completed P3 requires only its evidence digest")
        lifecycle = {"stage": "P3", "status": "verified"}
        recommendation = {"code": "keep_research", "reason": "P3 evidence completed; candidate remains research-only."}
        errors: list[str] = []
    elif p3 == "PARKED":
        if evidence_digest is not None or failure_class not in _P3_FAILURE_CLASSES:
            raise TqqqDailyControlPlaneSourceError("parked P3 requires a sanitized failure class")
        lifecycle = {"stage": "P3", "status": "parked"}
        recommendation = {"code": "park", "reason": f"P3 parked: {failure_class}."}
        errors = ["p3_parked"]
    elif not p3 and evidence_digest is None and not failure_class:
        lifecycle = {"stage": "P3", "status": "parked"}
        recommendation = {"code": "park", "reason": "P3 did not produce a terminal status."}
        errors = ["p3_terminal_missing"]
    else:
        raise TqqqDailyControlPlaneSourceError("invalid P3 terminal state")

    return {
        "schema_version": SOURCE_SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "generated_at": timestamp,
        "computed_at": timestamp,
        "data_status": "ready",
        "candidates": [
            _candidate(
                lifecycle=lifecycle,
                recommendation=recommendation,
                p1_manifest_sha256=manifest_digest,
                p2_config_sha256=config_digest,
                p3_evidence_sha256=evidence_digest,
                source_revision=revision,
            )
        ],
        "errors": errors,
    }


__all__ = [
    "SOURCE_ID",
    "SOURCE_SCHEMA_VERSION",
    "TqqqDailyControlPlaneSourceError",
    "build_tqqq_daily_control_plane_source_snapshot",
]
