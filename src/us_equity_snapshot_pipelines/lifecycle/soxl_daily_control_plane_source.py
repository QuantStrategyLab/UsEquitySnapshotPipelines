"""Pure, fail-closed control-plane source for the SOXL core-only lane.

The workflow supplies terminal P1/P3 state and digests; this module never
reads inputs, contacts a provider, or grants execution authority.  Its shape
intentionally mirrors the TQQQ daily source so the watcher can consume both
without a second lifecycle.
"""

from __future__ import annotations

import re
from datetime import datetime

from .soxl_core_only_p2_v3_contract import P2_V3_CONTRACT

SOURCE_ID = "uesp.soxl_daily_research"
SOURCE_SCHEMA_VERSION = "qsl_control_plane_source_snapshot.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_P1_STATUSES = frozenset({"ACCEPTED", "DEFERRED", "QUARANTINED"})
_P3_FAILURE_CLASSES = frozenset({
    "input_validation_failure", "config_contract_failure",
    "orchestrator_contract_failure", "risk_contract_failure",
    "evidence_validation_failure", "runtime_internal_failure",
})


class SoxlDailyControlPlaneSourceError(ValueError):
    """Raised when a terminal state cannot be represented faithfully."""


def _digest(value: object, label: str) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise SoxlDailyControlPlaneSourceError(f"invalid {label}")
    return value


def _revision(value: object) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise SoxlDailyControlPlaneSourceError("invalid source revision")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SoxlDailyControlPlaneSourceError("invalid computed timestamp")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SoxlDailyControlPlaneSourceError("invalid computed timestamp") from exc
    return value


def _candidate(*, lifecycle: dict[str, str], recommendation: dict[str, str],
               manifest: str | None, evidence: str | None, revision: str) -> dict[str, object]:
    return {
        "candidate_id": P2_V3_CONTRACT.candidate_id,
        "candidate_kind": "individual", "domain": "us_equity",
        "lifecycle": lifecycle,
        "evidence": {"p1_input_digest": manifest, "p2_config_digest": P2_V3_CONTRACT.config_sha256,
                     "p3_evidence_id": evidence, "source_revision": revision},
        "recommendation": recommendation,
        "freshness": {"status": "fresh", "age_seconds": 0},
    }


def build_soxl_daily_control_plane_source_snapshot(
    *, computed_at: object, source_revision: object, p1_status: object,
    p1_manifest_sha256: object, p2_config_sha256: object,
    p3_status: object = None, p3_evidence_sha256: object = None,
    p3_failure_class: object = None, p1_reason_code: object = None,
) -> dict[str, object]:
    """Build a metrics-free, research-only SOXL control-plane snapshot."""
    timestamp, revision = _timestamp(computed_at), _revision(source_revision)
    if not isinstance(p2_config_sha256, str) or p2_config_sha256 != P2_V3_CONTRACT.config_sha256:
        raise SoxlDailyControlPlaneSourceError("unexpected SOXL P2 config digest")
    manifest, evidence = _digest(p1_manifest_sha256, "P1 manifest digest"), _digest(p3_evidence_sha256, "P3 evidence digest")
    p1, p3 = str(p1_status or "").strip().upper(), str(p3_status or "").strip()
    reason = str(p1_reason_code or "").strip().lower()
    base = {"schema_version": SOURCE_SCHEMA_VERSION, "source_id": SOURCE_ID,
            "generated_at": timestamp, "computed_at": timestamp, "data_status": "ready"}
    if p1 not in _P1_STATUSES:
        base["candidates"] = [_candidate(lifecycle={"stage": "P1", "status": "parked"},
            recommendation={"code": "park", "reason": "P1 did not produce a terminal status."},
            manifest=None, evidence=None, revision=revision)]
        base["errors"] = ["p1_terminal_missing"]
        return base
    if p1 == "DEFERRED":
        if manifest or p3 or evidence:
            raise SoxlDailyControlPlaneSourceError("deferred P1 cannot carry a P3 result")
        if not reason:
            raise SoxlDailyControlPlaneSourceError("deferred P1 requires a reason code")
        base["candidates"] = [_candidate(lifecycle={"stage": "P1", "status": "deferred"},
            recommendation={"code": "defer", "reason": f"P1 deferred: {reason}; retry on the next scheduled session."},
            manifest=None, evidence=None, revision=revision)]
        base["errors"] = [f"p1_deferred_{reason}"]
        return base
    if p1 == "QUARANTINED":
        if manifest or p3 or evidence or not reason:
            raise SoxlDailyControlPlaneSourceError("invalid quarantined P1 terminal state")
        base["candidates"] = [_candidate(lifecycle={"stage": "P1", "status": "parked"},
            recommendation={"code": "park", "reason": f"P1 quarantined: {reason}."},
            manifest=None, evidence=None, revision=revision)]
        base["errors"] = [f"p1_quarantined_{reason}"]
        return base
    if manifest is None:
        raise SoxlDailyControlPlaneSourceError("accepted P1 requires a manifest digest")
    if p3 == "SUCCESS":
        if evidence is None:
            raise SoxlDailyControlPlaneSourceError("completed P3 requires an evidence digest")
        lifecycle, recommendation, errors = {"stage": "P3", "status": "verified"}, {"code": "keep_research", "reason": "P3 evidence completed; candidate remains research-only."}, []
    elif p3 == "PARKED":
        if evidence is not None or str(p3_failure_class or "") not in _P3_FAILURE_CLASSES:
            raise SoxlDailyControlPlaneSourceError("parked P3 requires a sanitized failure class")
        lifecycle, recommendation, errors = {"stage": "P3", "status": "parked"}, {"code": "park", "reason": f"P3 parked: {p3_failure_class}."}, ["p3_parked"]
    elif not p3 and evidence is None and not p3_failure_class:
        lifecycle, recommendation, errors = {"stage": "P3", "status": "parked"}, {"code": "park", "reason": "P3 did not produce a terminal status."}, ["p3_terminal_missing"]
    else:
        raise SoxlDailyControlPlaneSourceError("invalid P3 terminal state")
    base["candidates"] = [_candidate(lifecycle=lifecycle, recommendation=recommendation,
        manifest=manifest, evidence=evidence, revision=revision)]
    base["errors"] = errors
    return base


__all__ = ["SOURCE_ID", "SOURCE_SCHEMA_VERSION", "SoxlDailyControlPlaneSourceError", "build_soxl_daily_control_plane_source_snapshot"]
