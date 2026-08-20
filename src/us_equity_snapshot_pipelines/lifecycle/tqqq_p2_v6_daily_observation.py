"""Redacted, short-lived daily record for the TQQQ v6 observe-only signal.

This module joins three already-existing local inputs: a verified immutable
v5 P1 root, a completed v5 P3 daily status, and its validated P5 forward
observation.  It never changes the forward decision or exposes bars, signal
payload, account data, or orders.  A caller may retain the returned metadata
briefly as a GitHub Actions artifact; cloud/object-store persistence is outside
this module and deliberately not granted here.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path

from .tqqq_core_only_p1_binding import P2_V5_CONTRACT
from .tqqq_p2_v6_plugin_observe import (
    OBSERVE_ONLY_MODE,
    P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
    TqqqP2V6PluginObserveError,
)
from .tqqq_p2_v6_qqq_price_regime_root import (
    TqqqP2V6QqqPriceRegimeRootError,
    build_tqqq_p2_v6_qqq_price_regime_observe_from_root,
    verify_tqqq_p3_v6_qqq_price_regime_from_root,
)
from .tqqq_p5_forward_observation import (
    TqqqP5ForwardObservationError,
    validate_tqqq_p5_forward_observation,
)


SCHEMA_VERSION = "qsl.tqqq-p2-v6-daily-observation.v1"
ARTIFACT_RETENTION_DAYS = 35
QSP_QQQ_PRICE_REGIME_OBSERVER_REVISION = "0d5b48ce4f9dd56491d6a6b51fdf5b0aa4cb256c"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_FIELDS = frozenset(
    {
        "schema_version",
        "produced_at",
        "candidate",
        "source_evidence",
        "signal",
        "observer",
        "target_equivalence",
        "retention",
        "authority",
        "observation_sha256",
    }
)


class TqqqP2V6DailyObservationError(ValueError):
    """Sanitized error for a v6 daily observation that cannot be recorded."""


def _fail(code: str) -> None:
    raise TqqqP2V6DailyObservationError(code)


def _exact_mapping(value: object, fields: frozenset[str], code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(code)
    return value


def _digest(value: object, code: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(code)
    return value


def _revision(value: object, code: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail(code)
    return value


def _timestamp(value: object, code: str) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        _fail(code)
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise TqqqP2V6DailyObservationError(code) from exc
    return value


def _date(value: object, code: str) -> str:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        _fail(code)
    try:
        if date.fromisoformat(value).isoformat() != value:
            _fail(code)
    except ValueError as exc:
        raise TqqqP2V6DailyObservationError(code) from exc
    return value


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TqqqP2V6DailyObservationError("invalid_daily_observation") from exc


def calculate_tqqq_p2_v6_daily_observation_sha256(value: Mapping[str, object]) -> str:
    """Return the digest of one record excluding its self-digest."""

    material = dict(value)
    material.pop("observation_sha256", None)
    return hashlib.sha256(_canonical_bytes(material)).hexdigest()


def _daily_status_source(value: object) -> tuple[str, str, str]:
    status = _exact_mapping(
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
        "invalid_daily_status",
    )
    candidate = _exact_mapping(
        status["candidate"],
        frozenset({"candidate_id", "config_sha256"}),
        "invalid_daily_status",
    )
    terminal = _exact_mapping(
        status["p3_terminal"],
        frozenset({"evidence_sha256", "status", "verdict"}),
        "invalid_daily_status",
    )
    if (
        status["schema_version"] != "qsl.tqqq-daily-research-status.v1"
        or dict(candidate)
        != {"candidate_id": P2_V5_CONTRACT.candidate_id, "config_sha256": P2_V5_CONTRACT.config_sha256}
        or terminal["status"] != "EVIDENCE_V2_COMPLETE"
    ):
        _fail("invalid_daily_status")
    return (
        _digest(terminal["evidence_sha256"], "invalid_daily_status"),
        _digest(status["input_manifest_sha256"], "invalid_daily_status"),
        _date(status["date_cutoff"], "invalid_daily_status"),
    )


def _target_mapping(forward_observation: Mapping[str, object]) -> dict[str, int]:
    decision = forward_observation["forward_decision"]
    if not isinstance(decision, Mapping):
        _fail("invalid_forward_observation")
    allocation = decision.get("allocation_bps")
    if not isinstance(allocation, Mapping):
        _fail("invalid_forward_observation")
    return dict(allocation)


def _retention_boundary() -> dict[str, object]:
    return {
        "storage": "GITHUB_ACTIONS_ARTIFACT",
        "retention_days": ARTIFACT_RETENTION_DAYS,
        "durable_retention_authorized": False,
        "raw_bars_included": False,
    }


def _authority() -> dict[str, object]:
    return {
        "research_only": True,
        "no_order": True,
        "p4_p5_p6_authorized": False,
    }


def build_tqqq_p2_v6_daily_observation(
    *,
    snapshot_root: str | Path,
    daily_research_status: Mapping[str, object],
    forward_observation: Mapping[str, object],
    qsp_revision: str,
    produced_at: str,
) -> dict[str, object]:
    """Build one redacted record after v5 P3 and its unchanged forward decision.

    The observer targets are a detached copy of the existing v5 forward target
    mapping.  The strict v6 verifier recomputes the signal from the same P1
    root and proves this mapping has not been transformed.
    """

    p3_evidence_sha256, status_manifest_sha256, status_date_cutoff = _daily_status_source(
        daily_research_status
    )
    try:
        forward = validate_tqqq_p5_forward_observation(forward_observation)
    except TqqqP5ForwardObservationError as exc:
        raise TqqqP2V6DailyObservationError("invalid_forward_observation") from exc
    source = forward["source_evidence"]
    decision = forward["forward_decision"]
    assert isinstance(source, Mapping) and isinstance(decision, Mapping)
    if source["p3_evidence_sha256"] != p3_evidence_sha256:
        _fail("p3_evidence_mismatch")
    if source["p1_manifest_sha256"] != status_manifest_sha256:
        _fail("p1_manifest_mismatch")
    if qsp_revision != QSP_QQQ_PRICE_REGIME_OBSERVER_REVISION:
        _fail("invalid_qsp_revision")
    try:
        contract, signal = build_tqqq_p2_v6_qqq_price_regime_observe_from_root(
            snapshot_root=snapshot_root,
            qsp_revision=_revision(qsp_revision, "invalid_qsp_revision"),
        )
        target_mapping = _target_mapping(forward)
        evidence = verify_tqqq_p3_v6_qqq_price_regime_from_root(
            snapshot_root=snapshot_root,
            contract=contract,
            signal_envelope=signal,
            base_strategy_targets=target_mapping,
            observer_strategy_targets=dict(target_mapping),
        )
    except (TqqqP2V6QqqPriceRegimeRootError, TqqqP2V6PluginObserveError) as exc:
        raise TqqqP2V6DailyObservationError("v6_recomputation_failure") from exc
    if (
        evidence.get("status") != "VERIFIED_OBSERVE_ONLY"
        or evidence.get("schema_version") != "qsl.tqqq-p3-plugin-observe-evidence.v1"
    ):
        _fail("v6_recomputation_failure")
    candidate = evidence["candidate"]
    p1 = evidence["p1"]
    signal_reference = evidence["signal"]
    equivalence = evidence["target_equivalence"]
    if not all(isinstance(value, Mapping) for value in (candidate, p1, signal_reference, equivalence)):
        _fail("invalid_v6_evidence")
    if (
        p1["p1_manifest_sha256"] != status_manifest_sha256
        or p1["date_cutoff"] != status_date_cutoff
    ):
        _fail("p1_identity_mismatch")
    record: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "produced_at": _timestamp(produced_at, "invalid_produced_at"),
        "candidate": {
            "candidate_id": P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
            "contract_sha256": candidate["contract_sha256"],
            "base_candidate_id": P2_V5_CONTRACT.candidate_id,
            "base_config_sha256": P2_V5_CONTRACT.config_sha256,
        },
        "source_evidence": {
            "p1_manifest_sha256": p1["p1_manifest_sha256"],
            "input_root_sha256": p1["input_root_sha256"],
            "date_cutoff": p1["date_cutoff"],
            "p3_evidence_sha256": p3_evidence_sha256,
            "forward_decision_sha256": decision["decision_sha256"],
            "effective_session": decision["effective_session"],
        },
        "signal": dict(signal_reference),
        "observer": {
            "mode": OBSERVE_ONLY_MODE,
            "strategy_target_transform": "none",
            "execution_authorized": False,
            "ai_input_allowed": False,
        },
        "target_equivalence": dict(equivalence),
        "retention": _retention_boundary(),
        "authority": _authority(),
        "observation_sha256": "",
    }
    record["observation_sha256"] = calculate_tqqq_p2_v6_daily_observation_sha256(record)
    return validate_tqqq_p2_v6_daily_observation(record)


def validate_tqqq_p2_v6_daily_observation(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the bounded record that may be uploaded as a short-lived artifact."""

    record = _exact_mapping(value, _FIELDS, "invalid_daily_observation")
    if record["schema_version"] != SCHEMA_VERSION:
        _fail("invalid_daily_observation")
    candidate = _exact_mapping(
        record["candidate"],
        frozenset(
            {"candidate_id", "contract_sha256", "base_candidate_id", "base_config_sha256"}
        ),
        "invalid_daily_observation",
    )
    source = _exact_mapping(
        record["source_evidence"],
        frozenset(
            {
                "p1_manifest_sha256",
                "input_root_sha256",
                "date_cutoff",
                "p3_evidence_sha256",
                "forward_decision_sha256",
                "effective_session",
            }
        ),
        "invalid_daily_observation",
    )
    signal = _exact_mapping(
        record["signal"],
        frozenset({"schema_version", "plugin_id", "payload_sha256", "producer_revision", "config_sha256"}),
        "invalid_daily_observation",
    )
    observer = _exact_mapping(
        record["observer"],
        frozenset({"mode", "strategy_target_transform", "execution_authorized", "ai_input_allowed"}),
        "invalid_daily_observation",
    )
    equivalence = _exact_mapping(
        record["target_equivalence"],
        frozenset({"base_candidate_id", "equivalent", "strategy_targets_sha256"}),
        "invalid_daily_observation",
    )
    retention = _exact_mapping(
        record["retention"],
        frozenset({"storage", "retention_days", "durable_retention_authorized", "raw_bars_included"}),
        "invalid_daily_observation",
    )
    authority = _exact_mapping(
        record["authority"],
        frozenset({"research_only", "no_order", "p4_p5_p6_authorized"}),
        "invalid_daily_observation",
    )
    if dict(candidate) != {
        "candidate_id": P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
        "contract_sha256": _digest(candidate["contract_sha256"], "invalid_daily_observation"),
        "base_candidate_id": P2_V5_CONTRACT.candidate_id,
        "base_config_sha256": P2_V5_CONTRACT.config_sha256,
    }:
        _fail("invalid_daily_observation")
    for field in (
        "p1_manifest_sha256",
        "input_root_sha256",
        "p3_evidence_sha256",
        "forward_decision_sha256",
    ):
        _digest(source[field], "invalid_daily_observation")
    _date(source["date_cutoff"], "invalid_daily_observation")
    _date(source["effective_session"], "invalid_daily_observation")
    if (
        signal["schema_version"] != "qsl.strategy-plugin-signal.v2"
        or signal["plugin_id"] != "qqq_price_regime_observer"
        or signal["producer_revision"] != QSP_QQQ_PRICE_REGIME_OBSERVER_REVISION
    ):
        _fail("invalid_daily_observation")
    _digest(signal["payload_sha256"], "invalid_daily_observation")
    _digest(signal["config_sha256"], "invalid_daily_observation")
    _revision(signal["producer_revision"], "invalid_daily_observation")
    if dict(observer) != {
        "mode": OBSERVE_ONLY_MODE,
        "strategy_target_transform": "none",
        "execution_authorized": False,
        "ai_input_allowed": False,
    }:
        _fail("invalid_daily_observation")
    if (
        equivalence["base_candidate_id"] != P2_V5_CONTRACT.candidate_id
        or equivalence["equivalent"] is not True
    ):
        _fail("invalid_daily_observation")
    _digest(equivalence["strategy_targets_sha256"], "invalid_daily_observation")
    if dict(retention) != _retention_boundary() or dict(authority) != _authority():
        _fail("invalid_daily_observation")
    normalized = {
        "schema_version": SCHEMA_VERSION,
        "produced_at": _timestamp(record["produced_at"], "invalid_daily_observation"),
        "candidate": dict(candidate),
        "source_evidence": dict(source),
        "signal": dict(signal),
        "observer": dict(observer),
        "target_equivalence": dict(equivalence),
        "retention": dict(retention),
        "authority": dict(authority),
        "observation_sha256": _digest(record["observation_sha256"], "invalid_daily_observation"),
    }
    if normalized["observation_sha256"] != calculate_tqqq_p2_v6_daily_observation_sha256(normalized):
        _fail("invalid_daily_observation")
    return normalized


__all__ = [
    "ARTIFACT_RETENTION_DAYS",
    "QSP_QQQ_PRICE_REGIME_OBSERVER_REVISION",
    "SCHEMA_VERSION",
    "TqqqP2V6DailyObservationError",
    "build_tqqq_p2_v6_daily_observation",
    "calculate_tqqq_p2_v6_daily_observation_sha256",
    "validate_tqqq_p2_v6_daily_observation",
]
