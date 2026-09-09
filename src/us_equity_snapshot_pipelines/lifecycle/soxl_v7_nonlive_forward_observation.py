"""Candidate-bound, no-broker P4 observation records for SOXL V7.

The V7 candidate is not the platform's legacy ``soxl_soxx_trend_income``
profile.  This adapter creates an isolated Shadow decision context and
simulated-Paper replay input from the frozen V7 P1/P2 lineage. It never calls
a broker, changes a runtime target, or grants Live authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import exchange_calendars as xcals
import pandas as pd
from quant_platform_kit.strategy_lifecycle.forward_observation import (
    ForwardObservationPolicy,
    ForwardObservationPolicyError,
    ForwardObservationSnapshot,
    evaluate_forward_observation,
)
from quant_platform_kit.strategy_lifecycle.forward_observation_receipt import (
    InvalidForwardObservationReceipt,
    build_forward_observation_receipt,
    validate_forward_observation_receipt,
)

from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from .soxl_core_only_p4_v7_forward_confirmation_contract import (
    P4_V7_FORWARD_CONFIRMATION_CONTRACT,
)


NONLIVE_FORWARD_OBSERVATION_SCHEMA = "soxl_v7_nonlive_forward_observation.v2"
SOXL_V7_CONTROL_PLANE_SOURCE_ID = "uesp-soxl-v7-nonlive-forward"
CONTROL_PLANE_SOURCE_SCHEMA = "qsl_control_plane_source_snapshot.v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CALENDAR = xcals.get_calendar("XNYS")
_PAPER_REPLAY_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-input.v1"
_SOURCE_CONTEXT_SCHEMA = "qsl.soxl-core-only-p3-strategy-context.v1"
_INITIAL_EQUITY = 100_000.0
_SIMULATED_PAPER_COST_BPS = 10.0


class SoxlV7NonliveForwardObservationError(ValueError):
    """Raised for an invalid V7 non-live record without exposing price rows."""


def _fail(message: str = "invalid SOXL V7 non-live forward observation") -> None:
    raise SoxlV7NonliveForwardObservationError(message)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlV7NonliveForwardObservationError(
            "invalid SOXL V7 non-live forward observation"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        _fail(f"invalid {label}")
    return value


def _session_date(value: object) -> str:
    if not isinstance(value, str) or len(value) < 10:
        _fail("invalid observation session")
    try:
        parsed = date.fromisoformat(value[:10])
    except ValueError:
        _fail("invalid observation session")
    return parsed.isoformat()


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not value:
        _fail("invalid observation timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("invalid observation timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("invalid observation timestamp")
    return value


def _expected_sessions(start: str, end: str) -> tuple[str, ...]:
    try:
        sessions = _CALENDAR.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    except (TypeError, ValueError):
        _fail("invalid observation session range")
    return tuple(pd.Timestamp(session).date().isoformat() for session in sessions)


def _validated_forward_sessions(materialized: Mapping[str, object]) -> list[dict[str, Any]]:
    value = _mapping(materialized)
    p1 = _mapping(value.get("p1_identity"))
    p2 = _mapping(value.get("p2_identity"))
    _digest(p1.get("input_manifest_sha256"), "P1 manifest")
    if p2 != {
        "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
        "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
    }:
        _fail("invalid V7 P1/P2 identity")
    raw_sessions = value.get("sessions")
    if not isinstance(raw_sessions, list):
        _fail("invalid V7 materialized sessions")
    forward: list[dict[str, Any]] = []
    for raw in raw_sessions:
        item = _mapping(raw)
        if set(item) != {"as_of", "market_data", "prices"}:
            _fail("invalid V7 materialized sessions")
        session = _session_date(item["as_of"])
        if session >= P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session:
            if not isinstance(item["market_data"], Mapping) or not isinstance(item["prices"], Mapping):
                _fail("invalid V7 materialized sessions")
            forward.append(item)
    if not forward:
        _fail("V7 forward observation has not reached its first session")
    dates = tuple(_session_date(item["as_of"]) for item in forward)
    if dates != _expected_sessions(dates[0], dates[-1]):
        _fail("V7 forward observation sessions are not contiguous XNYS sessions")
    if dates[0] != P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session:
        _fail("V7 forward observation does not start at the frozen first session")
    if len(forward) > P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count:
        _fail("V7 forward observation would roll beyond its fixed window")
    return forward


@dataclass(frozen=True)
class SoxlV7NonliveForwardInputs:
    """Sanitized inputs for one Shadow plus simulated-Paper observation cycle."""

    p1_manifest_sha256: str
    observation_sessions: tuple[str, ...]
    shadow_source_context: Mapping[str, object]
    simulated_paper_replay_input: Mapping[str, object]


def build_soxl_v7_nonlive_forward_inputs(
    materialized: Mapping[str, object],
) -> SoxlV7NonliveForwardInputs:
    """Project an assured P1 materialization into V7-only non-live inputs."""

    value = _mapping(materialized)
    p1 = _mapping(value.get("p1_identity"))
    p1_manifest = _digest(p1.get("input_manifest_sha256"), "P1 manifest")
    forward = _validated_forward_sessions(value)
    latest = forward[-1]
    as_of = str(latest["as_of"])
    shadow_source_context: dict[str, object] = {
        "schema_version": _SOURCE_CONTEXT_SCHEMA,
        "as_of": as_of,
        "portfolio": {
            "as_of": as_of,
            "total_equity": _INITIAL_EQUITY,
            "buying_power": _INITIAL_EQUITY,
            "cash_balance": _INITIAL_EQUITY,
            "positions": [],
            "metadata": {"observed_effective_exposure": 0.0},
        },
        "market_data": dict(_mapping(latest["market_data"])),
    }
    simulated_paper_replay_input: dict[str, object] = {
        "schema_version": _PAPER_REPLAY_SCHEMA,
        "initial_equity": _INITIAL_EQUITY,
        "cost_bps": _SIMULATED_PAPER_COST_BPS,
        "sessions": [
            {
                "as_of": str(item["as_of"]),
                "market_data": dict(_mapping(item["market_data"])),
                "prices": dict(_mapping(item["prices"])),
            }
            for item in forward
        ],
    }
    return SoxlV7NonliveForwardInputs(
        p1_manifest_sha256=p1_manifest,
        observation_sessions=tuple(_session_date(item["as_of"]) for item in forward),
        shadow_source_context=shadow_source_context,
        simulated_paper_replay_input=simulated_paper_replay_input,
    )


def build_soxl_v7_nonlive_forward_policy() -> ForwardObservationPolicy:
    """Return the explicit non-live policy for the frozen SOXL V7 candidate."""

    return ForwardObservationPolicy(
        candidate_id=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
        strategy_profile="soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve",
        domain="us_equity",
        benchmark_symbol="SOXX",
        required_trading_sessions=P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count,
        review_milestones=(20, 60),
        automatic_non_live_modes=("shadow", "paper"),
        auto_resume_clean_sessions=3,
        observation_calendar="XNYS",
        observation_window_type="fixed",
        observation_start_session=P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session,
        window_rationale_ref=(
            "sha256:" + P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256
        ),
        non_live_evidence_modes=("shadow_decision", "simulated_replay"),
    )


def _previous_state(
    previous_record: Mapping[str, object] | None,
    *,
    policy: ForwardObservationPolicy,
) -> tuple[int, str, int, dict[str, object] | None]:
    if previous_record is None:
        return 0, "not_started", 0, None
    value = _mapping(previous_record)
    if value.get("schema_version") != NONLIVE_FORWARD_OBSERVATION_SCHEMA:
        _fail("invalid previous non-live record")
    if value.get("candidate_id") != P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id:
        _fail("previous record candidate mismatch")
    controller = _mapping(value.get("controller"))
    observed = controller.get("observations_completed")
    if not isinstance(observed, int) or isinstance(observed, bool) or observed < 0:
        _fail("invalid previous observation count")
    state = str(controller.get("state") or "")
    previous_state = {
        "PARKED": "not_started",
        "FORWARD_ACTIVE": "active",
        "PAUSED": "paused",
        "FORWARD_COMPLETE_HUMAN_REVIEW": "complete",
        "MANUAL_HOLD": "manual_hold",
        "IDENTITY_MISMATCH": "identity_mismatch",
        "RISK_BLOCKED": "risk_blocked",
        "REVOKED": "revoked",
        "SUPERSEDED": "superseded",
    }.get(state)
    if previous_state is None:
        _fail("invalid previous observation state")
    clean = value.get("clean_sessions_since_pause", 0)
    if not isinstance(clean, int) or isinstance(clean, bool) or clean < 0:
        _fail("invalid previous recovery count")
    try:
        receipt = validate_forward_observation_receipt(
            _mapping(value.get("forward_observation_receipt")), policy=policy
        )
    except InvalidForwardObservationReceipt as exc:
        raise SoxlV7NonliveForwardObservationError(
            "invalid previous non-live receipt"
        ) from exc
    if receipt["observation_index"] != observed:
        _fail("previous receipt observation count mismatch")
    return observed, previous_state, clean, receipt


def next_soxl_v7_nonlive_observation_session(
    *,
    requested_completed_session: str,
    previous_record: Mapping[str, object] | None,
) -> str | None:
    """Return the one safe P4 session to record, or ``None`` when caught up.

    The durable receipt chain advances by exactly one XNYS session.  A market
    data outage must therefore be backfilled before a newer session can be
    recorded; otherwise the next receipt would have a non-contiguous index
    and permanently park the forward observer.  This function only resolves
    a date from already-validated receipt state.  It has no market-data,
    storage, broker, deployment, or execution dependency.
    """

    requested = _session_date(requested_completed_session)
    if _expected_sessions(requested, requested) != (requested,):
        _fail("requested observation session is not an XNYS session")
    first = P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session
    if requested < first:
        _fail("requested observation session precedes the frozen window")

    if previous_record is None:
        next_session = first
    else:
        policy = build_soxl_v7_nonlive_forward_policy()
        observed, _, _, receipt = _previous_state(previous_record, policy=policy)
        if receipt is None:
            _fail("invalid previous non-live receipt")
        value = _mapping(previous_record)
        last = _session_date(value.get("last_observed_session"))
        sessions_value = value.get("observation_sessions")
        if not isinstance(sessions_value, list):
            _fail("invalid previous observation sessions")
        sessions = tuple(_session_date(item) for item in sessions_value)
        if (
            not sessions
            or sessions[0] != first
            or sessions != _expected_sessions(sessions[0], sessions[-1])
            or len(sessions) != observed
            or last != sessions[-1]
            or receipt["observation_session"] != last
            or receipt["observation_index"] != observed
        ):
            _fail("invalid previous observation chain")
        next_session = pd.Timestamp(_CALENDAR.next_session(pd.Timestamp(last))).date().isoformat()

    return next_session if next_session <= requested else None


def _evidence_digest(value: object | None, label: str, *, required: bool) -> str | None:
    if value is None and not required:
        return None
    return _digest(value, label)


def _strategy_release_identity_sha256() -> str:
    """Hash the frozen isolated runner identity without treating it as Live release authority."""

    return _sha256(
        {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "ues_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
            "qpk_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.qpk_revision,
            "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
            "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        }
    )


def _receipt_dependency_digests(inputs: SoxlV7NonliveForwardInputs) -> dict[str, str]:
    return {
        "p1_manifest": _digest(inputs.p1_manifest_sha256, "P1 manifest"),
        "p2_config": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        "p3_evidence": P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256,
        "risk_policy": P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256,
        "strategy_release": _strategy_release_identity_sha256(),
        "plugin_bundle": _sha256({"plugin_bundle": "none"}),
    }


def build_soxl_v7_nonlive_forward_record(
    *,
    observed_at: str,
    inputs: SoxlV7NonliveForwardInputs,
    shadow_observation_sha256: str | None,
    simulated_paper_observation_sha256: str | None,
    previous_record: Mapping[str, object] | None = None,
    data_status: str = "ready",
    shadow_status: str = "healthy",
    paper_status: str = "healthy",
    risk_status: str = "pass",
    control_status: str = "clear",
) -> dict[str, object]:
    """Build one durable, no-order V7 observation receipt.

    When source or replay health is not ready, evidence digests may be absent;
    the shared controller then pauses both non-live modes. A record never
    carries raw prices, account IDs, orders, or deployment instructions.
    """

    timestamp = _timestamp(observed_at)
    sessions = tuple(inputs.observation_sessions)
    if not sessions:
        _fail("missing observation sessions")
    if sessions != _expected_sessions(sessions[0], sessions[-1]):
        _fail("invalid observation sessions")
    if sessions[0] != P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session:
        _fail("invalid observation sessions")
    if len(sessions) > P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count:
        _fail("observation exceeds fixed window")
    policy = build_soxl_v7_nonlive_forward_policy()
    prior_count, previous_state, prior_clean, previous_receipt = _previous_state(
        previous_record, policy=policy
    )
    if prior_count > len(sessions):
        _fail("observation count regressed")
    healthy = (
        data_status == "ready"
        and shadow_status == "healthy"
        and paper_status == "healthy"
        and risk_status == "pass"
    )
    clean = prior_clean + 1 if healthy and previous_state == "paused" else 0
    shadow_digest = _evidence_digest(
        shadow_observation_sha256, "Shadow observation digest", required=healthy
    )
    paper_digest = _evidence_digest(
        simulated_paper_observation_sha256,
        "simulated Paper observation digest",
        required=healthy,
    )
    try:
        forward_receipt = build_forward_observation_receipt(
            policy=policy,
            observation_session=sessions[-1],
            observation_index=len(sessions),
            dependency_digests=_receipt_dependency_digests(inputs),
            evidence_modes=policy.non_live_evidence_modes,
            previous_receipt=previous_receipt,
        )
        controller = evaluate_forward_observation(
            policy,
            ForwardObservationSnapshot(
                historical_evidence_verified=True,
                historical_evidence_ref=(
                    "sha256:"
                    + P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256
                ),
                observations_completed=len(sessions),
                previous_observations_completed=prior_count,
                previous_state=previous_state,
                clean_sessions_since_pause=clean,
                data_status=data_status,
                shadow_status=shadow_status,
                paper_status=paper_status,
                risk_status=risk_status,
                control_status=control_status,
            ),
        )
    except (ForwardObservationPolicyError, InvalidForwardObservationReceipt) as exc:
        raise SoxlV7NonliveForwardObservationError(
            "invalid SOXL V7 non-live forward observation"
        ) from exc
    record: dict[str, object] = {
        "schema_version": NONLIVE_FORWARD_OBSERVATION_SCHEMA,
        "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
        "candidate_config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        "p4_policy_sha256": P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256,
        "observed_at": timestamp,
        "last_observed_session": sessions[-1],
        "p1_manifest_sha256": _digest(inputs.p1_manifest_sha256, "P1 manifest"),
        "observation_sessions": list(sessions),
        "shadow_observation_sha256": shadow_digest,
        "simulated_paper_observation_sha256": paper_digest,
        "forward_observation_receipt": forward_receipt,
        "controller": controller.to_dict(),
        "clean_sessions_since_pause": clean,
        "no_order": True,
        "broker_dependency": False,
        "permission_effect": "none",
        "live_authority_granted": False,
        "record_sha256": "",
    }
    record["record_sha256"] = _sha256(
        {key: value for key, value in record.items() if key != "record_sha256"}
    )
    return record


def build_soxl_v7_forward_control_plane_source(
    record: Mapping[str, object],
    *,
    generated_at: str,
) -> dict[str, object]:
    """Project one validated immutable V7 record into the existing web source shape."""

    value = _mapping(record)
    expected_fields = {
        "schema_version",
        "candidate_id",
        "candidate_config_sha256",
        "p4_policy_sha256",
        "observed_at",
        "last_observed_session",
        "p1_manifest_sha256",
        "observation_sessions",
        "shadow_observation_sha256",
        "simulated_paper_observation_sha256",
        "forward_observation_receipt",
        "controller",
        "clean_sessions_since_pause",
        "no_order",
        "broker_dependency",
        "permission_effect",
        "live_authority_granted",
        "record_sha256",
    }
    if set(value) != expected_fields:
        _fail()
    claimed_record_sha256 = _digest(value.get("record_sha256"), "record digest")
    if claimed_record_sha256 != _sha256(
        {key: item for key, item in value.items() if key != "record_sha256"}
    ):
        _fail("invalid record digest")
    if (
        value.get("schema_version") != NONLIVE_FORWARD_OBSERVATION_SCHEMA
        or value.get("candidate_id")
        != P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id
        or value.get("candidate_config_sha256")
        != P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256
        or value.get("p4_policy_sha256")
        != P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256
        or value.get("no_order") is not True
        or value.get("broker_dependency") is not False
        or value.get("permission_effect") != "none"
        or value.get("live_authority_granted") is not False
    ):
        _fail()
    observed_at = _timestamp(value.get("observed_at"))
    projected_at = _timestamp(generated_at)
    observed_time = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    projected_time = datetime.fromisoformat(projected_at.replace("Z", "+00:00"))
    age_seconds = int((projected_time - observed_time).total_seconds())
    if age_seconds < 0:
        _fail("projection timestamp precedes observation")

    sessions_value = value.get("observation_sessions")
    if not isinstance(sessions_value, list) or not sessions_value:
        _fail("invalid observation sessions")
    sessions = tuple(_session_date(item) for item in sessions_value)
    if (
        sessions[0] != P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session
        or sessions != _expected_sessions(sessions[0], sessions[-1])
        or len(sessions) > P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count
        or value.get("last_observed_session") != sessions[-1]
    ):
        _fail("invalid observation sessions")

    policy = build_soxl_v7_nonlive_forward_policy()
    try:
        receipt = validate_forward_observation_receipt(
            _mapping(value.get("forward_observation_receipt")),
            policy=policy,
        )
    except InvalidForwardObservationReceipt as exc:
        raise SoxlV7NonliveForwardObservationError(
            "invalid SOXL V7 non-live forward observation"
        ) from exc
    p1_manifest = _digest(value.get("p1_manifest_sha256"), "P1 manifest")
    expected_dependencies = {
        "p1_manifest": p1_manifest,
        "p2_config": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        "p3_evidence": P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256,
        "risk_policy": P4_V7_FORWARD_CONFIRMATION_CONTRACT.policy_config_sha256,
        "strategy_release": _strategy_release_identity_sha256(),
        "plugin_bundle": _sha256({"plugin_bundle": "none"}),
    }
    if (
        receipt["dependency_digests"] != expected_dependencies
        or receipt["observation_session"] != sessions[-1]
        or receipt["observation_index"] != len(sessions)
    ):
        _fail("invalid observation receipt binding")

    controller = _mapping(value.get("controller"))
    controller_fields = {
        "schema_version",
        "candidate_id",
        "strategy_profile",
        "domain",
        "benchmark_symbol",
        "state",
        "non_live_actions",
        "notifications",
        "reasons",
        "observations_completed",
        "required_trading_sessions",
        "historical_evidence_ref",
        "live_action",
        "no_order",
        "live_authority_granted",
    }
    states = {
        "PARKED",
        "FORWARD_ACTIVE",
        "PAUSED",
        "FORWARD_COMPLETE_HUMAN_REVIEW",
        "MANUAL_HOLD",
        "IDENTITY_MISMATCH",
        "RISK_BLOCKED",
        "REVOKED",
        "SUPERSEDED",
    }
    if (
        set(controller) != controller_fields
        or controller.get("candidate_id") != policy.candidate_id
        or controller.get("strategy_profile") != policy.strategy_profile
        or controller.get("domain") != policy.domain
        or controller.get("benchmark_symbol") != policy.benchmark_symbol
        or controller.get("state") not in states
        or type(controller.get("observations_completed")) is not int
        or controller.get("observations_completed") != len(sessions)
        or type(controller.get("required_trading_sessions")) is not int
        or controller.get("required_trading_sessions") != policy.required_trading_sessions
        or controller.get("historical_evidence_ref")
        != "sha256:" + P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256
        or controller.get("live_action") != "human_approval_required"
        or controller.get("no_order") is not True
        or controller.get("live_authority_granted") is not False
    ):
        _fail("invalid observation controller")
    state = str(controller["state"])
    if (
        state == "FORWARD_COMPLETE_HUMAN_REVIEW"
        and len(sessions) != policy.required_trading_sessions
    ) or (
        state == "FORWARD_ACTIVE"
        and len(sessions) >= policy.required_trading_sessions
    ):
        _fail("observation state and count disagree")
    shadow_digest = value.get("shadow_observation_sha256")
    paper_digest = value.get("simulated_paper_observation_sha256")
    if shadow_digest is not None:
        _digest(shadow_digest, "Shadow observation digest")
    if paper_digest is not None:
        _digest(paper_digest, "simulated Paper observation digest")
    if state in {"FORWARD_ACTIVE", "FORWARD_COMPLETE_HUMAN_REVIEW"} and (
        shadow_digest is None or paper_digest is None
    ):
        _fail("active observation evidence is missing")
    if state in {"FORWARD_ACTIVE"}:
        lifecycle = {"stage": "P4", "status": "shadow"}
        recommendation = {
            "code": "auto_shadow_evaluation",
            "reason": "V7 non-live forward observation remains active.",
        }
    elif state == "FORWARD_COMPLETE_HUMAN_REVIEW":
        lifecycle = {"stage": "P4", "status": "evidence_pending"}
        recommendation = {
            "code": "keep_research",
            "reason": "V7 fixed forward window completed; evidence remains research-only.",
        }
    else:
        lifecycle = {"stage": "P4", "status": "parked"}
        recommendation = {
            "code": "park",
            "reason": "V7 non-live forward observation is paused or blocked.",
        }
    candidate = {
        "candidate_id": policy.candidate_id,
        "candidate_kind": "individual",
        "domain": policy.domain,
        "lifecycle": lifecycle,
        "evidence": {
            "p1_input_digest": p1_manifest,
            "p2_config_digest": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
            "p3_evidence_id": P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256,
            "source_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
        },
        "recommendation": recommendation,
        "freshness": {"status": "fresh", "age_seconds": age_seconds},
        "forward_observation": {
            "state": state,
            "observations_completed": len(sessions),
            "required_trading_sessions": policy.required_trading_sessions,
            "last_observed_session": sessions[-1],
            "observed_at": observed_at,
            "no_order": True,
            "live_authority_granted": False,
        },
    }
    return {
        "schema_version": CONTROL_PLANE_SOURCE_SCHEMA,
        "source_id": SOXL_V7_CONTROL_PLANE_SOURCE_ID,
        "generated_at": projected_at,
        "computed_at": observed_at,
        "data_status": "ready",
        "candidates": [candidate],
        "errors": [],
    }


__all__ = [
    "CONTROL_PLANE_SOURCE_SCHEMA",
    "NONLIVE_FORWARD_OBSERVATION_SCHEMA",
    "SOXL_V7_CONTROL_PLANE_SOURCE_ID",
    "SoxlV7NonliveForwardInputs",
    "SoxlV7NonliveForwardObservationError",
    "build_soxl_v7_nonlive_forward_inputs",
    "build_soxl_v7_nonlive_forward_policy",
    "build_soxl_v7_nonlive_forward_record",
    "build_soxl_v7_forward_control_plane_source",
    "next_soxl_v7_nonlive_observation_session",
]
