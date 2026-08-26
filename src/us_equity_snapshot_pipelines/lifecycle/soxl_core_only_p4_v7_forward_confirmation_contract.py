"""Immutable P4 policy identity for SOXL V7 forward confirmation.

This contract is deliberately evidence-only.  It binds the accepted V7 P3
baseline to one future, non-rolling 252-session checkpoint and cannot grant
execution or promotion authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)

POLICY_SCHEMA = "qsl.soxl-soxx-core-only-p4-forward-confirmation-policy.v1"
POLICY_CONFIG_SHA256 = "23ff522399c7fbd83c768ae2f4dd0ea5753d7a2337d6260bed1d6191e1cd9ca2"
BASELINE_P1_DATE_CUTOFF = "2026-08-25"
BASELINE_P1_MANIFEST_SHA256 = "d16f5b4f5191215010a6d40218423ab12a5ff419adb172d9d31f7145f914f406"
BASELINE_P3_EVIDENCE_SUMMARY_SHA256 = "ef329f83923767cf9aa564f71bdcbd9225e6ac45f34a4b9ed9a465e2ac56242c"
FIRST_FORWARD_XNYS_SESSION = "2026-08-26"
FORWARD_SESSION_COUNT = 252
COST_SCENARIOS_BPS = (5, 10, 15)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SoxlCoreOnlyP4V7ForwardConfirmationContract:
    """Frozen P4 identity; it is never an execution entitlement."""

    p2_candidate_id: str
    p2_config_sha256: str
    policy_config_sha256: str
    baseline_p3_evidence_summary_sha256: str
    first_forward_xnys_session: str
    forward_session_count: int
    cost_scenarios_bps: tuple[int, ...]


P4_V7_FORWARD_CONFIRMATION_CONTRACT = SoxlCoreOnlyP4V7ForwardConfirmationContract(
    p2_candidate_id=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
    p2_config_sha256=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
    policy_config_sha256=POLICY_CONFIG_SHA256,
    baseline_p3_evidence_summary_sha256=BASELINE_P3_EVIDENCE_SUMMARY_SHA256,
    first_forward_xnys_session=FIRST_FORWARD_XNYS_SESSION,
    forward_session_count=FORWARD_SESSION_COUNT,
    cost_scenarios_bps=COST_SCENARIOS_BPS,
)


class SoxlCoreOnlyP4V7ForwardConfirmationContractError(ValueError):
    """Fail-closed P4 policy error without market or account material."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy") from exc


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    return dict(value)


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    return value


def _date(value: object) -> str:
    if not isinstance(value, str):
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy") from exc
    return value


def validate_soxl_core_only_p4_v7_forward_confirmation_policy(value: object) -> dict[str, object]:
    """Require the complete, pre-registered P4 policy before it is used."""
    policy = _mapping(value)
    if hashlib.sha256(_canonical(policy)).hexdigest() != POLICY_CONFIG_SHA256:
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    if set(policy) != {
        "acceptance",
        "baseline",
        "candidate",
        "cost_scenarios_bps",
        "created_at",
        "forward_window",
        "purpose",
        "schema_version",
    } or policy["schema_version"] != POLICY_SCHEMA:
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    candidate = _mapping(policy["candidate"])
    baseline = _mapping(policy["baseline"])
    forward_window = _mapping(policy["forward_window"])
    acceptance = _mapping(policy["acceptance"])
    if candidate != {
        "candidate_id": P4_V7_FORWARD_CONFIRMATION_CONTRACT.p2_candidate_id,
        "config_sha256": P4_V7_FORWARD_CONFIRMATION_CONTRACT.p2_config_sha256,
    }:
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    if (
        set(baseline) != {"p1_date_cutoff", "p1_manifest_sha256", "p3_evidence_summary_sha256", "p3_verdict"}
        or _date(baseline["p1_date_cutoff"]) != BASELINE_P1_DATE_CUTOFF
        or _digest(baseline["p1_manifest_sha256"]) != BASELINE_P1_MANIFEST_SHA256
        or _digest(baseline["p3_evidence_summary_sha256"])
        != P4_V7_FORWARD_CONFIRMATION_CONTRACT.baseline_p3_evidence_summary_sha256
        or baseline["p3_verdict"] != "PASS_PENDING_FORWARD_CONFIRMATION"
    ):
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    if (
        set(forward_window)
        != {"first_forward_xnys_session", "initial_state", "selection_rule", "session_count"}
        or _date(forward_window["first_forward_xnys_session"])
        != P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session
        or forward_window["initial_state"] != "100_percent_cash_at_first_forward_signal"
        or forward_window["selection_rule"] != "first_complete_fixed_window_only_no_rolling_relabeling"
        or forward_window["session_count"] != P4_V7_FORWARD_CONFIRMATION_CONTRACT.forward_session_count
        or policy["cost_scenarios_bps"] != list(P4_V7_FORWARD_CONFIRMATION_CONTRACT.cost_scenarios_bps)
        or acceptance.get("automatic_promotion") is not False
        or acceptance.get("promotion_requires_separate_human_decision") is not True
    ):
        raise SoxlCoreOnlyP4V7ForwardConfirmationContractError("invalid SOXL V7 P4 policy")
    return policy


__all__ = [
    "BASELINE_P3_EVIDENCE_SUMMARY_SHA256",
    "COST_SCENARIOS_BPS",
    "FIRST_FORWARD_XNYS_SESSION",
    "FORWARD_SESSION_COUNT",
    "P4_V7_FORWARD_CONFIRMATION_CONTRACT",
    "POLICY_CONFIG_SHA256",
    "POLICY_SCHEMA",
    "SoxlCoreOnlyP4V7ForwardConfirmationContract",
    "SoxlCoreOnlyP4V7ForwardConfirmationContractError",
    "validate_soxl_core_only_p4_v7_forward_confirmation_policy",
]
