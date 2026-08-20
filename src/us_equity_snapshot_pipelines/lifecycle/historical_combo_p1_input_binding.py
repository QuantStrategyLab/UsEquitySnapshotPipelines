"""Immutable, data-free P1 identity for a historical multi-strategy combo.

This module binds only hashes and declarations before a future replay can read
historical inputs.  It deliberately has no provider, storage, scheduler,
credential, broker, order, return, or promotion dependency.

The P2 candidate descriptor contains the P1 digest, so it cannot itself be
embedded here without creating a circular identity.  Instead this binding
freezes the P2 policy and a retention-safe projection of the existing
``virtual_combo_targets`` result.  The P3 preflight verifier checks the two
directions of that link before any later replay is allowed to start.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

SCHEMA_VERSION = "qsl.us-equity-historical-combo-p1-input-binding.v1"
VIRTUAL_COMBO_TARGET_SCHEMA = "qsl.us-equity-virtual-combo-target.v1"
VIRTUAL_COMBO_POLICY_SCHEMA = "qsl.us-equity-virtual-combo-policy.v1"
PORTFOLIO_RISK_BUDGET_SCHEMA = "qsl.portfolio-risk-budget-research.v1"
VIRTUAL_TARGET_EVIDENCE_SCOPE = "VIRTUAL_TARGET_CONSTRUCTION_ONLY"
P1_STATE = "FROZEN_RESEARCH_INPUT"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_EPSILON = 1e-12
_EXECUTION_TIMING = "next_complete_trading_session_after_signal_effective_date"
_PIT_SCHEMA = "qsl.point-in-time-data-declaration.v1"
_COST_SCHEMA = "qsl.research-cost-declaration.v1"
_ROOT_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "p1_state",
        "candidate",
        "common_cutoff",
        "component_candidates",
        "pit_declaration",
        "cost_declaration",
        "frozen_p2",
        "input_sha256",
    }
)
_CANDIDATE_FIELDS = frozenset({"candidate_id", "candidate_revision", "config_sha256"})
_COMPONENT_FIELDS = frozenset(
    {
        "leg_id",
        "strategy_id",
        "strategy_revision",
        "config_sha256",
        "source_p1_sha256",
        "source_date_cutoff",
    }
)
_PIT_FIELDS = frozenset(
    {
        "schema_version",
        "availability_basis",
        "future_data_allowed",
        "revised_data_allowed",
        "signal_execution_timing",
    }
)
_COST_FIELDS = frozenset(
    {
        "schema_version",
        "turnover_cost_bps",
        "borrow_cost_bps",
        "cash_yield_assumption",
        "execution_timing",
    }
)
_FROZEN_P2_FIELDS = frozenset(
    {
        "virtual_combo_policy_schema",
        "virtual_combo_policy_sha256",
        "portfolio_risk_budget_schema",
        "portfolio_risk_budget_policy_sha256",
        "virtual_target_summary",
    }
)
_VIRTUAL_TARGET_SUMMARY_FIELDS = frozenset(
    {"schema_version", "status", "policy_sha256", "input_sha256", "combo_target_sha256"}
)
_FULL_VIRTUAL_TARGET_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "execution_authorized",
        "evidence_scope",
        "status",
        "reason_codes",
        "policy_sha256",
        "input_sha256",
        "combo_target_weights",
        "summary",
        "combo_target_sha256",
    }
)


class HistoricalComboP1InputBindingError(ValueError):
    """Fail-closed error for a mutable, incomplete, or execution-capable P1 binding."""


def _fail(message: str) -> None:
    raise HistoricalComboP1InputBindingError(message)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HistoricalComboP1InputBindingError("invalid historical combo P1 binding") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _identity(value: object, label: str) -> str:
    if not isinstance(value, str) or not _IDENTITY.fullmatch(value):
        _fail(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail(f"invalid {label}")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(f"invalid {label}")
    return value


def _date(value: object, label: str) -> str:
    if not isinstance(value, str):
        _fail(f"invalid {label}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail(f"invalid {label}")
    if parsed.isoformat() != value:
        _fail(f"invalid {label}")
    return value


def _number(value: object, label: str, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(f"invalid {label}")
    result = float(value)
    if not math.isfinite(result) or (nonnegative and result < 0.0):
        _fail(f"invalid {label}")
    return result


def _candidate(value: object) -> dict[str, str]:
    candidate = _mapping(value, _CANDIDATE_FIELDS, "combo candidate")
    return {
        "candidate_id": _identity(candidate["candidate_id"], "candidate id"),
        "candidate_revision": _revision(candidate["candidate_revision"], "candidate revision"),
        "config_sha256": _digest(candidate["config_sha256"], "candidate config digest"),
    }


def _components(value: object, *, common_cutoff: str) -> list[dict[str, str]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) < 2:
        _fail("invalid component candidates")
    result: list[dict[str, str]] = []
    previous_leg_id: str | None = None
    for raw in value:
        component = _mapping(raw, _COMPONENT_FIELDS, "component candidate")
        leg_id = _identity(component["leg_id"], "component leg id")
        if previous_leg_id is not None and leg_id <= previous_leg_id:
            _fail("component candidates must be uniquely sorted")
        previous_leg_id = leg_id
        source_cutoff = _date(component["source_date_cutoff"], "component source date cutoff")
        if source_cutoff != common_cutoff:
            _fail("component candidates must share the common cutoff")
        result.append(
            {
                "leg_id": leg_id,
                "strategy_id": _identity(component["strategy_id"], "component strategy id"),
                "strategy_revision": _revision(
                    component["strategy_revision"], "component strategy revision"
                ),
                "config_sha256": _digest(component["config_sha256"], "component config digest"),
                "source_p1_sha256": _digest(component["source_p1_sha256"], "component source P1 digest"),
                "source_date_cutoff": source_cutoff,
            }
        )
    return result


def _pit_declaration(value: object) -> dict[str, object]:
    declaration = _mapping(value, _PIT_FIELDS, "point-in-time declaration")
    if (
        declaration["schema_version"] != _PIT_SCHEMA
        or declaration["availability_basis"] != "AS_OF_COMMON_CUTOFF"
        or declaration["future_data_allowed"] is not False
        or declaration["revised_data_allowed"] is not False
        or declaration["signal_execution_timing"] != _EXECUTION_TIMING
    ):
        _fail("invalid point-in-time declaration")
    return {
        "schema_version": _PIT_SCHEMA,
        "availability_basis": "AS_OF_COMMON_CUTOFF",
        "future_data_allowed": False,
        "revised_data_allowed": False,
        "signal_execution_timing": _EXECUTION_TIMING,
    }


def _cost_declaration(value: object) -> dict[str, object]:
    declaration = _mapping(value, _COST_FIELDS, "cost declaration")
    raw_costs = declaration["turnover_cost_bps"]
    if not isinstance(raw_costs, Sequence) or isinstance(raw_costs, (str, bytes)) or not raw_costs:
        _fail("invalid turnover cost declaration")
    costs = [_number(cost, "turnover cost", nonnegative=True) for cost in raw_costs]
    if costs != sorted(set(costs)) or all(cost <= _EPSILON for cost in costs):
        _fail("invalid turnover cost declaration")
    if (
        declaration["schema_version"] != _COST_SCHEMA
        or declaration["execution_timing"] != _EXECUTION_TIMING
    ):
        _fail("invalid cost declaration")
    return {
        "schema_version": _COST_SCHEMA,
        "turnover_cost_bps": costs,
        "borrow_cost_bps": _number(declaration["borrow_cost_bps"], "borrow cost", nonnegative=True),
        "cash_yield_assumption": _number(
            declaration["cash_yield_assumption"], "cash yield assumption", nonnegative=True
        ),
        "execution_timing": _EXECUTION_TIMING,
    }


def build_virtual_combo_target_summary(value: Mapping[str, object]) -> dict[str, str]:
    """Project an existing P2 virtual target into a retention-safe identity summary.

    The upstream virtual target's self-digest is checked before weights and
    metrics are discarded.  The result therefore reuses its public schema and
    digest rather than creating a second target format in this repository.
    """
    target = _mapping(value, _FULL_VIRTUAL_TARGET_FIELDS, "virtual combo target")
    claimed_digest = _digest(target.pop("combo_target_sha256"), "virtual combo target digest")
    if (
        target["schema_version"] != VIRTUAL_COMBO_TARGET_SCHEMA
        or target["research_only"] is not True
        or target["execution_authorized"] is not False
        or target["evidence_scope"] != VIRTUAL_TARGET_EVIDENCE_SCOPE
        or target["status"] not in {"APPROVE", "REDUCE"}
        or claimed_digest != _sha256(target)
    ):
        _fail("invalid virtual combo target")
    return {
        "schema_version": VIRTUAL_COMBO_TARGET_SCHEMA,
        "status": str(target["status"]),
        "policy_sha256": _digest(target["policy_sha256"], "virtual combo policy digest"),
        "input_sha256": _digest(target["input_sha256"], "virtual combo input digest"),
        "combo_target_sha256": claimed_digest,
    }


def _virtual_target_summary(value: object, *, policy_sha256: str) -> dict[str, str]:
    summary = _mapping(value, _VIRTUAL_TARGET_SUMMARY_FIELDS, "virtual target summary")
    if summary["schema_version"] != VIRTUAL_COMBO_TARGET_SCHEMA or summary["status"] not in {"APPROVE", "REDUCE"}:
        _fail("invalid virtual target summary")
    if _digest(summary["policy_sha256"], "virtual target summary policy digest") != policy_sha256:
        _fail("virtual target summary policy mismatch")
    return {
        "schema_version": VIRTUAL_COMBO_TARGET_SCHEMA,
        "status": str(summary["status"]),
        "policy_sha256": policy_sha256,
        "input_sha256": _digest(summary["input_sha256"], "virtual target summary input digest"),
        "combo_target_sha256": _digest(
            summary["combo_target_sha256"], "virtual target summary digest"
        ),
    }


def _frozen_p2(value: object) -> dict[str, object]:
    frozen = _mapping(value, _FROZEN_P2_FIELDS, "frozen P2 policy")
    if (
        frozen["virtual_combo_policy_schema"] != VIRTUAL_COMBO_POLICY_SCHEMA
        or frozen["portfolio_risk_budget_schema"] != PORTFOLIO_RISK_BUDGET_SCHEMA
    ):
        _fail("invalid frozen P2 policy")
    policy_sha256 = _digest(frozen["virtual_combo_policy_sha256"], "virtual combo policy digest")
    return {
        "virtual_combo_policy_schema": VIRTUAL_COMBO_POLICY_SCHEMA,
        "virtual_combo_policy_sha256": policy_sha256,
        "portfolio_risk_budget_schema": PORTFOLIO_RISK_BUDGET_SCHEMA,
        "portfolio_risk_budget_policy_sha256": _digest(
            frozen["portfolio_risk_budget_policy_sha256"], "portfolio risk policy digest"
        ),
        "virtual_target_summary": _virtual_target_summary(
            frozen["virtual_target_summary"], policy_sha256=policy_sha256
        ),
    }


def _without_input_digest(value: Mapping[str, object]) -> dict[str, object]:
    result = copy.deepcopy(dict(value))
    result.pop("input_sha256", None)
    return result


def build_historical_combo_p1_input_binding(
    *,
    candidate: object,
    common_cutoff: object,
    component_candidates: object,
    pit_declaration: object,
    cost_declaration: object,
    virtual_combo_policy_sha256: object,
    portfolio_risk_budget_policy_sha256: object,
    virtual_combo_target: Mapping[str, object],
) -> dict[str, object]:
    """Freeze P1 identity and declarations without acquiring historical data."""
    cutoff = _date(common_cutoff, "common cutoff")
    target_summary = build_virtual_combo_target_summary(virtual_combo_target)
    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "p1_state": P1_STATE,
        "candidate": _candidate(candidate),
        "common_cutoff": cutoff,
        "component_candidates": _components(component_candidates, common_cutoff=cutoff),
        "pit_declaration": _pit_declaration(pit_declaration),
        "cost_declaration": _cost_declaration(cost_declaration),
        "frozen_p2": {
            "virtual_combo_policy_schema": VIRTUAL_COMBO_POLICY_SCHEMA,
            "virtual_combo_policy_sha256": _digest(
                virtual_combo_policy_sha256, "virtual combo policy digest"
            ),
            "portfolio_risk_budget_schema": PORTFOLIO_RISK_BUDGET_SCHEMA,
            "portfolio_risk_budget_policy_sha256": _digest(
                portfolio_risk_budget_policy_sha256, "portfolio risk policy digest"
            ),
            "virtual_target_summary": target_summary,
        },
        "input_sha256": "",
    }
    result["frozen_p2"] = _frozen_p2(result["frozen_p2"])
    result["input_sha256"] = _sha256(_without_input_digest(result))
    return validate_historical_combo_p1_input_binding(result)


def validate_historical_combo_p1_input_binding(value: Mapping[str, object]) -> dict[str, object]:
    """Validate one exact, non-executing historical-combo P1 binding."""
    binding = _mapping(value, _ROOT_FIELDS, "historical combo P1 binding")
    if (
        binding["schema_version"] != SCHEMA_VERSION
        or binding["research_only"] is not True
        or binding["p1_state"] != P1_STATE
    ):
        _fail("invalid historical combo P1 boundary")
    cutoff = _date(binding["common_cutoff"], "common cutoff")
    normalized: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "p1_state": P1_STATE,
        "candidate": _candidate(binding["candidate"]),
        "common_cutoff": cutoff,
        "component_candidates": _components(binding["component_candidates"], common_cutoff=cutoff),
        "pit_declaration": _pit_declaration(binding["pit_declaration"]),
        "cost_declaration": _cost_declaration(binding["cost_declaration"]),
        "frozen_p2": _frozen_p2(binding["frozen_p2"]),
        "input_sha256": _digest(binding["input_sha256"], "P1 input digest"),
    }
    if normalized["input_sha256"] != _sha256(_without_input_digest(normalized)):
        _fail("historical combo P1 input digest mismatch")
    return normalized


def canonical_historical_combo_p1_input_binding_bytes(value: Mapping[str, object]) -> bytes:
    """Return canonical bytes for a validated P1 binding only."""
    return _canonical(validate_historical_combo_p1_input_binding(value))


def historical_combo_p1_input_sha256(value: Mapping[str, object]) -> str:
    """Return the self-digest after validating the P1 binding."""
    return str(validate_historical_combo_p1_input_binding(value)["input_sha256"])


__all__ = [
    "P1_STATE",
    "PORTFOLIO_RISK_BUDGET_SCHEMA",
    "SCHEMA_VERSION",
    "VIRTUAL_COMBO_POLICY_SCHEMA",
    "VIRTUAL_COMBO_TARGET_SCHEMA",
    "HistoricalComboP1InputBindingError",
    "build_historical_combo_p1_input_binding",
    "build_virtual_combo_target_summary",
    "canonical_historical_combo_p1_input_binding_bytes",
    "historical_combo_p1_input_sha256",
    "validate_historical_combo_p1_input_binding",
]
