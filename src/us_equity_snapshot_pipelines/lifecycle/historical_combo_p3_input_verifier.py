"""Fail-closed P3 preflight for an immutable historical-combo evidence chain.

The verifier consumes only the P1 binding and the existing UES P2 descriptor.
It never opens historical data, invokes a strategy, calculates a return, writes
an artifact, schedules work, or authorizes paper, shadow, or live execution.
Its ``READY_FOR_P3_REPLAY`` result is an input-integrity statement only, not a
performance result or a progression decision.
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

from .historical_combo_p1_input_binding import (
    P1_STATE,
    PORTFOLIO_RISK_BUDGET_SCHEMA,
    HistoricalComboP1InputBindingError,
    historical_combo_p1_input_sha256,
    validate_historical_combo_p1_input_binding,
)
from .historical_combo_p1_input_binding import (
    SCHEMA_VERSION as P1_INPUT_SCHEMA,
)

SCHEMA_VERSION = "qsl.us-equity-historical-combo-p3-input-verification.v1"
P2_CANDIDATE_SCHEMA = "qsl.us-equity-historical-combo-p2-candidate.v1"
P2_CANDIDATE_STATE = "FROZEN_RESEARCH_CANDIDATE"
READY_STATUS = "READY_FOR_P3_REPLAY"
PARKED_STATUS = "PARKED"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_EPSILON = 1e-12
_P2_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "candidate_state",
        "p1_input_sha256",
        "candidate",
        "selection_window",
        "holdout_window",
        "legs",
        "risk_budget",
        "promotion_recommendation",
        "p4_paper_authorized",
        "p5_shadow_authorized",
        "p6_live_authorized",
        "candidate_sha256",
    }
)
_CANDIDATE_FIELDS = frozenset({"candidate_id", "candidate_revision", "config_sha256"})
_WINDOW_FIELDS = frozenset({"start", "end"})
_LEG_FIELDS = frozenset(
    {"leg_id", "strategy_id", "strategy_revision", "config_sha256", "target_weight"}
)
_RISK_BUDGET_FIELDS = frozenset({"schema_version", "policy_sha256"})


class HistoricalComboP3InputVerifierError(ValueError):
    """Internal fail-closed error converted into a sanitized ``PARKED`` result."""

    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


def _fail(reason_code: str) -> None:
    raise HistoricalComboP3InputVerifierError(reason_code)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError):
        _fail("INVALID_P2_CANDIDATE")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, fields: frozenset[str], reason_code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(reason_code)
    return copy.deepcopy(dict(value))


def _identity(value: object, reason_code: str) -> str:
    if not isinstance(value, str) or not _IDENTITY.fullmatch(value):
        _fail(reason_code)
    return value


def _revision(value: object, reason_code: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail(reason_code)
    return value


def _digest(value: object, reason_code: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(reason_code)
    return value


def _date(value: object, reason_code: str) -> str:
    if not isinstance(value, str):
        _fail(reason_code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail(reason_code)
    if parsed.isoformat() != value:
        _fail(reason_code)
    return value


def _weight(value: object, reason_code: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(reason_code)
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0 or numeric > 1.0:
        _fail(reason_code)
    return numeric


def _candidate(value: object) -> dict[str, str]:
    candidate = _mapping(value, _CANDIDATE_FIELDS, "INVALID_P2_CANDIDATE")
    return {
        "candidate_id": _identity(candidate["candidate_id"], "INVALID_P2_CANDIDATE"),
        "candidate_revision": _revision(candidate["candidate_revision"], "INVALID_P2_CANDIDATE"),
        "config_sha256": _digest(candidate["config_sha256"], "INVALID_P2_CANDIDATE"),
    }


def _window(value: object) -> dict[str, str]:
    window = _mapping(value, _WINDOW_FIELDS, "INVALID_P2_CANDIDATE")
    start = _date(window["start"], "INVALID_P2_CANDIDATE")
    end = _date(window["end"], "INVALID_P2_CANDIDATE")
    if start > end:
        _fail("INVALID_P2_CANDIDATE")
    return {"start": start, "end": end}


def _p2_legs(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) < 2:
        _fail("INVALID_P2_CANDIDATE")
    legs: list[dict[str, object]] = []
    prior_leg_id: str | None = None
    for raw in value:
        leg = _mapping(raw, _LEG_FIELDS, "INVALID_P2_CANDIDATE")
        leg_id = _identity(leg["leg_id"], "INVALID_P2_CANDIDATE")
        if prior_leg_id is not None and leg_id <= prior_leg_id:
            _fail("INVALID_P2_CANDIDATE")
        prior_leg_id = leg_id
        legs.append(
            {
                "leg_id": leg_id,
                "strategy_id": _identity(leg["strategy_id"], "INVALID_P2_CANDIDATE"),
                "strategy_revision": _revision(leg["strategy_revision"], "INVALID_P2_CANDIDATE"),
                "config_sha256": _digest(leg["config_sha256"], "INVALID_P2_CANDIDATE"),
                "target_weight": _weight(leg["target_weight"], "INVALID_P2_CANDIDATE"),
            }
        )
    if not math.isclose(
        math.fsum(float(leg["target_weight"]) for leg in legs),
        1.0,
        rel_tol=0.0,
        abs_tol=_EPSILON,
    ):
        _fail("INVALID_P2_CANDIDATE")
    return legs


def _risk_budget(value: object) -> dict[str, str]:
    risk_budget = _mapping(value, _RISK_BUDGET_FIELDS, "INVALID_P2_CANDIDATE")
    if risk_budget["schema_version"] != PORTFOLIO_RISK_BUDGET_SCHEMA:
        _fail("INVALID_P2_CANDIDATE")
    return {
        "schema_version": PORTFOLIO_RISK_BUDGET_SCHEMA,
        "policy_sha256": _digest(risk_budget["policy_sha256"], "INVALID_P2_CANDIDATE"),
    }


def _p2_without_digest(value: Mapping[str, object]) -> dict[str, object]:
    result = copy.deepcopy(dict(value))
    result.pop("candidate_sha256", None)
    return result


def _validate_p2_candidate(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the published UES P2 descriptor at its hash boundary.

    This deliberately mirrors only the public, canonical envelope required for
    cross-repository verification.  UES remains the owner of P2 construction;
    no UES strategy or target-construction code is invoked here.
    """
    candidate = _mapping(value, _P2_FIELDS, "INVALID_P2_CANDIDATE")
    if (
        candidate["schema_version"] != P2_CANDIDATE_SCHEMA
        or candidate["research_only"] is not True
        or candidate["candidate_state"] != P2_CANDIDATE_STATE
        or candidate["promotion_recommendation"] is not None
        or candidate["p4_paper_authorized"] is not False
        or candidate["p5_shadow_authorized"] is not False
        or candidate["p6_live_authorized"] is not False
    ):
        _fail("INVALID_P2_CANDIDATE")
    selection = _window(candidate["selection_window"])
    holdout = _window(candidate["holdout_window"])
    if selection["end"] >= holdout["start"]:
        _fail("INVALID_P2_CANDIDATE")
    normalized: dict[str, object] = {
        "schema_version": P2_CANDIDATE_SCHEMA,
        "research_only": True,
        "candidate_state": P2_CANDIDATE_STATE,
        "p1_input_sha256": _digest(candidate["p1_input_sha256"], "INVALID_P2_CANDIDATE"),
        "candidate": _candidate(candidate["candidate"]),
        "selection_window": selection,
        "holdout_window": holdout,
        "legs": _p2_legs(candidate["legs"]),
        "risk_budget": _risk_budget(candidate["risk_budget"]),
        "promotion_recommendation": None,
        "p4_paper_authorized": False,
        "p5_shadow_authorized": False,
        "p6_live_authorized": False,
        "candidate_sha256": _digest(candidate["candidate_sha256"], "INVALID_P2_CANDIDATE"),
    }
    if normalized["candidate_sha256"] != _sha256(_p2_without_digest(normalized)):
        _fail("INVALID_P2_CANDIDATE")
    return normalized


def _verify_links(*, binding: Mapping[str, object], p2_candidate: Mapping[str, object]) -> dict[str, object]:
    if binding["schema_version"] != P1_INPUT_SCHEMA or binding["p1_state"] != P1_STATE:
        _fail("INVALID_P1_INPUT_BINDING")
    p1_input_sha256 = historical_combo_p1_input_sha256(binding)
    p2 = _validate_p2_candidate(p2_candidate)
    if p2["p1_input_sha256"] != p1_input_sha256:
        _fail("P1_P2_INPUT_DIGEST_MISMATCH")
    if p2["candidate"] != binding["candidate"]:
        _fail("P1_P2_CANDIDATE_MISMATCH")
    cutoff = str(binding["common_cutoff"])
    selection = p2["selection_window"]
    holdout = p2["holdout_window"]
    if not isinstance(selection, Mapping) or not isinstance(holdout, Mapping):
        _fail("INVALID_P2_CANDIDATE")
    if str(selection["end"]) > cutoff or str(holdout["end"]) > cutoff:
        _fail("FUTURE_LEAKAGE_DETECTED")
    components = binding["component_candidates"]
    legs = p2["legs"]
    if not isinstance(components, list) or not isinstance(legs, list):
        _fail("INVALID_P1_INPUT_BINDING")
    expected_components = [
        {
            "leg_id": component["leg_id"],
            "strategy_id": component["strategy_id"],
            "strategy_revision": component["strategy_revision"],
            "config_sha256": component["config_sha256"],
        }
        for component in components
    ]
    actual_components = [
        {
            "leg_id": leg["leg_id"],
            "strategy_id": leg["strategy_id"],
            "strategy_revision": leg["strategy_revision"],
            "config_sha256": leg["config_sha256"],
        }
        for leg in legs
    ]
    if actual_components != expected_components:
        _fail("P1_P2_COMPONENT_REVISION_MISMATCH")
    frozen_p2 = binding["frozen_p2"]
    if not isinstance(frozen_p2, Mapping) or not isinstance(p2["risk_budget"], Mapping):
        _fail("INVALID_P1_INPUT_BINDING")
    if p2["risk_budget"] != {
        "schema_version": frozen_p2["portfolio_risk_budget_schema"],
        "policy_sha256": frozen_p2["portfolio_risk_budget_policy_sha256"],
    }:
        _fail("P1_P2_RISK_POLICY_MISMATCH")
    summary = frozen_p2["virtual_target_summary"]
    if not isinstance(summary, Mapping) or summary.get("policy_sha256") != frozen_p2.get(
        "virtual_combo_policy_sha256"
    ):
        _fail("P1_VIRTUAL_TARGET_POLICY_MISMATCH")
    return {
        "p1_input_sha256": p1_input_sha256,
        "p2_candidate_sha256": p2["candidate_sha256"],
        "candidate": copy.deepcopy(binding["candidate"]),
        "common_cutoff": cutoff,
        "cost_declaration_sha256": _sha256(binding["cost_declaration"]),
        "virtual_target_summary": copy.deepcopy(dict(summary)),
    }


def _parked(reason_code: str) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "execution_authorized": False,
        "status": PARKED_STATUS,
        "reason_codes": [reason_code],
        "verified_inputs": None,
        "verification_sha256": None,
    }


def verify_historical_combo_p3_inputs(
    *, p1_input_binding: Mapping[str, object], p2_candidate: Mapping[str, object]
) -> dict[str, object]:
    """Return only a preflight identity summary or a fail-closed ``PARKED`` result."""
    try:
        binding = validate_historical_combo_p1_input_binding(p1_input_binding)
        verified_inputs = _verify_links(binding=binding, p2_candidate=p2_candidate)
        result: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "research_only": True,
            "execution_authorized": False,
            "status": READY_STATUS,
            "reason_codes": [],
            "verified_inputs": verified_inputs,
            "verification_sha256": "",
        }
        result["verification_sha256"] = _sha256(
            {key: value for key, value in result.items() if key != "verification_sha256"}
        )
        return result
    except HistoricalComboP3InputVerifierError as exc:
        return _parked(exc.reason_code)
    except HistoricalComboP1InputBindingError:
        return _parked("INVALID_P1_INPUT_BINDING")


__all__ = [
    "P2_CANDIDATE_SCHEMA",
    "PARKED_STATUS",
    "READY_STATUS",
    "SCHEMA_VERSION",
    "HistoricalComboP3InputVerifierError",
    "verify_historical_combo_p3_inputs",
]
