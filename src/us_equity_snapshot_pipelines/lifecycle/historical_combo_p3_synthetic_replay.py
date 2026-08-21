"""Offline-only synthetic replay for the frozen historical-combo P3 contract.

This module is deliberately a small test harness, not a market-data backtest
runner.  It accepts injected *synthetic* leg-return fixtures only after the
existing P1/P2 verifier has bound the candidate identity.  Its output is
explicitly incompatible with the completed-P3 evidence index: a synthetic
result can characterize this replay contract, but can never establish real
market performance, promotion, paper, shadow, or live authority.
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

from .historical_combo_p1_input_binding import validate_historical_combo_p1_input_binding
from .historical_combo_p3_input_verifier import (
    PARKED_STATUS as PREFLIGHT_PARKED_STATUS,
    READY_STATUS,
    verify_historical_combo_p3_inputs,
)

SCHEMA_VERSION = "qsl.us-equity-historical-combo-p3-synthetic-replay.v1"
INPUT_SCHEMA_VERSION = "qsl.us-equity-historical-combo-p3-synthetic-replay-input.v1"
COMPLETE_STATUS = "SYNTHETIC_REPLAY_COMPLETE_NOT_REAL_EVIDENCE"
PARKED_STATUS = "PARKED"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_EPSILON = 1e-12
_INPUT_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "data_class",
        "real_market_evidence",
        "p1_input_sha256",
        "p2_candidate_sha256",
        "p3_input_verification_sha256",
        "common_cutoff",
        "pit_declaration_sha256",
        "cost_declaration_sha256",
        "oos_segments",
        "observations",
        "input_sha256",
    }
)
_SEGMENT_FIELDS = frozenset({"segment_id", "start", "end"})
_OBSERVATION_FIELDS = frozenset({"session", "leg_returns"})


class HistoricalComboP3SyntheticReplayError(ValueError):
    """Fail-closed error translated to a retention-safe ``PARKED`` result."""

    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


def _fail(reason_code: str) -> None:
    raise HistoricalComboP3SyntheticReplayError(reason_code)


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError):
        _fail("INVALID_SYNTHETIC_REPLAY_INPUT")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, fields: frozenset[str], reason_code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(reason_code)
    return copy.deepcopy(dict(value))


def _digest(value: object, reason_code: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(reason_code)
    return value


def _identity(value: object, reason_code: str) -> str:
    if not isinstance(value, str) or not _IDENTITY.fullmatch(value):
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


def _return(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail("INVALID_SYNTHETIC_LEG_RETURN")
    result = float(value)
    if not math.isfinite(result) or result <= -1.0:
        _fail("INVALID_SYNTHETIC_LEG_RETURN")
    return result


def _without_input_sha256(value: Mapping[str, object]) -> dict[str, object]:
    result = copy.deepcopy(dict(value))
    result.pop("input_sha256", None)
    return result


def _ready_preflight(
    *, p1_input_binding: Mapping[str, object], p2_candidate: Mapping[str, object]
) -> dict[str, object] | None:
    """Return the already-established P3 identity or ``None`` if it parked."""
    result = verify_historical_combo_p3_inputs(
        p1_input_binding=p1_input_binding, p2_candidate=p2_candidate
    )
    if result["status"] == PREFLIGHT_PARKED_STATUS:
        return None
    if result["status"] != READY_STATUS:
        _fail("INVALID_P3_INPUT_VERIFICATION")
    verification_sha256 = _digest(
        result.get("verification_sha256"), "INVALID_P3_INPUT_VERIFICATION"
    )
    without_digest = {key: value for key, value in result.items() if key != "verification_sha256"}
    if verification_sha256 != _sha256(without_digest):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    verified_inputs = result.get("verified_inputs")
    if not isinstance(verified_inputs, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    required = frozenset(
        {
            "p1_input_sha256",
            "p2_candidate_sha256",
            "candidate",
            "common_cutoff",
            "cost_declaration_sha256",
            "virtual_target_summary",
        }
    )
    if set(verified_inputs) != required:
        _fail("INVALID_P3_INPUT_VERIFICATION")
    return copy.deepcopy(result)


def _p1_pit_declaration_sha256(p1_input_binding: Mapping[str, object]) -> str:
    binding = validate_historical_combo_p1_input_binding(p1_input_binding)
    declaration = binding["pit_declaration"]
    if not isinstance(declaration, Mapping):
        _fail("INVALID_P1_INPUT_BINDING")
    return _sha256(declaration)


def _segments(
    value: object,
    *,
    holdout_start: str,
    holdout_end: str,
    common_cutoff: str,
) -> list[dict[str, str]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        _fail("INVALID_OOS_SEGMENTS")
    result: list[dict[str, str]] = []
    previous_end: str | None = None
    previous_id: str | None = None
    for raw in value:
        segment = _mapping(raw, _SEGMENT_FIELDS, "INVALID_OOS_SEGMENTS")
        segment_id = _identity(segment["segment_id"], "INVALID_OOS_SEGMENTS")
        start = _date(segment["start"], "INVALID_OOS_SEGMENTS")
        end = _date(segment["end"], "INVALID_OOS_SEGMENTS")
        if start > end or start < holdout_start or end > holdout_end:
            _fail("OOS_SEGMENT_OUTSIDE_FROZEN_HOLDOUT")
        if end > common_cutoff:
            _fail("FUTURE_LEAKAGE_DETECTED")
        if previous_end is not None and start <= previous_end:
            _fail("OOS_SEGMENTS_OVERLAP_OR_UNSORTED")
        if previous_id is not None and segment_id <= previous_id:
            _fail("OOS_SEGMENTS_OVERLAP_OR_UNSORTED")
        previous_end = end
        previous_id = segment_id
        result.append({"segment_id": segment_id, "start": start, "end": end})
    return result


def _segment_for_session(session: str, segments: Sequence[Mapping[str, str]]) -> str | None:
    for segment in segments:
        if str(segment["start"]) <= session <= str(segment["end"]):
            return str(segment["segment_id"])
    return None


def _observations(
    value: object,
    *,
    segments: Sequence[Mapping[str, str]],
    leg_ids: frozenset[str],
    common_cutoff: str,
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        _fail("INVALID_SYNTHETIC_OBSERVATIONS")
    result: list[dict[str, object]] = []
    previous_session: str | None = None
    represented_segments: set[str] = set()
    for raw in value:
        observation = _mapping(raw, _OBSERVATION_FIELDS, "INVALID_SYNTHETIC_OBSERVATIONS")
        session = _date(observation["session"], "INVALID_SYNTHETIC_OBSERVATIONS")
        if session > common_cutoff:
            _fail("FUTURE_LEAKAGE_DETECTED")
        if previous_session is not None and session <= previous_session:
            _fail("SYNTHETIC_OBSERVATIONS_UNSORTED")
        segment_id = _segment_for_session(session, segments)
        if segment_id is None:
            _fail("SYNTHETIC_OBSERVATION_OUTSIDE_OOS")
        raw_returns = observation["leg_returns"]
        if not isinstance(raw_returns, Mapping) or set(raw_returns) != leg_ids:
            _fail("SYNTHETIC_LEG_SET_MISMATCH")
        returns = {leg_id: _return(raw_returns[leg_id]) for leg_id in sorted(leg_ids)}
        result.append({"session": session, "leg_returns": returns})
        represented_segments.add(segment_id)
        previous_session = session
    if represented_segments != {str(segment["segment_id"]) for segment in segments}:
        _fail("OOS_SEGMENT_WITHOUT_OBSERVATIONS")
    return result


def _validated_replay_input(
    value: Mapping[str, object],
    *,
    preflight: Mapping[str, object],
    pit_declaration_sha256: str,
    holdout_start: str,
    holdout_end: str,
    require_input_sha256: bool = True,
) -> dict[str, object]:
    replay_input = _mapping(value, _INPUT_FIELDS, "INVALID_SYNTHETIC_REPLAY_INPUT")
    if (
        replay_input["schema_version"] != INPUT_SCHEMA_VERSION
        or replay_input["research_only"] is not True
        or replay_input["data_class"] != "synthetic_fixture"
        or replay_input["real_market_evidence"] is not False
    ):
        _fail("SYNTHETIC_FIXTURE_REQUIRED")
    verified_inputs = preflight["verified_inputs"]
    if not isinstance(verified_inputs, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    if _digest(replay_input["p1_input_sha256"], "REPLAY_P1_INPUT_DIGEST_MISMATCH") != verified_inputs[
        "p1_input_sha256"
    ]:
        _fail("REPLAY_P1_INPUT_DIGEST_MISMATCH")
    if _digest(replay_input["p2_candidate_sha256"], "REPLAY_P2_CANDIDATE_DIGEST_MISMATCH") != verified_inputs[
        "p2_candidate_sha256"
    ]:
        _fail("REPLAY_P2_CANDIDATE_DIGEST_MISMATCH")
    if _digest(
        replay_input["p3_input_verification_sha256"], "REPLAY_PREFLIGHT_DIGEST_MISMATCH"
    ) != preflight["verification_sha256"]:
        _fail("REPLAY_PREFLIGHT_DIGEST_MISMATCH")
    common_cutoff = _date(replay_input["common_cutoff"], "REPLAY_COMMON_CUTOFF_MISMATCH")
    if common_cutoff != verified_inputs["common_cutoff"]:
        _fail("REPLAY_COMMON_CUTOFF_MISMATCH")
    if _digest(replay_input["pit_declaration_sha256"], "PIT_DECLARATION_MISMATCH") != pit_declaration_sha256:
        _fail("PIT_DECLARATION_MISMATCH")
    if _digest(replay_input["cost_declaration_sha256"], "COST_DECLARATION_MISMATCH") != verified_inputs[
        "cost_declaration_sha256"
    ]:
        _fail("COST_DECLARATION_MISMATCH")
    p2_candidate = preflight.get("_p2_candidate")
    if not isinstance(p2_candidate, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    raw_legs = p2_candidate.get("legs")
    if not isinstance(raw_legs, list):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    leg_ids = frozenset(str(leg["leg_id"]) for leg in raw_legs)
    if len(leg_ids) != len(raw_legs):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    segments = _segments(
        replay_input["oos_segments"],
        holdout_start=holdout_start,
        holdout_end=holdout_end,
        common_cutoff=common_cutoff,
    )
    observations = _observations(
        replay_input["observations"],
        segments=segments,
        leg_ids=leg_ids,
        common_cutoff=common_cutoff,
    )
    normalized: dict[str, object] = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "research_only": True,
        "data_class": "synthetic_fixture",
        "real_market_evidence": False,
        "p1_input_sha256": replay_input["p1_input_sha256"],
        "p2_candidate_sha256": replay_input["p2_candidate_sha256"],
        "p3_input_verification_sha256": replay_input["p3_input_verification_sha256"],
        "common_cutoff": common_cutoff,
        "pit_declaration_sha256": replay_input["pit_declaration_sha256"],
        "cost_declaration_sha256": replay_input["cost_declaration_sha256"],
        "oos_segments": segments,
        "observations": observations,
        "input_sha256": _digest(replay_input["input_sha256"], "INVALID_SYNTHETIC_REPLAY_INPUT"),
    }
    if require_input_sha256 and normalized["input_sha256"] != _sha256(
        _without_input_sha256(normalized)
    ):
        _fail("SYNTHETIC_REPLAY_INPUT_DIGEST_MISMATCH")
    return normalized


def _preflight_with_p2(
    *, p1_input_binding: Mapping[str, object], p2_candidate: Mapping[str, object]
) -> dict[str, object] | None:
    preflight = _ready_preflight(p1_input_binding=p1_input_binding, p2_candidate=p2_candidate)
    if preflight is None:
        return None
    # The preflight summary intentionally retains no P2 legs.  The evaluator
    # needs the already-verified, caller-supplied descriptor only to apply its
    # frozen weights, so keep it private and never serialize it into a result.
    prepared = copy.deepcopy(preflight)
    prepared["_p2_candidate"] = copy.deepcopy(dict(p2_candidate))
    return prepared


def build_historical_combo_p3_synthetic_replay_input(
    *,
    p1_input_binding: Mapping[str, object],
    p2_candidate: Mapping[str, object],
    oos_segments: object,
    observations: object,
) -> dict[str, object]:
    """Build a self-hashed synthetic fixture bound to a READY P1/P2 preflight.

    This convenience builder makes no I/O and does not evaluate performance.
    It is deliberately unable to construct a fixture for an invalid or parked
    P1/P2 chain.
    """
    preflight = _preflight_with_p2(
        p1_input_binding=p1_input_binding, p2_candidate=p2_candidate
    )
    if preflight is None:
        raise HistoricalComboP3SyntheticReplayError("P3_INPUT_PREFLIGHT_PARKED")
    binding = validate_historical_combo_p1_input_binding(p1_input_binding)
    verified_inputs = preflight["verified_inputs"]
    if not isinstance(verified_inputs, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    raw_p2 = preflight["_p2_candidate"]
    if not isinstance(raw_p2, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    holdout = raw_p2.get("holdout_window")
    if not isinstance(holdout, Mapping):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    candidate: dict[str, object] = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "research_only": True,
        "data_class": "synthetic_fixture",
        "real_market_evidence": False,
        "p1_input_sha256": verified_inputs["p1_input_sha256"],
        "p2_candidate_sha256": verified_inputs["p2_candidate_sha256"],
        "p3_input_verification_sha256": preflight["verification_sha256"],
        "common_cutoff": verified_inputs["common_cutoff"],
        "pit_declaration_sha256": _p1_pit_declaration_sha256(binding),
        "cost_declaration_sha256": verified_inputs["cost_declaration_sha256"],
        "oos_segments": oos_segments,
        "observations": observations,
        "input_sha256": "",
    }
    # Normalize the injected fixture before hashing so the builder and direct
    # evaluator accept one exact, canonical representation only.
    normalized = _validated_replay_input(
        {**candidate, "input_sha256": "0" * 64},
        preflight=preflight,
        pit_declaration_sha256=_p1_pit_declaration_sha256(binding),
        holdout_start=str(holdout.get("start")),
        holdout_end=str(holdout.get("end")),
        require_input_sha256=False,
    )
    normalized["input_sha256"] = _sha256(_without_input_sha256(normalized))
    return _validated_replay_input(
        normalized,
        preflight=preflight,
        pit_declaration_sha256=_p1_pit_declaration_sha256(binding),
        holdout_start=str(holdout.get("start")),
        holdout_end=str(holdout.get("end")),
    )


def _maximum_drawdown(equity: Sequence[float]) -> float:
    peak = 1.0
    maximum_drawdown = 0.0
    for value in equity:
        peak = max(peak, value)
        maximum_drawdown = min(maximum_drawdown, value / peak - 1.0)
    return maximum_drawdown


def _segment_metrics(
    *,
    segment: Mapping[str, str],
    observations: Sequence[Mapping[str, object]],
    target_weights: Mapping[str, float],
    turnover_cost_bps: float,
) -> dict[str, object]:
    gross_equity = 1.0
    net_equity = 1.0
    net_equity_path = [net_equity]
    one_way_turnover = 0.0
    cost_rate_sum = 0.0
    for index, observation in enumerate(observations):
        raw_returns = observation["leg_returns"]
        if not isinstance(raw_returns, Mapping):
            _fail("INVALID_SYNTHETIC_OBSERVATIONS")
        gross_return = math.fsum(
            target_weights[leg_id] * float(raw_returns[leg_id]) for leg_id in sorted(target_weights)
        )
        if gross_return <= -1.0 or not math.isfinite(gross_return):
            _fail("INVALID_SYNTHETIC_RETURN_PATH")
        gross_equity *= 1.0 + gross_return
        cost_rate = 0.0
        if index < len(observations) - 1:
            post_return_weights = {
                leg_id: target_weights[leg_id] * (1.0 + float(raw_returns[leg_id])) / (1.0 + gross_return)
                for leg_id in target_weights
            }
            session_turnover = 0.5 * math.fsum(
                abs(target_weights[leg_id] - post_return_weights[leg_id]) for leg_id in target_weights
            )
            if not math.isfinite(session_turnover) or session_turnover < 0.0:
                _fail("INVALID_SYNTHETIC_RETURN_PATH")
            one_way_turnover += session_turnover
            cost_rate = session_turnover * turnover_cost_bps / 10_000.0
            cost_rate_sum += cost_rate
        net_equity *= (1.0 + gross_return) * (1.0 - cost_rate)
        if not math.isfinite(net_equity) or net_equity <= 0.0:
            _fail("INVALID_SYNTHETIC_RETURN_PATH")
        net_equity_path.append(net_equity)
    return {
        "segment_id": str(segment["segment_id"]),
        "start": str(segment["start"]),
        "end": str(segment["end"]),
        "observation_count": len(observations),
        "gross_total_return": gross_equity - 1.0,
        "net_total_return": net_equity - 1.0,
        "net_max_drawdown": _maximum_drawdown(net_equity_path),
        "one_way_rebalance_turnover": one_way_turnover,
        "applied_turnover_cost_rate": cost_rate_sum,
    }


def _cost_scenario_metrics(
    *,
    replay_input: Mapping[str, object],
    p2_candidate: Mapping[str, object],
    cost_declaration: Mapping[str, object],
) -> list[dict[str, object]]:
    raw_legs = p2_candidate.get("legs")
    if not isinstance(raw_legs, list):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    target_weights = {str(leg["leg_id"]): float(leg["target_weight"]) for leg in raw_legs}
    if not math.isclose(math.fsum(target_weights.values()), 1.0, rel_tol=0.0, abs_tol=_EPSILON):
        _fail("INVALID_P3_INPUT_VERIFICATION")
    segments = replay_input["oos_segments"]
    observations = replay_input["observations"]
    if not isinstance(segments, list) or not isinstance(observations, list):
        _fail("INVALID_SYNTHETIC_REPLAY_INPUT")
    observations_by_segment: dict[str, list[Mapping[str, object]]] = {
        str(segment["segment_id"]): [] for segment in segments
    }
    for observation in observations:
        session = str(observation["session"])
        segment_id = _segment_for_session(session, segments)
        if segment_id is None:
            _fail("SYNTHETIC_OBSERVATION_OUTSIDE_OOS")
        observations_by_segment[segment_id].append(observation)
    raw_costs = cost_declaration.get("turnover_cost_bps")
    if not isinstance(raw_costs, list):
        _fail("COST_DECLARATION_MISMATCH")
    scenarios: list[dict[str, object]] = []
    for raw_cost_bps in raw_costs:
        turnover_cost_bps = float(raw_cost_bps)
        segment_metrics = [
            _segment_metrics(
                segment=segment,
                observations=observations_by_segment[str(segment["segment_id"])],
                target_weights=target_weights,
                turnover_cost_bps=turnover_cost_bps,
            )
            for segment in segments
        ]
        scenarios.append(
            {
                "turnover_cost_bps": turnover_cost_bps,
                "borrow_cost_bps": float(cost_declaration["borrow_cost_bps"]),
                "cash_yield_assumption": float(cost_declaration["cash_yield_assumption"]),
                "cost_application": "LONG_ONLY_TARGET_REBALANCE_ONLY",
                "borrow_and_cash_application": "NOT_APPLIED_WITH_FULLY_INVESTED_LONG_ONLY_P2_WEIGHTS",
                "synthetic_segment_metrics": segment_metrics,
                "summary": {
                    "segment_count": len(segment_metrics),
                    "observation_count": sum(
                        int(metric["observation_count"]) for metric in segment_metrics
                    ),
                    "mean_segment_gross_total_return": math.fsum(
                        float(metric["gross_total_return"]) for metric in segment_metrics
                    )
                    / len(segment_metrics),
                    "mean_segment_net_total_return": math.fsum(
                        float(metric["net_total_return"]) for metric in segment_metrics
                    )
                    / len(segment_metrics),
                    "worst_segment_net_total_return": min(
                        float(metric["net_total_return"]) for metric in segment_metrics
                    ),
                    "worst_segment_net_max_drawdown": min(
                        float(metric["net_max_drawdown"]) for metric in segment_metrics
                    ),
                    "total_one_way_rebalance_turnover": math.fsum(
                        float(metric["one_way_rebalance_turnover"]) for metric in segment_metrics
                    ),
                    "total_applied_turnover_cost_rate": math.fsum(
                        float(metric["applied_turnover_cost_rate"]) for metric in segment_metrics
                    ),
                },
            }
        )
    return scenarios


def _parked(reason_codes: Sequence[str]) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "execution_authorized": False,
        "real_market_evidence": False,
        "status": PARKED_STATUS,
        "reason_codes": list(reason_codes),
        "result": None,
        "replay_sha256": None,
    }


def evaluate_historical_combo_p3_synthetic_replay(
    *,
    p1_input_binding: Mapping[str, object],
    p2_candidate: Mapping[str, object],
    replay_input: Mapping[str, object],
) -> dict[str, object]:
    """Evaluate a bound synthetic fixture; never return real P3 evidence.

    A parked result is the only response to any malformed input, identity
    drift, invalid PIT declaration, or future observation.  A completed
    synthetic result deliberately has no promotion verdict and is not accepted
    by ``historical_combo_p3_evidence_index``.
    """
    raw_preflight = verify_historical_combo_p3_inputs(
        p1_input_binding=p1_input_binding, p2_candidate=p2_candidate
    )
    if raw_preflight["status"] == PREFLIGHT_PARKED_STATUS:
        reason_codes = raw_preflight.get("reason_codes")
        return _parked(
            [str(reason) for reason in reason_codes]
            if isinstance(reason_codes, list) and reason_codes
            else ["P3_INPUT_PREFLIGHT_PARKED"]
        )
    try:
        preflight = _preflight_with_p2(
            p1_input_binding=p1_input_binding, p2_candidate=p2_candidate
        )
        if preflight is None:
            return _parked(["P3_INPUT_PREFLIGHT_PARKED"])
        binding = validate_historical_combo_p1_input_binding(p1_input_binding)
        raw_p2 = preflight["_p2_candidate"]
        if not isinstance(raw_p2, Mapping):
            _fail("INVALID_P3_INPUT_VERIFICATION")
        holdout = raw_p2.get("holdout_window")
        if not isinstance(holdout, Mapping):
            _fail("INVALID_P3_INPUT_VERIFICATION")
        normalized_input = _validated_replay_input(
            replay_input,
            preflight=preflight,
            pit_declaration_sha256=_p1_pit_declaration_sha256(binding),
            holdout_start=str(holdout.get("start")),
            holdout_end=str(holdout.get("end")),
        )
        cost_declaration = binding["cost_declaration"]
        if not isinstance(cost_declaration, Mapping):
            _fail("INVALID_P1_INPUT_BINDING")
        scenarios = _cost_scenario_metrics(
            replay_input=normalized_input,
            p2_candidate=raw_p2,
            cost_declaration=cost_declaration,
        )
        verified_inputs = preflight["verified_inputs"]
        if not isinstance(verified_inputs, Mapping):
            _fail("INVALID_P3_INPUT_VERIFICATION")
        result: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "research_only": True,
            "execution_authorized": False,
            "real_market_evidence": False,
            "status": COMPLETE_STATUS,
            "reason_codes": [],
            "result": {
                "candidate": copy.deepcopy(verified_inputs["candidate"]),
                "common_cutoff": verified_inputs["common_cutoff"],
                "p1_input_sha256": verified_inputs["p1_input_sha256"],
                "p2_candidate_sha256": verified_inputs["p2_candidate_sha256"],
                "p3_input_verification_sha256": preflight["verification_sha256"],
                "synthetic_replay_input_sha256": normalized_input["input_sha256"],
                "cost_scenarios": scenarios,
                "promotion_recommendation": None,
                "paper_authorized": False,
                "shadow_authorized": False,
                "live_authorized": False,
            },
            "replay_sha256": "",
        }
        result["replay_sha256"] = _sha256(
            {key: value for key, value in result.items() if key != "replay_sha256"}
        )
        return result
    except HistoricalComboP3SyntheticReplayError as exc:
        return _parked([exc.reason_code])
    except (TypeError, ValueError):
        return _parked(["INVALID_SYNTHETIC_REPLAY_INPUT"])


__all__ = [
    "COMPLETE_STATUS",
    "INPUT_SCHEMA_VERSION",
    "PARKED_STATUS",
    "SCHEMA_VERSION",
    "HistoricalComboP3SyntheticReplayError",
    "build_historical_combo_p3_synthetic_replay_input",
    "evaluate_historical_combo_p3_synthetic_replay",
]
