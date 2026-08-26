"""Generic, non-promoting forward-confirmation receipts for levered candidates.

The caller supplies a candidate-bound expected exchange-session sequence from
its immutable P1/P3 calendar adapter.  This core verifies that observations
are an exact prefix (or complete match), retains only hashes and counts, and
can never authorize an order or an automatic promotion.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_PENDING_VERDICT = "PASS_PENDING_FORWARD_CONFIRMATION"
_HUMAN_VERDICT = "PASS_REQUIRES_SEPARATE_HUMAN_PROMOTION"
_REJECT_VERDICT = "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
_FORWARD_GATE_FIELDS = frozenset({"max_drawdown_not_exceeding_benchmark", "passed"})


class LeveredStrategyForwardConfirmationError(ValueError):
    """Fail closed without retaining raw market data or runtime credentials."""


def _fail() -> None:
    raise LeveredStrategyForwardConfirmationError("invalid forward-confirmation receipt input")


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _sha256(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail()
    return value


def _candidate(candidate_id: object, config_sha256: object) -> dict[str, str]:
    if (
        not isinstance(candidate_id, str)
        or not 8 <= len(candidate_id) <= 128
        or not candidate_id.isascii()
        or candidate_id.lower() != candidate_id
        or not all(char.isalnum() or char == "_" for char in candidate_id)
    ):
        _fail()
    return {"candidate_id": candidate_id, "config_sha256": _sha256(config_sha256)}


def _sessions(value: object) -> tuple[date, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail()
    sessions = tuple(value)
    if not sessions or any(type(session) is not date for session in sessions):
        _fail()
    if sessions != tuple(sorted(set(sessions))):
        _fail()
    return sessions


def _gate(value: object) -> dict[str, bool]:
    if not isinstance(value, Mapping) or set(value) != _FORWARD_GATE_FIELDS:
        _fail()
    drawdown = value["max_drawdown_not_exceeding_benchmark"]
    passed = value["passed"]
    if type(drawdown) is not bool or type(passed) is not bool or passed != drawdown:
        _fail()
    return {
        "max_drawdown_not_exceeding_benchmark": drawdown,
        "passed": passed,
    }


def _base(
    *,
    candidate_id: object,
    config_sha256: object,
    p3_evidence_sha256: object,
    relative_benchmark_policy_sha256: object,
    p3_cutoff: object,
    expected_sessions: object,
    observed_sessions: object,
    minimum_new_sessions: object,
) -> tuple[dict[str, object], tuple[date, ...], tuple[date, ...]]:
    candidate = _candidate(candidate_id, config_sha256)
    if type(p3_cutoff) is not date or type(minimum_new_sessions) is not int or minimum_new_sessions < 1:
        _fail()
    expected = _sessions(expected_sessions)
    observed = tuple(observed_sessions) if isinstance(observed_sessions, Sequence) and not isinstance(observed_sessions, (str, bytes)) else None
    if observed is None or any(type(session) is not date for session in observed):
        _fail()
    if len(expected) != minimum_new_sessions or expected[0] <= p3_cutoff or observed != expected[: len(observed)]:
        _fail()
    return (
        {
            "schema_version": "qsl.levered-strategy-forward-confirmation.v1",
            "candidate": candidate,
            "p3_evidence_sha256": _sha256(p3_evidence_sha256),
            "relative_benchmark_policy_sha256": _sha256(relative_benchmark_policy_sha256),
            "p3_cutoff": p3_cutoff.isoformat(),
            "calendar": "XNYS",
            "minimum_new_xnys_sessions": minimum_new_sessions,
            "expected_sessions_sha256": _digest([session.isoformat() for session in expected]),
            "observed_sessions_sha256": _digest([session.isoformat() for session in observed]),
            "observed_session_count": len(observed),
            "automatic_promotion": False,
            "promotion_eligible": False,
            "live_ready": False,
            "no_order": True,
            "size_zero_required": True,
        },
        expected,
        observed,
    )


def build_pending_forward_confirmation_state(
    *,
    candidate_id: object,
    config_sha256: object,
    p3_evidence_sha256: object,
    relative_benchmark_policy_sha256: object,
    p3_cutoff: object,
    expected_sessions: object,
    observed_sessions: object,
    minimum_new_sessions: object,
) -> dict[str, object]:
    """Report a bounded pending state from an exact observed-session prefix."""
    result, expected, observed = _base(
        candidate_id=candidate_id,
        config_sha256=config_sha256,
        p3_evidence_sha256=p3_evidence_sha256,
        relative_benchmark_policy_sha256=relative_benchmark_policy_sha256,
        p3_cutoff=p3_cutoff,
        expected_sessions=expected_sessions,
        observed_sessions=observed_sessions,
        minimum_new_sessions=minimum_new_sessions,
    )
    if len(observed) >= len(expected):
        _fail()
    return {
        **result,
        "status": "PENDING_FORWARD_CONFIRMATION",
        "remaining_session_count": len(expected) - len(observed),
        "strategy_verdict": _PENDING_VERDICT,
    }


def build_completed_forward_confirmation_receipt(
    *,
    candidate_id: object,
    config_sha256: object,
    p3_evidence_sha256: object,
    relative_benchmark_policy_sha256: object,
    p3_cutoff: object,
    expected_sessions: object,
    observed_sessions: object,
    minimum_new_sessions: object,
    forward_evidence_sha256: object,
    forward_drawdown_gates: object,
) -> dict[str, object]:
    """Bind a complete future window and retain human-only promotion control."""
    result, expected, observed = _base(
        candidate_id=candidate_id,
        config_sha256=config_sha256,
        p3_evidence_sha256=p3_evidence_sha256,
        relative_benchmark_policy_sha256=relative_benchmark_policy_sha256,
        p3_cutoff=p3_cutoff,
        expected_sessions=expected_sessions,
        observed_sessions=observed_sessions,
        minimum_new_sessions=minimum_new_sessions,
    )
    if observed != expected or not isinstance(forward_drawdown_gates, Sequence) or isinstance(forward_drawdown_gates, (str, bytes)):
        _fail()
    gates = [_gate(gate) for gate in forward_drawdown_gates]
    if not gates:
        _fail()
    all_drawdown_passed = all(gate["passed"] for gate in gates)
    return {
        **result,
        "status": "FORWARD_CONFIRMATION_COMPLETE",
        "forward_evidence_sha256": _sha256(forward_evidence_sha256),
        "forward_drawdown_all_passed": all_drawdown_passed,
        "strategy_verdict": _HUMAN_VERDICT if all_drawdown_passed else _REJECT_VERDICT,
        "automatic_promotion": False,
        "promotion_eligible": False,
        "live_ready": False,
    }


__all__ = [
    "LeveredStrategyForwardConfirmationError",
    "build_completed_forward_confirmation_receipt",
    "build_pending_forward_confirmation_state",
]
