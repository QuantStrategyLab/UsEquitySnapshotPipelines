from __future__ import annotations

from datetime import date

import pytest

from us_equity_snapshot_pipelines.lifecycle.levered_strategy_forward_confirmation import (
    LeveredStrategyForwardConfirmationError,
    build_completed_forward_confirmation_receipt,
    build_pending_forward_confirmation_state,
)


def _sessions(count: int = 3) -> tuple[date, ...]:
    return (date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31))[:count]


def _arguments(*, observed_sessions: tuple[date, ...]) -> dict[str, object]:
    return {
        "candidate_id": "tqqq_core_only_p2_v7_relative_benchmark",
        "config_sha256": "a" * 64,
        "p3_evidence_sha256": "b" * 64,
        "relative_benchmark_policy_sha256": "c" * 64,
        "p3_cutoff": date(2026, 8, 26),
        "expected_sessions": _sessions(),
        "observed_sessions": observed_sessions,
        "minimum_new_sessions": 3,
    }


def test_pending_forward_state_retains_only_hashes_and_cannot_promote() -> None:
    state = build_pending_forward_confirmation_state(**_arguments(observed_sessions=_sessions(1)))

    assert state["status"] == "PENDING_FORWARD_CONFIRMATION"
    assert state["remaining_session_count"] == 2
    assert state["strategy_verdict"] == "PASS_PENDING_FORWARD_CONFIRMATION"
    assert state["automatic_promotion"] is False
    assert "expected_sessions" not in state
    assert "observed_sessions" not in state


def test_completed_forward_receipt_needs_exact_calendar_sequence_and_human_promotion() -> None:
    receipt = build_completed_forward_confirmation_receipt(
        **_arguments(observed_sessions=_sessions()),
        forward_evidence_sha256="d" * 64,
        forward_drawdown_gates=[
            {"max_drawdown_not_exceeding_benchmark": True, "passed": True},
            {"max_drawdown_not_exceeding_benchmark": True, "passed": True},
        ],
    )

    assert receipt["status"] == "FORWARD_CONFIRMATION_COMPLETE"
    assert receipt["strategy_verdict"] == "PASS_REQUIRES_SEPARATE_HUMAN_PROMOTION"
    assert receipt["promotion_eligible"] is False
    assert receipt["live_ready"] is False


def test_forward_receipt_rejects_noncontiguous_or_out_of_order_observations() -> None:
    with pytest.raises(LeveredStrategyForwardConfirmationError):
        build_pending_forward_confirmation_state(
            **_arguments(observed_sessions=(_sessions()[1],))
        )
