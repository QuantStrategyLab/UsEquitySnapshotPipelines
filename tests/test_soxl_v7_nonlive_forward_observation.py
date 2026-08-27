from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import exchange_calendars as xcals
import pandas as pd
import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p4_v7_forward_confirmation_contract import (
    P4_V7_FORWARD_CONFIRMATION_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation import (
    SoxlV7NonliveForwardObservationError,
    build_soxl_v7_nonlive_forward_inputs,
    build_soxl_v7_nonlive_forward_policy,
    build_soxl_v7_nonlive_forward_record,
)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _forward_dates(count: int) -> list[str]:
    calendar = xcals.get_calendar("XNYS")
    sessions = calendar.sessions_window(
        pd.Timestamp(P4_V7_FORWARD_CONFIRMATION_CONTRACT.first_forward_xnys_session),
        count,
    )
    return [pd.Timestamp(session).date().isoformat() for session in sessions]


def _materialized(count: int = 3) -> dict[str, object]:
    sessions = []
    for session in _forward_dates(count):
        sessions.append(
            {
                "as_of": f"{session}T00:00:00+00:00",
                "market_data": {
                    "derived_indicators": {
                        "SOXL": {"price": 10.0, "ma_trend": 9.0},
                        "SOXX": {
                            "price": 100.0,
                            "ma_trend": 95.0,
                            "ma20": 99.0,
                            "ma20_slope": 0.1,
                            "rsi14": 55.0,
                            "bb_upper": 110.0,
                            "realized_volatility_10": 0.3,
                            "realized_volatility_10_dynamic_threshold": 0.5,
                            "realized_volatility_10_dynamic_sample_count": 200.0,
                        },
                    }
                },
                "prices": {"SOXL": 10.0, "SOXX": 100.0, "BOXX": 100.0},
            }
        )
    return {
        "p1_identity": {"input_manifest_sha256": _digest("p1")},
        "p2_identity": {
            "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
            "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        },
        "sessions": sessions,
    }


def _record_once(*, count: int, previous=None, **changes: object) -> dict[str, object]:
    inputs = build_soxl_v7_nonlive_forward_inputs(_materialized(count))
    values: dict[str, object] = {
        "observed_at": "2026-08-31T02:45:00Z",
        "inputs": inputs,
        "shadow_observation_sha256": _digest("shadow"),
        "simulated_paper_observation_sha256": _digest("paper"),
        "previous_record": previous,
    }
    values.update(changes)
    return build_soxl_v7_nonlive_forward_record(**values)  # type: ignore[arg-type]


def _record(*, count: int = 1, previous=None, **changes: object) -> dict[str, object]:
    if previous is not None:
        return _record_once(count=count, previous=previous, **changes)
    record = None
    for session_count in range(1, count + 1):
        record = _record_once(
            count=session_count,
            previous=record,
            **(changes if session_count == count else {}),
        )
    assert isinstance(record, dict)
    return record


def test_v7_inputs_keep_exact_candidate_and_simulated_paper_is_no_broker() -> None:
    inputs = build_soxl_v7_nonlive_forward_inputs(_materialized())

    assert inputs.observation_sessions == tuple(_forward_dates(3))
    assert inputs.shadow_source_context["portfolio"]["positions"] == []
    assert inputs.simulated_paper_replay_input["cost_bps"] == 10.0
    assert len(inputs.simulated_paper_replay_input["sessions"]) == 3


def test_v7_record_starts_both_nonlive_modes_without_live_authority() -> None:
    record = _record()
    controller = record["controller"]

    assert controller["state"] == "FORWARD_ACTIVE"
    assert controller["non_live_actions"] == ["start_shadow", "start_paper"]
    assert controller["live_action"] == "human_approval_required"
    assert record["no_order"] is True
    assert record["broker_dependency"] is False
    assert record["live_authority_granted"] is False
    receipt = record["forward_observation_receipt"]
    assert receipt["observation_index"] == 1
    assert receipt["evidence_modes"] == ["shadow_decision", "simulated_replay"]


def test_v7_record_pauses_nonlive_observation_for_transient_data_or_mode_failure() -> None:
    record = _record(
        data_status="stale",
        shadow_status="unavailable",
        paper_status="unavailable",
        shadow_observation_sha256=None,
        simulated_paper_observation_sha256=None,
    )

    assert record["controller"]["state"] == "PAUSED"
    assert record["controller"]["non_live_actions"] == ["pause_shadow", "pause_paper"]


def test_v7_nonlive_pause_recovers_after_three_healthy_sessions() -> None:
    paused = _record(
        count=1,
        data_status="stale",
        shadow_status="unavailable",
        paper_status="unavailable",
        shadow_observation_sha256=None,
        simulated_paper_observation_sha256=None,
    )
    first = _record(count=2, previous=paused)
    second = _record(count=3, previous=first)
    resumed = _record(count=4, previous=second)

    assert first["controller"]["state"] == "PAUSED"
    assert second["controller"]["state"] == "PAUSED"
    assert resumed["controller"]["state"] == "FORWARD_ACTIVE"
    assert resumed["controller"]["non_live_actions"] == ["resume_shadow", "resume_paper"]


def test_v7_risk_block_never_auto_resumes() -> None:
    blocked = _record(count=1, risk_status="blocked")
    still_blocked = _record(count=2, previous=blocked)

    assert blocked["controller"]["state"] == "RISK_BLOCKED"
    assert still_blocked["controller"]["state"] == "RISK_BLOCKED"
    assert still_blocked["controller"]["non_live_actions"] == [
        "keep_shadow_stopped",
        "keep_paper_stopped",
    ]


def test_v7_manual_hold_never_auto_resumes() -> None:
    held = _record(count=1, control_status="manual_hold")
    still_held = _record(count=2, previous=held)

    assert held["controller"]["state"] == "MANUAL_HOLD"
    assert still_held["controller"]["state"] == "MANUAL_HOLD"
    assert still_held["controller"]["non_live_actions"] == [
        "keep_shadow_stopped",
        "keep_paper_stopped",
    ]


def test_v7_receipt_chain_binds_and_verifies_its_predecessor() -> None:
    first = _record(count=1)
    second = _record(count=2, previous=first)

    first_receipt = first["forward_observation_receipt"]
    second_receipt = second["forward_observation_receipt"]
    assert second_receipt["previous_receipt_sha256"] == first_receipt["receipt_sha256"]

    tampered = deepcopy(first)
    tampered["forward_observation_receipt"]["receipt_sha256"] = "0" * 64
    with pytest.raises(SoxlV7NonliveForwardObservationError):
        _record(count=2, previous=tampered)


def test_full_v7_window_requires_human_live_review() -> None:
    record = _record(count=252)
    controller = record["controller"]

    assert build_soxl_v7_nonlive_forward_policy().required_trading_sessions == 252
    assert controller["state"] == "FORWARD_COMPLETE_HUMAN_REVIEW"
    assert controller["non_live_actions"] == ["keep_shadow_stopped", "keep_paper_stopped"]
    assert controller["live_authority_granted"] is False


def test_scheduled_observer_is_create_only_and_has_no_execution_target() -> None:
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/soxl-v7-nonlive-forward-observation.yml"
    ).read_text(encoding="utf-8")
    runner = (
        Path(__file__).resolve().parents[1]
        / "scripts/run_soxl_v7_nonlive_forward_observation.py"
    ).read_text(encoding="utf-8")

    assert 'cron: "45 2 * * 2-6"' in workflow
    assert "contents: read" in workflow
    assert "id-token: write" in workflow
    assert 'uv run --no-sync python - <<\'PY\' >> "$GITHUB_OUTPUT"' in workflow
    assert "gcloud storage cp --quiet --no-clobber" in workflow
    assert "history_available" in workflow
    assert "PREVIOUS_RECEIPT_UNAVAILABLE" in workflow
    assert "PARKED_PREVIOUS_RECEIPT_UNAVAILABLE" in workflow
    assert "history_available == 'true'" in workflow
    assert "run_soxl_v7_nonlive_forward_observation.py" in workflow
    assert "gcloud run" not in workflow.lower()
    assert "runtime_target_json" not in workflow.lower()
    assert "broker" not in workflow.lower()
    assert 'args.output.open("x"' in runner
    assert "runtime_target" not in runner.lower()
    assert "submit_order" not in runner.lower()
    assert "place_order" not in runner.lower()
