from __future__ import annotations

import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import P2_V5_CONTRACT
from us_equity_snapshot_pipelines.lifecycle import tqqq_p3_recovery as recovery


def _daily_status(*, terminal: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": recovery.DAILY_STATUS_SCHEMA,
        "candidate": {
            "candidate_id": P2_V5_CONTRACT.candidate_id,
            "config_sha256": P2_V5_CONTRACT.config_sha256,
        },
        "date_cutoff": "2026-08-18",
        "input_manifest_sha256": "a" * 64,
        "p1_health_sha256": "b" * 64,
        "p3_terminal": terminal,
    }


def _parked_terminal(
    *, failure_class: str = "runtime_internal_failure", replay_started: bool = True
) -> dict[str, object]:
    stages = {
        "input_validation_failure": "input_validation",
        "config_contract_failure": "config_contract",
        "orchestrator_contract_failure": "orchestrator_contract",
        "risk_contract_failure": "risk_contract",
        "evidence_validation_failure": "evidence_validation",
        "runtime_internal_failure": "runtime_internal",
    }
    return {
        "complete_evidence": False,
        "failure_class": failure_class,
        "replay_started": replay_started,
        "source_commit": "c" * 40,
        "stage": stages[failure_class],
        "status": "PARKED",
    }


def _completed_terminal() -> dict[str, object]:
    return {
        "evidence_sha256": "d" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
    }


def test_runtime_failure_after_replay_gets_exactly_one_recovery_plan() -> None:
    status = _daily_status(terminal=_parked_terminal())

    plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=status, recovery_record_exists=False
    )

    assert plan["schema_version"] == recovery.RECOVERY_PLAN_SCHEMA
    assert plan["status"] == "REPLAY_ONCE"
    assert plan["reason_code"] == "RUNTIME_REPLAY_RECOVERY"
    assert plan["recovery_attempt_limit"] == 1
    assert plan["input_manifest_sha256"] == "a" * 64
    expected = hashlib.sha256(
        json.dumps(status, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    assert plan["daily_status_sha256"] == expected


@pytest.mark.parametrize(
    ("terminal", "reason"),
    [
        (_parked_terminal(failure_class="evidence_validation_failure"), "P3_FAILURE_NOT_RETRIABLE"),
        (_parked_terminal(replay_started=False), "P3_FAILURE_NOT_RETRIABLE"),
        (_completed_terminal(), "P3_ALREADY_COMPLETE"),
    ],
)
def test_only_a_started_runtime_failure_is_retriable(terminal: dict[str, object], reason: str) -> None:
    plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=_daily_status(terminal=terminal), recovery_record_exists=False
    )

    assert plan["status"] == "PARKED"
    assert plan["reason_code"] == reason


def test_existing_create_only_record_prevents_a_second_replay() -> None:
    plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=_daily_status(terminal=_parked_terminal()), recovery_record_exists=True
    )

    assert plan["status"] == "PARKED"
    assert plan["reason_code"] == "RECOVERY_ALREADY_RECORDED"


def test_recovery_record_binds_the_single_attempt_to_the_original_plan() -> None:
    plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=_daily_status(terminal=_parked_terminal()), recovery_record_exists=False
    )

    record = recovery.build_tqqq_p3_recovery_record(
        plan=plan, p3_terminal=_completed_terminal(), produced_at="2026-08-20T04:00:00Z"
    )

    assert record["schema_version"] == recovery.RECOVERY_RECORD_SCHEMA
    assert record["recovery_attempt"] == 1
    assert record["p3_terminal"] == _completed_terminal()
    assert recovery.validate_tqqq_p3_recovery_record(record) == record


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value["candidate"].update({"candidate_id": "other"}), "unexpected candidate"),
        (lambda value: value["p3_terminal"].update({"stage": "runtime"}), "invalid parked P3 terminal"),
        (lambda value: value.update({"p1_health_sha256": "not-a-digest"}), "invalid P1 health digest"),
    ],
)
def test_daily_status_contract_fails_closed(mutate, message: str) -> None:
    status = _daily_status(terminal=_parked_terminal())
    mutate(status)

    with pytest.raises(recovery.TqqqP3RecoveryError, match=message):
        recovery.build_tqqq_p3_recovery_plan(daily_research_status=status, recovery_record_exists=False)


def test_record_cannot_be_built_from_a_parked_plan_or_be_tampered() -> None:
    parked_plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=_daily_status(terminal=_completed_terminal()), recovery_record_exists=False
    )
    with pytest.raises(recovery.TqqqP3RecoveryError, match="does not permit replay"):
        recovery.build_tqqq_p3_recovery_record(
            plan=parked_plan, p3_terminal=_completed_terminal(), produced_at="2026-08-20T04:00:00Z"
        )

    plan = recovery.build_tqqq_p3_recovery_plan(
        daily_research_status=_daily_status(terminal=_parked_terminal()), recovery_record_exists=False
    )
    record = recovery.build_tqqq_p3_recovery_record(
        plan=plan, p3_terminal=_completed_terminal(), produced_at="2026-08-20T04:00:00Z"
    )
    record["input_manifest_sha256"] = "e" * 64
    with pytest.raises(recovery.TqqqP3RecoveryError, match="invalid P3 recovery record digest"):
        recovery.validate_tqqq_p3_recovery_record(record)
