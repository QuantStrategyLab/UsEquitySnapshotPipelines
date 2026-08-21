from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.daily_research_schedule_watchdog import (
    DailyResearchScheduleWatchdogError,
    WATCHDOG_OBSERVED,
    WATCHDOG_PARKED,
    assess_scheduled_research_workflow,
    build_daily_research_schedule_watchdog_summary,
)


EXPECTED_DATE = "2026-08-21"


def _response(*, status: str = "completed", conclusion: str | None = "success") -> dict[str, object]:
    return {
        "workflow_runs": [
            {
                "id": 123,
                "event": "schedule",
                "created_at": "2026-08-21T02:36:00Z",
                "status": status,
                "conclusion": conclusion,
            }
        ]
    }


def test_successful_scheduled_run_is_observed() -> None:
    record = assess_scheduled_research_workflow(
        workflow_id="tqqq-p1-p3-daily-research",
        workflow_runs_response=_response(),
        expected_utc_date=EXPECTED_DATE,
    )

    assert record["status"] == WATCHDOG_OBSERVED
    assert record["reason_code"] == "SCHEDULED_RUN_SUCCEEDED"
    assert record["run"] == {
        "run_id": 123,
        "created_at": "2026-08-21T02:36:00Z",
        "status": "completed",
        "conclusion": "success",
    }


def test_missing_or_unsuccessful_run_is_parked() -> None:
    missing = assess_scheduled_research_workflow(
        workflow_id="tqqq-p1-p3-daily-research",
        workflow_runs_response={"workflow_runs": []},
        expected_utc_date=EXPECTED_DATE,
    )
    failed = assess_scheduled_research_workflow(
        workflow_id="soxl-p1-p3-daily-research",
        workflow_runs_response=_response(conclusion="failure"),
        expected_utc_date=EXPECTED_DATE,
    )

    assert (missing["status"], missing["reason_code"]) == (
        WATCHDOG_PARKED,
        "SCHEDULED_RUN_MISSING",
    )
    assert (failed["status"], failed["reason_code"]) == (
        WATCHDOG_PARKED,
        "SCHEDULED_RUN_NOT_SUCCESSFUL",
    )


def test_nonterminal_and_wrong_day_runs_do_not_pass() -> None:
    pending = assess_scheduled_research_workflow(
        workflow_id="tqqq-p1-p3-daily-research",
        workflow_runs_response=_response(status="in_progress", conclusion=None),
        expected_utc_date=EXPECTED_DATE,
    )
    previous_day = _response()
    previous_day["workflow_runs"][0]["created_at"] = "2026-08-20T02:36:00Z"  # type: ignore[index]
    stale = assess_scheduled_research_workflow(
        workflow_id="soxl-p1-p3-daily-research",
        workflow_runs_response=previous_day,
        expected_utc_date=EXPECTED_DATE,
    )

    assert pending["reason_code"] == "SCHEDULED_RUN_NOT_TERMINAL"
    assert stale["reason_code"] == "SCHEDULED_RUN_MISSING"


def test_summary_requires_both_workflows() -> None:
    summary = build_daily_research_schedule_watchdog_summary(
        expected_utc_date=EXPECTED_DATE,
        tqqq_workflow_runs_response=_response(),
        soxl_workflow_runs_response={"workflow_runs": []},
    )

    assert summary["status"] == WATCHDOG_PARKED
    assert summary["reason_codes"] == ["SCHEDULED_RUN_MISSING"]
    assert len(summary["workflows"]) == 2


def test_bad_github_response_is_rejected() -> None:
    try:
        assess_scheduled_research_workflow(
            workflow_id="tqqq-p1-p3-daily-research",
            workflow_runs_response={"workflow_runs": [{"event": "schedule"}]},
            expected_utc_date=EXPECTED_DATE,
        )
    except DailyResearchScheduleWatchdogError as exc:
        assert str(exc) == "invalid_created_at"
    else:  # pragma: no cover
        raise AssertionError("malformed Actions response must not pass")


def test_cli_never_exposes_input_error(tmp_path) -> None:
    tqqq_path = tmp_path / "tqqq.json"
    soxl_path = tmp_path / "soxl.json"
    tqqq_path.write_text(json.dumps(_response()), encoding="utf-8")
    soxl_path.write_text("{", encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(Path("scripts/check_daily_research_schedule_watchdog.py")),
            "--expected-utc-date",
            EXPECTED_DATE,
            "--tqqq-workflow-runs",
            str(tqqq_path),
            "--soxl-workflow-runs",
            str(soxl_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert json.loads(result.stdout) == {
        "reason_codes": ["WATCHDOG_INPUT_INVALID"],
        "schema_version": "qsl.daily-research-schedule-watchdog.v1",
        "status": "PARKED",
    }
