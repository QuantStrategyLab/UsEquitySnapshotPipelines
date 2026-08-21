from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-p3-replay-recovery.yml")


def test_recovery_workflow_is_scheduled_once_only_and_nonlive() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "cron: '5 5 * * 2-6'" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "group: tqqq-p3-replay-recovery-v5" in workflow
    assert "cancel-in-progress: false" in workflow
    assert workflow.count("environment: tqqq-p1-p3-nonlive") == 2
    assert "RUNTIME_REPLAY_RECOVERY" in workflow
    assert "recovery_attempt_limit" in workflow
    assert "p3-recovery-record.v1.json" in workflow
    assert "--no-clobber" in workflow
    recover_job = workflow.split("  recover:", maxsplit=1)[1]
    assert "path: p3-source" in recover_job
    assert recover_job.count("working-directory: p3-source") == 4


def test_recovery_workflow_reuses_only_the_verified_p1_root_without_provider_or_execution() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "plan_tqqq_p3_recovery.py" in workflow
    assert "build_tqqq_p3_recovery_record.py" in workflow
    assert "verify_tqqq_core_only_input_root" in workflow
    assert "--config config/tqqq_core_only_p2_v5.json" in workflow
    assert "ALPACA_API_KEY_ID" not in workflow
    assert "ALPACA_API_SECRET_KEY" not in workflow
    assert "workflow_dispatch:" not in workflow
    assert "gcloud storage rm" not in workflow
    assert "forward-observations" not in workflow
    assert "broker" not in workflow.lower()
    assert "placeorder" not in workflow.lower()
