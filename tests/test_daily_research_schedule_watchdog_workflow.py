from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/daily-research-schedule-watchdog.yml")


def test_watchdog_is_read_only_scheduled_and_cannot_trigger_research() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "cron: '20 4 * * 2-6'" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "actions: read" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" not in workflow
    assert "gh api" in workflow
    assert "check_daily_research_schedule_watchdog.py" in workflow
    assert "TQQQ Daily P1-P3 Research" not in workflow
    assert "SOXL Daily P1-P3 Research" not in workflow
    assert "workflow run" not in workflow.lower()
    assert "ALPACA_API" not in workflow
    assert "gcloud" not in workflow.lower()
    assert "broker" not in workflow.lower()
