from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/twelve-data-daily-access-diagnostic.yml")


def test_twelve_data_diagnostic_is_manual_nonlive_and_does_not_persist_market_data() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "pull_request:" not in workflow
    assert "environment: tqqq-p1-p3-nonlive" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" not in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "DATE_CUTOFF: ${{ inputs.date_cutoff }}" in workflow
    assert "scripts/diagnose_twelve_data_daily.py" in workflow
    assert "upload-artifact" not in workflow
    assert "gcloud" not in workflow
    assert "placeorder" not in workflow.lower()
    assert "broker" not in workflow.lower()
