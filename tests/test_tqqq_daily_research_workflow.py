from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-p1-p3-daily-research.yml")


def test_daily_research_workflow_is_scheduled_p2_v5_only_and_nonlive() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "cron: '35 2 * * 2-6'" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "pull_request:" not in workflow
    assert "group: tqqq-p1-p3-daily-research-v5" in workflow
    assert "cancel-in-progress: false" in workflow
    assert workflow.count("environment: tqqq-p1-p3-nonlive") == 3
    assert "config/tqqq_core_only_p2_v5.json" in workflow
    assert "P2_V5_CONTRACT" in workflow
    assert "P4_P5_P6=NOT_AUTHORIZED" in workflow
    assert "mandate_id:" not in workflow
    assert "verify_tqqq_p1_p3_mandate.py" not in workflow
    assert "workflow_run:" not in workflow
    assert "publish-control-plane:" in workflow
    assert "QSL_CONTROL_PLANE_SYNC_URL" in workflow
    assert "CONTROL_PLANE_SYNC_TOKEN" in workflow
    assert "/api/internal/sync-control-plane-source" in workflow


def test_daily_research_workflow_uses_bound_data_and_sanitized_status_only() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}" in workflow
    assert "ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}" in workflow
    assert "date_cutoff=cutoff" in workflow
    assert "assess_tqqq_core_only_p1_input_health" in workflow
    assert "build_tqqq_core_only_p1_input_unavailable_health" in workflow
    assert "publish_tqqq_core_only_p1_inputs_for_contract" in workflow
    assert "verify_tqqq_core_only_input_root" in workflow
    assert "--config config/tqqq_core_only_p2_v5.json" in workflow
    assert "invalid sanitized daily P3 failure" in workflow
    assert "daily-research-status.json" in workflow
    assert "daily-health.json" in workflow
    assert "gcloud storage cp --quiet --no-clobber" in workflow
    assert "actions/upload-artifact" not in workflow
    assert '"$root/bars.json"' in workflow
    assert '"$destination"' in workflow
    p3_job = workflow.split("  p3:", maxsplit=1)[1]
    assert "ALPACA_API_KEY_ID" not in p3_job
    assert "ALPACA_API_SECRET_KEY" not in p3_job
    publisher_job = workflow.split("  publish-control-plane:", maxsplit=1)[1]
    assert "ALPACA_API_KEY_ID" not in publisher_job
    assert "ALPACA_API_SECRET_KEY" not in publisher_job
    assert "gcloud storage" not in publisher_job
    assert "id-token: write" not in publisher_job
    assert "build_tqqq_daily_control_plane_source_snapshot.py" in publisher_job
    assert "--data-binary \"@$output_path\"" in publisher_job
    assert "broker" not in workflow.lower()
    assert "placeorder" not in workflow.lower()
