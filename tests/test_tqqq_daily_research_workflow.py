from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-p1-p3-daily-research.yml")
GITIGNORE = Path(".gitignore")


def test_ephemeral_google_auth_credentials_do_not_dirty_the_p3_checkout() -> None:
    gitignore = GITIGNORE.read_text(encoding="utf-8")

    assert "gha-creds-*.json" in gitignore


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
    assert "P1_REASON_CODE" in workflow
    assert "P1InputUnavailableError as exc" in workflow
    assert "MISSING_SESSIONS" in workflow
    assert "reason_code = exc.reason_code" in workflow
    assert "publish_tqqq_core_only_p1_inputs_for_contract" in workflow
    assert "verify_tqqq_core_only_input_root" in workflow
    assert "--config config/tqqq_core_only_p2_v5.json" in workflow
    assert "invalid sanitized daily P3 failure" in workflow
    assert "daily-research-status.json" in workflow
    assert "daily-health.json" in workflow
    assert "gcloud storage cp --quiet --no-clobber" in workflow
    assert "actions/upload-artifact@v7" in workflow
    assert "strategy_performance.v2.json" in workflow
    assert "build_tqqq_p3_strategy_performance.py" in workflow
    assert "Upload sanitized P3 research performance observation" in workflow
    assert "retention-days: 35" in workflow
    assert "Build a bound P5 forward observation" in workflow
    assert "build_tqqq_p5_forward_observation.py" in workflow
    assert "p5-forward-observation.v1.json" in workflow
    assert "tqqq-p1-p3/forward-observations/v1/${DATE_CUTOFF}.json" in workflow
    assert "Create-only upload of bound P5 forward observation" in workflow
    assert "P5_FORWARD_OBSERVATION_STATUS=RECORDED" in workflow
    assert "validate_tqqq_p5_forward_observation" in workflow
    assert "Build bounded P2 v6 plugin observation" in workflow
    assert "build_tqqq_p2_v6_daily_observation.py" in workflow
    assert "P2_V6_PLUGIN_OBSERVATION_STATUS=" in workflow
    assert "tqqq-p2-v6-plugin-observation-${{ github.run_id }}-${{ github.run_attempt }}" in workflow
    assert "0d5b48ce4f9dd56491d6a6b51fdf5b0aa4cb256c" in workflow
    assert "p2-v6-plugin-observation.v1.json" in workflow
    assert "invalid v6 plugin observation result" in workflow
    assert "invalid v6 plugin observation failure" in workflow
    assert '"$root/bars.json"' in workflow
    assert '"$destination"' in workflow
    p3_job = workflow.split("  p3:", maxsplit=1)[1]
    assert "path: p3-source" in p3_job
    assert p3_job.count("working-directory: p3-source") == 6
    assert "ALPACA_API_KEY_ID" not in p3_job
    assert "ALPACA_API_SECRET_KEY" not in p3_job
    assert "broker" not in p3_job.lower()
    assert "placeorder" not in p3_job.lower()
    publisher_job = workflow.split("  publish-control-plane:", maxsplit=1)[1]
    assert "ALPACA_API_KEY_ID" not in publisher_job
    assert "ALPACA_API_SECRET_KEY" not in publisher_job
    assert "gcloud storage" not in publisher_job
    assert "id-token: write" not in publisher_job
    assert "build_tqqq_daily_control_plane_source_snapshot.py" in publisher_job
    assert "--data-binary \"@$output_path\"" in publisher_job
    assert "broker" not in workflow.lower()
    assert "placeorder" not in workflow.lower()
