from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/soxl-p1-p3-daily-research.yml")


def test_soxl_daily_research_is_scheduled_nonlive_p1_p3_only() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "cron: '45 2 * * 2-6'" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "pull_request:" not in workflow
    assert "group: soxl-p1-p3-daily-research-v3" in workflow
    assert "cancel-in-progress: false" in workflow
    assert workflow.count("environment: market-data-nonlive") == 3
    assert "config/soxl_soxx_core_only_p2_v3.json" in workflow
    assert "P2_V3_CONTRACT" in workflow
    assert "placeorder" not in workflow.lower()
    assert "workflow_run:" not in workflow


def test_soxl_daily_research_defers_unavailable_p1_and_requires_remote_completion() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "P1InputUnavailableError as exc" in workflow
    assert "status = 'DEFERRED'" in workflow
    assert "reason_code = exc.reason_code" in workflow
    assert "provider_retry_state" in workflow
    assert "SOXL_P1_PROVIDER_RETRY_STATE" in workflow
    assert "steps.acquire.outputs.status == 'ACCEPTED'" in workflow
    assert "build_soxl_core_only_p1_remote_completion" in workflow
    assert "p1-complete.json" in workflow
    assert "verify_soxl_core_only_p1_remote_completion" in workflow
    assert "gcloud storage cp --quiet --no-clobber" in workflow
    assert "Build optional verified decision-data projection" in workflow
    assert "extract_soxl_bars_daily_series" in workflow
    assert 'strategy_scope="soxl_soxx_trend_income"' in workflow
    assert "decision-data-projections/v1/us_equity/soxl_soxx_trend_income/${PROJECTION_MANIFEST_SHA256}" in workflow
    assert '"$root/decision-price-series.json" "${destination}/decision-price-series.json"' in workflow
    assert "DECISION_DATA_PROJECTION_STATUS=PARKED" in workflow
    assert "decision_projection_terminal" in workflow
    assert "DECISION_DATA_PROJECTION_TERMINAL_STATUS" in workflow
    assert "--decision-projection-status" in workflow
    assert "strategy_performance.v2.json" in workflow
    assert "build_soxl_p3_strategy_performance.py" in workflow
    assert "actions/upload-artifact@v7" in workflow
    assert "retention-days: 35" in workflow
    p3_job = workflow.split("  p3:", maxsplit=1)[1]
    assert "ALPACA_API_KEY_ID" not in p3_job
    assert "ALPACA_API_SECRET_KEY" not in p3_job
    publisher_job = workflow.split("  publish-control-plane:", maxsplit=1)[1]
    assert "build_soxl_daily_control_plane_source_snapshot.py" in publisher_job
    assert "QSL_CONTROL_PLANE_SYNC_URL" in publisher_job
    assert "CONTROL_PLANE_SYNC_TOKEN" in publisher_job
    assert "--data-binary \"@$output_path\"" in publisher_job
    assert "ALPACA_API_KEY_ID" not in publisher_job
    assert "ALPACA_API_SECRET_KEY" not in publisher_job
    assert "gcloud storage" not in publisher_job
    assert "id-token: write" not in publisher_job
    assert "broker" not in publisher_job.lower()
    assert "placeorder" not in publisher_job.lower()
