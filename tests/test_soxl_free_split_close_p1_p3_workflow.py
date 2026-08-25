from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "soxl-free-split-close-p1-p3-research.yml"
V5_WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "soxl-v5-longterm-drawdown-p1-p3-research.yml"


def test_v4_workflow_is_manual_market_data_research_without_broker_or_raw_artifact_upload() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "run_soxl_core_only_free_split_close_p3_evidence.py" in workflow
    assert "SOXL_V4_P1_TERMINAL=" in workflow
    assert "SOXL_V4_P3_REVIEW=" in workflow
    assert "metrics_fields" in workflow
    assert "actions/upload-artifact" not in workflow
    assert "google-github-actions" not in workflow
    assert "gcloud " not in workflow
    assert "broker_order" not in workflow
    assert "id-token: write" not in workflow
    assert "${RUNNER_TEMP}/soxl-free-split-close-v4" in workflow


def test_v5_workflow_is_manual_market_data_research_with_only_sanitized_relative_evidence() -> None:
    workflow = V5_WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "P2_V5_LONGTERM_DRAWDOWN_CONTRACT" in workflow
    assert "--p2-profile v5_longterm_drawdown" in workflow
    assert "longterm_compounding_gate" in workflow
    assert "SOXL_V5_P3_REVIEW=" in workflow
    assert "actions/upload-artifact" not in workflow
    assert "google-github-actions" not in workflow
    assert "gcloud " not in workflow
    assert "broker_order" not in workflow
    assert "id-token: write" not in workflow
