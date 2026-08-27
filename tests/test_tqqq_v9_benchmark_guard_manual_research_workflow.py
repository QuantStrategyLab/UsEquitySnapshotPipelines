from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(
    ".github/workflows/tqqq-v9-benchmark-guard-p1-p3-manual-research.yml"
)


def test_v9_workflow_is_manual_candidate_bound_and_nonexecuting() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "tqqq_core_only_p2_v9_benchmark_drawdown_guard" in workflow
    assert "tqqq-v9-benchmark-guard-20260826" in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "ALPACA_API_KEY_ID" not in workflow
    assert "ALPACA_API_SECRET_KEY" not in workflow
    assert "contract=P2_V9_CONTRACT" in workflow
    assert "completed_sessions[-3]" in workflow
    assert "T+2 settlement point" in workflow
    assert "availability_diagnostic" in workflow
    assert "classify_tqqq_core_only_free_ohlcv_availability" in workflow
    assert "build_tqqq_p3_v9_evidence_index" in workflow
    assert "--no-clobber" in workflow
    assert "P4_P5_P6=NOT_AUTHORIZED" in workflow
    assert "broker_order" not in workflow
    assert "paper" not in workflow
    assert "shadow" not in workflow
    assert "live_enabled" not in workflow


def test_v9_workflow_never_uploads_raw_bars_to_github_artifacts() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    artifact_block = workflow.split("name: Upload sanitized terminal status only", 1)[1]

    assert "p1-status.json" in artifact_block
    assert "p3-result.json" in artifact_block
    assert "p3-status.json" in artifact_block
    assert "p3-evidence-index.json" in artifact_block
    assert "relative-benchmark-summary.json" in artifact_block
    assert '"$root/bars.json"' not in artifact_block
    assert '"$root/assurance.json"' not in artifact_block
