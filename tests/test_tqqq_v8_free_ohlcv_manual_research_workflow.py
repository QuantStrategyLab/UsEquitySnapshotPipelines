from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/tqqq-v8-free-ohlcv-p1-p3-manual-research.yml")


def test_v8_free_ohlcv_workflow_is_manual_nonexecution_and_does_not_use_alpaca() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "tqqq_core_only_p2_v8_free_ohlcv_relative_benchmark" in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "ALPACA_API_KEY_ID" not in workflow
    assert "ALPACA_API_SECRET_KEY" not in workflow
    assert "TwelveYahooOhlcvObserver" in workflow
    assert "Verify V8 scope record before provider access" in workflow
    assert "--no-clobber" in workflow
    assert "build_tqqq_p3_v8_evidence_index" in workflow
    assert "Create-only upload of sanitized V8 P3 metadata" in workflow
    assert "exit_code=0" in workflow
    assert "P4_P5_P6=NOT_AUTHORIZED" in workflow
    assert "broker_order" not in workflow
    assert "paper" not in workflow
    assert "shadow" not in workflow
    assert "live_enabled" not in workflow


def test_v8_workflow_never_uploads_raw_market_data_to_github_artifacts() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    artifact_block = workflow.split("name: Upload sanitized terminal status only", 1)[1]
    assert "p1-status.json" in artifact_block
    assert "p3-result.json" in artifact_block
    assert "p3-status.json" in artifact_block
    assert "p3-evidence-index.json" in artifact_block
    assert '"$root/bars.json"' not in artifact_block
    assert '"$root/assurance.json"' not in artifact_block
