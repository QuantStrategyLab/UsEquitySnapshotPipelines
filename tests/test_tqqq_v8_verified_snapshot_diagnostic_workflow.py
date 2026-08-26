from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/tqqq-v8-verified-snapshot-diagnostic.yml")


def test_v8_verified_snapshot_diagnostic_is_manual_nonexecution_and_source_free() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "manifest_sha256:" in workflow
    assert "verify_tqqq_core_only_free_ohlcv_p1_input_root" in workflow
    assert "gcloud storage cp --quiet \"$source/binding.json\" \"$source/bars.json\" \"$source/assurance.json\" \"$source/manifest.json\"" in workflow
    assert "build_verified_snapshot_relative_benchmark_diagnostic" in workflow
    assert "build_tqqq_p3_v8_evidence_index" not in workflow
    assert "TwelveYahooOhlcvObserver" not in workflow
    assert "TWELVE_DATA_API_KEY" not in workflow
    assert "--no-clobber" in workflow
    assert "diagnostic-replays/v1" in workflow
    assert "P4_P5_P6=NOT_AUTHORIZED" in workflow
    assert "broker_order" not in workflow
    assert "paper" not in workflow
    assert "shadow" not in workflow
    assert "live_enabled" not in workflow


def test_v8_verified_snapshot_diagnostic_never_uploads_raw_market_data_to_github() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    artifact_block = workflow.split("name: Upload sanitized diagnostic status only", 1)[1]
    assert "p3-result.json" in artifact_block
    assert "diagnostic-status.json" in artifact_block
    assert "diagnostic-record.v1.json" in artifact_block
    assert "relative-benchmark-summary.json" in artifact_block
    assert 'p1-root/bars.json' not in artifact_block
    assert 'p1-root/assurance.json' not in artifact_block
