from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-v7-p1-p3-manual-research.yml")


def test_v7_workflow_is_manual_and_cannot_touch_v5_or_execution_lanes() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "tqqq_core_only_p2_v7_relative_benchmark" in workflow
    assert "P2_V7_CONTRACT" in workflow
    assert "config/tqqq_core_only_p2_v5.json" not in workflow
    assert "P2_V5_CONTRACT" not in workflow
    assert "P4_P5_P6=NOT_AUTHORIZED" in workflow
    assert "automatic_promotion': False" in workflow
    assert "no_order': True" in workflow
    assert "broker_order" not in workflow
    assert "paper" not in workflow
    assert "shadow" not in workflow
    assert "live_enabled" not in workflow


def test_v7_workflow_requires_scope_record_before_provider_and_retains_no_raw_bars() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Verify external non-live scope record before provider access" in workflow
    assert "Re-verify external non-live scope record" in workflow
    assert "config/tqqq_p1_p3_mandates" in workflow
    assert "ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}" in workflow
    assert "ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}" in workflow
    assert '"$root/binding.json" "$root/bars.json" "$root/manifest.json"' in workflow
    assert "relative-benchmark-policy.v1.json" not in workflow
    assert "--no-clobber" in workflow
    assert "tqqq_p1_p3_v7_nonlive_evidence_index" in workflow
