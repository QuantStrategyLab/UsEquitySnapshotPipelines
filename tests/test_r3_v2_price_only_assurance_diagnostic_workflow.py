from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/r3-v2-price-only-assurance-diagnostic.yml")
FULL_OHLCV_WORKFLOW = Path(".github/workflows/r3-v2-free-source-assurance-diagnostic.yml")


def test_r3_v2_price_only_assurance_diagnostic_is_manual_readonly_and_isolated() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    full_ohlcv = FULL_OHLCV_WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "pull_request:" not in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" not in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "scripts/diagnose_r3_v2_price_only_assurance.py" in workflow
    assert "scripts/diagnose_r3_v2_free_source_assurance.py" not in workflow
    assert "upload-artifact" not in workflow
    assert "gcloud" not in workflow
    assert "placeorder" not in workflow.lower()
    assert "broker" not in workflow.lower()
    assert "price-only" in workflow.lower() or "price_only" in workflow.lower()

    # Existing full-OHLCV workflow must remain a separate contract entrypoint.
    assert "scripts/diagnose_r3_v2_free_source_assurance.py" in full_ohlcv
    assert "scripts/diagnose_r3_v2_price_only_assurance.py" not in full_ohlcv
