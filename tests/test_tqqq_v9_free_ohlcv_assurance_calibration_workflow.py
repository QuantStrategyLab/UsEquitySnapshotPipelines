from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-v9-free-ohlcv-assurance-calibration.yml")


def test_assurance_calibration_is_scheduled_observation_only_and_redacted() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert 'cron: "30 2 * * 2-6"' in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "actions: read" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" not in workflow
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in workflow
    assert "observe_tqqq_core_only_free_ohlcv_settlement" in workflow
    assert "build_tqqq_free_ohlcv_settlement_tracks.py" in workflow
    assert "Download prior redacted settlement observations" in workflow
    assert "P2_V9_CONTRACT" in workflow
    assert "retention-days: 45" in workflow
    assert "settlement-observations.json" in workflow
    assert "settlement-tracks.json" in workflow
    assert "p1-root" not in workflow
    assert '"$root/bars.json"' not in workflow
    assert "gcloud" not in workflow
    assert "P3" not in workflow
    assert "broker" not in workflow.lower()
    assert "placeorder" not in workflow.lower()
    assert "OBSERVATION_ONLY_NO_ORDER" in workflow
