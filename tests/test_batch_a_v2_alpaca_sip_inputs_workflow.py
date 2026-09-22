from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(".github/workflows/batch-a-v2-alpaca-sip-inputs.yml")


def test_batch_a_v2_workflow_is_manual_main_only_nonlive() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "execute:" in workflow
    assert "batch_id:" in workflow
    assert "default: false" in workflow
    assert "schedule:" not in workflow
    assert "pull_request:" not in workflow
    assert "push:" not in workflow
    assert "workflow_run:" not in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" in workflow
    assert "workload_identity_provider: ${{ vars.GCP_WORKLOAD_IDENTITY_PROVIDER }}" in workflow
    assert "service_account: ${{ vars.GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT }}" in workflow
    assert "ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}" in workflow
    assert "ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}" in workflow
    assert "scripts/acquire_batch_a_v2_price_snapshots_alpaca.py" in workflow
    assert "--execute" in workflow
    assert "--batch-id" in workflow
    assert "upload-artifact" not in workflow
    assert "placeorder" not in workflow.lower()
    assert "broker" not in workflow.lower()
