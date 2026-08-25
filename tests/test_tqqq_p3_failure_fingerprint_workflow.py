from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-p3-failure-fingerprint.yml")


def test_tqqq_p3_failure_fingerprint_is_manual_ephemeral_and_sanitized() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "input_manifest_sha256:" in workflow
    assert "schedule:" not in workflow
    assert "pull_request:" not in workflow
    assert "workflow_run:" not in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "path: p3-source" in workflow
    assert workflow.count("working-directory: p3-source") == 3
    assert "contents: read" in workflow
    assert "id-token: write" in workflow
    assert "gcloud storage cp --quiet" in workflow
    assert "Download the existing P1 root into ephemeral runner storage" in workflow
    execution_step = workflow.split(
        "Execute the full ephemeral P3 path and emit a non-reversible fingerprint",
        maxsplit=1,
    )[1]
    assert "uv run --no-sync python - <<'PY'" in execution_step
    assert "run_tqqq_promotion_evidence" in workflow
    assert "_completed_evidence_summary" in workflow
    assert "contract=P2_V5_CONTRACT" in workflow
    assert '"failure_fingerprint_sha256"' in workflow
    assert '"runner_checkout"' in workflow
    assert '"--porcelain"' in workflow
    assert '"P3_DIAGNOSTIC_PARKED"' in workflow
    assert "upload-artifact" not in workflow
    assert "ALPACA_API_KEY_ID" not in workflow
    assert "placeorder" not in workflow.lower()
    assert "broker" not in workflow.lower()
