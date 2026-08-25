from __future__ import annotations

import hashlib
from pathlib import Path


WORKFLOW = Path(".github/workflows/tqqq-p1-p3-one-shot.yml")
CONFIG = Path("config/tqqq_core_only_p2_v1.json")
P2_CONFIG_SHA256 = "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69"


def test_tqqq_workflow_is_manual_nonlive_and_pinned() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "schedule:" not in workflow
    assert "workflow_run:" not in workflow
    assert "pull_request:" not in workflow
    assert "concurrency:" in workflow
    assert "group: tqqq-p1-p3-nonlive" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "actions/checkout@11d5960a326750d5838078e36cf38b85af677262" in workflow
    assert "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065" in workflow
    assert "google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093" in workflow
    assert "google-github-actions/setup-gcloud@e427ad8a34f8676edf47cf7d7925499adf3eb74f" in workflow
    assert workflow.count("Install locked runtime") == 2
    assert workflow.count("uv==0.11.19") == 2
    assert workflow.count("uv sync --locked --no-dev --no-editable --python 3.11") == 2
    assert "python -m pip install --quiet ." not in workflow
    assert "uv run --no-sync python scripts/run_tqqq_p3.py" in workflow


def test_tqqq_workflow_uses_scoped_alpaca_headers_and_one_shot_p1_to_p3_path() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "mandate_id:" in workflow
    assert "required: true" in workflow
    assert "Resolve non-live scope record before P1 acquisition" in workflow
    assert "Re-verify non-live scope record" in workflow
    assert workflow.index("Resolve non-live scope record before P1 acquisition") < workflow.index("Acquire and verify P1 root")
    assert "inputs.mandate_receipt_sha256" not in workflow
    assert "ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}" in workflow
    assert "ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}" in workflow
    assert "--api-key" not in workflow
    assert "--api-secret" not in workflow
    assert "--no-clobber" in workflow
    assert "gs://qsl-runtime-logs-shared/tqqq-p1-p3" in workflow
    assert "verify_tqqq_core_only_input_root" in workflow
    assert "Build validated P1 completion marker" in workflow
    assert "p1-complete.json" in workflow
    assert "verify_tqqq_core_only_p1_remote_completion" in workflow
    assert "chmod 700" in workflow
    assert "scripts/run_tqqq_p3.py" in workflow
    assert "P3_FAILURE_CLASS=" in workflow
    assert "P3_FAILURE_STAGE=" in workflow
    assert "invalid sanitized P3 failure result" in workflow
    assert "Build validated P3 evidence index" in workflow
    assert "Create-only upload of validated P3 evidence index" in workflow
    assert "/p3-evidence-index/${P3_EVIDENCE_SHA256}.json" in workflow
    assert "validate_tqqq_p3_result" in workflow
    assert "build_tqqq_p3_evidence_index" in workflow
    assert '"mandate_id": os.environ["MANDATE_ID"]' in workflow
    assert "nonlive_scope_record=" in workflow
    assert "gcloud storage cp --quiet --no-clobber \"$INDEX_PATH\" \"$destination\"" in workflow
    assert "actions/upload-artifact" not in workflow
    p3_job = workflow.split("  p3:", maxsplit=1)[1]
    assert "path: p3-source" in p3_job
    assert p3_job.count("working-directory: p3-source") == 5
    assert "ALPACA_API_KEY_ID" not in p3_job
    assert "ALPACA_API_SECRET_KEY" not in p3_job
    assert p3_job.count("scripts/run_tqqq_p3.py") == 1


def test_tqqq_bundled_candidate_is_exact_canonical_p2_bytes() -> None:
    assert hashlib.sha256(CONFIG.read_bytes()).hexdigest() == P2_CONFIG_SHA256
