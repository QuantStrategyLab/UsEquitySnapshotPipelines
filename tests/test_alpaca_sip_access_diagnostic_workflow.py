from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/alpaca-sip-access-diagnostic.yml")


def test_alpaca_sip_access_diagnostic_is_manual_nonlive_and_sanitized() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "date_cutoff:" in workflow
    assert "Exact YYYY-MM-DD cutoff to compare with a daily P1 request." in workflow
    assert "required: true" in workflow
    assert "DATE_CUTOFF: ${{ inputs.date_cutoff }}" in workflow
    assert "schedule:" not in workflow
    assert "pull_request:" not in workflow
    assert "workflow_run:" not in workflow
    assert "environment: market-data-nonlive" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" not in workflow
    assert "ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}" in workflow
    assert "ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}" in workflow
    assert "https://data.alpaca.markets/v2/stocks/bars?{query}" in workflow
    assert '"feed": "sip"' in workflow
    assert '"end": date_cutoff' in workflow
    assert '"date_cutoff": date_cutoff' in workflow
    assert "DATE_CUTOFF must be an exact YYYY-MM-DD date" in workflow
    assert '"QQQ": "2018-01-02"' in workflow
    assert '"TQQQ": "2018-01-02"' in workflow
    assert '"QQQM": "2020-10-13"' in workflow
    assert '"SOXL": "2022-01-03"' in workflow
    assert '"SOXX": "2022-01-03"' in workflow
    assert '"BOXX": "2022-12-28"' in workflow
    assert '"symbol_statuses": symbol_statuses' in workflow
    assert '"ALPACA_AUTHENTICATION_FAILED"' in workflow
    assert '"ALPACA_SIP_ACCESS_FORBIDDEN"' in workflow
    assert '"ALPACA_SIP_ACCESS_OK"' in workflow
    assert '"ALPACA_SIP_ACCESS_PARTIALLY_AVAILABLE"' in workflow
    assert "upload-artifact" not in workflow
    assert "gcloud" not in workflow
    assert "placeorder" not in workflow.lower()
    assert "broker" not in workflow.lower()
