from pathlib import Path


def test_diagnostic_is_manual_nonlive_and_uses_explicit_feed_config():
    workflow = Path('.github/workflows/alpaca-sip-access-diagnostic.yml').read_text()
    assert 'workflow_dispatch:' in workflow
    assert 'schedule:' not in workflow and 'pull_request:' not in workflow
    assert 'environment: market-data-nonlive' in workflow
    assert 'contents: read' in workflow and 'id-token: write' not in workflow
    assert 'python scripts/alpaca_access_diagnostic.py' in workflow
    for key in ('ALPACA_SIP_MIN_DELAY_MINUTES', 'ALPACA_FEED_HISTORICAL', 'ALPACA_FEED_RECENT'):
        assert 'vars.' + key in workflow
    assert 'if: always()' in workflow and 'path: diagnostic.json' in workflow
    assert 'gcloud' not in workflow and 'placeorder' not in workflow.lower()


def test_fixed_probe_is_opt_in_and_uses_same_protected_secret_entry():
    workflow = Path('.github/workflows/alpaca-sip-access-diagnostic.yml').read_text()
    assert 'default: access' in workflow
    assert "if: inputs.diagnostic_mode == 'soxl-three-day'" in workflow
    assert "if: inputs.diagnostic_mode == 'access' || inputs.diagnostic_mode == ''" in workflow
    assert 'run: python scripts/alpaca_soxl_three_day_probe.py' in workflow
    probe_step = workflow.split('- name: Probe fixed SOXL three-day schema once')[1].split('- name: Upload')[0]
    assert 'ALPACA_API_KEY_ID: ${{ secrets.ALPACA_API_KEY_ID }}' not in probe_step
    assert 'ALPACA_API_SECRET_KEY: ${{ secrets.ALPACA_API_SECRET_KEY }}' not in probe_step
    assert 'DATE_CUTOFF: ${{ inputs.date_cutoff }}' in probe_step
    assert 'vars.' not in probe_step
    assert 'cancel-in-progress: false' in workflow
