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
