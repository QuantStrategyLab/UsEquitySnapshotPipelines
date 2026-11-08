import json
from datetime import date

import pytest
import exploratory as e
from check_local_candidate import CONFIG, ISSUER
from dataclasses import replace
from us_equity_strategies.research.rs04_b3_config import CONFIG as BASE
from us_equity_strategies.research.rs04_b3_runner import Ledger


def test_explicit_limit_before_any_read(tmp_path):
    calls = []
    for marker in (None, 'PROVEN'):
        plan = json.dumps({'schema': 'rs04.soxl.exploratory-plan.v1',
                           'manifest_sha256': "b" * 64,
                           'independent_full_split_coverage': marker}).encode()
        with pytest.raises(ValueError, match='EXPLORATORY_SCOPE_MISMATCH'):
            e.run_local(plan_bytes=plan, reviewed_digest=e.digest(plan), config_bytes=CONFIG,
                        issuer_bytes=ISSUER, data={}, client_factory=lambda: calls.append(1),
                        scratch=tmp_path, source_sha='a' * 40)
    assert not calls


def test_known_split_cash_and_cost_accounting():
    book = Ledger(1000., {'SOXX': 3}, {})
    before = book.nav({'SOXX': 90.})
    book.split('SOXX', 3.)
    assert book.quantity['SOXX'] == 9
    assert book.nav({'SOXX': 30.}) == before
    with pytest.raises(ValueError, match='FRACTIONAL_SPLIT'):
        Ledger(0., {'SOXX': 1}, {}).split('SOXX', .5)
    book.accrue('SOXX', 1., date(2024, 3, 12))
    assert book.nav({'SOXX': 30.}) == before + 9
    book.pay(date(2024, 3, 12))
    assert book.nav({'SOXX': 30.}) == before + 9
    fee = replace(BASE, commission_bps=2., slippage_including_spread_bps=5.,
                  minimum_commission_usd=0., commission_per_share_usd=0.)
    costs = book.rebalance({'SOXL': .4, 'BOXX': .4}, {'SOXX': 30., 'SOXL': 20., 'BOXX': 10.},
                           profile='SOXL', cost_multiplier=1., fee_schedule=fee)
    notional = sum(f['notional'] for f in book.last_fills)
    assert costs['commission'] == pytest.approx(notional * .0002)
    assert costs['slippage'] == pytest.approx(notional * .0005)
    assert book.nav({'SOXX': 30., 'SOXL': 20., 'BOXX': 10.}) == pytest.approx(before + 9 - sum(costs.values()))


def test_status_labels_and_failure(monkeypatch):
    monkeypatch.setattr(e, 'run_local', lambda **kwargs: (_ for _ in ()).throw(ValueError('private')))
    status, result = e.run_local_status()
    assert status == {'status': 'PARKED', 'reason_code': 'SOXL_EXPLORATORY_STOPPED',
                      'classification': 'EXPLORATORY'}
    assert result is None
