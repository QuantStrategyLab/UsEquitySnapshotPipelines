import json
from hashlib import sha256
from unittest.mock import patch

import protected_exploratory as p
from check_local_candidate import CONFIG, ISSUER


def fixture():
    data = {
        'input_root': 'gs://synthetic/research/v2/input', 'dataset': 'fixture',
        'producer_revision': 'a' * 40, 'registration': {'generation': '1'},
        'manifest': {'sha256': p.local.MANIFEST_SHA256},
        'workload_identity_provider': 'provider', 'service_account': 'service', 'project_id': 'project',
    }
    plan = {'schema': 'rs04.soxl.exploratory-plan.v1', 'manifest_sha256': p.local.MANIFEST_SHA256,
            'independent_full_split_coverage': 'UNPROVEN', 'source_sha': 'c' * 40,
            'archive_binding': data.copy(), 'result_uri': 'gs://synthetic/' + p.RESULT_OBJECT}
    raw = json.dumps(plan).encode()
    return data, dict(plan_bytes=raw, plan_digest=sha256(raw).hexdigest(), source_sha='c' * 40,
                      config_bytes=CONFIG, issuer_bytes=ISSUER, scratch='/tmp')


class Client:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def bucket(self, name):
        self.calls.append(('bucket', name))
        return self

    def blob(self, name):
        self.calls.append(('blob', name))
        return self

    def upload_from_string(self, body, **kw):
        self.calls.append(('write', json.loads(body), kw))
        if self.fail:
            raise RuntimeError('private sentinel')


def test_conditional_create_and_unknown_no_retry():
    data, args = fixture()
    scope = {'classification': 'EXPLORATORY', 'independent_full_split_coverage': 'UNPROVEN',
             'eligible_for_promotion': False, 'strict_pit_verified': False, 'production_use_authorized': False}
    for fail in (False, True):
        client = Client(fail)
        with patch.object(p.local, 'run_local', return_value={'scope': scope, 'scenarios': []}), \
             patch.object(p.local, 'gcs_reader', return_value='reader'):
            code = p.deliver(lambda project: client, data, **args)
        assert code == ('SOXL_EXPLORATORY_WRITE_UNCONFIRMED' if fail else p.CREATED)
        writes = [c for c in client.calls if c[0] == 'write']
        assert len(writes) == 1
        assert writes[0][2] == {'content_type': 'application/json', 'if_generation_match': 0,
                                'retry': None, 'timeout': 30}
        assert ('blob', p.RESULT_OBJECT) in client.calls
        assert writes[0][1]['scope'] == scope


def test_binding_failure_before_client_and_output_scope_before_write():
    data, args = fixture()
    clients = []
    for key, value in [('source_sha', 'd' * 40), ('plan_digest', '0' * 64)]:
        assert p.deliver(lambda project: clients.append(project), data, **(args | {key: value})) == 'SOXL_EXPLORATORY_BINDINGS_STOPPED'
    assert clients == []
    client = Client()
    with patch.object(p.local, 'run_local', return_value={'scope': {}}), \
         patch.object(p.local, 'gcs_reader', return_value='reader'):
        assert p.deliver(lambda project: client, data, **args) == 'SOXL_EXPLORATORY_REPLAY_STOPPED'
    assert client.calls == []


def test_replay_failure_and_exception_hidden(capsys, monkeypatch):
    data, args = fixture()
    client = Client()
    with patch.object(p.local, 'run_local', side_effect=ValueError('private sentinel')):
        assert p.deliver(lambda project: client, data, **args) == 'SOXL_EXPLORATORY_REPLAY_STOPPED'
    assert client.calls == []
    monkeypatch.setattr(p, 'runner_ok', lambda env: True)
    monkeypatch.setattr(p, 'qualify', lambda *args: (_ for _ in ()).throw(ValueError('private sentinel')))
    monkeypatch.setenv('RUNNER_TEMP', '/tmp')
    assert p.main() == 2
    assert 'private sentinel' not in capsys.readouterr().out
