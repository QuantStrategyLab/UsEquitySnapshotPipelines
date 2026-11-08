import contextlib
import io
import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import qqq_private_readback as app


class PrivateReadbackTests(unittest.TestCase):
    def setUp(self):
        self.data = {"input_root": "gs://example-bucket/research/v2/input/", "dataset": "fixture"}
        self.uri = f"gs://example-bucket/{app.RESULT_OBJECT}"
        self.client = Mock()
        self.reader = Mock()

    def test_wrong_destination_stops_before_input_or_write(self):
        status = app.deliver(self.client, self.data, self.uri + ".other", None, self.reader)
        self.assertEqual(status, "QQQ_RESULT_BINDING_INVALID")
        self.client.bucket.assert_not_called()
        self.reader.assert_not_called()

    @patch.object(app, "issuer_events")
    @patch.object(app, "archive_events")
    @patch.object(app, "locate_residuals")
    def test_conditional_create_only_and_no_retry_on_unknown_write(self, locate, archive, issuer):
        bucket = self.client.bucket.return_value
        names = ("registration.json", "manifest.json", "actions/QQQ/page-001.json")
        archive.side_effect = lambda data, read: [read(name, None) for name in names]
        payload = {"status": "QQQ_RESIDUALS_LOCATED", "fixture": "private fixture"}
        locate.return_value = payload
        writer = bucket.blob.return_value.upload_from_string
        self.assertEqual(app.deliver(self.client, self.data, self.uri, None, self.reader), "QQQ_PRIVATE_RESULT_CREATED")
        self.assertEqual([call.args[1] for call in self.reader.call_args_list],
                         [f"research/v2/input/fixture/{name}" for name in names])
        bucket.blob.assert_called_once_with(app.RESULT_OBJECT)
        writer.assert_called_once_with(json.dumps(payload, allow_nan=False, sort_keys=True),
                                       content_type="application/json", if_generation_match=0, retry=None, timeout=30)
        writer.reset_mock()
        writer.side_effect = TimeoutError("private failure")
        self.assertEqual(app.deliver(self.client, self.data, self.uri, None, self.reader),
                         "QQQ_RESULT_WRITE_UNCONFIRMED")
        writer.assert_called_once()
        writer.reset_mock()
        locate.return_value = {"status": "PARKED"}
        self.assertEqual(app.deliver(self.client, self.data, self.uri, None, self.reader),
                         "QQQ_PRIOR_RESULT_NOT_REPRODUCED")
        writer.assert_not_called()

    def test_entry_emits_only_fixed_status_even_if_inner_code_logs_or_fails(self):
        sha = "a" * 40
        env = {"RUNNER_OS": "Linux", "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
               "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": sha, "RS04_QQQ_EXPECTED_SHA": sha,
               "RS04_QQQ_PRIVATE_RESULT_URI": self.uri}
        modules = {"google.cloud": SimpleNamespace(storage=SimpleNamespace(Client=lambda: self.client)),
                   "inspect_gcs": SimpleNamespace(binding=lambda: self.data, exact_read=self.reader)}

        def noisy(*_):
            print("private stdout")
            print("private stderr", file=sys.stderr)
            raise RuntimeError("private exception")

        stdout, stderr = io.StringIO(), io.StringIO()
        with (patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, modules),
              patch.object(app, "deliver", side_effect=noisy),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            self.assertEqual(app.main(), 2)
        self.assertEqual(json.loads(stdout.getvalue()), {"status": "PARKED", "reason_code": "QQQ_PRIVATE_SETUP_STOPPED"})
        self.assertEqual(stderr.getvalue(), "")
