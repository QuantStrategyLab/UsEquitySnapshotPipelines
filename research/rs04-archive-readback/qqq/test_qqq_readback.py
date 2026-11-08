import unittest

from qqq_archive import PAGE_NAME
from qqq_readback import reconcile_qqq_gcs
from test_qqq_archive import FixedArchive


class ProtectedReaderTests(unittest.TestCase):
    def test_reads_only_the_bound_archive_members(self):
        fixture = FixedArchive()
        data = {
            **fixture.bind(),
            "input_root": "gs://example/research/v2/input",
            "dataset": "test-batch",
        }
        marker = object()

        class Client:
            def bucket(self, name):
                if name != "example":
                    raise AssertionError("wrong test bucket")
                return marker

        calls = []

        def read(bucket, name, reference):
            self.assertIs(bucket, marker)
            calls.append(name)
            prefix = "research/v2/input/test-batch/"
            self.assertTrue(name.startswith(prefix))
            return fixture.read(name[len(prefix):], reference)

        receipt = reconcile_qqq_gcs(Client(), data, fixture.public_csv(), read)
        self.assertEqual(receipt["status"], "QQQ_COMPARED")
        self.assertEqual(receipt["exact_matches"], 20)
        self.assertEqual(receipt["archive_unmatched"], 1)
        self.assertEqual(calls, [
            "research/v2/input/test-batch/registration.json",
            "research/v2/input/test-batch/manifest.json",
            "research/v2/input/test-batch/" + PAGE_NAME,
        ])
