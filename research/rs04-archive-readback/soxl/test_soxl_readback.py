import io
import unittest
from unittest.mock import patch

from soxl_readback import READ_MEMBERS, reconcile_soxl_gcs
from test_soxl_reconcile import fixture


class ProtectedReaderTests(unittest.TestCase):
    def test_fixed_group_comparison_through_gcs_adapter(self):
        data, issuer, reads, read = fixture()
        data.update(input_root="gs://example/research/v2/input", dataset="test-batch")
        marker = object()

        class Client:
            def bucket(self, name):
                return marker if name == "example" else None

        prefix = "research/v2/input/test-batch/"

        def exact_read(bucket, name, reference):
            self.assertIs(bucket, marker)
            self.assertTrue(name.startswith(prefix))
            return read(name.removeprefix(prefix), reference)

        result = reconcile_soxl_gcs(Client(), data, io.StringIO(issuer), exact_read)
        self.assertEqual(result["status"], "SOXL_COMPARED")
        self.assertEqual(result["symbols"]["BOXX"]["cash"]["exact_matches"], 1)
        self.assertCountEqual(reads, READ_MEMBERS)

    def test_io_boundary_rejects_other_members_and_repeated_reads(self):
        marker = object()
        data = {"input_root": "gs://example/research/v2/input", "dataset": "test-batch"}

        class Client:
            def bucket(self, name):
                if name != "example":
                    raise AssertionError("wrong test bucket")
                return marker

        calls = []

        def exact_read(bucket, name, ref):
            self.assertIs(bucket, marker)
            calls.append((name, ref))
            return b"synthetic"

        reference = ("7", 9, "a" * 64)

        def run(binding, reader, issuer):
            self.assertIs(binding, data)
            for name in sorted(READ_MEMBERS):
                self.assertEqual(reader(name, reference), b"synthetic")
            for name in ("actions/QQQ/page-001.json", "bars/SOXL/page-001.json", "manifest.json"):
                with self.assertRaisesRegex(ValueError, "^SOXL_READ_SCOPE_EXCEEDED$"):
                    reader(name, reference)
            return {"status": "SOXL_COMPARED"}

        with patch("soxl_readback.reconcile", side_effect=run):
            result = reconcile_soxl_gcs(Client(), data, None, exact_read)
        self.assertEqual(result, {"status": "SOXL_COMPARED"})
        self.assertEqual(calls, [(f"research/v2/input/test-batch/{name}", reference) for name in sorted(READ_MEMBERS)])

        with patch("soxl_readback.reconcile", return_value={"status": "SOXL_COMPARED"}):
            result = reconcile_soxl_gcs(Client(), data, None, exact_read)
        self.assertEqual(result, {"status": "PARKED", "reason_code": "SOXL_READ_SCOPE_INCOMPLETE"})
