"""Synthetic ledgers only: no provider, account, credential or cloud access."""
from __future__ import annotations

import copy
import hashlib
import io
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "compare_price_basis.py"
spec = importlib.util.spec_from_file_location("price_basis_comparison", MODULE)
research = importlib.util.module_from_spec(spec)
spec.loader.exec_module(research)


def fixture():
    # Weekdays here are synthetic input labels, not a real exchange calendar.
    start = date(2023, 1, 1)
    sessions = [(start + timedelta(days=i)).isoformat() for i in range(1120)
                if (start + timedelta(days=i)).weekday() < 5]
    arm = {"price_basis": "split_adjusted", "platform_revision": "a" * 40,
           "strategy_revision": "b" * 40, "effective_config_sha256": "c" * 64}
    meta = {
        "schema_version": 1, "comparison_kind": "signal_price_basis", "as_of": "2025-12-31",
        "calendar": "XNYS",
        "inception": "2024-06-03", "baseline": {**arm, "price_basis": "split_dividend_adjusted"},
        "candidate": arm, "shared": {key: "d" * 64 for key in (
            "data_manifest_sha256", "calendar_sha256", "replay_code_sha256", "cost_model_sha256",
            "plugin_binding_sha256", "initial_state_sha256", "external_flows_sha256")},
        "timing": "next_open", "unitized_twr": True, "cost_feedback": "self_financing",
        "cost_components": {key: {"status": "modeled", "reason_or_version": "synthetic-v1"}
                            for key in research.COST_COMPONENTS},
        "turnover_definition": "security_one_way_notional_over_prefill_nav",
        "limits": {"total": 0.97, "symbols": {"SYNTH": 0.679}},
        "size_bucket_edges": [0.01, 0.05], "threshold_bucket_edges": [0.001, 0.01],
        "thresholds_applicable": True, "corporate_action_coverage_complete": True,
    }
    observation = {"signals": {"signal": 1.0}, "weights": {"SYNTH": 0.5}, "orders": {},
                   "threshold_distances": {"entry": 0.002}, "gross_return": 0.001,
                   "net_return_1x": 0.0009, "net_return_2x": 0.0008, "turnover": 0.0,
                   "trade_count": 0}
    bundle = {"rows": [{"session": s, "baseline": copy.deepcopy(observation),
                        "candidate": copy.deepcopy(observation)}
                       for s in sessions if "2023-12-31" <= s <= meta["as_of"]],
              "corporate_actions": [{"session": "2024-06-03", "kind": "dividend"},
                                    {"session": "2024-07-01", "kind": "split"}]}
    return meta, bundle, sessions


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.meta, self.bundle, self.sessions = fixture()

    def run_comparison(self):
        return research.compare(self.meta, self.bundle, self.sessions)

    def test_equal_arms_and_two_distinct_windows(self):
        out = self.run_comparison()["windows"]
        self.assertGreater(out["trailing_two_years"]["sessions"], out["since_inception"]["sessions"])
        for result in out.values():
            self.assertEqual(result["decision_diff_days"], 0)
            self.assertEqual(result["daily_max_abs_weight_delta"]["max"], 0)
            self.assertEqual(result["performance"]["net_return_1x"]["delta"]["cagr"], 0)

    def test_weights_trade_direction_and_size_are_separate(self):
        a, b = self.bundle["rows"][-1]["baseline"], self.bundle["rows"][-1]["candidate"]
        b["weights"]["SYNTH"] += 0.1
        a["orders"], b["orders"] = {"SYNTH": 0.005}, {"SYNTH": -0.02}
        out = self.run_comparison()["windows"]["since_inception"]
        self.assertAlmostEqual(out["daily_max_abs_weight_delta"]["max"], 0.1)
        self.assertAlmostEqual(out["cash_weight_delta"]["max"], 0.1)
        self.assertEqual(out["decision_diff_days"], 1)
        self.assertEqual(out["trade_vs_no_trade_diff_days"], 0)
        self.assertEqual(out["direction_diff_days"], 1)
        self.assertEqual(out["size_bucket_diff_days"], 1)

    def test_quantiles_and_order_bucket_boundaries(self):
        self.assertEqual(research.quantiles([0, 1, 2, 3])["p50"], 1.5)
        self.assertAlmostEqual(research.quantiles([0, 1, 2, 3])["p95"], 2.85)
        self.assertEqual(research.decisions({"A": 0.01, "B": 0}, [0.01]), {"A": (1, 0)})

    def test_exact_order_size_difference_within_one_bucket(self):
        row = self.bundle["rows"][-1]
        row["baseline"]["orders"] = {"SYNTH": 0.002}
        row["candidate"]["orders"] = {"SYNTH": 0.008}
        out = self.run_comparison()["windows"]["since_inception"]
        self.assertEqual(out["decision_diff_days"], 1)
        self.assertEqual(out["order_size_diff_days"], 1)
        self.assertEqual(out["size_bucket_diff_days"], 0)
        self.assertEqual(out["direction_diff_days"], 0)

    def test_exact_position_difference_without_order_or_bucket_change(self):
        self.bundle["rows"][-1]["candidate"]["weights"]["SYNTH"] += 0.002
        out = self.run_comparison()["windows"]["since_inception"]
        self.assertEqual(out["position_diff_days"], 1)
        self.assertEqual(out["decision_diff_days"], 1)
        self.assertEqual(out["size_bucket_diff_days"], 0)

    def test_explicit_zero_order_and_weight_are_not_differences(self):
        self.bundle["rows"][-1]["candidate"]["orders"] = {"ZERO": 0}
        self.bundle["rows"][-1]["candidate"]["weights"]["ZERO"] = 0
        out = self.run_comparison()["windows"]["since_inception"]
        self.assertEqual(out["position_diff_days"], 0)
        self.assertEqual(out["decision_diff_days"], 0)

    def test_unknown_inception_preserves_trailing_window(self):
        for value in (None, "", "unknown"):
            with self.subTest(value=value):
                self.meta["inception"] = value
                out = self.run_comparison()["windows"]
                self.assertGreater(out["trailing_two_years"]["sessions"], 0)
                self.assertEqual(out["since_inception"]["status"], "NOT_COMPUTED")
                self.assertEqual(out["since_inception"]["reason"], "INCEPTION_UNKNOWN")
                self.assertIsNone(out["since_inception"]["performance"])
        del self.meta["inception"]
        self.assertEqual(self.run_comparison()["windows"]["since_inception"]["status"], "NOT_COMPUTED")

    def test_invalid_or_future_inception_is_still_rejected(self):
        for value in ("2024-02-30", "2026-01-01"):
            with self.subTest(value=value), self.assertRaises(research.ResearchStop):
                self.meta["inception"] = value
                self.run_comparison()

    def test_total_return_and_fill_count_for_both_arms_and_deltas(self):
        for row in self.bundle["rows"]:
            for arm in ("baseline", "candidate"):
                for field in ("gross_return", "net_return_1x", "net_return_2x"):
                    row[arm][field] = 0
        last = self.bundle["rows"][-1]
        for field in ("gross_return", "net_return_1x", "net_return_2x"):
            last["baseline"][field], last["candidate"][field] = 0.1, 0.2
        last["baseline"]["trade_count"], last["candidate"]["trade_count"] = 2, 5
        for window in self.run_comparison()["windows"].values():
            for metrics in window["performance"].values():
                self.assertAlmostEqual(metrics["baseline"]["total_return"], 0.1)
                self.assertAlmostEqual(metrics["candidate"]["total_return"], 0.2)
                self.assertAlmostEqual(metrics["delta"]["total_return"], 0.1)
                self.assertEqual(metrics["baseline"]["trade_count"], 2)
                self.assertEqual(metrics["candidate"]["trade_count"], 5)
                self.assertEqual(metrics["delta"]["trade_count"], 3)

    def test_missing_or_invalid_fill_count_is_not_inferred_from_orders(self):
        for value in (None, -1, 1.5, True):
            with self.subTest(value=value), self.assertRaisesRegex(research.ResearchStop, "TRADE_COUNT_REQUIRED"):
                self.bundle["rows"][0]["baseline"]["trade_count"] = value
                self.run_comparison()

    def test_action_windows_are_five_sessions_each_side_and_deduplicate(self):
        action = self.bundle["corporate_actions"][0]
        self.bundle["corporate_actions"].append(copy.deepcopy(action))
        result = self.run_comparison()["windows"]["trailing_two_years"]["corporate_action_windows"]["dividend"]
        self.assertEqual(result["sessions"], 11)
        self.assertEqual(result["events_in_window"], 2)

    def test_threshold_buckets_use_baseline_distance(self):
        for row in self.bundle["rows"]:
            row["candidate"]["threshold_distances"] = {"entry": 100.0}
        buckets = self.run_comparison()["windows"]["since_inception"]["threshold_buckets"]
        self.assertEqual(buckets[0]["sessions"], 0)
        self.assertGreater(buckets[1]["sessions"], 0)
        self.assertEqual(buckets[2]["sessions"], 0)

    def test_initial_nav_is_in_drawdown(self):
        row = {"session": "2025-01-02", "baseline": {"r": -0.2, "turnover": 0.3, "trade_count": 2}}
        metrics = research.performance([row], "baseline", "r")
        self.assertAlmostEqual(metrics["max_drawdown"], -0.2)
        self.assertEqual(metrics["turnover_sum"], 0.3)
        self.assertAlmostEqual(metrics["total_return"], -0.2)
        self.assertEqual(metrics["trade_count"], 2)

    def test_total_return_compounds_sessions(self):
        rows = [{"session": session, "baseline": {"r": value, "turnover": 0, "trade_count": count}}
                for session, value, count in (("2025-01-02", 0.1, 2), ("2025-01-03", -0.1, 3))]
        metrics = research.performance(rows, "baseline", "r")
        self.assertAlmostEqual(metrics["total_return"], -0.01)
        self.assertEqual(metrics["trade_count"], 5)

    def test_missing_session_rejected(self):
        self.bundle["rows"].pop(20)
        with self.assertRaisesRegex(research.ResearchStop, "SESSION_COVERAGE"):
            self.run_comparison()

    def test_duplicate_session_rejected(self):
        self.bundle["rows"].insert(20, self.bundle["rows"][20])
        with self.assertRaisesRegex(research.ResearchStop, "SESSION_COVERAGE"):
            self.run_comparison()

    def test_nonfinite_and_caps_rejected(self):
        for value in (float("nan"), float("inf"), 0.68):
            with self.subTest(value=value):
                self.bundle["rows"][0]["candidate"]["weights"]["SYNTH"] = value
                with self.assertRaises(research.ResearchStop):
                    self.run_comparison()

    def test_missing_effective_config_or_changed_code_rejected(self):
        for key, value in (("effective_config_sha256", ""), ("platform_revision", "e" * 40)):
            with self.subTest(key=key):
                meta = copy.deepcopy(self.meta)
                meta["candidate"][key] = value
                with self.assertRaises(research.ResearchStop):
                    research.compare(meta, self.bundle, self.sessions)

    def test_missing_cost_and_non_self_financing_rejected(self):
        for key, value in (("cost_components", {}), ("cost_feedback", "no_weight_feedback")):
            with self.subTest(key=key):
                meta = copy.deepcopy(self.meta)
                meta[key] = value
                with self.assertRaises(research.ResearchStop):
                    research.compare(meta, self.bundle, self.sessions)

    def test_incomplete_threshold_or_action_coverage_rejected(self):
        self.bundle["rows"][0]["candidate"]["threshold_distances"] = {}
        with self.assertRaisesRegex(research.ResearchStop, "THRESHOLD_COVERAGE"):
            self.run_comparison()
        self.meta["corporate_action_coverage_complete"] = False
        with self.assertRaisesRegex(research.ResearchStop, "CORPORATE_ACTION_COVERAGE"):
            self.run_comparison()

    def test_invalid_cost_stress_rejected(self):
        self.bundle["rows"][0]["candidate"]["net_return_2x"] = 0.1
        with self.assertRaisesRegex(research.ResearchStop, "COST_STRESS"):
            self.run_comparison()

    def test_summary_does_not_export_observations(self):
        out = json.dumps(self.run_comparison())
        for private_value in ("SYNTH", "2024-06-03", "entry", "signals", "weights", "orders"):
            self.assertNotIn(private_value, out)

    def test_local_and_nondefault_branch_guard(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(research.ResearchStop, "CLOUD_DEFAULT_BRANCH"):
                research.cloud_guard()
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "RUNNER_OS": "Linux",
                                    "GITHUB_DEFAULT_BRANCH": "main", "GITHUB_REF": "refs/heads/draft"}), \
                patch.object(research.platform, "system", return_value="Linux"):
            with self.assertRaises(research.ResearchStop):
                research.cloud_guard()

    def test_reference_rejects_traversal_foreign_prefix_and_unpinned_generation(self):
        root = "gs://synthetic-example/research/paired"
        for object_name, generation in (("research/paired/../escape", "1"),
                                        ("research/other/bundle", "1"), ("research/paired/bundle", ""),
                                        ("gs://elsewhere/research/x", "1"), ("/research/paired/bundle", "1")):
            with self.subTest(object_name=object_name):
                with self.assertRaises(research.ResearchStop):
                    research.reference({"object": object_name, "generation": generation, "sha256": "a" * 64}, root)

    def test_exact_object_reads_need_no_list_and_verify_pinned_bytes(self):
        root = "gs://synthetic-example/research/paired"
        body = b'{"synthetic":true}'
        ref = {"object": "research/paired/input-inventory.json", "generation": "7",
               "sha256": hashlib.sha256(body).hexdigest()}
        client = Mock(spec=["bucket"])
        cloud_bucket = client.bucket.return_value = Mock(spec=["blob"])
        blob = cloud_bucket.blob.return_value = Mock(spec=["size", "reload", "download_as_bytes"])
        blob.size = len(body)
        blob.download_as_bytes.return_value = body
        self.assertEqual(research.read_object(ref, root, client), {"synthetic": True})
        client.bucket.assert_called_once_with("synthetic-example")
        cloud_bucket.blob.assert_called_once_with(ref["object"], generation=7)
        blob.reload.assert_called_once_with(if_generation_match=7, retry=None, timeout=30)
        blob.download_as_bytes.assert_called_once_with(if_generation_match=7, retry=None, timeout=60)

    def test_manifest_validates_all_references_before_bundle_reads(self):
        root = "gs://synthetic-example/research/paired"
        target = {"alias": "target-1", "metadata": self.meta,
                  "replay_bundle": {"object": "research/paired/ledger.json", "generation": "7", "sha256": "a" * 64}}
        manifest = {"schema_version": 1, "targets": [target]}
        self.assertEqual(research.validate_manifest(manifest, root), [target])
        invalid = copy.deepcopy(target)
        invalid["alias"] = "target-2"
        invalid["replay_bundle"]["object"] = "research/other/ledger.json"
        manifest["targets"].append(invalid)
        with self.assertRaisesRegex(research.ResearchStop, "OBJECT_OUTSIDE_INPUT_ROOT"):
            research.validate_manifest(manifest, root)

    def test_main_exact_manifest_unknown_inception_and_read_set_validation(self):
        self.meta.pop("inception")
        manifest = {"schema_version": 1, "targets": [{"alias": "target-1", "metadata": self.meta,
                    "replay_bundle": {"object": "research/paired/bundle.json", "generation": "8", "sha256": "b" * 64}}]}
        calendar_module, storage_module, credentials_module = Mock(), Mock(), Mock()
        calendar_module.get_calendar.return_value.sessions_in_range.return_value = [
            datetime.fromisoformat(s) for s in self.sessions]
        with tempfile.TemporaryDirectory() as output:
            env = {"PRICE_BASIS_INPUT_ROOT": "gs://synthetic-example/research/paired",
                   "PRICE_BASIS_MANIFEST_OBJECT": "research/paired/reviewed.json",
                   "PRICE_BASIS_MANIFEST_GENERATION": "7", "PRICE_BASIS_MANIFEST_SHA256": "a" * 64,
                   "RUNNER_TEMP": output, "GOOGLE_CLOUD_PROJECT": "synthetic-project",
                   "GOOGLE_OAUTH_ACCESS_TOKEN": "synthetic-token"}
            modules = {"exchange_calendars": calendar_module, "google.cloud.storage": storage_module,
                       "google.cloud": Mock(storage=storage_module), "google.oauth2.credentials": credentials_module}
            with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, modules), \
                    patch.object(sys, "argv", [str(MODULE)]), patch.object(research, "cloud_guard"), \
                    patch.object(research, "read_object", side_effect=[manifest, self.bundle]) as read, \
                    redirect_stdout(io.StringIO()) as log:
                research.main()
            self.assertEqual(read.call_args_list[0].args[0], {
                "object": "research/paired/reviewed.json", "generation": "7", "sha256": "a" * 64})
            self.assertEqual(read.call_count, 2)
            result = json.loads((Path(output) / "price-basis-aggregate.json").read_text())
            self.assertEqual(result["targets"][0]["windows"]["since_inception"]["status"], "NOT_COMPUTED")
            self.assertGreater(result["targets"][0]["windows"]["trailing_two_years"]["sessions"], 0)
            self.assertNotIn("synthetic-token", log.getvalue())
            self.assertNotIn("research/paired", log.getvalue())
            invalid = copy.deepcopy(manifest["targets"][0])
            invalid["alias"] = "target-2"
            invalid["replay_bundle"]["generation"] = ""
            manifest["targets"].append(invalid)
            (Path(output) / "price-basis-aggregate.json").unlink()
            with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, modules), \
                    patch.object(sys, "argv", [str(MODULE)]), patch.object(research, "cloud_guard"), \
                    patch.object(research, "read_object", return_value=manifest) as read, \
                    redirect_stdout(io.StringIO()) as log, self.assertRaises(SystemExit) as stopped:
                research.main()
            self.assertEqual(stopped.exception.code, 1)
            self.assertEqual(read.call_count, 1)
            self.assertIn("OBJECT_GENERATION_REQUIRED", log.getvalue())
            self.assertFalse((Path(output) / "price-basis-aggregate.json").exists())

    def test_preflight_checks_reference_without_cloud_reads(self):
        env = {"PRICE_BASIS_INPUT_ROOT": "gs://synthetic-example/research/paired",
               "PRICE_BASIS_MANIFEST_OBJECT": "research/paired/reviewed.json",
               "PRICE_BASIS_MANIFEST_GENERATION": "7", "PRICE_BASIS_MANIFEST_SHA256": "a" * 64}
        with patch.dict(os.environ, env, clear=True), patch.object(sys, "argv", [str(MODULE), "--preflight"]), \
                patch.object(research, "cloud_guard"), patch.object(research, "read_object") as read, \
                redirect_stdout(io.StringIO()) as log:
            research.main()
        read.assert_not_called()
        self.assertIn("PREFLIGHT_VALIDATED_NO_DATA_READ", log.getvalue())

    def test_read_denial_stops_without_retry_or_media_read(self):
        ref = {"object": "research/paired/bundle", "generation": "7", "sha256": "a" * 64}
        client = Mock(spec=["bucket"])
        blob = client.bucket.return_value.blob.return_value
        blob.reload.side_effect = PermissionError("PRIVATE_PROVIDER_ERROR")
        with self.assertRaisesRegex(research.ResearchStop, "^CLOUD_INPUT_READ_FAILED_NO_RETRY$"):
            research.read_object(ref, "gs://synthetic-example/research/paired", client)
        blob.reload.assert_called_once_with(if_generation_match=7, retry=None, timeout=30)
        blob.download_as_bytes.assert_not_called()

    def test_cloud_read_checks_generation_digest_and_never_echoes_provider_error(self):
        root = "gs://synthetic-example/research/paired"
        ref = {"object": "research/paired/bundle", "generation": "7", "sha256": "a" * 64}
        client = Mock()
        blob = client.bucket.return_value.blob.return_value
        blob.size = 2
        blob.download_as_bytes.return_value = b"{}"
        with self.assertRaisesRegex(research.ResearchStop, "OBJECT_DIGEST_MISMATCH"):
            research.read_object(ref, root, client)
        blob.download_as_bytes.assert_called_once_with(if_generation_match=7, retry=None, timeout=60)
        blob.reload.side_effect = RuntimeError("PRIVATE_PROVIDER_ERROR")
        with self.assertRaisesRegex(research.ResearchStop, "CLOUD_INPUT_READ_FAILED_NO_RETRY"):
            research.read_object(ref, root, client)


if __name__ == "__main__":
    unittest.main()
