"""Synthetic ledgers only: no provider, account, credential or cloud access."""
from __future__ import annotations

import copy
import importlib.util
import json
import os
import unittest
from datetime import date, timedelta
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
                   "net_return_1x": 0.0009, "net_return_2x": 0.0008, "turnover": 0.0}
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
        row = {"session": "2025-01-02", "baseline": {"r": -0.2, "turnover": 0.3}}
        metrics = research.performance([row], "baseline", "r")
        self.assertAlmostEqual(metrics["max_drawdown"], -0.2)
        self.assertEqual(metrics["turnover_sum"], 0.3)

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
        for uri, generation in ((root + "/../escape", "1"),
                                ("gs://elsewhere/research/x", "1"), (root + "/bundle", "")):
            with self.subTest(uri=uri):
                with self.assertRaises(research.ResearchStop):
                    research.reference({"uri": uri, "generation": generation, "sha256": "a" * 64}, root)

    def test_cloud_read_checks_generation_digest_and_never_echoes_provider_error(self):
        root = "gs://synthetic-example/research/paired"
        ref = {"uri": root + "/bundle", "generation": "7", "sha256": "a" * 64}
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
