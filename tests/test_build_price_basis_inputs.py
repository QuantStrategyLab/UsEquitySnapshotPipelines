"""Offline synthetic inputs; no provider, broker or cloud client."""
import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("build_inputs", SCRIPTS / "build_price_basis_inputs.py")
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


def momentum(context):
    prices = context["history"][-1]["signal_ohlc"]["SYNTH"]
    previous = context["history"][-2]["signal_ohlc"]["SYNTH"]
    signal = prices["close"] / previous["close"] - 1
    return {"signals": {"momentum": signal}, "weights": {"SYNTH": 0.5 if signal > 0 else 0},
            "threshold_distances": {"momentum": signal}}


def fixture():
    sessions = ["2020-12-30", "2020-12-31", "2023-01-04", "2023-01-05", "2023-01-06"]
    def ohlc(p):
        return {"open": p, "high": p, "low": p, "close": p}
    market = []
    for i, session in enumerate(sessions):
        market.append({"session": session, "raw_ohlc": {"SYNTH": ohlc(10)},
                       "signal_ohlc": {
                           "split_adjusted": {"SYNTH": ohlc(10 + i)},
                           "split_dividend_adjusted": {"SYNTH": ohlc(10 - i / 2)}},
                       "available_session": session, "features": {}})
    inputs = {
        "artifact_kind": "SYNTHETIC", "calendar": {"name": "SYNTHETIC", "sessions": sessions},
        "market": market, "corporate_actions": [], "corporate_action_coverage_complete": True, "initial_state": {"cash": 1000.0, "shares": {"SYNTH": 0},
                                                                     "marks": {"SYNTH": 10}},
        "external_flows": {s: 0 for s in sessions[2:]},
        "config": {"warmup_sessions": 2, "min_notional": 1, "whole_shares": True,
                   "limits": {"total": 0.9, "symbols": {"SYNTH": 0.8}},
                   "size_bucket_edges": [0.01, 0.1], "threshold_bucket_edges": [0.001, 0.01],
                   "thresholds_applicable": True, "options_enabled": False},
        "costs": {key: {"status": "not_applicable", "reason_or_version": "synthetic equity fixture", "bps": 0, "model": "linear_bps"}
                  for key in build.consumer.COST_COMPONENTS},
        "plugin_binding": {"adapter": "synthetic momentum", "dependency_lock": "synthetic no dependencies"},
        "identity": {"platform_revision": "a" * 40, "strategy_revision": "b" * 40},
        "as_of": sessions[-1], "inception": sessions[2],
    }
    inputs["costs"]["commission"] = {"status": "modeled", "reason_or_version": "synthetic-bps-v1", "bps": 10, "model": "linear_bps"}
    return inputs


class ProducerTests(unittest.TestCase):
    def produce(self, inputs=None):
        return build.produce(inputs or fixture(), momentum)

    def test_independent_basis_and_stress_accounts(self):
        out = self.produce()
        rows = out["bundle"]["rows"]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["baseline"]["trade_count"], 0)
        self.assertEqual(rows[0]["candidate"]["trade_count"], 1)
        self.assertLess(rows[0]["candidate"]["net_return_2x"], rows[0]["candidate"]["net_return_1x"])
        self.assertEqual(rows[0]["candidate"]["gross_return"], 0)
        paths = out["audit"]["candidate"]
        self.assertGreater(build.nav(paths[0], paths[0]["marks"]), build.nav(paths[1], paths[1]["marks"]))
        self.assertGreater(build.nav(paths[1], paths[1]["marks"]), build.nav(paths[2], paths[2]["marks"]))
        self.assertEqual(out["qualification"]["status"], "SYNTHETIC_ONLY")
        opening = out["window_opening_states"]["trailing_two_years"]
        self.assertEqual(set(opening), {"baseline", "candidate"})
        self.assertEqual(len(opening["baseline"]), 3)
        self.assertEqual(opening["baseline"][0]["opening_nav"], 1000)

    def test_causality_and_input_immutability(self):
        source = fixture()
        before = copy.deepcopy(source)
        seen = []
        def observe(context):
            seen.append(context["history"][-1]["session"] < context["fill_session"])
            context["history"][0]["features"]["mutated"] = 1
            return momentum(context)
        build.produce(source, observe)
        self.assertTrue(all(seen))
        self.assertEqual(source, before)

    def test_future_feature_and_missing_warmup_rejected(self):
        for change in (lambda x: x["market"][1].update(available_session="2023-01-04"),
                       lambda x: x["config"].update(warmup_sessions=1)):
            source = fixture()
            change(source)
            with self.assertRaises(build.consumer.ResearchStop):
                self.produce(source)

    def test_carry_uses_inherited_balances_before_fills_and_flows(self):
        source = fixture()
        source["costs"]["cash_yield"].update(status="modeled", bps=36525)
        source["costs"]["fund_expenses"].update(status="modeled", bps=3652.5)
        source["costs"]["commission"]["bps"] = 0
        state = {"cash": 500, "shares": {"SYNTH": 50}, "marks": {"SYNTH": 10}, "session": "2023-01-03"}
        decision = {"signals": {"synthetic": 1}, "weights": {"SYNTH": 0.8}, "threshold_distances": {"entry": 1}}
        result = build.step(state, source["market"][2], decision, source, 1)
        # 500 cash earns 5, inherited 500 equities cost 0.5, before buying 30 shares.
        self.assertAlmostEqual(build.nav(state, state["marks"]), 1004.5)
        self.assertAlmostEqual(result["return"], 0.0045)

    def test_external_flow_is_not_return(self):
        source = fixture()
        source["external_flows"]["2023-01-05"] = 100
        out = self.produce(source)
        self.assertAlmostEqual(out["bundle"]["rows"][1]["baseline"]["gross_return"], 0)
        self.assertEqual(out["audit"]["baseline"][0]["cash"], 1100)

    def test_split_and_dividend_on_inherited_holdings(self):
        source = fixture()
        source["initial_state"].update(cash=0, shares={"SYNTH": 100})
        source["corporate_actions"] = [
            {"session": "2023-01-04", "symbol": "SYNTH", "kind": "split", "ratio": 2},
            {"session": "2023-01-04", "symbol": "SYNTH", "kind": "dividend", "cash_per_share": 0.5}]
        for row in source["market"][2:]:
            row["raw_ohlc"]["SYNTH"] = {key: 5 for key in ("open", "high", "low", "close")}
        out = self.produce(source)
        self.assertAlmostEqual(out["bundle"]["rows"][0]["baseline"]["gross_return"], 0.1)

    def test_missing_inputs_costs_and_unsupported_options_rejected(self):
        changes = [lambda x: x.update(initial_state=None),
                   lambda x: x.update(corporate_action_coverage_complete=False),
                   lambda x: x["costs"]["commission"].update(minimum_fee=1),
                   lambda x: x["costs"].pop("impact"),
                   lambda x: x["config"].update(options_enabled=True),
                   lambda x: x["market"][2]["raw_ohlc"].pop("SYNTH"),
                   lambda x: x["external_flows"].pop("2023-01-05")]
        for change in changes:
            source = fixture()
            change(source)
            with self.assertRaises(build.consumer.ResearchStop):
                self.produce(source)

    def test_bundle_hash_and_consumer_compatibility(self):
        out = self.produce()
        result = build.consumer.compare(out["metadata"], out["bundle"], fixture()["calendar"]["sessions"])
        self.assertEqual(result["windows"]["since_inception"]["status"], "AGGREGATED")
        self.assertEqual(out["bundle_sha256"], build.digest(out["bundle"]))
        changed = copy.deepcopy(out)
        changed["bundle"]["rows"][0]["candidate"]["orders"]["SYNTH"] = 0
        with self.assertRaisesRegex(build.consumer.ResearchStop, "BUNDLE_DIGEST"):
            build.validate_product(changed)

    def test_actual_weights_and_cost_path_fill_accounting(self):
        source = fixture()
        source["config"]["min_notional"] = 1000
        out = self.produce(source)
        self.assertEqual(out["bundle"]["rows"][0]["candidate"]["weights"]["SYNTH"], 0)
        self.assertEqual(out["bundle"]["rows"][0]["candidate"]["trade_count"], 0)
        out = self.produce()
        arm = out["bundle"]["rows"][0]["candidate"]
        self.assertEqual(set(arm["cost_paths"]), {"gross_return", "net_return_1x", "net_return_2x"})
        self.assertEqual(arm["orders"], arm["cost_paths"]["net_return_1x"]["orders"])
        changed = copy.deepcopy(out["bundle"]["rows"])
        changed[0]["candidate"]["cost_paths"]["gross_return"]["trade_count"] = 9
        stats = build.consumer.performance(changed, "candidate", "gross_return")
        self.assertEqual(stats["trade_count"], sum(r["candidate"]["cost_paths"]["gross_return"]["trade_count"]
                                                  for r in changed))
        bad_thresholds = copy.deepcopy(out["bundle"])
        bad_thresholds["rows"][0]["candidate"]["cost_paths"]["net_return_2x"]["threshold_distances"] = {}
        with self.assertRaisesRegex(build.consumer.ResearchStop, "THRESHOLD_COVERAGE_MISMATCH"):
            build.consumer.compare(out["metadata"], bad_thresholds, out["replay_sessions"])
        changed[0]["candidate"]["cost_paths"]["gross_return"]["return"] = 0.1
        with self.assertRaisesRegex(build.consumer.ResearchStop, "COST_PATH_RETURN_MISMATCH"):
            build.consumer.compare(out["metadata"], {**out["bundle"], "rows": changed}, out["replay_sessions"])

    def test_bad_prices_caps_flows_and_cost_declarations(self):
        changes = [lambda x: x["market"][0]["raw_ohlc"]["SYNTH"].update(close=float("nan")),
                   lambda x: x["config"]["limits"].update(total=1.1),
                   lambda x: x["external_flows"].update({"2023-01-04": -2000}),
                   lambda x: x["costs"]["impact"].update(bps=1),
                   lambda x: x["costs"]["borrow_margin"].update(status="modeled"),
                   lambda x: x["initial_state"]["shares"].update(SYNTH=0.5)]
        for change in changes:
            source = fixture()
            change(source)
            with self.assertRaises(build.consumer.ResearchStop):
                self.produce(source)

    def test_missing_reporting_sessions_not_hidden_as_warmup(self):
        source = fixture()
        source["calendar"]["sessions"][0] = "2023-01-02"
        source["calendar"]["sessions"][1] = "2023-01-03"
        for i in (0, 1):
            source["market"][i]["session"] = source["calendar"]["sessions"][i]
            source["market"][i]["available_session"] = source["market"][i]["session"]
        with self.assertRaisesRegex(build.consumer.ResearchStop, "SESSION_COVERAGE_INCOMPLETE"):
            self.produce(source)

    def test_no_network_used(self):
        with patch("socket.socket.connect", side_effect=AssertionError("network forbidden")), \
             patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
            self.produce()

    def test_unknown_inception_only_blocks_inception_window(self):
        source = fixture()
        source.pop("inception")
        out = self.produce(source)
        result = build.consumer.compare(out["metadata"], out["bundle"], source["calendar"]["sessions"])
        self.assertEqual(result["windows"]["trailing_two_years"]["status"], "AGGREGATED")
        self.assertEqual(result["windows"]["since_inception"]["status"], "NOT_COMPUTED")

    def test_real_producer_cannot_be_qualified_by_declaration(self):
        source = fixture()
        source["artifact_kind"] = "REVIEWED_REAL_INPUTS"
        with self.assertRaisesRegex(build.consumer.ResearchStop, "PRODUCER_REVIEW"):
            self.produce(source)


class ManifestTests(unittest.TestCase):
    def entries(self, directory):
        product = build.produce(fixture(), momentum)
        entries = []
        for i in range(1, 5):
            path = directory / f"target-{i}.json"
            path.write_bytes(build.consumer.canonical(product))
            entries.append({"alias": f"target-{i}", "product_path": str(path),
                            "replay_bundle": {"object": f"research/synthetic/target-{i}.json", "generation": "1",
                                              "sha256": product["bundle_sha256"]}})
        return entries

    def test_four_synthetic_targets_eight_structural_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = build.build_manifest(self.entries(Path(tmp)), "gs://example-fixtures/research/synthetic",
                                            synthetic=True)
        self.assertEqual(manifest["structurally_ready_windows"], 8)
        self.assertFalse(manifest["dispatch_ready"])
        tampered = copy.deepcopy(manifest)
        tampered["structurally_ready_windows"] = 7
        with self.assertRaisesRegex(build.consumer.ResearchStop, "WINDOW_COUNT_INVALID"):
            build.validate_reviewed_manifest(tampered, "gs://example-fixtures/research/synthetic", synthetic=True)
        with self.assertRaisesRegex(build.consumer.ResearchStop, "SYNTHETIC_MANIFEST"):
            build.consumer.validate_manifest(manifest, "gs://example-fixtures/research/synthetic")

    def test_synthetic_cannot_be_reviewed_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(build.consumer.ResearchStop, "REAL_QUALIFICATION"):
                build.build_manifest(self.entries(Path(tmp)), "gs://example-fixtures/research/synthetic")

    def test_null_unknown_and_mismatched_bytes_refused(self):
        for value in (None, "unknown", "", "null"):
            with tempfile.TemporaryDirectory() as tmp:
                entries = self.entries(Path(tmp))
                entries[0]["replay_bundle"]["generation"] = value
                with self.assertRaises(build.consumer.ResearchStop):
                    build.build_manifest(entries, "gs://example-fixtures/research/synthetic", synthetic=True)
        with tempfile.TemporaryDirectory() as tmp:
            entries = self.entries(Path(tmp))
            entries[0]["replay_bundle"]["sha256"] = "f" * 64
            with self.assertRaisesRegex(build.consumer.ResearchStop, "BUNDLE_DIGEST"):
                build.build_manifest(entries, "gs://example-fixtures/research/synthetic", synthetic=True)

    def test_duplicate_alias_and_unsafe_references(self):
        changes = [lambda x: x[1].update(alias="target-1"),
                   lambda x: x[0]["replay_bundle"].update(object="research/synthetic/../other.json"),
                   lambda x: x[0]["replay_bundle"].update(generation=True)]
        for change in changes:
            with tempfile.TemporaryDirectory() as tmp:
                entries = self.entries(Path(tmp))
                change(entries)
                with self.assertRaises(build.consumer.ResearchStop):
                    build.build_manifest(entries, "gs://example-fixtures/research/synthetic", synthetic=True)

    def test_review_bound_real_mode_and_changed_identity(self):
        source = fixture()
        # This is still fabricated input used only to test the reviewed-mode checks.
        source["artifact_kind"] = "REVIEWED_REAL_INPUTS"
        source["plugin_binding"].update(dependency_lock={"example": "1"}, target_evidence={"synthetic": True})
        identity = build.source_identity(momentum)
        source["review"] = {"status": "PASS", "scope": "equity_only", "reviewer": "synthetic-reviewer",
                            "reviewed_at": "2023-01-07", **identity, "input_sha256": build.digest(source),
                            "dependency_lock_sha256": build.digest(source["plugin_binding"]["dependency_lock"]),
                            "target_evidence_sha256": build.digest(source["plugin_binding"]["target_evidence"])}
        product = build.produce(source, momentum)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "product.json"
            path.write_bytes(build.consumer.canonical(product))
            entries = [{"alias": "target-1", "product_path": str(path),
                        "replay_bundle": {"object": "research/synthetic/bundle.json", "generation": "1",
                                          "sha256": product["bundle_sha256"]},
                        "review": {"status": "PASS", "reviewer": "synthetic-reviewer", "reviewed_at": "2023-01-07",
                                   "product_sha256": product["product_sha256"]}}]
            entries[0]["review"].update(metadata_sha256=build.digest(product["metadata"]),
                                         replay_bundle_reference_sha256=build.digest(entries[0]["replay_bundle"]))
            manifest = build.build_manifest(entries, "gs://example-fixtures/research/synthetic")
            self.assertEqual(len(build.validate_reviewed_manifest(manifest,
                                                                  "gs://example-fixtures/research/synthetic")), 1)
            tampered = copy.deepcopy(manifest)
            tampered["targets"][0]["replay_bundle"]["generation"] = "2"
            with self.assertRaisesRegex(build.consumer.ResearchStop, "BUNDLE_REVIEW_REQUIRED"):
                build.validate_reviewed_manifest(tampered, "gs://example-fixtures/research/synthetic")
            tampered = copy.deepcopy(manifest)
            tampered["targets"][0]["metadata"]["as_of"] = "2023-01-05"
            with self.assertRaisesRegex(build.consumer.ResearchStop, "BUNDLE_REVIEW_REQUIRED"):
                build.validate_reviewed_manifest(tampered, "gs://example-fixtures/research/synthetic")
            del manifest["targets"][0]["bundle_review"]
            with self.assertRaisesRegex(build.consumer.ResearchStop, "BUNDLE_REVIEW_REQUIRED"):
                build.validate_reviewed_manifest(manifest, "gs://example-fixtures/research/synthetic")
        source["config"]["min_notional"] = 2
        with self.assertRaisesRegex(build.consumer.ResearchStop, "PRODUCER_REVIEW_IDENTITY_MISMATCH"):
            build.produce(source, momentum)

    def test_cli_writes_only_requested_local_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            entries = self.entries(directory)
            request = directory / "request.json"
            request.write_text(json.dumps({"entries": entries, "input_root": "gs://example-fixtures/research/synthetic"}))
            output = directory / "manifest.json"
            with patch("sys.argv", ["build", "manifest", "--input", str(request), "--output", str(output), "--synthetic"]):
                build.main()
            self.assertEqual(json.loads(output.read_bytes())["structurally_ready_windows"], 8)


if __name__ == "__main__":
    unittest.main()
