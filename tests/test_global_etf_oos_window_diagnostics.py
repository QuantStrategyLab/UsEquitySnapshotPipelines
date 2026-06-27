from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines import global_etf_oos_window_diagnostics as diagnostics


class GlobalEtfOosWindowDiagnosticsTests(unittest.TestCase):
    def test_build_global_etf_oos_window_diagnostics_selects_worst_promoted_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pd.DataFrame(
                [
                    {
                        "Test Window": 2024,
                        "Selected Candidate": "candidate_a",
                        "Selection Action": "promote_candidate",
                        "Test Excess CAGR vs Baseline": 0.05,
                        "Test Drawdown Delta vs Baseline": 0.01,
                    },
                    {
                        "Test Window": 2025,
                        "Selected Candidate": "candidate_b",
                        "Selection Action": "promote_candidate",
                        "Test Excess CAGR vs Baseline": -0.10,
                        "Test Drawdown Delta vs Baseline": -0.02,
                    },
                ]
            ).to_csv(root / "walk_forward_selection_windows.csv", index=False)
            pd.DataFrame(
                [
                    {"rank": 1, "Candidate": "candidate_a", "Display Name": "A", "Candidate Group": "liveable_candidate"},
                    {"rank": 2, "Candidate": "candidate_b", "Display Name": "B", "Candidate Group": "liveable_candidate"},
                ]
            ).to_csv(root / "ranking.csv", index=False)
            pd.DataFrame(
                [
                    {"as_of": "2025-01-31", "live_global_etf_rotation_defensive_baseline": 0.01, "candidate_a": 0.02, "candidate_b": -0.03},
                    {"as_of": "2025-02-28", "live_global_etf_rotation_defensive_baseline": 0.01, "candidate_a": 0.01, "candidate_b": -0.02},
                    {"as_of": "2025-03-31", "live_global_etf_rotation_defensive_baseline": 0.00, "candidate_a": 0.00, "candidate_b": -0.01},
                ]
            ).to_csv(root / "portfolio_returns_with_benchmarks.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "candidate_id": "candidate_b",
                        "as_of": "2025-01-30",
                        "next_date": "2025-01-31",
                        "signal_description": "rule_b",
                        "overlay_weight": 0.10,
                        "base_candidate_id": "baseline",
                        "overlay_candidate_id": "overlay",
                    }
                ]
            ).to_csv(root / "rebalance_events.csv", index=False)

            result = diagnostics.build_global_etf_oos_window_diagnostics(artifact_dir=root, top_n_candidates=2)

            self.assertEqual(result["summary"]["test_year"], 2025)
            self.assertEqual(result["summary"]["selected_candidate"], "candidate_b")
            self.assertEqual(result["summary"]["worst_month"], "2025-01")
            self.assertEqual(pd.DataFrame(result["selected_candidate_signals"])["candidate_id"].tolist(), ["candidate_b"])

    def test_main_writes_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out = root / "out"
            pd.DataFrame(
                [
                    {
                        "Test Window": 2025,
                        "Selected Candidate": "candidate_b",
                        "Selection Action": "promote_candidate",
                        "Test Excess CAGR vs Baseline": -0.10,
                        "Test Drawdown Delta vs Baseline": -0.02,
                    }
                ]
            ).to_csv(root / "walk_forward_selection_windows.csv", index=False)
            pd.DataFrame(
                [{"rank": 1, "Candidate": "candidate_b", "Display Name": "B", "Candidate Group": "liveable_candidate"}]
            ).to_csv(root / "ranking.csv", index=False)
            pd.DataFrame(
                [
                    {"as_of": "2025-01-31", "live_global_etf_rotation_defensive_baseline": 0.01, "candidate_b": -0.03},
                ]
            ).to_csv(root / "portfolio_returns_with_benchmarks.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "candidate_id": "candidate_b",
                        "as_of": "2025-01-30",
                        "next_date": "2025-01-31",
                        "signal_description": "rule_b",
                        "overlay_weight": 0.10,
                    }
                ]
            ).to_csv(root / "rebalance_events.csv", index=False)

            rc = diagnostics.main(["--artifact-dir", str(root), "--output-dir", str(out)])

            self.assertEqual(rc, 0)
            self.assertTrue((out / "worst_oos_window_summary.json").exists())
            self.assertTrue((out / "worst_oos_window_report.md").exists())
            payload = json.loads((out / "worst_oos_window_summary.json").read_text())
            self.assertEqual(payload["selected_candidate"], "candidate_b")
