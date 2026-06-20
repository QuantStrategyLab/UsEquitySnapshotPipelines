from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from us_equity_snapshot_pipelines import live_strategy_health as health


def _return_matrix() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=620)
    return pd.DataFrame(
        {
            "as_of": dates,
            "strong_strategy": [0.0010] * len(dates),
            "weak_strategy": [0.0002] * len(dates),
            "buy_hold_SPY": [0.0005] * len(dates),
        }
    )


def _short_window_watch_matrix() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=620)
    strategy = [0.0015] * 540 + [-0.0005] * 80
    return pd.DataFrame(
        {
            "as_of": dates,
            "long_term_winner_recent_lag": strategy,
            "buy_hold_SPY": [0.0004] * len(dates),
        }
    )


class LiveStrategyHealthTests(unittest.TestCase):
    def test_health_summary_flags_underperformer_without_drawdown_advantage(self) -> None:
        window_health = health.build_strategy_window_health(
            _return_matrix(),
            strategies=("strong_strategy", "weak_strategy"),
            primary_benchmark="buy_hold_SPY",
            policy=health.HealthPolicy(min_observations=60),
        )
        summary = health.build_strategy_health_summary(window_health).set_index("strategy")

        self.assertEqual(summary.loc["strong_strategy", "overall_health_state"], health.KEEP)
        self.assertEqual(summary.loc["weak_strategy", "overall_health_state"], health.REVIEW_FOR_RETIREMENT)
        self.assertGreater(summary.loc["strong_strategy", "full_window_excess_cagr"], 0.0)
        self.assertLess(summary.loc["weak_strategy", "full_window_excess_cagr"], 0.0)

    def test_non_full_window_failure_downgrades_overall_to_watch(self) -> None:
        window_health = health.build_strategy_window_health(
            _short_window_watch_matrix(),
            strategies=("long_term_winner_recent_lag",),
            primary_benchmark="buy_hold_SPY",
            policy=health.HealthPolicy(min_observations=60),
        )
        summary = health.build_strategy_health_summary(window_health).set_index("strategy")

        self.assertEqual(summary.loc["long_term_winner_recent_lag", "overall_health_state"], health.WATCH)
        self.assertGreater(summary.loc["long_term_winner_recent_lag", "full_window_excess_cagr"], 0.0)
        self.assertIn("trailing_3m", summary.loc["long_term_winner_recent_lag", "watch_windows"])

    def test_missing_strategy_column_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing columns"):
            health.build_strategy_window_health(
                _return_matrix(),
                strategies=("missing_strategy",),
                primary_benchmark="buy_hold_SPY",
            )

    def test_cli_writes_health_report_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            returns_path = Path(tmp) / "returns.csv"
            output_dir = Path(tmp) / "health"
            _return_matrix().to_csv(returns_path, index=False)

            exit_code = health.main(
                [
                    "--returns",
                    str(returns_path),
                    "--strategies",
                    "strong_strategy,weak_strategy",
                    "--primary-benchmark",
                    "buy_hold_SPY",
                    "--output-dir",
                    str(output_dir),
                ]
            )

            self.assertEqual(exit_code, 0)
            expected = {
                "strategy_health_summary.csv",
                "strategy_health_windows.csv",
                "strategy_health_report.md",
                "run_manifest.json",
            }
            for name in expected:
                self.assertTrue((output_dir / name).exists(), name)

            manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["artifact_type"], "live_strategy_health_report")
            report = (output_dir / "strategy_health_report.md").read_text(encoding="utf-8")
            self.assertIn("evidence layer only", report)
            self.assertIn("review_for_retirement", report)


if __name__ == "__main__":
    unittest.main()
