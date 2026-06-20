from __future__ import annotations

import unittest
import pandas as pd

from us_equity_snapshot_pipelines import global_etf_offensive_rotation_research as research


def _price_history() -> pd.DataFrame:
    dates = pd.bdate_range(start="2024-01-02", periods=320)
    rows: list[dict[str, object]] = []
    growth = {"AAA": 0.0012, "SPY": 0.0006, "QQQ": 0.0008, "BIL": 0.00005}
    for symbol, daily_return in growth.items():
        price = 100.0
        for as_of in dates:
            price *= 1.0 + daily_return
            rows.append({"symbol": symbol, "as_of": as_of, "close": price, "volume": 1000})
    return pd.DataFrame(rows)


class GlobalEtfOffensiveRotationResearchTests(unittest.TestCase):
    def test_candidate_id_parser_preserves_case_and_underscores(self) -> None:
        candidate_ids = research._normalize_candidate_ids("offensive_growth_fast_top2_monthly, Live_Baseline ")

        self.assertEqual(candidate_ids, ("offensive_growth_fast_top2_monthly", "Live_Baseline"))

    def test_collect_required_symbols_includes_benchmarks_and_safe_haven_once(self) -> None:
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="test",
            display_name="Test",
            candidate_group="offensive_candidate",
            rule="monthly_top1",
            ranking_pool=("AAA", "SPY", "AAA"),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
        )

        symbols = research.collect_required_symbols((variant,))

        self.assertEqual(symbols, ("AAA", "SPY", "BIL", "QQQ"))

    def test_parse_periods_accepts_name_start_end_entries(self) -> None:
        periods = research._parse_periods("short:2025-01-01:,long:2020-01-01:2024-12-31")

        self.assertEqual(periods, (("short", "2025-01-01", None), ("long", "2020-01-01", "2024-12-31")))

    def test_run_offensive_research_returns_period_summary_ranking_and_weights(self) -> None:
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_test_top1",
            display_name="Offensive Test Top1",
            candidate_group="offensive_candidate",
            rule="monthly_top1",
            ranking_pool=("AAA",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
        )
        periods = (
            ("short", "2025-01-02", None),
            ("medium", "2024-08-01", None),
            ("long", "2024-01-02", None),
        )

        result = research.run_offensive_research(
            price_history=_price_history(),
            periods=periods,
            variants=(variant,),
            turnover_cost_bps=0.0,
        )

        self.assertEqual(result["period_summary"].shape[0], 3)
        self.assertEqual(result["ranking"]["Candidate"].iloc[0], "offensive_test_top1")
        self.assertIn("weights_offensive_test_top1", result)
        self.assertFalse(result["portfolio_returns"].empty)
        self.assertFalse(result["signal_history"].empty)

    def test_build_ranking_marks_spy_beating_qqq_drawdown_advantaged_candidate_for_review(self) -> None:
        rows = []
        for period in ("short", "medium", "long"):
            rows.append(
                {
                    "Period": period,
                    "Candidate": "offensive_candidate",
                    "Display Name": "Offensive Candidate",
                    "Candidate Group": "offensive_candidate",
                    "Rule": "monthly_top2",
                    "Primary Benchmark Symbol": "SPY",
                    "Secondary Benchmark Symbol": "QQQ",
                    "Trading Days": 120,
                    "CAGR": 0.18,
                    "Sharpe": 1.1,
                    "Max Drawdown": -0.10,
                    "Excess CAGR vs Benchmark": 0.04,
                    "Excess CAGR vs Secondary Benchmark": -0.01,
                    "Benchmark Max Drawdown": -0.15,
                    "Secondary Benchmark Max Drawdown": -0.20,
                    "Turnover/Year": 5.0,
                }
            )
        ranking = research.build_ranking(pd.DataFrame(rows))

        self.assertTrue(bool(ranking["research_gate_passed"].iloc[0]))
        self.assertTrue(bool(ranking["paper_review_candidate"].iloc[0]))
        self.assertEqual(ranking["review_action"].iloc[0], "paper_review_only")

    def test_build_candidate_robustness_diagnostics_emits_windows_and_summary(self) -> None:
        dates = pd.bdate_range(start="2020-01-02", periods=900)
        portfolio_returns = pd.DataFrame(
            {
                "candidate_a": [0.001] * len(dates),
                "candidate_b": [0.0005] * len(dates),
            },
            index=dates,
        )
        rows = []
        for symbol, daily_return in {"SPY": 0.0006, "QQQ": 0.0008, "BIL": 0.00005}.items():
            price = 100.0
            for as_of in dates:
                price *= 1.0 + daily_return
                rows.append({"symbol": symbol, "as_of": as_of, "close": price})
        weights = pd.DataFrame({"AAA": [1.0] * len(dates)}, index=dates)
        diagnostics = research.build_candidate_robustness_diagnostics(
            price_history=pd.DataFrame(rows),
            portfolio_returns=portfolio_returns,
            weights_by_candidate={"candidate_a": weights},
            candidate_ids=("candidate_a",),
            rolling_years=(3,),
            min_calendar_trading_days=120,
            min_rolling_trading_days_per_year=180,
        )

        self.assertFalse(diagnostics["robustness_windows"].empty)
        self.assertFalse(diagnostics["robustness_summary"].empty)
        self.assertIn("SPY CAGR Win Rate", diagnostics["robustness_summary"].columns)
        self.assertEqual(set(diagnostics["robustness_windows"]["Candidate"]), {"candidate_a"})
        self.assertTrue(pd.to_numeric(diagnostics["robustness_summary"]["Median Turnover/Year"]).notna().all())

    def test_period_end_keeps_open_ended_windows_unbounded(self) -> None:
        periods = (("closed", "2020-01-01", "2020-12-31"), ("open", "2023-01-01", None))

        self.assertIsNone(research._period_end(periods))

    def test_eaa_score_mode_runs_and_emits_new_candidate(self) -> None:
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_test_eaa",
            display_name="Offensive Test EAA",
            candidate_group="offensive_candidate",
            rule="monthly_top2_eaa",
            ranking_pool=("AAA", "QQQ"),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=2,
            canary_bad_threshold=99,
            score_mode="eaa_generalized",
        )
        result = research.run_offensive_research(
            price_history=_price_history(),
            periods=(("long", "2024-01-02", None),),
            variants=(variant,),
            turnover_cost_bps=0.0,
        )

        self.assertEqual(result["ranking"]["Candidate"].iloc[0], "offensive_test_eaa")
        self.assertEqual(result["ranking"]["Score Mode"].iloc[0], "eaa_generalized")
        self.assertFalse(result["portfolio_returns"].empty)

    def test_cash_fraction_canary_allocates_safe_haven_when_canary_is_bad(self) -> None:
        prices = _price_history()
        dates = pd.bdate_range(start="2024-01-02", periods=320)
        canary_rows = []
        price = 100.0
        for as_of in dates:
            price *= 0.999
            canary_rows.append({"symbol": "CAN", "as_of": as_of, "close": price, "volume": 1000})
        prices = pd.concat([prices, pd.DataFrame(canary_rows)], ignore_index=True)
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_test_daa_cash_fraction",
            display_name="Offensive Test DAA Cash Fraction",
            candidate_group="offensive_candidate",
            rule="monthly_top1_daa_cash_fraction",
            ranking_pool=("AAA",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("CAN",),
            top_n=1,
            canary_bad_threshold=99,
            canary_mode="cash_fraction",
            safe_fraction_per_bad_canary=0.50,
        )
        result = research.run_offensive_research(
            price_history=prices,
            periods=(("long", "2024-01-02", None),),
            variants=(variant,),
            turnover_cost_bps=0.0,
        )

        events = result["signal_history"].filter(regex="candidate_id|weight_BIL|weight_AAA")
        weighted_events = events.loc[pd.to_numeric(events["weight_BIL"], errors="coerce").gt(0.0)]
        self.assertFalse(weighted_events.empty)
        self.assertAlmostEqual(float(weighted_events["weight_BIL"].max()), 1.0)


if __name__ == "__main__":
    unittest.main()
