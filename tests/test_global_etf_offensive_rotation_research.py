from __future__ import annotations

import json
import unittest
import tempfile
from pathlib import Path
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

    def test_candidate_filters_reject_unknown_ids(self) -> None:
        variants = research._filter_variants(("live_global_etf_rotation_defensive_baseline",))
        composites = research._filter_liveable_composites(("liveable_blend_baseline90_fast10",))

        self.assertEqual(variants[0].candidate_id, "live_global_etf_rotation_defensive_baseline")
        self.assertEqual(composites[0].candidate_id, "liveable_blend_baseline90_fast10")
        with self.assertRaisesRegex(ValueError, "unknown variant"):
            research._filter_variants(("missing",))
        with self.assertRaisesRegex(ValueError, "unknown liveable"):
            research._filter_liveable_composites(("missing",))

    def test_parse_float_list_accepts_comma_separated_bps_values(self) -> None:
        values = research._parse_float_list("5,10, 25")

        self.assertEqual(values, (5.0, 10.0, 25.0))

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

    def test_portfolio_returns_with_benchmarks_adds_qqq_and_spy_returns(self) -> None:
        prices = _price_history()
        result = research.run_offensive_research(
            price_history=prices,
            periods=(("long", "2024-01-02", None),),
            variants=(
                research.GlobalEtfOffensiveVariantSpec(
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
                ),
            ),
            liveable_composites=(),
            turnover_cost_bps=0.0,
        )

        combined = research.build_portfolio_returns_with_benchmarks(
            price_history=prices,
            portfolio_returns=result["portfolio_returns"],
        )

        self.assertIn("offensive_test_top1", combined.columns)
        self.assertIn("QQQ", combined.columns)
        self.assertIn("SPY", combined.columns)
        self.assertFalse(combined[["QQQ", "SPY"]].dropna().empty)

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

    def test_build_ranking_marks_liveable_candidate_for_live_design_review(self) -> None:
        rows = []
        for period in ("short", "medium", "long"):
            rows.append(
                {
                    "Period": period,
                    "Candidate": "liveable_candidate",
                    "Display Name": "Liveable Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
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
        self.assertTrue(bool(ranking["live_review_candidate"].iloc[0]))
        self.assertFalse(bool(ranking["paper_review_candidate"].iloc[0]))
        self.assertEqual(ranking["review_action"].iloc[0], "live_design_review")

    def test_static_liveable_composite_recomputes_returns_from_combined_weights(self) -> None:
        prices = _price_history()
        close = research._normalize_price_history(prices)
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="context",
            display_name="Context",
            candidate_group="offensive_candidate",
            rule="context",
            ranking_pool=("AAA", "QQQ"),
        )
        context = research._build_indicator_context(close, variants=(variant,))
        index = pd.DatetimeIndex(context.returns.index[1:])
        base_weights = pd.DataFrame({"AAA": 1.0}, index=index)
        overlay_weights = pd.DataFrame({"QQQ": 1.0}, index=index)
        spec = research.GlobalEtfLiveableCompositeSpec(
            candidate_id="liveable_test_static",
            display_name="Liveable Test Static",
            rule="static_blend_test",
            base_candidate_id="base",
            overlay_candidate_id="overlay",
            overlay_weight=0.25,
        )

        result = research.run_liveable_composite_backtest(
            spec=spec,
            context=context,
            base_weights=base_weights,
            overlay_weights=overlay_weights,
            turnover_cost_bps=0.0,
        )

        returns = pd.Series(result["portfolio_returns"])
        expected = 0.75 * context.returns.loc[returns.index, "AAA"] + 0.25 * context.returns.loc[returns.index, "QQQ"]
        pd.testing.assert_series_equal(returns, expected.rename(spec.candidate_id), check_freq=False)
        weights = pd.DataFrame(result["weights_history"])
        self.assertAlmostEqual(float(weights["AAA"].iloc[-1]), 0.75)
        self.assertAlmostEqual(float(weights["QQQ"].iloc[-1]), 0.25)

    def test_trend_drawdown_brake_reduces_overlay_to_floor_when_regime_weakens(self) -> None:
        dates = pd.bdate_range(start="2024-01-02", periods=260)
        rows: list[dict[str, object]] = []
        qqq_price = 100.0
        aaa_price = 100.0
        for pos, as_of in enumerate(dates):
            qqq_price *= 1.002 if pos < 170 else 0.995
            aaa_price *= 1.0002
            rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_price, "volume": 1_000_000})
            rows.append({"symbol": "AAA", "as_of": as_of, "close": aaa_price, "volume": 1_000_000})
        close = research._normalize_price_history(pd.DataFrame(rows))
        context = research._build_indicator_context(
            close,
            variants=(
                research.GlobalEtfOffensiveVariantSpec(
                    candidate_id="context",
                    display_name="Context",
                    candidate_group="offensive_candidate",
                    rule="context",
                    ranking_pool=("AAA", "QQQ"),
                ),
            ),
        )
        spec = research.GlobalEtfLiveableCompositeSpec(
            candidate_id="liveable_brake_test",
            display_name="Liveable Brake Test",
            rule="trend_drawdown_brake_test",
            base_candidate_id="base",
            overlay_candidate_id="overlay",
            overlay_weight=0.15,
            regime_symbol="QQQ",
            trend_sma_period=20,
            trend_fast_momentum_required=False,
            drawdown_window=10,
            drawdown_threshold=-0.03,
            min_overlay_weight=0.10,
        )

        weight = research._build_composite_overlay_weight(context, spec, target_index=context.returns.index)
        active = weight.loc[weight.gt(0.0)]

        self.assertAlmostEqual(float(active.max()), 0.15)
        self.assertAlmostEqual(float(active.min()), 0.10)

    def test_baseline_relative_decay_brake_cuts_overlay_after_child_underperforms_baseline(self) -> None:
        dates = pd.bdate_range(start="2024-01-02", periods=220)
        rows: list[dict[str, object]] = []
        aaa_price = 100.0
        qqq_price = 100.0
        for pos, as_of in enumerate(dates):
            aaa_price *= 1.0004
            qqq_price *= 1.0015 if pos < 120 else 0.9970
            rows.append({"symbol": "AAA", "as_of": as_of, "close": aaa_price, "volume": 1_000_000})
            rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_price, "volume": 1_000_000})
        close = research._normalize_price_history(pd.DataFrame(rows))
        context = research._build_indicator_context(
            close,
            variants=(
                research.GlobalEtfOffensiveVariantSpec(
                    candidate_id="context",
                    display_name="Context",
                    candidate_group="offensive_candidate",
                    rule="context",
                    ranking_pool=("AAA", "QQQ"),
                ),
            ),
        )
        base_weights = pd.DataFrame({"AAA": 1.0}, index=context.returns.index)
        overlay_weights = pd.DataFrame({"QQQ": 1.0}, index=context.returns.index)
        spec = research.GlobalEtfLiveableCompositeSpec(
            candidate_id="liveable_relative_decay_test",
            display_name="Liveable Relative Decay Test",
            rule="baseline_relative_decay_brake_test",
            base_candidate_id="base",
            overlay_candidate_id="overlay",
            overlay_weight=0.10,
            min_overlay_weight=0.0,
            relative_decay_fast_window=20,
            relative_decay_slow_window=40,
            relative_decay_fast_threshold=-0.03,
            relative_decay_slow_threshold=0.0,
        )

        weight = research._build_composite_overlay_weight(
            context,
            spec,
            target_index=context.returns.index,
            base_weights=base_weights,
            overlay_weights=overlay_weights,
        )

        self.assertAlmostEqual(float(weight.max()), 0.10)
        self.assertEqual(float(weight.tail(30).max()), 0.0)

    def test_run_offensive_research_adds_liveable_composites_when_children_exist(self) -> None:
        baseline = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="live_global_etf_rotation_defensive_baseline",
            display_name="Live Baseline",
            candidate_group="current_live_baseline",
            rule="monthly_top1_base",
            ranking_pool=("AAA",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
        )
        overlay = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_growth_fast_top2_monthly",
            display_name="Fast Overlay",
            candidate_group="offensive_candidate",
            rule="monthly_top1_overlay",
            ranking_pool=("QQQ",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
            score_mode="fast_136w",
        )

        result = research.run_offensive_research(
            price_history=_price_history(),
            periods=(("long", "2024-01-02", None),),
            variants=(baseline, overlay),
            turnover_cost_bps=0.0,
        )

        self.assertIn("liveable_blend_baseline80_fast20", set(result["ranking"]["Candidate"]))
        self.assertIn("weights_liveable_blend_baseline80_fast20", result)
        self.assertIn("liveable_blend_baseline80_fast20", result["portfolio_returns"].columns)
        self.assertIn("liveable_trend_drawdown_brake_baseline85_fast15_floor10", set(result["ranking"]["Candidate"]))
        self.assertIn("liveable_trend_drawdown_brake_baseline85_fast15_floor0", set(result["ranking"]["Candidate"]))
        self.assertIn(
            "liveable_baseline_relative_decay_brake_baseline90_fast10_floor0",
            set(result["ranking"]["Candidate"]),
        )

    def test_default_liveable_composites_include_static_sleeve_sensitivity_ladder(self) -> None:
        weights_by_id = {
            spec.candidate_id: float(spec.overlay_weight)
            for spec in research.GLOBAL_ETF_LIVEABLE_COMPOSITES
            if str(spec.rule).startswith("static_blend_baseline")
        }

        self.assertEqual(
            {
                "liveable_blend_baseline90_fast10": 0.10,
                "liveable_blend_baseline85_fast15": 0.15,
                "liveable_blend_baseline80_fast20": 0.20,
                "liveable_blend_baseline75_fast25": 0.25,
                "liveable_blend_baseline70_fast30": 0.30,
            },
            weights_by_id,
        )

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

    def test_live_readiness_summary_passes_only_baseline_robust_liveable_candidate(self) -> None:
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "baseline",
                    "Candidate Group": "current_live_baseline",
                    "CAGR": 0.10,
                    "Max Drawdown": -0.20,
                },
                {
                    "Period": "long",
                    "Candidate": "candidate",
                    "Candidate Group": "liveable_candidate",
                    "CAGR": 0.112,
                    "Max Drawdown": -0.19,
                },
            ]
        )
        ranking = pd.DataFrame(
            [
                {
                    "Candidate": "baseline",
                    "Candidate Group": "current_live_baseline",
                    "research_gate_passed": True,
                    "live_review_candidate": False,
                    "median_turnover_per_year": 3.0,
                },
                {
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "live_review_candidate": True,
                    "median_turnover_per_year": 4.0,
                },
            ]
        )
        robustness_windows = pd.DataFrame(
            [
                {
                    "Candidate": candidate,
                    "Window Type": window_type,
                    "Window": window,
                    "CAGR": cagr,
                    "Max Drawdown": max_drawdown,
                    "Turnover/Year": turnover,
                }
                for candidate, values in {
                    "baseline": (0.10, -0.20, 3.0),
                    "candidate": (0.12, -0.18, 4.0),
                }.items()
                for window_type, window in (
                    ("calendar_year", "2021"),
                    ("calendar_year", "2022"),
                    ("rolling_3y", "2020_2022"),
                    ("rolling_3y", "2021_2023"),
                    ("rolling_5y", "2019_2023"),
                    ("rolling_5y", "2020_2024"),
                )
                for cagr, max_drawdown, turnover in [values]
            ]
        )

        summary = research.build_live_readiness_summary(
            period_summary=period_summary,
            ranking=ranking,
            robustness_windows=robustness_windows,
            baseline_candidate="baseline",
        )

        self.assertEqual(summary["Candidate"].tolist(), ["candidate"])
        self.assertTrue(bool(summary["live_gate_passed"].iloc[0]))
        self.assertEqual(summary["live_action"].iloc[0], "candidate_for_live_promotion_review")
        self.assertEqual(summary["live_gate_reason"].iloc[0], "pass")

    def test_live_readiness_summary_rejects_weak_rolling_baseline_comparison(self) -> None:
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "baseline",
                    "Candidate Group": "current_live_baseline",
                    "CAGR": 0.10,
                    "Max Drawdown": -0.20,
                },
                {
                    "Period": "long",
                    "Candidate": "candidate",
                    "Candidate Group": "liveable_candidate",
                    "CAGR": 0.12,
                    "Max Drawdown": -0.19,
                },
            ]
        )
        ranking = pd.DataFrame(
            [
                {
                    "Candidate": "baseline",
                    "Candidate Group": "current_live_baseline",
                    "research_gate_passed": True,
                    "live_review_candidate": False,
                    "median_turnover_per_year": 3.0,
                },
                {
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "live_review_candidate": True,
                    "median_turnover_per_year": 4.0,
                },
            ]
        )
        robustness_windows = pd.DataFrame(
            [
                {
                    "Candidate": "baseline",
                    "Window Type": "rolling_5y",
                    "Window": "2019_2023",
                    "CAGR": 0.10,
                    "Max Drawdown": -0.20,
                    "Turnover/Year": 3.0,
                },
                {
                    "Candidate": "candidate",
                    "Window Type": "rolling_5y",
                    "Window": "2019_2023",
                    "CAGR": 0.06,
                    "Max Drawdown": -0.18,
                    "Turnover/Year": 4.0,
                },
            ]
        )

        summary = research.build_live_readiness_summary(
            period_summary=period_summary,
            ranking=ranking,
            robustness_windows=robustness_windows,
            baseline_candidate="baseline",
        )

        self.assertFalse(bool(summary["live_gate_passed"].iloc[0]))
        self.assertEqual(summary["live_action"].iloc[0], "continue_research")
        self.assertIn("rolling_5y_baseline_win_rate_below_60pct", summary["live_gate_reason"].iloc[0])

    def test_build_cost_stress_live_readiness_summary_runs_requested_costs(self) -> None:
        baseline = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="live_global_etf_rotation_defensive_baseline",
            display_name="Live Baseline",
            candidate_group="current_live_baseline",
            rule="monthly_top1_base",
            ranking_pool=("AAA",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
        )
        overlay = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_growth_fast_top2_monthly",
            display_name="Fast Overlay",
            candidate_group="offensive_candidate",
            rule="monthly_top1_overlay",
            ranking_pool=("QQQ",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
            score_mode="fast_136w",
        )
        composite = research.GlobalEtfLiveableCompositeSpec(
            candidate_id="liveable_test_baseline90_fast10",
            display_name="Liveable Test 90 / 10",
            rule="static_blend_baseline90_fast10",
            base_candidate_id="live_global_etf_rotation_defensive_baseline",
            overlay_candidate_id="offensive_growth_fast_top2_monthly",
            overlay_weight=0.10,
        )

        summary = research.build_cost_stress_live_readiness_summary(
            price_history=_price_history(),
            periods=(("long", "2024-01-02", None),),
            cost_bps_values=(0.0, 10.0),
            variants=(baseline, overlay),
            liveable_composites=(composite,),
            robustness_candidates=(
                "live_global_etf_rotation_defensive_baseline",
                "liveable_test_baseline90_fast10",
            ),
        )

        self.assertEqual(set(summary["turnover_cost_bps"]), {0.0, 10.0})
        self.assertEqual(set(summary["Candidate"]), {"liveable_test_baseline90_fast10"})
        self.assertIn("live_gate_passed", summary.columns)

    def test_build_candidate_liquidity_diagnostics_flags_low_dollar_volume_weight(self) -> None:
        dates = pd.bdate_range(start="2024-01-02", periods=80)
        rows: list[dict[str, object]] = []
        for symbol, volume in {"AAA": 1_000_000, "QQQ": 100}.items():
            for as_of in dates:
                rows.append({"symbol": symbol, "as_of": as_of, "close": 100.0, "volume": volume})
        weights = pd.DataFrame({"AAA": 0.60, "QQQ": 0.40}, index=dates)

        diagnostics = research.build_candidate_liquidity_diagnostics(
            price_history=pd.DataFrame(rows),
            weights_by_candidate={"candidate": weights},
            candidate_ids=("candidate",),
            dollar_volume_window=20,
            low_liquidity_dollar_volume=50_000.0,
        )

        summary = diagnostics["liquidity_summary"]
        symbol_summary = diagnostics["liquidity_symbol_summary"]
        self.assertEqual(summary["Candidate"].tolist(), ["candidate"])
        self.assertAlmostEqual(float(summary["Max Low Liquidity Weight"].iloc[0]), 0.40)
        qqq = symbol_summary.loc[symbol_summary["Symbol"].eq("QQQ")].iloc[0]
        self.assertAlmostEqual(float(qqq["Median Dollar Volume"]), 10_000.0)
        self.assertAlmostEqual(float(qqq["Low Liquidity Day Rate"]), 1.0)

    def test_build_dynamic_execution_cost_adjusted_returns_penalizes_liquidity_and_participation(self) -> None:
        dates = pd.bdate_range(start="2024-01-02", periods=8)
        price_rows: list[dict[str, object]] = []
        for symbol, volume in {"AAA": 100_000, "BIL": 10_000_000}.items():
            for as_of in dates:
                price_rows.append({"symbol": symbol, "as_of": as_of, "close": 100.0, "volume": volume})
        weights = pd.DataFrame(
            {
                "AAA": [0.0, 1.0, 1.0, 0.5, 0.5, 0.0, 0.0, 0.0],
                "BIL": [1.0, 0.0, 0.0, 0.5, 0.5, 1.0, 1.0, 1.0],
            },
            index=dates,
        )

        result = research.build_dynamic_execution_cost_adjusted_returns(
            price_history=pd.DataFrame(price_rows),
            weights_by_candidate={"candidate": weights},
            candidate_ids=("candidate",),
            config=research.DynamicExecutionCostConfig(
                base_cost_bps=5.0,
                dollar_volume_window=2,
                low_liquidity_dollar_volume=50_000_000.0,
                estimated_portfolio_nav=1_000_000.0,
                participation_rate_threshold=0.02,
            ),
        )

        returns = result["portfolio_returns"]
        summary = result["dynamic_cost_summary"].iloc[0]
        self.assertIn("candidate", returns.columns)
        self.assertLess(float(returns["candidate"].sum()), 0.0)
        self.assertGreater(float(summary["Median Effective Cost Bps On Trade Days"]), 5.0)
        self.assertGreater(float(summary["Max Participation Rate"]), 0.02)

    def test_build_dynamic_cost_live_readiness_diagnostics_emits_live_summary(self) -> None:
        baseline = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="live_global_etf_rotation_defensive_baseline",
            display_name="Live Baseline",
            candidate_group="current_live_baseline",
            rule="monthly_top1_base",
            ranking_pool=("AAA",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
        )
        overlay = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_growth_fast_top2_monthly",
            display_name="Fast Overlay",
            candidate_group="offensive_candidate",
            rule="monthly_top1_overlay",
            ranking_pool=("QQQ",),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=1,
            canary_bad_threshold=99,
            score_mode="fast_136w",
        )
        composite = research.GlobalEtfLiveableCompositeSpec(
            candidate_id="liveable_test_baseline90_fast10",
            display_name="Liveable Test 90 / 10",
            rule="static_blend_baseline90_fast10",
            base_candidate_id="live_global_etf_rotation_defensive_baseline",
            overlay_candidate_id="offensive_growth_fast_top2_monthly",
            overlay_weight=0.10,
        )

        diagnostics = research.build_dynamic_cost_live_readiness_diagnostics(
            price_history=_price_history(),
            periods=(("long", "2024-01-02", None),),
            config=research.DynamicExecutionCostConfig(base_cost_bps=5.0, dollar_volume_window=20),
            variants=(baseline, overlay),
            liveable_composites=(composite,),
            robustness_candidates=(
                "live_global_etf_rotation_defensive_baseline",
                "liveable_test_baseline90_fast10",
            ),
        )

        self.assertFalse(diagnostics["dynamic_cost_summary"].empty)
        self.assertFalse(diagnostics["dynamic_cost_live_readiness_summary"].empty)
        self.assertIn("Annualized Cost Drag", diagnostics["dynamic_cost_live_readiness_summary"].columns)

    def test_build_walk_forward_selection_diagnostics_selects_candidate_oos(self) -> None:
        dates = pd.bdate_range(start="2018-01-02", end="2024-12-31")
        portfolio_returns = pd.DataFrame(
            {
                "baseline": [0.0002] * len(dates),
                "candidate": [0.0005] * len(dates),
            },
            index=dates,
        )
        weights = pd.DataFrame({"AAA": [1.0] * len(dates)}, index=dates)

        diagnostics = research.build_walk_forward_selection_diagnostics(
            portfolio_returns=portfolio_returns,
            weights_by_candidate={"candidate": weights},
            candidate_ids=("candidate",),
            baseline_candidate="baseline",
            train_years=2,
            min_train_days_per_year=10,
            min_test_days=10,
        )

        windows = diagnostics["walk_forward_windows"]
        summary = diagnostics["walk_forward_summary"].iloc[0]
        self.assertFalse(windows.empty)
        self.assertEqual(set(windows["Selected Candidate"]), {"candidate"})
        self.assertTrue(bool(summary["walk_forward_gate_passed"]))
        self.assertGreater(float(summary["OOS Baseline CAGR Win Rate"]), 0.99)

    def test_build_walk_forward_selection_diagnostics_keeps_baseline_when_train_edge_is_too_small(self) -> None:
        dates = pd.bdate_range(start="2018-01-02", end="2024-12-31")
        portfolio_returns = pd.DataFrame(
            {
                "baseline": [0.00020] * len(dates),
                "candidate": [0.00021] * len(dates),
            },
            index=dates,
        )
        weights = pd.DataFrame({"AAA": [1.0] * len(dates)}, index=dates)

        diagnostics = research.build_walk_forward_selection_diagnostics(
            portfolio_returns=portfolio_returns,
            weights_by_candidate={"candidate": weights},
            candidate_ids=("candidate",),
            baseline_candidate="baseline",
            train_years=2,
            min_train_days_per_year=10,
            min_test_days=10,
            min_train_excess_cagr_vs_baseline=0.005,
        )

        windows = diagnostics["walk_forward_windows"]
        summary = diagnostics["walk_forward_summary"].iloc[0]
        self.assertEqual(set(windows["Selection Action"]), {"keep_baseline"})
        self.assertEqual(int(summary["Promotion OOS Windows"]), 0)
        self.assertIn("not_enough_oos_windows", str(summary["walk_forward_gate_reason"]))

    def test_write_recommendation_uses_cost_stress_to_downgrade_live_candidate(self) -> None:
        ranking = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "paper_review_candidate": False,
                    "live_review_candidate": True,
                    "review_action": "live_design_review",
                }
            ]
        )
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "live_global_etf_rotation_defensive_baseline",
                    "CAGR": 0.10,
                    "Excess CAGR vs Benchmark": 0.01,
                    "Excess CAGR vs Secondary Benchmark": -0.02,
                    "Max Drawdown": -0.20,
                }
            ]
        )
        live_readiness = pd.DataFrame(
            [
                {
                    "Candidate": "candidate",
                    "live_gate_passed": True,
                    "long_excess_cagr_vs_baseline": 0.01,
                    "live_action": "candidate_for_live_promotion_review",
                }
            ]
        )
        cost_stress = pd.DataFrame(
            [
                {
                    "turnover_cost_bps": 5.0,
                    "Candidate": "candidate",
                    "live_gate_passed": True,
                    "long_excess_cagr_vs_baseline": 0.01,
                },
                {
                    "turnover_cost_bps": 10.0,
                    "Candidate": "candidate",
                    "live_gate_passed": False,
                    "long_excess_cagr_vs_baseline": 0.009,
                },
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = research.write_recommendation(
                Path(tmpdir),
                ranking=ranking,
                period_summary=period_summary,
                live_readiness_summary=live_readiness,
                cost_stress_summary=cost_stress,
            )
            text = path.read_text(encoding="utf-8")

        self.assertIn("成本压力未全通过", text)
        self.assertIn("最高通过成本 5.00 bps", text)
        self.assertNotIn("进入 live promotion review，但不自动替换 live；优先复核候选：candidate。", text)

    def test_write_recommendation_prioritizes_dynamic_cost_gate(self) -> None:
        ranking = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "paper_review_candidate": False,
                    "live_review_candidate": True,
                    "review_action": "live_design_review",
                }
            ]
        )
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "live_global_etf_rotation_defensive_baseline",
                    "CAGR": 0.10,
                    "Excess CAGR vs Benchmark": 0.01,
                    "Excess CAGR vs Secondary Benchmark": -0.02,
                    "Max Drawdown": -0.20,
                }
            ]
        )
        dynamic_cost = pd.DataFrame(
            [
                {
                    "Candidate": "candidate",
                    "live_gate_passed": False,
                    "live_gate_reason": "long_cagr_not_above_baseline",
                }
            ]
        )
        cost_stress = pd.DataFrame(
            [
                {
                    "turnover_cost_bps": 25.0,
                    "Candidate": "candidate",
                    "live_gate_passed": True,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = research.write_recommendation(
                Path(tmpdir),
                ranking=ranking,
                period_summary=period_summary,
                cost_stress_summary=cost_stress,
                dynamic_cost_summary=dynamic_cost,
            )
            text = path.read_text(encoding="utf-8")

        self.assertIn("动态成本模型下无候选通过 live gate", text)
        self.assertIn("Dynamic Cost Preview", text)
        self.assertNotIn("在最高成本压力 25.00 bps 下优先复核候选", text)

    def test_write_recommendation_downgrades_when_walk_forward_gate_fails(self) -> None:
        ranking = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "paper_review_candidate": False,
                    "live_review_candidate": True,
                    "review_action": "live_design_review",
                }
            ]
        )
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "live_global_etf_rotation_defensive_baseline",
                    "CAGR": 0.10,
                    "Excess CAGR vs Benchmark": 0.01,
                    "Excess CAGR vs Secondary Benchmark": -0.02,
                    "Max Drawdown": -0.20,
                }
            ]
        )
        live_readiness = pd.DataFrame([{"Candidate": "candidate", "live_gate_passed": True}])
        walk_forward = pd.DataFrame(
            [
                {
                    "walk_forward_gate_passed": False,
                    "walk_forward_gate_reason": "oos_win_rate_below_50pct",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = research.write_recommendation(
                Path(tmpdir),
                ranking=ranking,
                period_summary=period_summary,
                live_readiness_summary=live_readiness,
                walk_forward_summary=walk_forward,
            )
            text = path.read_text(encoding="utf-8")

        self.assertIn("walk-forward/OOS gate 未通过", text)
        self.assertIn("oos_win_rate_below_50pct", text)

    def test_write_recommendation_reports_highest_passing_dynamic_nav(self) -> None:
        ranking = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "Candidate": "candidate",
                    "Display Name": "Candidate",
                    "Candidate Group": "liveable_candidate",
                    "Rule": "static_blend",
                    "research_gate_passed": True,
                    "paper_review_candidate": False,
                    "live_review_candidate": True,
                    "review_action": "live_design_review",
                }
            ]
        )
        period_summary = pd.DataFrame(
            [
                {
                    "Period": "long",
                    "Candidate": "live_global_etf_rotation_defensive_baseline",
                    "CAGR": 0.10,
                    "Excess CAGR vs Benchmark": 0.01,
                    "Excess CAGR vs Secondary Benchmark": -0.02,
                    "Max Drawdown": -0.20,
                }
            ]
        )
        dynamic_cost = pd.DataFrame(
            [
                {"Candidate": "candidate", "Estimated Portfolio NAV": 100_000.0, "live_gate_passed": True},
                {"Candidate": "candidate", "Estimated Portfolio NAV": 1_000_000.0, "live_gate_passed": False},
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = research.write_recommendation(
                Path(tmpdir),
                ranking=ranking,
                period_summary=period_summary,
                dynamic_cost_summary=dynamic_cost,
            )
            text = path.read_text(encoding="utf-8")

        self.assertIn("动态成本 NAV 压力未全通过", text)
        self.assertIn("最高通过 NAV $100,000", text)

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

    def test_dual_momentum_score_mode_runs_and_emits_new_candidate(self) -> None:
        variant = research.GlobalEtfOffensiveVariantSpec(
            candidate_id="offensive_test_dual_momentum",
            display_name="Offensive Test Dual Momentum",
            candidate_group="offensive_candidate",
            rule="monthly_top2_dual_momentum_13612w_growth_pool",
            ranking_pool=("AAA", "QQQ"),
            primary_benchmark_symbol="SPY",
            secondary_benchmark_symbol="QQQ",
            safe_haven="BIL",
            canary_assets=("SPY",),
            top_n=2,
            canary_bad_threshold=99,
            score_mode="dual_momentum_13612w",
            sma_period=200,
        )
        result = research.run_offensive_research(
            price_history=_price_history(),
            periods=(("long", "2024-01-02", None),),
            variants=(variant,),
            turnover_cost_bps=0.0,
        )

        self.assertEqual(result["ranking"]["Candidate"].iloc[0], "offensive_test_dual_momentum")
        self.assertEqual(result["ranking"]["Score Mode"].iloc[0], "dual_momentum_13612w")
        self.assertFalse(result["portfolio_returns"].empty)

    def test_build_live_decision_summary_emits_structured_json_fields(self) -> None:
        ranking = pd.DataFrame(
            [
                {
                    "Candidate": "liveable_blend_baseline90_fast10",
                    "paper_review_candidate": True,
                    "live_review_candidate": False,
                }
            ]
        )
        period_summary = pd.DataFrame(
            [
                {
                    "Candidate": "live_global_etf_rotation_defensive_baseline",
                    "Period": "long",
                    "CAGR": 0.12,
                    "Excess CAGR vs Benchmark": 0.01,
                    "Excess CAGR vs Secondary Benchmark": -0.02,
                    "Max Drawdown": -0.20,
                }
            ]
        )

        summary = research.build_live_decision_summary(ranking=ranking, period_summary=period_summary)

        self.assertEqual(summary["manifest_type"], "global_etf_offensive_live_decision_summary")
        self.assertEqual(summary["decision_state"], "paper_review")
        self.assertEqual(summary["preferred_candidates"], ["liveable_blend_baseline90_fast10"])
        self.assertIn("paper review", str(summary["recommendation"]))

    def test_write_live_decision_summary_writes_json_file(self) -> None:
        ranking = pd.DataFrame([{"Candidate": "candidate", "paper_review_candidate": False, "live_review_candidate": False}])
        period_summary = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = research.write_live_decision_summary(
                Path(tmpdir),
                ranking=ranking,
                period_summary=period_summary,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(path.name, "live_decision_summary.json")
        self.assertEqual(payload["decision_state"], "hold_baseline")


if __name__ == "__main__":
    unittest.main()
