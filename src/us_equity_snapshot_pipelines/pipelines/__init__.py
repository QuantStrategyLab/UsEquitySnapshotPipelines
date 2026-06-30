"""Snapshot builders, backtest runners, and live monitors."""

from .live_decay_monitor import DecayPolicy, build_live_decay_monitor, build_markdown_report, build_strategy_decay_summary, main as live_decay_monitor_main, normalize_return_matrix
from .live_strategy_health import HealthPolicy, HealthWindow, build_health_windows, build_markdown_report as build_health_markdown_report, build_strategy_health_summary, build_strategy_window_health, classify_window_health, main as live_strategy_health_main, summarize_returns as health_summarize_returns
from .mega_cap_leader_rotation_backtest import MegaCapDynamicUniverseResearchDataResult, MegaCapResearchDataResult, build_dynamic_mega_universe_history, build_feature_snapshot_for_backtest, build_monthly_rebalance_dates, build_parser as mega_cap_leader_rotation_backtest_parser, build_static_universe, build_target_weights as mega_cap_leader_rotation_backtest_target_weights, main as mega_cap_leader_rotation_backtest_main, prepare_dynamic_mega_universe_research_input_data, prepare_research_input_data, resolve_active_universe as resolve_active_universe_mega, resolve_pool_symbols, run_backtest as mega_cap_leader_rotation_run_backtest, score_candidates, split_symbols, summarize_returns as mega_cap_leader_rotation_summarize_returns
from .mega_cap_leader_rotation_snapshot import MegaCapDynamicTop20BuildResult, build_artifacts as mega_cap_leader_rotation_build_artifacts, build_parser as mega_cap_leader_rotation_snapshot_parser, main as mega_cap_leader_rotation_snapshot_main
from .mag7_leveraged_pullback_backtest import build_parser as mag7_leveraged_pullback_backtest_parser, build_rebound_budget_schedule, build_rebound_budget_series, build_target_weights as mag7_pullback_target_weights, main as mag7_leveraged_pullback_backtest_main, rank_candidates, resolve_active_candidate_symbols, run_backtest as mag7_leveraged_pullback_run_backtest, summarize_returns as mag7_pullback_summarize_returns
from .new_r1000_residual_strength_20_snapshot import ResidualStrength20BuildResult, build_artifacts as r1000_residual_build_artifacts, build_parser as r1000_residual_snapshot_parser, main as r1000_residual_snapshot_main
from .russell_1000_multi_factor_backtest import build_monthly_rebalance_dates as r1000_mf_build_monthly_rebalance_dates, resolve_active_universe as r1000_mf_resolve_active_universe, run_backtest as r1000_mf_run_backtest, summarize_backtest
from .russell_1000_multi_factor_defensive_snapshot import build_feature_snapshot as r1000_mf_build_feature_snapshot, read_table, write_table
from .snapshot_promotion_bundle import build_parser as snapshot_promotion_bundle_parser, build_snapshot_promotion_bundle, main as snapshot_promotion_bundle_main
from .snapshot_shadow_review import build_parser as snapshot_shadow_review_parser, build_snapshot_shadow_review_artifacts, build_snapshot_shadow_review_rows, main as snapshot_shadow_review_main
from .soxl_soxx_trend_income_backtest import build_indicator_history as soxl_build_indicator_history, build_parser as soxl_trend_income_backtest_parser, main as soxl_trend_income_backtest_main, run_backtest as soxl_trend_income_run_backtest
from .tecl_xlk_trend_income_backtest import build_indicator_history as tecl_build_indicator_history, build_parser as tecl_xlk_trend_income_backtest_parser, main as tecl_xlk_trend_income_backtest_main, run_backtest as tecl_xlk_trend_income_run_backtest
from .global_etf_rotation_snapshot import GlobalEtfRotationRuntimeArtifactResult, build_default_symbol_specs, build_global_etf_rotation_audit, build_global_etf_rotation_feature_snapshot, build_global_etf_rotation_rule_spec, build_parser as global_etf_rotation_snapshot_parser, build_runtime_ranking, main as global_etf_rotation_snapshot_main, write_runtime_artifacts

__all__ = [
    # mega_cap_leader_rotation_snapshot
    "MegaCapDynamicTop20BuildResult",
    "mega_cap_leader_rotation_build_artifacts",
    "mega_cap_leader_rotation_snapshot_parser",
    "mega_cap_leader_rotation_snapshot_main",
    # global_etf_rotation_snapshot
    "GlobalEtfRotationRuntimeArtifactResult",
    "build_default_symbol_specs",
    "build_global_etf_rotation_audit",
    "build_global_etf_rotation_feature_snapshot",
    "build_global_etf_rotation_rule_spec",
    "global_etf_rotation_snapshot_parser",
    "build_runtime_ranking",
    "global_etf_rotation_snapshot_main",
    "write_runtime_artifacts",
    # new_r1000_residual_strength_20_snapshot
    "ResidualStrength20BuildResult",
    "r1000_residual_build_artifacts",
    "r1000_residual_snapshot_parser",
    "r1000_residual_snapshot_main",
    # russell_1000_multi_factor_defensive_snapshot
    "r1000_mf_build_feature_snapshot",
    "read_table",
    "write_table",
    # mega_cap_leader_rotation_backtest
    "MegaCapResearchDataResult",
    "MegaCapDynamicUniverseResearchDataResult",
    "split_symbols",
    "resolve_pool_symbols",
    "build_static_universe",
    "build_dynamic_mega_universe_history",
    "prepare_research_input_data",
    "prepare_dynamic_mega_universe_research_input_data",
    "resolve_active_universe_mega",
    "build_monthly_rebalance_dates",
    "build_feature_snapshot_for_backtest",
    "score_candidates",
    "mega_cap_leader_rotation_backtest_target_weights",
    "mega_cap_leader_rotation_summarize_returns",
    "mega_cap_leader_rotation_run_backtest",
    "mega_cap_leader_rotation_backtest_parser",
    "mega_cap_leader_rotation_backtest_main",
    # mag7_leveraged_pullback_backtest
    "resolve_active_candidate_symbols",
    "rank_candidates",
    "mag7_pullback_target_weights",
    "mag7_pullback_summarize_returns",
    "build_rebound_budget_schedule",
    "build_rebound_budget_series",
    "mag7_leveraged_pullback_run_backtest",
    "mag7_leveraged_pullback_backtest_parser",
    "mag7_leveraged_pullback_backtest_main",
    # soxl_soxx_trend_income_backtest
    "soxl_build_indicator_history",
    "soxl_trend_income_run_backtest",
    "soxl_trend_income_backtest_parser",
    "soxl_trend_income_backtest_main",
    # tecl_xlk_trend_income_backtest
    "tecl_build_indicator_history",
    "tecl_xlk_trend_income_run_backtest",
    "tecl_xlk_trend_income_backtest_parser",
    "tecl_xlk_trend_income_backtest_main",
    # russell_1000_multi_factor_backtest
    "r1000_mf_resolve_active_universe",
    "r1000_mf_build_monthly_rebalance_dates",
    "summarize_backtest",
    "r1000_mf_run_backtest",
    # live_strategy_health
    "HealthPolicy",
    "HealthWindow",
    "build_health_windows",
    "health_summarize_returns",
    "classify_window_health",
    "build_strategy_window_health",
    "build_strategy_health_summary",
    "build_health_markdown_report",
    "live_strategy_health_main",
    # live_decay_monitor
    "DecayPolicy",
    "normalize_return_matrix",
    "classify_decay_window",
    "build_live_decay_monitor",
    "build_markdown_report",
    "build_strategy_decay_summary",
    "live_decay_monitor_main",
    # snapshot_promotion_bundle
    "build_snapshot_promotion_bundle",
    "snapshot_promotion_bundle_parser",
    "snapshot_promotion_bundle_main",
    # snapshot_shadow_review
    "build_snapshot_shadow_review_rows",
    "build_snapshot_shadow_review_artifacts",
    "snapshot_shadow_review_parser",
    "snapshot_shadow_review_main",
]
