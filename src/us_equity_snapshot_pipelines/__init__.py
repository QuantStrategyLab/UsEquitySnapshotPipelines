from __future__ import annotations

from .contracts import (
    SnapshotProfileContract,
    get_profile_contract,
    list_profile_contracts,
)

# Backward-compatible re-exports from pipelines subpackage
from .pipelines import (
    # Snapshot builders
    mega_cap_leader_rotation_build_artifacts,
    r1000_mf_build_feature_snapshot,
    # Backtest runners
    mega_cap_leader_rotation_run_backtest,
    mag7_leveraged_pullback_run_backtest,
    soxl_trend_income_run_backtest,
    tecl_xlk_trend_income_run_backtest,
    r1000_mf_run_backtest,
    # Live monitors
    build_live_decay_monitor,
    build_strategy_decay_summary,
    build_strategy_window_health,
    build_strategy_health_summary,
    # Promotion / review
    build_snapshot_promotion_bundle,
    build_snapshot_shadow_review_artifacts,
    build_snapshot_shadow_review_rows,
)

# Backward-compatible re-exports from lifecycle subpackage
from .lifecycle import (
    UsEquityBacktestRunner,
    build_backtest_runner,
    read_us_equity_returns,
    list_us_equity_strategies,
)

__all__ = [
    "SnapshotProfileContract",
    "get_profile_contract",
    "list_profile_contracts",
    # Pipelines
    "mega_cap_leader_rotation_build_artifacts",
    "r1000_mf_build_feature_snapshot",
    "mega_cap_leader_rotation_run_backtest",
    "mag7_leveraged_pullback_run_backtest",
    "soxl_trend_income_run_backtest",
    "tecl_xlk_trend_income_run_backtest",
    "r1000_mf_run_backtest",
    "build_live_decay_monitor",
    "build_strategy_decay_summary",
    "build_strategy_window_health",
    "build_strategy_health_summary",
    "build_snapshot_promotion_bundle",
    "build_snapshot_shadow_review_artifacts",
    "build_snapshot_shadow_review_rows",
    # Lifecycle
    "UsEquityBacktestRunner",
    "build_backtest_runner",
    "read_us_equity_returns",
    "list_us_equity_strategies",
]
