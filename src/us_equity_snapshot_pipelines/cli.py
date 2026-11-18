"""Unified `useq` CLI dispatching to UsEquitySnapshotPipelines entrypoints."""

from __future__ import annotations

import argparse
import importlib
import inspect
import sys
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class CommandSpec:
    """Lazy-import target for a `useq` subcommand."""

    module: str
    attr: str = "main"
    summary: str = ""


COMMANDS: dict[str, CommandSpec] = {
    # Snapshot & Artifact Pipelines
    "publish-snapshot-artifacts": CommandSpec(
        "us_equity_snapshot_pipelines.publish",
        summary="Publish verified snapshot artifacts",
    ),
    "build-global-etf-rotation-snapshot": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.global_etf_rotation_snapshot",
        summary="Build Global ETF Rotation feature snapshot",
    ),
    "build-russell-top50-leader-rotation-snapshot": CommandSpec(
        "us_equity_snapshot_pipelines.russell_top50_leader_rotation",
        summary="Build Russell Top50 Leader Rotation snapshot",
    ),
    "build-new-r1000-residual-strength-20-snapshot": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.new_r1000_residual_strength_20_snapshot",
        summary="Build R1000 residual strength 20 snapshot",
    ),
    "update-russell-1000-input-data": CommandSpec(
        "us_equity_snapshot_pipelines.russell_1000_inputs",
        summary="Update Russell 1000 input data",
    ),
    "run-strategy-plugins": CommandSpec(
        "us_equity_snapshot_pipelines.strategy_plugin_runner",
        summary="Run registered strategy plugins",
    ),
    # Shadow & Live Health
    "build-live-strategy-health-report": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.live_strategy_health",
        summary="Build live strategy health report",
    ),
    "build-live-decay-monitor": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.live_decay_monitor",
        summary="Build live decay monitor report",
    ),
    "build-global-etf-shadow-review": CommandSpec(
        "us_equity_snapshot_pipelines.global_etf_rotation_shadow_review",
        summary="Build Global ETF shadow review",
    ),
    "build-global-etf-shadow-review-input": CommandSpec(
        "us_equity_snapshot_pipelines.global_etf_rotation_shadow_review_input",
        summary="Build Global ETF shadow review input",
    ),
    "run-global-etf-shadow-cycle": CommandSpec(
        "us_equity_snapshot_pipelines.global_etf_rotation_shadow_cycle",
        summary="Run Global ETF shadow cycle",
    ),
    "build-russell-top50-shadow-review": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_shadow_review",
        summary="Build Russell Top50 shadow review",
    ),
    "build-russell-top50-shadow-live-ledger": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_shadow_live_ledger",
        summary="Build Russell Top50 shadow live ledger",
    ),
    "run-russell-top50-shadow-cycle": CommandSpec(
        "us_equity_snapshot_pipelines.russell_leader_rotation_shadow_cycle",
        summary="Run Russell Top50 shadow cycle",
    ),
    "build-crisis-context-pack": CommandSpec(
        "us_equity_snapshot_pipelines.research.crisis_context_research",
        summary="Build crisis context pack",
    ),
    "build-crisis-response-shadow-signal": CommandSpec(
        "us_equity_snapshot_pipelines.research.crisis_response_shadow_plugin",
        summary="Build crisis response shadow signal",
    ),
    "build-taco-rebound-shadow-signal": CommandSpec(
        "us_equity_snapshot_pipelines.research.taco_rebound_shadow_plugin",
        summary="Build TACO rebound shadow signal",
    ),
    # Backtest, Research & Promotion
    "backtest-global-etf-offensive-rotation": CommandSpec(
        "us_equity_snapshot_pipelines.global_etf_offensive_rotation_research",
        summary="Run Global ETF offensive rotation research/backtest",
    ),
    "backtest-global-etf-universe-sensitivity": CommandSpec(
        "us_equity_snapshot_pipelines.global_etf_rotation_universe_sensitivity",
        summary="Run Global ETF universe sensitivity backtest",
    ),
    "backtest-russell-top50-leader-rotation": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.mega_cap_leader_rotation_backtest",
        summary="Backtest Russell Top50 leader rotation",
    ),
    "backtest-russell-top50-leader-rotation-robustness": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_robustness",
        summary="Backtest Russell Top50 leader rotation robustness",
    ),
    "validate-russell-top50-leader-rotation-dynamic-universe": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_dynamic_validation",
        summary="Validate Russell Top50 dynamic universe",
    ),
    "research-russell-top50-leader-rotation-capacity-stress": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_capacity_stress",
        summary="Research Russell Top50 capacity stress",
    ),
    "research-russell-top50-leader-rotation-concentration-variants": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants",
        summary="Research Russell Top50 concentration variants",
    ),
    "research-russell-top50-leader-rotation-frequency-risk": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_frequency_risk_research",
        summary="Research Russell Top50 frequency risk",
    ),
    "research-russell-top50-leader-rotation-live-readiness": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_live_readiness",
        summary="Research Russell Top50 live readiness",
    ),
    "research-russell-top50-leader-rotation-stress-readiness": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_stress_readiness",
        summary="Research Russell Top50 stress readiness",
    ),
    "research-russell-top50-leader-rotation-crash-brake": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research",
        summary="Research Russell Top50 crash brake",
    ),
    "research-russell-top50-leader-rotation-overfit-diagnostics": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_overfit_diagnostics",
        summary="Research Russell Top50 overfit diagnostics",
    ),
    "research-russell-top50-leader-rotation-reality-check": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_reality_check",
        summary="Research Russell Top50 reality check",
    ),
    "research-russell-top50-leader-rotation-liquidity-diagnostics": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_liquidity_diagnostics",
        summary="Research Russell Top50 liquidity diagnostics",
    ),
    "research-russell-top50-leader-rotation-spa-check": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_spa_check",
        summary="Research Russell Top50 SPA check",
    ),
    "research-russell-top50-leader-rotation-era-split": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_era_split_diagnostics",
        summary="Research Russell Top50 era split diagnostics",
    ),
    "research-russell-top50-leader-rotation-mcs-diagnostics": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_mcs_diagnostics",
        summary="Research Russell Top50 MCS diagnostics",
    ),
    "research-russell-top50-leader-rotation-dsr-pbo": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_dsr_pbo_diagnostics",
        summary="Research Russell Top50 DSR/PBO diagnostics",
    ),
    "research-russell-top50-leader-rotation-promotion-review": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_review",
        summary="Research Russell Top50 promotion review",
    ),
    "research-russell-top50-leader-rotation-promotion-bundle": CommandSpec(
        "us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_bundle",
        summary="Research Russell Top50 promotion bundle",
    ),
    "research-russell-1000-proxy-long-history": CommandSpec(
        "us_equity_snapshot_pipelines.russell_1000_proxy_long_history",
        summary="Research Russell 1000 proxy long history",
    ),
    "research-index-leaps-growth-overlay": CommandSpec(
        "us_equity_snapshot_pipelines.research.leaps_growth_overlay_research",
        summary="Research index LEAPS growth overlay",
    ),
    "backtest-mag7-leveraged-pullback": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.mag7_leveraged_pullback_backtest",
        summary="Backtest MAG7 leveraged pullback",
    ),
    "backtest-soxl-soxx-trend-income": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.soxl_soxx_trend_income_backtest",
        summary="Backtest SOXL/SOXX trend income",
    ),
    "archive-soxl-soxx-trend-income": CommandSpec(
        "us_equity_snapshot_pipelines.soxl_soxx_trend_income_archive",
        summary="Archive SOXL/SOXX trend income",
    ),
    "backtest-tecl-xlk-trend-income": CommandSpec(
        "us_equity_snapshot_pipelines.pipelines.tecl_xlk_trend_income_backtest",
        summary="Backtest TECL/XLK trend income",
    ),
    "archive-tqqq-growth-income": CommandSpec(
        "us_equity_snapshot_pipelines.tqqq_growth_income_archive",
        summary="Archive TQQQ growth income",
    ),
    "research-taco-panic-rebound": CommandSpec(
        "us_equity_snapshot_pipelines.research.taco_panic_rebound_research",
        summary="Research TACO panic rebound",
    ),
    "backtest-taco-panic-rebound-portfolio": CommandSpec(
        "us_equity_snapshot_pipelines.research.taco_panic_rebound_backtest",
        summary="Backtest TACO panic rebound portfolio",
    ),
    "backtest-taco-panic-rebound-overlay-compare": CommandSpec(
        "us_equity_snapshot_pipelines.research.taco_panic_rebound_overlay_compare",
        summary="Compare TACO panic rebound overlays",
    ),
    "backtest-crisis-regime-guard": CommandSpec(
        "us_equity_snapshot_pipelines.research.crisis_regime_guard_research",
        summary="Backtest crisis regime guard",
    ),
    "backtest-crisis-response": CommandSpec(
        "us_equity_snapshot_pipelines.research.crisis_response_research",
        summary="Backtest crisis response",
    ),
    "research-memory-semiconductor-momentum": CommandSpec(
        "us_equity_snapshot_pipelines.research.memory_semiconductor_momentum_research",
        summary="Research memory semiconductor momentum",
    ),
    "research-ibit-smart-dca": CommandSpec(
        "us_equity_snapshot_pipelines.ibit_smart_dca_research",
        summary="Research IBIT smart DCA",
    ),
    "backtest-us-equity-strategy-candidates": CommandSpec(
        "us_equity_snapshot_pipelines.research.us_equity_strategy_candidates",
        summary="Backtest US equity strategy candidates",
    ),
    "backtest-leveraged-strategy-candidates": CommandSpec(
        "us_equity_snapshot_pipelines.research.leveraged_strategy_candidates",
        summary="Backtest leveraged strategy candidates",
    ),
    "run-soxl-promotion-research": CommandSpec(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner",
        summary="Run SOXL promotion research lifecycle",
    ),
}


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level `useq` argument parser."""
    width = max((len(name) for name in COMMANDS), default=20)
    lines = [f"  {name:<{width}}  {spec.summary}" for name, spec in COMMANDS.items()]
    epilog = "available subcommands:\n" + "\n".join(lines)
    parser = argparse.ArgumentParser(
        prog="useq",
        description="Unified CLI for UsEquitySnapshotPipelines.",
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("subcommand", nargs="?", help="Subcommand to execute")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments passed to the subcommand")
    return parser


def dispatch(subcommand: str, argv: list[str]) -> int:
    """Lazily import and invoke the target entrypoint for `subcommand`."""
    spec = COMMANDS[subcommand]
    module = importlib.import_module(spec.module)
    target = getattr(module, spec.attr)
    if len(inspect.signature(target).parameters) == 0:
        old_argv = sys.argv
        sys.argv = [f"useq {subcommand}", *argv]
        try:
            result = target()
        finally:
            sys.argv = old_argv
    else:
        result = target(argv)
    return 0 if result is None else int(result)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for `useq`."""
    args_list = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()

    if not args_list or args_list[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    subcommand, sub_argv = args_list[0], args_list[1:]
    if subcommand not in COMMANDS:
        parser.error(f"unknown subcommand: {subcommand!r}")

    return dispatch(subcommand, sub_argv)


if __name__ == "__main__":
    raise SystemExit(main())
