"""Research and exploration scripts."""

from .ai_rule_proposal import AI_HARD_GATE_KEY_TO_METRIC, AI_HARD_GATE_OPERATORS, build_rule_spec_from_ai_proposal
from .crisis_context_research import build_crisis_context_features, main as crisis_context_research_main
from .crisis_regime_guard_research import CrisisGuardSpec, build_crisis_guard_specs, main as crisis_regime_guard_research_main, run_crisis_guard_research
from .crisis_response_research import main as crisis_response_research_main, run_crisis_response_research
from .crisis_response_shadow_plugin import main as crisis_response_shadow_plugin_main
from .ibit_smart_dca_research import IbitDcaResearchConfig, build_ibit_smart_dca_research, main as ibit_smart_dca_research_main
from .ibit_zscore_exit_plugin import build_ibit_zscore_exit_signal, write_ibit_zscore_exit_outputs
from .leaps_growth_overlay_research import LeapsProxyConfig, main as leaps_growth_overlay_research_main, run_leaps_growth_overlay_proxy
from .leveraged_strategy_candidates import LeveragedCandidateSpec, LEVERAGED_CANDIDATES, main as leveraged_strategy_candidates_main, run_candidate_backtest
from .memory_semiconductor_momentum_research import build_default_symbol_specs as memory_semiconductor_build_default_symbol_specs, build_memory_semiconductor_audit, build_memory_semiconductor_rule_spec, main as memory_semiconductor_momentum_research_main
from .taco_panic_rebound_backtest import main as taco_panic_rebound_backtest_main
from .taco_panic_rebound_overlay_compare import main as taco_panic_rebound_overlay_compare_main
from .taco_panic_rebound_research import main as taco_panic_rebound_research_main
from .taco_rebound_shadow_plugin import main as taco_rebound_shadow_plugin_main
from .us_equity_strategy_candidates import EtfCandidateSpec, SnapshotCandidateSpec, main as us_equity_strategy_candidates_main, run_candidate_research as us_equity_run_candidate_research

__all__ = [
    # ai_rule_proposal
    "build_rule_spec_from_ai_proposal",
    "AI_HARD_GATE_KEY_TO_METRIC",
    "AI_HARD_GATE_OPERATORS",
    # crisis_context_research
    "build_crisis_context_features",
    "crisis_context_research_main",
    # crisis_regime_guard_research
    "CrisisGuardSpec",
    "build_crisis_guard_specs",
    "run_crisis_guard_research",
    "crisis_regime_guard_research_main",
    # crisis_response_research
    "run_crisis_response_research",
    "crisis_response_research_main",
    # crisis_response_shadow_plugin
    "crisis_response_shadow_plugin_main",
    # ibit_smart_dca_research
    "IbitDcaResearchConfig",
    "build_ibit_smart_dca_research",
    "ibit_smart_dca_research_main",
    # ibit_zscore_exit_plugin
    "build_ibit_zscore_exit_signal",
    "write_ibit_zscore_exit_outputs",
    # leaps_growth_overlay_research
    "LeapsProxyConfig",
    "run_leaps_growth_overlay_proxy",
    "leaps_growth_overlay_research_main",
    # leveraged_strategy_candidates
    "LeveragedCandidateSpec",
    "LEVERAGED_CANDIDATES",
    "run_candidate_backtest",
    "leveraged_strategy_candidates_main",
    # memory_semiconductor_momentum_research
    "memory_semiconductor_build_default_symbol_specs",
    "build_memory_semiconductor_audit",
    "build_memory_semiconductor_rule_spec",
    "memory_semiconductor_momentum_research_main",
    # taco_panic_rebound_backtest
    "taco_panic_rebound_backtest_main",
    # taco_panic_rebound_overlay_compare
    "taco_panic_rebound_overlay_compare_main",
    # taco_panic_rebound_research
    "taco_panic_rebound_research_main",
    # taco_rebound_shadow_plugin
    "taco_rebound_shadow_plugin_main",
    # us_equity_strategy_candidates
    "EtfCandidateSpec",
    "SnapshotCandidateSpec",
    "us_equity_run_candidate_research",
    "us_equity_strategy_candidates_main",
]
