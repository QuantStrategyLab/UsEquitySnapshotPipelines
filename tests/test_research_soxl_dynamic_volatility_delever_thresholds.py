from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "research_soxl_dynamic_volatility_delever_thresholds.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("research_soxl_dynamic_volatility_delever_thresholds", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_long_term_risk_budget_variants_reduce_only_the_leveraged_sleeve() -> None:
    module = _load_module()
    variants = dict(module._variants())

    expected_caps = {
        "core_cap60_55": (0.60, 0.55),
        "core_cap50_45": (0.50, 0.45),
        "core_cap40_35": (0.40, 0.35),
        "risk_budget60_55_p95_hysteresis7_5pp_cooldown2d": (0.60, 0.55),
        "risk_budget50_45_p95_hysteresis7_5pp_cooldown2d": (0.50, 0.45),
        "risk_budget40_35_p95_hysteresis7_5pp_cooldown2d": (0.40, 0.35),
    }
    for name, (full_weight, mid_weight) in expected_caps.items():
        overrides = variants[name]["strategy_overrides"]
        assert overrides["blend_gate_soxl_weight"] == full_weight
        assert overrides["blend_gate_mid_soxl_weight"] == mid_weight
        assert "blend_gate_active_soxx_weight" not in overrides
        assert "blend_gate_defensive_soxx_weight" not in overrides


def test_hysteresis_risk_budget_preserves_the_existing_external_volatility_rules() -> None:
    module = _load_module()
    variants = dict(module._variants())
    candidate = variants["risk_budget50_45_p95_hysteresis7_5pp_cooldown2d"]

    assert candidate["soxl_delever_overlay_kind"] == "volatility"
    assert candidate["soxl_delever_overlay_threshold_mode"] == "rolling_percentile"
    assert candidate["soxl_delever_overlay_threshold_percentile"] == 0.95
    assert candidate["soxl_delever_overlay_reentry_hysteresis"] == 0.075
    assert candidate["soxl_delever_overlay_reentry_cooldown_days"] == 2
