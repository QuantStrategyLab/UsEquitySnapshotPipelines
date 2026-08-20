from __future__ import annotations

import hashlib
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v2_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    FUTURE_INPUT_CONTRACT_ID,
    FUTURE_P3_VERIFIER_GATE,
    P2_V2_CONTRACT,
    QPK_REVISION,
    UES_REVISION,
)


_CONFIG_PATH = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v2.json"


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def test_soxl_core_only_p2_v2_contract_matches_the_frozen_config() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert _canonical_sha256(config) == CONFIG_SHA256
    assert config["schema_version"] == "qsl.soxl-soxx-core-only-p2-candidate.v2"
    assert config["candidate_id"] == CANDIDATE_ID == P2_V2_CONTRACT.candidate_id
    assert P2_V2_CONTRACT.config_sha256 == CONFIG_SHA256
    source = config["source"]
    assert source["repository"] == "QuantStrategyLab/UsEquityStrategies"
    assert source["revision"] == UES_REVISION
    assert source["entrypoint"] == "us_equity_strategies.entrypoints.build_soxl_soxx_core_only_p2_v2_research_decision"
    assert source["builder"] == "us_equity_strategies.strategies.soxl_soxx_trend_income.build_rebalance_plan"
    assert source["dependency_lock"] == {
        "quant_platform_kit_revision": QPK_REVISION,
        "strategy_dependency_upgrade_required_before_p1_or_p3": True,
    }


def test_soxl_core_only_p2_v2_is_frozen_but_has_no_p1_or_execution_authority() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert P2_V2_CONTRACT.future_input_contract_id == FUTURE_INPUT_CONTRACT_ID
    assert config["classification"] == {
        "execution_authorized": False,
        "no_order": True,
        "research_only": True,
        "size_zero_required": True,
    }
    assert config["p1_entry"] == {
        "eligible_now": False,
        "forbidden_operations": ["parameter_tuning", "broker_order", "paper", "shadow", "live"],
        "reason": "A dedicated daily P1 input contract and P3 verifier do not exist yet. The fixed-cutoff legacy SOXL acquisition and replay path cannot be reused.",
        "unique_next_gate": FUTURE_P3_VERIFIER_GATE,
    }
    assert config["required_inputs"]["tradable_assets"] == ["SOXL", "SOXX", "BOXX"]
    assert config["runtime_config"]["managed_symbols"] == ["SOXL", "SOXX", "BOXX"]
    assert config["runtime_config"]["income_layer_enabled"] is False
    assert config["runtime_config"]["option_overlay_enabled"] is False
    assert config["runtime_config"]["market_regime_control_enabled"] is False
    assert config["runtime_config"]["blend_gate_volatility_delever_retention_mode"] == "none"
