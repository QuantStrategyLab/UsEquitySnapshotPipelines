from __future__ import annotations

import hashlib
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    INPUT_CONTRACT_ID,
    P1_P3_RESEARCH_ONLY_GATE,
    P2_V3_CONTRACT,
    QPK_REVISION,
    UES_REVISION,
)

_CONFIG_PATH = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v3.json"


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


def test_soxl_core_only_p2_v3_contract_freezes_eligible_research_identity() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert _canonical_sha256(config) == CONFIG_SHA256
    assert config["schema_version"] == "qsl.soxl-soxx-core-only-p2-candidate.v3"
    assert config["candidate_id"] == CANDIDATE_ID == P2_V3_CONTRACT.candidate_id
    assert P2_V3_CONTRACT.input_contract_id == INPUT_CONTRACT_ID
    assert config["source"]["repository"] == "QuantStrategyLab/UsEquityStrategies"
    assert config["source"]["revision"] == UES_REVISION
    assert config["source"]["entrypoint"] == (
        "us_equity_strategies.entrypoints.build_soxl_soxx_core_only_p2_v2_research_decision"
    )
    assert config["source"]["dependency_lock"] == {
        "quant_platform_kit_revision": QPK_REVISION,
        "strategy_dependency_upgrade_required_before_p1_or_p3": False,
    }


def test_soxl_core_only_p2_v3_allows_only_research_p1_p3_and_preserves_no_execution() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert config["p1_entry"]["eligible_now"] is True
    assert config["p1_entry"]["unique_next_gate"] == P1_P3_RESEARCH_ONLY_GATE
    assert config["p1_entry"]["forbidden_operations"] == [
        "parameter_tuning",
        "broker_order",
        "paper",
        "shadow",
        "live",
    ]
    assert config["classification"] == {
        "execution_authorized": False,
        "no_order": True,
        "research_only": True,
        "size_zero_required": True,
    }
    assert config["runtime_config"]["income_layer_enabled"] is False
    assert config["runtime_config"]["option_overlay_enabled"] is False
    assert config["runtime_config"]["market_regime_control_enabled"] is False
    assert config["runtime_config"]["blend_gate_volatility_delever_retention_mode"] == "none"
