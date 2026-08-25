from __future__ import annotations

import hashlib
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v4_free_split_close_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    INPUT_CONTRACT_ID,
    P1_P3_RESEARCH_ONLY_GATE,
    P2_V4_FREE_SPLIT_CLOSE_CONTRACT,
    QPK_REVISION,
    UES_REVISION,
)

_CONFIG_PATH = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v4_free_split_close.json"


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


def test_v4_contract_freezes_the_new_two_source_split_adjusted_close_identity() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    assert _canonical_sha256(config) == CONFIG_SHA256
    assert config["schema_version"] == "qsl.soxl-soxx-core-only-p2-candidate.v4"
    assert config["candidate_id"] == CANDIDATE_ID == P2_V4_FREE_SPLIT_CLOSE_CONTRACT.candidate_id
    assert P2_V4_FREE_SPLIT_CLOSE_CONTRACT.input_contract_id == INPUT_CONTRACT_ID
    assert config["source"]["revision"] == UES_REVISION
    assert config["source"]["dependency_lock"]["quant_platform_kit_revision"] == QPK_REVISION
    assert config["data_contract"] == {
        "adjustment_basis": "split_adjusted",
        "canonical_source_id": "twelve_data_1day_split_adjusted",
        "compare_volume": False,
        "price_field": "close",
        "required_price_fields": ["close"],
        "required_source_ids": [
            "twelve_data_1day_split_adjusted",
            "yahoo_finance_chart_1day_split_adjusted",
        ],
        "rule": (
            "Twelve Data is canonical only after it and Yahoo Finance independently cover every expected "
            "XNYS session and agree within the fixed close-price tolerance. Yahoo is a verifier, never a "
            "fallback; no source averaging or substitution is permitted."
        ),
    }


def test_v4_contract_is_research_only_and_preserves_the_frozen_runtime_parameters() -> None:
    config = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

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
