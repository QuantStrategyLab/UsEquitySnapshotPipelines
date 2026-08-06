from __future__ import annotations

import ast
import copy
from pathlib import Path

import pytest

import us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer as producer
from test_soxl_pit_input_packager import _raw_sessions, _source_contract
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import FROZEN_XNYS_SESSIONS
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer import (
    ALL_LOGICAL_INPUTS,
    CANDIDATE_ID,
    UNAVAILABLE_COMPONENTS,
    SoxlPITRegimeProducerError,
    produce_soxl_pit_regime_component_receipt,
    validate_soxl_pit_regime_source_contract,
)


_EXTERNAL_FIELDS = ("SPY", "XLF", "KRE", "HYG", "IEF", "LQD", "TLT", "VIX")


def test_core_only_receipt_is_deterministic_unavailable_and_digest_only() -> None:
    rows = _raw_sessions()
    source = validate_soxl_pit_regime_source_contract(
        rows,
        _source_contract(rows),
        expected_sessions=FROZEN_XNYS_SESSIONS,
    )

    first = produce_soxl_pit_regime_component_receipt(rows, source, session_index=251)
    second = produce_soxl_pit_regime_component_receipt(rows, source, session_index=251)

    assert first == second
    assert first["candidate_id"] == CANDIDATE_ID
    assert first["market_regime_control_enabled"] is False
    assert first["unavailable_components"] == {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    assert first["position_control_allowed"] is False
    assert first["real_producer"] is False
    assert first["evidence_class"] == "synthetic_fixture"
    assert first["provenance"]["prefix_session_count"] == 252
    assert first["provenance"]["prefix_end"] == rows[251]["date"]
    assert first["provenance"]["future_sessions_exposed"] is False
    assert first["provenance"]["logical_input_ids"] == list(ALL_LOGICAL_INPUTS)
    assert first["provenance"]["raw_series_persisted"] is False
    assert {"open", "high", "low", "close", "volume"}.isdisjoint(first)
    assert "canonical_route" not in str(first)
    assert "suggested_action" not in str(first)
    assert "VIX" not in str(first)


def test_core_only_producer_has_no_qsp_builder_surface() -> None:
    rows = _raw_sessions()
    source = validate_soxl_pit_regime_source_contract(
        rows,
        _source_contract(rows),
        expected_sessions=FROZEN_XNYS_SESSIONS,
    )

    receipt = produce_soxl_pit_regime_component_receipt(
        rows,
        source,
        session_index=len(rows) - 1,
    )

    assert receipt["unavailable_components"] == {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    assert not any(
        hasattr(producer, name)
        for name in (
            "build_crisis_response_shadow_signal",
            "build_macro_risk_governor_signal",
            "build_market_regime_control_signal",
            "build_volatility_delever_price_rebound_context",
        )
    )
    assert "provider_id" not in str(receipt)
    assert "logical_inputs" not in str(receipt)
    assert "metrics" not in str(receipt)


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda source: source["calendar"].update(source_revision="wrong"), "calendar identity"),
        (lambda source: source.update(fixed_cutoff="2026-08-05T04:00:00Z"), "fixed cutoff"),
        (lambda source: source.update(input_content_sha256="0" * 64), "input content digest"),
        (lambda source: source.update(candidate_id="wrong"), "candidate identity"),
        (
            lambda source: source.update(candidate_contract_sha256="0" * 64),
            "candidate contract identity",
        ),
        (lambda source: source["logical_inputs"][0].update(provider_id=""), "provider_id"),
        (
            lambda source: source["logical_inputs"][1].update(provider_instrument_id="SOXL"),
            "alias collision",
        ),
        (lambda source: source["producer"].update(module_blob_sha="0" * 40), "producer source"),
        (
            lambda source: source["logical_inputs"][0].update(
                entitlement_receipt_sha256="not-a-digest"
            ),
            "entitlement_receipt_sha256",
        ),
        (
            lambda source: source["logical_inputs"][1].update(adjustment_contract="unadjusted"),
            "source semantics",
        ),
        (
            lambda source: source["logical_inputs"][-1].update(last_date="2026-08-03"),
            "content identity",
        ),
        (
            lambda source: source["logical_inputs"].append(
                {**copy.deepcopy(source["logical_inputs"][-1]), "logical_input_id": "VIX"}
            ),
            "exact 9 logical input",
        ),
    ],
)
def test_source_calendar_license_adjustment_code_and_content_mismatch_fail_closed(
    mutator, message: str
) -> None:
    rows = _raw_sessions()
    source = _source_contract(rows)
    mutator(source)

    with pytest.raises(SoxlPITRegimeProducerError, match=message):
        validate_soxl_pit_regime_source_contract(
            rows,
            source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )


@pytest.mark.parametrize("external_field", _EXTERNAL_FIELDS)
def test_each_removed_external_field_is_rejected(external_field: str) -> None:
    rows = _raw_sessions()
    rows[0]["regime_inputs"] = {external_field: 1.0}

    with pytest.raises(SoxlPITRegimeProducerError, match="exact prefix session"):
        validate_soxl_pit_regime_source_contract(
            rows,
            _source_contract(_raw_sessions()),
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda bar: bar.pop("volume"),
        lambda bar: bar.update(close="invalid"),
        lambda bar: bar.update(close=-1.0),
        lambda bar: bar.update(low=100.0, high=101.0),
    ],
)
def test_direct_source_validator_rejects_invalid_bars(mutator) -> None:
    rows = _raw_sessions()
    mutator(rows[0]["bars"]["SOXL"])

    with pytest.raises(SoxlPITRegimeProducerError, match="invalid bar"):
        validate_soxl_pit_regime_source_contract(
            rows,
            _source_contract(rows),
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )


def test_future_duplicate_missing_proxy_and_post_validation_mutation_fail_closed() -> None:
    rows = _raw_sessions()
    source = _source_contract(rows)
    with pytest.raises(SoxlPITRegimeProducerError, match="exact XNYS sessions"):
        validate_soxl_pit_regime_source_contract(
            [*rows, copy.deepcopy(rows[-1])],
            source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    duplicate = copy.deepcopy(rows)
    duplicate[1]["date"] = duplicate[0]["date"]
    with pytest.raises(SoxlPITRegimeProducerError, match="exact prefix session"):
        validate_soxl_pit_regime_source_contract(
            duplicate,
            _source_contract(duplicate),
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    provider_source = _source_contract(rows)
    provider_source["data_class"] = "provider_observed"
    for entry in provider_source["logical_inputs"]:
        entry["data_origin"] = "provider_observed"
        entry["provider_id"] = "synthetic-proxy"
    with pytest.raises(SoxlPITRegimeProducerError, match="synthetic or proxy"):
        validate_soxl_pit_regime_source_contract(
            rows,
            provider_source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    unauthenticated_source = _source_contract(rows)
    unauthenticated_source["data_class"] = "provider_observed"
    for entry in unauthenticated_source["logical_inputs"]:
        entry["data_origin"] = "provider_observed"
        entry["provider_id"] = "future-approved-provider"
    with pytest.raises(SoxlPITRegimeProducerError, match="trusted source contract identity"):
        validate_soxl_pit_regime_source_contract(
            rows,
            unauthenticated_source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    validated = validate_soxl_pit_regime_source_contract(
        rows,
        source,
        expected_sessions=FROZEN_XNYS_SESSIONS,
    )
    with pytest.raises(SoxlPITRegimeProducerError, match="invalid prefix end"):
        produce_soxl_pit_regime_component_receipt(rows, validated, session_index=len(rows))

    mutated_after_validation = copy.deepcopy(rows)
    mutated_after_validation[0]["bars"]["SOXL"]["close"] += 1.0
    with pytest.raises(SoxlPITRegimeProducerError, match="prefix input identity"):
        produce_soxl_pit_regime_component_receipt(
            mutated_after_validation,
            validated,
            session_index=0,
        )


def test_producer_has_no_provider_qsp_network_credential_cloud_or_runtime_surface() -> None:
    tree = ast.parse(Path(producer.__file__).read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imported_roots.isdisjoint(
        {
            "requests",
            "urllib",
            "httpx",
            "socket",
            "google",
            "ib_insync",
            "yfinance",
            "boto3",
            "quant_strategy_plugins",
        }
    )
    assert not hasattr(producer, "main")
