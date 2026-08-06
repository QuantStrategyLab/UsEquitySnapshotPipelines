from __future__ import annotations

import ast
import copy
from pathlib import Path
from unittest.mock import Mock

import pytest

import us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer as producer
from test_soxl_pit_input_packager import _raw_sessions, _source_contract
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import FROZEN_XNYS_SESSIONS
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer import (
    ACTIVE_COMPONENTS,
    DISABLED_COMPONENTS,
    REGIME_LOGICAL_INPUTS,
    SoxlPITRegimeProducerError,
    produce_soxl_pit_regime_component_receipt,
    validate_soxl_pit_regime_source_contract,
)


def _component(profile: str, as_of: str, generated_at: str) -> dict[str, object]:
    return {
        "profile": profile,
        "schema_version": f"{profile}.v1",
        "as_of": as_of,
        "canonical_route": "no_action",
        "suggested_action": "no_action",
        "reason_codes": [],
        "generated_at": generated_at,
    }


def _price_rebound(as_of: str, generated_at: str) -> dict[str, object]:
    return {
        "schema_version": "volatility_delever_price_rebound_context.v1",
        "enabled": True,
        "confirmed": False,
        "as_of": as_of,
        "benchmark_symbol": "SOXX",
        "vix_symbol": "VIX",
        "reason_codes": [],
        "volatility_triggered": False,
        "trend_ok": True,
        "slope_ok": True,
        "constructive": True,
        "rebound_1d": False,
        "rebound_nd": False,
        "hard_filter": False,
        "soft_filter": False,
        "metrics": {"benchmark_close": 1.0},
        "generated_at": generated_at,
    }


def test_pinned_builders_receive_exact_prefix_and_generated_at_is_not_digest_input(
    monkeypatch,
) -> None:
    rows = _raw_sessions()
    source = validate_soxl_pit_regime_source_contract(
        rows,
        _source_contract(rows),
        expected_sessions=FROZEN_XNYS_SESSIONS,
    )
    generated = iter(("first", "first", "first", "second", "second", "second"))
    crisis = Mock(
        side_effect=lambda prices, **kwargs: _component(
            "crisis_response_shadow", kwargs["as_of"], next(generated)
        )
    )
    macro = Mock(
        side_effect=lambda prices, **kwargs: _component(
            "macro_risk_governor", kwargs["as_of"], next(generated)
        )
    )
    rebound = Mock(
        side_effect=lambda prices, config: _price_rebound(config["as_of"], next(generated))
    )
    monkeypatch.setattr(producer, "build_crisis_response_shadow_signal", crisis)
    monkeypatch.setattr(producer, "build_macro_risk_governor_signal", macro)
    monkeypatch.setattr(producer, "build_volatility_delever_price_rebound_context", rebound)
    monkeypatch.setattr(producer, "_verify_qsp_identity", lambda value: copy.deepcopy(value))

    first = produce_soxl_pit_regime_component_receipt(rows, source, session_index=251)
    second = produce_soxl_pit_regime_component_receipt(rows, source, session_index=251)

    assert first == second
    assert set(first["active_components"]) == set(ACTIVE_COMPONENTS)
    assert first["disabled_components"] == {
        component: {"enabled": False, "available": False} for component in DISABLED_COMPONENTS
    }
    assert first["real_producer"] is False
    assert first["evidence_class"] == "synthetic_fixture"
    assert "generated_at" not in str(first)
    assert first["provenance"]["prefix_session_count"] == 252
    assert first["provenance"]["prefix_end"] == rows[251]["date"]
    assert first["provenance"]["future_sessions_exposed"] is False

    prices = crisis.call_args_list[0].args[0]
    assert list(prices.columns) == list(REGIME_LOGICAL_INPUTS)
    assert len(prices) == 252
    assert prices.index[-1].date().isoformat() == rows[251]["date"]
    crisis_kwargs = crisis.call_args_list[0].kwargs
    assert len(crisis_kwargs["events"]) == 23
    assert crisis_kwargs["external_context"] is None
    assert crisis_kwargs["benchmark_symbol"] == "SOXX"
    assert crisis_kwargs["attack_symbol"] == "SOXL"
    assert crisis_kwargs["market_symbol"] == "SPY"
    assert crisis_kwargs["financial_symbols"] == ("XLF", "KRE")
    assert crisis_kwargs["credit_pairs"] == (("HYG", "IEF"), ("LQD", "IEF"))
    assert crisis_kwargs["rate_symbols"] == ("IEF", "TLT")
    assert crisis_kwargs["synthetic_attack_multiple"] == 0.0
    assert crisis_kwargs["ai_audit_enabled"] is False
    macro_kwargs = macro.call_args_list[0].kwargs
    assert macro_kwargs["external_context"] is None
    assert macro_kwargs["benchmark_symbol"] == "SOXX"
    assert macro_kwargs["attack_symbol"] == "SOXL"
    assert macro_kwargs["vix_symbols"] == ("VIX",)
    assert macro_kwargs["vix3m_symbols"] == ()
    assert macro_kwargs["credit_pairs"] == (("HYG", "IEF"), ("LQD", "IEF"))
    assert macro_kwargs["external_stress_actionable"] is False
    assert macro_kwargs["delever_risk_asset_scalar"] == 0.0
    rebound_config = rebound.call_args_list[0].args[1]
    assert rebound_config["strategy"] == "soxl_soxx_trend_income"
    assert rebound_config["benchmark_symbol"] == "SOXX"
    assert rebound_config["vix_symbols"] == ("VIX",)
    assert rebound_config["credit_pairs"] == ("HYG:IEF", "LQD:IEF")
    assert rebound_config["financial_symbols"] == ("XLF", "KRE")


def test_real_pinned_qsp_builders_produce_active_evidence_only_receipt() -> None:
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

    assert receipt["active_components"]["crisis"]["profile"] == "crisis_response_shadow"
    assert receipt["active_components"]["macro"]["profile"] == "macro_risk_governor"
    assert receipt["price_rebound_context"]["enabled"] is True
    assert receipt["disabled_components"] == {
        component: {"enabled": False, "available": False} for component in DISABLED_COMPONENTS
    }
    assert receipt["real_producer"] is False
    assert "provider_id" not in str(receipt)
    assert "logical_inputs" not in str(receipt)
    assert "generated_at" not in str(receipt)
    assert "metrics" not in str(receipt)
    assert "benchmark_close" not in str(receipt)


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda source: source["calendar"].update(source_revision="wrong"),
            "calendar identity",
        ),
        (lambda source: source.update(fixed_cutoff="2026-08-05T04:00:00Z"), "fixed cutoff"),
        (lambda source: source.update(input_content_sha256="0" * 64), "input content digest"),
        (lambda source: source.update(config_sha256="0" * 64), "config identity"),
        (
            lambda source: source["logical_inputs"][0].update(provider_id=""),
            "provider_id",
        ),
        (
            lambda source: source["logical_inputs"][1].update(provider_instrument_id="SOXL"),
            "alias collision",
        ),
        (lambda source: source["producer"].update(module_blob_sha="0" * 40), "producer source"),
        (
            lambda source: source["qsp"]["sources"]["macro"].update(blob_sha="0" * 40),
            "QSP source identity",
        ),
        (
            lambda source: source["logical_inputs"][0].update(
                entitlement_receipt_sha256="not-a-digest"
            ),
            "entitlement_receipt_sha256",
        ),
        (
            lambda source: source["logical_inputs"][0].update(
                license_or_usage_receipt_sha256="not-a-digest"
            ),
            "license_or_usage_receipt_sha256",
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
            lambda source: source["logical_inputs"][-1].update(logical_input_id="^VIX"),
            "exact 17 logical input",
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


def test_future_duplicate_missing_and_proxy_substitution_fail_closed() -> None:
    rows = _raw_sessions()
    source = _source_contract(rows)
    with pytest.raises(SoxlPITRegimeProducerError, match="exact XNYS sessions"):
        validate_soxl_pit_regime_source_contract(
            [*rows, copy.deepcopy(rows[-1])],
            source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    with pytest.raises(SoxlPITRegimeProducerError, match="exact XNYS sessions"):
        validate_soxl_pit_regime_source_contract(
            rows[:-1],
            source,
            expected_sessions=FROZEN_XNYS_SESSIONS,
        )

    duplicate = copy.deepcopy(rows)
    duplicate[1]["date"] = duplicate[0]["date"]
    duplicate_source = _source_contract(duplicate)
    with pytest.raises(SoxlPITRegimeProducerError, match="exact prefix session"):
        validate_soxl_pit_regime_source_contract(
            duplicate,
            duplicate_source,
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
        produce_soxl_pit_regime_component_receipt(
            rows,
            validated,
            session_index=len(rows),
        )

    mutated_after_validation = copy.deepcopy(rows)
    mutated_after_validation[0]["regime_inputs"]["VIX"] += 1.0
    with pytest.raises(SoxlPITRegimeProducerError, match="prefix input identity"):
        produce_soxl_pit_regime_component_receipt(
            mutated_after_validation,
            validated,
            session_index=0,
        )


def test_producer_has_no_provider_network_credential_cloud_or_runtime_surface() -> None:
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
        }
    )
    assert not hasattr(producer, "main")
