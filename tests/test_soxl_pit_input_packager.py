from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path

import pytest
from quant_platform_kit.risk.contracts import CandidateRiskIdentity

from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import (
    FIRST_ELIGIBLE_SESSION,
    FROZEN_XNYS_SESSIONS,
    MARKET_REGIME_CONFIG_SHA256,
    QPK_REVISION,
    QSP_REVISION,
    SOXL_PROMOTION_ASSETS,
    SoxlPITPackagerError,
    canonical_json_bytes,
    prepare_soxl_pit_input,
    publish_soxl_pit_input,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner import SoxlPromotionRunner


RUNNER_REVISION = "c" * 40
STRATEGY_REVISION = "d" * 40
MANDATE_ID = "soxl_p3_promotion_research_9_asset_one_run_v1"


def _components(session_date: str) -> dict[str, dict[str, object]]:
    return {
        "crisis": {
            "plugin": "crisis_response_shadow",
            "schema_version": "crisis_response_shadow.v1",
            "as_of": session_date,
            "canonical_route": "no_action",
            "suggested_action": "no_action",
            "reason_codes": [],
        },
        "macro": {
            "plugin": "macro_risk_governor",
            "schema_version": "macro_risk_governor.v1",
            "as_of": session_date,
            "canonical_route": "no_action",
            "suggested_action": "no_action",
            "reason_codes": [],
        },
        "taco": {
            "plugin": "taco_rebound_shadow",
            "schema_version": "taco_rebound_shadow.v1",
            "as_of": session_date,
            "canonical_route": "no_action",
            "suggested_action": "no_action",
            "reason_codes": [],
        },
        "panic_reversal": {
            "plugin": "panic_reversal_shadow",
            "schema_version": "panic_reversal_shadow.v1",
            "as_of": session_date,
            "canonical_route": "no_action",
            "suggested_action": "no_action",
            "reason_codes": [],
        },
    }


def _raw_sessions() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for session_index, session_date in enumerate(FROZEN_XNYS_SESSIONS):
        eligible = [
            symbol
            for symbol in SOXL_PROMOTION_ASSETS
            if symbol not in FIRST_ELIGIBLE_SESSION
            or session_date >= FIRST_ELIGIBLE_SESSION[symbol]
        ]
        bars: dict[str, dict[str, float]] = {}
        for symbol_index, symbol in enumerate(eligible):
            close = 50.0 + symbol_index * 5.0 + session_index * 0.01
            bars[symbol] = {
                "open": close * 0.999,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1_000_000.0 + session_index,
            }
        rows.append(
            {
                "date": session_date,
                "bars": bars,
                "regime_components": _components(session_date),
            }
        )
    return rows


def _source_contract(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "source_id": "synthetic-offline-bars-and-regime-components",
        "source_revision": "synthetic-fixture-v1",
        "observed_at": "2026-08-05T04:00:00Z",
        "effective_at": "2026-08-05T04:00:00Z",
        "as_of": "2026-08-05T04:01:00Z",
        "fixed_cutoff": "2026-08-05T03:59:59Z",
        "calendar_source": "exchange_calendars",
        "calendar_source_revision": "4.13.2",
        "adjustment_source": "synthetic-total-return-adjusted",
        "adjustment_source_revision": "synthetic-fixture-v1",
        "content_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
        "request_contract_sha256": "e" * 64,
        "license_id": "synthetic-fixture-only",
        "usage_scope": "offline-static-contract-tests-only",
        "market_regime_control_enabled": True,
        "producer": {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": "f" * 40,
            "tool": "soxl_pit_input_packager",
            "tool_version": "1",
        },
    }


def _binding(input_manifest_sha256: str) -> dict[str, object]:
    candidate = CandidateRiskIdentity(
        strategy_profile="soxl_soxx_trend_income",
        account_mode="single_strategy",
        strategy_revision=STRATEGY_REVISION,
        runner_revision=RUNNER_REVISION,
        config_sha256="1" * 64,
        input_manifest_sha256=input_manifest_sha256,
        authority_receipt_sha256="2" * 64,
    )
    return {
        "strategy_profile": candidate.strategy_profile,
        "account_mode": candidate.account_mode,
        "strategy_revision": candidate.strategy_revision,
        "runner_revision": candidate.runner_revision,
        "qpk_revision": QPK_REVISION,
        "plugin_revision": QSP_REVISION,
        "plugin_config_sha256": MARKET_REGIME_CONFIG_SHA256,
        "config_sha256": candidate.config_sha256,
        "input_manifest_sha256": candidate.input_manifest_sha256,
        "authority_receipt_sha256": candidate.authority_receipt_sha256,
        "candidate_identity_sha256": candidate.candidate_sha256,
        "mandate_id": MANDATE_ID,
        "mandate_digest_sha256": "3" * 64,
    }


def _assert_no_sensitive_keys(value: object) -> None:
    forbidden = {
        "authorization",
        "credential",
        "credentials",
        "password",
        "secret",
        "token",
        "cookie",
        "jwt",
        "api_key",
        "access_token",
        "refresh_token",
        "request_headers",
        "response_headers",
        "raw_response",
        "response_body",
        "provider_response",
    }
    if isinstance(value, dict):
        assert forbidden.isdisjoint(value)
        for item in value.values():
            _assert_no_sensitive_keys(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_sensitive_keys(item)


def test_exact_xnys_contract_and_atomic_package_are_deterministic(tmp_path: Path) -> None:
    rows = _raw_sessions()
    assert len(FROZEN_XNYS_SESSIONS) == 2_010
    assert FROZEN_XNYS_SESSIONS[0] == "2018-08-03"
    assert FROZEN_XNYS_SESSIONS[-1] == "2026-08-04"
    assert hashlib.sha256(canonical_json_bytes(list(FROZEN_XNYS_SESSIONS))).hexdigest() == (
        "6e3bf4713cca22264987c583cf4c5c94923850de4a3d18e76f66f42e719f2290"
    )

    prepared = prepare_soxl_pit_input(rows, _source_contract(rows))
    binding = _binding(prepared.input_manifest_sha256)
    first = publish_soxl_pit_input(prepared, binding, tmp_path / "first")
    second = publish_soxl_pit_input(prepared, binding, tmp_path / "second")

    assert first == second
    for filename in ("sessions.json", "input-manifest.json", "input.json", "package-manifest.json"):
        first_path = tmp_path / "first" / filename
        second_path = tmp_path / "second" / filename
        assert first_path.read_bytes() == second_path.read_bytes()
        assert first_path.stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "first").stat().st_mode & 0o777 == 0o700

    sessions = json.loads((tmp_path / "first" / "sessions.json").read_bytes())
    input_payload = json.loads((tmp_path / "first" / "input.json").read_bytes())
    package_manifest = json.loads((tmp_path / "first" / "package-manifest.json").read_bytes())
    assert input_payload == {
        "schema_version": "soxl_promotion_input.v2",
        "input_manifest": json.loads((tmp_path / "first" / "input-manifest.json").read_bytes()),
        "sessions": sessions,
    }
    runner = object.__new__(SoxlPromotionRunner)
    runner.input_payload = input_payload
    runner._validate_input()
    assert runner.input_manifest_sha256 == prepared.input_manifest_sha256
    assert package_manifest["identity"] == binding
    assert package_manifest["input_manifest_sha256"] == prepared.input_manifest_sha256
    assert package_manifest["contract"]["assets"] == list(SOXL_PROMOTION_ASSETS)
    assert package_manifest["contract"]["calendar"]["session_count"] == 2_010
    assert package_manifest["contract"]["windows"]["folds"] == [
        {"train_sessions": 420, "purge_sessions": 20, "test_sessions": 126, "embargo_sessions": 20},
        {"train_sessions": 420, "purge_sessions": 20, "test_sessions": 126, "embargo_sessions": 20},
        {"train_sessions": 420, "purge_sessions": 20, "test_sessions": 126, "embargo_sessions": 20},
    ]
    assert package_manifest["contract"]["windows"]["final_oos"] == {
        "start": "2025-08-04",
        "end": "2026-08-04",
        "sessions": 252,
        "minimum_calendar_months": 12,
        "actual_nine_assets_only": True,
    }
    assert package_manifest["contract"]["execution"] == {
        "signal_timing": "close_t",
        "execution_timing": "open_t_plus_1",
        "continuous_state": [
            "cash",
            "holdings",
            "lots",
            "hysteresis",
            "income",
            "executable_5pct_stop",
            "drawdown",
            "strategy_breaker",
            "account_breaker",
        ],
    }
    assert package_manifest["contract"]["cost_model_bps"] == [5, 10, 25]
    assert package_manifest["lifecycle_claims"] == {
        "promotion_eligible": False,
        "paper_authority": False,
        "shadow_authority": False,
        "live_authority": False,
        "order_authority": False,
        "real_backtest_executed": False,
    }

    qqqi_index = FROZEN_XNYS_SESSIONS.index(FIRST_ELIGIBLE_SESSION["QQQI"])
    assert "QQQI" not in sessions[qqqi_index - 1]["bars"]
    assert "QQQ" in sessions[qqqi_index - 1]["bars"]
    assert set(sessions[qqqi_index]["bars"]) == set(SOXL_PROMOTION_ASSETS)
    final_oos = sessions[-252:]
    assert final_oos[0]["date"] == "2025-08-04"
    assert all(set(row["bars"]) == set(SOXL_PROMOTION_ASSETS) for row in final_oos)

    for index, session in enumerate(sessions):
        regime = session["market_regime"]
        assert regime["schema_version"] == "market_regime_control.v1"
        assert regime["profile"] == "market_regime_control"
        assert regime["as_of"] == session["date"]
        assert regime["pit_provenance"]["prefix_session_count"] == index + 1
        assert regime["pit_provenance"]["prefix_end"] == session["date"]
        assert regime["pit_provenance"]["future_sessions_exposed"] is False
        assert regime["execution_controls"]["position_control_allowed"] is False
    _assert_no_sensitive_keys(input_payload)
    _assert_no_sensitive_keys(package_manifest)


def test_input_contract_fail_closed_matrix() -> None:
    original = _raw_sessions()

    cases: list[tuple[list[dict[str, object]], dict[str, object], str]] = []
    missing_component = copy.deepcopy(original)
    del missing_component[0]["regime_components"]["macro"]
    cases.append((missing_component, _source_contract(missing_component), "regime components"))

    future_component = copy.deepcopy(original)
    future_component[0]["regime_components"]["crisis"]["as_of"] = FROZEN_XNYS_SESSIONS[1]
    cases.append((future_component, _source_contract(future_component), "component as_of"))

    prelisting_bar = copy.deepcopy(original)
    prelisting_bar[0]["bars"]["SGOV"] = dict(prelisting_bar[0]["bars"]["QQQ"])
    cases.append((prelisting_bar, _source_contract(prelisting_bar), "eligible bar set"))

    missing_first_eligible = copy.deepcopy(original)
    first_eligible_index = FROZEN_XNYS_SESSIONS.index(FIRST_ELIGIBLE_SESSION["QQQI"])
    del missing_first_eligible[first_eligible_index]["bars"]["QQQI"]
    cases.append((missing_first_eligible, _source_contract(missing_first_eligible), "eligible bar set"))

    wrong_calendar = copy.deepcopy(original)
    wrong_calendar[1]["date"] = wrong_calendar[0]["date"]
    cases.append((wrong_calendar, _source_contract(wrong_calendar), "XNYS sessions"))

    sensitive = copy.deepcopy(original)
    sensitive[0]["regime_components"]["crisis"]["authorization"] = "do-not-persist"
    cases.append((sensitive, _source_contract(sensitive), "sensitive input"))

    camel_case_sensitive = copy.deepcopy(original)
    camel_case_sensitive[0]["regime_components"]["crisis"]["apiKey"] = "do-not-persist"
    cases.append((camel_case_sensitive, _source_contract(camel_case_sensitive), "sensitive input"))

    for rows, source, message in cases:
        with pytest.raises(SoxlPITPackagerError, match=message):
            prepare_soxl_pit_input(rows, source)

    source = _source_contract(original)
    source["fixed_cutoff"] = "2026-08-05T04:00:00Z"
    with pytest.raises(SoxlPITPackagerError, match="fixed cutoff"):
        prepare_soxl_pit_input(original, source)

    source = _source_contract(original)
    source["content_sha256"] = "0" * 64
    with pytest.raises(SoxlPITPackagerError, match="source digest"):
        prepare_soxl_pit_input(original, source)

    source = _source_contract(original)
    source["market_regime_control_enabled"] = False
    with pytest.raises(SoxlPITPackagerError, match="market regime control"):
        prepare_soxl_pit_input(original, source)


def test_identity_binding_and_publication_fail_closed(tmp_path: Path) -> None:
    rows = _raw_sessions()
    prepared = prepare_soxl_pit_input(rows, _source_contract(rows))
    valid = _binding(prepared.input_manifest_sha256)

    mutations = (
        ("input_manifest_sha256", "0" * 64, "input manifest identity"),
        ("candidate_identity_sha256", "0" * 64, "candidate identity"),
        ("plugin_revision", "0" * 40, "plugin revision"),
        ("qpk_revision", "0" * 40, "QPK revision"),
        ("runner_revision", "0" * 40, "producer and runner revision"),
    )
    for index, (field, value, message) in enumerate(mutations):
        binding = dict(valid)
        binding[field] = value
        with pytest.raises(SoxlPITPackagerError, match=message):
            publish_soxl_pit_input(prepared, binding, tmp_path / f"invalid-{index}")

    existing = tmp_path / "existing"
    existing.mkdir()
    sentinel = existing / "sentinel"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(SoxlPITPackagerError, match="must not exist"):
        publish_soxl_pit_input(prepared, valid, existing)
    assert sentinel.read_text(encoding="utf-8") == "keep"

    parent = tmp_path / "real-parent"
    parent.mkdir()
    (parent / "nested").mkdir()
    symlink_parent = tmp_path / "symlink-parent"
    symlink_parent.symlink_to(parent, target_is_directory=True)
    with pytest.raises(SoxlPITPackagerError, match="symlink"):
        publish_soxl_pit_input(prepared, valid, symlink_parent / "nested" / "package")
    assert not (parent / "nested" / "package").exists()


def test_packager_has_no_provider_cloud_broker_or_runtime_surface(monkeypatch) -> None:
    import us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
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
        {"requests", "urllib", "httpx", "socket", "google", "ib_insync", "yfinance"}
    )
    assert not hasattr(module, "main")

    monkeypatch.setattr(module, "_QSP_PLUGIN_SOURCE_SHA256", "0" * 64)
    with pytest.raises(SoxlPITPackagerError, match="dependency source identity mismatch"):
        prepare_soxl_pit_input([], {})
