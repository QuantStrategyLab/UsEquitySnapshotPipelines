from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_p3_isolated.py"
P2_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v2.json"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_p3_isolated", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _context() -> dict[str, object]:
    as_of = "2026-08-20T12:00:00+00:00"
    return {
        "schema_version": "qsl.soxl-core-only-p3-strategy-context.v1",
        "as_of": as_of,
        "portfolio": {
            "as_of": as_of,
            "total_equity": 100_000.0,
            "buying_power": 100_000.0,
            "cash_balance": 100_000.0,
            "positions": [],
            "metadata": {"observed_effective_exposure": 0.0},
        },
        "market_data": {
            "derived_indicators": {
                "SOXL": {"price": 80.0, "ma_trend": 75.0},
                "SOXX": {
                    "price": 109.0,
                    "ma_trend": 100.0,
                    "ma20": 105.0,
                    "ma20_slope": 1.0,
                    "rsi14": 50.0,
                    "bb_upper": 115.0,
                    "realized_volatility_10": 0.20,
                    "realized_volatility_10_dynamic_threshold": 0.50,
                    "realized_volatility_10_dynamic_sample_count": 252.0,
                },
            }
        },
    }


def test_context_validator_requires_exact_research_only_shape() -> None:
    module = _module()
    assert module.validate_source_context(_context())["schema_version"] == module.INPUT_SCHEMA

    payload = _context()
    payload["market_data"]["derived_indicators"]["SOXX"].pop("ma20")
    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module.validate_source_context(payload)


def test_context_validator_rejects_account_or_unbounded_metadata() -> None:
    module = _module()
    payload = _context()
    payload["portfolio"]["metadata"]["broker"] = "forbidden"
    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module.validate_source_context(payload)


def test_p2_candidate_validator_requires_the_complete_frozen_file() -> None:
    module = _module()
    candidate = json.loads(P2_CANDIDATE.read_text(encoding="utf-8"))

    identity = module.validate_p2_candidate(candidate)
    assert identity["candidate_id"] == module.P2_CANDIDATE_ID
    assert identity["config_sha256"] == module.P2_CONFIG_SHA256

    candidate["runtime_config"]["blend_gate_soxl_weight"] = 0.69
    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module.validate_p2_candidate(candidate)


def test_outer_runner_requires_digest_checked_source_result(monkeypatch, tmp_path) -> None:
    module = _module()
    context = tmp_path / "context.json"
    context.write_text(json.dumps(_context()), encoding="utf-8")
    project = tmp_path / "ues"
    project.mkdir()
    expected_execution_identity = {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": module.P2_UES_REVISION,
        "quant_platform_kit_revision": module.P2_QPK_REVISION,
        "uv_lock_sha256": module.P2_UES_UV_LOCK_SHA256,
    }
    source_decision = {
        "schema_version": module.DECISION_SCHEMA,
        "entrypoint": module.ENTRYPOINT,
        "as_of": "2026-08-20T12:00:00+00:00",
        "target_values": {"SOXL": 70_000.0, "SOXX": 20_000.0, "BOXX": 10_000.0},
        "diagnostics": {},
    }
    source_decision["output_sha256"] = hashlib.sha256(
        json.dumps(source_decision, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    expected_p2 = {
        "candidate_id": module.P2_CANDIDATE_ID,
        "config_sha256": module.P2_CONFIG_SHA256,
        "runtime_config": {},
    }
    monkeypatch.setattr(module, "validate_ues_project", lambda path: expected_execution_identity)
    monkeypatch.setattr(module, "validate_p2_candidate", lambda value: expected_p2)
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/uv")
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(source_decision)),
    )

    result = module.run_isolated_source(
        ues_project=project,
        input_path=context,
        p2_candidate_path=P2_CANDIDATE,
    )

    assert result["status"] == "SUCCESS"
    assert result["execution_identity"] == expected_execution_identity
    assert result["p2_identity"] == {
        "candidate_id": module.P2_CANDIDATE_ID,
        "config_sha256": module.P2_CONFIG_SHA256,
    }
    assert result["decision"] == source_decision
    material = {key: value for key, value in result.items() if key != "result_sha256"}
    assert result["result_sha256"] == hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()


def test_outer_runner_rejects_tampered_source_digest(monkeypatch, tmp_path) -> None:
    module = _module()
    context = tmp_path / "context.json"
    context.write_text(json.dumps(_context()), encoding="utf-8")
    project = tmp_path / "ues"
    project.mkdir()
    monkeypatch.setattr(module, "validate_ues_project", lambda path: {})
    monkeypatch.setattr(
        module,
        "validate_p2_candidate",
        lambda value: {"candidate_id": module.P2_CANDIDATE_ID, "config_sha256": module.P2_CONFIG_SHA256},
    )
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/uv")
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"schema_version": module.DECISION_SCHEMA, "output_sha256": "tampered"}),
        ),
    )

    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module.run_isolated_source(
            ues_project=project,
            input_path=context,
            p2_candidate_path=P2_CANDIDATE,
        )


def test_outer_batch_runner_binds_one_digest_to_ordered_decisions(monkeypatch, tmp_path) -> None:
    module = _module()
    context = tmp_path / "batch.json"
    context.write_text(
        json.dumps({"schema_version": module.BATCH_INPUT_SCHEMA, "contexts": [_context()]}),
        encoding="utf-8",
    )
    project = tmp_path / "ues"
    project.mkdir()
    source_batch = {
        "schema_version": module.BATCH_DECISION_SCHEMA,
        "entrypoint": module.ENTRYPOINT,
        "count": 1,
        "decisions": [],
    }
    source_batch["output_sha256"] = hashlib.sha256(
        json.dumps(source_batch, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(module, "validate_ues_project", lambda path: {})
    monkeypatch.setattr(
        module,
        "validate_p2_candidate",
        lambda value: {"candidate_id": module.P2_CANDIDATE_ID, "config_sha256": module.P2_CONFIG_SHA256},
    )
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/uv")
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(source_batch)),
    )

    result = module.run_isolated_batch(
        ues_project=project,
        input_path=context,
        p2_candidate_path=P2_CANDIDATE,
    )

    assert result["schema_version"] == module.ISOLATED_BATCH_RESULT_SCHEMA
    assert result["decision_batch"] == source_batch


def test_inner_batch_rejects_duplicate_or_unbounded_contexts(monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(module, "_source_decision", lambda context, candidate: {"as_of": "same"})
    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module._source_decision_batch(
            {"schema_version": module.BATCH_INPUT_SCHEMA, "contexts": [{}, {}]},
            {},
        )

    with pytest.raises(module.SoxlCoreOnlyP3IsolatedRunnerError):
        module._source_decision_batch(
            {"schema_version": module.BATCH_INPUT_SCHEMA, "contexts": [{}] * (module.MAX_BATCH_CONTEXTS + 1)},
            {},
        )


def test_source_contains_no_provider_or_execution_integration() -> None:
    source = SCRIPT.read_text(encoding="utf-8").lower()
    for forbidden in (
        "alpaca",
        "yfinance",
        "google.cloud",
        "requests",
        "boto",
        "orderintent",
        "build_risk_engine",
        "record_strategy_decision",
    ):
        assert forbidden not in source
