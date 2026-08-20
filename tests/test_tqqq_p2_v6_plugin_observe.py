from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pytest
from quant_strategy_plugins.plugin_signal_envelope_v2 import build_signal_envelope, payload_sha256

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding
from us_equity_snapshot_pipelines.lifecycle.tqqq_p2_v6_plugin_observe import (
    OBSERVE_ONLY_MODE,
    P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
    P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION,
    build_tqqq_p2_v6_plugin_observe_contract,
    build_tqqq_p2_v6_qqq_price_regime_observe_contract,
    observe_only_strategy_targets,
    verify_tqqq_p3_v6_plugin_observe,
    verify_tqqq_p3_v6_qqq_price_regime_observe,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "tqqq_p2_v6_plugin_observe"
_INPUT_ROOT_SHA256 = "f" * 64
_TARGETS = dict(
    json.loads(
        (Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v5.json").read_text(
            encoding="utf-8"
        )
    )["target_mapping"]["risk_on_normal"]
)


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
        "tool_version": "v1",
    }


def _synthetic_p1() -> tuple[dict[str, object], dict[str, object]]:
    """Build metadata-only P1 provenance; it reads neither files nor market data."""

    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(
        p1_binding.P2_V5_CONTRACT, date_cutoff="2026-08-18"
    )
    member_bytes = b'{"synthetic":true,"schema_version":"tqqq_core_only_private_bars.v1"}'
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=member_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(f"synthetic:{symbol}".encode()).hexdigest()
            for symbol in binding["data_identity"]["universe"]
        },
        contract=p1_binding.P2_V5_CONTRACT,
    )
    return binding, manifest


def _signal(
    binding: dict[str, object], manifest: dict[str, object], *, root_sha256: str = _INPUT_ROOT_SHA256
) -> dict[str, object]:
    payload = json.loads((FIXTURE_ROOT / "observer_payload.json").read_text(encoding="utf-8"))
    manifest_sha256 = p1_binding.validate_tqqq_core_only_input_manifest(
        manifest, binding, contract=p1_binding.P2_V5_CONTRACT
    )
    return build_signal_envelope(
        plugin_id="tqqq_regime_observer",
        producer={
            "repo": "QuantStrategyLab/QuantStrategyPlugins",
            "revision": "7" * 40,
            "entrypoint": "quant_strategy_plugins.market_regime_observer:build_signal",
            "code_sha256": "8" * 64,
            "config_sha256": "9" * 64,
        },
        input_provenance={
            "p1_manifest_sha256": manifest_sha256,
            "input_root_sha256": root_sha256,
            "date_cutoff": binding["data_identity"]["date_cutoff"],
        },
        payload=payload,
    )


def _contract_and_inputs() -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    binding, manifest = _synthetic_p1()
    signal = _signal(binding, manifest)
    contract = build_tqqq_p2_v6_plugin_observe_contract(
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        signal_envelope=signal,
    )
    return contract, binding, manifest, signal


def _qqq_bars(*, cutoff: str = "2026-08-18") -> list[dict[str, object]]:
    last = date.fromisoformat(cutoff)
    rows: list[dict[str, object]] = []
    for index in range(260):
        close = (100.0 + index * 0.2) * (1.0 + ((index % 7) - 3) * 0.002)
        rows.append(
            {
                "date": (last - timedelta(days=259 - index)).isoformat(),
                "close": close,
            }
        )
    return rows


def test_v6_observer_binds_exact_v5_p1_and_keeps_synthetic_targets_identical() -> None:
    contract, binding, manifest, signal = _contract_and_inputs()

    evidence = verify_tqqq_p3_v6_plugin_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        signal_envelope=signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert observe_only_strategy_targets(_TARGETS) == _TARGETS
    assert evidence["schema_version"] == P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION
    assert evidence["status"] == "VERIFIED_OBSERVE_ONLY"
    assert evidence["candidate"]["candidate_id"] == P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID
    assert evidence["p1"] == {
        "p1_manifest_sha256": signal["input"]["p1_manifest_sha256"],
        "input_root_sha256": _INPUT_ROOT_SHA256,
        "date_cutoff": "2026-08-18",
    }
    assert evidence["signal"] == {
        "schema_version": signal["schema_version"],
        "plugin_id": "tqqq_regime_observer",
        "payload_sha256": signal["payload_sha256"],
        "producer_revision": "7" * 40,
        "config_sha256": "9" * 64,
    }
    assert evidence["observer"] == {
        "mode": OBSERVE_ONLY_MODE,
        "strategy_target_transform": "none",
        "execution_authorized": False,
        "ai_input_allowed": False,
    }
    assert evidence["target_equivalence"]["equivalent"] is True
    assert "payload" not in evidence["signal"]
    assert "strategy_targets" not in evidence["target_equivalence"]


@pytest.mark.parametrize(
    ("mutator", "expected_reason"),
    [
        (lambda contract, signal: contract.pop("p1"), "invalid_observation_contract"),
        (lambda contract, signal: None, "p1_root_mismatch"),
        (
            lambda contract, signal: signal["input"].update({"date_cutoff": "2026-08-17"}),
            "plugin_input_provenance_mismatch",
        ),
        (
            lambda contract, signal: signal.update({"payload_sha256": "0" * 64}),
            "plugin_signal_rejected",
        ),
    ],
)
def test_v6_observer_parks_missing_or_mismatched_provenance(
    mutator, expected_reason: str
) -> None:
    contract, binding, manifest, signal = _contract_and_inputs()
    contract = deepcopy(contract)
    signal = deepcopy(signal)
    mutator(contract, signal)
    root_sha256 = "e" * 64 if expected_reason == "p1_root_mismatch" else _INPUT_ROOT_SHA256

    evidence = verify_tqqq_p3_v6_plugin_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=root_sha256,
        signal_envelope=signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == expected_reason
    assert set(evidence) == {"schema_version", "status", "reason_code", "authority"}


@pytest.mark.parametrize("forbidden_key", ["ai_model", "target_weight"])
def test_v6_observer_parks_ai_or_target_action_signals(forbidden_key: str) -> None:
    contract, binding, manifest, signal = _contract_and_inputs()
    unsafe_signal = deepcopy(signal)
    payload = dict(unsafe_signal["payload"])
    payload[forbidden_key] = "unsafe"
    unsafe_signal["payload"] = payload
    unsafe_signal["payload_sha256"] = payload_sha256(payload)

    evidence = verify_tqqq_p3_v6_plugin_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        signal_envelope=unsafe_signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert evidence == {
        "schema_version": P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION,
        "status": "PARKED",
        "reason_code": "plugin_signal_rejected",
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }


def test_v6_observer_parks_when_the_observer_candidate_changes_v5_targets() -> None:
    contract, binding, manifest, signal = _contract_and_inputs()
    changed_targets = dict(_TARGETS)
    changed_targets["TQQQ"] = 0.0
    changed_targets["BOXX"] = 0.53

    evidence = verify_tqqq_p3_v6_plugin_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        signal_envelope=signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=changed_targets,
    )

    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == "strategy_target_equivalence_failed"


def test_strict_qqq_v6_p3_recomputes_the_p1_bound_signal_before_recording_evidence() -> None:
    binding, manifest = _synthetic_p1()
    bars = _qqq_bars()
    contract, signal = build_tqqq_p2_v6_qqq_price_regime_observe_contract(
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=bars,
        qsp_revision="7" * 40,
    )

    evidence = verify_tqqq_p3_v6_qqq_price_regime_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=bars,
        signal_envelope=signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert evidence["status"] == "VERIFIED_OBSERVE_ONLY"
    assert evidence["signal"]["plugin_id"] == "qqq_price_regime_observer"
    assert evidence["recomputation"] == {
        "method": "IN_MEMORY_P1_VERIFIED_QQQ_CLOSE_ONLY",
        "matched": True,
    }
    assert "payload" not in evidence["signal"]


def test_strict_qqq_v6_p3_parks_if_even_one_p1_bar_no_longer_recomputes_the_signal() -> None:
    binding, manifest = _synthetic_p1()
    bars = _qqq_bars()
    contract, signal = build_tqqq_p2_v6_qqq_price_regime_observe_contract(
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=bars,
        qsp_revision="7" * 40,
    )
    changed_bars = deepcopy(bars)
    changed_bars[-1]["close"] = float(changed_bars[-1]["close"]) * 0.8

    evidence = verify_tqqq_p3_v6_qqq_price_regime_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=changed_bars,
        signal_envelope=signal,
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == "qqq_observer_recomputation_mismatch"


def test_strict_qqq_v6_p3_parks_a_non_mapping_signal_before_recomputation() -> None:
    binding, manifest = _synthetic_p1()
    bars = _qqq_bars()
    contract, _signal = build_tqqq_p2_v6_qqq_price_regime_observe_contract(
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=bars,
        qsp_revision="7" * 40,
    )

    evidence = verify_tqqq_p3_v6_qqq_price_regime_observe(
        contract=contract,
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=_INPUT_ROOT_SHA256,
        qqq_bars=bars,
        signal_envelope=[],  # type: ignore[arg-type]
        base_strategy_targets=_TARGETS,
        observer_strategy_targets=_TARGETS,
    )

    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == "plugin_signal_rejected"
