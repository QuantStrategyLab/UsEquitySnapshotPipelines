from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _candidate() -> dict[str, object]:
    return {
        "schema_version": "qsl.tqqq-core-only-p2-candidate.v1",
        "candidate_id": "tqqq_core_only_p2_v1",
        "runtime_config": {
            "benchmark_symbol": "QQQ", "managed_symbols": ["TQQQ", "QQQM", "BOXX"],
            "dual_drive_qqq_weight": 0.45, "dual_drive_tqqq_weight": 0.45,
            "dual_drive_unlevered_symbol": "QQQM", "dual_drive_cash_reserve_ratio": 0.02,
            "income_layer_enabled": False, "option_overlay_enabled": False,
            "option_growth_overlay_enabled": False, "option_income_overlay_enabled": False,
            "ai_extensions": {"enabled": False}, "market_regime_control_enabled": False,
            "dual_drive_macro_risk_governor_enabled": False,
            "dual_drive_crisis_defense_enabled": False,
            "dual_drive_volatility_delever_retention_mode": "none",
            "dual_drive_volatility_delever_retention_ratio": 0.0,
        },
        "evaluation_plan": {
            "purge_sessions": 252,
            "purged_folds": [
                {"id": "f1", "train": ["2018-01-02", "2020-12-31"], "evaluation": ["2022-01-03", "2022-12-30"], "purge_sessions_after_train": 252},
                {"id": "f2", "train": ["2018-01-02", "2021-12-31"], "evaluation": ["2023-01-03", "2023-12-29"], "purge_sessions_after_train": 252},
                {"id": "f3", "train": ["2018-01-02", "2022-12-30"], "evaluation": ["2024-01-02", "2024-06-28"], "purge_sessions_after_train": 252},
            ],
            "locked_oos": {"start": "2025-08-01", "end": "2026-07-31"},
        },
        "cost_assumptions": {"turnover_cost_bps": 5.0, "stress_turnover_cost_bps": [10.0, 25.0]},
        "classification": {"research_only": True, "no_order": True, "size_zero_required": True, "legacy_parity_status": "NOT_COMPARABLE"},
        "explicitly_not_in_candidate": {"stop": {"status": "not_in_candidate"}, "cooldown": {"status": "not_in_candidate"}},
    }


def _envelope(candidate: dict[str, object]) -> dict[str, object]:
    return {"candidate": candidate, "risk_standard_id": "risk.v1", "risk_standard_sha256": "2" * 64}


def test_matching_full_candidate_is_accepted_and_old_config_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = _candidate()
    monkeypatch.setattr(evidence, "CANDIDATE_CONFIG_SHA256", _digest(candidate))
    assert evidence._validate_config(_envelope(candidate))["candidate"] == candidate
    with pytest.raises(evidence.TqqqPromotionEvidenceError):
        evidence._validate_config({"strategy_profile": "tqqq_core_parity_v1"})


def test_tampered_candidate_fails_digest_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = _candidate()
    monkeypatch.setattr(evidence, "CANDIDATE_CONFIG_SHA256", _digest(candidate))
    changed = deepcopy(candidate)
    changed["runtime_config"]["dual_drive_tqqq_weight"] = 0.15  # type: ignore[index]
    with pytest.raises(evidence.TqqqPromotionEvidenceError, match="config digest"):
        evidence._validate_config(_envelope(changed))


def test_full_runtime_config_is_propagated_without_overlay() -> None:
    candidate = _candidate()
    assert evidence._runtime_config(candidate) == candidate["runtime_config"]
    assert evidence._runtime_config(candidate)["dual_drive_macro_risk_governor_enabled"] is False


def test_replay_evidence_names_the_actual_private_ues_callable() -> None:
    assert evidence._tqqq_replay_callable_identity() == {
        "callable": "us_equity_strategies.entrypoints._build_tqqq_growth_income_decision",
        "ues_revision": "8b6b418bac74318f8054c5951521c9b62391de3e",
    }


def test_p1_binding_uses_the_authoritative_p2_digest() -> None:
    assert evidence.CANDIDATE_CONFIG_SHA256 == (
        "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69"
    )


def test_v2_candidate_selects_the_public_research_adapter() -> None:
    candidate = json.loads(
        (Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v2.json").read_text(
            encoding="utf-8"
        )
    )

    assert evidence._validate_config(candidate)["candidate"] == candidate
    callable_, identity = evidence._tqqq_replay_callable_and_identity(
        p1_binding.P2_V2_CONTRACT
    )

    assert identity == {
        "callable": "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision",
        "ues_revision": "5f0c30cdcaf3ee0f3f1c050acbe172580ea40c81",
    }
    assert callable_.__name__ == "build_tqqq_core_only_p2_v2_research_decision"


def test_v3_candidate_reuses_the_exact_public_research_adapter() -> None:
    candidate = json.loads(
        (Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v3.json").read_text(
            encoding="utf-8"
        )
    )

    assert evidence._validate_config(candidate)["candidate"] == candidate
    callable_, identity = evidence._tqqq_replay_callable_and_identity(
        p1_binding.P2_V3_CONTRACT
    )

    assert identity == {
        "callable": "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision",
        "ues_revision": "5f0c30cdcaf3ee0f3f1c050acbe172580ea40c81",
    }
    assert callable_.__name__ == "build_tqqq_core_only_p2_v2_research_decision"


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _canonical_alpaca_input_payload() -> dict[str, object]:
    binding = p1_binding.build_tqqq_core_only_p1_binding()
    symbols: dict[str, object] = {}
    for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX"):
        first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
        symbols[symbol] = {
            "bars": [
                {
                    "date": session.isoformat(),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                }
                for session in p1_binding._expected_xnys_sessions("2026-07-31")
                if first_eligible is None or session.isoformat() >= first_eligible
            ]
        }
    bars = {"schema_version": "tqqq_core_only_private_bars.v1", "symbols": symbols}
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-15T00:00:00Z",
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": "a" * 40,
            "tree_sha": "b" * 40,
            "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
            "tool_version": "v1",
        },
        member_bytes=_canonical(bars),
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(value)).hexdigest()
            for symbol, value in symbols.items()
        },
    )
    return {"binding": binding, "input_manifest": manifest, "bars": bars}


def test_static_consumer_accepts_canonical_alpaca_root() -> None:
    provenance, bars, manifest_sha256 = evidence._validate_input(
        _canonical_alpaca_input_payload(), {"candidate": _candidate()}
    )

    assert provenance["source"] == "ALPACA_MARKET_DATA"
    assert set(bars) == {"QQQ", "TQQQ", "QQQM", "BOXX"}
    assert isinstance(manifest_sha256, str) and len(manifest_sha256) == 64


def test_static_consumer_rejects_tampered_or_mixed_source_identity() -> None:
    payload = _canonical_alpaca_input_payload()
    payload["input_manifest"]["sources"][0]["source_id"] = "ibkr_adjusted_last:BOXX"  # type: ignore[index]

    with pytest.raises(evidence.TqqqPromotionEvidenceError, match="input binding"):
        evidence._validate_input(payload, {"candidate": _candidate()})
