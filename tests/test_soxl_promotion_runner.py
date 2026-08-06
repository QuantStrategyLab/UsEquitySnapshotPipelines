from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from quant_platform_kit.data.research_input import research_input_manifest_sha256
from quant_platform_kit.risk.contracts import RiskAction
from quant_platform_kit.strategy_contracts import PositionTarget, StrategyDecision

from us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner import (
    SOXL_PROMOTION_ASSETS,
    SoxlPromotionContractError,
    SoxlPromotionRunner,
    canonical_json_bytes,
    run_soxl_promotion_research,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import INPUT_CONTRACT_ID
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer import (
    CANDIDATE_ID,
    CORE_ONLY_CONFIG_SHA256,
    MARKET_REGIME_SCHEMA,
    SOURCE_CONTRACT_SCHEMA,
    UNAVAILABLE_COMPONENTS,
)
import us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner as runner_module


QPK_REVISION = "2f75b59289ef24ab47a3ed8d522c9ef8d6aea6b2"
UES_REVISION = "f799ad115660b17bc888cbe6e7461255ccee1735"
RUNNER_REVISION = "c" * 40
NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
VARIANTS = ("explicit_qqq_fallback", "cash_origin")
FIRST_ELIGIBLE_SESSION = {
    "SGOV": "2020-05-26",
    "SPYI": "2022-08-29",
    "BOXX": "2022-12-27",
    "QQQI": "2024-01-29",
}
AVAILABILITY_CONTRACT = {
    "schema_version": "soxl_asset_availability.v1",
    "universe": list(SOXL_PROMOTION_ASSETS),
    "always_eligible": ["SOXL", "SOXX", "SCHD", "DGRO", "QQQ"],
    "first_eligible_session": FIRST_ELIGIBLE_SESSION,
    "ordered_variants": list(VARIANTS),
    "primary_variant": "explicit_qqq_fallback",
    "transition_rule": "qqq_to_qqqi_close_t_open_t_plus_1",
    "unavailable_target_policy": "cash_without_renormalization",
    "price_identity_policy": "actual_symbol_only_no_proxy_backfill_forward_fill_substitution",
    "initial_state": "100_percent_cash",
}
SOURCE_CONTRACT_SHA256 = "b" * 64
SEGMENTS = (
    ("2018-08-03", "2020-04-03", 420),
    ("2020-04-06", "2020-05-04", 20),
    ("2020-05-05", "2020-10-30", 126),
    ("2020-11-02", "2020-11-30", 20),
    ("2020-12-01", "2022-08-02", 420),
    ("2022-08-03", "2022-08-30", 20),
    ("2022-08-31", "2023-03-02", 126),
    ("2023-03-03", "2023-03-30", 20),
    ("2023-03-31", "2024-11-29", 420),
    ("2024-12-02", "2024-12-30", 20),
    ("2024-12-31", "2025-07-03", 126),
    ("2025-07-07", "2025-08-01", 20),
    ("2025-08-04", "2026-08-04", 252),
)


@pytest.fixture(autouse=True)
def _bind_runner_revision(monkeypatch):
    monkeypatch.setattr(runner_module, "_resolve_runner_revision", lambda: RUNNER_REVISION)


def _segment_dates(start: str, end: str, count: int) -> list[date]:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    weekdays = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            weekdays.append(current)
        current += timedelta(days=1)
    indexes = [round(offset * (len(weekdays) - 1) / (count - 1)) for offset in range(count)]
    selected = [weekdays[index] for index in indexes]
    required_dates = [
        date.fromisoformat(value)
        for value in FIRST_ELIGIBLE_SESSION.values()
        if start_date <= date.fromisoformat(value) <= end_date
    ]
    for required_date in required_dates:
        if required_date not in selected:
            replace_index = min(
                (
                    index
                    for index in range(1, len(selected) - 1)
                    if selected[index] not in required_dates
                ),
                key=lambda index: abs((selected[index] - required_date).days),
            )
            selected[replace_index] = required_date
            selected.sort()
    assert len(set(selected)) == count
    assert selected[0] == start_date
    assert selected[-1] == end_date
    return selected


def _sessions() -> list[dict[str, object]]:
    sessions: list[dict[str, object]] = []
    session_dates = [
        session_date
        for start, end, count in SEGMENTS
        for session_date in _segment_dates(start, end, count)
    ]
    assert len(session_dates) == 2_010
    for index, session_date in enumerate(session_dates):
        eligible_assets = tuple(
            symbol
            for symbol in SOXL_PROMOTION_ASSETS
            if symbol not in FIRST_ELIGIBLE_SESSION
            or session_date >= date.fromisoformat(FIRST_ELIGIBLE_SESSION[symbol])
        )
        bars = {}
        for symbol_index, symbol in enumerate(eligible_assets):
            close = 50.0 + symbol_index * 5.0 + index * (0.025 + symbol_index * 0.001)
            bars[symbol] = {
                "open": close * 0.999,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1_000_000.0 + index,
            }
        sessions.append(
            {
                "date": session_date.isoformat(),
                "bars": bars,
                "eligible_assets": list(eligible_assets),
                "market_regime": {
                    "schema_version": MARKET_REGIME_SCHEMA,
                    "profile": "market_regime_control",
                    "candidate_id": CANDIDATE_ID,
                    "as_of": session_date.isoformat(),
                    "market_regime_control_enabled": False,
                    "component_signals": {
                        component: {"enabled": False, "available": False}
                        for component in UNAVAILABLE_COMPONENTS
                    },
                    "execution_controls": {
                        "broker_order_allowed": False,
                        "live_allocation_mutation_allowed": False,
                        "repository_broker_write_allowed": False,
                        "repository_allocation_mutation_allowed": False,
                        "position_control_allowed": False,
                        "consumption_evidence_status": "static_research_only",
                    },
                    "pit_provenance": {
                        "source_contract_sha256": SOURCE_CONTRACT_SHA256,
                        "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
                        "producer_receipt_sha256": hashlib.sha256(
                            f"producer:{session_date.isoformat()}".encode()
                        ).hexdigest(),
                        "prefix_input_manifest_sha256": hashlib.sha256(
                            f"prefix:{session_date.isoformat()}".encode()
                        ).hexdigest(),
                        "logical_input_ids": list(SOXL_PROMOTION_ASSETS),
                        "evidence_class": "synthetic_fixture",
                        "real_producer": False,
                        "prefix_session_count": index + 1,
                        "prefix_end": session_date.isoformat(),
                        "future_sessions_exposed": False,
                        "raw_series_persisted": False,
                    },
                },
            }
        )
    return sessions


def _manifest(sessions: list[dict[str, object]]) -> dict[str, object]:
    session_bytes = canonical_json_bytes(sessions)
    final_date = str(sessions[-1]["date"])
    return {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": "soxl-promotion-input-20260805",
        "research_input_contract_id": INPUT_CONTRACT_ID,
        "domain": "us_equity",
        "profile": "soxl_soxx_trend_income",
        "artifact_type": "immutable_adjusted_ohlcv_core_only",
        "observed_at": "2026-08-05T11:00:00Z",
        "effective_at": "2026-08-05T11:00:00Z",
        "as_of": "2026-08-05T12:00:00Z",
        "producer": {
            "repository": "private-input-package",
            "commit_sha": "d" * 40,
            "tree_sha": "e" * 40,
            "tool": "offline-fixture",
            "tool_version": "1",
        },
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_date": final_date,
            "source": "exchange-calendar",
            "source_revision": "fixture-v1",
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "private-input-package",
            "source_revision": "fixture-v1",
        },
        "sources": [
            {
                "source_id": symbol,
                "revision": "fixture-v1",
                "observed_at": "2026-08-05T11:00:00Z",
                "content_sha256": hashlib.sha256(
                    symbol.encode() + session_bytes
                ).hexdigest(),
            }
            for symbol in sorted(SOXL_PROMOTION_ASSETS)
        ],
        "members": [
            {
                "path": "sessions.json",
                "media_type": "application/json",
                "size_bytes": len(session_bytes),
                "sha256": hashlib.sha256(session_bytes).hexdigest(),
            }
        ],
    }


def _session_index(sessions: list[dict[str, object]], session_date: str) -> int:
    return next(index for index, session in enumerate(sessions) if session["date"] == session_date)


def _frozen_strategy_config() -> dict[str, object]:
    from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

    return json.loads(canonical_json_bytes(soxl_soxx_trend_income_manifest.default_config))


def _payloads() -> tuple[dict[str, object], dict[str, object]]:
    sessions = _sessions()
    manifest = _manifest(sessions)
    frozen_config = _frozen_strategy_config()
    manifest_sha256 = research_input_manifest_sha256(manifest)
    config_without_authority = {
        "schema_version": "soxl_p3_core_only_9_input_config.v1",
        "candidate_id": CANDIDATE_ID,
        "input_contract_id": INPUT_CONTRACT_ID,
        "source_contract_schema": SOURCE_CONTRACT_SCHEMA,
        "source_contract_sha256": SOURCE_CONTRACT_SHA256,
        "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
        "market_regime_control_enabled": False,
        "benchmark_symbol": "SOXX",
        "substitution_policy": "none_no_proxy_no_alias",
        "position_control_allowed": False,
        "strategy_profile": "soxl_soxx_trend_income",
        "domain": "us_equity",
        "account_mode": "single_strategy",
        "strategy_revision": UES_REVISION,
        "runner_revision": RUNNER_REVISION,
        "qpk_revision": QPK_REVISION,
        "frozen_strategy_config": frozen_config,
        "availability_contract": json.loads(canonical_json_bytes(AVAILABILITY_CONTRACT)),
        "ordered_variants": list(VARIANTS),
        "initial_equity": 100_000.0,
        "initial_weights": {},
        "stop_loss_distance": 0.05,
        "purge_sessions": 20,
        "embargo_sessions": 20,
        "folds": [
            {
                "train_start": "2018-08-03",
                "train_end": "2020-04-03",
                "test_start": "2020-05-05",
                "test_end": "2020-10-30",
            },
            {
                "train_start": "2020-12-01",
                "train_end": "2022-08-02",
                "test_start": "2022-08-31",
                "test_end": "2023-03-02",
            },
            {
                "train_start": "2023-03-31",
                "train_end": "2024-11-29",
                "test_start": "2024-12-31",
                "test_end": "2025-07-03",
            },
        ],
        "locked_oos": {"start": "2025-08-04", "end": "2026-08-04"},
        "risk_standard_id": "soxl_p3_candidate_bound_v1",
        "risk_standard_sha256": "f" * 64,
        "input_license": "authority-bound private internal research",
        "input_usage_scope": "non-commercial internal research",
        "learning_only": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }
    config_sha256 = hashlib.sha256(canonical_json_bytes(config_without_authority)).hexdigest()
    candidate = {
        "strategy_profile": "soxl_soxx_trend_income",
        "account_mode": "single_strategy",
        "strategy_revision": UES_REVISION,
        "runner_revision": RUNNER_REVISION,
        "config_sha256": config_sha256,
        "input_manifest_sha256": manifest_sha256,
        "authority_receipt_sha256": "a" * 64,
    }
    from quant_platform_kit.risk.contracts import CandidateRiskIdentity

    candidate_sha256 = CandidateRiskIdentity(**candidate).candidate_sha256
    factors = {symbol: 3 if symbol == "SOXL" else 1 for symbol in SOXL_PROMOTION_ASSETS}
    caps = {symbol: 0.15 if symbol == "SOXL" else 0.50 for symbol in SOXL_PROMOTION_ASSETS}
    mandate = {
        "mandate_id": "soxl_p3_core_only_9_input_research_v1",
        "mandate_version": "2026-08-05.1",
        "authority_receipt_sha256": "a" * 64,
        "authority_scope": "RESEARCH_ONLY",
        "strategy_profile": "soxl_soxx_trend_income",
        "account_mode": "single_strategy",
        "strategy_revision": UES_REVISION,
        "runner_revision": RUNNER_REVISION,
        "config_sha256": config_sha256,
        "input_manifest_sha256": manifest_sha256,
        "candidate_identity_sha256": candidate_sha256,
        "effective_at": "2026-08-05T11:00:00Z",
        "expires_at": "2026-08-05T13:00:00Z",
        "max_snapshot_age_seconds": 300,
        "effective_exposure_cap": 0.50,
        "loss_budget": 0.01,
        "product_caps": caps,
        "nominal_caps": caps,
        "product_leverage_factors": factors,
        "allowed_nonzero_assets": list(SOXL_PROMOTION_ASSETS),
        "source_revision": QPK_REVISION,
    }
    config = {
        **config_without_authority,
        "candidate_identity": candidate,
        "mandate_provenance": mandate,
    }
    return {
        "schema_version": INPUT_CONTRACT_ID,
        "input_manifest": manifest,
        "sessions": sessions,
    }, config


def _rebind_config_identity(config: dict[str, object]) -> None:
    config_sha256 = hashlib.sha256(
        canonical_json_bytes(
            {
                key: value
                for key, value in config.items()
                if key not in {"candidate_identity", "mandate_provenance"}
            }
        )
    ).hexdigest()
    config["candidate_identity"]["config_sha256"] = config_sha256
    config["mandate_provenance"]["config_sha256"] = config_sha256
    from quant_platform_kit.risk.contracts import CandidateRiskIdentity

    candidate_sha256 = CandidateRiskIdentity(**config["candidate_identity"]).candidate_sha256
    config["mandate_provenance"]["candidate_identity_sha256"] = candidate_sha256


def _approve(decision: StrategyDecision):
    return SimpleNamespace(
        decision=StrategyDecision(
            positions=decision.positions,
            budgets=decision.budgets,
            risk_flags=("risk_gate:passed",),
            diagnostics={**decision.diagnostics, "risk_gate": "APPROVE"},
        ),
        assessment=SimpleNamespace(outcome="APPROVE", assessment_sha256="1" * 64),
    )


def test_input_contract_requires_exact_point_in_time_assets_and_bound_manifest() -> None:
    input_payload, config = _payloads()
    assert SOXL_PROMOTION_ASSETS == (
        "SOXL",
        "SOXX",
        "BOXX",
        "SCHD",
        "DGRO",
        "SGOV",
        "SPYI",
        "QQQI",
        "QQQ",
    )
    assert tuple(input_payload["sessions"][0]["bars"]) == ("SOXL", "SOXX", "SCHD", "DGRO", "QQQ")
    qqqi_index = _session_index(input_payload["sessions"], FIRST_ELIGIBLE_SESSION["QQQI"])
    assert "QQQI" not in input_payload["sessions"][qqqi_index - 1]["bars"]
    assert "QQQI" in input_payload["sessions"][qqqi_index]["bars"]
    assert tuple(input_payload["sessions"][-1]["bars"]) == SOXL_PROMOTION_ASSETS
    assert [source["source_id"] for source in input_payload["input_manifest"]["sources"]] == sorted(
        SOXL_PROMOTION_ASSETS
    )
    first_regime = input_payload["sessions"][0]["market_regime"]
    assert first_regime["market_regime_control_enabled"] is False
    assert first_regime["component_signals"] == {
        component: {"enabled": False, "available": False}
        for component in UNAVAILABLE_COMPONENTS
    }
    assert first_regime["execution_controls"]["position_control_allowed"] is False
    assert "VIX" not in canonical_json_bytes(input_payload).decode()

    missing = json.loads(json.dumps(input_payload))
    del missing["sessions"][qqqi_index]["bars"]["QQQI"]
    with pytest.raises(SoxlPromotionContractError, match="eligible bar set"):
        SoxlPromotionRunner(missing, config, variant_id=VARIANTS[0])

    backfilled = json.loads(json.dumps(input_payload))
    backfilled["sessions"][qqqi_index - 1]["bars"]["QQQI"] = dict(
        backfilled["sessions"][qqqi_index]["bars"]["QQQI"]
    )
    with pytest.raises(SoxlPromotionContractError, match="eligible bar set"):
        SoxlPromotionRunner(backfilled, config, variant_id=VARIANTS[0])

    mismatched = json.loads(json.dumps(input_payload))
    mismatched["sessions"][700]["bars"]["SOXL"]["close"] += 0.001
    with pytest.raises(SoxlPromotionContractError, match="sessions.json digest"):
        SoxlPromotionRunner(mismatched, config, variant_id=VARIANTS[0])


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda payload: payload["input_manifest"]["sources"].append(
                {
                    "source_id": "VIX",
                    "revision": "fixture-v1",
                    "observed_at": "2026-08-05T11:00:00Z",
                    "content_sha256": "f" * 64,
                }
            ),
            "exact 9 input",
        ),
        (
            lambda payload: payload["sessions"][0]["market_regime"]["component_signals"][
                "crisis"
            ].update(available=True),
            "unavailable component",
        ),
        (
            lambda payload: payload["sessions"][1]["market_regime"]["pit_provenance"].update(
                source_contract_sha256="0" * 64
            ),
            "source contract identity",
        ),
        (
            lambda payload: payload["sessions"][0]["market_regime"].update(route="no_action"),
            "market regime fields",
        ),
    ],
)
def test_core_only_market_regime_contract_fails_closed(mutator, message: str) -> None:
    input_payload, config = _payloads()
    mutator(input_payload)

    with pytest.raises(SoxlPromotionContractError, match=message):
        SoxlPromotionRunner(input_payload, config, variant_id=VARIANTS[0])


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda config: config.update(purge_sessions=19), "20-session purge"),
        (lambda config: config.update(embargo_sessions=19), "20-session embargo"),
        (lambda config: config.update(folds=config["folds"][:2]), "exactly three"),
        (
            lambda config: config["locked_oos"].update(end=config["folds"][2]["test_end"]),
            "locked OOS",
        ),
        (lambda config: config.update(ordered_variants=list(reversed(VARIANTS))), "variant"),
        (
            lambda config: config["availability_contract"]["first_eligible_session"].update(
                QQQI="2024-01-30"
            ),
            "availability",
        ),
        (lambda config: config.update(candidate_id="wrong"), "core-only candidate"),
        (lambda config: config.update(benchmark_symbol="QQQ"), "SOXX benchmark"),
        (
            lambda config: config.update(source_contract_sha256="0" * 64),
            "source contract identity",
        ),
        (
            lambda config: config.update(position_control_allowed=True),
            "lifecycle claims",
        ),
        (
            lambda config: config["mandate_provenance"].update(
                mandate_id="soxl_p3_promotion_research_v1"
            ),
            "core-only mandate",
        ),
        (
            lambda config: config["mandate_provenance"].update(authority_scope="LIVE"),
            "core-only mandate",
        ),
    ],
)
def test_window_contract_fails_closed(mutator, message: str) -> None:
    input_payload, config = _payloads()
    mutator(config)
    _rebind_config_identity(config)
    with pytest.raises(SoxlPromotionContractError, match=message):
        SoxlPromotionRunner(input_payload, config, variant_id=VARIANTS[0])


@pytest.mark.parametrize(
    ("variant_id", "fallback_symbol"),
    (("explicit_qqq_fallback", "QQQ"), ("cash_origin", None)),
)
def test_indicator_and_candidate_decision_use_point_in_time_inputs_once(
    monkeypatch, variant_id: str, fallback_symbol: str | None
) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=variant_id, assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    index = _session_index(input_payload["sessions"], "2023-03-02")
    indicator_builder = Mock(return_value={"SOXL": {"close": 1.0}, "SOXX": {"close": 1.0}})
    evaluator = Mock(
        return_value=_approve(
            StrategyDecision(positions=(PositionTarget(symbol="BOXX", target_weight=0.50),))
        )
    )
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.build_semiconductor_rotation_indicators_from_history",
        indicator_builder,
    )
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.evaluate_soxl_soxx_trend_income_promotion_research",
        evaluator,
    )
    target = runner._evaluate_close(index, state)

    assert target == {"BOXX": 0.50}
    assert indicator_builder.call_count == 1
    assert len(indicator_builder.call_args.kwargs["soxl_history"]) == index + 1
    assert indicator_builder.call_args.kwargs["soxl_history"][-1] == input_payload["sessions"][index]["bars"]["SOXL"]["close"]
    assert evaluator.call_count == 1
    assert evaluator.call_args.kwargs["point_in_time_eligible_assets"] == frozenset(
        input_payload["sessions"][index]["eligible_assets"]
    )
    assert evaluator.call_args.kwargs["qqqi_preinception_fallback_symbol"] == fallback_symbol
    assert set(evaluator.call_args.kwargs["stop_loss_distances"]) == set(
        input_payload["sessions"][index]["eligible_assets"]
    )
    ctx = evaluator.call_args.args[0]
    assert ctx.portfolio.metadata["market_regime"]["as_of"] == input_payload["sessions"][index]["date"]
    assert ctx.portfolio.metadata["simulated_session"] == input_payload["sessions"][index]["date"]


def test_real_qpk_indicator_ues_bridge_and_risk_engine_are_compatible(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    index = _session_index(input_payload["sessions"], "2024-01-29")
    boxx_close = input_payload["sessions"][index]["bars"]["BOXX"]["close"]
    state.cash = 50_000.0
    state.quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
    state.quantities["BOXX"] = 50_000.0 / boxx_close
    state.lots = {symbol: [] for symbol in SOXL_PROMOTION_ASSETS}
    state.lots["BOXX"] = [runner._lot(state.quantities["BOXX"], boxx_close)]
    state.last_equity = 100_000.0
    state.high_water_equity = 100_000.0
    engine = Mock()
    engine.assess.return_value = RiskAction(action="approve", reason="passed")
    monkeypatch.setattr("quant_platform_kit.risk.gate._utc_now", lambda: NOW)
    monkeypatch.setattr("quant_platform_kit.risk.gate.build_risk_engine", lambda: engine)

    target = runner._evaluate_close(index, state)

    assert set(target).issubset(SOXL_PROMOTION_ASSETS)
    assert state.assessment_count == 1
    engine.assess.assert_called_once()
    assert "QQQI" in input_payload["sessions"][index]["eligible_assets"]


def test_runner_revision_mismatch_fails_before_execution(monkeypatch) -> None:
    input_payload, config = _payloads()
    monkeypatch.setattr(runner_module, "_resolve_runner_revision", lambda: "d" * 40)

    with pytest.raises(SoxlPromotionContractError, match="candidate revision"):
        SoxlPromotionRunner(input_payload, config, variant_id=VARIANTS[0])


def test_initial_state_is_cash_and_variant_bound() -> None:
    input_payload, config = _payloads()
    primary = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    sensitivity = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[1], assessment_clock=lambda: NOW
    )
    primary_state = primary._initial_state()
    sensitivity_state = sensitivity._initial_state()

    assert primary_state.cash == 100_000.0
    assert set(primary_state.quantities.values()) == {0.0}
    assert not any(primary_state.lots.values())
    assert primary_state.normalized is True
    assert primary._state_digest(primary_state) != sensitivity._state_digest(sensitivity_state)


def test_qqq_gap_stop_precedes_pending_order_and_blocks_same_session_reentry(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    state.normalized = True
    state.cash = 0.0
    state.quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
    state.quantities["QQQ"] = 100.0
    state.lots["QQQ"] = [runner._lot(100.0, 100.0)]
    state.pending_target = {"QQQ": 0.15, "BOXX": 0.35}
    gate = Mock(side_effect=lambda decision, *args, **kwargs: _approve(decision))
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        gate,
    )
    index = _session_index(input_payload["sessions"], "2023-03-02")
    session = runner.sessions[index]
    session["bars"]["QQQ"].update(open=94.0, high=96.0, low=90.0, close=93.0)

    runner._execute_open(index, state, total_cost_bps=5.0)

    assert state.quantities["QQQ"] == 0.0
    assert "QQQ" in state.stopped_today
    assert state.lots["QQQ"] == []
    assert gate.call_count == 1
    assert state.assessment_count == 1


def test_new_open_lot_is_not_stopped_by_same_sessions_pre_entry_low(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    state.normalized = True
    state.cash = 100_000.0
    state.quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
    state.lots = {symbol: [] for symbol in SOXL_PROMOTION_ASSETS}
    state.pending_target = {"SOXL": 0.10}
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        lambda decision, *args, **kwargs: _approve(decision),
    )
    index = _session_index(input_payload["sessions"], "2024-11-29")
    session = input_payload["sessions"][index]
    session["bars"]["SOXL"].update(open=100.0, high=102.0, low=90.0, close=101.0)

    runner._execute_open(index, state, total_cost_bps=5.0)

    assert state.quantities["SOXL"] > 0.0
    assert len(state.lots["SOXL"]) == 1
    assert state.stop_count == 0


def test_qqq_to_qqqi_transition_executes_next_open_with_full_half_l1_cost(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    index = _session_index(input_payload["sessions"], "2024-01-29")
    qqq_close = input_payload["sessions"][index]["bars"]["QQQ"]["close"]
    state.cash = 90_000.0
    state.quantities["QQQ"] = 10_000.0 / qqq_close
    state.lots["QQQ"] = [runner._lot(state.quantities["QQQ"], qqq_close)]
    evaluator = Mock(
        return_value=_approve(
            StrategyDecision(positions=(PositionTarget(symbol="QQQI", target_weight=0.10),))
        )
    )
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.build_semiconductor_rotation_indicators_from_history",
        lambda **kwargs: {"SOXL": {}, "SOXX": {}},
    )
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.evaluate_soxl_soxx_trend_income_promotion_research",
        evaluator,
    )

    state.pending_target = runner._evaluate_close(index, state)
    next_opens = {
        symbol: float(value["open"])
        for symbol, value in input_payload["sessions"][index + 1]["bars"].items()
    }
    equity_before = runner._equity(state, next_opens)
    runner._execute_open(index + 1, state, total_cost_bps=5.0)

    assert evaluator.call_count == 1
    assert evaluator.call_args.kwargs["qqqi_preinception_fallback_symbol"] is None
    assert state.quantities["QQQ"] == 0.0
    assert state.quantities["QQQI"] > 0.0
    assert state.turnover == pytest.approx(0.10)
    assert state.costs_paid == pytest.approx(equity_before * 0.10 * 5.0 / 10_000.0)
    assert state.lots["QQQI"][0].stop_price == pytest.approx(
        state.lots["QQQI"][0].entry_price * 0.95
    )


def test_external_evaluation_exception_is_redacted_and_fails_closed(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    state.normalized = True
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.build_semiconductor_rotation_indicators_from_history",
        lambda **kwargs: {"SOXL": {}, "SOXX": {}},
    )
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.evaluate_soxl_soxx_trend_income_promotion_research",
        Mock(side_effect=RuntimeError("private provider detail")),
    )

    with pytest.raises(SoxlPromotionContractError, match="candidate decision evaluation failed") as exc:
        runner._evaluate_close(_session_index(input_payload["sessions"], "2024-11-29"), state)

    assert "private provider detail" not in str(exc.value)
    assert state.assessment_count == 0


def test_half_l1_cost_and_continuous_state_are_recorded(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    state.normalized = True
    state.cash = 100_000.0
    state.quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
    state.lots = {symbol: [] for symbol in SOXL_PROMOTION_ASSETS}
    state.pending_target = {"BOXX": 0.50}
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        lambda decision, *args, **kwargs: _approve(decision),
    )

    runner._execute_open(
        _session_index(input_payload["sessions"], "2024-11-29"), state, total_cost_bps=10.0
    )

    assert state.turnover == pytest.approx(0.50)
    assert state.costs_paid == pytest.approx(50.0)
    assert state.pending_target is None
    assert state.quantities["BOXX"] > 0.0
    assert state.lots["BOXX"]


def test_breakers_are_persistent_and_fail_closed(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(
        input_payload, config, variant_id=VARIANTS[0], assessment_clock=lambda: NOW
    )
    state = runner._initial_state()
    state.normalized = True
    state.high_water_equity = 100_000.0
    state.last_equity = 89_000.0
    gate = Mock(side_effect=lambda decision, *args, **kwargs: _approve(decision))
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        gate,
    )

    index = _session_index(input_payload["sessions"], "2024-11-29")
    assert runner._evaluate_close(index, state) == {}
    assert state.account_parked is True
    assert runner._evaluate_close(index + 1, state) == {}
    assert gate.call_count == 2


def test_producer_uses_qpk_orchestrator_and_writes_truthful_25bp_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    input_payload, config = _payloads()
    replay = Mock()

    def replay_window(runner, start, end, cost):
        replay(runner.variant_id, start, end, cost)
        return _synthetic_window(start, end, cost)

    monkeypatch.setattr(SoxlPromotionRunner, "_replay_window", replay_window)

    result = run_soxl_promotion_research(
        input_payload=input_payload,
        config_payload=config,
        output_dir=tmp_path,
        generated_at="2026-08-05T12:00:00Z",
    )

    evidence_path = tmp_path / "strategy-evidence-package.v2.json"
    stress_25_path = tmp_path / "artifacts" / "cost-stress-25bp.json"
    assert evidence_path.is_file()
    assert stress_25_path.is_file()
    evidence = json.loads(evidence_path.read_text())
    stress_25 = json.loads(stress_25_path.read_text())
    config_artifact = json.loads((tmp_path / "artifacts" / "config.json").read_text())
    backtest_artifact = json.loads((tmp_path / "artifacts" / "backtest.json").read_text())
    risk_artifact = json.loads((tmp_path / "artifacts" / "risk.json").read_text())
    assert evidence["backtest"]["orchestrator"] == "BacktestOrchestrator"
    assert [item["total_cost_bps"] for item in evidence["cost_stress"]["scenarios"]] == [5.0, 10.0, 15.0]
    assert stress_25["total_cost_bps"] == 25.0
    assert set(stress_25["variants"]) == set(VARIANTS)
    assert stress_25["variants"][VARIANTS[0]]["locked_oos_result"]["total_return"] == pytest.approx(0.20)
    assert config_artifact["availability_contract"] == AVAILABILITY_CONTRACT
    assert config_artifact["ordered_variants"] == list(VARIANTS)
    assert config_artifact["initial_weights"] == {}
    assert backtest_artifact["schema_version"] == "soxl_promotion_backtest.v2"
    availability_sha256 = hashlib.sha256(canonical_json_bytes(AVAILABILITY_CONTRACT)).hexdigest()
    assert backtest_artifact["availability_contract_sha256"] == availability_sha256
    assert backtest_artifact["availability_segments"]["pre_qqqi"]["observed_qqqi"] is False
    assert backtest_artifact["availability_segments"]["locked_oos"] == {
        "start": "2025-08-04",
        "end": "2026-08-04",
        "session_count": 252,
        "actual_only": True,
    }
    assert set(backtest_artifact["variants"]) == set(VARIANTS)
    assert risk_artifact["schema_version"] == "soxl_p3_acceptance.v2"
    assert risk_artifact["availability_contract_sha256"] == availability_sha256
    assert risk_artifact["status"] == "PASS"
    assert risk_artifact["proxy_sensitive"] is False
    assert all(risk_artifact["variants"][variant]["status"] == "PASS" for variant in VARIANTS)
    assert all(risk_artifact["variants"][variant]["final_oos_actual_only"] for variant in VARIANTS)
    promotion_run = evidence["backtest"]["promotion_run"]
    assert promotion_run["locked_oos_start"] == "2025-08-04"
    assert promotion_run["locked_oos_end"] == "2026-08-04"
    assert promotion_run["locked_oos_result"]["observation_count"] == 252
    assert result["cost_stress_25bp_sha256"] == hashlib.sha256(stress_25_path.read_bytes()).hexdigest()
    assert evidence["human_acceptance"] is None
    assert evidence["lifecycle_claims"] == {
        "learning_only": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }
    from quant_platform_kit.strategy_lifecycle import validate_evidence_package_v2

    assert validate_evidence_package_v2(evidence, base_dir=tmp_path) == ()
    assert replay.call_count == 32


def test_producer_fails_proxy_sensitive_when_variant_direction_reverses(
    tmp_path: Path, monkeypatch
) -> None:
    input_payload, config = _payloads()

    def replay_window(runner, start, end, cost):
        promotion_passes = runner.variant_id == VARIANTS[0]
        return _synthetic_window(start, end, cost, promotion_passes=promotion_passes)

    monkeypatch.setattr(SoxlPromotionRunner, "_replay_window", replay_window)

    with pytest.raises(SoxlPromotionContractError, match="PROXY_SENSITIVE"):
        run_soxl_promotion_research(
            input_payload=input_payload,
            config_payload=config,
            output_dir=tmp_path,
            generated_at="2026-08-05T12:00:00Z",
        )

    risk_artifact = json.loads((tmp_path / "artifacts" / "risk.json").read_text())
    assert risk_artifact["status"] == "PROXY_SENSITIVE"
    assert risk_artifact["proxy_sensitive"] is True
    assert risk_artifact["variants"][VARIANTS[0]]["status"] == "PASS"
    assert risk_artifact["variants"][VARIANTS[1]]["status"] == "FAIL"


def _synthetic_window(
    start: date,
    end: date,
    total_cost_bps: float,
    *,
    promotion_passes: bool = True,
):
    from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult
    from us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner import WindowEvidence

    total_return = (
        0.20 if total_cost_bps == 25.0 else 0.30 - total_cost_bps / 1_000.0
    ) if promotion_passes else 0.05
    result = BacktestResult(
        strategy_profile="soxl_soxx_trend_income",
        domain="us_equity",
        param_set_id="soxl_p3",
        params={},
        sharpe_ratio=1.2,
        calmar_ratio=1.5,
        sortino_ratio=1.8,
        max_drawdown=-0.10 if promotion_passes else -0.20,
        cagr=0.15 if promotion_passes else 0.05,
        volatility=0.20,
        win_rate=0.55,
        total_return=total_return,
        start_date=start,
        end_date=end,
        observation_count=252 if (start, end) == (date(2025, 8, 4), date(2026, 8, 4)) else 126,
        benchmark_symbol="SOXX",
        benchmark_cagr=0.10,
        benchmark_max_drawdown=-0.12,
        excess_cagr=0.05 if promotion_passes else -0.05,
        oos_sharpe=1.2,
        oos_calmar=1.5,
        oos_max_drawdown=-0.10 if promotion_passes else -0.20,
        walk_forward_stability=1.0,
        run_duration_seconds=0.01,
    )
    return WindowEvidence(
        result=result,
        recovery_sessions=20 if promotion_passes else None,
        recovery_censored=not promotion_passes,
        benchmark_recovery_sessions=25,
        benchmark_recovery_censored=False,
        benchmark_total_return=0.10,
        upside_capture=0.75,
        upside_participation=0.70,
        turnover=1.0,
        trade_count=10,
        profit_factor=1.4,
        var_95=-0.02,
        cvar_95=-0.03,
        information_ratio=0.8,
        information_coefficient=0.1,
        costs_paid=100.0,
        assessment_count=10,
        state_digest_sha256="2" * 64,
    )
