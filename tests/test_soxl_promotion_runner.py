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
import us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner as runner_module


QPK_REVISION = "2f75b59289ef24ab47a3ed8d522c9ef8d6aea6b2"
UES_REVISION = "b49bde5910276187b83b4a587e4ddf210bcece89"
RUNNER_REVISION = "c" * 40
NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _bind_runner_revision(monkeypatch):
    monkeypatch.setattr(runner_module, "_resolve_runner_revision", lambda: RUNNER_REVISION)


def _sessions(count: int = 2_050) -> list[dict[str, object]]:
    sessions: list[dict[str, object]] = []
    session_date = date(2018, 1, 2)
    for index in range(count):
        while session_date.weekday() >= 5:
            session_date += timedelta(days=1)
        bars = {}
        for symbol_index, symbol in enumerate(SOXL_PROMOTION_ASSETS):
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
                "market_regime": {
                    "plugin": "market_regime_control",
                    "schema_version": "market_regime_control.v1",
                    "as_of": session_date.isoformat(),
                    "route": "no_action",
                    "active": False,
                },
            }
        )
        session_date += timedelta(days=1)
    return sessions


def _manifest(sessions: list[dict[str, object]]) -> dict[str, object]:
    session_bytes = canonical_json_bytes(sessions)
    final_date = str(sessions[-1]["date"])
    return {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": "soxl-promotion-input-20260805",
        "research_input_contract_id": "soxl_promotion_input.v1",
        "domain": "us_equity",
        "profile": "soxl_soxx_trend_income",
        "artifact_type": "immutable_adjusted_ohlcv_and_pit_regime",
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
                "source_id": "bars-and-regime",
                "revision": "fixture-v1",
                "observed_at": "2026-08-05T11:00:00Z",
                "content_sha256": hashlib.sha256(session_bytes).hexdigest(),
            }
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


def _frozen_strategy_config() -> dict[str, object]:
    from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

    return json.loads(canonical_json_bytes(soxl_soxx_trend_income_manifest.default_config))


def _payloads() -> tuple[dict[str, object], dict[str, object]]:
    sessions = _sessions()
    manifest = _manifest(sessions)
    frozen_config = _frozen_strategy_config()
    config_sha256 = hashlib.sha256(canonical_json_bytes(frozen_config)).hexdigest()
    manifest_sha256 = research_input_manifest_sha256(manifest)
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
        "mandate_id": "soxl_p3_promotion_research_v1",
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
    folds = []
    for train_start, train_end, test_start, test_end in (
        (0, 419, 440, 565),
        (586, 1005, 1026, 1151),
        (1172, 1591, 1612, 1737),
    ):
        folds.append(
            {
                "train_start": sessions[train_start]["date"],
                "train_end": sessions[train_end]["date"],
                "test_start": sessions[test_start]["date"],
                "test_end": sessions[test_end]["date"],
            }
        )
    config = {
        "schema_version": "soxl_promotion_config.v1",
        "strategy_profile": "soxl_soxx_trend_income",
        "domain": "us_equity",
        "account_mode": "single_strategy",
        "strategy_revision": UES_REVISION,
        "runner_revision": RUNNER_REVISION,
        "qpk_revision": QPK_REVISION,
        "frozen_strategy_config": frozen_config,
        "candidate_identity": candidate,
        "mandate_provenance": mandate,
        "initial_equity": 100_000.0,
        "initial_weights": {"BOXX": 1.0},
        "stop_loss_distance": 0.05,
        "purge_sessions": 20,
        "embargo_sessions": 20,
        "folds": folds,
        "locked_oos": {
            "start": sessions[1758]["date"],
            "end": sessions[2049]["date"],
        },
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
    return {
        "schema_version": "soxl_promotion_input.v1",
        "input_manifest": manifest,
        "sessions": sessions,
    }, config


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


def test_input_contract_requires_all_eight_assets_and_bound_manifest() -> None:
    input_payload, config = _payloads()
    assert tuple(input_payload["sessions"][0]["bars"]) == SOXL_PROMOTION_ASSETS

    missing = json.loads(json.dumps(input_payload))
    del missing["sessions"][700]["bars"]["QQQI"]
    with pytest.raises(SoxlPromotionContractError, match="complete eight-asset"):
        SoxlPromotionRunner(missing, config)

    mismatched = json.loads(json.dumps(input_payload))
    mismatched["sessions"][700]["bars"]["SOXL"]["close"] += 0.001
    with pytest.raises(SoxlPromotionContractError, match="sessions.json digest"):
        SoxlPromotionRunner(mismatched, config)


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
    ],
)
def test_window_contract_fails_closed(mutator, message: str) -> None:
    input_payload, config = _payloads()
    mutator(config)
    with pytest.raises(SoxlPromotionContractError, match=message):
        SoxlPromotionRunner(input_payload, config)


def test_indicator_and_candidate_decision_use_point_in_time_inputs_once(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
    state = runner._initial_state()
    index = 500
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
    state.normalized = True

    target = runner._evaluate_close(index, state)

    assert target == {"BOXX": 0.50}
    assert indicator_builder.call_count == 1
    assert len(indicator_builder.call_args.kwargs["soxl_history"]) == index + 1
    assert indicator_builder.call_args.kwargs["soxl_history"][-1] == input_payload["sessions"][index]["bars"]["SOXL"]["close"]
    assert evaluator.call_count == 1
    ctx = evaluator.call_args.args[0]
    assert ctx.portfolio.metadata["market_regime"]["as_of"] == input_payload["sessions"][index]["date"]
    assert ctx.portfolio.metadata["simulated_session"] == input_payload["sessions"][index]["date"]


def test_real_qpk_indicator_ues_bridge_and_risk_engine_are_compatible(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
    state = runner._initial_state()
    state.normalized = True
    index = 500
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


def test_runner_revision_mismatch_fails_before_execution(monkeypatch) -> None:
    input_payload, config = _payloads()
    monkeypatch.setattr(runner_module, "_resolve_runner_revision", lambda: "d" * 40)

    with pytest.raises(SoxlPromotionContractError, match="candidate revision"):
        SoxlPromotionRunner(input_payload, config)


def test_initial_boxx_normalization_is_reduce_only_and_assessed_once(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
    state = runner._initial_state()
    gate = Mock(side_effect=lambda decision, *args, **kwargs: _approve(decision))
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        gate,
    )

    target = runner._evaluate_close(420, state)

    assert target == {"BOXX": 0.50}
    assert gate.call_count == 1
    assert gate.call_args.kwargs["normalization_origin_weights"] == {"BOXX": 1.0}
    assert state.normalized is True


def test_gap_stop_precedes_pending_order_and_blocks_same_session_reentry(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
    state = runner._initial_state()
    state.normalized = True
    state.cash = 0.0
    state.quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
    state.quantities["SOXL"] = 100.0
    state.lots["SOXL"] = [runner._lot(100.0, 100.0)]
    state.pending_target = {"SOXL": 0.15, "BOXX": 0.35}
    gate = Mock(side_effect=lambda decision, *args, **kwargs: _approve(decision))
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        gate,
    )
    session = input_payload["sessions"][500]
    session["bars"]["SOXL"].update(open=94.0, high=96.0, low=90.0, close=93.0)

    runner._execute_open(500, state, total_cost_bps=5.0)

    assert state.quantities["SOXL"] == 0.0
    assert "SOXL" in state.stopped_today
    assert state.lots["SOXL"] == []
    assert gate.call_count == 1
    assert state.assessment_count == 1


def test_new_open_lot_is_not_stopped_by_same_sessions_pre_entry_low(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
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
    session = input_payload["sessions"][500]
    session["bars"]["SOXL"].update(open=100.0, high=102.0, low=90.0, close=101.0)

    runner._execute_open(500, state, total_cost_bps=5.0)

    assert state.quantities["SOXL"] > 0.0
    assert len(state.lots["SOXL"]) == 1
    assert state.stop_count == 0


def test_external_evaluation_exception_is_redacted_and_fails_closed(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
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
        runner._evaluate_close(500, state)

    assert "private provider detail" not in str(exc.value)
    assert state.assessment_count == 0


def test_half_l1_cost_and_continuous_state_are_recorded(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
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

    runner._execute_open(420, state, total_cost_bps=10.0)

    assert state.turnover == pytest.approx(0.50)
    assert state.costs_paid == pytest.approx(50.0)
    assert state.pending_target is None
    assert state.quantities["BOXX"] > 0.0
    assert state.lots["BOXX"]


def test_breakers_are_persistent_and_fail_closed(monkeypatch) -> None:
    input_payload, config = _payloads()
    runner = SoxlPromotionRunner(input_payload, config, assessment_clock=lambda: NOW)
    state = runner._initial_state()
    state.normalized = True
    state.high_water_equity = 100_000.0
    state.last_equity = 89_000.0
    gate = Mock(side_effect=lambda decision, *args, **kwargs: _approve(decision))
    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner.assess_with_evidence",
        gate,
    )

    assert runner._evaluate_close(500, state) == {}
    assert state.account_parked is True
    assert runner._evaluate_close(501, state) == {}
    assert gate.call_count == 2


def test_producer_uses_qpk_orchestrator_and_writes_truthful_25bp_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    input_payload, config = _payloads()
    replay = Mock(side_effect=lambda start, end, cost: _synthetic_window(start, end, cost))
    monkeypatch.setattr(SoxlPromotionRunner, "_replay_window", replay)

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
    assert evidence["backtest"]["orchestrator"] == "BacktestOrchestrator"
    assert [item["total_cost_bps"] for item in evidence["cost_stress"]["scenarios"]] == [5.0, 10.0, 15.0]
    assert stress_25["total_cost_bps"] == 25.0
    assert stress_25["locked_oos_result"]["total_return"] == pytest.approx(0.20)
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
    assert replay.call_count == 16


def _synthetic_window(start: date, end: date, total_cost_bps: float):
    from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult
    from us_equity_snapshot_pipelines.lifecycle.soxl_promotion_runner import WindowEvidence

    total_return = 0.20 if total_cost_bps == 25.0 else 0.30 - total_cost_bps / 1_000.0
    result = BacktestResult(
        strategy_profile="soxl_soxx_trend_income",
        domain="us_equity",
        param_set_id="soxl_p3",
        params={},
        sharpe_ratio=1.2,
        calmar_ratio=1.5,
        sortino_ratio=1.8,
        max_drawdown=-0.10,
        cagr=0.15,
        volatility=0.20,
        win_rate=0.55,
        total_return=total_return,
        start_date=start,
        end_date=end,
        observation_count=270,
        benchmark_symbol="SOXX",
        benchmark_cagr=0.10,
        benchmark_max_drawdown=-0.12,
        excess_cagr=0.05,
        oos_sharpe=1.2,
        oos_calmar=1.5,
        oos_max_drawdown=-0.10,
        walk_forward_stability=1.0,
        run_duration_seconds=0.01,
    )
    return WindowEvidence(
        result=result,
        recovery_sessions=20,
        recovery_censored=False,
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
