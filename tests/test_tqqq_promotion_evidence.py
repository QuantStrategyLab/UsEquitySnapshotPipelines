from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.risk.engine import build_risk_engine
from quant_platform_kit.strategy_contracts import StrategyDecision
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    read_evidence_package_v2_json,
    validate_evidence_package_v2,
)
from us_equity_strategies import entrypoints

import us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration as acquisition_module
import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence_module
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    FROZEN_XNYS_SESSIONS,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    build_tqqq_core_only_input_manifest,
    build_tqqq_core_only_p1_binding,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
    _Bar,
    _digest,
    _ImmutableReplayProducer,
    _ReplayState,
    _state_projection,
    run_tqqq_promotion_evidence,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    TqqqSwitchingTrace,
)

RUNNER_REVISION = "1" * 40
MANDATE_RECEIPT_SHA256 = "6" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sessions() -> list[date]:
    first_test = date(2019, 7, 30)
    warmup: list[date] = []
    cursor = first_test - timedelta(days=1)
    while len(warmup) < 260:
        if cursor.weekday() < 5:
            warmup.append(cursor)
        cursor -= timedelta(days=1)
    boundaries = {
        date(2018, 1, 2),
        date(2019, 7, 30),
        date(2019, 7, 31),
        date(2020, 1, 31),
        date(2020, 3, 2),
        date(2020, 10, 13),
        date(2021, 10, 1),
        date(2021, 10, 4),
        date(2022, 3, 31),
        date(2022, 5, 2),
        date(2022, 12, 27),
        date(2022, 12, 28),
        date(2023, 1, 31),
        date(2023, 5, 22),
        date(2023, 5, 31),
        date(2023, 6, 30),
        date(2023, 7, 3),
        date(2023, 11, 30),
        date(2023, 12, 1),
        date(2023, 12, 4),
        date(2024, 5, 31),
        date(2024, 7, 1),
        date(2024, 7, 2),
        date(2025, 7, 1),
        date(2025, 7, 2),
        date(2026, 7, 31),
    }
    development_sessions = {
        date.fromisoformat(value)
        for value in FROZEN_XNYS_SESSIONS
        if "2022-12-27" <= value <= "2025-06-30"
    }
    locked_sessions = {
        date.fromisoformat(value)
        for value in FROZEN_XNYS_SESSIONS
        if "2025-07-02" <= value <= "2026-07-31"
    }
    return sorted(
        set(warmup) | boundaries | development_sessions | locked_sessions
    )


def _bar(symbol: str, session: date, index: int) -> dict[str, object]:
    base = 100.0 + index * (0.08 if symbol == "QQQ" else 0.12)
    if symbol == "BOXX":
        base = 100.0 + index * 0.01
    return {
        "date": session.isoformat(),
        "open": base,
        "high": base * 1.01,
        "low": base * 0.99,
        "close": base * 1.002,
        "volume": 1_000_000.0 + index,
    }


def _input_payload() -> dict[str, object]:
    sessions = _sessions()
    bars = {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            "BOXX": [
                _bar("BOXX", session, index)
                for index, session in enumerate(sessions)
                if session >= date(2022, 12, 28)
            ],
            "QQQ": [_bar("QQQ", session, index) for index, session in enumerate(sessions)],
            "QQQM": [
                _bar("QQQM", session, index)
                for index, session in enumerate(sessions)
                if session >= date(2020, 10, 13)
            ],
            "TQQQ": [_bar("TQQQ", session, index) for index, session in enumerate(sessions)],
        },
    }
    binding = build_tqqq_core_only_p1_binding()
    manifest = build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-14T00:00:00Z",
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": RUNNER_REVISION,
            "tool": "tqqq_core_only_p1_publisher",
            "tool_version": "v1",
        },
        member_bytes=_canonical(bars),
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(bars["symbols"][symbol])).hexdigest()
            for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
        },
    )
    return {"binding": binding, "input_manifest": manifest, "bars": bars}


def _refresh_manifest(payload: dict[str, object]) -> None:
    bars = payload["bars"]
    binding = payload["binding"]
    assert isinstance(bars, dict) and isinstance(binding, dict)
    payload["input_manifest"] = build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-14T00:00:00Z",
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": RUNNER_REVISION,
            "tool": "tqqq_core_only_p1_publisher",
            "tool_version": "v1",
        },
        member_bytes=_canonical(bars),
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(bars["symbols"][symbol])).hexdigest()
            for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
        },
    )


def _config() -> dict[str, object]:
    return {
        "schema_version": "tqqq_etf_only_replay_config.v1",
        "strategy_profile": "tqqq_core_parity_v1",
        "signal_model": (
            "ues_tqqq_growth_income_core_parity_5loss_20xnys_defensive_cooldown"
        ),
        "signal_window_sessions": 257,
        "tqqq_nominal_cap": 0.15,
        "qqqm_nominal_cap": 0.50,
        "boxx_nominal_cap": 0.50,
        "risk_mandate_id": "tqqq_core_parity_v1",
        "risk_standard_id": "qpk.strategy_promotion_risk_standard.zh-CN.v2",
        "risk_standard_sha256": "2" * 64,
        "authority_receipt_sha256": "3" * 64,
    }


def test_acquisition_freezes_distinct_signal_model_before_authorization() -> None:
    authority = SimpleNamespace(
        risk_standard_id="qpk.strategy_promotion_risk_standard.zh-CN.v2",
        risk_standard_sha256="2" * 64,
        authority_receipt_sha256="3" * 64,
        platform_execution_revision="4" * 40,
    )

    config = acquisition_module._config(authority, session_class="paper")

    assert config["signal_model"] == (
        "ues_tqqq_growth_income_core_parity_5loss_20xnys_defensive_cooldown"
    )
    assert evidence_module._validate_config(_config()) == _config()


def test_evidence_rejects_legacy_signal_model_instead_of_rewriting_identity() -> None:
    legacy = _config()
    legacy["signal_model"] = "ues_tqqq_growth_income_core_parity"

    with pytest.raises(TqqqPromotionEvidenceError, match="invalid frozen config"):
        evidence_module._validate_config(legacy)


def test_acquisition_readback_requires_learning_only_lifecycle_claims() -> None:
    claims = {
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }

    assert acquisition_module._is_learning_only_evidence_readback(
        {"lifecycle_claims": claims}, claims
    )
    assert not acquisition_module._is_learning_only_evidence_readback(
        {"lifecycle_claims": {**claims, "learning_only": False}}, claims
    )
    assert not acquisition_module._is_learning_only_evidence_readback(
        {"lifecycle_claims": claims}, {**claims, "promotion_eligible": True}
    )


def test_consumer_contract_requires_raw_ues_core_builder_and_risk_engine() -> None:
    assert hasattr(entrypoints, "_build_tqqq_growth_income_decision")
    assert (
        evidence_module._build_tqqq_growth_income_decision
        is entrypoints._build_tqqq_growth_income_decision
    )
    assert not hasattr(evidence_module, "evaluate_tqqq_research_contract")
    assert not hasattr(evidence_module, "risk_budgeted_target_weight")


def test_real_consumer_writes_valid_redacted_evidence_v2(tmp_path: Path) -> None:
    payload = _input_payload()
    assert payload["bars"]["symbols"]["BOXX"][0]["date"] == "2022-12-28"
    assert "2022-12-27" in {bar["date"] for bar in payload["bars"]["symbols"]["QQQ"]}

    with (
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        patch.object(
            entrypoints,
            "compute_tqqq_growth_income_decision",
            side_effect=AssertionError("legacy risk-gated entrypoint must not be called"),
        ) as legacy_entrypoint,
    ):
        result = run_tqqq_promotion_evidence(
            input_payload=_input_payload(),
            config_payload=_config(),
            output_dir=tmp_path,
            generated_at="2026-08-10T00:00:00Z",
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )

    evidence_path = tmp_path / "strategy-evidence-package.v2.json"
    evidence = read_evidence_package_v2_json(evidence_path)
    assert validate_evidence_package_v2(evidence, base_dir=tmp_path) == ()
    assert evidence["backtest"]["orchestrator"] == "BacktestOrchestrator"
    promotion_run = evidence["backtest"]["promotion_run"]
    assert {
        result["params"]["mandate_receipt_sha256"]
        for result in [*promotion_run["fold_results"], promotion_run["locked_oos_result"]]
    } == {MANDATE_RECEIPT_SHA256}
    assert evidence["backtest"]["promotion_run"]["source_revision"] == (
        "8b6b418bac74318f8054c5951521c9b62391de3e"
    )
    assert evidence["cost_stress"]["scenarios"] == [
        {"multiplier": 1, "total_cost_bps": 5.0},
        {"multiplier": 2, "total_cost_bps": 10.0},
        {"multiplier": 3, "total_cost_bps": 25.0},
    ]
    assert evidence["lifecycle_claims"] == {
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }
    assert result["evidence_sha256"] == hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    terminal = json.loads((tmp_path / "promotion-research-result.v1.json").read_bytes())
    assert terminal["verdict"] == result["verdict"]
    risk = json.loads((tmp_path / "artifacts" / "risk.json").read_bytes())
    assert risk["strategy_losing_exit_cooldown_threshold"] == 5
    assert risk["protective_cooldown_execution_sessions"] == 20
    assert "strategy_losing_exit_breaker" not in risk
    backtest = json.loads((tmp_path / "artifacts" / "backtest.json").read_bytes())
    assert backtest["development_robustness_plan"]["aggregate_plan_sha256"] == (
        "28c4b4fbf587891112f1994b44a6ff3d111742cdb854adfcd172cfe664b1ae52"
    )
    assert backtest["frozen_trial_ledger"]["complete_before_replay"] is True
    assert backtest["systematic_reporting"]["aggregate_plan_sha256"] == (
        backtest["development_robustness_plan"]["aggregate_plan_sha256"]
    )
    assert backtest["systematic_reporting"]["overfitting_diagnostics"]["pbo"]["status"] == (
        "NOT_APPLICABLE"
    )
    assert set(backtest["systematic_reporting"]["regime_coverage"]) == {
        "bear",
        "bull",
        "sideways",
    }
    systematic_by_cost = {
        str(scenario["total_cost_bps"]): scenario
        for scenario in backtest["systematic_reporting"]["cost_scenarios"]
    }
    for scenario in backtest["scenarios"].values():
        cost = str(int(scenario["promotion_run"]["cost_model"]["slippage_bps"]))
        systematic_decisions = sum(
            window["decision_count"]
            for horizon in systematic_by_cost.get(cost, {"horizons": []})["horizons"]
            for window in horizon["windows"]
        )
        assert (
            sum(window["decision_count"] for window in scenario["windows"])
            + systematic_decisions
            == risk["scenario_counts"][cost]["decisions"]
        )
        for window in scenario["windows"]:
            assert window["decision_count"] == window["risk_assessment_count"] > 0
            assert window["relative_metrics"]["boxx_total_return"] is not None
            assert set(window["episode_summary"]) == {
                "episode_session_count",
                "tqqq_exposure_session_count",
                "qqqm_exposure_session_count",
                "boxx_exposure_session_count",
                "cash_only_session_count",
                "parked_session_count",
                "tqqq_entry_count",
                "tqqq_stop_armed_count",
                "tqqq_stop_crossing_count",
                "tqqq_stop_fill_count",
                "tqqq_unprotected_holding_session_count",
                "breaker_reason",
                "first_park_session",
            }
            assert window["switching_traces"]
            assert set(window["switching_traces"][0]) == {
                "signal_session",
                "execution_session",
                "signal_state",
                "signal_regime",
                "intended_allocation",
                "risk_disposition",
                "risk_reason_codes",
                "replay_target_allocation",
                "executed_allocation",
            }
    legacy_entrypoint.assert_not_called()
    assert all(
        counts["assessments"] == counts["decisions"]
        for counts in risk["scenario_counts"].values()
    )
    assert (
        canonical_research_input_manifest_bytes(_input_payload()["input_manifest"])
        == (tmp_path / "artifacts" / "data-manifest.json").read_bytes()
    )
    packaged = b"".join(path.read_bytes() for path in tmp_path.rglob("*") if path.is_file())
    assert b'"open"' not in packaged
    assert b'"volume"' not in packaged


def test_input_tamper_and_nonempty_output_fail_closed_before_replay(
    tmp_path: Path,
) -> None:
    tampered = copy.deepcopy(_input_payload())
    tampered["bars"]["symbols"]["QQQ"][0]["close"] = 999.0

    with pytest.raises(TqqqPromotionEvidenceError, match="input identity"):
        run_tqqq_promotion_evidence(
            input_payload=tampered,
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )
    assert not any(tmp_path.iterdir())

    (tmp_path / "existing").write_text("x", encoding="utf-8")
    with pytest.raises(TqqqPromotionEvidenceError, match="empty"):
        run_tqqq_promotion_evidence(
            input_payload=_input_payload(),
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )


def test_input_with_missing_locked_oos_session_fails_closed(tmp_path: Path) -> None:
    payload = _input_payload()
    missing_session = "2025-07-03"
    for symbol in ("BOXX", "QQQ", "QQQM", "TQQQ"):
        payload["bars"]["symbols"][symbol] = [
            row
            for row in payload["bars"]["symbols"][symbol]
            if row["date"] != missing_session
        ]
    _refresh_manifest(payload)

    with pytest.raises(TqqqPromotionEvidenceError, match="locked OOS calendar identity"):
        run_tqqq_promotion_evidence(
            input_payload=payload,
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )


def test_provider_observed_contract_rejects_boxx_backfill(tmp_path: Path) -> None:
    payload = _input_payload()
    payload["bars"]["symbols"]["BOXX"].insert(0, _bar("BOXX", date(2022, 12, 23), 0))
    _refresh_manifest(payload)

    with pytest.raises(TqqqPromotionEvidenceError, match="BOXX eligibility"):
        run_tqqq_promotion_evidence(
            input_payload=payload,
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )


def _episode_producer(sessions: tuple[date, ...]) -> _ImmutableReplayProducer:
    producer = object.__new__(_ImmutableReplayProducer)
    producer.candidate = CandidateRiskIdentity(
        authority_receipt_sha256="1" * 64,
        strategy_profile="tqqq_core_parity_v1",
        account_mode="single_strategy_account_v1",
        strategy_revision="2" * 40,
        runner_revision="3" * 40,
        config_sha256="4" * 64,
        input_manifest_sha256="5" * 64,
    )
    producer.identity = SimpleNamespace(initial_state_sha256="6" * 64)
    producer.qqq = tuple(
        _Bar(session, 100.0, 101.0, 99.0, 100.0, 1_000_000.0)
        for session in sessions
    )
    producer.prices = {
        symbol: {
            session: _Bar(session, 100.0, 101.0, 99.0, 100.0, 1_000_000.0)
            for session in sessions
        }
        for symbol in evidence_module._ORDERABLE_ASSETS
    }
    producer._index = {session: index for index, session in enumerate(sessions)}
    producer._scenario = None
    producer._state = _ReplayState()
    producer._state_sha256 = producer.identity.initial_state_sha256
    producer._scenario_counts = {}
    producer._switching_traces = []
    return producer


def _approved_result(
    weights: dict[str, float], *, signal_state: str = "entry"
) -> SimpleNamespace:
    return SimpleNamespace(
        assessment=SimpleNamespace(
            outcome="APPROVE",
            reason_codes=(),
            execution_authorized=False,
        ),
        decision=SimpleNamespace(
            positions=tuple(
                SimpleNamespace(symbol=symbol, target_weight=weight)
                for symbol, weight in weights.items()
                if weight > 0.0
            ),
            diagnostics={
                "notification_context": {"signal": {"state": signal_state}}
            },
        ),
    )


def test_episode_executes_and_counts_only_in_window_sessions() -> None:
    first = date(2022, 4, 14)
    sessions = tuple(first + timedelta(days=index) for index in range(262))
    producer = _episode_producer(sessions)
    start, end = sessions[258], sessions[260]
    traded: list[date] = []
    assessed_for_execution: list[date] = []

    def assess(_signal_index: int, execution_session: date, _equity: float):
        assessed_for_execution.append(execution_session)
        producer._state.decision_count += 1
        producer._state.assessment_count += 1
        producer._scenario_counts[5]["decisions"] += 1
        producer._scenario_counts[5]["assessments"] += 1
        return {symbol: 0.0 for symbol in evidence_module._ORDERABLE_ASSETS}

    with (
        patch.object(producer, "_assessment", side_effect=assess),
        patch.object(producer, "_trade_to_target", side_effect=lambda session, _cost: traded.append(session)),
        patch.object(producer, "_apply_stop"),
        patch.object(producer, "_equity", return_value=100_000.0),
        patch.object(
            producer,
            "_current_weights",
            return_value={symbol: 0.0 for symbol in evidence_module._ORDERABLE_ASSETS},
        ),
    ):
        replay = producer(start, end, 5, producer.identity.initial_state_sha256)

    assert traded == list(sessions[258:261])
    assert assessed_for_execution == list(sessions[258:261])
    assert replay.trade_count == 0
    assert replay.decision_count == replay.risk_assessment_count == 3
    assert replay.episode_summary.episode_session_count == 3


def test_deterministic_switching_characterization_uses_member_and_account_risk_engines() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-520:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    engine = build_risk_engine()
    with patch.object(evidence_module, "build_risk_engine", return_value=engine), patch.object(engine, "assess", wraps=engine.assess) as assess:
        risk_on = producer._assessment(299, sessions[300], 100_000.0)
        producer._state.pending_weights = risk_on
        producer._trade_to_target(sessions[300], 5)
    assert assess.call_count == 2
    trace = producer.switching_traces[-1]
    assert trace.risk_disposition == "APPROVE"
    assert dict(trace.intended_allocation) != dict(trace.executed_allocation)

def test_account_overlay_tightening_does_not_mutate_core_or_member_targets() -> None:
    producer = _unit_producer(date(2025, 1, 2))
    raw = {"TQQQ": 0.80, "QQQM": 0.0, "BOXX": 0.0}
    strategy_risk = {"TQQQ": 0.60, "QQQM": 0.0, "BOXX": 0.0}

    baseline = producer._account_overlay_target(
        strategy_risk,
        loss_budget=0.01,
        nominal_caps={"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50},
    )
    tightened = producer._account_overlay_target(
        strategy_risk,
        loss_budget=0.0025,
        nominal_caps={"TQQQ": 0.05, "QQQM": 0.50, "BOXX": 0.50},
    )

    assert raw == {"TQQQ": 0.80, "QQQM": 0.0, "BOXX": 0.0}
    assert strategy_risk == {"TQQQ": 0.60, "QQQM": 0.0, "BOXX": 0.0}
    assert baseline["TQQQ"] == pytest.approx(0.15)
    assert tightened["TQQQ"] == pytest.approx(0.05)


def test_risk_on_trace_preserves_raw_core_target_before_account_overlay() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    raw_decision = StrategyDecision(
        positions=(SimpleNamespace(symbol="TQQQ", target_value=80_000.0),),
        diagnostics={"notification_context": {"signal": {"state": "entry"}}},
    )

    with (
        patch.object(
            evidence_module,
            "_build_tqqq_growth_income_decision",
            return_value=raw_decision,
        ),
    ):
        strategy_risk = producer._assessment(257, sessions[258], 100_000.0)

    trace = producer.switching_traces[-1]
    assert dict(trace.intended_allocation)["TQQQ"] == pytest.approx(0.80)
    assert strategy_risk["TQQQ"] == pytest.approx(0.80)
    assert dict(trace.replay_target_allocation)["TQQQ"] == pytest.approx(0.80)
    assert dict(trace.executed_allocation)["TQQQ"] == pytest.approx(0.15)


def test_member_risk_reject_keeps_raw_signal_but_zeros_replay_and_overlay() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    raw_decision = StrategyDecision(
        positions=(SimpleNamespace(symbol="TQQQ", target_value=80_000.0),),
        diagnostics={"notification_context": {"signal": {"state": "entry"}}},
    )
    rejected_engine = SimpleNamespace(
        assess=lambda *_args, **_kwargs: SimpleNamespace(
            action="reject", reason="MEMBER_BLOCKED"
        )
    )

    with (
        patch.object(
            evidence_module,
            "_build_tqqq_growth_income_decision",
            return_value=raw_decision,
        ),
        patch.object(evidence_module, "build_risk_engine", return_value=rejected_engine),
    ):
        targets = producer._assessment(257, sessions[258], 100_000.0)

    trace = producer.switching_traces[-1]
    assert dict(trace.intended_allocation)["TQQQ"] == pytest.approx(0.80)
    assert targets == {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.0}
    assert trace.risk_disposition == "REJECT"
    assert trace.risk_reason_codes == ("MEMBER_BLOCKED",)
    assert dict(trace.replay_target_allocation)["cash"] == pytest.approx(1.0)
    assert dict(trace.executed_allocation)["cash"] == pytest.approx(1.0)


def test_cooldown_applies_to_strategy_risk_without_rewriting_raw_signal() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-520:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    producer._state.cooldown_remaining_execution_sessions = 20
    raw = StrategyDecision(positions=(SimpleNamespace(symbol="TQQQ", target_value=80_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    defensive = StrategyDecision(positions=(SimpleNamespace(symbol="BOXX", target_value=20_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    with patch.object(evidence_module, "_build_tqqq_growth_income_decision", side_effect=(raw, defensive)):
        targets = producer._assessment(299, sessions[300], 100_000.0)
    trace = producer.switching_traces[-1]
    assert dict(trace.intended_allocation)["TQQQ"] == pytest.approx(0.80)
    assert targets["BOXX"] == pytest.approx(0.20)
    assert trace.signal_state == "protective_cooldown"

def test_cooldown_is_exactly_20_execution_sessions_before_fresh_base_signal() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    producer._state.tqqq_entry_price = 100.0
    producer._state.consecutive_losing_exits = 4
    producer._record_completed_exit(95.0, sessions[257])
    raw = StrategyDecision(positions=(SimpleNamespace(symbol="QQQM", target_value=10_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    defensive = StrategyDecision(positions=(SimpleNamespace(symbol="BOXX", target_value=20_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    with patch.object(evidence_module, "_build_tqqq_growth_income_decision", side_effect=[x for _ in range(20) for x in (raw, defensive)] + [raw]):
        for offset in range(20):
            assert producer._assessment(257 + offset, sessions[258 + offset], 100_000.0)["BOXX"] == pytest.approx(0.20)
            producer._complete_cooldown_execution_session(sessions[258 + offset])
        reentry = producer._assessment(277, sessions[278], 100_000.0)
    assert producer._state.cooldown_remaining_execution_sessions == 0
    assert reentry["QQQM"] == pytest.approx(0.10)

def test_switching_characterization_invalid_input_fails_closed_without_decision() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    del producer.prices["QQQM"][sessions[-2]]
    with pytest.raises(TqqqPromotionEvidenceError, match="eligible asset data unavailable"):
        producer(sessions[-2], sessions[-1], 5, producer.identity.initial_state_sha256)
    assert producer._state.assessment_count == 0

def test_pre_common_eligibility_episode_fails_closed() -> None:
    first = date(2022, 4, 13)
    sessions = tuple(first + timedelta(days=index) for index in range(260))
    producer = _episode_producer(sessions)
    start = date(2022, 12, 27)
    assert sessions[258] == start
    producer.prices["BOXX"].pop(start)

    with pytest.raises(TqqqPromotionEvidenceError, match="exact common eligibility"):
        producer(start, sessions[259], 5, producer.identity.initial_state_sha256)


def test_missing_orderable_asset_inside_eligible_episode_fails_closed() -> None:
    first = date(2022, 4, 14)
    sessions = tuple(first + timedelta(days=index) for index in range(261))
    producer = _episode_producer(sessions)
    producer.prices["BOXX"].pop(sessions[260])

    with pytest.raises(TqqqPromotionEvidenceError, match="eligible asset data unavailable"):
        producer(sessions[258], sessions[260], 5, producer.identity.initial_state_sha256)


def test_episode_reports_boxx_cash_and_park_separately() -> None:
    first = date(2023, 1, 2)
    sessions = tuple(first + timedelta(days=index) for index in range(262))
    producer = _episode_producer(sessions)
    start, cash_session, park_session, after_park_session = sessions[258:262]

    def trade(session: date, _cost: int) -> None:
        producer._state.quantities["BOXX"] = 100.0 if session == start else 0.0

    def apply_stop(session: date, _cost: int) -> None:
        if session == park_session:
            producer._park("ACCOUNT_DRAWDOWN", session)
        elif session == after_park_session:
            producer._park("RISK_ENGINE_NON_APPROVE", session)

    def assess(_signal_index: int, _execution_session: date, _equity: float):
        producer._state.decision_count += 1
        producer._state.assessment_count += 1
        producer._scenario_counts[5]["decisions"] += 1
        producer._scenario_counts[5]["assessments"] += 1
        return {symbol: 0.0 for symbol in evidence_module._ORDERABLE_ASSETS}

    with (
        patch.object(producer, "_assessment", side_effect=assess),
        patch.object(producer, "_trade_to_target", side_effect=trade),
        patch.object(producer, "_apply_stop", side_effect=apply_stop),
        patch.object(producer, "_equity", return_value=100_000.0),
        patch.object(
            producer,
            "_current_weights",
            return_value={symbol: 0.0 for symbol in evidence_module._ORDERABLE_ASSETS},
        ),
    ):
        replay = producer(start, after_park_session, 5, producer.identity.initial_state_sha256)

    summary = replay.episode_summary
    assert summary.episode_session_count == 4
    assert summary.tqqq_exposure_session_count == 0
    assert summary.qqqm_exposure_session_count == 0
    assert summary.boxx_exposure_session_count == 1
    assert summary.cash_only_session_count == 1
    assert summary.parked_session_count == 2
    assert summary.breaker_reason == "ACCOUNT_DRAWDOWN"
    assert summary.first_park_session == park_session
    assert replay.decision_count == replay.risk_assessment_count == 4
    assert cash_session < park_session


def test_member_risk_non_approve_zeros_only_the_rejected_strategy_target() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    raw = StrategyDecision(positions=(SimpleNamespace(symbol="TQQQ", target_value=80_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    engine = SimpleNamespace(assess=lambda *_args, **_kwargs: SimpleNamespace(action="reject", reason="MATERIAL_RISK_REJECTION"))
    with patch.object(evidence_module, "_build_tqqq_growth_income_decision", return_value=raw), patch.object(evidence_module, "build_risk_engine", return_value=engine):
        targets = producer._assessment(257, sessions[258], 100_000.0)
    assert not any(targets.values())
    assert producer.switching_traces[-1].risk_disposition == "REJECT"

def test_episode_breaker_reason_is_sticky() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)

    producer._park("ACCOUNT_DRAWDOWN", session)
    producer._park("RISK_ENGINE_NON_APPROVE", session + timedelta(days=1))

    assert producer._state.parked is True
    assert producer._state.breaker_reason == "ACCOUNT_DRAWDOWN"
    assert producer._state.first_park_session == session


def test_account_park_does_not_mutate_strategy_replay_target() -> None:
    session = date(2025, 1, 3)
    producer = _unit_producer(session)
    allocation = (("BOXX", 0.0), ("QQQM", 0.0), ("TQQQ", 0.10), ("cash", 0.90))
    producer._switching_traces = [TqqqSwitchingTrace(session - timedelta(days=1), session, "entry", "RISK_ON", allocation, "APPROVE", (), allocation, allocation)]
    producer._state.high_water_equity = 100_000.0
    producer._apply_drawdown_breaker(session, 89_000.0)
    trace = producer.switching_traces[-1]
    assert trace.risk_disposition == "APPROVE"
    assert trace.replay_target_allocation == allocation
    assert dict(trace.executed_allocation)["cash"] == pytest.approx(1.0)

def test_fifth_losing_target_exit_enters_cooldown_without_liquidating_other_assets() -> None:
    previous_session = date(2025, 1, 2)
    session = date(2025, 1, 3)
    producer = _episode_producer((previous_session, session))
    producer._reset(5, producer.identity.initial_state_sha256)
    risk_on = (("BOXX", 0.50), ("QQQM", 0.10), ("TQQQ", 0.0), ("cash", 0.40))
    producer._switching_traces.append(
        TqqqSwitchingTrace(
            signal_session=previous_session,
            execution_session=session,
            signal_state="macro_delever",
            signal_regime="RISK_ON",
            intended_allocation=risk_on,
            risk_disposition="APPROVE",
            risk_reason_codes=(),
            replay_target_allocation=risk_on,
            executed_allocation=(),
        )
    )
    producer._state = _ReplayState(
        cash=80_000.0,
        quantities={"TQQQ": 100.0, "QQQM": 100.0, "BOXX": 0.0},
        tqqq_entry_price=110.0,
        tqqq_stop_price=104.50,
        pending_weights={"TQQQ": 0.0, "QQQM": 0.10, "BOXX": 0.50},
        consecutive_losing_exits=4,
        last_session=previous_session,
    )

    producer._trade_to_target(session, 5)

    assert producer._state.quantities == pytest.approx(
        {"TQQQ": 0.0, "QQQM": 100.0, "BOXX": 500.0 / 1.0005}
    )
    assert producer._state.consecutive_losing_exits == 0
    assert producer._state.cooldown_remaining_execution_sessions == 20
    assert producer._state.parked is False
    trace = producer.switching_traces[-1]
    assert trace.signal_state == "macro_delever"
    assert trace.risk_disposition == "APPROVE"
    assert trace.risk_reason_codes == ()
    assert trace.intended_allocation == trace.replay_target_allocation == risk_on


def test_final_session_drawdown_breaker_is_account_overlay_only() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)
    allocation = (("BOXX", 0.0), ("QQQM", 0.0), ("TQQQ", 0.10), ("cash", 0.90))
    producer._switching_traces = [TqqqSwitchingTrace(session - timedelta(days=1), session, "entry", "RISK_ON", allocation, "APPROVE", (), allocation, allocation)]
    producer._state.high_water_equity = 100_000.0
    producer._apply_drawdown_breaker(session, 89_000.0)
    assert producer._state.parked is True
    assert producer.switching_traces[-1].risk_disposition == "APPROVE"
    assert dict(producer.switching_traces[-1].executed_allocation)["cash"] == pytest.approx(1.0)

def _unit_producer(session: date, *, tqqq_open: float = 100.0, tqqq_low: float = 99.0):
    producer = object.__new__(_ImmutableReplayProducer)
    producer.candidate = CandidateRiskIdentity(
        authority_receipt_sha256="1" * 64,
        strategy_profile="tqqq_core_parity_v1",
        account_mode="single_strategy_account_v1",
        strategy_revision="2" * 40,
        runner_revision="3" * 40,
        config_sha256="4" * 64,
        input_manifest_sha256="5" * 64,
    )
    producer.prices = {
        "TQQQ": {
            session: _Bar(
                session,
                tqqq_open,
                max(tqqq_open, 101.0),
                tqqq_low,
                tqqq_open,
                1_000_000.0,
            )
        },
        "QQQM": {session: _Bar(session, 100.0, 101.0, 99.0, 100.0, 1_000_000.0)},
        "BOXX": {session: _Bar(session, 100.0, 101.0, 99.0, 100.0, 1_000_000.0)},
    }
    producer._state = _ReplayState()
    return producer


def test_multi_asset_target_rebalances_without_completed_exit_round_trip() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)
    producer._state = _ReplayState(
        cash=65_000.0,
        quantities={"TQQQ": 100.0, "QQQM": 100.0, "BOXX": 150.0},
        tqqq_entry_price=100.0,
        tqqq_stop_price=95.0,
        pending_weights={"TQQQ": 0.05, "QQQM": 0.20, "BOXX": 0.10},
        consecutive_losing_exits=2,
    )

    producer._trade_to_target(session, 0)

    assert producer._state.quantities == pytest.approx(
        {"TQQQ": 50.0, "QQQM": 200.0, "BOXX": 100.0}
    )
    assert producer._state.cash == pytest.approx(65_000.0)
    assert producer._state.consecutive_losing_exits == 2
    assert producer._state.trade_count == 3


def test_fifth_losing_hard_stop_starts_cooldown_and_keeps_other_assets() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session, tqqq_open=95.0, tqqq_low=94.0)
    target = (("BOXX", 0.0), ("QQQM", 0.10), ("TQQQ", 0.10), ("cash", 0.80))
    producer._switching_traces = [
        TqqqSwitchingTrace(
            signal_session=session - timedelta(days=1),
            execution_session=session,
            signal_state="hold",
            signal_regime="RISK_ON",
            intended_allocation=target,
            risk_disposition="APPROVE",
            risk_reason_codes=(),
            replay_target_allocation=target,
            executed_allocation=target,
        )
    ]
    producer._state = _ReplayState(
        cash=80_000.0,
        quantities={"TQQQ": 100.0, "QQQM": 100.0, "BOXX": 0.0},
        tqqq_entry_price=100.0,
        tqqq_stop_price=95.0,
        consecutive_losing_exits=4,
    )

    producer._apply_stop(session, 0)

    assert producer._state.quantities == {"TQQQ": 0.0, "QQQM": 100.0, "BOXX": 0.0}
    assert producer._state.consecutive_losing_exits == 0
    assert producer._state.cooldown_remaining_execution_sessions == 20
    assert producer._state.parked is False
    assert producer._state.tqqq_entry_price is None
    assert producer._state.tqqq_stop_price is None
    assert producer.switching_traces[-1].risk_disposition == "APPROVE"
    assert producer.switching_traces[-1].risk_reason_codes == ()


def test_terminal_park_liquidation_cannot_start_protective_cooldown() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session, tqqq_open=95.0, tqqq_low=94.0)
    cash = (("BOXX", 0.0), ("QQQM", 0.0), ("TQQQ", 0.0), ("cash", 1.0))
    producer._switching_traces = [
        TqqqSwitchingTrace(
            signal_session=session - timedelta(days=1),
            execution_session=session,
            signal_state="risk_engine_non_approve",
            signal_regime="DEFENSIVE",
            intended_allocation=cash,
            risk_disposition="PARK",
            risk_reason_codes=("RISK_ENGINE_NON_APPROVE",),
            replay_target_allocation=cash,
            executed_allocation=(),
        )
    ]
    producer._state = _ReplayState(
        cash=90_500.0,
        quantities={"TQQQ": 100.0, "QQQM": 0.0, "BOXX": 0.0},
        tqqq_entry_price=100.0,
        tqqq_stop_price=95.0,
        consecutive_losing_exits=4,
        parked=True,
        breaker_reason="RISK_ENGINE_NON_APPROVE",
        first_park_session=session,
    )

    producer._trade_to_target(session, 0)

    assert producer._state.parked is True
    assert producer._state.cooldown_remaining_execution_sessions == 0
    assert producer._state.quantities == {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.0}


@pytest.mark.parametrize("fill", [100.0, 101.0])
def test_winning_or_breakeven_full_exit_resets_streak(fill: float) -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)
    producer._state.tqqq_entry_price = 100.0
    producer._state.consecutive_losing_exits = 4

    producer._record_completed_exit(fill, session)

    assert producer._state.consecutive_losing_exits == 0
    assert producer._state.cooldown_remaining_execution_sessions == 0
    assert producer._state.parked is False


def test_multi_asset_state_projection_digest_is_deterministic() -> None:
    state = _ReplayState(
        cash=65_000.0,
        quantities={"TQQQ": 50.0, "QQQM": 200.0, "BOXX": 100.0},
        pending_weights={"TQQQ": 0.05, "QQQM": 0.20, "BOXX": 0.10},
        consecutive_losing_exits=2,
    )

    assert _digest(_state_projection(state)) == _digest(_state_projection(copy.deepcopy(state)))


def test_member_risk_engine_rejection_does_not_park_the_strategy_episode() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    raw = StrategyDecision(positions=(SimpleNamespace(symbol="TQQQ", target_value=80_000.0),), diagnostics={"notification_context": {"signal": {"state": "entry"}}})
    engine = SimpleNamespace(assess=lambda *_args, **_kwargs: SimpleNamespace(action="reject", reason="RISK_ENGINE_NON_APPROVE"))
    with patch.object(evidence_module, "_build_tqqq_growth_income_decision", return_value=raw), patch.object(evidence_module, "build_risk_engine", return_value=engine):
        targets = producer._assessment(257, sessions[258], 100_000.0)
    assert not any(targets.values())
    assert producer._state.parked is False
    assert producer.switching_traces[-1].risk_disposition == "REJECT"

def _new_p1_input_payload() -> dict[str, object]:
    return _input_payload()


def _new_p3_config() -> dict[str, object]:
    return _config()


def test_new_p1_manifest_enters_p3_without_legacy_session_or_platform_fields() -> None:
    config = _new_p3_config()
    payload = _new_p1_input_payload()

    assert evidence_module._validate_config(config) == config
    provenance, _, manifest_sha256 = evidence_module._validate_input(payload, config)

    assert manifest_sha256 == research_input_manifest_sha256(payload["input_manifest"])
    assert provenance["source"] == "IBKR"
    assert "session_class" not in payload
    assert "platform_execution_revision" not in config


def test_new_p1_member_tamper_and_retention_binding_fail_closed() -> None:
    payload = _new_p1_input_payload()
    tampered = copy.deepcopy(payload)
    tampered["bars"]["symbols"]["QQQ"][0]["close"] = 999.0

    with pytest.raises(TqqqPromotionEvidenceError, match="input identity"):
        evidence_module._validate_input(tampered, _new_p3_config())

    invalid_retention = copy.deepcopy(payload)
    invalid_retention["binding"]["data_identity"]["retention"]["redistribution_allowed"] = True
    with pytest.raises(TqqqPromotionEvidenceError, match="binding"):
        evidence_module._validate_input(invalid_retention, _new_p3_config())
