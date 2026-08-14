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
        "schema_version": "tqqq_etf_only_private_bars.v1",
        "symbols": {
            "BOXX": [
                _bar("BOXX", session, index) for index, session in enumerate(sessions) if session >= date(2022, 12, 28)
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
    bars_bytes = _canonical(bars)
    observed_at = "2026-08-10T00:00:00Z"
    sources = []
    for symbol in ("BOXX", "QQQ", "QQQM", "TQQQ"):
        symbol_bytes = _canonical(bars["symbols"][symbol])
        sources.append(
            {
                "source_id": f"ibkr:{symbol}",
                "revision": "server-version-176",
                "observed_at": observed_at,
                "content_sha256": hashlib.sha256(symbol_bytes).hexdigest(),
            }
        )
    manifest = {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": (
            "tqqq-ibkr-paper-single-acquisition-"
            f"{hashlib.sha256(bars_bytes).hexdigest()[:24]}"
        ),
        "research_input_contract_id": "tqqq_etf_only_ibkr_adjusted_last.v1",
        "domain": "us_equity",
        "profile": "tqqq_core_parity_v1",
        "artifact_type": "immutable_adjusted_ohlcv_etf_only",
        "observed_at": observed_at,
        "effective_at": observed_at,
        "as_of": observed_at,
        "producer": {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": RUNNER_REVISION,
            "tool": "tqqq_ibkr_paper_single_acquisition",
            "tool_version": "v1",
        },
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_date": "2026-07-31",
            "source": "exchange_calendars",
            "source_revision": (
                "exchange_calendars:4.13.2:XNYS:"
                "18b12a992cfb245e6aec7145797e5f0b7b2b03eed880961896ba370d8a7d5380"
            ),
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "IBKR_ADJUSTED_LAST",
            "source_revision": "server-version-176",
        },
        "sources": sources,
        "members": [
            {
                "path": "bars.json",
                "media_type": "application/json",
                "size_bytes": len(bars_bytes),
                "sha256": hashlib.sha256(bars_bytes).hexdigest(),
            }
        ],
    }
    return {
        "provenance": {
            "evidence_class": "provider_observed",
            "real_producer": True,
            "provider": "IBKR Paper Gateway TWS API",
            "provider_revision": "server-version-176",
            "session_class": "paper",
            "license": "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04",
            "usage_scope": "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION",
        },
        "input_manifest": manifest,
        "bars": bars,
    }


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
        "platform_execution_revision": "4" * 40,
        "input_license": "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04",
        "input_usage_scope": "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION",
        "session_class": "paper",
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
    assert evidence_module._validate_config(config) == config


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


def test_consumer_contract_requires_direct_ues_core_parity_seam() -> None:
    assert hasattr(entrypoints, "evaluate_tqqq_growth_income_promotion_research")
    assert (
        evidence_module.evaluate_tqqq_growth_income_promotion_research
        is entrypoints.evaluate_tqqq_growth_income_promotion_research
    )
    assert not hasattr(evidence_module, "evaluate_tqqq_research_contract")
    assert not hasattr(evidence_module, "risk_budgeted_target_weight")


def test_real_consumer_writes_valid_redacted_evidence_v2(tmp_path: Path) -> None:
    payload = _input_payload()
    assert payload["bars"]["symbols"]["BOXX"][0]["date"] == "2022-12-28"
    assert "2022-12-27" in {bar["date"] for bar in payload["bars"]["symbols"]["QQQ"]}

    contexts = []
    direct_calls = 0
    assessment_count = 0
    engine = build_risk_engine()
    original_assess = engine.assess

    def evaluate(ctx, **kwargs):
        nonlocal direct_calls
        direct_calls += 1
        if len(contexts) < 4:
            contexts.append(ctx)
        return entrypoints.evaluate_tqqq_growth_income_promotion_research(ctx, **kwargs)

    def counted_assess(*args, **kwargs):
        nonlocal assessment_count
        assessment_count += 1
        return original_assess(*args, **kwargs)

    engine.assess = counted_assess

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
            evidence_module,
            "evaluate_tqqq_growth_income_promotion_research",
            new=evaluate,
        ),
        patch("quant_platform_kit.risk.gate.build_risk_engine", return_value=engine),
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
        {"multiplier": 3, "total_cost_bps": 15.0},
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
    reported_assessment_count = sum(
        counts["assessments"] for counts in risk["scenario_counts"].values()
    )
    assert direct_calls == assessment_count == reported_assessment_count
    legacy_entrypoint.assert_not_called()
    assert all(
        counts["assessments"] == counts["decisions"]
        for counts in risk["scenario_counts"].values()
    )
    assert contexts
    assert all(ctx.as_of.tzinfo is not None and ctx.as_of.utcoffset() is not None for ctx in contexts)
    assert all(dict(ctx.runtime_config) == evidence_module._RUNTIME_OVERRIDES for ctx in contexts)
    assert all(len(ctx.market_data["benchmark_history"]) >= 257 for ctx in contexts)
    assert all(
        max(row["date"] for row in ctx.market_data["benchmark_history"])
        == ctx.market_data["signal_session"]
        == ctx.as_of.date().isoformat()
        and ctx.market_data["next_execution_session"] > ctx.market_data["signal_session"]
        for ctx in contexts
    )
    assert any(ctx.portfolio.positions for ctx in contexts[1:])
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
        source = next(
            item
            for item in payload["input_manifest"]["sources"]
            if item["source_id"] == f"ibkr:{symbol}"
        )
        source["content_sha256"] = hashlib.sha256(
            _canonical(payload["bars"]["symbols"][symbol])
        ).hexdigest()
    bars_bytes = _canonical(payload["bars"])
    payload["input_manifest"]["members"][0].update(
        size_bytes=len(bars_bytes),
        sha256=hashlib.sha256(bars_bytes).hexdigest(),
    )
    payload["input_manifest"]["manifest_id"] = (
        "tqqq-ibkr-paper-single-acquisition-"
        f"{hashlib.sha256(bars_bytes).hexdigest()[:24]}"
    )

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
    bars_bytes = _canonical(payload["bars"])
    payload["input_manifest"]["members"][0].update(
        size_bytes=len(bars_bytes), sha256=hashlib.sha256(bars_bytes).hexdigest()
    )
    payload["input_manifest"]["manifest_id"] = (
        "tqqq-ibkr-paper-single-acquisition-"
        f"{hashlib.sha256(bars_bytes).hexdigest()[:24]}"
    )
    boxx_bytes = _canonical(payload["bars"]["symbols"]["BOXX"])
    payload["input_manifest"]["sources"][0]["content_sha256"] = hashlib.sha256(boxx_bytes).hexdigest()

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


def test_deterministic_switching_characterization_matches_direct_ues_seam() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-520:])
    producer = _episode_producer(sessions)
    qqq_values = [100.0 + index * 0.5 for index in range(300)] + [
        249.5 - (index + 1) * 0.8 for index in range(220)
    ]
    producer.qqq = tuple(
        _Bar(session, value, value * 1.001, value * 0.999, value, 1_000_000.0)
        for session, value in zip(sessions, qqq_values)
    )
    producer._index = {session: index for index, session in enumerate(sessions)}
    producer._reset(5, producer.identity.initial_state_sha256)
    assert producer._state.parked is False
    assert producer._state.breaker_reason is None
    engine = build_risk_engine()

    with (
        patch.object(
            evidence_module,
            "evaluate_tqqq_growth_income_promotion_research",
            wraps=entrypoints.evaluate_tqqq_growth_income_promotion_research,
        ) as direct_seam,
        patch("quant_platform_kit.risk.gate.build_risk_engine", return_value=engine),
        patch.object(engine, "assess", wraps=engine.assess) as assess,
    ):
        risk_on = producer._assessment(299, sessions[300], 100_000.0)
        producer._state.pending_weights = risk_on
        producer._trade_to_target(sessions[300], 5)
        defensive = producer._assessment(
            518,
            sessions[519],
            producer._equity(sessions[518], "close"),
        )
        producer._state.pending_weights = defensive
        producer._trade_to_target(sessions[519], 5)

    assert direct_seam.call_count == assess.call_count == producer._state.assessment_count == 2
    assert risk_on["TQQQ"] > 0.0 or risk_on["QQQM"] > 0.0
    assert defensive["TQQQ"] == defensive["QQQM"] == 0.0
    assert defensive["BOXX"] > 0.0
    assert producer.switching_traces[0].signal_regime == "RISK_ON"
    assert producer.switching_traces[1].signal_regime == "DEFENSIVE"
    assert producer.switching_traces[0].execution_session == sessions[300]
    assert producer.switching_traces[1].execution_session == sessions[519]
    assert producer.switching_traces[0].replay_target_allocation == tuple(
        sorted({**risk_on, "cash": 1.0 - sum(risk_on.values())}.items())
    )
    assert producer.switching_traces[1].replay_target_allocation == tuple(
        sorted({**defensive, "cash": 1.0 - sum(defensive.values())}.items())
    )
    executed_defensive = dict(producer.switching_traces[1].executed_allocation)
    assert executed_defensive["TQQQ"] == executed_defensive["QQQM"] == 0.0
    assert executed_defensive["BOXX"] > 0.0


def test_cooldown_uses_direct_ues_seam_and_mandate_sizing_once_per_session() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-520:])
    producer = _episode_producer(sessions)
    qqq_values = [100.0 + index * 0.5 for index in range(len(sessions))]
    producer.qqq = tuple(
        _Bar(session, value, value * 1.001, value * 0.999, value, 1_000_000.0)
        for session, value in zip(sessions, qqq_values)
    )
    producer._index = {session: index for index, session in enumerate(sessions)}
    producer._reset(5, producer.identity.initial_state_sha256)
    producer._state.cooldown_remaining_execution_sessions = 20
    engine = build_risk_engine()
    contexts = []

    def evaluate(ctx, **kwargs):
        contexts.append(ctx)
        return entrypoints.evaluate_tqqq_growth_income_promotion_research(ctx, **kwargs)

    with (
        patch.object(
            evidence_module,
            "evaluate_tqqq_growth_income_promotion_research",
            side_effect=evaluate,
        ) as direct_seam,
        patch("quant_platform_kit.risk.gate.build_risk_engine", return_value=engine),
        patch.object(engine, "assess", wraps=engine.assess) as assess,
    ):
        normal_drawdown = producer._assessment(299, sessions[300], 100_000.0)
        producer._state.high_water_equity = 100_000.0
        reduced_drawdown = producer._assessment(300, sessions[301], 94_000.0)

    assert normal_drawdown == pytest.approx({"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.20})
    assert reduced_drawdown == pytest.approx({"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.10})
    assert direct_seam.call_count == assess.call_count == 2
    assert producer._state.decision_count == producer._state.assessment_count == 2
    assert all(
        float(ctx.runtime_config["dual_drive_tqqq_weight"])
        == float(ctx.runtime_config["dual_drive_qqq_weight"])
        == 0.0
        for ctx in contexts
    )
    assert all(
        trace.signal_state == "protective_cooldown"
        and trace.signal_regime == "DEFENSIVE"
        and trace.risk_disposition == "APPROVE"
        for trace in producer.switching_traces
    )
    assert producer._state.parked is False


def test_cooldown_is_exactly_20_execution_sessions_before_fresh_base_signal() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    producer._reset(5, producer.identity.initial_state_sha256)
    producer._state.tqqq_entry_price = 100.0
    producer._state.consecutive_losing_exits = 4
    producer._record_completed_exit(95.0, sessions[257])
    assert producer._state.cooldown_remaining_execution_sessions == 20
    contexts = []

    def evaluate(ctx, **_kwargs):
        contexts.append(ctx)
        cooldown = "dual_drive_tqqq_weight" in ctx.runtime_config
        return _approved_result(
            {"BOXX": 0.20} if cooldown else {"QQQM": 0.10},
            signal_state="entry",
        )

    with patch.object(
        evidence_module,
        "evaluate_tqqq_growth_income_promotion_research",
        side_effect=evaluate,
    ):
        for offset in range(20):
            execution_session = sessions[258 + offset]
            targets = producer._assessment(257 + offset, execution_session, 100_000.0)
            assert targets == {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.20}
            producer._complete_cooldown_execution_session(execution_session)
            assert producer._state.cooldown_remaining_execution_sessions == 19 - offset
        reentry = producer._assessment(277, sessions[278], 100_000.0)

    assert len(contexts) == 21
    assert [trace.signal_state for trace in producer.switching_traces[:20]] == [
        "protective_cooldown"
    ] * 20
    assert producer.switching_traces[0].risk_reason_codes == (
        "FIFTH_CONSECUTIVE_TQQQ_LOSING_EXIT",
    )
    assert all(not trace.risk_reason_codes for trace in producer.switching_traces[1:20])
    assert producer.switching_traces[19].execution_session == sessions[277]
    assert producer.switching_traces[20].signal_state == "entry"
    assert producer.switching_traces[20].execution_session == sessions[278]
    assert reentry == {"TQQQ": 0.0, "QQQM": 0.10, "BOXX": 0.0}
    assert producer._state.decision_count == producer._state.assessment_count == 21


def test_switching_characterization_invalid_input_fails_closed_without_decision() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    del producer.prices["QQQM"][sessions[-2]]

    with (
        patch.object(
            evidence_module,
            "evaluate_tqqq_growth_income_promotion_research",
            wraps=entrypoints.evaluate_tqqq_growth_income_promotion_research,
        ) as direct_seam,
        pytest.raises(TqqqPromotionEvidenceError, match="eligible asset data unavailable"),
    ):
        producer(
            sessions[-2],
            sessions[-1],
            5,
            producer.identity.initial_state_sha256,
        )

    direct_seam.assert_not_called()
    assert producer._state.assessment_count == 0
    assert producer.switching_traces == ()


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
    assert replay.decision_count == replay.risk_assessment_count == 3
    assert cash_session < park_session


def test_mid_window_risk_non_approve_counts_park_from_execution_session() -> None:
    sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS[-300:])
    producer = _episode_producer(sessions)
    start, park_session, end = sessions[258:261]
    calls = 0

    def evaluate(_ctx, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _approved_result({"BOXX": 0.20}, signal_state="idle")
        return SimpleNamespace(
            assessment=SimpleNamespace(
                outcome="REJECT",
                reason_codes=("MATERIAL_RISK_REJECTION",),
                execution_authorized=False,
            ),
            decision=SimpleNamespace(positions=(), diagnostics={}),
        )

    with patch.object(
        evidence_module,
        "evaluate_tqqq_growth_income_promotion_research",
        side_effect=evaluate,
    ):
        replay = producer(start, end, 5, producer.identity.initial_state_sha256)

    assert calls == replay.decision_count == replay.risk_assessment_count == 2
    assert replay.episode_summary.first_park_session == park_session
    assert replay.episode_summary.parked_session_count == 2
    assert sum(
        trace.risk_disposition == "PARK" for trace in replay.switching_traces
    ) == 2


def test_episode_breaker_reason_is_sticky() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)

    producer._park("ACCOUNT_DRAWDOWN", session)
    producer._park("RISK_ENGINE_NON_APPROVE", session + timedelta(days=1))

    assert producer._state.parked is True
    assert producer._state.breaker_reason == "ACCOUNT_DRAWDOWN"
    assert producer._state.first_park_session == session


def test_parked_episode_liquidates_and_reports_without_reusing_stale_trace() -> None:
    previous_session = date(2025, 1, 2)
    session = date(2025, 1, 3)
    producer = _episode_producer((previous_session, session))
    producer._reset(5, producer.identity.initial_state_sha256)
    risk_on = (("BOXX", 0.0), ("QQQM", 0.0), ("TQQQ", 0.10), ("cash", 0.90))
    producer._switching_traces.append(
        TqqqSwitchingTrace(
            signal_session=previous_session - timedelta(days=1),
            execution_session=previous_session,
            signal_state="entry",
            signal_regime="RISK_ON",
            intended_allocation=risk_on,
            risk_disposition="APPROVE",
            risk_reason_codes=(),
            replay_target_allocation=risk_on,
            executed_allocation=risk_on,
        )
    )
    producer._state = _ReplayState(
        cash=90_000.0,
        quantities={"TQQQ": 100.0, "QQQM": 0.0, "BOXX": 0.0},
        tqqq_entry_price=100.0,
        tqqq_stop_price=95.0,
        parked=True,
        breaker_reason="ACCOUNT_DRAWDOWN",
        first_park_session=previous_session,
        last_session=previous_session,
    )

    producer._trade_to_target(session, 5)

    assert producer._state.quantities == {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.0}
    assert producer._state.parked is True
    parked = producer.switching_traces[-1]
    assert parked.execution_session == session
    assert parked.signal_state == "parked"
    assert parked.signal_regime == "DEFENSIVE"
    assert parked.risk_disposition == "PARK"
    assert parked.risk_reason_codes == ("ACCOUNT_DRAWDOWN",)
    assert dict(parked.executed_allocation) == pytest.approx(
        {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.0, "cash": 1.0}
    )


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


def test_final_session_drawdown_breaker_is_reported_without_new_decision() -> None:
    session = date(2025, 1, 2)
    producer = _unit_producer(session)
    risk_on = (("BOXX", 0.0), ("QQQM", 0.0), ("TQQQ", 0.10), ("cash", 0.90))
    producer._switching_traces = [
        TqqqSwitchingTrace(
            signal_session=session - timedelta(days=1),
            execution_session=session,
            signal_state="entry",
            signal_regime="RISK_ON",
            intended_allocation=risk_on,
            risk_disposition="APPROVE",
            risk_reason_codes=(),
            replay_target_allocation=risk_on,
            executed_allocation=risk_on,
        )
    ]
    producer._state.high_water_equity = 100_000.0
    producer._state.decision_count = 7
    producer._state.assessment_count = 7

    producer._apply_drawdown_breaker(session, 89_000.0)

    assert producer._state.parked is True
    assert producer._state.breaker_reason == "ACCOUNT_DRAWDOWN"
    assert producer._state.first_park_session == session
    assert producer._state.decision_count == producer._state.assessment_count == 7
    assert producer.switching_traces[-1].risk_disposition == "PARK"
    assert producer.switching_traces[-1].risk_reason_codes == ("ACCOUNT_DRAWDOWN",)



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


def test_non_approve_direct_seam_enters_terminal_park_after_one_assessment() -> None:
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
    start = date(2024, 1, 1)
    producer.qqq = tuple(
        _Bar(start + timedelta(days=index), 100.0, 101.0, 99.0, 100.0 + index, 1_000_000.0)
        for index in range(257)
    )
    producer.prices = {symbol: {} for symbol in evidence_module._ORDERABLE_ASSETS}
    producer._state = _ReplayState()
    producer._state_sha256 = "7" * 64
    producer._scenario = 5
    producer._scenario_counts = {5: {"assessments": 0, "decisions": 0}}
    producer._switching_traces = []
    rejected = SimpleNamespace(
        assessment=SimpleNamespace(
            outcome="REJECT",
            reason_codes=("risk_engine_non_approve",),
            execution_authorized=False,
        ),
        decision=StrategyDecision(),
    )

    with patch.object(
        evidence_module,
        "evaluate_tqqq_growth_income_promotion_research",
        return_value=rejected,
    ) as direct_seam:
        targets = producer._assessment(
            256, start + timedelta(days=257), 100_000.0
        )

    direct_seam.assert_called_once()
    assert producer._scenario_counts[5] == {"assessments": 1, "decisions": 1}
    assert targets == {"TQQQ": 0.0, "QQQM": 0.0, "BOXX": 0.0}
    assert producer._state.parked is True
    assert producer._state.breaker_reason == "RISK_ENGINE_NON_APPROVE"
    assert producer._state.first_park_session == start + timedelta(days=257)
    assert producer.switching_traces[-1].risk_disposition == "PARK"
    assert producer.switching_traces[-1].signal_state == "risk_engine_non_approve"
    assert producer.switching_traces[-1].risk_reason_codes == (
        "RISK_ENGINE_NON_APPROVE",
    )
