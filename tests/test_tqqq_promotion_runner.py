from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from unittest.mock import patch

import pytest
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    LEGACY_PARITY_CLASSIFICATIONS,
    TQQQ_SWITCHING_CHARACTERIZATION_SHA256,
    TQQQ_ACCEPTANCE_INCONCLUSIVE,
    TQQQ_ACCEPTANCE_PASS,
    TQQQ_ACCEPTANCE_REJECT,
    TqqqEpisodeSummary,
    TqqqPromotionContractError,
    TqqqPromotionIdentity,
    TqqqPromotionPlan,
    TqqqPromotionRunner,
    TqqqSwitchingTrace,
    TqqqWindowReplay,
    build_tqqq_development_robustness_plan,
    build_tqqq_switching_characterization_contract,
    classify_tqqq_legacy_parity,
    evaluate_tqqq_pre_result_acceptance,
    run_tqqq_promotion_research,
)

QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
RUNNER_REVISION = "1" * 40


def _sha256(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _acceptance_source(metrics, traces: tuple[TqqqSwitchingTrace, ...]) -> str:
    return "tqqq_promotion_runner:" + _sha256(
        {
            "qqq_total_return": metrics.qqq_total_return,
            "boxx_total_return": metrics.boxx_total_return,
            "signal_regimes": [trace.signal_regime for trace in traces],
        }
    )


def _locked_cagr(total_return: float) -> float:
    years = (_plan().locked_oos_end - _plan().locked_oos_start).days / 365.25
    return (1.0 + total_return) ** (1.0 / years) - 1.0


def _identity() -> TqqqPromotionIdentity:
    return TqqqPromotionIdentity(
        qpk_revision=QPK_REVISION,
        ues_revision=UES_REVISION,
        runner_revision=RUNNER_REVISION,
        platform_execution_revision="2" * 40,
        config_sha256="3" * 64,
        input_manifest_sha256="4" * 64,
        mandate_receipt_sha256="5" * 64,
        initial_state_sha256="6" * 64,
    )


def _plan() -> TqqqPromotionPlan:
    return TqqqPromotionPlan(
        folds=(
            PurgedWalkForwardFold(
                train_start=date(2018, 1, 2),
                train_end=date(2022, 11, 25),
                test_start=date(2022, 12, 28),
                test_end=date(2023, 1, 31),
            ),
            PurgedWalkForwardFold(
                train_start=date(2023, 2, 21),
                train_end=date(2023, 4, 28),
                test_start=date(2023, 5, 22),
                test_end=date(2023, 6, 30),
            ),
            PurgedWalkForwardFold(
                train_start=date(2023, 7, 24),
                train_end=date(2023, 10, 31),
                test_start=date(2023, 12, 1),
                test_end=date(2024, 5, 31),
            ),
        ),
        locked_oos_start=date(2025, 7, 2),
        locked_oos_end=date(2026, 7, 31),
        purge_days=20,
        embargo_days=20,
    )


class SyntheticReplay:
    def __init__(self, *, park_first_window: bool = False) -> None:
        self.calls: list[tuple[date, date, int, str]] = []
        self.park_first_window = park_first_window

    def __call__(
        self,
        start_date: date,
        end_date: date,
        total_cost_bps: int,
        prior_state_sha256: str,
    ) -> TqqqWindowReplay:
        from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
            FROZEN_XNYS_SESSIONS,
        )

        self.calls.append((start_date, end_date, total_cost_bps, prior_state_sha256))
        call_number = (len(self.calls) - 1) % 4 + 1
        if start_date == _plan().locked_oos_start and end_date == _plan().locked_oos_end:
            sessions = tuple(
                date.fromisoformat(value)
                for value in FROZEN_XNYS_SESSIONS
                if start_date.isoformat() <= value <= end_date.isoformat()
            )
        else:
            window_sessions = tuple(
                date.fromisoformat(value)
                for value in FROZEN_XNYS_SESSIONS
                if start_date.isoformat() <= value <= end_date.isoformat()
            )
            sessions = window_sessions
        frozen_sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS)
        previous_session = {
            session: frozen_sessions[index - 1]
            for index, session in enumerate(frozen_sessions)
            if index
        }
        strategy = (100.0, 102.0, 99.0) + tuple(
            99.0 + (105.0 - 99.0) * index / (len(sessions) - 2)
            for index in range(1, len(sessions) - 1)
        )
        benchmark = (100.0, 101.0, 100.0) + tuple(
            100.0 + 3.0 * index / (len(sessions) - 2)
            for index in range(1, len(sessions) - 1)
        )
        defensive_benchmark = (100.0, 100.1, 100.2) + tuple(
            100.2 + 0.1 * index / (len(sessions) - 2)
            for index in range(1, len(sessions) - 1)
        )
        intended = (("TQQQ", 0.05), ("QQQM", 0.20), ("BOXX", 0.10), ("cash", 0.65))
        switching_traces = tuple(
            TqqqSwitchingTrace(
                signal_session=previous_session[execution_session],
                execution_session=execution_session,
                signal_state="entry" if index == 0 else "hold",
                signal_regime="RISK_ON",
                intended_allocation=intended,
                risk_disposition="APPROVE",
                risk_reason_codes=(),
                replay_target_allocation=intended,
                executed_allocation=intended,
            )
            for index, execution_session in enumerate(sessions)
        )
        summary = TqqqEpisodeSummary(
            episode_session_count=len(sessions),
            tqqq_exposure_session_count=len(sessions),
            qqqm_exposure_session_count=len(sessions),
            boxx_exposure_session_count=len(sessions),
            cash_only_session_count=0,
            parked_session_count=0,
            tqqq_entry_count=0,
            tqqq_stop_armed_count=0,
            tqqq_stop_crossing_count=0,
            tqqq_stop_fill_count=0,
            tqqq_unprotected_holding_session_count=0,
            breaker_reason=None,
            first_park_session=None,
        )
        decision_count = len(sessions)
        if self.park_first_window and call_number == 1:
            cash = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.0), ("cash", 1.0))
            switching_traces = (
                *switching_traces[:-1],
                replace(
                    switching_traces[-1],
                    signal_state="parked",
                    signal_regime="DEFENSIVE",
                    intended_allocation=cash,
                    risk_disposition="PARK",
                    risk_reason_codes=("ACCOUNT_DRAWDOWN",),
                    replay_target_allocation=cash,
                    executed_allocation=cash,
                ),
            )
            decision_count -= 1
            summary = replace(
                summary,
                parked_session_count=1,
                breaker_reason="ACCOUNT_DRAWDOWN",
                first_park_session=start_date,
            )
        return TqqqWindowReplay(
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=prior_state_sha256,
            final_state_sha256=f"{call_number:x}" * 64,
            strategy_equity=strategy,
            qqq_total_return_equity=benchmark,
            boxx_total_return_equity=defensive_benchmark,
            asset_weights=(("TQQQ", 0.05), ("QQQM", 0.20), ("BOXX", 0.10)),
            turnover=0.4,
            trade_count=2,
            decision_count=decision_count,
            risk_assessment_count=decision_count,
            warmup_sessions=257,
            episode_summary=summary,
            sessions=sessions,
            switching_traces=switching_traces,
        )


def _run(replay: SyntheticReplay | None = None):
    replay = replay or SyntheticReplay()
    with patch(
        "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
        return_value=RUNNER_REVISION,
    ):
        result = run_tqqq_promotion_research(_identity(), _plan(), replay)
    return result, replay


def test_delegates_exact_three_cost_scenarios_to_qpk_promotion_orchestrator() -> None:
    with patch.object(
        BacktestOrchestrator,
        "run_promotion",
        autospec=True,
        wraps=BacktestOrchestrator.run_promotion,
    ) as run_promotion:
        result, replay = _run()

    assert TqqqPromotionRunner.runner_kind == "real"
    assert run_promotion.call_count == 3
    assert [scenario.total_cost_bps for scenario in result.scenarios] == [5, 10, 15]
    assert all(scenario.cost_model_scope == "ALL_IN_PER_SIDE" for scenario in result.scenarios)
    assert len(replay.calls) == 12
    assert all(
        scenario.promotion_run.source_revision == UES_REVISION
        for scenario in result.scenarios
    )


def test_each_fold_and_locked_oos_starts_from_fresh_episode_state() -> None:
    result, replay = _run()

    for offset in (0, 4, 8):
        calls = replay.calls[offset : offset + 4]
        assert [call[3] for call in calls] == [_identity().initial_state_sha256] * 4

    locked = result.scenarios[0].windows[-1]
    metrics = locked.relative_metrics
    assert metrics.benchmark_symbol == "QQQ"
    assert metrics.strategy_total_return == pytest.approx(0.05)
    assert metrics.qqq_total_return == pytest.approx(0.03)
    assert metrics.boxx_total_return == pytest.approx(0.003)
    assert metrics.excess_total_return == pytest.approx(0.02)
    assert metrics.strategy_max_drawdown < 0.0
    assert metrics.qqq_max_drawdown < 0.0
    assert metrics.max_drawdown_delta == pytest.approx(
        metrics.strategy_max_drawdown - metrics.qqq_max_drawdown
    )
    assert metrics.strategy_recovery_sessions is not None
    assert metrics.strategy_unrecovered_at_end is False
    assert metrics.up_market_capture > 0.0
    assert metrics.down_market_capture > 0.0
    assert metrics.alpha is not None
    assert metrics.beta is not None
    assert metrics.information_ratio is not None
    assert metrics.cvar_95 <= metrics.var_95
    assert metrics.turnover == pytest.approx(0.4)
    assert metrics.trade_count == 2


def test_replay_contract_requires_sanitized_episode_summary() -> None:
    assert "episode_summary" in TqqqWindowReplay.__dataclass_fields__


def test_prior_fold_park_does_not_enter_the_next_episode() -> None:
    _, replay = _run(SyntheticReplay(park_first_window=True))

    for offset in (0, 4, 8):
        assert replay.calls[offset][3] == _identity().initial_state_sha256
        assert replay.calls[offset + 1][3] == _identity().initial_state_sha256


def test_result_is_research_only_and_has_no_execution_reachability() -> None:
    result, _ = _run()

    assert result.authority_scope == "RESEARCH_ONLY"
    assert result.no_order is True
    assert result.size_zero_required is True
    assert result.promotion_eligible is False
    assert result.live_ready is False
    assert result.executable_plan == ()
    assert result.order_client_intents == ()


def test_frozen_development_plan_enumerates_every_3_6_12_24_month_window() -> None:
    from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
        FROZEN_XNYS_SESSIONS,
    )

    plan = build_tqqq_development_robustness_plan(
        tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS)
    )

    assert plan["seen_development_cutoff_inclusive"] == "2025-07-01"
    assert plan["merge_with_locked_oos"] is False
    assert plan["promotion_evidence"] is False
    assert {
        horizon: (
            plan["rolling_windows"][horizon]["count"],
            plan["rolling_windows"][horizon]["first"],
            plan["rolling_windows"][horizon]["last"],
            plan["rolling_windows"][horizon]["sha256"],
        )
        for horizon in ("3_month", "6_month", "12_month", "24_month")
    } == {
        "3_month": (
            28,
            ["2023-01-03", "2023-03-31"],
            ["2025-04-01", "2025-06-30"],
            "877136166f09def7019ba2fe7616c8c820bae3c13212f3b485cfe001b455d66f",
        ),
        "6_month": (
            25,
            ["2023-01-03", "2023-06-30"],
            ["2025-01-02", "2025-06-30"],
            "31a9a72c6839e8ea117184aa0af19ebf1063d83dd8231457d50a1a6cc7d73434",
        ),
        "12_month": (
            19,
            ["2023-01-03", "2023-12-29"],
            ["2024-07-01", "2025-06-30"],
            "145a3ef1598a54a3c1e138a223e67ab2325357d7bd405749730be9baa2d76adc",
        ),
        "24_month": (
            7,
            ["2023-01-03", "2024-12-31"],
            ["2023-07-03", "2025-06-30"],
            "1a3a85d1d10a8151bd3e4ff5218d3017ce19323927b0a2c2c7f614216916301e",
        ),
    }

    development_sessions = tuple(
        date.fromisoformat(value)
        for value in FROZEN_XNYS_SESSIONS
        if "2023-01-01" <= value <= "2025-06-30"
    )
    with pytest.raises(TqqqPromotionContractError, match="development calendar identity"):
        build_tqqq_development_robustness_plan(
            development_sessions[:100] + development_sessions[101:]
        )


def test_switching_characterization_contract_has_frozen_semantic_identity() -> None:
    contract = build_tqqq_switching_characterization_contract()

    assert contract["sha256"] == TQQQ_SWITCHING_CHARACTERIZATION_SHA256
    assert contract["timing"] == "completed_close_t_to_open_t_plus_1"
    assert contract["cases"] == (
        "RISK_ON_TQQQ_OR_QQQM",
        "DEFENSIVE_BOXX",
        "RISK_ON_TO_DEFENSIVE_TRANSITION",
        "INVALID_OR_PRELISTING_INPUT_FAILS_CLOSED",
        "FRESH_EPISODE_WITHOUT_INHERITED_PARK",
    )
    assert contract["minimum_risk_asset_occupancy"] is None
    assert contract["minimum_trade_count"] is None


def _legacy_identity(
    rows: tuple[dict[str, object], ...] | None = None,
) -> dict[str, object]:
    rows = rows or (_parity_session(),)
    sessions = [str(row["session"]) for row in rows]
    return {
        "code_commit": "1" * 40,
        "code_sha256": "2" * 64,
        "runtime_config_sha256": "3" * 64,
        "switching_rules_sha256": "4" * 64,
        "data_source": "bound-private-source",
        "adjustment": "total_return_adjusted",
        "calendar": "XNYS",
        "range_start": sessions[0],
        "range_end": sessions[-1],
        "session_count": len(rows),
        "sessions_sha256": _sha256(sessions),
        "initial_state_sha256": "5" * 64,
        "decision_timing": "completed_close_t",
        "fill_timing": "same_close_t",
        "cost_model_sha256": "6" * 64,
        "input_sha256": "7" * 64,
        "metrics_sha256": _sha256(
            [
                {
                    "session": row["session"],
                    "gross_return": row["gross_return"],
                    "net_return": row["net_return"],
                }
                for row in rows
            ]
        ),
        "trades_sha256": _sha256(
            [
                {
                    "session": row["session"],
                    "trade_count": row["trade_count"],
                    "cost": row["cost"],
                }
                for row in rows
            ]
        ),
        "allocations_sha256": _sha256(
            [
                {
                    "session": row["session"],
                    "signal": row["signal"],
                    "regime": row["regime"],
                    "target_allocation": row["target_allocation"],
                    "switch": row["switch"],
                }
                for row in rows
            ]
        ),
    }


def _parity_session(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "session": "2025-06-30",
        "signal": "RISK_ON",
        "regime": "BULL",
        "target_allocation": {"TQQQ": 0.15, "QQQM": 0.35, "BOXX": 0.0, "cash": 0.5},
        "switch": True,
        "gross_return": 0.01,
        "trade_count": 1,
        "cost": 0.001,
        "net_return": 0.009,
    }
    row.update(updates)
    return row


def test_legacy_parity_contract_is_session_first_and_fail_closed() -> None:
    legacy = (_parity_session(),)

    assert LEGACY_PARITY_CLASSIFICATIONS == frozenset(
        {
            "MATCH",
            "EXPECTED_DIFFERENCE_DUE_TO_EXPLICIT_ARCHITECTURE_CHANGE",
            "UNEXPLAINED_CORE_STRATEGY_DRIFT",
            "NOT_COMPARABLE",
        }
    )
    assert classify_tqqq_legacy_parity({}, legacy, legacy) == "NOT_COMPARABLE"
    assert classify_tqqq_legacy_parity(_legacy_identity(), legacy, legacy) == "MATCH"
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            (_parity_session(signal="RISK_OFF"),),
            explicit_architecture_changes=("CLOSE_T_TO_OPEN_T_PLUS_1",),
        )
        == "UNEXPLAINED_CORE_STRATEGY_DRIFT"
    )
    sized = _parity_session(
        target_allocation={"TQQQ": 0.10, "QQQM": 0.30, "BOXX": 0.0, "cash": 0.60},
        gross_return=0.008,
        trade_count=2,
        cost=0.0008,
        net_return=0.007,
    )
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            (sized,),
            explicit_architecture_changes=("RISK_ENGINE_AND_APPROVED_SIZING",),
        )
        == "EXPECTED_DIFFERENCE_DUE_TO_EXPLICIT_ARCHITECTURE_CHANGE"
    )
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            (_parity_session(switch=False, gross_return=0.008, trade_count=2, net_return=0.007),),
            explicit_architecture_changes=("CLOSE_T_TO_OPEN_T_PLUS_1",),
        )
        == "EXPECTED_DIFFERENCE_DUE_TO_EXPLICIT_ARCHITECTURE_CHANGE"
    )
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            (_parity_session(net_return=0.008),),
            explicit_architecture_changes=("EXECUTION_SESSION_COUNTER_ATTRIBUTION",),
        )
        == "UNEXPLAINED_CORE_STRATEGY_DRIFT"
    )
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            legacy,
            explicit_architecture_changes=None,  # type: ignore[arg-type]
        )
        == "NOT_COMPARABLE"
    )
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(),
            legacy,
            legacy,
            explicit_architecture_changes=([],),  # type: ignore[arg-type]
        )
        == "NOT_COMPARABLE"
    )
    assert (
        classify_tqqq_legacy_parity(
            {**_legacy_identity(), "metrics_sha256": "0" * 64}, legacy, legacy
        )
        == "NOT_COMPARABLE"
    )
    complete = (
        _parity_session(session="2025-06-27"),
        _parity_session(session="2025-06-28"),
        _parity_session(),
    )
    incomplete = (complete[0], complete[-1])
    assert (
        classify_tqqq_legacy_parity(
            _legacy_identity(complete), incomplete, incomplete
        )
        == "NOT_COMPARABLE"
    )


def _acceptance_result(*, candidate_returns: tuple[float, float, float]):
    result, _ = _run()
    scenarios = []
    for scenario, candidate_return in zip(result.scenarios, candidate_returns):
        locked = scenario.windows[-1]
        strategy_cagr = _locked_cagr(candidate_return)
        metrics = replace(
            locked.relative_metrics,
            strategy_total_return=candidate_return,
            strategy_cagr=strategy_cagr,
            excess_cagr=strategy_cagr - locked.relative_metrics.qqq_cagr,
            strategy_max_drawdown=-0.05,
            qqq_max_drawdown=-0.08,
        )
        scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        total_return=candidate_return,
                        cagr=strategy_cagr,
                        excess_cagr=strategy_cagr - locked.relative_metrics.qqq_cagr,
                        max_drawdown=-0.05,
                        benchmark_max_drawdown=-0.08,
                        oos_max_drawdown=-0.05,
                        source_script=_acceptance_source(metrics, locked.switching_traces),
                    ),
                ),
                windows=(*scenario.windows[:-1], replace(locked, relative_metrics=metrics)),
            )
        )
    return replace(result, scenarios=tuple(scenarios))


def test_pre_result_numeric_terminal_mapping_never_rejects_for_parity_defects() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))

    assert evaluate_tqqq_pre_result_acceptance(passing, "MATCH") == TQQQ_ACCEPTANCE_PASS
    assert evaluate_tqqq_pre_result_acceptance(passing, "NOT_COMPARABLE") == TQQQ_ACCEPTANCE_PASS
    defensive_allocation = (
        ("TQQQ", 0.0),
        ("QQQM", 0.0),
        ("BOXX", 0.50),
        ("cash", 0.50),
    )
    defensive_scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        traces = tuple(
            replace(
                trace,
                signal_state="idle",
                signal_regime="DEFENSIVE",
                intended_allocation=defensive_allocation,
                replay_target_allocation=defensive_allocation,
                executed_allocation=defensive_allocation,
            )
            for trace in locked.switching_traces
        )
        defensive_scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        source_script=_acceptance_source(locked.relative_metrics, traces),
                    ),
                ),
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        switching_traces=traces,
                    ),
                ),
            )
        )
    defensive = replace(passing, scenarios=tuple(defensive_scenarios))
    assert evaluate_tqqq_pre_result_acceptance(defensive, "NOT_COMPARABLE") == (
        TQQQ_ACCEPTANCE_PASS
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            passing, "UNEXPLAINED_CORE_STRATEGY_DRIFT"
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    locked = passing.scenarios[0].windows[-1]
    trace = locked.switching_traces[0]
    semantic_drift = replace(
        trace,
        executed_allocation=(
            ("TQQQ", 0.0),
            ("QQQM", 0.0),
            ("BOXX", 0.50),
            ("cash", 0.50),
        ),
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(
                            *passing.scenarios[0].windows[:-1],
                            replace(
                                locked,
                                switching_traces=(semantic_drift, *locked.switching_traces[1:]),
                            ),
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "NOT_COMPARABLE",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    inconsistent_benchmark = replace(
        passing.scenarios[0].windows[-1].relative_metrics,
        qqq_total_return=0.20,
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(
                            *passing.scenarios[0].windows[:-1],
                            replace(
                                passing.scenarios[0].windows[-1],
                                relative_metrics=inconsistent_benchmark,
                            ),
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    wrong_cost_model = replace(
        passing.scenarios[0].promotion_run.cost_model,
        slippage_bps=0.0,
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        promotion_run=replace(
                            passing.scenarios[0].promotion_run,
                            cost_model=wrong_cost_model,
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(replace(passing, no_order=False), "MATCH")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, timing_sha256="0" * 64), "MATCH"
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, identity=replace(passing.identity, config_sha256="0" * 64)),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(passing.scenarios[0], windows=None),  # type: ignore[arg-type]
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    locked_run = passing.scenarios[0].promotion_run.locked_oos_result
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        promotion_run=replace(
                            passing.scenarios[0].promotion_run,
                            locked_oos_result=replace(
                                locked_run,
                                params={
                                    **locked_run.params,
                                    "config_sha256": "0" * 64,
                                },
                            ),
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    incomplete = passing.scenarios[0].windows[-1]
    incomplete = replace(
        incomplete,
        relative_metrics=replace(incomplete.relative_metrics, information_ratio=float("nan")),
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(*passing.scenarios[0].windows[:-1], incomplete),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    sparse = passing.scenarios[0].windows[-1]
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(
                            *passing.scenarios[0].windows[:-1],
                            replace(sparse, sessions=sparse.sessions[1:]),
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    invalid_counts = passing.scenarios[0].windows[-1]
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(
                            *passing.scenarios[0].windows[:-1],
                            replace(
                                invalid_counts,
                                episode_summary=replace(
                                    invalid_counts.episode_summary,
                                    tqqq_stop_crossing_count=2,
                                    tqqq_stop_fill_count=2,
                                    tqqq_entry_count=1,
                                    tqqq_stop_armed_count=1,
                                ),
                            ),
                        ),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            _acceptance_result(candidate_returns=(0.07, 0.065, 0.01)),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_REJECT
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            _acceptance_result(candidate_returns=(0.06, 0.07, 0.065)),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )
    unprotected = passing.scenarios[0].windows[-1]
    unprotected = replace(
        unprotected,
        episode_summary=replace(
            unprotected.episode_summary,
            tqqq_unprotected_holding_session_count=1,
        ),
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(
                passing,
                scenarios=(
                    replace(
                        passing.scenarios[0],
                        windows=(*passing.scenarios[0].windows[:-1], unprotected),
                    ),
                    *passing.scenarios[1:],
                ),
            ),
            "MATCH",
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_rejects_material_cost_adjusted_execution_allocation_drift() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    locked = passing.scenarios[-1].windows[-1]
    trace = locked.switching_traces[0]
    drift = replace(
        trace,
        executed_allocation=(
            ("TQQQ", 0.0),
            ("QQQM", 0.000001),
            ("BOXX", 0.0),
            ("cash", 0.999999),
        ),
    )
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                passing.scenarios[-1],
                windows=(
                    *passing.scenarios[-1].windows[:-1],
                    replace(
                        locked,
                        switching_traces=(drift, *locked.switching_traces[1:]),
                    ),
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )

    within_cost_tolerance = replace(
        trace,
        executed_allocation=(
            ("TQQQ", 0.04993),
            ("QQQM", 0.19970),
            ("BOXX", 0.09985),
            ("cash", 0.65052),
        ),
    )
    accepted = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                passing.scenarios[-1],
                windows=(
                    *passing.scenarios[-1].windows[:-1],
                    replace(
                        locked,
                        switching_traces=(
                            within_cost_tolerance,
                            *locked.switching_traces[1:],
                        ),
                    ),
                ),
            ),
        ),
    )
    assert (
        evaluate_tqqq_pre_result_acceptance(accepted, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_PASS
    )


def test_acceptance_requires_window_metrics_to_match_locked_backtest_result() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    inconsistent = replace(
        scenario.promotion_run.locked_oos_result,
        total_return=-0.20,
        max_drawdown=-0.20,
        oos_max_drawdown=-0.20,
    )
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=inconsistent,
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_requires_strategy_total_return_cagr_consistency() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        metrics = replace(locked.relative_metrics, strategy_total_return=0.50)
        scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        total_return=0.50,
                    ),
                ),
                windows=(
                    *scenario.windows[:-1],
                    replace(locked, relative_metrics=metrics),
                ),
            )
        )
    result = replace(passing, scenarios=tuple(scenarios))

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_rejects_breaker_metadata_without_park_sessions() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    locked = scenario.windows[-1]
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        episode_summary=replace(
                            locked.episode_summary,
                            breaker_reason="ACCOUNT_DRAWDOWN",
                            first_park_session=locked.end_date,
                        ),
                    ),
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_binds_threshold_benchmark_returns_to_locked_result() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        metrics = replace(
            locked.relative_metrics,
            qqq_total_return=0.01,
            boxx_total_return=-0.10,
        )
        scenarios.append(
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(locked, relative_metrics=metrics),
                ),
            )
        )

    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, scenarios=tuple(scenarios)), "NOT_COMPARABLE"
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_binds_defensive_only_classification_to_locked_result() -> None:
    passing = _acceptance_result(candidate_returns=(0.013, 0.012, 0.011))
    defensive = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.50), ("cash", 0.50))
    risk_on = (("TQQQ", 0.0), ("QQQM", 0.50), ("BOXX", 0.0), ("cash", 0.50))
    scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        metrics = replace(
            locked.relative_metrics,
            qqq_total_return=0.02,
            boxx_total_return=0.08,
            qqq_cagr=_locked_cagr(0.02),
            strategy_max_drawdown=-0.01,
            qqq_max_drawdown=-0.02,
        )
        defensive_traces = tuple(
            replace(
                trace,
                signal_state="idle",
                signal_regime="DEFENSIVE",
                intended_allocation=defensive,
                replay_target_allocation=defensive,
                executed_allocation=defensive,
            )
            for trace in locked.switching_traces
        )
        relabeled = tuple(
            replace(
                trace,
                signal_state="hold",
                signal_regime="RISK_ON",
                intended_allocation=risk_on,
                replay_target_allocation=risk_on,
                executed_allocation=risk_on,
            )
            for trace in defensive_traces
        )
        scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        max_drawdown=-0.01,
                        benchmark_cagr=_locked_cagr(0.02),
                        benchmark_max_drawdown=-0.02,
                        oos_max_drawdown=-0.01,
                        source_script=_acceptance_source(metrics, defensive_traces),
                    ),
                ),
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        relative_metrics=metrics,
                        switching_traces=relabeled,
                    ),
                ),
            )
        )

    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, scenarios=tuple(scenarios)), "NOT_COMPARABLE"
        )
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_requires_adjacent_frozen_xnys_switching_sessions() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    locked = scenario.windows[-1]
    trace = locked.switching_traces[1]
    stale_signal = replace(
        trace,
        signal_session=locked.sessions[0] - date.resolution,
    )
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        switching_traces=(
                            locked.switching_traces[0],
                            stale_signal,
                            *locked.switching_traces[2:],
                        ),
                    ),
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_fails_closed_on_malformed_locked_result_params() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    malformed = replace(
        scenario.promotion_run.locked_oos_result,
        params=["invalid"],  # type: ignore[arg-type]
    )
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=malformed,
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_enforces_caps_on_every_switching_target() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    locked = scenario.windows[-1]
    trace = locked.switching_traces[0]
    over_cap = (("TQQQ", 1.0), ("QQQM", 0.0), ("BOXX", 0.0), ("cash", 0.0))
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        switching_traces=(
                            replace(
                                trace,
                                intended_allocation=over_cap,
                                replay_target_allocation=over_cap,
                                executed_allocation=over_cap,
                            ),
                            *locked.switching_traces[1:],
                        ),
                    ),
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_acceptance_requires_park_trace_and_summary_to_agree() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    scenario = passing.scenarios[-1]
    locked = scenario.windows[-1]
    cash = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.0), ("cash", 1.0))
    parked = replace(
        locked.switching_traces[-1],
        signal_state="parked",
        signal_regime="DEFENSIVE",
        intended_allocation=cash,
        risk_disposition="PARK",
        risk_reason_codes=("ACCOUNT_DRAWDOWN",),
        replay_target_allocation=cash,
        executed_allocation=cash,
    )
    result = replace(
        passing,
        scenarios=(
            *passing.scenarios[:-1],
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        decision_count=locked.decision_count - 1,
                        risk_assessment_count=locked.risk_assessment_count - 1,
                        switching_traces=(*locked.switching_traces[:-1], parked),
                    ),
                ),
            ),
        ),
    )

    assert (
        evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
        == TQQQ_ACCEPTANCE_INCONCLUSIVE
    )


def test_parked_trace_reaches_frozen_reject_terminal() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    cash = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.0), ("cash", 1.0))
    scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        parked = replace(
            locked.switching_traces[-1],
            signal_state="parked",
            signal_regime="DEFENSIVE",
            intended_allocation=cash,
            risk_disposition="PARK",
            risk_reason_codes=("ACCOUNT_DRAWDOWN",),
            replay_target_allocation=cash,
            executed_allocation=cash,
        )
        traces = (*locked.switching_traces[:-1], parked)
        scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        source_script=_acceptance_source(locked.relative_metrics, traces),
                    ),
                ),
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        decision_count=locked.decision_count - 1,
                        risk_assessment_count=locked.risk_assessment_count - 1,
                        switching_traces=traces,
                        episode_summary=replace(
                            locked.episode_summary,
                            parked_session_count=1,
                            breaker_reason="ACCOUNT_DRAWDOWN",
                            first_park_session=locked.end_date,
                        ),
                    ),
                ),
            )
        )

    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, scenarios=tuple(scenarios)), "NOT_COMPARABLE"
        )
        == TQQQ_ACCEPTANCE_REJECT
    )


def test_post_approval_breaker_trace_reaches_frozen_reject_terminal() -> None:
    passing = _acceptance_result(candidate_returns=(0.07, 0.065, 0.06))
    cash = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.0), ("cash", 1.0))
    scenarios = []
    for scenario in passing.scenarios:
        locked = scenario.windows[-1]
        parked = replace(
            locked.switching_traces[-1],
            risk_disposition="PARK",
            risk_reason_codes=("CONSECUTIVE_TQQQ_LOSING_EXITS",),
            executed_allocation=cash,
        )
        scenarios.append(
            replace(
                scenario,
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        switching_traces=(*locked.switching_traces[:-1], parked),
                        episode_summary=replace(
                            locked.episode_summary,
                            parked_session_count=1,
                            breaker_reason="CONSECUTIVE_TQQQ_LOSING_EXITS",
                            first_park_session=locked.end_date,
                        ),
                    ),
                ),
            )
        )

    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(passing, scenarios=tuple(scenarios)), "NOT_COMPARABLE"
        )
        == TQQQ_ACCEPTANCE_REJECT
    )


def test_defensive_only_positive_market_retains_boxx_relative_threshold() -> None:
    result = _acceptance_result(candidate_returns=(0.013, 0.012, 0.011))
    defensive = (("TQQQ", 0.0), ("QQQM", 0.0), ("BOXX", 0.50), ("cash", 0.50))
    scenarios = []
    for scenario in result.scenarios:
        locked = scenario.windows[-1]
        metrics = replace(
            locked.relative_metrics,
            qqq_total_return=0.02,
            boxx_total_return=0.08,
            qqq_cagr=_locked_cagr(0.02),
            strategy_max_drawdown=-0.01,
            qqq_max_drawdown=-0.02,
        )
        scenarios.append(
            replace(
                scenario,
                promotion_run=replace(
                    scenario.promotion_run,
                    locked_oos_result=replace(
                        scenario.promotion_run.locked_oos_result,
                        max_drawdown=-0.01,
                        benchmark_cagr=_locked_cagr(0.02),
                        benchmark_max_drawdown=-0.02,
                        oos_max_drawdown=-0.01,
                        source_script=_acceptance_source(
                            metrics,
                            tuple(
                                replace(
                                    trace,
                                    signal_state="idle",
                                    signal_regime="DEFENSIVE",
                                    intended_allocation=defensive,
                                    replay_target_allocation=defensive,
                                    executed_allocation=defensive,
                                )
                                for trace in locked.switching_traces
                            ),
                        ),
                    ),
                ),
                windows=(
                    *scenario.windows[:-1],
                    replace(
                        locked,
                        relative_metrics=metrics,
                        switching_traces=tuple(
                            replace(
                                trace,
                                signal_state="idle",
                                signal_regime="DEFENSIVE",
                                intended_allocation=defensive,
                                replay_target_allocation=defensive,
                                executed_allocation=defensive,
                            )
                            for trace in locked.switching_traces
                        ),
                    ),
                ),
            )
        )

    assert (
        evaluate_tqqq_pre_result_acceptance(
            replace(result, scenarios=tuple(scenarios)), "NOT_COMPARABLE"
        )
        == TQQQ_ACCEPTANCE_REJECT
    )


@pytest.mark.parametrize(
    "update,message",
    [
        ({"data_available": False}, "data unavailable"),
        ({"asset_weights": (("QQQ", 0.1), ("QQQM", 0.0), ("BOXX", 0.0))}, "ETF-only"),
        ({"asset_weights": (("TQQQ", 0.15), ("QQQM", 0.10), ("BOXX", 0.0))}, "effective exposure"),
        ({"income_layer_enabled": True}, "income layer"),
        ({"option_overlay_enabled": True}, "option overlay"),
        ({"order_intents": (object(),)}, "order intents"),
        ({"executable_plan": (object(),)}, "executable plan"),
        ({"risk_assessment_count": 2}, "exactly once"),
        ({"market_regime_control_sha256": ""}, "state/plugin"),
        ({"warmup_sessions": 256}, "warmup"),
        ({"cash_reset": True}, "cash reset"),
    ],
)
def test_noncanonical_or_unavailable_replay_fails_closed(
    update: dict[str, object], message: str
) -> None:
    replay = SyntheticReplay()
    original = replay.__call__

    def invalid(*args, **kwargs):
        return replace(original(*args, **kwargs), **update)

    with (
        patch.object(replay, "__call__", side_effect=invalid),
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),pytest.raises(TqqqPromotionContractError, match=message)
    ):
        run_tqqq_promotion_research(_identity(), _plan(), invalid)


def test_sparse_purged_fold_calendar_fails_closed() -> None:
    replay = SyntheticReplay()

    def sparse(start_date, end_date, total_cost_bps, prior_state_sha256):
        window = replay(start_date, end_date, total_cost_bps, prior_state_sha256)
        if start_date != _plan().folds[0].test_start:
            return window
        return replace(
            window,
            strategy_equity=window.strategy_equity[:3],
            qqq_total_return_equity=window.qqq_total_return_equity[:3],
            boxx_total_return_equity=window.boxx_total_return_equity[:3],
            decision_count=2,
            risk_assessment_count=2,
            episode_summary=replace(
                window.episode_summary,
                episode_session_count=2,
                tqqq_exposure_session_count=2,
                qqqm_exposure_session_count=2,
                boxx_exposure_session_count=2,
            ),
            sessions=(window.sessions[0], window.sessions[-1]),
            switching_traces=(window.switching_traces[0], window.switching_traces[-1]),
        )

    with (
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        pytest.raises(TqqqPromotionContractError, match="session identity"),
    ):
        run_tqqq_promotion_research(_identity(), _plan(), sparse)


def test_identity_and_timing_are_immutable_and_fail_closed_before_replay() -> None:
    replay = SyntheticReplay()
    identity = replace(_identity(), qpk_revision="7" * 40)

    with patch(
        "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
        return_value=RUNNER_REVISION,
    ), pytest.raises(TqqqPromotionContractError, match="QPK revision"):
        run_tqqq_promotion_research(identity, _plan(), replay)

    assert replay.calls == []


def test_pre_common_eligibility_plan_fails_closed_before_replay() -> None:
    replay = SyntheticReplay()
    first = replace(_plan().folds[0], test_start=date(2022, 12, 27))
    plan = replace(_plan(), folds=(first, *_plan().folds[1:]))

    with pytest.raises(TqqqPromotionContractError, match="exact common eligibility"):
        run_tqqq_promotion_research(_identity(), plan, replay)

    assert replay.calls == []


def test_locked_oos_replay_requires_every_frozen_session() -> None:
    replay = SyntheticReplay()
    original = replay.__call__

    def sparse(*args, **kwargs):
        result = original(*args, **kwargs)
        if result.start_date != _plan().locked_oos_start:
            return result
        sessions = result.sessions[:100] + result.sessions[101:]
        traces = result.switching_traces[:100] + result.switching_traces[101:]
        return replace(
            result,
            sessions=sessions,
            switching_traces=traces,
            decision_count=len(sessions),
            risk_assessment_count=len(sessions),
            episode_summary=replace(
                result.episode_summary,
                episode_session_count=len(sessions),
                tqqq_exposure_session_count=len(sessions),
                qqqm_exposure_session_count=len(sessions),
                boxx_exposure_session_count=len(sessions),
            ),
            strategy_equity=result.strategy_equity[:101] + result.strategy_equity[102:],
            qqq_total_return_equity=(
                result.qqq_total_return_equity[:101]
                + result.qqq_total_return_equity[102:]
            ),
            boxx_total_return_equity=(
                result.boxx_total_return_equity[:101]
                + result.boxx_total_return_equity[102:]
            ),
        )

    with (
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        pytest.raises(TqqqPromotionContractError, match="replay session identity"),
    ):
        run_tqqq_promotion_research(_identity(), _plan(), sparse)
