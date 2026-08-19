from __future__ import annotations

from datetime import date

import pytest

from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner import (
    TqqqPromotionIdentity,
    TqqqPromotionContractError,
    TqqqPromotionRunner,
    TQQQ_SWITCHING_CHARACTERIZATION_SHA256,
    TqqqEpisodeSummary,
    TqqqPromotionPlan,
    TqqqWindowReplay,
    _cost_scenarios,
    _canonical_sha256,
    _params,
    _p2_v5_oos_bounds,
    _timing_sha256,
    _validate_identity,
    _validate_plan,
    build_tqqq_frozen_trial_ledger,
    build_tqqq_switching_characterization_contract,
)


def _plan() -> TqqqPromotionPlan:
    return TqqqPromotionPlan(
        folds=(
            PurgedWalkForwardFold(date(2018,1,2), date(2020,12,31), date(2022,1,3), date(2022,12,30)),
            PurgedWalkForwardFold(date(2018,1,2), date(2021,12,31), date(2023,1,3), date(2023,12,29)),
            PurgedWalkForwardFold(date(2018,1,2), date(2022,12,30), date(2024,1,2), date(2024,6,28)),
        ), locked_oos_start=date(2025, 8, 1), locked_oos_end=date(2026, 7, 31),
        purge_days=252, embargo_days=0,
    )


def _v4_plan() -> TqqqPromotionPlan:
    return TqqqPromotionPlan(
        folds=(
            PurgedWalkForwardFold(date(2022, 12, 28), date(2023, 6, 30), date(2023, 7, 3), date(2023, 12, 29)),
            PurgedWalkForwardFold(date(2024, 1, 2), date(2024, 6, 28), date(2024, 7, 1), date(2024, 12, 31)),
            PurgedWalkForwardFold(date(2025, 1, 2), date(2025, 2, 28), date(2025, 3, 3), date(2025, 7, 31)),
        ),
        locked_oos_start=date(2025, 8, 4),
        locked_oos_end=date(2026, 8, 4),
        purge_days=1,
        embargo_days=1,
    )


def _identity(**overrides: str) -> TqqqPromotionIdentity:
    values = {
        "qpk_revision": "730ad9f3983bd90cd75adecb67fcf483ffb96736",
        "ues_revision": "8b6b418bac74318f8054c5951521c9b62391de3e",
        "runner_revision": "a" * 40,
        "config_sha256": "b" * 64,
        "input_manifest_sha256": "c" * 64,
        "mandate_receipt_sha256": "d" * 64,
        "initial_state_sha256": "e" * 64,
    }
    values.update(overrides)
    return TqqqPromotionIdentity(**values)


def _episode_summary() -> TqqqEpisodeSummary:
    return TqqqEpisodeSummary(
        episode_session_count=0,
        tqqq_exposure_session_count=0,
        qqqm_exposure_session_count=0,
        boxx_exposure_session_count=0,
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


def test_exact_core_only_plan_and_costs_are_accepted() -> None:
    _validate_plan(_plan())
    assert _cost_scenarios({"turnover_cost_bps": 5.0, "stress_turnover_cost_bps": [10.0, 25.0]}) == (5, 10, 25)


def test_v4_plan_is_chronological_and_uses_fixed_strategy_cost_stress() -> None:
    _validate_plan(_v4_plan(), candidate_profile="tqqq_core_only_p2_v4")
    assert _cost_scenarios(
        {"turnover_cost_bps": 5.0, "stress_turnover_cost_bps": [10.0, 15.0]},
        candidate_profile="tqqq_core_only_p2_v4",
    ) == (5, 10, 15)
    with pytest.raises(TqqqPromotionContractError, match="unknown TQQQ candidate geometry"):
        _validate_plan(_v4_plan(), candidate_profile="tqqq_core_only_p2_v3")


def test_v5_plan_uses_only_a_binding_derived_trailing_oos_window() -> None:
    oos_start, oos_end = _p2_v5_oos_bounds(date(2026, 8, 18))
    plan = TqqqPromotionPlan(
        folds=_v4_plan().folds,
        locked_oos_start=oos_start,
        locked_oos_end=oos_end,
        purge_days=1,
        embargo_days=1,
    )

    _validate_plan(plan, candidate_profile="tqqq_core_only_p2_v5")
    assert _cost_scenarios(
        {"turnover_cost_bps": 5.0, "stress_turnover_cost_bps": [10.0, 15.0]},
        candidate_profile="tqqq_core_only_p2_v5",
    ) == (5, 10, 15)
    with pytest.raises(TqqqPromotionContractError):
        _validate_plan(
            TqqqPromotionPlan(
                folds=plan.folds,
                locked_oos_start=oos_start,
                locked_oos_end=date(2026, 8, 16),
                purge_days=1,
                embargo_days=1,
            ),
            candidate_profile="tqqq_core_only_p2_v5",
        )


@pytest.mark.parametrize("bad", [
    TqqqPromotionPlan(_plan().folds, date(2025, 7, 2), date(2026, 7, 31), 252, 0),
    TqqqPromotionPlan(_plan().folds, date(2025, 8, 1), date(2026, 7, 31), 20, 20),
])
def test_old_oos_or_purge_plan_is_rejected(bad: TqqqPromotionPlan) -> None:
    with pytest.raises(TqqqPromotionContractError):
        _validate_plan(bad)


def test_active_runner_uses_qpk_evidence_runner_not_any_execution_api() -> None:
    import inspect
    import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner as runner

    source = inspect.getsource(runner.run_tqqq_promotion_research)
    assert ".run_promotion(" in source
    assert "order_client" not in source
    assert "broker" not in source


def test_default_candidate_identity_keeps_v1_contract_output() -> None:
    identity = _identity()

    assert identity.candidate_profile == "tqqq_core_only_p2_v1"
    assert identity.candidate_variant == "tqqq_core_only_p2_v1"
    assert build_tqqq_switching_characterization_contract(identity) == (
        build_tqqq_switching_characterization_contract()
    )
    assert (
        build_tqqq_switching_characterization_contract()["sha256"]
        == TQQQ_SWITCHING_CHARACTERIZATION_SHA256
    )
    assert build_tqqq_frozen_trial_ledger(identity) == build_tqqq_frozen_trial_ledger()
    assert _params(identity, _timing_sha256(_plan()))["candidate_variant"] == (
        "tqqq_core_only_p2_v1"
    )


def test_candidate_identity_rejects_mismatched_profile_and_variant() -> None:
    identity = _identity(
        ues_revision="f" * 40,
        candidate_profile="tqqq_core_only_p2_v2",
        candidate_variant="tqqq_core_only_p2_v3",
    )

    with pytest.raises(TqqqPromotionContractError, match="candidate profile/variant mismatch"):
        _validate_identity(identity)


def test_custom_candidate_identity_can_pin_its_own_component_revisions() -> None:
    with pytest.raises(TqqqPromotionContractError, match="QPK revision mismatch"):
        _validate_identity(_identity(qpk_revision="f" * 40))

    _validate_identity(
        _identity(
            qpk_revision="3" * 40,
            ues_revision="f" * 40,
            candidate_profile="tqqq_core_only_p2_v2",
            candidate_variant="tqqq_core_only_p2_v2",
        )
    )


def test_custom_candidate_identity_propagates_to_runner_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner as runner_module

    identity = _identity(
        ues_revision="f" * 40,
        candidate_profile="tqqq_core_only_p2_v2",
        candidate_variant="tqqq_core_only_p2_v2",
    )
    plan = _plan()
    timing_sha256 = _timing_sha256(plan)
    characterization = build_tqqq_switching_characterization_contract(identity)
    ledger = build_tqqq_frozen_trial_ledger(identity)

    assert characterization["candidate_profile"] == identity.candidate_profile
    assert characterization["candidate_variant"] == identity.candidate_variant
    assert characterization["sha256"] != TQQQ_SWITCHING_CHARACTERIZATION_SHA256
    assert characterization["sha256"] == _canonical_sha256(
        {key: value for key, value in characterization.items() if key != "sha256"}
    )
    assert ledger["candidate_profile"] == identity.candidate_profile
    assert ledger["candidate_variant"] == identity.candidate_variant
    assert ledger["entries"][0]["candidate_variant"] == identity.candidate_variant

    def replay_window(
        start_date: date,
        end_date: date,
        _total_cost_bps: int,
        prior_state_sha256: str,
    ) -> TqqqWindowReplay:
        return TqqqWindowReplay(
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=prior_state_sha256,
            final_state_sha256="f" * 64,
            strategy_equity=(1.0, 1.0),
            qqq_total_return_equity=(1.0, 1.0),
            boxx_total_return_equity=(1.0, 1.0),
            asset_weights=(),
            turnover=0.0,
            trade_count=0,
            decision_count=1,
            risk_assessment_count=1,
            warmup_sessions=0,
            episode_summary=_episode_summary(),
            sessions=(),
        )

    monkeypatch.setattr(runner_module, "_validate_replay", lambda *args, **kwargs: None)
    promotion_runner = TqqqPromotionRunner(
        identity,
        plan,
        replay_window,
        total_cost_bps=5,
    )

    result = promotion_runner.run(
        identity.candidate_profile,
        _params(identity, timing_sha256),
        start_date=plan.folds[0].test_start,
        end_date=plan.folds[0].test_end,
    )

    assert result.strategy_profile == identity.candidate_profile
