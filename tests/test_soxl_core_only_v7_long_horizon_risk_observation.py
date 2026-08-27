from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v7_long_horizon_risk_observation as observation
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")


def _sessions() -> list[dict[str, object]]:
    first = date(2023, 1, 2)
    return [
        {
            "as_of": f"{(first + timedelta(days=index)).isoformat()}T00:00:00+00:00",
            "prices": {"SOXX": 100.0 + index, "SOXL": 50.0 + index, "BOXX": 10.0},
            "market_data": {"derived_indicators": {}},
        }
        for index in range(756)
    ]


def _replay(*, cost_bps: int, session_count: int) -> dict[str, object]:
    replay = {
        "schema_version": "qsl.soxl-core-only-p3-stateful-replay-result.v1",
        "cost_bps": cost_bps,
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0 + session_count,
        "executed_signal_count": session_count - 1,
        "unexecuted_final_signal": True,
        "one_way_turnover": 0.0,
        "cost_total": 0.0,
        "decisions": [
            {"equity_before_signal": 100_000.0 + index * (1.0 if cost_bps != 15 else 0.5)}
            for index in range(session_count)
        ],
    }
    replay["output_sha256"] = hashlib.sha256(_canonical(replay)).hexdigest()
    outer = {
        "schema_version": "qsl.soxl-core-only-p3-isolated-replay-result.v1",
        "status": "SUCCESS",
        "execution_identity": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
            "quant_platform_kit_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.qpk_revision,
            "uv_lock_sha256": "3ab6974ae8c2cece2fcff527828612eab6d4ab1baf5ab3b4a6f648c057ecc301",
        },
        "p2_identity": {
            "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
            "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
        },
        "replay": replay,
    }
    outer["result_sha256"] = hashlib.sha256(_canonical(outer)).hexdigest()
    return outer


def _fixture() -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[tuple[int, int], dict[str, object]]]:
    materialized = {"sessions": _sessions()}
    dates = [row["as_of"].removesuffix("T00:00:00+00:00") for row in materialized["sessions"]]
    requests = [
        {
            "window_id": "trailing_252_xnys_session_oos",
            "window_kind": "rolling_locked_oos",
            "cost_bps": 10,
            "session_dates": dates[-252:],
        },
        {
            "window_id": "continuous_756_xnys_session_long_horizon",
            "window_kind": "continuous_long_horizon",
            "cost_bps": 10,
            "session_dates": dates,
        },
        {
            "window_id": "continuous_756_xnys_session_long_horizon",
            "window_kind": "continuous_long_horizon",
            "cost_bps": 15,
            "session_dates": dates,
        },
    ]
    plan = {"requests": requests}
    replays = {(10, 252): _replay(cost_bps=10, session_count=252), (10, 756): _replay(cost_bps=10, session_count=756), (15, 756): _replay(cost_bps=15, session_count=756)}
    runs = []
    sessions_by_date = {row["as_of"].removesuffix("T00:00:00+00:00"): row for row in materialized["sessions"]}
    for request in requests:
        replay_input = {
            "schema_version": "qsl.soxl-core-only-p3-stateful-replay-input.v1",
            "initial_equity": 100_000.0,
            "cost_bps": request["cost_bps"],
            "sessions": [sessions_by_date[item] for item in request["session_dates"]],
        }
        replay = replays[(request["cost_bps"], len(request["session_dates"]))]["replay"]
        runs.append(
            {
                "window_id": request["window_id"],
                "window_kind": request["window_kind"],
                "cost_bps": request["cost_bps"],
                "replay_input_sha256": hashlib.sha256(_canonical(replay_input)).hexdigest(),
                "metrics": {"replay_result_sha256": replay["output_sha256"]},
            }
        )
    summary = {
        "p1_identity": {"input_manifest_sha256": "1" * 64},
        "execution_identity": replays[(10, 756)]["execution_identity"],
        "runs": runs,
        "evidence_summary_sha256": "2" * 64,
    }
    return materialized, plan, summary, replays


def test_builds_private_p3_observation_with_true_oos_cost_stress_and_paired_bootstraps(monkeypatch) -> None:
    materialized, plan, summary, replays = _fixture()
    monkeypatch.setattr(
        observation,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan",
        lambda _materialized: plan,
    )
    monkeypatch.setattr(
        observation,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary",
        lambda **_kwargs: summary,
    )

    result = observation.build_soxl_core_only_v7_long_horizon_risk_observation(
        materialized=materialized,
        evidence_plan=plan,
        evidence_summary=summary,
        replay_executor=lambda replay_input: replays[(int(replay_input["cost_bps"]), len(replay_input["sessions"]))],
    )

    assert result["schema"] == observation.RISK_OBSERVATION_SCHEMA
    assert result["benchmark"] == {
        "benchmark_id": "soxx",
        "benchmark_kind": "unlevered_reference",
        "sessions_per_year": 252,
    }
    assert [item["scenario_kind"] for item in result["scenario_paths"]] == [
        "WALK_FORWARD",
        "STRESS",
        *["BOOTSTRAP"] * 8,
    ]
    assert result["scenario_paths"][0]["session_count"] == 252
    assert len(result["scenario_paths"][0]["strategy_returns_bps"]) == 251
    assert result["scenario_paths"][1]["session_count"] == 756
    assert result["observation_sha256"] == observation.calculate_soxl_core_only_v7_long_horizon_risk_observation_sha256(
        result
    )
    v2 = observation.build_soxl_core_only_v7_long_horizon_risk_observation_v2(
        materialized=materialized,
        evidence_plan=plan,
        evidence_summary=summary,
        replay_executor=lambda replay_input: replays[(int(replay_input["cost_bps"]), len(replay_input["sessions"]))],
    )
    assert v2["schema"] == "qsl.long_horizon_risk_observation.v2"
    assert v2["candidate"] == result["candidate"]
    assert v2["source_evidence"] == result["source_evidence"]
    assert v2["scenario_paths"] == result["scenario_paths"]
    assert v2["risk_capability"] == {
        "portfolio_scope": "SINGLE_CANDIDATE",
        "return_evaluation": "REPLAY_REQUIRED",
        "cashflow_treatment": "NOT_APPLICABLE",
        "risk_factor_coverage": ["CONCENTRATION", "LEVERAGE", "VOLATILITY"],
    }
    assert v2["benchmark_policy"]["return_basis"] == "SPLIT_ADJUSTED_PRICE_RETURN"
    assert v2["observation_sha256"] == observation.calculate_soxl_core_only_v7_long_horizon_risk_observation_v2_sha256(v2)
    comparison = observation.build_soxl_core_only_v7_long_horizon_risk_observation_comparison(
        v1_observation=result,
        v2_observation=v2,
    )
    assert comparison["status"] == "CONSISTENT"
    assert comparison["v1_observation_sha256"] == result["observation_sha256"]
    assert comparison["v2_observation_sha256"] == v2["observation_sha256"]
    assert "returns" not in json.dumps(comparison, sort_keys=True).lower()
    assert comparison[
        "comparison_sha256"
    ] == observation.calculate_soxl_core_only_v7_long_horizon_risk_observation_comparison_sha256(comparison)


def test_mismatched_replay_receipt_fails_closed(monkeypatch) -> None:
    materialized, plan, summary, replays = _fixture()
    monkeypatch.setattr(
        observation,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan",
        lambda _materialized: plan,
    )
    monkeypatch.setattr(
        observation,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary",
        lambda **_kwargs: summary,
    )
    corrupted = {**replays[(10, 252)], "result_sha256": "0" * 64}

    with pytest.raises(observation.SoxlCoreOnlyV7LongHorizonRiskObservationError):
        observation.build_soxl_core_only_v7_long_horizon_risk_observation(
            materialized=materialized,
            evidence_plan=plan,
            evidence_summary=summary,
            replay_executor=lambda replay_input: corrupted
            if len(replay_input["sessions"]) == 252
            else replays[(int(replay_input["cost_bps"]), len(replay_input["sessions"]))],
        )
