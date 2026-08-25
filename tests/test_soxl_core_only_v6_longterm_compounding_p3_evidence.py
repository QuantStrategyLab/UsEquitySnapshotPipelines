from __future__ import annotations

from datetime import date, timedelta

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v6_longterm_compounding_p3_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle.levered_strategy_benchmark import (
    build_same_window_buy_and_hold_benchmark,
)


def _materialized() -> dict[str, object]:
    sessions = []
    for index in range(756):
        session_date = date(2023, 1, 2) + timedelta(days=index)
        sessions.append(
            {
                "as_of": f"{session_date.isoformat()}T00:00:00+00:00",
                "prices": {"SOXX": 100.0 + index},
            }
        )
    return {
        "schema_version": "qsl.soxl-soxx-core-only-p3-free-split-close-materialized-input.v1",
        "sessions": sessions,
    }


def _plan(materialized: dict[str, object]) -> dict[str, object]:
    dates = [row["as_of"].removesuffix("T00:00:00+00:00") for row in materialized["sessions"]]
    short_dates = dates[:4]
    requests = [
        {
            "window_id": f"short-{cost}",
            "window_kind": "purged_sequential_evidence",
            "cost_bps": cost,
            "session_dates": short_dates,
        }
        for cost in (5, 10, 15)
        for _ in range(4)
    ]
    requests.extend(
        {
            "window_id": "continuous_756_xnys_session_long_horizon",
            "window_kind": "continuous_long_horizon",
            "cost_bps": cost,
            "session_dates": dates,
        }
        for cost in (5, 10, 15)
    )
    return {"requests": requests}


def _base_summary(materialized: dict[str, object], plan: dict[str, object], *, reject_short: bool) -> dict[str, object]:
    sessions_by_date = {
        row["as_of"].removesuffix("T00:00:00+00:00"): row for row in materialized["sessions"]
    }
    runs = []
    for request in plan["requests"]:
        benchmark = build_same_window_buy_and_hold_benchmark(
            [sessions_by_date[item] for item in request["session_dates"]], symbol="SOXX"
        )
        is_long = request["window_kind"] == "continuous_long_horizon"
        max_drawdown = float(benchmark["max_drawdown"])
        if reject_short and not is_long:
            max_drawdown += 0.01
        runs.append(
            {
                "window_id": request["window_id"],
                "window_kind": request["window_kind"],
                "cost_bps": request["cost_bps"],
                "replay_input_sha256": "a" * 64,
                "metrics": {
                    "max_drawdown": max_drawdown,
                    "calmar": float(benchmark["calmar"]) + 1.0,
                },
            }
        )
    base = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-summary.v1",
        "status": "SUCCESS",
        "p1_identity": {},
        "p2_identity": {},
        "materialized_input_sha256": "b" * 64,
        "evidence_plan_sha256": "c" * 64,
        "execution_identity": {},
        "runs": runs,
    }
    base["evidence_summary_sha256"] = evidence._sha256(base)
    return base


def test_v6_plan_adds_one_continuous_long_window_per_cost(monkeypatch) -> None:
    materialized = _materialized()
    base = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-plan.v1",
        "cost_bps": [5, 10, 15],
        "requests": [],
        "evidence_plan_sha256": "placeholder",
    }
    monkeypatch.setattr(
        evidence,
        "build_soxl_core_only_free_split_close_p3_evidence_plan",
        lambda *_args, **_kwargs: base,
    )

    plan = evidence.build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan(materialized)

    assert plan["schema_version"] == "qsl.soxl-soxx-core-only-p3-evidence-plan.v2"
    long_runs = [item for item in plan["requests"] if item["window_kind"] == "continuous_long_horizon"]
    assert [item["cost_bps"] for item in long_runs] == [5, 10, 15]
    assert all(len(item["session_dates"]) == 756 for item in long_runs)


def test_v6_summary_keeps_short_calmar_diagnostic_and_requires_future_confirmation(monkeypatch) -> None:
    materialized = _materialized()
    plan = _plan(materialized)
    base = _base_summary(materialized, plan, reject_short=False)
    monkeypatch.setattr(evidence, "_build_base_summary", lambda **_kwargs: base)

    result = evidence.build_soxl_core_only_v6_longterm_compounding_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=lambda _input: {},
    )

    policy = result["relative_benchmark_policy"]
    assert policy["evidence_status"] == "EVIDENCE_COMPLETE"
    assert policy["strategy_verdict"] == "PASS_PENDING_FORWARD_CONFIRMATION"
    assert policy["automatic_promotion"] is False
    assert result["runs"][0]["relative_benchmark_gate_scope"] == "drawdown_only"
    assert result["runs"][-1]["relative_benchmark_gate_scope"] == "longterm_compounding"


def test_v6_summary_rejects_any_short_window_drawdown_failure(monkeypatch) -> None:
    materialized = _materialized()
    plan = _plan(materialized)
    base = _base_summary(materialized, plan, reject_short=True)
    monkeypatch.setattr(evidence, "_build_base_summary", lambda **_kwargs: base)

    result = evidence.build_soxl_core_only_v6_longterm_compounding_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=lambda _input: {},
    )

    assert result["relative_benchmark_policy"]["strategy_verdict"] == "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
