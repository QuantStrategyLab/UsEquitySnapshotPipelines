from __future__ import annotations

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v5_longterm_drawdown_p3_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle.levered_strategy_benchmark import (
    build_same_window_buy_and_hold_benchmark,
)


def _materialized() -> dict[str, object]:
    return {
        "sessions": [
            {"as_of": "2026-01-02T00:00:00+00:00", "prices": {"SOXX": 100.0}},
            {"as_of": "2026-01-05T00:00:00+00:00", "prices": {"SOXX": 120.0}},
            {"as_of": "2026-01-06T00:00:00+00:00", "prices": {"SOXX": 90.0}},
            {"as_of": "2026-01-07T00:00:00+00:00", "prices": {"SOXX": 108.0}},
        ]
    }


def test_v5_summary_binds_every_costed_run_to_same_window_soxx(monkeypatch) -> None:
    materialized = _materialized()
    plan = {
        "requests": [
            {
                "window_id": "fixed_fold_1",
                "window_kind": "fixed_fold",
                "cost_bps": 5,
                "session_dates": ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"],
            }
        ]
    }
    monkeypatch.setattr(evidence, "build_soxl_core_only_free_split_close_p3_evidence_plan", lambda *_args, **_kwargs: plan)
    benchmark = build_same_window_buy_and_hold_benchmark(materialized["sessions"], symbol="SOXX")
    base = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-summary.v1",
        "status": "SUCCESS",
        "p1_identity": {},
        "p2_identity": {},
        "materialized_input_sha256": "a" * 64,
        "evidence_plan_sha256": "b" * 64,
        "execution_identity": {},
        "runs": [
            {
                "window_id": "fixed_fold_1",
                "window_kind": "fixed_fold",
                "cost_bps": 5,
                "replay_input_sha256": "c" * 64,
                "metrics": {
                    "max_drawdown": float(benchmark["max_drawdown"]) - 0.01,
                    "calmar": float(benchmark["calmar"]) + 0.01,
                },
            }
        ],
    }
    base["evidence_summary_sha256"] = evidence._sha256(base)
    monkeypatch.setattr(
        evidence,
        "build_soxl_core_only_free_split_close_p3_evidence_summary",
        lambda **_kwargs: base,
    )

    result = evidence.build_soxl_core_only_v5_longterm_drawdown_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=lambda _input: {},
    )

    assert result["runs"][0]["benchmark"]["benchmark_symbol"] == "SOXX"
    assert result["runs"][0]["longterm_compounding_gate"]["passed"] is True
    assert result["longterm_compounding_gate"] == {
        "benchmark_symbol": "SOXX",
        "benchmark_policy": "buy_and_hold_unlevered_same_assured_close_series",
        "strategy_max_drawdown_must_not_exceed_benchmark": True,
        "require_incremental_calmar_after_cost": True,
        "all_fixed_folds_and_cost_scenarios_passed": True,
        "automatic_promotion": False,
    }
