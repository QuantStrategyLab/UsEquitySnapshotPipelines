from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.lifecycle.relative_benchmark_terminal_summary import (
    RelativeBenchmarkTerminalSummaryError,
    build_relative_benchmark_terminal_summary,
    canonical_relative_benchmark_terminal_summary_bytes,
    validate_relative_benchmark_terminal_summary,
)


def _candidate() -> dict[str, str]:
    return {
        "candidate_id": "tqqq_core_only_p2_v8_free_ohlcv_relative_benchmark",
        "config_sha256": "a" * 64,
    }


def _p3_result() -> dict[str, str]:
    return {
        "evidence_sha256": "1" * 64,
        "promotion_result_sha256": "2" * 64,
        "relative_benchmark_policy_sha256": "3" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
    }


def _policy() -> dict[str, object]:
    short_gate = {
        "max_drawdown_not_exceeding_benchmark": False,
        "passed": False,
    }
    long_gate = {
        "max_drawdown_not_exceeding_benchmark": True,
        "incremental_calmar_after_cost": False,
        "passed": False,
    }
    return {
        "schema_version": "qsl.tqqq-p2-v7-relative-benchmark-policy.v1",
        "candidate_id": _candidate()["candidate_id"],
        "benchmark_symbol": "QQQ",
        "benchmark_policy": "buy_and_hold_unlevered_same_assured_close_series",
        "evidence_status": "EVIDENCE_COMPLETE",
        "short_window_drawdown_all_passed": False,
        "long_window_drawdown_all_passed": True,
        "long_window_incremental_calmar_all_passed": False,
        "forward_confirmation_satisfied": False,
        "strategy_verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        "automatic_promotion": False,
        "scenarios": [
            {
                "total_cost_bps": 5,
                "short_windows": [
                    {"start": "private", "end": "private", "gate": short_gate},
                    {"start": "private", "end": "private", "gate": short_gate},
                ],
                "continuous_long_horizon": {
                    "start": "private",
                    "end": "private",
                    "gate": long_gate,
                },
            }
        ],
    }


def test_terminal_summary_contains_only_gates_and_never_private_window_material() -> None:
    summary = build_relative_benchmark_terminal_summary(
        candidate=_candidate(),
        input_manifest_sha256="4" * 64,
        p3_result=_p3_result(),
        relative_benchmark_policy=_policy(),
    )

    assert validate_relative_benchmark_terminal_summary(summary) == summary
    assert summary["cost_scenarios"] == [
        {
            "total_cost_bps": 5,
            "short_window_drawdown_failed_count": 2,
            "long_window_drawdown_passed": True,
            "long_window_incremental_calmar_passed": False,
            "long_window_passed": False,
        }
    ]
    encoded = canonical_relative_benchmark_terminal_summary_bytes(summary).decode("utf-8")
    assert "private" not in encoded
    assert '"start"' not in encoded
    assert '"end"' not in encoded
    assert json.loads(encoded)["p3"]["verdict"] == "REJECT_NEGATIVE_STRATEGY_EVIDENCE"


def test_terminal_summary_rejects_a_policy_for_another_candidate() -> None:
    policy = _policy()
    policy["candidate_id"] = "tqqq_core_only_p2_v7_relative_benchmark"

    with pytest.raises(RelativeBenchmarkTerminalSummaryError):
        build_relative_benchmark_terminal_summary(
            candidate=_candidate(),
            input_manifest_sha256="4" * 64,
            p3_result=_p3_result(),
            relative_benchmark_policy=policy,
        )


def test_terminal_summary_rejects_an_aggregate_that_disagrees_with_its_gates() -> None:
    policy = _policy()
    policy["short_window_drawdown_all_passed"] = True

    with pytest.raises(RelativeBenchmarkTerminalSummaryError):
        build_relative_benchmark_terminal_summary(
            candidate=_candidate(),
            input_manifest_sha256="4" * 64,
            p3_result=_p3_result(),
            relative_benchmark_policy=policy,
        )
