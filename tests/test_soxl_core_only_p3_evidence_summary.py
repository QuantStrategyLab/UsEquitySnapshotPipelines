from __future__ import annotations

import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p3_evidence_summary as summary
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v2_contract import P2_V2_CONTRACT
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p3_input_materializer import (
    MATERIALIZED_INPUT_SCHEMA,
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _materialized() -> dict[str, object]:
    return {
        "schema_version": MATERIALIZED_INPUT_SCHEMA,
        "sessions": [
            {
                "as_of": "2026-08-03T00:00:00+00:00",
                "market_data": {"derived_indicators": {}},
                "prices": {"SOXL": 1.0, "SOXX": 1.0, "BOXX": 1.0},
            },
            {
                "as_of": "2026-08-04T00:00:00+00:00",
                "market_data": {"derived_indicators": {}},
                "prices": {"SOXL": 1.0, "SOXX": 1.0, "BOXX": 1.0},
            },
        ],
    }


def _plan() -> dict[str, object]:
    result = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-plan.v1",
        "p1_identity": {"input_manifest_sha256": "a" * 64},
        "p2_identity": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "materialized_input_sha256": "c" * 64,
        "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        "purge_sessions": 1,
        "cost_bps": [5, 10, 15],
        "requests": [
            {
                "window_id": "fold",
                "window_kind": "purged_sequential_evidence",
                "session_dates": ["2026-08-03", "2026-08-04"],
                "cost_bps": 5,
            }
        ],
    }
    result["evidence_plan_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result


def _isolated_result(cost_bps: int) -> dict[str, object]:
    replay = {
        "schema_version": "qsl.soxl-core-only-p3-stateful-replay-result.v1",
        "cost_bps": cost_bps,
        "initial_equity": 100_000.0,
        "final_equity": 101_000.0,
        "cost_total": 25.0,
        "one_way_turnover": 0.5,
        "executed_signal_count": 1,
        "unexecuted_final_signal": True,
        "decisions": [
            {"equity_before_signal": 100_000.0},
            {"equity_before_signal": 99_000.0},
            {"equity_before_signal": 100_500.0},
        ],
    }
    replay["output_sha256"] = hashlib.sha256(_canonical(replay)).hexdigest()
    result = {
        "schema_version": "qsl.soxl-core-only-p3-isolated-replay-result.v1",
        "status": "SUCCESS",
        "execution_identity": {"repository": "QuantStrategyLab/UsEquityStrategies", "revision": "d" * 40},
        "p2_identity": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "replay": replay,
    }
    result["result_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result


def test_summary_executes_only_the_fixed_plan_and_keeps_metrics_only(monkeypatch) -> None:
    materialized = _materialized()
    expected_plan = _plan()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(summary, "build_soxl_core_only_p3_evidence_plan", lambda value: expected_plan)

    def execute(replay_input: dict[str, object]) -> dict[str, object]:
        calls.append(replay_input)
        return _isolated_result(int(replay_input["cost_bps"]))

    result = summary.build_soxl_core_only_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=expected_plan,
        replay_executor=execute,
    )

    assert len(calls) == 1
    assert calls[0]["schema_version"] == "qsl.soxl-core-only-p3-stateful-replay-input.v1"
    assert result["status"] == "SUCCESS"
    assert result["runs"][0]["metrics"]["net_return"] == pytest.approx(0.01)
    assert result["runs"][0]["metrics"]["max_drawdown"] == pytest.approx(0.01)
    assert result["runs"][0]["metrics"]["win_rate"] == pytest.approx(2 / 3)
    assert result["runs"][0]["metrics"]["calmar"] > 0.0
    assert "sessions" not in result["runs"][0]
    assert "market_data" not in json.dumps(result)


def test_summary_rejects_a_changed_plan_before_invoking_executor(monkeypatch) -> None:
    expected_plan = _plan()
    monkeypatch.setattr(summary, "build_soxl_core_only_p3_evidence_plan", lambda value: expected_plan)
    changed = {**expected_plan, "cost_bps": [5]}
    with pytest.raises(summary.SoxlCoreOnlyP3EvidenceSummaryError):
        summary.build_soxl_core_only_p3_evidence_summary(
            materialized=_materialized(),
            evidence_plan=changed,
            replay_executor=lambda value: _isolated_result(5),
        )
