from __future__ import annotations

import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_p3_strategy_performance as performance
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import P2_V3_CONTRACT

NOW = "2026-08-21T03:00:00Z"
PRODUCER_REVISION = "a" * 40
STRATEGY_REVISION = "b" * 40
MANIFEST = "c" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _summary() -> dict[str, object]:
    result: dict[str, object] = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-summary.v1",
        "status": "SUCCESS",
        "p1_identity": {
            "input_manifest_sha256": MANIFEST,
            "binding_sha256": "d" * 64,
            "bars_member_sha256": "e" * 64,
            "date_cutoff": "2026-08-20",
        },
        "p2_identity": {
            "candidate_id": P2_V3_CONTRACT.candidate_id,
            "config_sha256": P2_V3_CONTRACT.config_sha256,
        },
        "materialized_input_sha256": "f" * 64,
        "evidence_plan_sha256": "1" * 64,
        "execution_identity": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": STRATEGY_REVISION,
            "quant_platform_kit_revision": "3acab1923a97b805b077c85c6c19657be0143bac",
            "uv_lock_sha256": "2" * 64,
        },
        "runs": [
            {
                "window_id": "trailing_252_xnys_session_oos",
                "window_kind": "rolling_locked_oos",
                "cost_bps": 10,
                "replay_input_sha256": "3" * 64,
                "metrics": {
                    "initial_equity": 100_000.0,
                    "final_equity": 110_000.0,
                    "net_return": 0.1,
                    "max_drawdown": 0.2,
                    "one_way_turnover": 0.4,
                    "cost_total": 123.0,
                    "executed_signal_count": 251,
                    "unexecuted_final_signal": True,
                    "replay_result_sha256": "4" * 64,
                    "sharpe": 1.2,
                    "cagr": 0.1,
                    "calmar": 0.5,
                    "win_rate": 0.55,
                },
            }
        ],
    }
    result["evidence_summary_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result


def _build(**overrides: object) -> dict[str, object]:
    summary = _summary()
    summary.update(overrides)
    return performance.build_soxl_p3_strategy_performance(
        evidence_summary=summary,
        expected_evidence_sha256=summary["evidence_summary_sha256"],
        producer_revision=PRODUCER_REVISION,
        computed_at=NOW,
    )


def test_projects_only_the_predeclared_trailing_oos_10bps_metrics() -> None:
    payload = _build()

    assert payload == {
        "schema_version": "strategy_performance.v2",
        "metrics_kind": "performance",
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "strategy_profile": "soxl_soxx_core_only_p2_v3",
        "candidate_kind": "individual",
        "domain": "us_equity",
        "generated_at": NOW,
        "as_of": "2026-08-20",
        "current_metrics": {
            "sharpe": 1.2,
            "cagr": 0.1,
            "calmar": 0.5,
            "win_rate": 0.55,
            "max_dd": 0.2,
        },
        "evidence": {
            "p1_input_digest": MANIFEST,
            "p2_config_digest": P2_V3_CONTRACT.config_sha256,
            "p3_evidence_id": _summary()["evidence_summary_sha256"],
            "strategy_revision": STRATEGY_REVISION,
            "producer_revision": PRODUCER_REVISION,
        },
        "lifecycle": {"stage": "P3", "status": "verified"},
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }
    encoded = performance.canonical_soxl_p3_strategy_performance_bytes(payload).decode("utf-8")
    for forbidden in ("bars", "account", "broker", "path"):
        assert forbidden not in encoded


def test_rejects_any_summary_digest_or_fixed_oos_selection_drift() -> None:
    summary = _summary()
    with pytest.raises(performance.SoxlP3StrategyPerformanceError, match="invalid SOXL P3 evidence summary"):
        performance.build_soxl_p3_strategy_performance(
            evidence_summary=summary,
            expected_evidence_sha256="0" * 64,
            producer_revision=PRODUCER_REVISION,
            computed_at=NOW,
        )

    drifted = _summary()
    run = dict(drifted["runs"][0])
    run["cost_bps"] = 5
    drifted["runs"] = [run]
    drifted["evidence_summary_sha256"] = hashlib.sha256(
        _canonical({key: value for key, value in drifted.items() if key != "evidence_summary_sha256"})
    ).hexdigest()
    with pytest.raises(performance.SoxlP3StrategyPerformanceError, match="fixed trailing OOS"):
        performance.build_soxl_p3_strategy_performance(
            evidence_summary=drifted,
            expected_evidence_sha256=drifted["evidence_summary_sha256"],
            producer_revision=PRODUCER_REVISION,
            computed_at=NOW,
        )
