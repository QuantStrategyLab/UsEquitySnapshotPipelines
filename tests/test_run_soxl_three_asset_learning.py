from __future__ import annotations

import importlib.util
import json
import math
import statistics
from datetime import date
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_three_asset_learning.py"
P2_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v3.json"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_three_asset_learning", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _materialized() -> dict[str, object]:
    def session(day: int, *, soxl: float, soxx: float) -> dict[str, object]:
        return {
            "as_of": f"2025-07-{day:02d}T00:00:00+00:00",
            "market_data": {"derived_indicators": {
                "SOXL": {"price": soxl, "ma_trend": 100.0},
                "SOXX": {
                    "price": soxx, "ma_trend": 100.0, "ma20": 100.0,
                    "ma20_slope": 1.0, "rsi14": 50.0, "bb_upper": 120.0,
                    "realized_volatility_10": 0.2,
                    "realized_volatility_10_dynamic_threshold": 0.5,
                    "realized_volatility_10_dynamic_sample_count": 252.0,
                },
            }},
            "prices": {"SOXL": soxl, "SOXX": soxx, "BOXX": 100.0},
        }

    payload: dict[str, object] = {
        "schema_version": "qsl.soxl-core-only-p3-materialized-input.v1",
        "p1_identity": {
            "input_manifest_sha256": "a" * 64, "binding_sha256": "b" * 64,
            "bars_member_sha256": "c" * 64, "date_cutoff": "2026-09-08",
        },
        "p2_identity": {
            "candidate_id": "soxl_soxx_core_only_p2_v3",
            "config_sha256": "ff8fa0acf4f175a7c40c3e1e6a3304ea2748b6b81c3797342085a4df3810ab4d",
        },
        "indicator_spec": {"id": "test"},
        "sessions": [
            session(28, soxl=100.0, soxx=107.0),
            session(29, soxl=105.0, soxx=107.0),
            session(30, soxl=110.0, soxx=107.0),
            session(31, soxl=115.0, soxx=107.0),
            session(31, soxl=999.0, soxx=999.0) | {"as_of": "2025-08-01T00:00:00+00:00"},
        ],
    }
    module = _module()
    payload["materialized_input_sha256"] = module._sha256(payload)
    return payload


def test_build_learning_requests_binds_dev_cutoff_and_finite_single_parameter() -> None:
    module = _module()
    requests = module.build_learning_requests(
        _materialized(), mid_soxl_weights=(0.60, 0.65), initial_equity=100_000.0
    )

    assert len(requests) == 6
    assert {request["cost_bps"] for request in requests} == {5.0, 10.0, 15.0}
    assert all(request["sessions"][-1]["as_of"].startswith("2025-07-31") for request in requests)
    assert all(set(request["parameter_override"]) == {"blend_gate_mid_soxl_weight"} for request in requests)
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.build_learning_requests(_materialized(), mid_soxl_weights=(0.4, 0.5, 0.6, 0.65))
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.build_learning_requests(_materialized(), mid_soxl_weights=(0.66,))
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.build_learning_requests(_materialized(), mid_soxl_weights=(0.60,))


def test_learning_uses_isolated_results_and_never_claims_promotion() -> None:
    module = _module()
    calls: list[dict[str, object]] = []

    def execute(request):
        calls.append(request)
        value = request["parameter_override"]["blend_gate_mid_soxl_weight"]
        cost = request["cost_bps"]
        result = {
            "schema_version": module.LEARNING_REPLAY_RESULT_SCHEMA,
            "status": "SUCCESS",
            "parameter_override": request["parameter_override"],
            "cost_bps": cost,
            "backtest_result": {
                "strategy_profile": module.LEARNING_PROFILE,
                "total_return": value - cost / 10_000.0,
                "observation_count": 3,
            },
        }
        result["output_sha256"] = module._sha256(result)
        return result

    result = module.run_learning(
        materialized=_materialized(), mid_soxl_weights=(0.60, 0.65), execute=execute
    )

    assert len(calls) == 6
    assert result["status"] == "SUCCESS"
    assert result["learning_only"] is True
    assert result["no_order"] is True
    assert result["size_zero_required"] is True
    assert result["promotion_eligible"] is False
    assert result["research_executed"] is True
    assert result["p1_identity"]["input_manifest_sha256"] == "a" * 64
    assert "selected" not in json.dumps(result).lower()

    def mismatched(request):
        value = execute(request)
        value["cost_bps"] = 15.0 if request["cost_bps"] != 15.0 else 5.0
        material = {key: item for key, item in value.items() if key != "output_sha256"}
        value["output_sha256"] = module._sha256(material)
        return value

    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_learning(
            materialized=_materialized(),
            mid_soxl_weights=(0.60, 0.65),
            execute=mismatched,
        )


def test_learning_rejects_a_single_return_interval() -> None:
    module = _module()
    materialized = _materialized()
    materialized["sessions"] = materialized["sessions"][:2]
    materialized.pop("materialized_input_sha256")
    materialized["materialized_input_sha256"] = module._sha256(materialized)

    with pytest.raises(module.SoxlThreeAssetLearningError, match="development input unavailable"):
        module.build_learning_requests(materialized, mid_soxl_weights=(0.65,))
    with pytest.raises(module.SoxlThreeAssetLearningError, match="learning result unavailable"):
        module._backtest_result({"decisions": [{}, {}]}, parameter=0.65)


def test_backtest_metrics_match_positive_drawdown_and_sample_stddev() -> None:
    module = _module()
    replay = {
        "initial_equity": 100.0,
        "final_equity": 94.5,
        "cost_bps": 5.0,
        "decisions": [
            {"signal_as_of": "2025-07-28T00:00:00+00:00", "equity_before_signal": 100.0},
            {"signal_as_of": "2025-07-29T00:00:00+00:00", "equity_before_signal": 90.0},
            {"signal_as_of": "2025-07-30T00:00:00+00:00", "equity_before_signal": 94.5},
        ],
    }

    result = module._backtest_result(replay, parameter=0.65)

    returns = (-0.10, 0.05)
    assert result.max_drawdown == pytest.approx(0.10)
    assert result.volatility == pytest.approx(statistics.stdev(returns) * math.sqrt(252.0))
    assert result.sharpe_ratio == pytest.approx(
        statistics.mean(returns) / statistics.stdev(returns) * math.sqrt(252.0)
    )
    assert result.cagr == pytest.approx(0.945 ** 126 - 1.0)


def test_source_learning_changes_real_mid_tier_plan_and_computes_metrics() -> None:
    module = _module()
    requests = module.build_learning_requests(
        _materialized(), mid_soxl_weights=(0.60, 0.65)
    )
    candidate = json.loads(P2_CANDIDATE.read_text(encoding="utf-8"))

    candidate_result = module._source_learning_replay(requests[0], candidate)
    baseline_result = module._source_learning_replay(requests[3], candidate)

    assert candidate_result["backtest_result"]["params"] == {
        "blend_gate_mid_soxl_weight": 0.60
    }
    assert baseline_result["backtest_result"]["params"] == {
        "blend_gate_mid_soxl_weight": 0.65
    }
    assert candidate_result["backtest_result"]["total_return"] != pytest.approx(
        baseline_result["backtest_result"]["total_return"]
    )
    assert candidate_result["backtest_result"]["observation_count"] == 3
    assert "replay" not in candidate_result


def test_cli_failure_is_sanitized_and_nonzero(capsys) -> None:
    module = _module()
    code = module.main(["--source-learning-replay", "missing.json", "--p2-candidate", str(P2_CANDIDATE)])
    output = json.loads(capsys.readouterr().out)
    assert code == 2
    assert output == {
        "schema_version": module.LEARNING_SCHEMA,
        "status": "PARKED",
        "failure_class": "learning_input_or_runtime_unavailable",
    }


def _development_summary() -> dict[str, object]:
    rows = []
    values = {
        0.65: (0.62, 1.24, 0.33),
        0.60: (0.60, 1.23, 0.32),
        0.55: (0.59, 1.22, 0.31),
    }
    for weight, (cagr, sharpe, drawdown) in values.items():
        for cost in (5.0, 10.0, 15.0):
            rows.append({
                "parameter_override": {"blend_gate_mid_soxl_weight": weight},
                "cost_bps": cost,
                "output_sha256": "d" * 64,
                "backtest_result": {
                    "strategy_profile": "soxl_soxx_three_asset_mid_weight_learning_v1",
                    "start_date": "2023-01-03",
                    "end_date": "2025-07-31",
                    "observation_count": 645,
                    "cagr": cagr - cost / 10_000.0,
                    "sharpe_ratio": sharpe - cost / 10_000.0,
                    "max_drawdown": drawdown + cost / 100_000.0,
                    "volatility": 0.48,
                    "total_return": 2.0,
                },
            })
    return {
        "schema_version": "qsl.soxl-manual-learning-run.v1",
        "status": "accepted",
        "operation": "soxl_learning",
        "learning_only": True,
        "no_order": True,
        "size_zero_required": True,
        "promotion_eligible": False,
        "research_executed": True,
        "development_cutoff": "2025-07-31",
        "parameter_key": "blend_gate_mid_soxl_weight",
        "parameter_values": [0.65, 0.60, 0.55],
        "cost_bps": [5.0, 10.0, 15.0],
        "input_identity": {"manifest_sha256": "a" * 64, "member_count": 4},
        "numeric_execution": {"status": "succeeded"},
        "numeric_result_sha256": "e" * 64,
        "authority": {
            "actor": "Pigbibi",
            "event_name": "workflow_dispatch",
            "ref": "refs/heads/main",
            "repository": "QuantStrategyLab/AIAuditBridge",
            "run_attempt": 1,
            "run_id": "34376283866",
        },
        "numeric_source_identity": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": "7756fe32585e85cf1d09a163203a02e3eee39fe1",
            "quant_platform_kit_revision": "3acab1923a97b805b077c85c6c19657be0143bac",
            "uv_lock_sha256": "6c12df9b3412681829295f15de7e2ce7fc5b708d1de815f72d654fc16b7848e6",
        },
        "consumer_source": {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "revision": "b03ecbe4e0a7a0de22f298499f867a7039e4b60a",
        },
        "numeric_summary": rows,
    }


def _validation_materialized() -> dict[str, object]:
    module = _module()

    def session(value: str) -> dict[str, object]:
        return {
            "as_of": f"{value}T00:00:00+00:00",
            "market_data": {"derived_indicators": {}},
            "prices": {"SOXL": 100.0, "SOXX": 100.0, "BOXX": 100.0},
        }

    dates = (
        "2023-07-03", "2023-10-02", "2023-12-29",
        "2024-07-01", "2024-10-01", "2024-12-31",
        "2025-03-03", "2025-05-15", "2025-07-31",
        "2025-08-04", "2026-02-02", "2026-08-04",
    )
    payload: dict[str, object] = {
        "schema_version": "qsl.soxl-core-only-p3-materialized-input.v1",
        "p1_identity": {
            "input_manifest_sha256": "a" * 64,
            "binding_sha256": "b" * 64,
            "bars_member_sha256": "c" * 64,
            "date_cutoff": "2026-09-08",
        },
        "p2_identity": {
            "candidate_id": "soxl_soxx_core_only_p2_v3",
            "config_sha256": "ff8fa0acf4f175a7c40c3e1e6a3304ea2748b6b81c3797342085a4df3810ab4d",
        },
        "indicator_spec": {"id": "test"},
        "sessions": [session(value) for value in dates],
    }
    payload["materialized_input_sha256"] = module._sha256(payload)
    return payload


def test_validation_runs_fixed_candidate_through_qpk_promotion_backtests() -> None:
    module = _module()
    development_summary = _development_summary()
    module.DEVELOPMENT_SUMMARY_SHA256 = module._sha256(development_summary)
    calls = []

    def execute(request):
        calls.append(request)
        start = str(request["sessions"][0]["as_of"])[:10]
        end = str(request["sessions"][-1]["as_of"])[:10]
        weight = request["parameter_override"]["blend_gate_mid_soxl_weight"]
        result = {
            "schema_version": module.LEARNING_REPLAY_RESULT_SCHEMA,
            "status": "SUCCESS",
            "parameter_override": {"blend_gate_mid_soxl_weight": weight},
            "cost_bps": request["cost_bps"],
            "backtest_result": {
                "strategy_profile": module.LEARNING_PROFILE,
                "domain": "us_equity",
                "param_set_id": "",
                "params": {"blend_gate_mid_soxl_weight": weight},
                "sharpe_ratio": 1.0 if weight == 0.55 else 1.1,
                "max_drawdown": 0.25 if weight == 0.55 else 0.30,
                "cagr": 0.5 if weight == 0.55 else 0.55,
                "volatility": 0.4,
                "total_return": 0.8,
                "start_date": start,
                "end_date": end,
                "observation_count": len(request["sessions"]) - 1,
                "source_script": "run_soxl_three_asset_learning.py",
                "source_revision": "7756fe32585e85cf1d09a163203a02e3eee39fe1",
                "cost_model": f"all_in_per_side_{request['cost_bps']:g}bps",
                "cost_inputs": {"total_cost_bps": request["cost_bps"]},
            },
        }
        result["output_sha256"] = module._sha256(result)
        return result

    proposal, baseline_evidence, candidate_evidence = module.run_fixed_validation(
        materialized=_validation_materialized(),
        development_summary=development_summary,
        execute=execute,
    )

    assert proposal.current_params == {"blend_gate_mid_soxl_weight": 0.65}
    assert proposal.proposed_params == {"blend_gate_mid_soxl_weight": 0.55}
    assert proposal.recommendation == "research_candidate"
    assert proposal.walk_forward_passed is False
    assert "sha256:" in proposal.optimization_method
    assert len(baseline_evidence) == len(candidate_evidence) == 3
    assert len(calls) == 24
    assert {request["cost_bps"] for request in calls} == {5.0, 10.0, 15.0}
    assert {request["parameter_override"]["blend_gate_mid_soxl_weight"] for request in calls} == {0.55, 0.65}
    assert {
        (str(request["sessions"][0]["as_of"])[:10], str(request["sessions"][-1]["as_of"])[:10])
        for request in calls
    } == {
        ("2023-07-03", "2023-12-29"),
        ("2024-07-01", "2024-12-31"),
        ("2025-03-03", "2025-07-31"),
        ("2025-08-04", "2026-08-04"),
    }
    assert {run.locked_oos_start for run in candidate_evidence} == {date(2025, 8, 4)}
    assert {run.locked_oos_end for run in candidate_evidence} == {date(2026, 8, 4)}
    assert all(run.purge_days == 1 and run.embargo_days == 1 for run in candidate_evidence)
    assert all(len(run.folds) == 3 for run in candidate_evidence)
    summary_digest = module._sha256(development_summary)
    assert all(
        summary_digest in result.param_set_id
        for run in (*baseline_evidence, *candidate_evidence)
        for result in (*run.fold_results, run.locked_oos_result)
    )
    from quant_platform_kit.strategy_lifecycle.contracts import PromotionBacktestRun
    assert all(isinstance(run, PromotionBacktestRun) for run in candidate_evidence)
    assert all(
        result.validation_identity.protocol == "purged_walk_forward.v1"
        for run in candidate_evidence
        for result in (*run.fold_results, run.locked_oos_result)
    )
    for baseline, candidate in zip(baseline_evidence, candidate_evidence):
        for baseline_result, candidate_result in zip(
            (*baseline.fold_results, baseline.locked_oos_result),
            (*candidate.fold_results, candidate.locked_oos_result),
        ):
            assert candidate_result.max_drawdown < baseline_result.max_drawdown
            assert candidate_result.cagr < baseline_result.cagr
            assert candidate_result.sharpe_ratio < baseline_result.sharpe_ratio
            assert candidate_result.param_set_id.split("-")[0:4] == baseline_result.param_set_id.split("-")[0:4]
    output = module._validation_output(proposal, baseline_evidence, candidate_evidence)
    assert output["status"] == "PROMOTION_BACKTEST_RUNS_BUILT"
    assert output["stage"] == "promotion_validation"
    assert output["learning_only"] is True
    assert output["no_order"] is True
    assert output["size_zero_required"] is True
    assert output["promotion_eligible"] is False
    assert output["live_authority_granted"] is False
    assert "schema_version" not in output
    assert len(output["baseline_promotion_runs"]) == 3
    assert len(output["candidate_promotion_runs"]) == 3


def test_validation_rejects_unbound_or_changed_development_summary() -> None:
    module = _module()
    original = _development_summary()
    module.DEVELOPMENT_SUMMARY_SHA256 = module._sha256(original)
    mismatched = _development_summary()
    mismatched["input_identity"] = {"manifest_sha256": "f" * 64, "member_count": 4}
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(), development_summary=mismatched,
            execute=lambda request: {},
        )

    changed = _development_summary()
    changed["parameter_values"] = [0.65, 0.60]
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(), development_summary=changed,
            execute=lambda request: {},
        )


def test_validation_cli_is_explicit_and_failure_stays_non_promotable(capsys) -> None:
    module = _module()
    code = module.main([
        "--p1-binding", "missing-binding.json",
        "--input-manifest", "missing-manifest.json",
        "--bars-member", "missing-bars.json",
        "--ues-project", "missing-ues",
        "--p2-candidate", str(P2_CANDIDATE),
        "--promotion-validation-development-summary", "missing-summary.json",
    ])
    output = json.loads(capsys.readouterr().out)
    assert code == 2
    assert output == {
        "status": "PARKED",
        "stage": "promotion_validation",
        "failure_class": "validation_input_or_runtime_unavailable",
        "learning_only": True,
        "no_order": True,
        "size_zero_required": True,
        "promotion_eligible": False,
        "live_authority_granted": False,
    }


def test_validation_stops_on_first_invalid_numeric_result() -> None:
    module = _module()
    development_summary = _development_summary()
    module.DEVELOPMENT_SUMMARY_SHA256 = module._sha256(development_summary)
    calls = []

    def invalid_execute(request):
        calls.append(request)
        return {"status": "SUCCESS"}

    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(),
            development_summary=development_summary,
            execute=invalid_execute,
        )
    assert len(calls) == 1
