from __future__ import annotations

import copy
import importlib.util
import json
import math
import os
import statistics
import subprocess
import sys
from collections.abc import Mapping
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
    assert "initial_equity" not in result
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


def test_attribution_requests_are_fixed_and_aggregate_results_do_not_leak_daily_data() -> None:
    module = _module()
    requests = module.build_attribution_requests(_materialized())
    assert [(item["variant"], item["cost_bps"]) for item in requests] == [
        (variant, cost)
        for variant in ("baseline_mid_065", "soxx_buy_hold", "fixed_full_weights")
        for cost in (5.0, 10.0, 15.0)
    ]
    assert all(item["initial_equity"] == 100_000.0 for item in requests)
    assert all(item["sessions"][-1]["as_of"].startswith("2025-07-31") for item in requests)
    calls = []

    def execute(request):
        calls.append(request)
        result = {
            "schema_version": module.ATTRIBUTION_REPLAY_RESULT_SCHEMA,
            "status": "SUCCESS",
            "variant": request["variant"],
            "cost_bps": request["cost_bps"],
            "initial_equity": 100_000.0,
            "final_equity": 101_000.0,
            "total_return": 0.01,
            "max_drawdown": 0.02,
            "cost_total": 10.0,
            "one_way_turnover": 0.2,
            "asset_pnl_usd": {"SOXL": 500.0, "SOXX": 400.0, "BOXX": 110.0},
            "cash_pnl_usd": 0.0,
            "external_flow_usd": 0.0,
            "reconciliation_residual_usd": 0.0,
            "contribution_pct_points": {
                "SOXL": 0.5, "SOXX": 0.4, "BOXX": 0.11, "cash": 0.0,
                "execution_cost": -0.01,
            },
            "start_date": "2025-07-28",
            "end_date": "2025-07-31",
            "observation_count": 3,
            "unexecuted_final_signal": True,
        }
        result["output_sha256"] = module._sha256(result)
        return result

    result = module.run_attribution(materialized=_materialized(), execute=execute)

    assert len(calls) == 9
    assert result["schema_version"] == "qsl.soxl-three-asset-attribution.v1"
    assert result["study_kind"] == "retrospective_research"
    assert result["learning_only"] is True
    assert result["no_order"] is True
    assert result["size_zero_required"] is True
    assert result["promotion_eligible"] is False
    assert result["live_ready"] is False
    assert result["live_authority_granted"] is False
    assert result["initial_equity"] == 100_000.0
    assert result["price_basis"] == "verified_p1_adjusted_close_as_materialized"
    assert result["cash_interest_assumption"] == "zero"
    assert result["external_flow_assumption"] == "zero"
    assert result["execution_cost_basis"] == "original_simulated_all_in_per_side"
    assert result["additional_dividend_or_fund_expense_adjustment"] is False
    assert result["causal_attribution_claimed"] is False
    assert result["p1_identity"]["input_manifest_sha256"] == "a" * 64
    serialized = json.dumps(result, sort_keys=True)
    for forbidden in ('"sessions"', '"prices"', '"decisions"', '"positions"'):
        assert forbidden not in serialized
    material = {key: value for key, value in result.items() if key != "result_sha256"}
    assert result["result_sha256"] == module._sha256(material)


def test_volatility_ablation_is_exactly_baseline_on_off_at_10bps() -> None:
    module = _module()
    materialized = _materialized()
    requests = module.build_volatility_ablation_requests(materialized)
    original_baseline = module.build_attribution_requests(materialized)[1]

    assert requests == [
        original_baseline,
        original_baseline | {"variant": "baseline_without_volatility_delever"},
    ]
    calls = []

    def execute(request):
        calls.append(request)
        result = {
            "schema_version": module.ATTRIBUTION_REPLAY_RESULT_SCHEMA,
            "status": "SUCCESS",
            "variant": request["variant"],
            "cost_bps": request["cost_bps"],
            "initial_equity": 100_000.0,
            "final_equity": 101_000.0,
            "total_return": 0.01,
            "max_drawdown": 0.02,
            "cost_total": 10.0,
            "one_way_turnover": 0.2,
            "asset_pnl_usd": {"SOXL": 500.0, "SOXX": 400.0, "BOXX": 110.0},
            "cash_pnl_usd": 0.0,
            "external_flow_usd": 0.0,
            "reconciliation_residual_usd": 0.0,
            "contribution_pct_points": {
                "SOXL": 0.5, "SOXX": 0.4, "BOXX": 0.11, "cash": 0.0,
                "execution_cost": -0.01,
            },
            "start_date": "2025-07-28",
            "end_date": "2025-07-31",
            "observation_count": 3,
            "unexecuted_final_signal": True,
        }
        result["output_sha256"] = module._sha256(result)
        return result

    summary = module.run_volatility_ablation(
        materialized=materialized,
        execute=execute,
    )

    assert calls == requests
    assert summary["schema_version"] == module.ATTRIBUTION_SCHEMA
    assert summary["study_variant"] == "volatility_delever_on_off_v1"
    assert summary["variants"] == [
        "baseline_mid_065",
        "baseline_without_volatility_delever",
    ]
    assert summary["cost_bps"] == [10.0]
    assert summary["causal_attribution_claimed"] is False
    assert summary["source_config_reference"] == materialized["p2_identity"]
    assert summary["source_identity"]["revision"] == "7756fe32585e85cf1d09a163203a02e3eee39fe1"
    serialized = json.dumps(summary, sort_keys=True)
    for forbidden in ('"sessions"', '"prices"', '"decisions"', '"positions"'):
        assert forbidden not in serialized


def test_source_volatility_ablation_reuses_baseline_and_changes_only_delever_switch() -> None:
    module = _module()
    materialized = _materialized()
    for session in materialized["sessions"]:
        session["market_data"]["derived_indicators"]["SOXX"]["realized_volatility_10"] = 0.8
    materialized.pop("materialized_input_sha256")
    materialized["materialized_input_sha256"] = module._sha256(materialized)
    candidate = json.loads(P2_CANDIDATE.read_text(encoding="utf-8"))
    requests = module.build_volatility_ablation_requests(materialized)

    baseline = module._source_attribution_replay(requests[0], candidate)
    without_delever = module._source_attribution_replay(requests[1], candidate)
    original_baseline = module._source_attribution_replay(
        module.build_attribution_requests(materialized)[1],
        candidate,
    )

    assert baseline == original_baseline
    assert without_delever["variant"] == "baseline_without_volatility_delever"
    assert without_delever["final_equity"] != pytest.approx(baseline["final_equity"])
    assert abs(without_delever["reconciliation_residual_usd"]) <= 1e-7
    with pytest.raises(module.SoxlThreeAssetLearningError, match="invalid attribution input"):
        module._source_attribution_replay(requests[1] | {"cost_bps": 5.0}, candidate)


def test_source_attribution_variants_use_one_engine_and_soxx_buy_hold_does_not_rebalance() -> None:
    module = _module()
    requests = module.build_attribution_requests(_materialized())
    candidate = json.loads(P2_CANDIDATE.read_text(encoding="utf-8"))

    results = [module._source_attribution_replay(requests[index], candidate) for index in (0, 3, 6)]

    assert [result["variant"] for result in results] == list(module.ATTRIBUTION_VARIANTS)
    assert results[1]["one_way_turnover"] == pytest.approx(0.97)
    assert all(abs(result["reconciliation_residual_usd"]) <= 1e-7 for result in results)
    assert all(result["unexecuted_final_signal"] is True for result in results)


def test_isolated_attribution_uses_pinned_source_gate_and_internal_mode(
    monkeypatch, tmp_path
) -> None:
    module = _module()
    project = tmp_path / "ues"
    project.mkdir()
    candidate_path = tmp_path / "candidate.json"
    candidate = json.loads(P2_CANDIDATE.read_text(encoding="utf-8"))
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    validations = []

    class Isolated:
        @staticmethod
        def validate_ues_project(value):
            validations.append(("ues", value))

        @staticmethod
        def validate_p2_candidate(value):
            validations.append(("candidate", value))

    commands = []

    def run(command, **kwargs):
        commands.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, '{"status":"SUCCESS"}', "")

    monkeypatch.setattr(module, "_load_isolated_module", lambda: Isolated)
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/uv")
    monkeypatch.setattr(module.subprocess, "run", run)

    assert module.run_isolated_attribution_request(
        {"schema_version": module.ATTRIBUTION_REPLAY_SCHEMA},
        ues_project=project,
        p2_candidate_path=candidate_path,
    ) == {"status": "SUCCESS"}
    assert validations == [("ues", project), ("candidate", candidate)]
    command, kwargs = commands[0]
    assert "--source-attribution-replay" in command
    assert "--attribution" not in command
    assert command[:5] == ("uv", "run", "--locked", "--project", str(project))
    assert kwargs == {
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": 120,
    }


def test_attribution_stops_on_first_nonfinite_result() -> None:
    module = _module()
    calls = []

    def execute(request):
        calls.append(request)
        return {
            "schema_version": module.ATTRIBUTION_REPLAY_RESULT_SCHEMA,
            "status": "SUCCESS",
            "variant": request["variant"],
            "cost_bps": request["cost_bps"],
            "initial_equity": 100_000.0,
            "final_equity": math.nan,
        }

    with pytest.raises(module.SoxlThreeAssetLearningError, match="attribution result unavailable"):
        module.run_attribution(materialized=_materialized(), execute=execute)
    assert len(calls) == 1


def test_attribution_cli_requires_fixed_outer_mode_and_rejects_learning_parameters(
    monkeypatch, tmp_path, capsys
) -> None:
    module = _module()
    paths = {}
    for name, content in {
        "binding": {}, "manifest": {}, "bars": "bars", "candidate": {},
    }.items():
        path = tmp_path / name
        path.write_text(json.dumps(content) if isinstance(content, dict) else content, encoding="utf-8")
        paths[name] = path
    project = tmp_path / "ues"
    project.mkdir()
    expected = {"schema_version": module.ATTRIBUTION_SCHEMA, "status": "SUCCESS"}
    captured = {}

    def run(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(module, "run_attribution_from_verified_p1", run)
    base = [
        "--p1-binding", str(paths["binding"]),
        "--input-manifest", str(paths["manifest"]),
        "--bars-member", str(paths["bars"]),
        "--ues-project", str(project),
        "--p2-candidate", str(paths["candidate"]),
        "--attribution",
    ]
    assert module.main(base) == 0
    assert json.loads(capsys.readouterr().out) == expected
    assert set(captured) == {"binding", "manifest", "member_bytes", "ues_project", "p2_candidate_path"}

    assert module.main([*base, "--blend-gate-mid-soxl-weight", "0.65"]) == 2
    parked = json.loads(capsys.readouterr().out)
    assert parked["schema_version"] == module.ATTRIBUTION_SCHEMA
    assert parked["status"] == "PARKED"
    assert parked["no_order"] is True


def test_volatility_ablation_cli_is_explicit_and_rejects_other_study_modes(
    monkeypatch, tmp_path, capsys
) -> None:
    module = _module()
    paths = {}
    for name, content in {
        "binding": {}, "manifest": {}, "bars": "bars", "candidate": {},
    }.items():
        path = tmp_path / name
        path.write_text(json.dumps(content) if isinstance(content, dict) else content, encoding="utf-8")
        paths[name] = path
    project = tmp_path / "ues"
    project.mkdir()
    expected = {
        "schema_version": module.ATTRIBUTION_SCHEMA,
        "status": "SUCCESS",
        "study_variant": module.VOLATILITY_ABLATION_STUDY_VARIANT,
    }
    captured = {}

    def run(**kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(module, "run_volatility_ablation_from_verified_p1", run)
    base = [
        "--p1-binding", str(paths["binding"]),
        "--input-manifest", str(paths["manifest"]),
        "--bars-member", str(paths["bars"]),
        "--ues-project", str(project),
        "--p2-candidate", str(paths["candidate"]),
        "--volatility-ablation",
    ]
    assert module.main(base) == 0
    assert json.loads(capsys.readouterr().out) == expected
    assert set(captured) == {"binding", "manifest", "member_bytes", "ues_project", "p2_candidate_path"}

    with pytest.raises(SystemExit):
        module.main([*base, "--attribution"])
    assert module.main([*base, "--blend-gate-mid-soxl-weight", "0.65"]) == 2
    parked = json.loads(capsys.readouterr().out)
    assert parked["schema_version"] == module.ATTRIBUTION_SCHEMA
    assert parked["study_variant"] == module.VOLATILITY_ABLATION_STUDY_VARIANT
    assert parked["status"] == "PARKED"


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


def _watcher_development_summary() -> dict[str, object]:
    summary = _development_summary()
    summary.pop("authority")
    summary.update({
        "operation": "soxl_watcher_learning",
        "source": "watcher_event_independent_learning",
        "task_id": "watcher-123456789abc",
        "task_sha256": "f" * 64,
        "experiment": {
            "parameter_bounds_sha256": "cd48b224d6c4a28100d3de9c226dc9cff927bfbb0cd72b528ea157b55089a2be",
        },
    })
    return summary


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


@pytest.mark.parametrize("watcher", (False, True))
def test_validation_runs_fixed_candidate_through_qpk_promotion_backtests(watcher: bool) -> None:
    module = _module()
    development_summary = _watcher_development_summary() if watcher else _development_summary()
    watcher_summary_sha256 = module._sha256(development_summary) if watcher else None
    if not watcher:
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
        watcher_development_summary_sha256=watcher_summary_sha256,
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


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("operation",), "soxl_learning"),
        (("status",), "parked"),
        (("task_id",), "watcher-invalid"),
        (("task_sha256",), ""),
        (("no_order",), False),
        (("parameter_values",), [0.65, 0.55]),
        (("cost_bps",), [5.0, 10.0]),
        (("development_cutoff",), "2025-08-01"),
        (("input_identity", "manifest_sha256"), "b" * 64),
        (("numeric_summary",), None),
        (("numeric_source_identity",), None),
        (("numeric_result_sha256",), ""),
    ),
)
def test_watcher_validation_rejects_changed_contract_fields(
    path: tuple[str, ...], value: object,
) -> None:
    module = _module()
    summary = _watcher_development_summary()
    target = summary
    for key in path[:-1]:
        target = target[key]  # type: ignore[assignment,index]
    target[path[-1]] = value

    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(),
            development_summary=summary,
            watcher_development_summary_sha256=module._sha256(summary),
            execute=lambda request: {},
        )


@pytest.mark.parametrize("digest", ("", "0" * 64, "A" * 64))
def test_watcher_validation_rejects_empty_or_mismatched_digest(digest: str) -> None:
    module = _module()
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(),
            development_summary=_watcher_development_summary(),
            watcher_development_summary_sha256=digest,
            execute=lambda request: {},
        )


def test_watcher_validation_rejects_changed_numeric_trial_grid() -> None:
    module = _module()
    summary = _watcher_development_summary()
    summary["numeric_summary"][1]["cost_bps"] = 15.0  # type: ignore[index]
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(),
            development_summary=summary,
            watcher_development_summary_sha256=module._sha256(summary),
            execute=lambda request: {},
        )


def test_legacy_manual_validation_keeps_frozen_digest_default() -> None:
    module = _module()
    summary = _development_summary()
    with pytest.raises(module.SoxlThreeAssetLearningError):
        module.run_fixed_validation(
            materialized=_validation_materialized(),
            development_summary=summary,
            execute=lambda request: {},
        )

    module.DEVELOPMENT_SUMMARY_SHA256 = module._sha256(summary)
    proposal = module._validation_proposal(summary, input_manifest_sha256="a" * 64)
    assert proposal.optimization_method.endswith(module.DEVELOPMENT_SUMMARY_SHA256)


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


def test_watcher_digest_cli_requires_promotion_validation_summary(capsys) -> None:
    module = _module()
    code = module.main([
        "--p1-binding", "missing-binding.json",
        "--p2-candidate", str(P2_CANDIDATE),
        "--watcher-development-summary-sha256", "a" * 64,
    ])
    output = json.loads(capsys.readouterr().out)
    assert code == 2
    assert output["stage"] == "promotion_validation"
    assert output["failure_class"] == "validation_input_or_runtime_unavailable"
    assert output["no_order"] is True
    assert output["promotion_eligible"] is False


@pytest.mark.parametrize(
    ("mode_flag", "mode_value"),
    (
        ("--source-learning-replay", "source.json"),
        ("--source-paired-shadow-decision", "source.json"),
        ("--source-paired-shadow-envelope", "source.json"),
        ("--paired-shadow-session", "source.json"),
    ),
)
def test_watcher_digest_cli_rejects_non_p1_modes_before_execution(
    mode_flag: str, mode_value: str, tmp_path: Path, monkeypatch, capsys,
) -> None:
    module = _module()
    source = tmp_path / mode_value
    source.write_text("{}")
    summary = tmp_path / "summary.json"
    summary.write_text("{}")
    for name in (
        "_source_learning_replay", "_source_paired_shadow_decision",
        "_source_paired_shadow_envelope", "run_paired_shadow_session",
    ):
        monkeypatch.setattr(module, name, lambda *args, **kwargs: pytest.fail("source mode must not execute"))
    argv = [
        mode_flag, str(source),
        "--p2-candidate", str(P2_CANDIDATE),
        "--promotion-validation-development-summary", str(summary),
        "--watcher-development-summary-sha256", "a" * 64,
    ]
    if mode_flag == "--paired-shadow-session":
        argv.extend(("--ues-project", str(tmp_path), "--qpk-python", str(tmp_path / "python")))
    code = module.main(argv)
    output = json.loads(capsys.readouterr().out)
    assert code == 2
    assert output["stage"] == "promotion_validation"
    assert output["failure_class"] == "validation_input_or_runtime_unavailable"


def test_watcher_digest_cli_rejects_learning_weights_before_execution(monkeypatch, capsys) -> None:
    module = _module()
    monkeypatch.setattr(
        module,
        "run_fixed_validation_from_verified_p1",
        lambda **kwargs: pytest.fail("validation must not execute"),
    )
    code = module.main([
        "--p1-binding", "binding.json",
        "--p2-candidate", str(P2_CANDIDATE),
        "--promotion-validation-development-summary", "summary.json",
        "--watcher-development-summary-sha256", "a" * 64,
        "--blend-gate-mid-soxl-weight", "0.65",
    ])
    output = json.loads(capsys.readouterr().out)
    assert code == 2
    assert output["stage"] == "promotion_validation"
    assert output["failure_class"] == "validation_input_or_runtime_unavailable"


def test_watcher_digest_cli_is_forwarded_with_validation_summary(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    module = _module()
    paths = {name: tmp_path / name for name in ("binding.json", "manifest.json", "bars.json", "summary.json")}
    for name, path in paths.items():
        path.write_text("{}" if name != "summary.json" else json.dumps(_watcher_development_summary()))
    captured = {}

    def fake_validation(**kwargs):
        captured.update(kwargs)
        return object(), (), ()

    monkeypatch.setattr(module, "run_fixed_validation_from_verified_p1", fake_validation)
    monkeypatch.setattr(module, "_validation_output", lambda proposal, baseline, candidate: {"status": "ok"})
    digest = "a" * 64
    code = module.main([
        "--p1-binding", str(paths["binding.json"]),
        "--input-manifest", str(paths["manifest.json"]),
        "--bars-member", str(paths["bars.json"]),
        "--ues-project", str(tmp_path),
        "--p2-candidate", str(P2_CANDIDATE),
        "--promotion-validation-development-summary", str(paths["summary.json"]),
        "--watcher-development-summary-sha256", digest,
    ])
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"status": "ok"}
    assert captured["watcher_development_summary_sha256"] == digest


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


def test_paired_shadow_envelope_stays_pending_until_external_policy_window_completes(
    tmp_path: Path,
) -> None:
    ues_project = os.environ.get("QSL_UES7756_PROJECT")
    qpk_python = os.environ.get("QSL_QPK736_PYTHON")
    if not ues_project or not qpk_python:
        pytest.skip("exact paired-shadow runtimes not configured")
    module = _module()
    policy = {
        "schema_version": "forward_observation_policy.v1",
        "candidate_id": module.PAIRED_SHADOW_CANDIDATE_ID,
        "strategy_profile": module.LEARNING_PROFILE,
        "domain": "us_equity",
        "benchmark_symbol": "SOXX",
        "required_trading_sessions": 2,
        "review_milestones": [1],
        "automatic_non_live_modes": ["shadow"],
        "auto_resume_clean_sessions": 1,
        "observation_calendar": "XNYS",
        "observation_window_type": "fixed",
        "observation_start_session": "2026-09-10",
        "window_rationale_ref": "human-frozen-three-asset-policy",
        "non_live_evidence_modes": ["shadow_decision"],
        "live_authority_granted": False,
    }
    dependencies = {
        "p1_manifest": "1" * 64,
        "p2_config": "2" * 64,
        "p3_evidence": "3" * 64,
        "risk_policy": "4" * 64,
        "strategy_release": "5" * 64,
        "plugin_bundle": "6" * 64,
    }
    initial_state = {
        "cash": 100_000.0,
        "quantities": {"SOXL": 0.0, "SOXX": 0.0, "BOXX": 0.0},
        "pending_target_weights": None,
        "pending_cash_weight": None,
        "previous_equity": 100_000.0,
    }

    def observation(value: str, *, soxl: float) -> dict[str, object]:
        source = copy.deepcopy(_materialized()["sessions"][0])
        source["as_of"] = f"{value}T20:00:00+00:00"
        source["prices"] = {"SOXL": soxl, "SOXX": 107.0, "BOXX": 100.0}
        source["market_data"]["derived_indicators"]["SOXL"]["price"] = soxl
        source["input_snapshot_sha256"] = module._sha256(source)
        return source

    def run_cli(request: Mapping[str, object], path: Path) -> dict[str, object]:
        path.write_bytes(module._canonical(request))
        completed = subprocess.run(
            (
                sys.executable,
                str(SCRIPT),
                "--paired-shadow-session",
                str(path),
                "--ues-project",
                ues_project,
                "--qpk-python",
                qpk_python,
                "--p2-candidate",
                str(P2_CANDIDATE),
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=240,
        )
        assert completed.returncode == 0, completed.stdout
        return json.loads(completed.stdout)

    first_request = {
        "schema_version": module.PAIRED_SHADOW_SESSION_SCHEMA,
        "policy": policy,
        "dependency_digests": dependencies,
        "baseline_id": module.PAIRED_SHADOW_BASELINE_ID,
        "session": observation("2026-09-10", soxl=100.0),
        "cost_bps": 10.0,
        "baseline_state": initial_state,
        "candidate_state": initial_state,
        "previous_forward_observation_receipt": None,
        "previous_paired_shadow_evidence": None,
    }
    request_path = tmp_path / "paired-shadow-session.json"
    first = run_cli(first_request, request_path)

    assert first["status"] == "pending"
    assert first["passed"] is False
    assert first["forward_observation"]["state"] == "PARKED"
    assert first["promotion_eligible"] is False
    assert first["no_order"] is True and first["live_authority_granted"] is False
    assert first["forward_observation_receipt"]["observation_index"] == 1

    second_request = {
        "schema_version": module.PAIRED_SHADOW_SESSION_SCHEMA,
        "policy": policy,
        "dependency_digests": dependencies,
        "baseline_id": module.PAIRED_SHADOW_BASELINE_ID,
        "session": observation("2026-09-11", soxl=105.0),
        "cost_bps": 10.0,
        "baseline_state": first["baseline_state"],
        "candidate_state": first["candidate_state"],
        "previous_forward_observation_receipt": first["forward_observation_receipt"],
        "previous_paired_shadow_evidence": first["evidence"],
    }
    second = run_cli(second_request, request_path)
    assert second["status"] == "window_material_complete_external_admission_required"
    assert second["passed"] is False
    assert second["promotion_eligible"] is False
    assert second["window_material_complete"] is True
    assert second["forward_observation"]["state"] == "PARKED"
    assert second["no_order"] is True and second["live_authority_granted"] is False
    assert second["forward_observation_receipt"]["observation_index"] == 2
    assert (
        second["evidence"]["candidate"]["position"]["input_state_sha256"]
        == first["evidence"]["candidate"]["hypothetical_order"]["next_state_sha256"]
    )

    mismatched_ledger = copy.deepcopy(second_request)
    mismatched_ledger["candidate_state"]["cash"] += 1.0
    request_path.write_bytes(module._canonical(mismatched_ledger))
    failed = subprocess.run(
        (
            sys.executable,
            str(SCRIPT),
            "--paired-shadow-session",
            str(request_path),
            "--ues-project",
            ues_project,
            "--qpk-python",
            qpk_python,
            "--p2-candidate",
            str(P2_CANDIDATE),
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert failed.returncode == 2
    assert json.loads(failed.stdout) == {
        "status": "PARKED",
        "stage": "paired_shadow",
        "failure_class": "paired_shadow_input_or_runtime_unavailable",
        "no_order": True,
        "live_authority_granted": False,
    }
    assert first["evidence"]["candidate"]["signal"] != first["evidence"]["baseline"]["signal"]
    assert (
        second["evidence"]["candidate"]["cost"]["model"]
        == "one_way_turnover_all_in_bps"
    )


def test_source_paired_shadow_decision_calls_frozen_three_asset_signal() -> None:
    module = _module()
    session = _materialized()["sessions"][0]
    result = module._source_paired_shadow_decision(
        {
            "schema_version": module.PAIRED_SHADOW_DECISION_SCHEMA,
            "as_of": session["as_of"],
            "portfolio": {
                "as_of": session["as_of"],
                "total_equity": 100_000.0,
                "buying_power": 100_000.0,
                "cash_balance": 100_000.0,
                "positions": [],
                "metadata": {"observed_effective_exposure": 0.0},
            },
            "market_data": session["market_data"],
            "parameter_override": {"blend_gate_mid_soxl_weight": 0.55},
        },
        json.loads(P2_CANDIDATE.read_text()),
    )

    assert set(result["target_values"]) == {"SOXL", "SOXX", "BOXX"}
    assert result["entrypoint"].endswith("soxl_soxx_trend_income.build_rebalance_plan")
    assert result["output_sha256"] == module._sha256({
        key: value for key, value in result.items() if key != "output_sha256"
    })
