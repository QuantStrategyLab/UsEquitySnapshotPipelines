from __future__ import annotations

import importlib.util
import json
import math
import statistics
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
