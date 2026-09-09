#!/usr/bin/env python3
"""Run bounded SOXL/SOXX/BOXX learning on verified P1 development data."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import shutil
import statistics
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path

LEARNING_PROFILE = "soxl_soxx_three_asset_mid_weight_learning_v1"
LEARNING_SCHEMA = "qsl.soxl-soxx-three-asset-learning.v1"
LEARNING_REPLAY_SCHEMA = "qsl.soxl-soxx-three-asset-learning-replay.v1"
LEARNING_REPLAY_RESULT_SCHEMA = "qsl.soxl-soxx-three-asset-learning-replay-result.v1"
DEVELOPMENT_CUTOFF = "2025-07-31"
BASELINE_MID_SOXL_WEIGHT = 0.65
P2_UES_UV_LOCK_SHA256 = "6c12df9b3412681829295f15de7e2ce7fc5b708d1de815f72d654fc16b7848e6"
COST_BPS = (5.0, 10.0, 15.0)
MAX_TRIALS = 3


class SoxlThreeAssetLearningError(ValueError):
    """Sanitized failure at the bounded learning boundary."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise SoxlThreeAssetLearningError("invalid learning input") from exc


def _sha256(value: object) -> str:
    import hashlib

    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlThreeAssetLearningError("invalid learning input")
    return dict(value)


def _weights(values: Sequence[float], *, require_baseline: bool = True) -> tuple[float, ...]:
    if isinstance(values, (str, bytes)) or not 1 <= len(values) <= MAX_TRIALS:
        raise SoxlThreeAssetLearningError("invalid learning parameters")
    result: list[float] = []
    for raw in values:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise SoxlThreeAssetLearningError("invalid learning parameters")
        value = float(raw)
        if not math.isfinite(value) or not 0.0 <= value <= BASELINE_MID_SOXL_WEIGHT:
            raise SoxlThreeAssetLearningError("invalid learning parameters")
        result.append(value)
    if len(set(result)) != len(result):
        raise SoxlThreeAssetLearningError("invalid learning parameters")
    if require_baseline and BASELINE_MID_SOXL_WEIGHT not in result:
        raise SoxlThreeAssetLearningError("baseline learning parameter required")
    return tuple(result)


def build_learning_requests(
    materialized: Mapping[str, object],
    *,
    mid_soxl_weights: Sequence[float],
    initial_equity: float = 100_000.0,
) -> list[dict[str, object]]:
    """Project verified materialized P1 data into bounded development requests."""
    payload = _mapping(materialized)
    required = {"schema_version", "p1_identity", "p2_identity", "indicator_spec", "sessions", "materialized_input_sha256"}
    if set(payload) != required:
        raise SoxlThreeAssetLearningError("invalid materialized input")
    claimed = payload.pop("materialized_input_sha256")
    if not isinstance(claimed, str) or claimed != _sha256(payload):
        raise SoxlThreeAssetLearningError("invalid materialized input")
    p2 = _mapping(payload["p2_identity"])
    if p2 != {
        "candidate_id": "soxl_soxx_core_only_p2_v3",
        "config_sha256": "ff8fa0acf4f175a7c40c3e1e6a3304ea2748b6b81c3797342085a4df3810ab4d",
    }:
        raise SoxlThreeAssetLearningError("invalid materialized input")
    sessions = payload["sessions"]
    if not isinstance(sessions, list):
        raise SoxlThreeAssetLearningError("invalid materialized input")
    development = [
        row for row in sessions
        if isinstance(row, Mapping) and str(row.get("as_of") or "")[:10] <= DEVELOPMENT_CUTOFF
    ]
    if len(development) < 3:
        raise SoxlThreeAssetLearningError("development input unavailable")
    equity = float(initial_equity)
    if not math.isfinite(equity) or equity <= 0:
        raise SoxlThreeAssetLearningError("invalid learning input")
    return [
        {
            "schema_version": LEARNING_REPLAY_SCHEMA,
            "initial_equity": equity,
            "cost_bps": cost,
            "sessions": development,
            "parameter_override": {"blend_gate_mid_soxl_weight": weight},
        }
        for weight in _weights(mid_soxl_weights)
        for cost in COST_BPS
    ]


def _load_isolated_module():
    path = Path(__file__).with_name("run_soxl_core_only_p3_isolated.py")
    spec = importlib.util.spec_from_file_location("qsl_soxl_learning_isolated", path)
    if spec is None or spec.loader is None:
        raise SoxlThreeAssetLearningError("isolated runtime unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _backtest_result(replay: Mapping[str, object], *, parameter: float):
    from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult

    decisions = replay["decisions"]
    if not isinstance(decisions, list) or len(decisions) < 3:
        raise SoxlThreeAssetLearningError("learning result unavailable")
    curve = [float(decision["equity_before_signal"]) for decision in decisions]
    returns = [curve[index] / curve[index - 1] - 1.0 for index in range(1, len(curve))]
    final_equity = float(replay["final_equity"])
    initial_equity = float(replay["initial_equity"])
    total_return = final_equity / initial_equity - 1.0
    periods = len(returns)
    cagr = (1.0 + total_return) ** (252.0 / periods) - 1.0 if total_return > -1.0 else -1.0
    volatility = statistics.stdev(returns) * math.sqrt(252.0) if len(returns) > 1 else 0.0
    sharpe = (
        statistics.mean(returns) / statistics.stdev(returns) * math.sqrt(252.0)
        if len(returns) > 1 and statistics.stdev(returns)
        else 0.0
    )
    peak = curve[0]
    max_drawdown = 0.0
    for equity in [*curve, final_equity]:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1.0 - equity / peak)
    return BacktestResult(
        strategy_profile=LEARNING_PROFILE,
        domain="us_equity",
        param_set_id=f"mid-{parameter:g}-cost-{float(replay['cost_bps']):g}",
        params={"blend_gate_mid_soxl_weight": parameter},
        sharpe_ratio=sharpe,
        max_drawdown=max_drawdown,
        cagr=cagr,
        volatility=volatility,
        total_return=total_return,
        start_date=date.fromisoformat(str(decisions[0]["signal_as_of"])[:10]),
        end_date=date.fromisoformat(str(decisions[-1]["signal_as_of"])[:10]),
        observation_count=periods,
        source_script="run_soxl_three_asset_learning.py",
        source_revision="7756fe32585e85cf1d09a163203a02e3eee39fe1",
        cost_model=f"all_in_per_side_{float(replay['cost_bps']):g}bps",
        cost_inputs={"total_cost_bps": float(replay["cost_bps"])},
    )


def _source_learning_replay(value: Mapping[str, object], candidate: Mapping[str, object]) -> dict[str, object]:
    isolated = _load_isolated_module()
    request = _mapping(value)
    if set(request) != {"schema_version", "initial_equity", "cost_bps", "sessions", "parameter_override"} or request["schema_version"] != LEARNING_REPLAY_SCHEMA:
        raise SoxlThreeAssetLearningError("invalid learning input")
    override = _mapping(request.pop("parameter_override"))
    if set(override) != {"blend_gate_mid_soxl_weight"}:
        raise SoxlThreeAssetLearningError("invalid learning parameters")
    parameter = _weights((override["blend_gate_mid_soxl_weight"],), require_baseline=False)[0]
    request["schema_version"] = isolated.STATEFUL_REPLAY_INPUT_SCHEMA
    p2 = isolated.validate_p2_candidate(candidate)

    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from us_equity_strategies import entrypoints
    from us_equity_strategies.strategies import soxl_soxx_trend_income as strategy

    runtime_config = dict(p2["runtime_config"])
    runtime_config["blend_gate_mid_soxl_weight"] = parameter

    def decide(state: object) -> Mapping[str, object]:
        item = _mapping(state)
        context = StrategyContext(
            as_of=item["as_of"], portfolio=item["portfolio"],
            market_data=_mapping(item["market_data"]), runtime_config=runtime_config,
        )
        config = entrypoints.merge_runtime_config(entrypoints.soxl_soxx_trend_income_manifest.default_config, context)
        entrypoints._validate_soxl_soxx_core_only_research_runtime_config(config)
        entrypoints.pop_option_overlay_config(config)
        symbols = tuple(str(symbol) for symbol in config.pop("managed_symbols", ()))
        config.pop("signal_text_fn", None)
        config.pop("signal_effective_after_trading_days", None)
        reserved = entrypoints.pop_reserved_cash_policy_config(config)
        entrypoints.pop_execution_only_config(config)
        entrypoints.apply_reserved_cash_policy_to_ratio_config(config, reserved)
        translator = config.pop("translator", entrypoints.default_translator)
        plan = strategy.build_rebalance_plan(
            _mapping(item["market_data"])["derived_indicators"],
            entrypoints._build_tiered_blend_account_state_from_portfolio(item["portfolio"], strategy_symbols=symbols),
            translator=translator, **config,
        )
        summary = {
            "schema_version": "qsl.soxl-soxx-three-asset-learning-decision.v1",
            "entrypoint": "us_equity_strategies.strategies.soxl_soxx_trend_income.build_rebalance_plan",
            "as_of": item["as_of"].isoformat(),
            "target_values": {symbol: float(plan["targets"][symbol]) for symbol in ("SOXL", "SOXX", "BOXX")},
            "diagnostics": {field: plan.get(field) for field in isolated._DIAGNOSTIC_FIELDS},
        }
        summary["output_sha256"] = isolated._sha256(summary)
        return summary

    replay = isolated._stateful_replay_with_decision_builder(
        request, decision_builder=decide,
        entrypoint="us_equity_strategies.strategies.soxl_soxx_trend_income.build_rebalance_plan",
        result_schema=LEARNING_REPLAY_RESULT_SCHEMA,
    )
    result = {
        "schema_version": LEARNING_REPLAY_RESULT_SCHEMA,
        "status": "SUCCESS",
        "parameter_override": {"blend_gate_mid_soxl_weight": parameter},
        "cost_bps": float(replay["cost_bps"]),
        "backtest_result": _backtest_result(replay, parameter=parameter).to_dict(),
    }
    result["output_sha256"] = _sha256(result)
    return result


def run_learning(
    *, materialized: Mapping[str, object], mid_soxl_weights: Sequence[float],
    execute: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    requests = build_learning_requests(materialized, mid_soxl_weights=mid_soxl_weights)
    trials = []
    for request in requests:
        result = _mapping(execute(request))
        if result.get("status") != "SUCCESS" or result.get("schema_version") != LEARNING_REPLAY_RESULT_SCHEMA:
            raise SoxlThreeAssetLearningError("learning result unavailable")
        if (
            result.get("parameter_override") != request["parameter_override"]
            or result.get("cost_bps") != request["cost_bps"]
        ):
            raise SoxlThreeAssetLearningError("learning result unavailable")
        claimed = result.pop("output_sha256", None)
        if not isinstance(claimed, str) or claimed != _sha256(result):
            raise SoxlThreeAssetLearningError("learning result unavailable")
        result["output_sha256"] = claimed
        trials.append(result)
    source = _mapping(materialized)
    result: dict[str, object] = {
        "schema_version": LEARNING_SCHEMA, "status": "SUCCESS",
        "learning_profile": LEARNING_PROFILE,
        "learning_only": True, "no_order": True, "size_zero_required": True,
        "promotion_eligible": False, "research_executed": True,
        "development_cutoff": DEVELOPMENT_CUTOFF,
        "p1_identity": source["p1_identity"],
        "source_identity": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": "7756fe32585e85cf1d09a163203a02e3eee39fe1",
            "quant_platform_kit_revision": "3acab1923a97b805b077c85c6c19657be0143bac",
            "uv_lock_sha256": P2_UES_UV_LOCK_SHA256,
        },
        "parameter_key": "blend_gate_mid_soxl_weight",
        "trial_count": len(_weights(mid_soxl_weights)), "cost_bps": list(COST_BPS),
        "results": trials,
    }
    result["result_sha256"] = _sha256(result)
    return result


def run_isolated_learning_request(
    request: Mapping[str, object], *, ues_project: Path, p2_candidate_path: Path,
) -> dict[str, object]:
    isolated = _load_isolated_module()
    isolated.validate_ues_project(ues_project)
    isolated.validate_p2_candidate(json.loads(p2_candidate_path.read_text()))
    if shutil.which("uv") is None:
        raise SoxlThreeAssetLearningError("isolated runtime unavailable")
    with tempfile.TemporaryDirectory(prefix="qsl-soxl-learning-") as directory:
        path = Path(directory) / "request.json"
        path.write_bytes(_canonical(request))
        try:
            completed = subprocess.run(
                (
                    "uv", "run", "--locked", "--project", str(ues_project), "python",
                    str(Path(__file__).resolve()), "--source-learning-replay", str(path),
                    "--p2-candidate", str(p2_candidate_path.resolve()),
                ),
                check=False, capture_output=True, text=True, timeout=120,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SoxlThreeAssetLearningError("isolated runtime unavailable") from exc
    if completed.returncode != 0:
        raise SoxlThreeAssetLearningError("isolated runtime unavailable")
    try:
        result = _mapping(json.loads(completed.stdout))
    except (TypeError, json.JSONDecodeError) as exc:
        raise SoxlThreeAssetLearningError("isolated runtime unavailable") from exc
    if result.get("status") != "SUCCESS":
        raise SoxlThreeAssetLearningError("isolated runtime unavailable")
    return result


def run_learning_from_verified_p1(
    *, binding: Mapping[str, object], manifest: Mapping[str, object], member_bytes: bytes,
    ues_project: Path, p2_candidate_path: Path, mid_soxl_weights: Sequence[float],
) -> dict[str, object]:
    from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p3_input_materializer import (
        materialize_soxl_core_only_p3_input,
    )

    materialized = materialize_soxl_core_only_p3_input(
        binding=binding, manifest=manifest, member_bytes=member_bytes,
    )
    return run_learning(
        materialized=materialized, mid_soxl_weights=mid_soxl_weights,
        execute=lambda request: run_isolated_learning_request(
            request, ues_project=ues_project, p2_candidate_path=p2_candidate_path,
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--source-learning-replay")
    mode.add_argument("--p1-binding", type=Path)
    parser.add_argument("--input-manifest", type=Path)
    parser.add_argument("--bars-member", type=Path)
    parser.add_argument("--ues-project", type=Path)
    parser.add_argument("--blend-gate-mid-soxl-weight", action="append", type=float)
    parser.add_argument("--p2-candidate", required=True, type=Path)
    args = parser.parse_args(argv)
    failed = False
    try:
        if args.source_learning_replay:
            result = _source_learning_replay(
                json.loads(Path(args.source_learning_replay).read_text()),
                json.loads(args.p2_candidate.read_text()),
            )
        else:
            if not all((args.input_manifest, args.bars_member, args.ues_project, args.blend_gate_mid_soxl_weight)):
                raise SoxlThreeAssetLearningError("invalid learning arguments")
            result = run_learning_from_verified_p1(
                binding=json.loads(args.p1_binding.read_text()),
                manifest=json.loads(args.input_manifest.read_text()),
                member_bytes=args.bars_member.read_bytes(),
                ues_project=args.ues_project,
                p2_candidate_path=args.p2_candidate,
                mid_soxl_weights=args.blend_gate_mid_soxl_weight,
            )
    except Exception:
        failed = True
        result = {"schema_version": LEARNING_SCHEMA, "status": "PARKED", "failure_class": "learning_input_or_runtime_unavailable"}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
