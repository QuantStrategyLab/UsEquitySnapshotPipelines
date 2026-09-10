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
from datetime import date, datetime
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
VALIDATION_CANDIDATE_MID_SOXL_WEIGHT = 0.55
DEVELOPMENT_SUMMARY_SHA256 = "89418d4e13efa9379f91c522ccbe084e2cbf180ba343103d5b73fb7cdbb955a8"
VALIDATION_OOS_START = date(2025, 8, 4)
VALIDATION_OOS_END = date(2026, 8, 4)
VALIDATION_FOLDS = (
    (date(2022, 12, 28), date(2023, 6, 30), date(2023, 7, 3), date(2023, 12, 29)),
    (date(2024, 1, 2), date(2024, 6, 28), date(2024, 7, 1), date(2024, 12, 31)),
    (date(2025, 1, 2), date(2025, 2, 28), date(2025, 3, 3), date(2025, 7, 31)),
)
PAIRED_SHADOW_CANDIDATE_ID = "soxl_soxx_three_asset_mid_weight_055_v1"
PAIRED_SHADOW_BASELINE_ID = "soxl_soxx_three_asset_mid_weight_065_v1"
PAIRED_SHADOW_SESSION_SCHEMA = "qsl.soxl-three-asset-paired-shadow-session.v1"
PAIRED_SHADOW_DECISION_SCHEMA = "qsl.soxl-three-asset-paired-shadow-decision.v1"
PAIRED_SHADOW_QPK_REVISION = "7363011d56926d39f4fffeb036e511391114e39f"


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


def _load_paired_shadow_module():
    path = (
        Path(__file__).parents[1] / "src" / "us_equity_snapshot_pipelines" /
        "lifecycle" / "soxl_three_asset_paired_shadow.py"
    )
    spec = importlib.util.spec_from_file_location("qsl_soxl_three_asset_paired_shadow", path)
    if spec is None or spec.loader is None:
        raise SoxlThreeAssetLearningError("paired shadow runtime unavailable")
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


def _validation_proposal(
    development_summary: Mapping[str, object], *, input_manifest_sha256: str,
):
    from quant_platform_kit.strategy_lifecycle.contracts import OptimizationProposal

    summary = _mapping(development_summary)
    summary_sha256 = _sha256(summary)
    if summary_sha256 != DEVELOPMENT_SUMMARY_SHA256:
        raise SoxlThreeAssetLearningError("invalid validation proposal")
    input_identity = _mapping(summary.get("input_identity"))
    if input_identity != {"manifest_sha256": input_manifest_sha256, "member_count": 4}:
        raise SoxlThreeAssetLearningError("invalid validation proposal")
    return OptimizationProposal(
        strategy_profile=LEARNING_PROFILE,
        domain="us_equity",
        current_params={"blend_gate_mid_soxl_weight": BASELINE_MID_SOXL_WEIGHT},
        proposed_params={"blend_gate_mid_soxl_weight": VALIDATION_CANDIDATE_MID_SOXL_WEIGHT},
        improvement_score=0.0,
        confidence=0.0,
        winning_dimensions=("max_drawdown",),
        regressing_dimensions=("cagr", "sharpe_ratio"),
        recommendation="research_candidate",
        walk_forward_passed=False,
        optimization_method=f"bounded_development_tradeoff:sha256:{summary_sha256}",
        search_iterations=3,
    )


class _NoWritePromotionStore:
    def save_backtest_result(self, result: object) -> None:
        del result


class _ThreeAssetPromotionRunner:
    runner_kind = "real"

    def __init__(self, materialized: Mapping[str, object], execute: Callable[[Mapping[str, object]], Mapping[str, object]]):
        self._sessions = tuple(_mapping(item) for item in materialized["sessions"])  # type: ignore[index]
        self._execute = execute

    def _run(self, params: Mapping[str, object], *, start: date, end: date, cost_model: object):
        from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult

        if params != {"blend_gate_mid_soxl_weight": params.get("blend_gate_mid_soxl_weight")}:
            raise SoxlThreeAssetLearningError("invalid validation parameters")
        weight = _weights((params["blend_gate_mid_soxl_weight"],), require_baseline=False)[0]
        if weight not in (BASELINE_MID_SOXL_WEIGHT, VALIDATION_CANDIDATE_MID_SOXL_WEIGHT):
            raise SoxlThreeAssetLearningError("invalid validation parameters")
        cost = sum(float(getattr(cost_model, field)) for field in ("commission_bps", "slippage_bps", "market_impact_bps"))
        if cost not in COST_BPS:
            raise SoxlThreeAssetLearningError("invalid validation cost")
        sessions = [item for item in self._sessions if start.isoformat() <= str(item.get("as_of"))[:10] <= end.isoformat()]
        if len(sessions) < 3 or str(sessions[0].get("as_of"))[:10] != start.isoformat() or str(sessions[-1].get("as_of"))[:10] != end.isoformat():
            raise SoxlThreeAssetLearningError("validation window unavailable")
        request = {
            "schema_version": LEARNING_REPLAY_SCHEMA,
            "initial_equity": 100_000.0,
            "cost_bps": cost,
            "sessions": sessions,
            "parameter_override": {"blend_gate_mid_soxl_weight": weight},
        }
        result = _mapping(self._execute(request))
        claimed = result.pop("output_sha256", None)
        if (
            result.get("schema_version") != LEARNING_REPLAY_RESULT_SCHEMA
            or result.get("status") != "SUCCESS"
            or result.get("parameter_override") != request["parameter_override"]
            or result.get("cost_bps") != cost
            or not isinstance(claimed, str)
            or claimed != _sha256(result)
        ):
            raise SoxlThreeAssetLearningError("validation result unavailable")
        raw = _mapping(result.get("backtest_result"))
        if raw.get("params") != request["parameter_override"]:
            raise SoxlThreeAssetLearningError("validation result unavailable")
        try:
            return BacktestResult(
                strategy_profile=str(raw["strategy_profile"]), domain=str(raw["domain"]),
                param_set_id=str(raw.get("param_set_id") or ""), params=_mapping(raw["params"]),
                sharpe_ratio=float(raw["sharpe_ratio"]), max_drawdown=float(raw["max_drawdown"]),
                cagr=float(raw["cagr"]), volatility=float(raw["volatility"]),
                total_return=float(raw["total_return"]), start_date=date.fromisoformat(str(raw["start_date"])),
                end_date=date.fromisoformat(str(raw["end_date"])), observation_count=int(raw["observation_count"]),
                source_script=str(raw.get("source_script") or ""), source_revision=str(raw.get("source_revision") or ""),
                cost_model=str(raw.get("cost_model") or ""), cost_inputs=_mapping(raw.get("cost_inputs")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SoxlThreeAssetLearningError("validation result unavailable") from exc

    def run_purged_fold(self, strategy_profile: str, params: Mapping[str, object], *, fold: object, purge_days: int, embargo_days: int, cost_model: object):
        if strategy_profile != LEARNING_PROFILE or purge_days != 1 or embargo_days != 1:
            raise SoxlThreeAssetLearningError("invalid validation plan")
        return self._run(params, start=fold.test_start, end=fold.test_end, cost_model=cost_model)

    def run_locked_oos(self, strategy_profile: str, params: Mapping[str, object], *, start_date: date, end_date: date, cost_model: object):
        if strategy_profile != LEARNING_PROFILE:
            raise SoxlThreeAssetLearningError("invalid validation plan")
        return self._run(params, start=start_date, end=end_date, cost_model=cost_model)


def run_fixed_validation(
    *, materialized: Mapping[str, object], development_summary: Mapping[str, object],
    execute: Callable[[Mapping[str, object]], Mapping[str, object]],
):
    """Run the one preselected 0.65/0.55 comparison through QPK promotion backtests."""
    from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
    from quant_platform_kit.strategy_lifecycle.contracts import PromotionCostModel, PurgedWalkForwardFold

    build_learning_requests(materialized, mid_soxl_weights=(0.65, 0.55))
    source = _mapping(materialized)
    p1 = _mapping(source.get("p1_identity"))
    proposal = _validation_proposal(development_summary, input_manifest_sha256=str(p1.get("input_manifest_sha256")))
    folds = tuple(PurgedWalkForwardFold(*boundaries) for boundaries in VALIDATION_FOLDS)
    orchestrator = BacktestOrchestrator(store=_NoWritePromotionStore())
    orchestrator.register_runner("us_equity", _ThreeAssetPromotionRunner(source, execute))
    summary_digest = proposal.optimization_method.rsplit(":", 1)[-1]

    def runs_for(weight: float, role: str):
        return tuple(
            orchestrator.run_promotion(
                LEARNING_PROFILE, domain="us_equity",
                params={"blend_gate_mid_soxl_weight": weight}, folds=folds,
                locked_oos_start=VALIDATION_OOS_START, locked_oos_end=VALIDATION_OOS_END,
                purge_days=1, embargo_days=1,
                source_revision="7756fe32585e85cf1d09a163203a02e3eee39fe1",
                cost_model=PromotionCostModel(
                    model_id=f"all_in_per_side_{cost:g}bps", commission_bps=0.0,
                    slippage_bps=cost, market_impact_bps=0.0,
                ),
                param_set_id=f"soxl-three-asset-{summary_digest}-{role}-cost-{cost:g}",
            )
            for cost in COST_BPS
        )

    baseline = runs_for(BASELINE_MID_SOXL_WEIGHT, "baseline")
    candidate = runs_for(VALIDATION_CANDIDATE_MID_SOXL_WEIGHT, "candidate")
    return proposal, baseline, candidate


def run_fixed_validation_from_verified_p1(
    *, binding: Mapping[str, object], manifest: Mapping[str, object], member_bytes: bytes,
    development_summary: Mapping[str, object], ues_project: Path, p2_candidate_path: Path,
):
    from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p3_input_materializer import (
        materialize_soxl_core_only_p3_input,
    )

    materialized = materialize_soxl_core_only_p3_input(
        binding=binding, manifest=manifest, member_bytes=member_bytes,
    )
    return run_fixed_validation(
        materialized=materialized,
        development_summary=development_summary,
        execute=lambda request: run_isolated_learning_request(
            request, ues_project=ues_project, p2_candidate_path=p2_candidate_path,
        ),
    )


def _validation_output(proposal: object, baseline: Sequence[object], candidate: Sequence[object]) -> dict[str, object]:
    return {
        "status": "PROMOTION_BACKTEST_RUNS_BUILT",
        "stage": "promotion_validation",
        "learning_only": True,
        "size_zero_required": True,
        "promotion_eligible": False,
        "proposal": proposal.to_dict(),
        "baseline_promotion_runs": [run.to_dict() for run in baseline],
        "candidate_promotion_runs": [run.to_dict() for run in candidate],
        "no_order": True,
        "live_authority_granted": False,
    }


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


def _source_paired_shadow_decision(
    value: Mapping[str, object], candidate: Mapping[str, object]
) -> dict[str, object]:
    """Evaluate one explicit portfolio through the frozen UES three-asset signal."""
    isolated = _load_isolated_module()
    request = _mapping(value)
    if set(request) != {
        "schema_version", "as_of", "portfolio", "market_data", "parameter_override"
    } or request["schema_version"] != PAIRED_SHADOW_DECISION_SCHEMA:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input")
    override = _mapping(request["parameter_override"])
    if set(override) != {"blend_gate_mid_soxl_weight"}:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input")
    weight = _weights((override["blend_gate_mid_soxl_weight"],), require_baseline=False)[0]
    if weight not in {BASELINE_MID_SOXL_WEIGHT, VALIDATION_CANDIDATE_MID_SOXL_WEIGHT}:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input")
    p2 = isolated.validate_p2_candidate(candidate)
    portfolio = _mapping(request["portfolio"])
    if set(portfolio) != {
        "as_of", "total_equity", "buying_power", "cash_balance", "positions", "metadata"
    }:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input")
    try:
        as_of = datetime.fromisoformat(str(request["as_of"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input") from exc
    if as_of.tzinfo is None or portfolio["as_of"] != request["as_of"]:
        raise SoxlThreeAssetLearningError("invalid paired shadow decision input")

    from quant_platform_kit.common.models import PortfolioSnapshot, Position
    from quant_platform_kit.common.strategy_contracts import StrategyContext
    from us_equity_strategies import entrypoints
    from us_equity_strategies.strategies import soxl_soxx_trend_income as strategy

    positions = tuple(
        Position(
            symbol=str(row["symbol"]), quantity=float(row["quantity"]),
            market_value=float(row["market_value"]), currency=str(row["currency"]),
        )
        for row in portfolio["positions"]
    )
    snapshot = PortfolioSnapshot(
        as_of=as_of,
        total_equity=float(portfolio["total_equity"]),
        buying_power=float(portfolio["buying_power"]),
        cash_balance=float(portfolio["cash_balance"]),
        positions=positions,
        metadata=_mapping(portfolio["metadata"]),
    )
    runtime_config = dict(p2["runtime_config"])
    runtime_config["blend_gate_mid_soxl_weight"] = weight
    context = StrategyContext(
        as_of=as_of,
        portfolio=snapshot,
        market_data=_mapping(request["market_data"]),
        runtime_config=runtime_config,
    )
    config = entrypoints.merge_runtime_config(
        entrypoints.soxl_soxx_trend_income_manifest.default_config, context
    )
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
        _mapping(request["market_data"])["derived_indicators"],
        entrypoints._build_tiered_blend_account_state_from_portfolio(
            snapshot, strategy_symbols=symbols
        ),
        translator=translator,
        **config,
    )
    result = {
        "schema_version": "qsl.soxl-soxx-three-asset-learning-decision.v1",
        "entrypoint": "us_equity_strategies.strategies.soxl_soxx_trend_income.build_rebalance_plan",
        "as_of": str(request["as_of"]),
        "target_values": {
            symbol: float(plan["targets"][symbol]) for symbol in ("SOXL", "SOXX", "BOXX")
        },
        "diagnostics": {
            field: plan.get(field) for field in isolated._DIAGNOSTIC_FIELDS
        },
    }
    result["output_sha256"] = isolated._sha256(result)
    return result


def _forward_policy(value: object):
    from quant_platform_kit.strategy_lifecycle.forward_observation import ForwardObservationPolicy

    raw = _mapping(value)
    if raw.pop("schema_version", None) != "forward_observation_policy.v1":
        raise SoxlThreeAssetLearningError("invalid paired shadow policy")
    if raw.pop("live_authority_granted", None) is not False:
        raise SoxlThreeAssetLearningError("invalid paired shadow policy")
    try:
        policy = ForwardObservationPolicy(
            **{
                **raw,
                "review_milestones": tuple(raw["review_milestones"]),
                "automatic_non_live_modes": tuple(raw["automatic_non_live_modes"]),
                "non_live_evidence_modes": tuple(raw["non_live_evidence_modes"]),
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SoxlThreeAssetLearningError("invalid paired shadow policy") from exc
    if (
        policy.candidate_id != PAIRED_SHADOW_CANDIDATE_ID
        or policy.strategy_profile != LEARNING_PROFILE
        or tuple(policy.automatic_non_live_modes) != ("shadow",)
        or tuple(policy.non_live_evidence_modes) != ("shadow_decision",)
    ):
        raise SoxlThreeAssetLearningError("invalid paired shadow policy")
    return policy


def _source_paired_shadow_envelope(value: Mapping[str, object]) -> dict[str, object]:
    """Bind one financial observation to QPK736 receipt/evidence contracts."""
    from quant_platform_kit.strategy_lifecycle.forward_observation import (
        ForwardObservationSnapshot,
        evaluate_forward_observation,
    )
    from quant_platform_kit.strategy_lifecycle.forward_observation_receipt import (
        build_forward_observation_receipt,
    )
    from quant_platform_kit.strategy_lifecycle.paired_shadow_adapter import (
        PairedShadowObservation,
        collect_paired_shadow_for_promotion,
    )

    request = _mapping(value)
    required = {
        "policy", "dependency_digests", "baseline_id", "observation_session",
        "observed_at", "input_snapshot_sha256", "candidate", "baseline",
        "previous_forward_observation_receipt", "previous_paired_shadow_evidence",
    }
    if set(request) != required or request["baseline_id"] != PAIRED_SHADOW_BASELINE_ID:
        raise SoxlThreeAssetLearningError("invalid paired shadow envelope")
    previous_receipt = request["previous_forward_observation_receipt"]
    previous_evidence = request["previous_paired_shadow_evidence"]
    if (previous_receipt is None) != (previous_evidence is None):
        raise SoxlThreeAssetLearningError("invalid paired shadow predecessor pair")
    if previous_evidence is not None:
        predecessor = _mapping(previous_evidence)
        for leg_name in ("candidate", "baseline"):
            prior_leg = _mapping(predecessor.get(leg_name))
            current_leg = _mapping(request.get(leg_name))
            prior_order = _mapping(prior_leg.get("hypothetical_order"))
            current_position = _mapping(current_leg.get("position"))
            if (
                prior_order.get("next_state_sha256")
                != current_position.get("input_state_sha256")
            ):
                raise SoxlThreeAssetLearningError("paired shadow predecessor ledger mismatch")
    policy = _forward_policy(request["policy"])
    previous_index = 0 if previous_receipt is None else int(_mapping(previous_receipt)["observation_index"])
    receipt = build_forward_observation_receipt(
        policy=policy,
        observation_session=str(request["observation_session"]),
        observation_index=previous_index + 1,
        dependency_digests=_mapping(request["dependency_digests"]),
        evidence_modes=("shadow_decision",),
        previous_receipt=previous_receipt,
    )
    record = collect_paired_shadow_for_promotion(PairedShadowObservation(
        policy=policy,
        forward_observation_receipt=receipt,
        baseline_id=PAIRED_SHADOW_BASELINE_ID,
        observed_at=str(request["observed_at"]),
        input_snapshot_sha256=str(request["input_snapshot_sha256"]),
        candidate=_mapping(request["candidate"]),
        baseline=_mapping(request["baseline"]),
        previous_evidence=previous_evidence,
        previous_forward_observation_receipt=previous_receipt,
    ))
    forward = evaluate_forward_observation(
        policy,
        ForwardObservationSnapshot(
            # This bounded consumer validates an externally supplied digest but
            # is not the trusted historical-evidence verifier. Keep promotion
            # parked until the established outer cycle supplies that admission.
            historical_evidence_verified=False,
            observations_completed=int(receipt["observation_index"]),
            previous_observations_completed=previous_index,
            previous_state="not_started" if previous_index == 0 else "active",
            paper_status="unsupported",
        ),
    ).to_dict()
    window_material_complete = (
        int(receipt["observation_index"]) >= policy.required_trading_sessions
    )
    record.update({
        "status": (
            "window_material_complete_external_admission_required"
            if window_material_complete
            else "pending"
        ),
        "passed": False,
        "promotion_eligible": False,
        "window_material_complete": window_material_complete,
        "forward_observation_receipt": receipt,
        "forward_observation": forward,
        "no_order": True,
        "live_authority_granted": False,
    })
    return record


def _validate_qpk_python(path: Path) -> None:
    if not path.is_file():
        raise SoxlThreeAssetLearningError("isolated QPK paired shadow unavailable")
    try:
        actual = subprocess.run(
            (
                str(path),
                "-c",
                "import importlib.metadata,json;"
                "d=importlib.metadata.distribution('quant-platform-kit');"
                "print(json.loads(d.read_text('direct_url.json') or '{}')"
                "['vcs_info']['commit_id'])",
            ),
            check=True,
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise SoxlThreeAssetLearningError(
            "isolated QPK paired shadow unavailable"
        ) from exc
    if actual != PAIRED_SHADOW_QPK_REVISION:
        raise SoxlThreeAssetLearningError("isolated QPK paired shadow identity mismatch")


def _run_isolated_json_mode(
    request: Mapping[str, object], *, command: tuple[str, ...],
    arguments: tuple[str, ...],
) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="qsl-soxl-paired-shadow-") as directory:
        path = Path(directory) / "request.json"
        path.write_bytes(_canonical(request))
        try:
            completed = subprocess.run(
                (*command, str(Path(__file__).resolve()), arguments[0], str(path),
                 *arguments[1:]),
                check=False, capture_output=True, text=True, timeout=120,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SoxlThreeAssetLearningError("isolated paired shadow runtime unavailable") from exc
    if completed.returncode != 0:
        raise SoxlThreeAssetLearningError("isolated paired shadow runtime unavailable")
    try:
        return _mapping(json.loads(completed.stdout))
    except (TypeError, json.JSONDecodeError) as exc:
        raise SoxlThreeAssetLearningError("isolated paired shadow runtime unavailable") from exc


def run_paired_shadow_session(
    value: Mapping[str, object], *, ues_project: Path, qpk_python: Path,
    p2_candidate_path: Path,
) -> dict[str, object]:
    """Run one offline paired session through UES7756 and QPK736."""
    isolated = _load_isolated_module()
    isolated.validate_ues_project(ues_project)
    _validate_qpk_python(qpk_python)
    p2_candidate = json.loads(p2_candidate_path.read_text())
    isolated.validate_p2_candidate(p2_candidate)
    request = _mapping(value)
    required = {
        "schema_version", "policy", "dependency_digests", "baseline_id", "session",
        "cost_bps", "baseline_state", "candidate_state",
        "previous_forward_observation_receipt", "previous_paired_shadow_evidence",
    }
    if set(request) != required or request["schema_version"] != PAIRED_SHADOW_SESSION_SCHEMA:
        raise SoxlThreeAssetLearningError("invalid paired shadow session")
    session = _mapping(request["session"])
    try:
        observed_at = datetime.fromisoformat(str(session["as_of"]).replace("Z", "+00:00"))
        calendar_name = str(_mapping(request["policy"])["observation_calendar"])
        import exchange_calendars as xcals

        if observed_at.tzinfo is None or not xcals.get_calendar(calendar_name).is_session(
            observed_at.date().isoformat()
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise SoxlThreeAssetLearningError("invalid paired shadow observation session") from exc
    paired_shadow = _load_paired_shadow_module()

    def decide(**kwargs):
        decision_request = {
            "schema_version": PAIRED_SHADOW_DECISION_SCHEMA,
            "as_of": kwargs["as_of"],
            "portfolio": kwargs["portfolio"],
            "market_data": kwargs["market_data"],
            "parameter_override": {
                "blend_gate_mid_soxl_weight": kwargs["mid_soxl_weight"]
            },
        }
        return _run_isolated_json_mode(
            decision_request,
            command=(
                "uv", "run", "--locked", "--no-editable", "--project",
                str(ues_project), "python",
            ),
            arguments=("--source-paired-shadow-decision", "--p2-candidate", str(p2_candidate_path)),
        )

    financial = paired_shadow.advance_paired_shadow_session(
        session=session,
        baseline_state=_mapping(request["baseline_state"]),
        candidate_state=_mapping(request["candidate_state"]),
        cost_bps=float(request["cost_bps"]),
        decide=decide,
    )
    observed_at = str(financial["observed_at"])
    envelope_request = {
        "policy": request["policy"],
        "dependency_digests": request["dependency_digests"],
        "baseline_id": request["baseline_id"],
        "observation_session": observed_at[:10],
        "observed_at": observed_at,
        "input_snapshot_sha256": financial["input_snapshot_sha256"],
        "candidate": financial["candidate"],
        "baseline": financial["baseline"],
        "previous_forward_observation_receipt": request["previous_forward_observation_receipt"],
        "previous_paired_shadow_evidence": request["previous_paired_shadow_evidence"],
    }
    envelope = _run_isolated_json_mode(
        envelope_request,
        command=(str(qpk_python),),
        arguments=(
            "--source-paired-shadow-envelope", "--p2-candidate", str(p2_candidate_path)
        ),
    )
    envelope["baseline_state"] = financial["baseline_state"]
    envelope["candidate_state"] = financial["candidate_state"]
    return envelope


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--source-learning-replay")
    mode.add_argument("--source-paired-shadow-decision")
    mode.add_argument("--source-paired-shadow-envelope")
    mode.add_argument("--paired-shadow-session")
    mode.add_argument("--p1-binding", type=Path)
    parser.add_argument("--input-manifest", type=Path)
    parser.add_argument("--bars-member", type=Path)
    parser.add_argument("--ues-project", type=Path)
    parser.add_argument("--qpk-python", type=Path)
    parser.add_argument("--blend-gate-mid-soxl-weight", action="append", type=float)
    parser.add_argument("--promotion-validation-development-summary", type=Path)
    parser.add_argument("--p2-candidate", required=True, type=Path)
    args = parser.parse_args(argv)
    failed = False
    try:
        if args.source_paired_shadow_decision:
            result = _source_paired_shadow_decision(
                json.loads(Path(args.source_paired_shadow_decision).read_text()),
                json.loads(args.p2_candidate.read_text()),
            )
        elif args.source_paired_shadow_envelope:
            result = _source_paired_shadow_envelope(
                json.loads(Path(args.source_paired_shadow_envelope).read_text())
            )
        elif args.paired_shadow_session:
            if not all((args.ues_project, args.qpk_python, args.p2_candidate)):
                raise SoxlThreeAssetLearningError("invalid paired shadow arguments")
            result = run_paired_shadow_session(
                json.loads(Path(args.paired_shadow_session).read_text()),
                ues_project=args.ues_project,
                qpk_python=args.qpk_python,
                p2_candidate_path=args.p2_candidate,
            )
        elif args.source_learning_replay:
            result = _source_learning_replay(
                json.loads(Path(args.source_learning_replay).read_text()),
                json.loads(args.p2_candidate.read_text()),
            )
        elif args.promotion_validation_development_summary:
            if not all((args.p1_binding, args.input_manifest, args.bars_member, args.ues_project)) or args.blend_gate_mid_soxl_weight:
                raise SoxlThreeAssetLearningError("invalid validation arguments")
            proposal, baseline, candidate = run_fixed_validation_from_verified_p1(
                binding=json.loads(args.p1_binding.read_text()),
                manifest=json.loads(args.input_manifest.read_text()),
                member_bytes=args.bars_member.read_bytes(),
                development_summary=json.loads(args.promotion_validation_development_summary.read_text()),
                ues_project=args.ues_project,
                p2_candidate_path=args.p2_candidate,
            )
            result = _validation_output(proposal, baseline, candidate)
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
        if args.paired_shadow_session or args.source_paired_shadow_decision or args.source_paired_shadow_envelope:
            result = {
                "status": "PARKED",
                "stage": "paired_shadow",
                "failure_class": "paired_shadow_input_or_runtime_unavailable",
                "no_order": True,
                "live_authority_granted": False,
            }
        elif args.promotion_validation_development_summary:
            result = {
                "status": "PARKED",
                "stage": "promotion_validation",
                "failure_class": "validation_input_or_runtime_unavailable",
                "learning_only": True,
                "no_order": True,
                "size_zero_required": True,
                "promotion_eligible": False,
                "live_authority_granted": False,
            }
        else:
            result = {"schema_version": LEARNING_SCHEMA, "status": "PARKED", "failure_class": "learning_input_or_runtime_unavailable"}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
