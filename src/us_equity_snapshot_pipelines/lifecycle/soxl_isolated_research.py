"""Admission and explicit execution for a bounded SOXL development study.

The original package and its candidate identity are inputs, never new execution
authority. The default entry point only validates input. Explicit development
execution stays in memory and never writes market data or promotion records.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import stat
import time
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from importlib.metadata import distribution
from pathlib import Path
from typing import Any

from quant_platform_kit.common.execution_translation import (
    translate_value_decision_to_weight_targets,
)
from quant_platform_kit.common.strategy_contracts import PositionTarget, StrategyContext, StrategyDecision
from quant_platform_kit.position_sizing import risk_budgeted_target_weights
from quant_platform_kit.risk.engine import build_risk_engine
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult, OptimizationProposal
from us_equity_strategies.entrypoints import _build_soxl_soxx_trend_income_decision
from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

from . import soxl_pit_input_packager as packager
from .soxl_promotion_runner import (
    SOXL_PROMOTION_ASSETS,
    WindowEvidence,
    _FROZEN_FOLDS,
    _SoxlReplayCalculations,
)


UES_REVISION = "33d8c09a9aa517cde94f36d2f67e526c340ea6e9"
SOURCE_UES_REVISION = "15df2a42df5d230cfb03a7cb655fd4b226956681"
SOURCE_QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
DEFAULT_ENTRY_BUFFERS = (0.08, 0.10, 0.12)
MAX_TRIALS = 25
DEVELOPMENT_COST_BPS = (5.0, 10.0, 15.0)
DEVELOPMENT_TIMEOUT_SECONDS = 300.0
DEVELOPMENT_END = date.fromisoformat(_FROZEN_FOLDS[-1][-1])
_LEARNING_PROFILE = "soxl_soxx_trend_income_isolated_learning"
_SIMULATED_RISK_POLICY = "soxl_isolated_learning_simulation_v1"
_LOSS_BUDGET = 0.01
_EFFECTIVE_EXPOSURE_CAP = 0.50
_PRODUCT_FACTORS = {symbol: 3 if symbol == "SOXL" else 1 for symbol in SOXL_PROMOTION_ASSETS}
_PRODUCT_CAPS = {symbol: 0.15 if symbol == "SOXL" else 0.50 for symbol in SOXL_PROMOTION_ASSETS}
_MEMBERS = frozenset({"input.json", "input-manifest.json", "sessions.json"})
_COMPLETED_RESULT_MAX_BYTES = 1_048_576
_COMPLETED_RESULT_KEYS = frozenset(
    {
        "status",
        "failure_reason",
        "trial_count_requested",
        "trial_count_started",
        "trial_count_completed",
        "cost_scenario_count_started",
        "cost_scenario_count_completed",
        "trials",
        "window_class",
        "cost_scenarios_bps",
        "loss_budget",
        "stop_loss_distance",
        "effective_exposure_cap",
        "soxl_nominal_cap",
        "account_drawdown_breaker",
        "strategy_stop_breaker_count",
        "learning_only",
        "promotion_eligible",
        "live_ready",
        "size_zero_required",
        "no_order",
        "real_backtest_executed",
    }
)
_COMPLETED_SCENARIO_KEYS = frozenset(
    {
        "total_cost_bps",
        "total_return",
        "cagr",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown",
        "turnover",
        "costs_paid",
        "trade_count",
        "risk_assessment_count",
    }
)
_DEVELOPMENT_FAILURE_REASONS = frozenset(
    {
        "development_decision_failed",
        "development_input_invalid",
        "development_request_invalid",
        "development_risk_rejected",
        "development_timeout",
        "development_trial_failed",
    }
)


class SoxlIsolatedResearchError(ValueError):
    """Fixed failure category without source rows or underlying diagnostics."""


class _DiscardingResultSink:
    def save_backtest_result(self, _result: BacktestResult) -> None:
        return None


def _completed_result_invalid() -> None:
    raise SoxlIsolatedResearchError("completed_result_invalid")


def _completed_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _completed_result_invalid()
    number = float(value)
    if not math.isfinite(number):
        _completed_result_invalid()
    return number


def _completed_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _completed_result_invalid()
    return value


def evaluate_soxl_completed_research_result(
    document: dict[str, Any],
) -> OptimizationProposal:
    """Validate completed aggregate research and recommend hold or manual review."""
    try:
        if not isinstance(document, dict) or set(document) != {"result", "verification"}:
            _completed_result_invalid()
        result = document["result"]
        verification = document["verification"]
        if not isinstance(result, dict) or set(result) != _COMPLETED_RESULT_KEYS:
            _completed_result_invalid()
        if (
            not isinstance(verification, dict)
            or set(verification)
            != {
                "wrapper_status",
                "entrypoint_exit_code",
                "filevault_enabled",
                "original_tree_metadata_unchanged",
                "network_disabled",
            }
            or verification["wrapper_status"] != "entrypoint_returned"
            or type(verification["entrypoint_exit_code"]) is not int
            or verification["entrypoint_exit_code"] != 0
            or verification["filevault_enabled"] is not True
            or verification["original_tree_metadata_unchanged"] is not True
            or verification["network_disabled"] is not True
        ):
            _completed_result_invalid()
        if (
            result["status"] != "development_completed"
            or result["failure_reason"] is not None
            or result["window_class"] != "seen_development"
            or result["learning_only"] is not True
            or result["promotion_eligible"] is not False
            or result["live_ready"] is not False
            or result["size_zero_required"] is not True
            or result["no_order"] is not True
            or result["real_backtest_executed"] is not True
        ):
            _completed_result_invalid()
        fixed_numbers = {
            "loss_budget": 0.01,
            "stop_loss_distance": 0.05,
            "effective_exposure_cap": 0.50,
            "soxl_nominal_cap": 0.15,
            "account_drawdown_breaker": 0.10,
        }
        if any(_completed_number(result[key]) != expected for key, expected in fixed_numbers.items()):
            _completed_result_invalid()
        if _completed_count(result["strategy_stop_breaker_count"]) != 3:
            _completed_result_invalid()
        costs = result["cost_scenarios_bps"]
        if not isinstance(costs, list) or tuple(_completed_number(cost) for cost in costs) != DEVELOPMENT_COST_BPS:
            _completed_result_invalid()
        trials = result["trials"]
        if not isinstance(trials, list) or not 2 <= len(trials) <= MAX_TRIALS:
            _completed_result_invalid()
        trial_count = len(trials)
        if any(
            _completed_count(result[key]) != trial_count
            for key in (
                "trial_count_requested",
                "trial_count_started",
                "trial_count_completed",
            )
        ):
            _completed_result_invalid()
        expected_scenario_count = trial_count * len(DEVELOPMENT_COST_BPS)
        if any(
            _completed_count(result[key]) != expected_scenario_count
            for key in (
                "cost_scenario_count_started",
                "cost_scenario_count_completed",
            )
        ):
            _completed_result_invalid()

        parameter_values: list[float] = []
        comparable: list[tuple[tuple[float | int, ...], ...]] = []
        metric_keys = tuple(sorted(_COMPLETED_SCENARIO_KEYS - {"total_cost_bps"}))
        for trial in trials:
            if (
                not isinstance(trial, dict)
                or set(trial) != {"status", "trend_entry_buffer", "cost_scenarios"}
                or trial["status"] != "completed"
            ):
                _completed_result_invalid()
            parameter = _completed_number(trial["trend_entry_buffer"])
            if not 0.08 <= parameter <= 0.12:
                _completed_result_invalid()
            parameter_values.append(parameter)
            scenarios = trial["cost_scenarios"]
            if not isinstance(scenarios, list) or len(scenarios) != len(DEVELOPMENT_COST_BPS):
                _completed_result_invalid()
            trial_metrics: list[tuple[float | int, ...]] = []
            for expected_cost, scenario in zip(DEVELOPMENT_COST_BPS, scenarios, strict=True):
                if not isinstance(scenario, dict) or set(scenario) != _COMPLETED_SCENARIO_KEYS:
                    _completed_result_invalid()
                if _completed_number(scenario["total_cost_bps"]) != expected_cost:
                    _completed_result_invalid()
                values: list[float | int] = []
                for key in metric_keys:
                    value = scenario[key]
                    if key in {"trade_count", "risk_assessment_count"}:
                        values.append(_completed_count(value))
                    else:
                        values.append(_completed_number(value))
                trial_metrics.append(tuple(values))
            comparable.append(tuple(trial_metrics))
        if parameter_values[0] != 0.08 or any(
            left >= right for left, right in zip(parameter_values, parameter_values[1:])
        ):
            _completed_result_invalid()
    except SoxlIsolatedResearchError:
        raise
    except Exception:
        raise SoxlIsolatedResearchError("completed_result_invalid") from None

    recommendation = "hold" if all(item == comparable[0] for item in comparable[1:]) else "requires_review"
    baseline = {"trend_entry_buffer": 0.08}
    return OptimizationProposal(
        strategy_profile=_LEARNING_PROFILE,
        domain="us_equity",
        current_params=baseline,
        proposed_params=baseline,
        improvement_score=0.0,
        confidence=0.0,
        recommendation=recommendation,
        walk_forward_passed=False,
        optimization_method="grid_search_seen_development_result_only",
        search_iterations=len(parameter_values),
        computed_at="",
    )


def load_soxl_completed_research_result(
    path: str | Path,
    *,
    expected_sha256: str,
) -> OptimizationProposal:
    """Read one bounded aggregate artifact and bind it to an expected byte digest."""
    try:
        expected = _digest(expected_sha256)
        result_path = Path(path).absolute()
        packager._reject_symlink_ancestors(result_path)
        descriptor = os.open(result_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or not 0 < info.st_size <= _COMPLETED_RESULT_MAX_BYTES
            ):
                _completed_result_invalid()
            raw = stream.read(_COMPLETED_RESULT_MAX_BYTES + 1)
        if len(raw) != info.st_size or hashlib.sha256(raw).hexdigest() != expected:
            _completed_result_invalid()
        document = _json(raw)
        if not isinstance(document, dict):
            _completed_result_invalid()
        return evaluate_soxl_completed_research_result(document)
    except SoxlIsolatedResearchError:
        raise
    except Exception:
        raise SoxlIsolatedResearchError("completed_result_invalid") from None


class SoxlDevelopmentLearningRunner(_SoxlReplayCalculations):
    """In-memory, non-promotion adapter over the existing SOXL event calculations."""

    def __init__(
        self,
        input_payload: dict[str, Any],
        runtime_config: dict[str, Any],
        *,
        source_identity: dict[str, Any],
        deadline: float,
        indicator_cache: dict[int, dict[str, Any]] | None = None,
    ) -> None:
        if set(input_payload) != {"schema_version", "input_manifest", "sessions"}:
            raise SoxlIsolatedResearchError("development_input_invalid")
        sessions = input_payload["sessions"]
        if not isinstance(sessions, list) or len(sessions) != 2_010:
            raise SoxlIsolatedResearchError("development_input_invalid")
        self.sessions = copy.deepcopy(sessions)
        self._date_to_index = {
            date.fromisoformat(session["date"]): index
            for index, session in enumerate(self.sessions)
        }
        if (
            not self.sessions
            or date.fromisoformat(self.sessions[0]["date"]) not in self._date_to_index
            or DEVELOPMENT_END not in self._date_to_index
        ):
            raise SoxlIsolatedResearchError("development_input_invalid")
        frozen = dict(soxl_soxx_trend_income_manifest.default_config)
        entry_buffer = runtime_config.get("trend_entry_buffer")
        if (
            isinstance(entry_buffer, bool)
            or not isinstance(entry_buffer, (int, float))
            or not 0.08 <= entry_buffer <= 0.12
        ):
            raise SoxlIsolatedResearchError("development_request_invalid")
        expected_runtime_config = copy.deepcopy(frozen)
        expected_runtime_config["trend_entry_buffer"] = entry_buffer
        if (
            packager.canonical_json_bytes(runtime_config)
            != packager.canonical_json_bytes(expected_runtime_config)
            or source_identity.get("strategy_revision") != SOURCE_UES_REVISION
            or source_identity.get("qpk_revision") != SOURCE_QPK_REVISION
        ):
            raise SoxlIsolatedResearchError("development_request_invalid")
        self.config = {"frozen_strategy_config": copy.deepcopy(runtime_config)}
        self.source_identity = copy.deepcopy(source_identity)
        self.variant_id = "explicit_qqq_fallback"
        self.initial_equity = 100_000.0
        self.stop_loss_distance = 0.05
        self.mandate = {"product_leverage_factors": dict(_PRODUCT_FACTORS)}
        self._assessment_clock = lambda: datetime.now(UTC)
        self._risk_engine = build_risk_engine()
        self._deadline = deadline
        self._indicator_cache = indicator_cache if indicator_cache is not None else {}
        self.scenarios: tuple[WindowEvidence, ...] = ()
        self.scenario_started_count = 0

    def _check_deadline(self) -> None:
        if time.monotonic() > self._deadline:
            raise SoxlIsolatedResearchError("development_timeout")

    def _execute_open(self, index: int, state, *, total_cost_bps: float) -> None:
        self._check_deadline()
        return super()._execute_open(index, state, total_cost_bps=total_cost_bps)

    @staticmethod
    def _validated_targets(target_weights: dict[str, float]) -> dict[str, float]:
        if not isinstance(target_weights, dict):
            raise SoxlIsolatedResearchError("development_risk_rejected")
        normalized: dict[str, float] = {}
        for symbol, value in target_weights.items():
            if (
                symbol not in _PRODUCT_FACTORS
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise SoxlIsolatedResearchError("development_risk_rejected")
            try:
                number = float(value)
            except (OverflowError, TypeError, ValueError):
                raise SoxlIsolatedResearchError("development_risk_rejected") from None
            if not math.isfinite(number) or not 0.0 <= number <= _PRODUCT_CAPS[symbol]:
                raise SoxlIsolatedResearchError("development_risk_rejected")
            if number > 0.0:
                normalized[symbol] = number
        effective = sum(normalized[symbol] * _PRODUCT_FACTORS[symbol] for symbol in normalized)
        if effective > _EFFECTIVE_EXPOSURE_CAP + 1e-9:
            raise SoxlIsolatedResearchError("development_risk_rejected")
        return normalized

    def _risk_sanity(
        self,
        index: int,
        state,
        target_weights: dict[str, float],
        *,
        reason: str,
        market_regime: dict[str, Any] | None = None,
        price_field: str = "close",
        require_full_approval: bool = False,
    ) -> dict[str, float]:
        targets = self._validated_targets(target_weights)
        decision = StrategyDecision(
            positions=tuple(
                PositionTarget(symbol=symbol, target_weight=weight, role="research_simulation")
                for symbol, weight in sorted(targets.items())
            ),
            diagnostics={"isolated_learning_control": reason},
        )
        observed_regime = (
            self.sessions[index]["market_regime"]
            if market_regime is None
            else market_regime
        )
        snapshot = self._portfolio_snapshot(
            index,
            state,
            market_regime=observed_regime,
            price_field=price_field,
        )
        try:
            action = self._risk_engine.assess(
                decision,
                snapshot,
                market_data={"market_regime": observed_regime},
            )
        except Exception:
            raise SoxlIsolatedResearchError("development_risk_rejected") from None
        state.assessment_count += 1
        if action.action != "approve":
            raise SoxlIsolatedResearchError("development_risk_rejected")
        scalars = (
            action.budget_scalar,
            action.leverage_scalar,
            action.risk_asset_scalar,
        )
        normalized_scalars: list[float] = []
        for value in scalars:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SoxlIsolatedResearchError("development_risk_rejected")
            try:
                number = float(value)
            except (OverflowError, TypeError, ValueError):
                raise SoxlIsolatedResearchError("development_risk_rejected") from None
            if not math.isfinite(number) or not 0.0 <= number <= 1.0:
                raise SoxlIsolatedResearchError("development_risk_rejected")
            normalized_scalars.append(number)
        scalar = min(normalized_scalars)
        if require_full_approval and scalar != 1.0:
            raise SoxlIsolatedResearchError("development_risk_rejected")
        return self._validated_targets(
            {symbol: weight * scalar for symbol, weight in targets.items()}
        )

    def _assess_control(
        self,
        index: int,
        state,
        target_weights,
        *,
        reason: str,
        normalization_origin_weights=None,
    ) -> dict[str, float]:
        del normalization_origin_weights
        if reason == "executable_5pct_stop":
            if index <= 0:
                raise SoxlIsolatedResearchError("development_risk_rejected")
            return self._risk_sanity(
                index,
                state,
                dict(target_weights),
                reason=reason,
                market_regime=self.sessions[index - 1]["market_regime"],
                price_field="open",
                require_full_approval=True,
            )
        return self._risk_sanity(index, state, dict(target_weights), reason=reason)

    def _evaluate_close(self, index: int, state) -> dict[str, float]:
        drawdown = max(0.0, 1.0 - state.last_equity / state.high_water_equity)
        if drawdown > 0.10:
            state.account_parked = True
        if state.stop_count >= 3:
            state.strategy_parked = True
        if state.account_parked or state.strategy_parked:
            return self._assess_control(index, state, {}, reason="breaker_flatten")
        try:
            from quant_platform_kit import build_semiconductor_rotation_indicators_from_history

            indicators = self._indicator_cache.get(index)
            if indicators is None:
                indicators = build_semiconductor_rotation_indicators_from_history(
                    soxl_history=[
                        float(self.sessions[offset]["bars"]["SOXL"]["close"])
                        for offset in range(index + 1)
                    ],
                    soxx_history=[
                        float(self.sessions[offset]["bars"]["SOXX"]["close"])
                        for offset in range(index + 1)
                    ],
                )
                self._indicator_cache[index] = copy.deepcopy(indicators)
            else:
                indicators = copy.deepcopy(indicators)
            regime = self.sessions[index]["market_regime"]
            snapshot = self._portfolio_snapshot(index, state, market_regime=regime)
            context = StrategyContext(
                as_of=self.sessions[index]["date"],
                market_data={"derived_indicators": indicators},
                portfolio=snapshot,
                state=copy.deepcopy(state.strategy_state),
                runtime_config=copy.deepcopy(self.config["frozen_strategy_config"]),
            )
            raw = _build_soxl_soxx_trend_income_decision(context)
            weighted = translate_value_decision_to_weight_targets(
                raw,
                total_equity=float(snapshot.total_equity),
            )
        except Exception:
            raise SoxlIsolatedResearchError("development_decision_failed") from None
        eligible = frozenset(self.sessions[index]["eligible_assets"])
        raw_weights = {
            position.symbol: float(position.target_weight)
            for position in weighted.positions
            if position.target_weight is not None
        }
        if set(raw_weights) - set(soxl_soxx_trend_income_manifest.default_config["managed_symbols"]):
            raise SoxlIsolatedResearchError("development_decision_failed")
        if "QQQI" not in eligible and "QQQI" in raw_weights:
            if "QQQ" not in eligible:
                raise SoxlIsolatedResearchError("development_decision_failed")
            raw_weights["QQQ"] = raw_weights.pop("QQQI")
        raw_weights = {symbol: weight for symbol, weight in raw_weights.items() if symbol in eligible}
        scalar = 0.5 if drawdown > 0.05 else 1.0
        targets = risk_budgeted_target_weights(
            raw_target_weights=raw_weights,
            risk_mandate_id=_SIMULATED_RISK_POLICY,
            risk_fraction=_LOSS_BUDGET,
            stop_loss_distances={symbol: self.stop_loss_distance for symbol in raw_weights},
            drawdown_scalar=scalar,
            available_effective_exposure=_EFFECTIVE_EXPOSURE_CAP,
            product_leverage_factors={symbol: _PRODUCT_FACTORS[symbol] for symbol in raw_weights},
            inputs_fresh=True,
        )
        if any(weight > 0.0 for weight in raw_weights.values()) and not targets:
            raise SoxlIsolatedResearchError("development_risk_rejected")
        targets = self._risk_sanity(index, state, targets, reason="candidate_decision")
        state.strategy_state = {
            "last_signal_session": self.sessions[index]["date"],
            "last_active_risk_asset": raw.diagnostics.get("active_risk_asset"),
            "last_blend_tier": raw.diagnostics.get("blend_tier"),
            "last_income_ratio": raw.diagnostics.get("income_ratio_text"),
        }
        return targets

    def run(
        self,
        strategy_profile: str,
        params: dict[str, Any],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> BacktestResult:
        expected = {
            "trend_entry_buffer": self.config["frozen_strategy_config"]["trend_entry_buffer"],
            "learning_only": True,
            "no_order": True,
        }
        if (
            strategy_profile != _LEARNING_PROFILE
            or params != expected
            or start_date != date.fromisoformat(self.sessions[0]["date"])
            or end_date != DEVELOPMENT_END
        ):
            raise SoxlIsolatedResearchError("development_request_invalid")
        scenarios: list[WindowEvidence] = []
        for cost_bps in DEVELOPMENT_COST_BPS:
            self._check_deadline()
            self.scenario_started_count += 1
            scenarios.append(self._replay_window(start_date, end_date, cost_bps))
            self.scenarios = tuple(scenarios)
        return replace(
            self.scenarios[0].result,
            strategy_profile=_LEARNING_PROFILE,
            params=dict(expected),
            param_set_id=f"entry_buffer_{expected['trend_entry_buffer']:.3f}",
            source_script="soxl_isolated_research",
            oos_sharpe=None,
            oos_calmar=None,
            oos_max_drawdown=None,
            walk_forward_stability=None,
        )


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError()
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError()
    return parsed.astimezone(UTC)


def _digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError()
    return value


def _json(payload: bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError()
            result[key] = value
        return result

    def invalid(_value):
        raise ValueError()

    return json.loads(payload, object_pairs_hook=pairs, parse_constant=invalid)


def _read_private(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError()
        return stream.read()


def _parameter_values(values: Sequence[float], provider: str) -> tuple[float, ...]:
    if (
        provider != "codex" or not isinstance(values, (list, tuple))
        or not 1 <= len(values) <= MAX_TRIALS
        or any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in values)
        or any(not 0.08 <= value <= 0.12 or not math.isfinite(value) for value in values)
        or values[0] != 0.08
        or any(left >= right for left, right in zip(values, values[1:]))
    ):
        raise SoxlIsolatedResearchError("research_request_invalid")
    return tuple(float(value) for value in values)


def _frozen_config() -> dict[str, Any]:
    for name, revision in (("us-equity-strategies", UES_REVISION), ("quant-platform-kit", packager.QPK_REVISION)):
        direct = _json((distribution(name).read_text("direct_url.json") or "{}").encode())
        if direct.get("vcs_info", {}).get("commit_id") != revision:
            raise ValueError()
    config = copy.deepcopy(dict(soxl_soxx_trend_income_manifest.default_config))
    if config.get("trend_entry_buffer") != 0.08:
        raise ValueError()
    return config


def preflight_soxl_isolated_research(
    snapshot_dir: str | Path,
    *,
    expected_package_sha256: str,
    expected_input_manifest_sha256: str,
    entry_buffers: Sequence[float] = DEFAULT_ENTRY_BUFFERS,
    execution_provider: str = "codex",
) -> dict[str, Any]:
    """Validate original input bytes and prepare at most 25 learning configurations.

    The returned paths identify the original private input, not a copy. Source
    identity remains the original candidate's identity. The runtime configs are
    independent copies for development only, not promotion configurations or
    evidence. A later executing caller must recheck this gate at consumption.
    """
    values = _parameter_values(entry_buffers, execution_provider)
    try:
        frozen_config = _frozen_config()
    except Exception:
        raise SoxlIsolatedResearchError("research_dependency_mismatch") from None
    try:
        package_digest = _digest(expected_package_sha256)
        input_digest = _digest(expected_input_manifest_sha256)
        root = Path(snapshot_dir).absolute()
        packager._reject_symlink_ancestors(root)
        if not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700:
            raise ValueError()
        if {path.name for path in root.iterdir()} != _MEMBERS | {"package-manifest.json"}:
            raise ValueError()
        raw_package = _read_private(root / "package-manifest.json")
        if hashlib.sha256(raw_package).hexdigest() != package_digest:
            raise ValueError()
        package = _json(raw_package)
        if (
            package["schema_version"] != "soxl_core_only_9_input_package_manifest.v1"
            or package["package_type"] != "promotion_research_input_static_only"
            or package["candidate_id"] != packager.CANDIDATE_ID
            or package["input_contract_id"] != packager.INPUT_CONTRACT_ID
            or package["input_manifest_sha256"] != input_digest
        ):
            raise ValueError()
        source = package["source_contract"]
        now = _now()
        if now.tzinfo is None or source["data_class"] != "provider_observed":
            raise ValueError()
        if _timestamp(source["as_of"]) > now:
            raise ValueError()
        logical = source["logical_inputs"]
        if len(logical) != 9 or any(
            _timestamp(item["retention_expires_at"]) <= now
            or item["retention_scope"] != "filevault_local_encrypted_immutable_internal_research_only"
            for item in logical
        ):
            raise ValueError()
        if package["lifecycle_claims"] != {
            "promotion_eligible": False, "live_ready": False, "paper_authority": False,
            "shadow_authority": False, "live_authority": False, "order_authority": False,
            "position_control_allowed": False, "size_zero_required": True, "no_order": True,
            "real_producer": True, "synthetic_fixture": False, "real_backtest_executed": False,
        }:
            raise ValueError()
        members = package["members"]
        if len(members) != 3 or {member["path"] for member in members} != _MEMBERS:
            raise ValueError()
        contents = {}
        member_digests = {}
        for member in members:
            if (
                set(member) != {"path", "media_type", "size_bytes", "sha256"}
                or member["media_type"] != "application/json"
                or type(member["size_bytes"]) is not int
            ):
                raise ValueError()
            data = _read_private(root / member["path"])
            if len(data) != member["size_bytes"] or hashlib.sha256(data).hexdigest() != _digest(member["sha256"]):
                raise ValueError()
            contents[member["path"]] = data
            member_digests[member["path"]] = member["sha256"]
        source_digest = _digest(package["identity"]["source_contract_sha256"])
        # Reuse the existing pure packager to verify its original canonical bytes,
        # including source, calendar, availability and prefix provenance. Nothing
        # is published and no new authority/receipt is created or consumed.
        sessions = _json(contents["sessions.json"])
        prepared = packager.prepare_soxl_pit_input(
            [{"date": row["date"], "bars": row["bars"]} for row in sessions],
            source, trusted_regime_source_contract_sha256=source_digest,
        )
        if (
            prepared.sessions_bytes != contents["sessions.json"]
            or prepared.input_bytes != contents["input.json"]
            or prepared.input_manifest_bytes != contents["input-manifest.json"]
            or prepared.input_manifest_sha256 != input_digest
            or _json(prepared.contract_bytes) != package["contract"]
            or package["identity"]["strategy_revision"] != SOURCE_UES_REVISION
        ):
            raise ValueError()
        source_identity = packager._validate_binding(
            prepared,
            package["identity"],
            expected_qpk_revision=SOURCE_QPK_REVISION,
        )
    except Exception:
        raise SoxlIsolatedResearchError("snapshot_invalid") from None
    configs = tuple(copy.deepcopy(frozen_config) for _ in values)
    for config, value in zip(configs, values, strict=True):
        config["trend_entry_buffer"] = value
    return {
        "input_path": root / "input.json",
        "input_manifest_path": root / "input-manifest.json",
        "package_sha256": package_digest,
        "input_manifest_sha256": input_digest,
        "input_sha256": member_digests["input.json"],
        "source_identity": source_identity,
        "runtime_configs": configs,
        "parameter_values": values,
        "execution_provider": "codex",
        "max_trials": MAX_TRIALS,
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
        "real_backtest_executed": False,
    }


def _scenario_summary(cost_bps: float, evidence: WindowEvidence) -> dict[str, Any]:
    result = evidence.result
    return {
        "total_cost_bps": cost_bps,
        "total_return": result.total_return,
        "cagr": result.cagr,
        "sharpe_ratio": result.sharpe_ratio,
        "sortino_ratio": result.sortino_ratio,
        "max_drawdown": result.max_drawdown,
        "turnover": evidence.turnover,
        "costs_paid": evidence.costs_paid,
        "trade_count": evidence.trade_count,
        "risk_assessment_count": evidence.assessment_count,
    }


def run_soxl_isolated_development(
    snapshot_dir: str | Path,
    *,
    expected_package_sha256: str,
    expected_input_manifest_sha256: str,
    entry_buffers: Sequence[float] = DEFAULT_ENTRY_BUFFERS,
    execution_provider: str = "codex",
    timeout_seconds: float = DEVELOPMENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run bounded, seen-development trials after repeating the current input gate."""
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(float(timeout_seconds))
        or not 0.0 < float(timeout_seconds) <= DEVELOPMENT_TIMEOUT_SECONDS
    ):
        raise SoxlIsolatedResearchError("research_request_invalid")
    admission = preflight_soxl_isolated_research(
        snapshot_dir,
        expected_package_sha256=expected_package_sha256,
        expected_input_manifest_sha256=expected_input_manifest_sha256,
        entry_buffers=entry_buffers,
        execution_provider=execution_provider,
    )
    try:
        input_bytes = _read_private(admission["input_path"])
        if hashlib.sha256(input_bytes).hexdigest() != admission["input_sha256"]:
            raise ValueError()
        input_payload = _json(input_bytes)
    except Exception:
        raise SoxlIsolatedResearchError("snapshot_invalid") from None
    deadline = time.monotonic() + float(timeout_seconds)
    completed: list[dict[str, Any]] = []
    failure_reason: str | None = None
    indicator_cache: dict[int, dict[str, Any]] = {}
    trial_started_count = 0
    scenario_started_count = 0
    scenario_completed_count = 0
    for config in admission["runtime_configs"]:
        runner: SoxlDevelopmentLearningRunner | None = None
        try:
            runner = SoxlDevelopmentLearningRunner(
                input_payload,
                config,
                source_identity=admission["source_identity"],
                deadline=deadline,
                indicator_cache=indicator_cache,
            )
            orchestrator = BacktestOrchestrator(store=_DiscardingResultSink())
            orchestrator.register_runner("us_equity", runner)
            trial_started_count += 1
            orchestrator.run(
                _LEARNING_PROFILE,
                domain="us_equity",
                params={
                    "trend_entry_buffer": config["trend_entry_buffer"],
                    "learning_only": True,
                    "no_order": True,
                },
                start_date=date.fromisoformat(runner.sessions[0]["date"]),
                end_date=DEVELOPMENT_END,
            )
            if len(runner.scenarios) != len(DEVELOPMENT_COST_BPS):
                raise SoxlIsolatedResearchError("development_trial_failed")
            scenario_started_count += runner.scenario_started_count
            scenario_completed_count += len(runner.scenarios)
            completed.append(
                {
                    "status": "completed",
                    "trend_entry_buffer": config["trend_entry_buffer"],
                    "cost_scenarios": tuple(
                        _scenario_summary(cost_bps, evidence)
                        for cost_bps, evidence in zip(
                            DEVELOPMENT_COST_BPS, runner.scenarios, strict=True
                        )
                    ),
                }
            )
        except SoxlIsolatedResearchError as exc:
            reason = str(exc)
            failure_reason = (
                reason
                if reason in _DEVELOPMENT_FAILURE_REASONS
                else "development_trial_failed"
            )
            if runner is not None:
                scenario_started_count += runner.scenario_started_count
                scenario_completed_count += len(runner.scenarios)
                if runner.scenario_started_count:
                    completed.append(
                        {
                            "status": "incomplete",
                            "trend_entry_buffer": config["trend_entry_buffer"],
                            "cost_scenarios": tuple(
                                _scenario_summary(cost_bps, evidence)
                                for cost_bps, evidence in zip(
                                    DEVELOPMENT_COST_BPS,
                                    runner.scenarios,
                                    strict=False,
                                )
                            ),
                        }
                    )
            break
        except Exception:
            failure_reason = "development_trial_failed"
            if runner is not None:
                scenario_started_count += runner.scenario_started_count
                scenario_completed_count += len(runner.scenarios)
                if runner.scenario_started_count:
                    completed.append(
                        {
                            "status": "incomplete",
                            "trend_entry_buffer": config["trend_entry_buffer"],
                            "cost_scenarios": tuple(
                                _scenario_summary(cost_bps, evidence)
                                for cost_bps, evidence in zip(
                                    DEVELOPMENT_COST_BPS,
                                    runner.scenarios,
                                    strict=False,
                                )
                            ),
                        }
                    )
            break
    return {
        "status": (
            "development_completed" if failure_reason is None else "development_incomplete"
        ),
        "failure_reason": failure_reason,
        "trial_count_requested": len(admission["runtime_configs"]),
        "trial_count_started": trial_started_count,
        "trial_count_completed": sum(trial["status"] == "completed" for trial in completed),
        "cost_scenario_count_started": scenario_started_count,
        "cost_scenario_count_completed": scenario_completed_count,
        "trials": tuple(completed),
        "window_class": "seen_development",
        "cost_scenarios_bps": DEVELOPMENT_COST_BPS,
        "loss_budget": _LOSS_BUDGET,
        "stop_loss_distance": 0.05,
        "effective_exposure_cap": _EFFECTIVE_EXPOSURE_CAP,
        "soxl_nominal_cap": _PRODUCT_CAPS["SOXL"],
        "account_drawdown_breaker": 0.10,
        "strategy_stop_breaker_count": 3,
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
        "real_backtest_executed": scenario_started_count > 0,
    }
