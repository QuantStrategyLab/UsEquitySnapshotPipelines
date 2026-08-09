"""Offline TQQQ ETF-only promotion-runner contract.

The runner accepts only caller-supplied immutable synthetic/PIT replay material.
It has no provider, broker, order-client, or runtime integration.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import statistics
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import (
    BacktestResult,
    PromotionBacktestRun,
    PromotionCostModel,
    PurgedWalkForwardFold,
)

_QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
_UES_REVISION = "15df2a42df5d230cfb03a7cb655fd4b226956681"
_PROFILE = "tqqq_etf_only_single_strategy_research_v1"
_DOMAIN = "us_equity"
_ALLOWED_ASSETS = frozenset({"TQQQ", "BOXX"})
_COST_SCENARIOS_BPS = (5, 10, 15)


class TqqqPromotionContractError(ValueError):
    """Fail-closed error for noncanonical promotion-research material."""


@dataclass(frozen=True)
class TqqqPromotionIdentity:
    qpk_revision: str
    ues_revision: str
    runner_revision: str
    platform_execution_revision: str
    config_sha256: str
    input_manifest_sha256: str
    mandate_receipt_sha256: str
    initial_state_sha256: str


@dataclass(frozen=True)
class TqqqPromotionPlan:
    folds: tuple[PurgedWalkForwardFold, ...]
    locked_oos_start: date
    locked_oos_end: date
    purge_days: int
    embargo_days: int


@dataclass(frozen=True)
class TqqqWindowReplay:
    start_date: date
    end_date: date
    prior_state_sha256: str
    final_state_sha256: str
    strategy_equity: tuple[float, ...]
    qqq_total_return_equity: tuple[float, ...]
    asset_weights: tuple[tuple[str, float], ...]
    turnover: float
    trade_count: int
    decision_count: int
    risk_assessment_count: int
    warmup_sessions: int
    market_regime_control_sha256: str = "a" * 64
    risk_active_state_sha256: str = "b" * 64
    volatility_hysteresis_state_sha256: str = "c" * 64
    retention_state_sha256: str = "d" * 64
    data_available: bool = True
    signal_effective_after_trading_days: int = 1
    state_continuity: str = "continuous"
    cash_reset: bool = False
    income_layer_enabled: bool = False
    option_overlay_enabled: bool = False
    option_growth_overlay_enabled: bool = False
    option_income_overlay_enabled: bool = False
    order_intents: tuple[object, ...] = ()
    executable_plan: tuple[object, ...] = ()
    execution_authorized: bool = False


@dataclass(frozen=True)
class TqqqQqqRelativeMetrics:
    benchmark_symbol: str
    strategy_total_return: float
    qqq_total_return: float
    excess_total_return: float
    strategy_cagr: float
    qqq_cagr: float
    excess_cagr: float
    strategy_max_drawdown: float
    qqq_max_drawdown: float
    max_drawdown_delta: float
    strategy_recovery_sessions: int | None
    qqq_recovery_sessions: int | None
    strategy_unrecovered_at_end: bool
    qqq_unrecovered_at_end: bool
    up_market_capture: float
    down_market_capture: float
    alpha: float
    beta: float
    information_ratio: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    annualized_volatility: float
    var_95: float
    cvar_95: float
    turnover: float
    trade_count: int
    win_rate: float
    profit_factor: float
    information_coefficient: float


@dataclass(frozen=True)
class TqqqWindowEvidence:
    start_date: date
    end_date: date
    prior_state_sha256: str
    final_state_sha256: str
    relative_metrics: TqqqQqqRelativeMetrics


@dataclass(frozen=True)
class TqqqCostScenarioResult:
    total_cost_bps: int
    cost_model_scope: str
    promotion_run: PromotionBacktestRun
    windows: tuple[TqqqWindowEvidence, ...]


@dataclass(frozen=True)
class TqqqPromotionResearchResult:
    identity: TqqqPromotionIdentity
    timing_sha256: str
    scenarios: tuple[TqqqCostScenarioResult, ...]
    authority_scope: str = "RESEARCH_ONLY"
    no_order: bool = True
    size_zero_required: bool = True
    promotion_eligible: bool = False
    live_ready: bool = False
    executable_plan: tuple[object, ...] = ()
    order_client_intents: tuple[object, ...] = ()


ReplayWindow = Callable[[date, date, int, str], TqqqWindowReplay]


class _MemoryPerformanceStore:
    def __init__(self) -> None:
        self.results: list[BacktestResult] = []

    def save_backtest_result(self, result: BacktestResult) -> None:
        self.results.append(result)


def _is_hex(value: object, length: int) -> bool:
    return (
        type(value) is str
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _finite(value: object, label: str, *, nonnegative: bool = False) -> float:
    if type(value) not in {int, float}:
        raise TqqqPromotionContractError(f"invalid {label}")
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise TqqqPromotionContractError(f"invalid {label}") from exc
    if not math.isfinite(number) or (nonnegative and number < 0.0):
        raise TqqqPromotionContractError(f"invalid {label}")
    return number


def _validate_identity(identity: TqqqPromotionIdentity) -> None:
    if type(identity) is not TqqqPromotionIdentity:
        raise TqqqPromotionContractError("invalid immutable identity")
    if identity.qpk_revision != _QPK_REVISION:
        raise TqqqPromotionContractError("QPK revision mismatch")
    if identity.ues_revision != _UES_REVISION:
        raise TqqqPromotionContractError("UES revision mismatch")
    for label, value in (
        ("runner revision", identity.runner_revision),
        ("platform execution revision", identity.platform_execution_revision),
    ):
        if not _is_hex(value, 40):
            raise TqqqPromotionContractError(f"invalid {label}")
    for label, value in (
        ("config", identity.config_sha256),
        ("input manifest", identity.input_manifest_sha256),
        ("mandate receipt", identity.mandate_receipt_sha256),
        ("initial state", identity.initial_state_sha256),
    ):
        if not _is_hex(value, 64):
            raise TqqqPromotionContractError(f"invalid {label} identity")


def _validate_plan(plan: TqqqPromotionPlan) -> None:
    if type(plan) is not TqqqPromotionPlan or type(plan.folds) is not tuple:
        raise TqqqPromotionContractError("invalid promotion timing plan")
    if len(plan.folds) < 3 or any(type(fold) is not PurgedWalkForwardFold for fold in plan.folds):
        raise TqqqPromotionContractError("at least three typed purged folds are required")
    if type(plan.locked_oos_start) is not date or type(plan.locked_oos_end) is not date:
        raise TqqqPromotionContractError("invalid locked OOS timing")
    if (
        type(plan.purge_days) is not int
        or plan.purge_days <= 0
        or type(plan.embargo_days) is not int
        or plan.embargo_days <= 0
    ):
        raise TqqqPromotionContractError("purge and embargo must be positive integers")


def _timing_sha256(plan: TqqqPromotionPlan) -> str:
    material = {
        "folds": [fold.to_dict() for fold in plan.folds],
        "locked_oos_start": plan.locked_oos_start.isoformat(),
        "locked_oos_end": plan.locked_oos_end.isoformat(),
        "purge_days": plan.purge_days,
        "embargo_days": plan.embargo_days,
        "signal_effective_after_trading_days": 1,
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _resolve_runner_revision() -> str:
    try:
        distribution = importlib.metadata.distribution("us-equity-snapshot-pipelines")
        raw = distribution.read_text("direct_url.json")
        direct_url = json.loads(raw) if raw else {}
        revision = direct_url.get("vcs_info", {}).get("commit_id")
        if _is_hex(revision, 40):
            return revision
    except (importlib.metadata.PackageNotFoundError, json.JSONDecodeError, TypeError, ValueError):
        pass

    repository = Path(__file__).resolve().parents[3]
    try:
        status = subprocess.run(
            ["git", "-C", str(repository), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        )
        if status.stdout:
            raise TqqqPromotionContractError("runner implementation checkout is not immutable")
        completed = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise TqqqPromotionContractError("runner revision is unavailable") from exc
    revision = completed.stdout.strip()
    if not _is_hex(revision, 40):
        raise TqqqPromotionContractError("invalid installed runner revision")
    return revision


def _returns(equity: tuple[float, ...]) -> tuple[float, ...]:
    return tuple(equity[index] / equity[index - 1] - 1.0 for index in range(1, len(equity)))


def _annualized_ratio(values: tuple[float, ...], *, downside_only: bool = False) -> float:
    denominator_values = tuple(min(value, 0.0) for value in values) if downside_only else values
    if len(denominator_values) < 2:
        return 0.0
    denominator = statistics.pstdev(denominator_values)
    return statistics.fmean(values) / denominator * math.sqrt(252.0) if denominator > 0.0 else 0.0


def _correlation(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if len(left) < 2 or len(left) != len(right):
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    covariance = statistics.fmean(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    denominator = statistics.pstdev(left) * statistics.pstdev(right)
    return covariance / denominator if denominator > 0.0 else 0.0


def _drawdown_recovery(equity: tuple[float, ...]) -> tuple[float, int | None, bool]:
    high = equity[0]
    drawdown_start: int | None = None
    recovery_sessions: int | None = None
    maximum_drawdown = 0.0
    for index, value in enumerate(equity):
        if value >= high:
            if drawdown_start is not None:
                recovery_sessions = index - drawdown_start
                drawdown_start = None
            high = value
        else:
            if drawdown_start is None:
                drawdown_start = index - 1
            maximum_drawdown = min(maximum_drawdown, value / high - 1.0)
    return maximum_drawdown, recovery_sessions, drawdown_start is not None


def _cagr(first: float, last: float, start: date, end: date) -> float:
    years = (end - start).days / 365.25
    if years <= 0.0:
        raise TqqqPromotionContractError("invalid replay date window")
    return (last / first) ** (1.0 / years) - 1.0


def _quantile(values: tuple[float, ...], probability: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _validate_replay(
    replay: TqqqWindowReplay,
    *,
    start_date: date,
    end_date: date,
    prior_state_sha256: str,
) -> None:
    if type(replay) is not TqqqWindowReplay:
        raise TqqqPromotionContractError("invalid replay material")
    if replay.start_date != start_date or replay.end_date != end_date:
        raise TqqqPromotionContractError("replay timing mismatch")
    if replay.data_available is not True:
        raise TqqqPromotionContractError("data unavailable")
    if replay.prior_state_sha256 != prior_state_sha256 or not _is_hex(replay.final_state_sha256, 64):
        raise TqqqPromotionContractError("continuous state identity mismatch")
    for value in (
        replay.market_regime_control_sha256,
        replay.risk_active_state_sha256,
        replay.volatility_hysteresis_state_sha256,
        replay.retention_state_sha256,
    ):
        if not _is_hex(value, 64):
            raise TqqqPromotionContractError("required state/plugin identity is unavailable")
    if replay.signal_effective_after_trading_days != 1:
        raise TqqqPromotionContractError("signal timing must be next eligible session")
    if replay.state_continuity != "continuous" or replay.cash_reset is not False:
        raise TqqqPromotionContractError("cash reset is forbidden; state must be continuous")
    if type(replay.warmup_sessions) is not int or replay.warmup_sessions < 252:
        raise TqqqPromotionContractError("warmup must include at least 252 sessions")
    if replay.income_layer_enabled is not False:
        raise TqqqPromotionContractError("income layer must remain disabled")
    if (
        replay.option_overlay_enabled is not False
        or replay.option_growth_overlay_enabled is not False
        or replay.option_income_overlay_enabled is not False
    ):
        raise TqqqPromotionContractError("option overlay must remain disabled")
    if type(replay.order_intents) is not tuple or replay.order_intents:
        raise TqqqPromotionContractError("order intents are forbidden")
    if type(replay.executable_plan) is not tuple or replay.executable_plan:
        raise TqqqPromotionContractError("executable plan is forbidden")
    if replay.execution_authorized is not False:
        raise TqqqPromotionContractError("execution authority is forbidden")
    if type(replay.asset_weights) is not tuple:
        raise TqqqPromotionContractError("invalid ETF-only weights")
    seen: set[str] = set()
    nonzero = 0
    for item in replay.asset_weights:
        if type(item) is not tuple or len(item) != 2 or type(item[0]) is not str:
            raise TqqqPromotionContractError("invalid ETF-only weights")
        symbol, raw_weight = item
        if symbol not in _ALLOWED_ASSETS:
            raise TqqqPromotionContractError("ETF-only universe violation")
        if symbol in seen:
            raise TqqqPromotionContractError("duplicate ETF weight")
        seen.add(symbol)
        weight = _finite(raw_weight, "ETF weight", nonnegative=True)
        nonzero += int(weight > 0.0)
    if nonzero > 1:
        raise TqqqPromotionContractError("TQQQ and BOXX must be mutually exclusive")
    if (
        type(replay.decision_count) is not int
        or replay.decision_count <= 0
        or type(replay.risk_assessment_count) is not int
        or replay.risk_assessment_count != replay.decision_count
    ):
        raise TqqqPromotionContractError("RiskEngine assessment must occur exactly once per decision")
    if type(replay.trade_count) is not int or replay.trade_count < 0:
        raise TqqqPromotionContractError("invalid trade count")
    _finite(replay.turnover, "turnover", nonnegative=True)
    if (
        type(replay.strategy_equity) is not tuple
        or type(replay.qqq_total_return_equity) is not tuple
        or len(replay.strategy_equity) < 2
        or len(replay.strategy_equity) != len(replay.qqq_total_return_equity)
    ):
        raise TqqqPromotionContractError("aligned strategy/QQQ equity is required")
    for series in (replay.strategy_equity, replay.qqq_total_return_equity):
        if any(_finite(value, "equity") <= 0.0 for value in series):
            raise TqqqPromotionContractError("equity must be positive")


def _relative_metrics(replay: TqqqWindowReplay) -> TqqqQqqRelativeMetrics:
    strategy = tuple(float(value) for value in replay.strategy_equity)
    benchmark = tuple(float(value) for value in replay.qqq_total_return_equity)
    strategy_returns = _returns(strategy)
    benchmark_returns = _returns(benchmark)
    excess_returns = tuple(a - b for a, b in zip(strategy_returns, benchmark_returns))
    strategy_total = strategy[-1] / strategy[0] - 1.0
    benchmark_total = benchmark[-1] / benchmark[0] - 1.0
    strategy_cagr = _cagr(strategy[0], strategy[-1], replay.start_date, replay.end_date)
    benchmark_cagr = _cagr(benchmark[0], benchmark[-1], replay.start_date, replay.end_date)
    strategy_mdd, strategy_recovery, strategy_unrecovered = _drawdown_recovery(strategy)
    benchmark_mdd, benchmark_recovery, benchmark_unrecovered = _drawdown_recovery(benchmark)
    up_indexes = tuple(index for index, value in enumerate(benchmark_returns) if value > 0.0)
    down_indexes = tuple(index for index, value in enumerate(benchmark_returns) if value < 0.0)
    benchmark_up = sum(benchmark_returns[index] for index in up_indexes)
    benchmark_down = sum(benchmark_returns[index] for index in down_indexes)
    up_capture = (
        sum(strategy_returns[index] for index in up_indexes) / benchmark_up
        if benchmark_up > 0.0
        else 0.0
    )
    down_capture = (
        sum(strategy_returns[index] for index in down_indexes) / benchmark_down
        if benchmark_down < 0.0
        else 0.0
    )
    benchmark_variance = statistics.pvariance(benchmark_returns)
    beta = (
        statistics.fmean(
            (a - statistics.fmean(strategy_returns))
            * (b - statistics.fmean(benchmark_returns))
            for a, b in zip(strategy_returns, benchmark_returns)
        )
        / benchmark_variance
        if benchmark_variance > 0.0
        else 0.0
    )
    alpha = (statistics.fmean(strategy_returns) - beta * statistics.fmean(benchmark_returns)) * 252.0
    losses = abs(sum(value for value in strategy_returns if value < 0.0))
    profit_factor = (
        sum(value for value in strategy_returns if value > 0.0) / losses
        if losses > 0.0
        else 0.0
    )
    var_95 = _quantile(strategy_returns, 0.05)
    tail = tuple(value for value in strategy_returns if value <= var_95)
    cvar_95 = statistics.fmean(tail) if tail else var_95
    sharpe = _annualized_ratio(strategy_returns)
    return TqqqQqqRelativeMetrics(
        benchmark_symbol="QQQ",
        strategy_total_return=strategy_total,
        qqq_total_return=benchmark_total,
        excess_total_return=strategy_total - benchmark_total,
        strategy_cagr=strategy_cagr,
        qqq_cagr=benchmark_cagr,
        excess_cagr=strategy_cagr - benchmark_cagr,
        strategy_max_drawdown=strategy_mdd,
        qqq_max_drawdown=benchmark_mdd,
        max_drawdown_delta=strategy_mdd - benchmark_mdd,
        strategy_recovery_sessions=strategy_recovery,
        qqq_recovery_sessions=benchmark_recovery,
        strategy_unrecovered_at_end=strategy_unrecovered,
        qqq_unrecovered_at_end=benchmark_unrecovered,
        up_market_capture=up_capture,
        down_market_capture=down_capture,
        alpha=alpha,
        beta=beta,
        information_ratio=_annualized_ratio(excess_returns),
        sharpe_ratio=sharpe,
        sortino_ratio=_annualized_ratio(strategy_returns, downside_only=True),
        calmar_ratio=strategy_cagr / abs(strategy_mdd) if strategy_mdd < 0.0 else 0.0,
        annualized_volatility=statistics.pstdev(strategy_returns) * math.sqrt(252.0),
        var_95=var_95,
        cvar_95=cvar_95,
        turnover=float(replay.turnover),
        trade_count=replay.trade_count,
        win_rate=sum(value > 0.0 for value in strategy_returns) / len(strategy_returns),
        profit_factor=profit_factor,
        information_coefficient=_correlation(strategy_returns, benchmark_returns),
    )


class TqqqPromotionRunner:
    """Explicit real-runner protocol over an injected immutable replay function."""

    runner_kind = "real"

    def __init__(
        self,
        identity: TqqqPromotionIdentity,
        plan: TqqqPromotionPlan,
        replay_window: ReplayWindow,
        *,
        total_cost_bps: int,
    ) -> None:
        _validate_identity(identity)
        _validate_plan(plan)
        if type(total_cost_bps) is not int or total_cost_bps not in _COST_SCENARIOS_BPS:
            raise TqqqPromotionContractError("invalid frozen cost scenario")
        if not callable(replay_window):
            raise TqqqPromotionContractError("replay function is required")
        self.identity = identity
        self.plan = plan
        self.replay_window = replay_window
        self.total_cost_bps = total_cost_bps
        self._state_sha256 = identity.initial_state_sha256
        self._windows: list[TqqqWindowEvidence] = []

    @property
    def windows(self) -> tuple[TqqqWindowEvidence, ...]:
        return tuple(self._windows)

    def run_purged_fold(
        self,
        strategy_profile: str,
        params: Mapping[str, object],
        *,
        fold: PurgedWalkForwardFold,
        purge_days: int,
        embargo_days: int,
        cost_model: PromotionCostModel,
    ) -> BacktestResult:
        if fold not in self.plan.folds:
            raise TqqqPromotionContractError("unknown purged fold")
        if purge_days != self.plan.purge_days or embargo_days != self.plan.embargo_days:
            raise TqqqPromotionContractError("purge/embargo timing mismatch")
        return self._run_window(
            strategy_profile,
            params,
            start_date=fold.test_start,
            end_date=fold.test_end,
            cost_model=cost_model,
        )

    def run_locked_oos(
        self,
        strategy_profile: str,
        params: Mapping[str, object],
        *,
        start_date: date,
        end_date: date,
        cost_model: PromotionCostModel,
    ) -> BacktestResult:
        if start_date != self.plan.locked_oos_start or end_date != self.plan.locked_oos_end:
            raise TqqqPromotionContractError("locked OOS timing mismatch")
        return self._run_window(
            strategy_profile,
            params,
            start_date=start_date,
            end_date=end_date,
            cost_model=cost_model,
        )

    def _run_window(
        self,
        strategy_profile: str,
        params: Mapping[str, object],
        *,
        start_date: date,
        end_date: date,
        cost_model: PromotionCostModel,
    ) -> BacktestResult:
        if strategy_profile != _PROFILE or dict(params) != _params(self.identity, _timing_sha256(self.plan)):
            raise TqqqPromotionContractError("candidate-bound parameters mismatch")
        total_cost = _finite(cost_model.commission_bps, "commission", nonnegative=True)
        total_cost += _finite(cost_model.slippage_bps, "slippage", nonnegative=True)
        total_cost += _finite(cost_model.market_impact_bps, "market impact", nonnegative=True)
        if total_cost != float(self.total_cost_bps):
            raise TqqqPromotionContractError("all-in per-side cost mismatch")
        replay = self.replay_window(
            start_date,
            end_date,
            self.total_cost_bps,
            self._state_sha256,
        )
        _validate_replay(
            replay,
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=self._state_sha256,
        )
        metrics = _relative_metrics(replay)
        self._state_sha256 = replay.final_state_sha256
        self._windows.append(
            TqqqWindowEvidence(
                start_date=start_date,
                end_date=end_date,
                prior_state_sha256=replay.prior_state_sha256,
                final_state_sha256=replay.final_state_sha256,
                relative_metrics=metrics,
            )
        )
        return BacktestResult(
            strategy_profile=_PROFILE,
            domain=_DOMAIN,
            param_set_id=f"tqqq_etf_only_{self.total_cost_bps}bp",
            params={},
            sharpe_ratio=metrics.sharpe_ratio,
            calmar_ratio=metrics.calmar_ratio,
            sortino_ratio=metrics.sortino_ratio,
            max_drawdown=metrics.strategy_max_drawdown,
            cagr=metrics.strategy_cagr,
            volatility=metrics.annualized_volatility,
            win_rate=metrics.win_rate,
            total_return=metrics.strategy_total_return,
            start_date=start_date,
            end_date=end_date,
            observation_count=len(replay.strategy_equity),
            benchmark_symbol="QQQ",
            benchmark_cagr=metrics.qqq_cagr,
            benchmark_max_drawdown=metrics.qqq_max_drawdown,
            excess_cagr=metrics.excess_cagr,
            oos_sharpe=metrics.sharpe_ratio,
            oos_calmar=metrics.calmar_ratio,
            oos_max_drawdown=metrics.strategy_max_drawdown,
            walk_forward_stability=1.0,
            run_duration_seconds=0.0,
            source_script="tqqq_promotion_runner",
        )


def _params(identity: TqqqPromotionIdentity, timing_sha256: str) -> dict[str, object]:
    return {
        "authority_scope": "RESEARCH_ONLY",
        "config_sha256": identity.config_sha256,
        "input_manifest_sha256": identity.input_manifest_sha256,
        "mandate_receipt_sha256": identity.mandate_receipt_sha256,
        "platform_execution_revision": identity.platform_execution_revision,
        "qpk_revision": identity.qpk_revision,
        "runner_revision": identity.runner_revision,
        "timing_sha256": timing_sha256,
        "ues_revision": identity.ues_revision,
    }


def _cost_model(total_cost_bps: int) -> PromotionCostModel:
    return PromotionCostModel(
        model_id=f"tqqq_all_in_per_side_{total_cost_bps}bp.v1",
        commission_bps=0.0,
        slippage_bps=float(total_cost_bps),
        market_impact_bps=0.0,
    )


def run_tqqq_promotion_research(
    identity: TqqqPromotionIdentity,
    plan: TqqqPromotionPlan,
    replay_window: ReplayWindow,
) -> TqqqPromotionResearchResult:
    """Run contract-only synthetic/PIT evidence; never grant execution authority."""

    _validate_identity(identity)
    _validate_plan(plan)
    if _resolve_runner_revision() != identity.runner_revision:
        raise TqqqPromotionContractError("runner revision mismatch")
    timing_sha256 = _timing_sha256(plan)
    scenarios: list[TqqqCostScenarioResult] = []
    for total_cost_bps in _COST_SCENARIOS_BPS:
        runner = TqqqPromotionRunner(
            identity,
            plan,
            replay_window,
            total_cost_bps=total_cost_bps,
        )
        orchestrator = BacktestOrchestrator(store=_MemoryPerformanceStore())
        orchestrator.register_runner(_DOMAIN, runner)
        promotion_run = orchestrator.run_promotion(
            _PROFILE,
            domain=_DOMAIN,
            params=_params(identity, timing_sha256),
            folds=plan.folds,
            locked_oos_start=plan.locked_oos_start,
            locked_oos_end=plan.locked_oos_end,
            purge_days=plan.purge_days,
            embargo_days=plan.embargo_days,
            source_revision=identity.ues_revision,
            cost_model=_cost_model(total_cost_bps),
            param_set_id=f"tqqq_etf_only_{total_cost_bps}bp",
        )
        scenarios.append(
            TqqqCostScenarioResult(
                total_cost_bps=total_cost_bps,
                cost_model_scope="ALL_IN_PER_SIDE",
                promotion_run=promotion_run,
                windows=runner.windows,
            )
        )
    return TqqqPromotionResearchResult(
        identity=identity,
        timing_sha256=timing_sha256,
        scenarios=tuple(scenarios),
    )
