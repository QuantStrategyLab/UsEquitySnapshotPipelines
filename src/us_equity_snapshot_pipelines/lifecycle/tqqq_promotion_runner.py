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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import (
    BacktestResult,
    PromotionBacktestRun,
    PromotionCostModel,
    PurgedWalkForwardFold,
)

from .soxl_pit_input_packager import _xnys_holidays

_QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
_UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
_PROFILE = "tqqq_core_parity_v1"
_CANDIDATE_VARIANT = "tqqq_core_parity_5loss_20xnys_defensive_cooldown_v1"
_DOMAIN = "us_equity"
_ALLOWED_ASSETS = frozenset({"TQQQ", "QQQM", "BOXX"})
_ASSET_FACTORS = {"TQQQ": 3, "QQQM": 1, "BOXX": 1}
_ASSET_CAPS = {"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50}
_EFFECTIVE_EXPOSURE_CAP = 0.50
_COST_SCENARIOS_BPS = (5, 10, 15)
_EXACT_COMMON_ELIGIBILITY = date(2022, 12, 28)
_LOCKED_OOS_START = date(2025, 7, 2)
_LOCKED_OOS_END = date(2026, 7, 31)
_LOCKED_OOS_SESSION_COUNT = 272
_LOCKED_OOS_SESSIONS_SHA256 = "fe4120013da919f99ec3585898c82409e8fc26423df4649377eafa665da103b8"
_FROZEN_CALENDAR_SHA256 = "18b12a992cfb245e6aec7145797e5f0b7b2b03eed880961896ba370d8a7d5380"
_FROZEN_CALENDAR_SOURCE_REVISION = (
    "exchange_calendars:4.13.2:XNYS:"
    "18b12a992cfb245e6aec7145797e5f0b7b2b03eed880961896ba370d8a7d5380"
)
_SYSTEMATIC_WINDOW_SHA256 = {
    3: "877136166f09def7019ba2fe7616c8c820bae3c13212f3b485cfe001b455d66f",
    6: "31a9a72c6839e8ea117184aa0af19ebf1063d83dd8231457d50a1a6cc7d73434",
    12: "145a3ef1598a54a3c1e138a223e67ab2325357d7bd405749730be9baa2d76adc",
    24: "1a3a85d1d10a8151bd3e4ff5218d3017ce19323927b0a2c2c7f614216916301e",
}
_SYSTEMATIC_PLAN_SHA256 = "28c4b4fbf587891112f1994b44a6ff3d111742cdb854adfcd172cfe664b1ae52"
_DEVELOPMENT_SESSION_COUNT = 624
_DEVELOPMENT_SESSIONS_SHA256 = (
    "80aa5b9ed15cbe1263f212d121968bd0eed5a2553261f25c8df99457a5432fb4"
)
_FROZEN_TIMING_SHA256 = (
    "72973ff51aa0b99524e67352c67f4b98cb3c6ca5d62f9585c4d26c82221d17f1"
)
TQQQ_SWITCHING_CHARACTERIZATION_SHA256 = (
    "1d92933226e8481698b242f5f073224e34ee4a739e27daea477d0e7c8e577c41"
)

LEGACY_PARITY_CLASSIFICATIONS = frozenset(
    {
        "MATCH",
        "EXPECTED_DIFFERENCE_DUE_TO_EXPLICIT_ARCHITECTURE_CHANGE",
        "UNEXPLAINED_CORE_STRATEGY_DRIFT",
        "NOT_COMPARABLE",
    }
)
TQQQ_ACCEPTANCE_PASS = "PASS_READY_FOR_SEPARATE_HUMAN_PROMOTION_DECISION"
TQQQ_ACCEPTANCE_REJECT = "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
TQQQ_ACCEPTANCE_INCONCLUSIVE = "INCONCLUSIVE_DATA_OR_EXECUTION"

_EXPLICIT_ARCHITECTURE_CHANGES = frozenset(
    {
        "RISK_ENGINE_AND_APPROVED_SIZING",
        "CLOSE_T_TO_OPEN_T_PLUS_1",
        "COST_NORMALIZATION_5_10_15_BPS",
        "FRESH_EPISODE_INITIAL_STATE",
        "PRE_WINDOW_TRADE_DELETION",
        "PARK_NOT_CARRIED_BETWEEN_EPISODES",
        "EXECUTION_SESSION_COUNTER_ATTRIBUTION",
    }
)
_LEGACY_REFERENCE_FIELDS = {
    "code_commit",
    "code_sha256",
    "runtime_config_sha256",
    "switching_rules_sha256",
    "data_source",
    "adjustment",
    "calendar",
    "range_start",
    "range_end",
    "session_count",
    "sessions_sha256",
    "initial_state_sha256",
    "decision_timing",
    "fill_timing",
    "cost_model_sha256",
    "input_sha256",
    "metrics_sha256",
    "trades_sha256",
    "allocations_sha256",
}
_LEGACY_SESSION_FIELDS = {
    "session",
    "signal",
    "regime",
    "target_allocation",
    "switch",
    "gross_return",
    "trade_count",
    "cost",
    "net_return",
}
_PARITY_ASSETS = {"TQQQ", "QQQM", "BOXX", "cash"}


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
class TqqqSwitchingTrace:
    signal_session: date
    execution_session: date
    signal_state: str
    signal_regime: str
    intended_allocation: tuple[tuple[str, float], ...]
    risk_disposition: str
    risk_reason_codes: tuple[str, ...]
    replay_target_allocation: tuple[tuple[str, float], ...]
    executed_allocation: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class TqqqEpisodeSummary:
    episode_session_count: int
    tqqq_exposure_session_count: int
    qqqm_exposure_session_count: int
    boxx_exposure_session_count: int
    cash_only_session_count: int
    parked_session_count: int
    tqqq_entry_count: int
    tqqq_stop_armed_count: int
    tqqq_stop_crossing_count: int
    tqqq_stop_fill_count: int
    tqqq_unprotected_holding_session_count: int
    breaker_reason: str | None
    first_park_session: date | None


@dataclass(frozen=True)
class TqqqWindowReplay:
    start_date: date
    end_date: date
    prior_state_sha256: str
    final_state_sha256: str
    strategy_equity: tuple[float, ...]
    qqq_total_return_equity: tuple[float, ...]
    boxx_total_return_equity: tuple[float, ...]
    asset_weights: tuple[tuple[str, float], ...]
    turnover: float
    trade_count: int
    decision_count: int
    risk_assessment_count: int
    warmup_sessions: int
    episode_summary: TqqqEpisodeSummary
    sessions: tuple[date, ...]
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
    switching_traces: tuple[TqqqSwitchingTrace, ...] = ()


@dataclass(frozen=True)
class TqqqQqqRelativeMetrics:
    benchmark_symbol: str
    strategy_total_return: float
    qqq_total_return: float
    boxx_total_return: float
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
    episode_summary: TqqqEpisodeSummary
    decision_count: int
    risk_assessment_count: int
    sessions: tuple[date, ...]
    switching_traces: tuple[TqqqSwitchingTrace, ...]


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
    switching_characterization_sha256: str = TQQQ_SWITCHING_CHARACTERIZATION_SHA256
    authority_scope: str = "RESEARCH_ONLY"
    learning_only: bool = True
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


def _canonical_sha256(material: object) -> str:
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _frozen_xnys_sessions() -> tuple[date, ...]:
    start = date(2018, 1, 2)
    holidays = set().union(
        *(_xnys_holidays(year) for year in range(start.year, _LOCKED_OOS_END.year + 1))
    )
    sessions: list[date] = []
    current = start
    while current <= _LOCKED_OOS_END:
        if current.weekday() < 5 and current not in holidays:
            sessions.append(current)
        current += timedelta(days=1)
    result = tuple(sessions)
    if (
        _canonical_sha256([session.isoformat() for session in result])
        != _FROZEN_CALENDAR_SHA256
    ):
        raise RuntimeError("frozen TQQQ XNYS calendar contract is inconsistent")
    locked = tuple(
        session for session in result if _LOCKED_OOS_START <= session <= _LOCKED_OOS_END
    )
    if (
        len(locked) != _LOCKED_OOS_SESSION_COUNT
        or _canonical_sha256([session.isoformat() for session in locked])
        != _LOCKED_OOS_SESSIONS_SHA256
    ):
        raise RuntimeError("frozen TQQQ locked OOS calendar identity is inconsistent")
    return result


_FROZEN_XNYS_SESSIONS = _frozen_xnys_sessions()
_FROZEN_XNYS_SESSION_INDEX = {
    session: index for index, session in enumerate(_FROZEN_XNYS_SESSIONS)
}


def build_tqqq_switching_characterization_contract() -> dict[str, object]:
    """Return the frozen, quota-free deterministic switching contract."""

    material: dict[str, object] = {
        "schema_version": "tqqq_switching_characterization.v1",
        "candidate_profile": _PROFILE,
        "candidate_variant": _CANDIDATE_VARIANT,
        "timing": "completed_close_t_to_open_t_plus_1",
        "cases": (
            "RISK_ON_TQQQ_OR_QQQM",
            "DEFENSIVE_BOXX",
            "RISK_ON_TO_DEFENSIVE_TRANSITION",
            "FIFTH_LOSS_TO_20_XNYS_PROTECTIVE_COOLDOWN",
            "FRESH_BASE_SIGNAL_AFTER_COOLDOWN",
            "INVALID_OR_PRELISTING_INPUT_FAILS_CLOSED",
            "FRESH_EPISODE_WITHOUT_INHERITED_PARK",
        ),
        "trace_order": (
            "signal_state",
            "signal_regime",
            "intended_allocation",
            "risk_disposition",
            "replay_target_allocation",
            "executed_allocation",
        ),
        "risk_on_drift_terminal": TQQQ_ACCEPTANCE_INCONCLUSIVE,
        "defensive_only_rule": (
            "evaluate_with_frozen_performance_benchmarks_after_valid_input_config_"
            "and_multicycle_coverage"
        ),
        "minimum_risk_asset_occupancy": None,
        "minimum_trade_count": None,
    }
    if _canonical_sha256(material) != TQQQ_SWITCHING_CHARACTERIZATION_SHA256:
        raise TqqqPromotionContractError("switching characterization identity mismatch")
    return {**material, "sha256": TQQQ_SWITCHING_CHARACTERIZATION_SHA256}


def build_tqqq_development_robustness_plan(
    sessions: Sequence[date],
) -> dict[str, object]:
    """Freeze every complete 3/6/12/24-month seen-development window."""

    if (
        isinstance(sessions, (str, bytes))
        or not sessions
        or any(type(session) is not date for session in sessions)
    ):
        raise TqqqPromotionContractError("invalid development calendar")
    ordered = tuple(sessions)
    if ordered != tuple(sorted(set(ordered))):
        raise TqqqPromotionContractError("invalid development calendar")
    grouped: dict[tuple[int, int], list[date]] = {}
    development_sessions = tuple(
        session
        for session in ordered
        if date(2023, 1, 1) <= session <= date(2025, 6, 30)
    )
    if (
        len(development_sessions) != _DEVELOPMENT_SESSION_COUNT
        or _canonical_sha256(
            [session.isoformat() for session in development_sessions]
        )
        != _DEVELOPMENT_SESSIONS_SHA256
    ):
        raise TqqqPromotionContractError("development calendar identity mismatch")
    for session in development_sessions:
        grouped.setdefault((session.year, session.month), []).append(session)
    expected_months = tuple(
        (year, month)
        for year in range(2023, 2026)
        for month in range(1, 13)
        if (year, month) <= (2025, 6)
    )
    if tuple(sorted(grouped)) != expected_months:
        raise TqqqPromotionContractError("incomplete development calendar")

    rolling: dict[str, dict[str, object]] = {}
    all_windows: list[dict[str, object]] = []
    for horizon in (3, 6, 12, 24):
        windows = [
            {
                "horizon_months": horizon,
                "start_month": (
                    f"{expected_months[index - horizon + 1][0]:04d}-"
                    f"{expected_months[index - horizon + 1][1]:02d}"
                ),
                "end_month": (
                    f"{expected_months[index][0]:04d}-{expected_months[index][1]:02d}"
                ),
                "start_session": grouped[expected_months[index - horizon + 1]][0].isoformat(),
                "end_session": grouped[expected_months[index]][-1].isoformat(),
            }
            for index in range(horizon - 1, len(expected_months))
        ]
        if _canonical_sha256(windows) != _SYSTEMATIC_WINDOW_SHA256[horizon]:
            raise TqqqPromotionContractError("development window identity mismatch")
        all_windows.extend(windows)
        rolling[f"{horizon}_month"] = {
            "count": len(windows),
            "first": [windows[0]["start_session"], windows[0]["end_session"]],
            "last": [windows[-1]["start_session"], windows[-1]["end_session"]],
            "sha256": _SYSTEMATIC_WINDOW_SHA256[horizon],
            "windows": windows,
        }
    if _canonical_sha256(all_windows) != _SYSTEMATIC_PLAN_SHA256:
        raise TqqqPromotionContractError("development plan identity mismatch")
    return {
        "aggregate_plan_sha256": _SYSTEMATIC_PLAN_SHA256,
        "seen_development_cutoff_inclusive": "2025-07-01",
        "whole_month_range": "2023-01 through 2025-06",
        "enumeration_rule": (
            "for H in 3,6,12,24, enumerate every complete H-month window ending at every "
            "eligible month-end; never select or delete windows"
        ),
        "merge_with_locked_oos": False,
        "promotion_evidence": False,
        "regime_labels": {
            "basis": "annualized QQQ adjusted total return on the same window/calendar valuation",
            "bear": "<= -0.05",
            "bull": ">= +0.05",
            "sideways": "otherwise",
            "report_only": True,
        },
        "rolling_windows": rolling,
    }


def _valid_legacy_reference(reference: Mapping[str, object]) -> bool:
    if not isinstance(reference, Mapping) or set(reference) != _LEGACY_REFERENCE_FIELDS:
        return False
    if not _is_hex(reference["code_commit"], 40):
        return False
    for field in (
        "code_sha256",
        "runtime_config_sha256",
        "switching_rules_sha256",
        "initial_state_sha256",
        "cost_model_sha256",
        "input_sha256",
        "metrics_sha256",
        "trades_sha256",
        "allocations_sha256",
        "sessions_sha256",
    ):
        if not _is_hex(reference[field], 64):
            return False
    for field in (
        "data_source",
        "adjustment",
        "calendar",
        "decision_timing",
        "fill_timing",
    ):
        if not isinstance(reference[field], str) or not reference[field].strip():
            return False
    try:
        start = date.fromisoformat(str(reference["range_start"]))
        end = date.fromisoformat(str(reference["range_end"]))
    except ValueError:
        return False
    return (
        start <= end <= date(2025, 7, 1)
        and type(reference["session_count"]) is int
        and reference["session_count"] > 0
    )


def _validated_parity_rows(
    values: Sequence[Mapping[str, object]],
    reference: Mapping[str, object],
    *,
    verify_reference_digests: bool,
) -> tuple[dict[str, object], ...] | None:
    if isinstance(values, (str, bytes)) or not values:
        return None
    rows: list[dict[str, object]] = []
    for value in values:
        if not isinstance(value, Mapping) or set(value) != _LEGACY_SESSION_FIELDS:
            return None
        try:
            session = date.fromisoformat(str(value["session"]))
            gross_return = _finite(value["gross_return"], "legacy gross return")
            cost = _finite(value["cost"], "legacy cost", nonnegative=True)
            net_return = _finite(value["net_return"], "legacy net return")
        except (TqqqPromotionContractError, ValueError):
            return None
        allocation = value["target_allocation"]
        if not isinstance(allocation, Mapping) or set(allocation) != _PARITY_ASSETS:
            return None
        try:
            normalized_allocation = {
                symbol: _finite(allocation[symbol], "legacy allocation", nonnegative=True)
                for symbol in sorted(_PARITY_ASSETS)
            }
        except TqqqPromotionContractError:
            return None
        if not math.isclose(sum(normalized_allocation.values()), 1.0, abs_tol=1e-12):
            return None
        if (
            not isinstance(value["signal"], str)
            or not value["signal"]
            or not isinstance(value["regime"], str)
            or not value["regime"]
            or type(value["switch"]) is not bool
            or type(value["trade_count"]) is not int
            or value["trade_count"] < 0
        ):
            return None
        rows.append(
            {
                "session": session,
                "signal": value["signal"],
                "regime": value["regime"],
                "target_allocation": normalized_allocation,
                "switch": value["switch"],
                "gross_return": gross_return,
                "trade_count": value["trade_count"],
                "cost": cost,
                "net_return": net_return,
            }
        )
    sessions = tuple(row["session"] for row in rows)
    if sessions != tuple(sorted(set(sessions))):
        return None
    if (
        sessions[0].isoformat() != reference["range_start"]
        or sessions[-1].isoformat() != reference["range_end"]
        or len(sessions) != reference["session_count"]
        or _canonical_sha256([session.isoformat() for session in sessions])
        != reference["sessions_sha256"]
    ):
        return None
    allocations = [
        {
            "session": row["session"].isoformat(),
            "signal": row["signal"],
            "regime": row["regime"],
            "target_allocation": row["target_allocation"],
            "switch": row["switch"],
        }
        for row in rows
    ]
    trades = [
        {
            "session": row["session"].isoformat(),
            "trade_count": row["trade_count"],
            "cost": row["cost"],
        }
        for row in rows
    ]
    metrics = [
        {
            "session": row["session"].isoformat(),
            "gross_return": row["gross_return"],
            "net_return": row["net_return"],
        }
        for row in rows
    ]
    if verify_reference_digests and (
        _canonical_sha256(allocations) != reference["allocations_sha256"]
        or _canonical_sha256(trades) != reference["trades_sha256"]
        or _canonical_sha256(metrics) != reference["metrics_sha256"]
    ):
        return None
    return tuple(rows)


def _same_number(left: object, right: object) -> bool:
    return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=1e-12)


def _window_metrics_match_backtest(
    window: TqqqWindowEvidence,
    result: BacktestResult,
    total_cost_bps: int,
) -> bool:
    metrics = window.relative_metrics
    if (
        type(window) is not TqqqWindowEvidence
        or type(metrics) is not TqqqQqqRelativeMetrics
        or type(result) is not BacktestResult
        or metrics.benchmark_symbol != result.benchmark_symbol
        or result.start_date is None
        or result.end_date is None
        or metrics.strategy_total_return <= -1.0
        or metrics.qqq_total_return <= -1.0
        or not _same_number(
            metrics.strategy_cagr,
            _cagr(
                1.0,
                1.0 + metrics.strategy_total_return,
                result.start_date,
                result.end_date,
            ),
        )
        or not _same_number(
            metrics.qqq_cagr,
            _cagr(1.0, 1.0 + metrics.qqq_total_return, result.start_date, result.end_date),
        )
        or result.source_script != _window_acceptance_source(window, total_cost_bps)
    ):
        return False
    fields = (
        ("strategy_total_return", "total_return"),
        ("strategy_cagr", "cagr"),
        ("strategy_max_drawdown", "max_drawdown"),
        ("qqq_cagr", "benchmark_cagr"),
        ("qqq_max_drawdown", "benchmark_max_drawdown"),
        ("excess_cagr", "excess_cagr"),
        ("sharpe_ratio", "sharpe_ratio"),
        ("sortino_ratio", "sortino_ratio"),
        ("calmar_ratio", "calmar_ratio"),
        ("annualized_volatility", "volatility"),
        ("win_rate", "win_rate"),
        ("sharpe_ratio", "oos_sharpe"),
        ("calmar_ratio", "oos_calmar"),
        ("strategy_max_drawdown", "oos_max_drawdown"),
    )
    return all(
        _same_number(
            getattr(metrics, metric_field),
            _finite(getattr(result, result_field), "locked backtest metric"),
        )
        for metric_field, result_field in fields
    )


def _window_acceptance_source(
    window: TqqqWindowEvidence,
    total_cost_bps: int,
) -> str:
    metrics = window.relative_metrics
    summary = window.episode_summary
    digest = _canonical_sha256(
        {
            "total_cost_bps": total_cost_bps,
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "prior_state_sha256": window.prior_state_sha256,
            "final_state_sha256": window.final_state_sha256,
            "relative_metrics": vars(metrics),
            "episode_summary": {
                **vars(summary),
                "first_park_session": (
                    summary.first_park_session.isoformat()
                    if summary.first_park_session is not None
                    else None
                ),
            },
            "decision_count": window.decision_count,
            "risk_assessment_count": window.risk_assessment_count,
            "sessions": [session.isoformat() for session in window.sessions],
            "switching_traces": [
                {
                    "signal_session": trace.signal_session.isoformat(),
                    "execution_session": trace.execution_session.isoformat(),
                    "signal_state": trace.signal_state,
                    "signal_regime": trace.signal_regime,
                    "intended_allocation": trace.intended_allocation,
                    "risk_disposition": trace.risk_disposition,
                    "risk_reason_codes": trace.risk_reason_codes,
                    "replay_target_allocation": trace.replay_target_allocation,
                    "executed_allocation": trace.executed_allocation,
                }
                for trace in window.switching_traces
            ],
        }
    )
    return f"tqqq_promotion_runner:{digest}"


def classify_tqqq_legacy_parity(
    legacy_reference: Mapping[str, object],
    legacy_sessions: Sequence[Mapping[str, object]],
    candidate_sessions: Sequence[Mapping[str, object]],
    *,
    explicit_architecture_changes: Sequence[str] = (),
) -> str:
    """Classify same-range session-first parity without reconstructing missing legacy evidence."""

    if not _valid_legacy_reference(legacy_reference):
        return "NOT_COMPARABLE"
    try:
        if isinstance(explicit_architecture_changes, (str, bytes)):
            return "NOT_COMPARABLE"
        changes = tuple(explicit_architecture_changes)
        change_set = set(changes)
    except TypeError:
        return "NOT_COMPARABLE"
    if (
        any(type(change) is not str for change in changes)
        or len(changes) != len(change_set)
        or not change_set <= _EXPLICIT_ARCHITECTURE_CHANGES
    ):
        return "NOT_COMPARABLE"
    legacy = _validated_parity_rows(
        legacy_sessions, legacy_reference, verify_reference_digests=True
    )
    candidate = _validated_parity_rows(
        candidate_sessions, legacy_reference, verify_reference_digests=False
    )
    if legacy is None or candidate is None:
        return "NOT_COMPARABLE"
    if tuple(row["session"] for row in legacy) != tuple(row["session"] for row in candidate):
        return "NOT_COMPARABLE"

    expected_difference = False
    timing_or_state_changes = {
        "CLOSE_T_TO_OPEN_T_PLUS_1",
        "FRESH_EPISODE_INITIAL_STATE",
        "PRE_WINDOW_TRADE_DELETION",
        "PARK_NOT_CARRIED_BETWEEN_EPISODES",
    }
    for old, new in zip(legacy, candidate):
        if old["signal"] != new["signal"] or old["regime"] != new["regime"]:
            return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
        if any(
            not _same_number(old["target_allocation"][symbol], new["target_allocation"][symbol])
            for symbol in _PARITY_ASSETS
        ):
            if "RISK_ENGINE_AND_APPROVED_SIZING" not in change_set:
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
        if old["switch"] != new["switch"]:
            if "CLOSE_T_TO_OPEN_T_PLUS_1" not in change_set:
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
        if not _same_number(old["gross_return"], new["gross_return"]):
            if not change_set & (
                timing_or_state_changes | {"RISK_ENGINE_AND_APPROVED_SIZING"}
            ):
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
        if old["trade_count"] != new["trade_count"]:
            if not change_set & (
                timing_or_state_changes
                | {
                    "EXECUTION_SESSION_COUNTER_ATTRIBUTION",
                    "RISK_ENGINE_AND_APPROVED_SIZING",
                }
            ):
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
        if not _same_number(old["cost"], new["cost"]):
            if not change_set & {
                "CLOSE_T_TO_OPEN_T_PLUS_1",
                "COST_NORMALIZATION_5_10_15_BPS",
                "RISK_ENGINE_AND_APPROVED_SIZING",
            }:
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
        if not _same_number(old["net_return"], new["net_return"]):
            if not change_set & (
                timing_or_state_changes
                | {
                    "RISK_ENGINE_AND_APPROVED_SIZING",
                    "COST_NORMALIZATION_5_10_15_BPS",
                }
            ):
                return "UNEXPLAINED_CORE_STRATEGY_DRIFT"
            expected_difference = True
    return (
        "EXPECTED_DIFFERENCE_DUE_TO_EXPLICIT_ARCHITECTURE_CHANGE"
        if expected_difference
        else "MATCH"
    )


def _validated_switching_allocation(
    values: tuple[tuple[str, float], ...],
) -> dict[str, float]:
    if type(values) is not tuple or any(
        type(item) is not tuple or len(item) != 2 or type(item[0]) is not str
        for item in values
    ):
        raise TqqqPromotionContractError("invalid switching allocation")
    allocation = {
        symbol: _finite(value, "switching allocation", nonnegative=True)
        for symbol, value in values
    }
    if (
        len(allocation) != len(values)
        or set(allocation) != _PARITY_ASSETS
        or not math.isclose(sum(allocation.values()), 1.0, rel_tol=0.0, abs_tol=1e-9)
    ):
        raise TqqqPromotionContractError("invalid switching allocation")
    return allocation


def _validate_switching_traces(
    traces: tuple[TqqqSwitchingTrace, ...],
    *,
    sessions: tuple[date, ...],
    decision_count: int,
    total_cost_bps: int,
) -> None:
    if (
        type(traces) is not tuple
        or len(traces) != len(sessions)
        or total_cost_bps not in _COST_SCENARIOS_BPS
    ):
        raise TqqqPromotionContractError("complete switching traces are required")
    execution_sessions: list[date] = []
    approved_count = 0
    cooldown_count = 0
    cooldown_last_execution: date | None = None
    cost_rate = total_cost_bps / 10_000.0
    allocation_tolerance = cost_rate / (1.0 - cost_rate) + 1e-9
    states = {
        "entry": "RISK_ON",
        "hold": "RISK_ON",
        "macro_delever": "RISK_ON",
        "exit": "DEFENSIVE",
        "idle": "DEFENSIVE",
        "macro_risk_defense": "DEFENSIVE",
        "crisis_defense": "DEFENSIVE",
        "protective_cooldown": "DEFENSIVE",
        "risk_engine_non_approve": "DEFENSIVE",
        "parked": "DEFENSIVE",
    }
    for trace in traces:
        if type(trace) is not TqqqSwitchingTrace:
            raise TqqqPromotionContractError("invalid switching trace")
        signal_index = _FROZEN_XNYS_SESSION_INDEX.get(trace.signal_session)
        execution_index = _FROZEN_XNYS_SESSION_INDEX.get(trace.execution_session)
        if (
            type(trace.signal_session) is not date
            or type(trace.execution_session) is not date
            or signal_index is None
            or execution_index != signal_index + 1
            or trace.execution_session not in sessions
            or states.get(trace.signal_state) != trace.signal_regime
            or type(trace.risk_reason_codes) is not tuple
            or any(type(code) is not str or not code for code in trace.risk_reason_codes)
        ):
            raise TqqqPromotionContractError("invalid switching trace")
        intended = _validated_switching_allocation(trace.intended_allocation)
        target = _validated_switching_allocation(trace.replay_target_allocation)
        executed = _validated_switching_allocation(trace.executed_allocation)
        if cooldown_count and trace.signal_state != "protective_cooldown":
            if (
                cooldown_count != 20
                or trace.signal_session != cooldown_last_execution
                or trace.risk_disposition != "APPROVE"
                or trace.signal_state in {"parked", "risk_engine_non_approve"}
            ):
                raise TqqqPromotionContractError("invalid protective cooldown sequence")
            cooldown_count = 0
            cooldown_last_execution = None
        for allocation in (intended, target):
            if (
                any(
                    allocation[symbol] > _ASSET_CAPS[symbol] + 1e-12
                    for symbol in _ALLOWED_ASSETS
                )
                or math.fsum(
                    allocation[symbol] * _ASSET_FACTORS[symbol]
                    for symbol in _ALLOWED_ASSETS
                )
                > _EFFECTIVE_EXPOSURE_CAP + 1e-12
            ):
                raise TqqqPromotionContractError("switching target exposure cap exceeded")
        if any(not _same_number(intended[symbol], target[symbol]) for symbol in _PARITY_ASSETS):
            raise TqqqPromotionContractError("UES/replay target allocation drift")
        if trace.risk_disposition == "PARK":
            execution_is_cash = all(
                executed[symbol] <= 1e-12 for symbol in _ALLOWED_ASSETS
            ) and _same_number(executed["cash"], 1.0)
            execution_matches_target = all(
                abs(executed[symbol] - target[symbol]) <= allocation_tolerance
                for symbol in _PARITY_ASSETS
            )
            if trace.risk_reason_codes not in {
                ("ACCOUNT_DRAWDOWN",),
                ("RISK_ENGINE_NON_APPROVE",),
            }:
                raise TqqqPromotionContractError("invalid parked switching trace")
            if trace.signal_state in {"parked", "risk_engine_non_approve"}:
                if (
                    any(target[symbol] > 1e-12 for symbol in _ALLOWED_ASSETS)
                    or not _same_number(target["cash"], 1.0)
                    or not execution_is_cash
                ):
                    raise TqqqPromotionContractError("invalid parked switching trace")
                if trace.signal_state == "risk_engine_non_approve":
                    approved_count += 1
            else:
                if not (execution_is_cash or execution_matches_target):
                    raise TqqqPromotionContractError("invalid parked switching trace")
                approved_count += 1
                intended_risk = intended["TQQQ"] + intended["QQQM"]
                if trace.signal_regime == "RISK_ON":
                    if intended_risk <= 0.0:
                        raise TqqqPromotionContractError("invalid parked switching trace")
                elif intended_risk > 1e-12 or intended["BOXX"] <= 0.0:
                    raise TqqqPromotionContractError("invalid parked switching trace")
            execution_sessions.append(trace.execution_session)
            continue
        if any(
            abs(executed[symbol] - target[symbol]) > allocation_tolerance
            for symbol in _PARITY_ASSETS
        ):
            raise TqqqPromotionContractError("cost-adjusted execution allocation drift")
        if trace.risk_disposition != "APPROVE":
            raise TqqqPromotionContractError("invalid switching risk disposition")
        approved_count += 1
        intended_risk = intended["TQQQ"] + intended["QQQM"]
        executed_risk = executed["TQQQ"] + executed["QQQM"]
        if trace.signal_regime == "RISK_ON":
            if intended_risk <= 0.0 or executed_risk <= 0.0:
                raise TqqqPromotionContractError("risk-on switching execution drift")
        elif intended_risk > 1e-12 or intended["BOXX"] <= 0.0 or executed["BOXX"] <= 0.0:
            raise TqqqPromotionContractError("defensive switching execution drift")
        if trace.signal_state == "protective_cooldown":
            if (
                trace.risk_reason_codes
                or intended["TQQQ"] > 1e-12
                or intended["QQQM"] > 1e-12
                or executed["TQQQ"] > 1e-12
                or executed["QQQM"] > 1e-12
                or not any(
                    _same_number(intended["BOXX"], expected)
                    for expected in (0.10, 0.20)
                )
            ):
                raise TqqqPromotionContractError("invalid protective cooldown allocation")
            cooldown_count += 1
            cooldown_last_execution = trace.execution_session
            if cooldown_count > 20:
                raise TqqqPromotionContractError("invalid protective cooldown sequence")
        execution_sessions.append(trace.execution_session)
    if (
        tuple(execution_sessions) != sessions
        or approved_count != decision_count
        or cooldown_count
    ):
        raise TqqqPromotionContractError("invalid switching trace order")


def _validated_window_evidence(
    window: TqqqWindowEvidence,
    backtest: BacktestResult,
    *,
    expected_sessions: tuple[date, ...],
    expected_params: Mapping[str, object],
    expected_param_set_id: str,
    expected_initial_state_sha256: str,
    expected_source_revision: str,
    total_cost_bps: int,
) -> tuple[TqqqQqqRelativeMetrics, TqqqEpisodeSummary, bool]:
    if (
        type(window) is not TqqqWindowEvidence
        or type(backtest) is not BacktestResult
        or window.start_date != backtest.start_date
        or window.end_date != backtest.end_date
        or window.sessions != expected_sessions
        or type(window.sessions) is not tuple
        or window.sessions != tuple(sorted(set(window.sessions)))
        or backtest.observation_count != len(expected_sessions) + 1
        or backtest.strategy_profile != _PROFILE
        or backtest.domain != _DOMAIN
        or backtest.param_set_id != expected_param_set_id
        or dict(backtest.params) != expected_params
        or backtest.source_revision != expected_source_revision
        or backtest.cost_model != f"tqqq_all_in_per_side_{total_cost_bps}bp.v1"
        or dict(backtest.cost_inputs)
        != {
            "commission_bps": 0.0,
            "slippage_bps": float(total_cost_bps),
            "market_impact_bps": 0.0,
        }
        or backtest.validation_identity is None
        or backtest.validation_identity.fold_id != expected_param_set_id
        or window.prior_state_sha256 != expected_initial_state_sha256
        or not _is_hex(window.final_state_sha256, 64)
        or type(window.decision_count) is not int
        or not 0 < window.decision_count <= len(window.sessions)
        or window.decision_count != window.risk_assessment_count
    ):
        raise TqqqPromotionContractError("window evidence identity mismatch")
    _validate_switching_traces(
        window.switching_traces,
        sessions=window.sessions,
        decision_count=window.decision_count,
        total_cost_bps=total_cost_bps,
    )
    metrics = window.relative_metrics
    summary = window.episode_summary
    if type(metrics) is not TqqqQqqRelativeMetrics or type(summary) is not TqqqEpisodeSummary:
        raise TqqqPromotionContractError("invalid window evidence")
    if (
        sum(trace.risk_disposition == "PARK" for trace in window.switching_traces)
        != summary.parked_session_count
        or not _window_metrics_match_backtest(window, backtest, total_cost_bps)
    ):
        raise TqqqPromotionContractError("window replay/backtest mismatch")
    summary_counts = (
        summary.episode_session_count,
        summary.tqqq_exposure_session_count,
        summary.qqqm_exposure_session_count,
        summary.boxx_exposure_session_count,
        summary.cash_only_session_count,
        summary.parked_session_count,
        summary.tqqq_entry_count,
        summary.tqqq_stop_armed_count,
        summary.tqqq_stop_crossing_count,
        summary.tqqq_stop_fill_count,
        summary.tqqq_unprotected_holding_session_count,
    )
    if (
        any(type(value) is not int or value < 0 for value in summary_counts)
        or summary.episode_session_count != len(window.sessions)
        or any(value > summary.episode_session_count for value in summary_counts[1:])
        or summary.cash_only_session_count + summary.parked_session_count
        > summary.episode_session_count
        or summary.tqqq_entry_count != summary.tqqq_stop_armed_count
        or summary.tqqq_stop_crossing_count != summary.tqqq_stop_fill_count
        or summary.tqqq_stop_crossing_count > summary.tqqq_entry_count
        or summary.tqqq_unprotected_holding_session_count != 0
        or (
            summary.parked_session_count > 0
            and (
                summary.breaker_reason
                not in {"ACCOUNT_DRAWDOWN", "RISK_ENGINE_NON_APPROVE"}
                or type(summary.first_park_session) is not date
                or not window.start_date
                <= summary.first_park_session
                <= window.end_date
            )
        )
        or (
            summary.parked_session_count == 0
            and (
                summary.breaker_reason is not None
                or summary.first_park_session is not None
            )
        )
    ):
        raise TqqqPromotionContractError("invalid episode summary")
    if metrics.benchmark_symbol != "QQQ":
        raise TqqqPromotionContractError("invalid benchmark identity")
    for field_name, value in vars(metrics).items():
        if field_name == "benchmark_symbol":
            continue
        if field_name in {"strategy_recovery_sessions", "qqq_recovery_sessions"}:
            if value is not None and (type(value) is not int or value < 0):
                raise TqqqPromotionContractError("invalid recovery metric")
            continue
        if field_name in {"strategy_unrecovered_at_end", "qqq_unrecovered_at_end"}:
            if type(value) is not bool:
                raise TqqqPromotionContractError("invalid recovery metric")
            continue
        _finite(value, "acceptance metric")
    if metrics.strategy_max_drawdown > 0.0 or metrics.qqq_max_drawdown > 0.0:
        raise TqqqPromotionContractError("invalid drawdown metric")
    defensive_only = all(
        trace.signal_regime == "DEFENSIVE" for trace in window.switching_traces
    )
    return metrics, summary, defensive_only


def evaluate_tqqq_pre_result_acceptance(
    result: TqqqPromotionResearchResult,
    legacy_parity_classification: str,
) -> str:
    """Apply the frozen pre-result terminal mapping to complete replay evidence."""

    if (
        type(result) is not TqqqPromotionResearchResult
        or type(legacy_parity_classification) is not str
        or legacy_parity_classification not in LEGACY_PARITY_CLASSIFICATIONS
    ):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if legacy_parity_classification == "UNEXPLAINED_CORE_STRATEGY_DRIFT":
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if (
        result.switching_characterization_sha256
        != TQQQ_SWITCHING_CHARACTERIZATION_SHA256
        or result.authority_scope != "RESEARCH_ONLY"
        or result.learning_only is not True
        or result.no_order is not True
        or result.size_zero_required is not True
        or result.promotion_eligible is not False
        or result.live_ready is not False
        or result.executable_plan
        or result.order_client_intents
    ):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    try:
        _validate_identity(result.identity)
        if result.timing_sha256 != _FROZEN_TIMING_SHA256:
            return TQQQ_ACCEPTANCE_INCONCLUSIVE
        expected_params = _params(result.identity, result.timing_sha256)
        scenarios = {scenario.total_cost_bps: scenario for scenario in result.scenarios}
    except (AttributeError, TypeError, TqqqPromotionContractError):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if set(scenarios) != set(_COST_SCENARIOS_BPS) or len(result.scenarios) != 3:
        return TQQQ_ACCEPTANCE_INCONCLUSIVE

    locked_metrics: dict[int, TqqqQqqRelativeMetrics] = {}
    locked_backtests: dict[int, BacktestResult] = {}
    locked_summaries: dict[int, TqqqEpisodeSummary] = {}
    locked_defensive_only: dict[int, bool] = {}
    try:
        for cost in _COST_SCENARIOS_BPS:
            scenario = scenarios[cost]
            if (
                scenario.cost_model_scope != "ALL_IN_PER_SIDE"
                or type(scenario.windows) is not tuple
                or len(scenario.windows) != 4
            ):
                return TQQQ_ACCEPTANCE_INCONCLUSIVE
            promotion_run = scenario.promotion_run
            promotion_plan = TqqqPromotionPlan(
                folds=promotion_run.folds,
                locked_oos_start=promotion_run.locked_oos_start,
                locked_oos_end=promotion_run.locked_oos_end,
                purge_days=promotion_run.purge_days,
                embargo_days=promotion_run.embargo_days,
            )
            _validate_plan(promotion_plan)
            if (
                _timing_sha256(promotion_plan) != result.timing_sha256
                or promotion_run.source_revision != result.identity.ues_revision
                or len(promotion_run.fold_results) != len(promotion_run.folds)
                or tuple(
                    (fold_result.start_date, fold_result.end_date)
                    for fold_result in promotion_run.fold_results
                )
                != tuple((fold.test_start, fold.test_end) for fold in promotion_run.folds)
                or promotion_run.locked_oos_result.start_date != _LOCKED_OOS_START
                or promotion_run.locked_oos_result.end_date != _LOCKED_OOS_END
                or tuple((window.start_date, window.end_date) for window in scenario.windows)
                != (
                    *tuple(
                        (fold.test_start, fold.test_end)
                        for fold in promotion_run.folds
                    ),
                    (_LOCKED_OOS_START, _LOCKED_OOS_END),
                )
            ):
                return TQQQ_ACCEPTANCE_INCONCLUSIVE
            cost_model = scenario.promotion_run.cost_model
            if (
                type(cost_model) is not PromotionCostModel
                or cost_model.model_id != f"tqqq_all_in_per_side_{cost}bp.v1"
                or _finite(cost_model.commission_bps, "commission", nonnegative=True)
                + _finite(cost_model.slippage_bps, "slippage", nonnegative=True)
                + _finite(cost_model.market_impact_bps, "market impact", nonnegative=True)
                != float(cost)
            ):
                return TQQQ_ACCEPTANCE_INCONCLUSIVE
            window_results = (*promotion_run.fold_results, promotion_run.locked_oos_result)
            param_set_ids = (
                *(
                    f"tqqq_etf_only_{cost}bp_wf{index}"
                    for index in range(len(promotion_run.folds))
                ),
                f"tqqq_etf_only_{cost}bp_locked_oos",
            )
            validated_windows = []
            for window, backtest, param_set_id in zip(
                scenario.windows, window_results, param_set_ids, strict=True
            ):
                expected_sessions = tuple(
                    session
                    for session in _FROZEN_XNYS_SESSIONS
                    if window.start_date <= session <= window.end_date
                )
                validated_windows.append(
                    _validated_window_evidence(
                        window,
                        backtest,
                        expected_sessions=expected_sessions,
                        expected_params=expected_params,
                        expected_param_set_id=param_set_id,
                        expected_initial_state_sha256=result.identity.initial_state_sha256,
                        expected_source_revision=result.identity.ues_revision,
                        total_cost_bps=cost,
                    )
                )
            locked = scenario.windows[-1]
            if locked.start_date != _LOCKED_OOS_START or locked.end_date != _LOCKED_OOS_END:
                return TQQQ_ACCEPTANCE_INCONCLUSIVE
            if (
                len(locked.sessions) != _LOCKED_OOS_SESSION_COUNT
                or locked.sessions[0] != _LOCKED_OOS_START
                or locked.sessions[-1] != _LOCKED_OOS_END
                or _canonical_sha256(
                    [session.isoformat() for session in locked.sessions]
                )
                != _LOCKED_OOS_SESSIONS_SHA256
            ):
                return TQQQ_ACCEPTANCE_INCONCLUSIVE
            metrics, summary, defensive_only = validated_windows[-1]
            locked_metrics[cost] = metrics
            locked_backtests[cost] = promotion_run.locked_oos_result
            locked_summaries[cost] = summary
            locked_defensive_only[cost] = defensive_only
    except (AttributeError, KeyError, TypeError, ValueError, TqqqPromotionContractError):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE

    if (
        float(locked_backtests[10].total_return)
        > float(locked_backtests[5].total_return) + 1e-12
        or float(locked_backtests[15].total_return)
        > float(locked_backtests[10].total_return) + 1e-12
    ):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if any(
        not _same_number(getattr(locked_metrics[cost], field), getattr(locked_metrics[15], field))
        for cost in (5, 10)
        for field in ("qqq_total_return", "boxx_total_return", "qqq_max_drawdown")
    ):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if any(
        summary.parked_session_count
        and summary.breaker_reason != "ACCOUNT_DRAWDOWN"
        for summary in locked_summaries.values()
    ):
        return TQQQ_ACCEPTANCE_INCONCLUSIVE
    if any(summary.parked_session_count for summary in locked_summaries.values()):
        return TQQQ_ACCEPTANCE_REJECT
    if any(
        abs(float(result.max_drawdown)) > 0.10 + 1e-12
        for result in locked_backtests.values()
    ):
        return TQQQ_ACCEPTANCE_REJECT

    metrics = locked_metrics[15]
    locked_backtest = locked_backtests[15]
    candidate_return = float(locked_backtest.total_return)
    qqq_return = metrics.qqq_total_return
    boxx_return = metrics.boxx_total_return
    candidate_mdd = abs(float(locked_backtest.max_drawdown))
    qqq_mdd = abs(float(locked_backtest.benchmark_max_drawdown))
    if qqq_return <= 0.0:
        passed = (
            candidate_return >= qqq_return
            and candidate_mdd <= min(0.10, 0.60 * qqq_mdd) + 1e-12
            and candidate_return >= boxx_return - 0.02
        )
    else:
        passed = (
            candidate_return > 0.0
            and candidate_return >= 0.50 * qqq_return
            and candidate_mdd <= qqq_mdd + 1e-12
            and candidate_mdd <= 0.10 + 1e-12
            and (
                not locked_defensive_only[15]
                or candidate_return >= boxx_return - 0.02
            )
        )
    return TQQQ_ACCEPTANCE_PASS if passed else TQQQ_ACCEPTANCE_REJECT


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
    if plan.locked_oos_start != _LOCKED_OOS_START or plan.locked_oos_end != _LOCKED_OOS_END:
        raise TqqqPromotionContractError("locked OOS calendar identity mismatch")
    if (
        type(plan.purge_days) is not int
        or plan.purge_days <= 0
        or type(plan.embargo_days) is not int
        or plan.embargo_days <= 0
    ):
        raise TqqqPromotionContractError("purge and embargo must be positive integers")
    if any(fold.test_start < _EXACT_COMMON_ELIGIBILITY for fold in plan.folds):
        raise TqqqPromotionContractError("replay window precedes exact common eligibility")
    if plan.locked_oos_start < _EXACT_COMMON_ELIGIBILITY:
        raise TqqqPromotionContractError("replay window precedes exact common eligibility")


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
    total_cost_bps: int,
) -> None:
    if type(replay) is not TqqqWindowReplay:
        raise TqqqPromotionContractError("invalid replay material")
    if replay.start_date != start_date or replay.end_date != end_date:
        raise TqqqPromotionContractError("replay timing mismatch")
    if replay.data_available is not True:
        raise TqqqPromotionContractError("data unavailable")
    if replay.prior_state_sha256 != prior_state_sha256 or not _is_hex(replay.final_state_sha256, 64):
        raise TqqqPromotionContractError("episode initial state identity mismatch")
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
        raise TqqqPromotionContractError("in-episode cash reset is forbidden; state must be continuous")
    if type(replay.warmup_sessions) is not int or replay.warmup_sessions < 257:
        raise TqqqPromotionContractError("warmup must include 252 dynamic-volatility observations")
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
    summary = replay.episode_summary
    if type(summary) is not TqqqEpisodeSummary:
        raise TqqqPromotionContractError("invalid episode summary")
    counts = (
        summary.episode_session_count,
        summary.tqqq_exposure_session_count,
        summary.qqqm_exposure_session_count,
        summary.boxx_exposure_session_count,
        summary.cash_only_session_count,
        summary.parked_session_count,
        summary.tqqq_entry_count,
        summary.tqqq_stop_armed_count,
        summary.tqqq_stop_crossing_count,
        summary.tqqq_stop_fill_count,
        summary.tqqq_unprotected_holding_session_count,
    )
    if any(type(value) is not int or value < 0 for value in counts):
        raise TqqqPromotionContractError("invalid episode summary")
    if any(value > summary.episode_session_count for value in counts[1:]):
        raise TqqqPromotionContractError("invalid episode summary")
    if summary.cash_only_session_count + summary.parked_session_count > summary.episode_session_count:
        raise TqqqPromotionContractError("invalid episode summary")
    if (
        summary.tqqq_entry_count != summary.tqqq_stop_armed_count
        or summary.tqqq_stop_crossing_count != summary.tqqq_stop_fill_count
        or summary.tqqq_stop_crossing_count > summary.tqqq_entry_count
        or summary.tqqq_unprotected_holding_session_count != 0
    ):
        raise TqqqPromotionContractError("TQQQ stop coverage is incomplete")
    allowed_breakers = {"ACCOUNT_DRAWDOWN", "RISK_ENGINE_NON_APPROVE"}
    if summary.parked_session_count:
        if summary.breaker_reason not in allowed_breakers or type(summary.first_park_session) is not date:
            raise TqqqPromotionContractError("invalid episode breaker summary")
        if not start_date <= summary.first_park_session <= end_date:
            raise TqqqPromotionContractError("invalid episode breaker summary")
    elif summary.breaker_reason is not None or summary.first_park_session is not None:
        raise TqqqPromotionContractError("invalid episode breaker summary")
    if type(replay.asset_weights) is not tuple:
        raise TqqqPromotionContractError("invalid ETF-only weights")
    seen: set[str] = set()
    effective_exposure = 0.0
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
        if weight > _ASSET_CAPS[symbol]:
            raise TqqqPromotionContractError("ETF nominal cap exceeded")
        effective_exposure += weight * _ASSET_FACTORS[symbol]
    if seen != _ALLOWED_ASSETS:
        raise TqqqPromotionContractError("ETF-only universe is incomplete")
    if effective_exposure > _EFFECTIVE_EXPOSURE_CAP + 1e-12:
        raise TqqqPromotionContractError("effective exposure cap exceeded")
    if (
        type(replay.decision_count) is not int
        or replay.decision_count <= 0
        or type(replay.risk_assessment_count) is not int
        or replay.risk_assessment_count != replay.decision_count
    ):
        raise TqqqPromotionContractError("RiskEngine assessment must occur exactly once per decision")
    if type(replay.trade_count) is not int or replay.trade_count < 0:
        raise TqqqPromotionContractError("invalid trade count")
    if (
        type(replay.sessions) is not tuple
        or not replay.sessions
        or any(type(session) is not date for session in replay.sessions)
        or replay.sessions != tuple(sorted(set(replay.sessions)))
        or replay.sessions[0] != start_date
        or replay.sessions[-1] != end_date
    ):
        raise TqqqPromotionContractError("replay session identity mismatch")
    expected_sessions = tuple(
        session
        for session in _FROZEN_XNYS_SESSIONS
        if start_date <= session <= end_date
    )
    if replay.sessions != expected_sessions:
        raise TqqqPromotionContractError("replay session identity mismatch")
    if (
        not 0 < replay.decision_count <= len(replay.sessions)
        or summary.episode_session_count != len(replay.sessions)
    ):
        raise TqqqPromotionContractError("episode session count mismatch")
    _validate_switching_traces(
        replay.switching_traces,
        sessions=replay.sessions,
        decision_count=replay.decision_count,
        total_cost_bps=total_cost_bps,
    )
    if (
        sum(trace.risk_disposition == "PARK" for trace in replay.switching_traces)
        != summary.parked_session_count
    ):
        raise TqqqPromotionContractError("parked trace/summary mismatch")
    _finite(replay.turnover, "turnover", nonnegative=True)
    if (
        type(replay.strategy_equity) is not tuple
        or type(replay.qqq_total_return_equity) is not tuple
        or type(replay.boxx_total_return_equity) is not tuple
        or len(replay.strategy_equity) < 2
        or len(replay.strategy_equity) != len(replay.qqq_total_return_equity)
        or len(replay.strategy_equity) != len(replay.boxx_total_return_equity)
    ):
        raise TqqqPromotionContractError("aligned strategy/QQQ/BOXX equity is required")
    for series in (
        replay.strategy_equity,
        replay.qqq_total_return_equity,
        replay.boxx_total_return_equity,
    ):
        if any(_finite(value, "equity") <= 0.0 for value in series):
            raise TqqqPromotionContractError("equity must be positive")
    if summary.episode_session_count != len(replay.strategy_equity) - 1:
        raise TqqqPromotionContractError("episode session count mismatch")


def _relative_metrics(replay: TqqqWindowReplay) -> TqqqQqqRelativeMetrics:
    strategy = tuple(float(value) for value in replay.strategy_equity)
    benchmark = tuple(float(value) for value in replay.qqq_total_return_equity)
    defensive_benchmark = tuple(float(value) for value in replay.boxx_total_return_equity)
    strategy_returns = _returns(strategy)
    benchmark_returns = _returns(benchmark)
    excess_returns = tuple(a - b for a, b in zip(strategy_returns, benchmark_returns))
    strategy_total = strategy[-1] / strategy[0] - 1.0
    benchmark_total = benchmark[-1] / benchmark[0] - 1.0
    defensive_benchmark_total = defensive_benchmark[-1] / defensive_benchmark[0] - 1.0
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
        boxx_total_return=defensive_benchmark_total,
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
            self.identity.initial_state_sha256,
        )
        _validate_replay(
            replay,
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=self.identity.initial_state_sha256,
            total_cost_bps=self.total_cost_bps,
        )
        metrics = _relative_metrics(replay)
        window = TqqqWindowEvidence(
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=replay.prior_state_sha256,
            final_state_sha256=replay.final_state_sha256,
            relative_metrics=metrics,
            episode_summary=replay.episode_summary,
            decision_count=replay.decision_count,
            risk_assessment_count=replay.risk_assessment_count,
            sessions=replay.sessions,
            switching_traces=replay.switching_traces,
        )
        self._windows.append(window)
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
            source_script=_window_acceptance_source(window, self.total_cost_bps),
        )


def _params(identity: TqqqPromotionIdentity, timing_sha256: str) -> dict[str, object]:
    return {
        "authority_scope": "RESEARCH_ONLY",
        "candidate_variant": _CANDIDATE_VARIANT,
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
