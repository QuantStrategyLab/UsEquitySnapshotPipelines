"""Models, constants, and configuration specs for Global ETF offensive rotation research."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from us_equity_strategies.manifests import global_etf_rotation_manifest
from us_equity_strategies.strategies import global_etf_rotation as strategy

DEFAULT_PERIODS = (
    ("short", "2025-06-01", None),
    ("medium", "2023-06-01", None),
    ("long", "2015-01-01", None),
)
DEFAULT_PRICE_START_DATE = "2010-01-01"
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_PRIMARY_BENCHMARK = "SPY"
DEFAULT_SECONDARY_BENCHMARK = "QQQ"
DEFAULT_SAFE_HAVEN = str(global_etf_rotation_manifest.default_config.get("safe_haven", strategy.SAFE_HAVEN))
DEFAULT_CANARY_ASSETS = tuple(
    global_etf_rotation_manifest.default_config.get("canary_assets", tuple(strategy.CANARY_ASSETS))
)
DEFAULT_BASE_POOL = tuple(global_etf_rotation_manifest.default_config.get("ranking_pool", tuple(strategy.RANKING_POOL)))
DEFAULT_MONTHLY_REBALANCE_MONTHS = tuple(range(1, 13))
DEFAULT_OFFENSIVE_POOL = (
    "QQQ",
    "VUG",
    "IWF",
    "MTUM",
    "VOO",
    "XLK",
    "SMH",
    "SOXX",
    "IGV",
    "IHI",
    "ITA",
    "XLC",
    "XLY",
    "XLE",
    "XLF",
    "KRE",
    "EWY",
    "EWT",
    "INDA",
    "EEM",
    "EWJ",
    "VGK",
)
MIN_TRADING_DAYS_PER_PERIOD = 60
DRAWNDOWN_ADVANTAGE_VS_QQQ = 0.05
DEFAULT_ROBUSTNESS_CANDIDATES = (
    "offensive_growth_fast_top2_monthly",
    "offensive_growth_dual_momentum_top2_monthly",
    "liveable_blend_baseline90_fast10",
    "liveable_blend_baseline90_dual10",
    "liveable_baseline_relative_decay_brake_baseline90_fast10_floor0",
    "liveable_blend_baseline85_fast15",
    "liveable_trend_drawdown_brake_baseline85_fast15_floor10",
    "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
    "liveable_blend_baseline80_fast20",
    "liveable_blend_baseline75_fast25",
    "liveable_blend_baseline70_fast30",
    "liveable_regime_qqqtrend_baseline70_fast30",
    "liveable_volmanaged_baseline70_fast30",
    "live_global_etf_rotation_defensive_baseline",
)
DEFAULT_ROLLING_ROBUSTNESS_YEARS = (3, 5)
DEFAULT_LIVE_BASELINE_CANDIDATE = "live_global_etf_rotation_defensive_baseline"
DEFAULT_WALK_FORWARD_CANDIDATES = (
    "liveable_blend_baseline90_fast10",
    "liveable_blend_baseline90_dual10",
    "liveable_baseline_relative_decay_brake_baseline90_fast10_floor0",
    "liveable_blend_baseline85_fast15",
    "liveable_trend_drawdown_brake_baseline85_fast15_floor10",
    "liveable_trend_drawdown_brake_baseline85_fast15_floor0",
    "liveable_blend_baseline80_fast20",
    "liveable_blend_baseline75_fast25",
    "liveable_blend_baseline70_fast30",
    "liveable_regime_qqqtrend_baseline70_fast30",
    "liveable_volmanaged_baseline70_fast30",
)
DEFAULT_WALK_FORWARD_TRAIN_YEARS = 5
DEFAULT_WALK_FORWARD_MIN_TRAIN_DAYS_PER_YEAR = 180
DEFAULT_WALK_FORWARD_MIN_TEST_DAYS = 120
DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR = 0.0
DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW = 63
DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME = 50_000_000.0
DEFAULT_DYNAMIC_COST_LOW_LIQUIDITY_PENALTY_BPS = 10.0
DEFAULT_DYNAMIC_COST_MAX_LOW_LIQUIDITY_PENALTY_BPS = 25.0
DEFAULT_DYNAMIC_COST_PARTICIPATION_RATE_THRESHOLD = 0.02
DEFAULT_DYNAMIC_COST_PARTICIPATION_PENALTY_BPS = 10.0
DEFAULT_DYNAMIC_COST_MAX_PARTICIPATION_PENALTY_BPS = 25.0
SAFE_LIKE_SYMBOLS = {"BIL", "BOXX", "SGOV", "CASH"}
LIVE_MIN_LONG_EXCESS_CAGR_VS_BASELINE = 0.0025
LIVE_MAX_LONG_DRAWDOWN_DEGRADATION_VS_BASELINE = 0.02
LIVE_MAX_MEDIAN_TURNOVER_INCREASE_VS_BASELINE = 2.0
LIVE_MIN_CALENDAR_BASELINE_CAGR_WIN_RATE = 0.50
LIVE_MIN_ROLLING_3Y_BASELINE_CAGR_WIN_RATE = 0.50
LIVE_MIN_ROLLING_5Y_BASELINE_CAGR_WIN_RATE = 0.60
LIVE_MIN_WORST_ROLLING_EXCESS_CAGR_VS_BASELINE = -0.03
LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE = 0.03
WALK_FORWARD_MIN_WINDOW_COUNT = 3
WALK_FORWARD_MIN_OOS_WIN_RATE = 0.50
WALK_FORWARD_MIN_OOS_MEDIAN_EXCESS_CAGR = 0.0
WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR = -0.03
WALK_FORWARD_MAX_WORST_OOS_DRAWDOWN_DEGRADATION = 0.03


@dataclass(frozen=True)
class GlobalEtfOffensiveVariantSpec:
    candidate_id: str
    display_name: str
    candidate_group: str
    rule: str
    ranking_pool: tuple[str, ...]
    primary_benchmark_symbol: str = DEFAULT_PRIMARY_BENCHMARK
    secondary_benchmark_symbol: str = DEFAULT_SECONDARY_BENCHMARK
    safe_haven: str = DEFAULT_SAFE_HAVEN
    canary_assets: tuple[str, ...] = DEFAULT_CANARY_ASSETS
    top_n: int = 2
    rebalance_months: tuple[int, ...] = DEFAULT_MONTHLY_REBALANCE_MONTHS
    sma_period: int = 250
    hold_bonus: float = 0.02
    canary_bad_threshold: int = 4
    score_mode: str = "runtime_13612w"
    canary_mode: str = "threshold"
    safe_fraction_per_bad_canary: float = 0.0
    max_safe_fraction: float = 1.0
    score_volatility_window: int = 126
    score_correlation_window: int = 126
    confidence_weighting_enabled: bool = False
    confidence_threshold: float = 1.0
    confidence_top1_weight: float = 0.75
    confidence_volatility_gate_enabled: bool = False
    confidence_volatility_window: int = 126
    confidence_volatility_max_ratio: float = 1.3
    notes: str = ""


@dataclass(frozen=True)
class GlobalEtfLiveableCompositeSpec:
    candidate_id: str
    display_name: str
    rule: str
    base_candidate_id: str
    overlay_candidate_id: str
    overlay_weight: float
    regime_symbol: str = DEFAULT_SECONDARY_BENCHMARK
    trend_sma_period: int = 200
    trend_fast_momentum_required: bool = True
    volatility_window: int = 63
    target_volatility: float = 0.18
    drawdown_window: int = 63
    drawdown_threshold: float = -0.08
    min_overlay_weight: float = 0.0
    relative_decay_fast_window: int = 63
    relative_decay_slow_window: int = 126
    relative_decay_fast_threshold: float = -0.03
    relative_decay_slow_threshold: float = 0.0
    notes: str = ""


@dataclass(frozen=True)
class DynamicExecutionCostConfig:
    base_cost_bps: float = DEFAULT_TURNOVER_COST_BPS
    dollar_volume_window: int = DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW
    low_liquidity_dollar_volume: float = DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME
    low_liquidity_penalty_bps: float = DEFAULT_DYNAMIC_COST_LOW_LIQUIDITY_PENALTY_BPS
    max_low_liquidity_penalty_bps: float = DEFAULT_DYNAMIC_COST_MAX_LOW_LIQUIDITY_PENALTY_BPS
    estimated_portfolio_nav: float = 0.0
    participation_rate_threshold: float = DEFAULT_DYNAMIC_COST_PARTICIPATION_RATE_THRESHOLD
    participation_penalty_bps: float = DEFAULT_DYNAMIC_COST_PARTICIPATION_PENALTY_BPS
    max_participation_penalty_bps: float = DEFAULT_DYNAMIC_COST_MAX_PARTICIPATION_PENALTY_BPS


GLOBAL_ETF_OFFENSIVE_VARIANTS: tuple[GlobalEtfOffensiveVariantSpec, ...] = (
    GlobalEtfOffensiveVariantSpec(
        candidate_id="live_global_etf_rotation_defensive_baseline",
        display_name="Live Global ETF Rotation Defensive Baseline",
        candidate_group="current_live_baseline",
        rule="quarterly_top2_confidence_vol_gate",
        ranking_pool=DEFAULT_BASE_POOL,
        rebalance_months=tuple(
            global_etf_rotation_manifest.default_config.get("rebalance_months", strategy.REBALANCE_MONTHS)
        ),
        sma_period=int(global_etf_rotation_manifest.default_config.get("sma_period", 250)),
        hold_bonus=float(global_etf_rotation_manifest.default_config.get("hold_bonus", 0.02)),
        canary_bad_threshold=int(global_etf_rotation_manifest.default_config.get("canary_bad_threshold", 4)),
        confidence_weighting_enabled=bool(
            global_etf_rotation_manifest.default_config.get("confidence_weighting_enabled", True)
        ),
        confidence_threshold=float(global_etf_rotation_manifest.default_config.get("confidence_threshold", 1.0)),
        confidence_top1_weight=float(global_etf_rotation_manifest.default_config.get("confidence_top1_weight", 0.75)),
        confidence_volatility_gate_enabled=bool(
            global_etf_rotation_manifest.default_config.get("confidence_volatility_gate_enabled", True)
        ),
        confidence_volatility_window=int(
            global_etf_rotation_manifest.default_config.get("confidence_volatility_window", 126)
        ),
        confidence_volatility_max_ratio=float(
            global_etf_rotation_manifest.default_config.get("confidence_volatility_max_ratio", 1.3)
        ),
        notes="Current live defensive profile; included only as baseline, not as an offensive candidate.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_top2_monthly",
        display_name="Offensive Growth Top2 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_growth_pool",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        canary_bad_threshold=4,
        notes="Aggressive growth/cyclical ETF pool, monthly rebalance, equal-weight top 2.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_top1_monthly",
        display_name="Offensive Growth Top1 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top1_growth_pool",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=1,
        sma_period=200,
        canary_bad_threshold=4,
        notes="Highest-conviction monthly top-1 variant; expected to raise return and concentration risk.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_top2_conf75_monthly",
        display_name="Offensive Growth Top2 Confidence 75/25 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_confidence_75_25_growth_pool",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        canary_bad_threshold=4,
        confidence_weighting_enabled=True,
        confidence_threshold=1.0,
        confidence_top1_weight=0.75,
        confidence_volatility_gate_enabled=True,
        confidence_volatility_window=126,
        confidence_volatility_max_ratio=1.3,
        notes="Monthly top-2 with 75/25 confidence tilt only when score gap is strong and top1 vol is not too high.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_top2_weak_canary_monthly",
        display_name="Offensive Growth Top2 Weak-Canary Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_growth_pool_weak_canary",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        canary_bad_threshold=99,
        notes="Monthly top-2 variant that disables daily all-BIL canary exits; included to measure offensive trade-off.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_eaa_top2_monthly",
        display_name="Offensive Growth EAA Top2 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_eaa_generalized_momentum",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        score_mode="eaa_generalized",
        score_volatility_window=126,
        score_correlation_window=126,
        notes=(
            "EAA-inspired generalized momentum: prefer high momentum, lower realized volatility, "
            "and lower correlation to the offensive pool."
        ),
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_fast_top2_monthly",
        display_name="Offensive Growth Fast-Momentum Top2 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_fast_1_3_6_momentum",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        score_mode="fast_136w",
        notes="VAA-inspired fast momentum filter using 1/3/6-month weighted returns and SMA eligibility.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_dual_momentum_top2_monthly",
        display_name="Offensive Growth Dual-Momentum Top2 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_dual_momentum_13612w_growth_pool",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        score_mode="dual_momentum_13612w",
        notes=(
            "Dual-momentum candidate: rank the offensive pool by 1/3/6/12-month relative strength, "
            "but require each selected ETF to have positive absolute momentum and be above its 200-day SMA."
        ),
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_daa_cash_fraction_top2_monthly",
        display_name="Offensive Growth DAA Cash-Fraction Top2 Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_daa_cash_fraction",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        canary_mode="cash_fraction",
        safe_fraction_per_bad_canary=0.25,
        max_safe_fraction=1.0,
        notes="DAA-inspired canary breadth: allocate 25% to safe haven for each bad canary instead of all-or-nothing exits.",
    ),
    GlobalEtfOffensiveVariantSpec(
        candidate_id="offensive_growth_eaa_daa_cash_fraction_monthly",
        display_name="Offensive Growth EAA + DAA Cash-Fraction Monthly",
        candidate_group="offensive_candidate",
        rule="monthly_top2_eaa_daa_cash_fraction",
        ranking_pool=DEFAULT_OFFENSIVE_POOL,
        top_n=2,
        sma_period=200,
        score_mode="eaa_generalized",
        canary_mode="cash_fraction",
        safe_fraction_per_bad_canary=0.25,
        max_safe_fraction=1.0,
        score_volatility_window=126,
        score_correlation_window=126,
        notes="Combines EAA-style generalized momentum selection with DAA-style proportional canary cash fraction.",
    ),
)

GLOBAL_ETF_LIVEABLE_COMPOSITES: tuple[GlobalEtfLiveableCompositeSpec, ...] = (
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline90_fast10",
        display_name="Liveable Blend Baseline 90 / Fast 10",
        rule="static_blend_baseline90_fast10",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.10,
        notes=(
            "Research-only sleeve-sensitivity candidate: keep 90% in current defensive baseline and allocate "
            "10% to the fast offensive sleeve."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline90_dual10",
        display_name="Liveable Blend Baseline 90 / Dual-Momentum 10",
        rule="static_blend_baseline90_dual10",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_dual_momentum_top2_monthly",
        overlay_weight=0.10,
        notes=(
            "Research-only dual-momentum sleeve: keep 90% in the current defensive baseline and allocate "
            "10% to the dual-momentum offensive sleeve."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_baseline_relative_decay_brake_baseline90_fast10_floor0",
        display_name="Liveable Baseline-Relative Decay Brake Baseline 90 / Fast 10 Floor 0",
        rule="baseline_relative_decay_brake_baseline90_fast10_floor0",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.10,
        min_overlay_weight=0.0,
        relative_decay_fast_window=63,
        relative_decay_slow_window=126,
        relative_decay_fast_threshold=-0.03,
        relative_decay_slow_threshold=0.0,
        notes=(
            "Research-only pre-registered narrow candidate: normally keep a 10% fast offensive sleeve, "
            "but reduce it to 0% on the next monthly rebalance when the fast sleeve's trailing 63-day "
            "gross return lags the defensive baseline by more than 3% and its trailing 126-day gross "
            "return also lags the baseline. This tests strategy-relative decay, not QQQ-only timing."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline85_fast15",
        display_name="Liveable Blend Baseline 85 / Fast 15",
        rule="static_blend_baseline85_fast15",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.15,
        notes=(
            "Research-only sleeve-sensitivity candidate: keep 85% in current defensive baseline and allocate "
            "15% to the fast offensive sleeve."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_trend_drawdown_brake_baseline85_fast15_floor10",
        display_name="Liveable Trend/Drawdown Brake Baseline 85 / Fast 15 Floor 10",
        rule="trend_drawdown_brake_baseline85_fast15_floor10",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.15,
        regime_symbol=DEFAULT_SECONDARY_BENCHMARK,
        trend_sma_period=200,
        drawdown_window=63,
        drawdown_threshold=-0.08,
        min_overlay_weight=0.10,
        notes=(
            "Research-only deterministic brake: cap the fast sleeve at 15%, but reduce it to 10% "
            "on the next monthly rebalance when QQQ is below its 200-day trend, fast momentum is "
            "negative, or the 63-day QQQ drawdown is worse than -8%."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_trend_drawdown_brake_baseline85_fast15_floor0",
        display_name="Liveable Trend/Drawdown Brake Baseline 85 / Fast 15 Floor 0",
        rule="trend_drawdown_brake_baseline85_fast15_floor0",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.15,
        regime_symbol=DEFAULT_SECONDARY_BENCHMARK,
        trend_sma_period=200,
        drawdown_window=63,
        drawdown_threshold=-0.08,
        min_overlay_weight=0.0,
        notes=(
            "Research-only deterministic brake: cap the fast sleeve at 15%, but switch back to 100% "
            "current defensive baseline on the next monthly rebalance when QQQ is below its 200-day "
            "trend, fast momentum is negative, or the 63-day QQQ drawdown is worse than -8%."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline80_fast20",
        display_name="Liveable Blend Baseline 80 / Fast 20",
        rule="static_blend_baseline80_fast20",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.20,
        notes=(
            "Research-only liveable sleeve candidate: keep 80% in current defensive baseline and allocate "
            "20% to the fast offensive sleeve. Composite returns are recomputed from combined daily weights."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline75_fast25",
        display_name="Liveable Blend Baseline 75 / Fast 25",
        rule="static_blend_baseline75_fast25",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.25,
        notes=(
            "Research-only sleeve-sensitivity candidate: keep 75% in current defensive baseline and allocate "
            "25% to the fast offensive sleeve."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_blend_baseline70_fast30",
        display_name="Liveable Blend Baseline 70 / Fast 30",
        rule="static_blend_baseline70_fast30",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.30,
        notes=(
            "Research-only liveable sleeve candidate: larger offensive sleeve while retaining the current "
            "defensive baseline as the core allocation."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_regime_qqqtrend_baseline70_fast30",
        display_name="Liveable QQQ-Trend Overlay Baseline 70 / Fast 30",
        rule="qqq_trend_overlay_baseline70_fast30",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.30,
        regime_symbol=DEFAULT_SECONDARY_BENCHMARK,
        trend_sma_period=200,
        notes=(
            "Research-only liveable overlay: add the 30% fast offensive sleeve only after QQQ is above its "
            "200-day trend and fast momentum is positive; otherwise hold 100% of the defensive baseline."
        ),
    ),
    GlobalEtfLiveableCompositeSpec(
        candidate_id="liveable_volmanaged_baseline70_fast30",
        display_name="Liveable Vol-Managed Overlay Baseline 70 / Fast 30",
        rule="volatility_managed_overlay_baseline70_fast30",
        base_candidate_id="live_global_etf_rotation_defensive_baseline",
        overlay_candidate_id="offensive_growth_fast_top2_monthly",
        overlay_weight=0.30,
        regime_symbol=DEFAULT_SECONDARY_BENCHMARK,
        trend_sma_period=200,
        volatility_window=63,
        target_volatility=0.18,
        min_overlay_weight=0.05,
        notes=(
            "Research-only liveable overlay: monthly QQQ trend gate plus volatility-managed fast sleeve. "
            "The overlay is capped at 30% and scaled down when 63-day realized QQQ volatility is above 18%."
        ),
    ),
)


@dataclass(frozen=True)
class GlobalEtfExperimentProfile:
    profile_id: str
    liveable_composite_ids: tuple[str, ...]
    description: str = ""


GLOBAL_ETF_EXPERIMENT_PROFILES: dict[str, GlobalEtfExperimentProfile] = {
    "live_replacement_shortlist_v1": GlobalEtfExperimentProfile(
        profile_id="live_replacement_shortlist_v1",
        liveable_composite_ids=(
            "liveable_blend_baseline85_fast15",
            "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
        ),
        description="Narrow Global ETF live-replacement shortlist for monthly review bundles.",
    ),
    "dynamic_overlay_cap_v1": GlobalEtfExperimentProfile(
        profile_id="dynamic_overlay_cap_v1",
        liveable_composite_ids=(
            "liveable_trend_drawdown_brake_baseline82_fast18_floor8",
            "liveable_oos_tail_guard_baseline82_fast18_floor12",
            "liveable_oos_tail_guard_baseline82_fast18_floor8",
        ),
        description="Dynamic overlay cap experiment candidates for promotion bundle scoping.",
    ),
}


def resolve_experiment_profile(profile_id: str | None) -> GlobalEtfExperimentProfile | None:
    cleaned = str(profile_id or "").strip()
    if not cleaned:
        return None
    return GLOBAL_ETF_EXPERIMENT_PROFILES.get(cleaned)


def _parse_periods(
    raw_periods: str | Sequence[tuple[str, str, str | None]] | None,
) -> tuple[tuple[str, str, str | None], ...]:
    if raw_periods is None:
        return DEFAULT_PERIODS
    if not isinstance(raw_periods, str):
        return tuple((str(name), str(start), None if end is None else str(end)) for name, start, end in raw_periods)

    periods: list[tuple[str, str, str | None]] = []
    for item in raw_periods.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) not in {2, 3}:
            raise ValueError("periods must use name:start[:end] entries")
        name, start = parts[0].strip(), parts[1].strip()
        end = parts[2].strip() if len(parts) == 3 and parts[2].strip() else None
        if not name or not start:
            raise ValueError("period name and start are required")
        periods.append((name, start, end))
    if not periods:
        raise ValueError("at least one period is required")
    return tuple(periods)


def _period_start(periods: Sequence[tuple[str, str, str | None]]) -> str:
    return min(pd.Timestamp(start).date().isoformat() for _name, start, _end in periods)


def _period_end(periods: Sequence[tuple[str, str, str | None]]) -> str | None:
    if any(end is None for _name, _start, end in periods):
        return None
    ends = [pd.Timestamp(end).date().isoformat() for _name, _start, end in periods if end]
    return max(ends) if ends else None


def _normalize_symbols(values: Sequence[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_values = values.split(",") if isinstance(values, str) else values
    cleaned: list[str] = []
    for value in raw_values:
        symbol = str(value or "").strip().upper()
        if symbol and symbol not in cleaned:
            cleaned.append(symbol)
    return tuple(cleaned)


def _normalize_candidate_ids(values: Sequence[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_values = values.split(",") if isinstance(values, str) else values
    cleaned: list[str] = []
    for value in raw_values:
        candidate_id = str(value or "").strip()
        if candidate_id and candidate_id not in cleaned:
            cleaned.append(candidate_id)
    return tuple(cleaned)


def _filter_variants(candidate_ids: Sequence[str]) -> tuple[GlobalEtfOffensiveVariantSpec, ...]:
    ids = tuple(candidate_ids)
    if not ids:
        return GLOBAL_ETF_OFFENSIVE_VARIANTS
    by_id = {spec.candidate_id: spec for spec in GLOBAL_ETF_OFFENSIVE_VARIANTS}
    missing = [candidate_id for candidate_id in ids if candidate_id not in by_id]
    if missing:
        raise ValueError(f"unknown variant candidate IDs: {missing}")
    return tuple(by_id[candidate_id] for candidate_id in ids)


def _filter_liveable_composites(candidate_ids: Sequence[str]) -> tuple[GlobalEtfLiveableCompositeSpec, ...]:
    ids = tuple(candidate_ids)
    if not ids:
        return GLOBAL_ETF_LIVEABLE_COMPOSITES
    by_id = {spec.candidate_id: spec for spec in GLOBAL_ETF_LIVEABLE_COMPOSITES}
    missing = [candidate_id for candidate_id in ids if candidate_id not in by_id]
    if missing:
        raise ValueError(f"unknown liveable composite IDs: {missing}")
    return tuple(by_id[candidate_id] for candidate_id in ids)


def _parse_float_list(values: Sequence[float] | str | None) -> tuple[float, ...]:
    if values is None:
        return ()
    raw_values = values.split(",") if isinstance(values, str) else values
    cleaned: list[float] = []
    for value in raw_values:
        raw = str(value).strip()
        if not raw:
            continue
        parsed = float(raw)
        if parsed < 0.0:
            raise ValueError("cost values must be non-negative")
        if parsed not in cleaned:
            cleaned.append(parsed)
    return tuple(cleaned)


def collect_required_symbols(
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
    composites: Sequence[GlobalEtfLiveableCompositeSpec] = GLOBAL_ETF_LIVEABLE_COMPOSITES,
) -> tuple[str, ...]:
    symbols: list[str] = []
    for variant in variants:
        for symbol in (
            *variant.ranking_pool,
            *variant.canary_assets,
            variant.safe_haven,
            variant.primary_benchmark_symbol,
            variant.secondary_benchmark_symbol,
        ):
            normalized = str(symbol or "").strip().upper()
            if normalized and normalized not in symbols:
                symbols.append(normalized)
    for composite in composites:
        normalized = str(composite.regime_symbol or "").strip().upper()
        if normalized and normalized not in symbols:
            symbols.append(normalized)
    return tuple(symbols)
