from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from us_equity_strategies.manifests import global_etf_rotation_manifest
from us_equity_strategies.strategies import global_etf_rotation as strategy

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .yfinance_prices import download_price_history

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
    "live_global_etf_rotation_defensive_baseline",
)
DEFAULT_ROLLING_ROBUSTNESS_YEARS = (3, 5)


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


def collect_required_symbols(
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
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
    return tuple(symbols)


def _normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise ValueError("price_history has no usable rows")
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.map(str).str.upper()
    return close.ffill()


def _normalize_weights(weights: Mapping[str, float], *, safe_haven: str) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): float(weight)
        for symbol, weight in weights.items()
        if str(symbol or "").strip() and pd.notna(weight) and abs(float(weight)) > 1e-12
    }
    total = sum(cleaned.values())
    if total <= 0.0:
        return {safe_haven.upper(): 1.0}
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def _compute_turnover(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    symbols = set(current) | set(target)
    return 0.5 * sum(abs(float(target.get(symbol, 0.0)) - float(current.get(symbol, 0.0))) for symbol in symbols)


def _benchmark_summary(returns: pd.Series, index: pd.DatetimeIndex) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(returns).reindex(index), errors="coerce").dropna()
    if clean.empty:
        return {"total_return": float("nan"), "cagr": float("nan"), "max_drawdown": float("nan")}
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return": float(equity.iloc[-1] - 1.0),
        "cagr": float(equity.iloc[-1] ** (1.0 / years) - 1.0),
        "max_drawdown": float(drawdown.min()),
    }


def summarize_returns(
    returns: pd.Series,
    *,
    weights_history: pd.DataFrame | None = None,
    benchmark_returns: pd.Series | None = None,
    secondary_benchmark_returns: pd.Series | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, float | str | int]:
    clean = pd.to_numeric(pd.Series(returns), errors="coerce").dropna()
    if clean.empty:
        return {
            "Start": start_date or "",
            "End": end_date or "",
            "Trading Days": 0,
            "Total Return": float("nan"),
            "CAGR": float("nan"),
            "Max Drawdown": float("nan"),
            "Volatility": float("nan"),
            "Sharpe": float("nan"),
            "Calmar": float("nan"),
            "Benchmark Total Return": float("nan"),
            "Benchmark CAGR": float("nan"),
            "Benchmark Max Drawdown": float("nan"),
            "Excess CAGR vs Benchmark": float("nan"),
            "Secondary Benchmark Total Return": float("nan"),
            "Secondary Benchmark CAGR": float("nan"),
            "Secondary Benchmark Max Drawdown": float("nan"),
            "Excess CAGR vs Secondary Benchmark": float("nan"),
            "Turnover/Year": float("nan"),
            "Avg Risk Exposure": float("nan"),
        }

    clean.index = pd.to_datetime(clean.index).tz_localize(None).normalize()
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(clean.std(ddof=0) * math.sqrt(252.0))
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252.0)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    benchmark = (
        _benchmark_summary(benchmark_returns, pd.DatetimeIndex(clean.index)) if benchmark_returns is not None else {}
    )
    secondary = (
        _benchmark_summary(secondary_benchmark_returns, pd.DatetimeIndex(clean.index))
        if secondary_benchmark_returns is not None
        else {}
    )
    benchmark_cagr = float(benchmark.get("cagr", float("nan")))
    secondary_cagr = float(secondary.get("cagr", float("nan")))

    turnover_per_year = float("nan")
    avg_risk_exposure = float("nan")
    if weights_history is not None and not weights_history.empty:
        weight_frame = pd.DataFrame(weights_history).copy()
        if "as_of" in weight_frame.columns:
            weight_frame["as_of"] = pd.to_datetime(weight_frame["as_of"]).dt.tz_localize(None).dt.normalize()
            weight_frame = weight_frame.set_index("as_of")
        weight_frame = weight_frame.reindex(clean.index).ffill().fillna(0.0)
        if not weight_frame.empty:
            turnover_per_year = float((0.5 * weight_frame.diff().abs().sum(axis=1).fillna(0.0)).sum() / years)
            safe_like = {"BIL", "BOXX", "SGOV", "CASH"}
            risk_columns = [column for column in weight_frame.columns if str(column).upper() not in safe_like]
            avg_risk_exposure = float(weight_frame[risk_columns].sum(axis=1).mean()) if risk_columns else 0.0

    return {
        "Start": clean.index[0].date().isoformat(),
        "End": clean.index[-1].date().isoformat(),
        "Trading Days": int(len(clean)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Benchmark Total Return": float(benchmark.get("total_return", float("nan"))),
        "Benchmark CAGR": benchmark_cagr,
        "Benchmark Max Drawdown": float(benchmark.get("max_drawdown", float("nan"))),
        "Excess CAGR vs Benchmark": cagr - benchmark_cagr if not pd.isna(benchmark_cagr) else float("nan"),
        "Secondary Benchmark Total Return": float(secondary.get("total_return", float("nan"))),
        "Secondary Benchmark CAGR": secondary_cagr,
        "Secondary Benchmark Max Drawdown": float(secondary.get("max_drawdown", float("nan"))),
        "Excess CAGR vs Secondary Benchmark": cagr - secondary_cagr if not pd.isna(secondary_cagr) else float("nan"),
        "Turnover/Year": turnover_per_year,
        "Avg Risk Exposure": avg_risk_exposure,
    }


@dataclass(frozen=True)
class IndicatorContext:
    close: pd.DataFrame
    returns: pd.DataFrame
    momentum: pd.DataFrame
    fast_momentum: pd.DataFrame
    sma_pass_by_period: Mapping[int, pd.DataFrame]
    volatility_by_window: Mapping[int, pd.DataFrame]


def _compute_13612w_momentum_series(series: pd.Series) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    output = pd.Series(float("nan"), index=series.index, dtype=float)
    if len(clean) < 253:
        return output
    monthly_last = clean.groupby(clean.index.to_period("M")).last()
    for as_of, current in clean.items():
        current_period = pd.Timestamp(as_of).to_period("M")
        values: list[float] = []
        for months, weight in ((1, 12), (3, 4), (6, 2), (12, 1)):
            prior = monthly_last.get(current_period - months, float("nan"))
            if pd.isna(prior) or float(prior) == 0.0:
                values = []
                break
            values.append(float(weight) * (float(current) / float(prior) - 1.0))
        if values:
            output.at[as_of] = float(sum(values) / 19.0)
    return output


def _build_indicator_context(
    close: pd.DataFrame,
    *,
    variants: Sequence[GlobalEtfOffensiveVariantSpec],
) -> IndicatorContext:
    sma_periods = sorted({int(variant.sma_period) for variant in variants})
    volatility_windows = sorted(
        {int(variant.confidence_volatility_window) for variant in variants}
        | {int(variant.score_volatility_window) for variant in variants}
    )
    returns = close.pct_change(fill_method=None).fillna(0.0)
    momentum = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for symbol in close.columns:
        momentum[symbol] = _compute_13612w_momentum_series(close[symbol])
    fast_momentum = (
        12.0 * (close / close.shift(21) - 1.0)
        + 4.0 * (close / close.shift(63) - 1.0)
        + 2.0 * (close / close.shift(126) - 1.0)
    ) / 18.0
    sma_pass_by_period = {
        period: close.gt(close.rolling(int(period), min_periods=int(period)).mean()) for period in sma_periods
    }
    volatility_by_window = {
        window: returns.rolling(int(window), min_periods=int(window)).std(ddof=0) * math.sqrt(252.0)
        for window in volatility_windows
    }
    return IndicatorContext(
        close=close,
        returns=returns,
        momentum=momentum,
        fast_momentum=fast_momentum,
        sma_pass_by_period=sma_pass_by_period,
        volatility_by_window=volatility_by_window,
    )


def _rebalance_dates(index: pd.DatetimeIndex, months: Sequence[int]) -> set[pd.Timestamp]:
    allowed_months = {int(month) for month in months}
    frame = pd.DataFrame({"as_of": index}, index=index)
    month_ends = frame.groupby(index.to_period("M"))["as_of"].tail(1)
    return {pd.Timestamp(value).normalize() for value in month_ends if pd.Timestamp(value).month in allowed_months}


def _indicator_value(frame: pd.DataFrame, as_of: pd.Timestamp, symbol: str) -> float:
    symbol = str(symbol).strip().upper()
    if symbol not in frame.columns or as_of not in frame.index:
        return float("nan")
    value = frame.at[as_of, symbol]
    return float(value) if pd.notna(value) else float("nan")


def _zscore_mapping(values: Mapping[str, float]) -> dict[str, float]:
    clean = {symbol: float(value) for symbol, value in values.items() if pd.notna(value)}
    if not clean:
        return {}
    series = pd.Series(clean, dtype=float)
    std = float(series.std(ddof=0))
    if std <= 0.0 or pd.isna(std):
        return {symbol: 0.0 for symbol in clean}
    return ((series - float(series.mean())) / std).to_dict()


def _pool_correlation_scores(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
) -> dict[str, float]:
    window = max(2, int(spec.score_correlation_window))
    pool = [symbol for symbol in _normalize_symbols(spec.ranking_pool) if symbol in context.returns.columns]
    frame = context.returns.loc[:as_of, pool].tail(window).dropna(axis=1, how="all")
    if len(frame) < max(2, window // 2) or frame.shape[1] < 2:
        return {}
    pool_index = frame.mean(axis=1)
    correlations: dict[str, float] = {}
    pool_std = float(pool_index.std(ddof=0))
    for symbol in frame.columns:
        symbol_std = float(frame[symbol].std(ddof=0))
        if pool_std <= 0.0 or symbol_std <= 0.0 or pd.isna(pool_std) or pd.isna(symbol_std):
            correlations[str(symbol)] = 0.0
            continue
        corr = frame[symbol].corr(pool_index)
        correlations[str(symbol)] = float(corr) if pd.notna(corr) else 0.0
    return correlations


def _candidate_scores(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
    current_holdings: Sequence[str],
) -> dict[str, float]:
    sma_pass = context.sma_pass_by_period[int(spec.sma_period)]
    current_set = {str(symbol).strip().upper() for symbol in current_holdings}
    rows: dict[str, dict[str, float]] = {}
    for ticker in spec.ranking_pool:
        symbol = str(ticker).strip().upper()
        momentum = _indicator_value(context.momentum, as_of, symbol)
        fast_momentum = _indicator_value(context.fast_momentum, as_of, symbol)
        if bool(_indicator_value(sma_pass, as_of, symbol)) is False:
            continue
        score_mode = str(spec.score_mode)
        if score_mode == "runtime_13612w":
            if pd.isna(momentum):
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
        elif score_mode == "fast_136w":
            if pd.isna(fast_momentum) or fast_momentum <= 0.0:
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
        else:
            if pd.isna(momentum) or momentum <= 0.0:
                continue
            rows[symbol] = {"momentum": momentum, "fast_momentum": fast_momentum}
    if not rows:
        return {}

    if str(spec.score_mode) == "runtime_13612w":
        return {
            symbol: values["momentum"] + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            for symbol, values in rows.items()
        }
    if str(spec.score_mode) == "fast_136w":
        return {
            symbol: values["fast_momentum"] + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            for symbol, values in rows.items()
        }
    if str(spec.score_mode) == "eaa_generalized":
        momentum_z = _zscore_mapping({symbol: values["momentum"] for symbol, values in rows.items()})
        volatility = context.volatility_by_window[int(spec.score_volatility_window)]
        vol_z = _zscore_mapping({symbol: _indicator_value(volatility, as_of, symbol) for symbol in rows})
        corr_z = _zscore_mapping(_pool_correlation_scores(context, spec, as_of=as_of))
        return {
            symbol: (
                momentum_z.get(symbol, 0.0)
                - 0.50 * vol_z.get(symbol, 0.0)
                - 0.25 * corr_z.get(symbol, 0.0)
                + (float(spec.hold_bonus) if symbol in current_set else 0.0)
            )
            for symbol in rows
        }
    raise ValueError(f"unsupported score_mode: {spec.score_mode}")


def _resolve_variant_target_weights(
    context: IndicatorContext,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    as_of: pd.Timestamp,
    current_holdings: Sequence[str],
    rebalance_dates: set[pd.Timestamp],
) -> tuple[dict[str, float] | None, str, bool, str]:
    safe_haven = spec.safe_haven.upper()
    n_bad = 0
    canary_details: list[str] = []
    for ticker in spec.canary_assets:
        symbol = str(ticker).strip().upper()
        momentum = _indicator_value(context.momentum, as_of, symbol)
        if pd.isna(momentum) or momentum < 0.0:
            n_bad += 1
            canary_details.append(f"{symbol}:❌({momentum:.3f})" if not pd.isna(momentum) else f"{symbol}:❌(nan)")
        else:
            canary_details.append(f"{symbol}:✅({momentum:.3f})")
    canary_str = ", ".join(canary_details)
    canary_mode = str(spec.canary_mode).strip().lower()
    if canary_mode == "threshold" and n_bad >= int(spec.canary_bad_threshold):
        return {safe_haven: 1.0}, "emergency", True, canary_str

    if pd.Timestamp(as_of).normalize() not in rebalance_dates:
        return None, "daily_check", False, canary_str

    scores = _candidate_scores(context, spec, as_of=as_of, current_holdings=current_holdings)
    sorted_tickers = sorted(scores.items(), key=lambda item: -item[1])
    top = sorted_tickers[: max(1, int(spec.top_n))]
    if not top:
        return {safe_haven: 1.0}, "emergency", False, canary_str

    per_weight = 1.0 / float(max(1, int(spec.top_n)))
    weights = {symbol: per_weight for symbol, _score in top}
    if spec.confidence_weighting_enabled and int(spec.top_n) == 2 and len(top) == 2:
        score_values = [score for _symbol, score in sorted_tickers]
        dispersion = float(np.nanstd(score_values))
        confidence = (
            float("nan") if dispersion <= 0.0 or np.isnan(dispersion) else float((top[0][1] - top[1][1]) / dispersion)
        )
        use_confidence_weight = not pd.isna(confidence) and confidence >= float(spec.confidence_threshold)
        if use_confidence_weight and spec.confidence_volatility_gate_enabled:
            volatility = context.volatility_by_window[int(spec.confidence_volatility_window)]
            top1_vol = _indicator_value(volatility, as_of, top[0][0])
            top2_vol = _indicator_value(volatility, as_of, top[1][0])
            use_confidence_weight = (
                not pd.isna(top1_vol)
                and not pd.isna(top2_vol)
                and top2_vol > 0.0
                and top1_vol <= top2_vol * float(spec.confidence_volatility_max_ratio)
            )
        if use_confidence_weight:
            top1_weight = min(1.0, max(per_weight, float(spec.confidence_top1_weight)))
            weights = {top[0][0]: top1_weight, top[1][0]: 1.0 - top1_weight}

    if canary_mode == "cash_fraction":
        canary_count = max(1, len(tuple(spec.canary_assets)))
        safe_fraction = min(float(spec.max_safe_fraction), float(spec.safe_fraction_per_bad_canary) * float(n_bad))
        if n_bad >= canary_count:
            safe_fraction = float(spec.max_safe_fraction)
        safe_fraction = max(0.0, min(1.0, safe_fraction))
        if safe_fraction > 0.0:
            risk_fraction = 1.0 - safe_fraction
            weights = {symbol: weight * risk_fraction for symbol, weight in weights.items()}
            weights[safe_haven] = weights.get(safe_haven, 0.0) + safe_fraction
    elif canary_mode != "threshold":
        raise ValueError(f"unsupported canary_mode: {spec.canary_mode}")
    return weights, "rebalance", False, canary_str


def run_variant_backtest(
    price_history: pd.DataFrame,
    spec: GlobalEtfOffensiveVariantSpec,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    indicator_context: IndicatorContext | None = None,
) -> dict[str, object]:
    close = _normalize_price_history(price_history) if indicator_context is None else indicator_context.close
    full_start = pd.Timestamp(start_date).normalize() if start_date else close.index.min()
    full_end = pd.Timestamp(end_date).normalize() if end_date else close.index.max()
    close = close.loc[close.index <= full_end].copy()
    context = indicator_context or _build_indicator_context(close, variants=(spec,))
    if context.close.index[-1] > full_end:
        context = IndicatorContext(
            close=context.close.loc[:full_end].copy(),
            returns=context.returns.loc[:full_end].copy(),
            momentum=context.momentum.loc[:full_end].copy(),
            fast_momentum=context.fast_momentum.loc[:full_end].copy(),
            sma_pass_by_period={key: value.loc[:full_end].copy() for key, value in context.sma_pass_by_period.items()},
            volatility_by_window={
                key: value.loc[:full_end].copy() for key, value in context.volatility_by_window.items()
            },
        )
    active_index = context.close.index[context.close.index >= full_start]
    if len(active_index) < 2:
        raise ValueError(f"not enough price history for {spec.candidate_id}")

    safe_haven = spec.safe_haven.upper()
    symbols = tuple(
        dict.fromkeys(
            (
                *tuple(symbol.upper() for symbol in spec.ranking_pool),
                *tuple(symbol.upper() for symbol in spec.canary_assets),
                spec.primary_benchmark_symbol.upper(),
                spec.secondary_benchmark_symbol.upper(),
                safe_haven,
            )
        )
    )
    portfolio_returns = pd.Series(0.0, index=active_index[1:], name=spec.candidate_id)
    turnover_history = pd.Series(0.0, index=active_index[1:], name="turnover")
    weights_history = pd.DataFrame(0.0, index=active_index[1:], columns=symbols)
    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: tuple[str, ...] = ()
    signal_rows: list[dict[str, object]] = []
    rebalance_dates = _rebalance_dates(active_index, spec.rebalance_months)

    for pos, as_of in enumerate(active_index[:-1]):
        next_date = active_index[pos + 1]
        weights, signal_description, is_emergency, canary = _resolve_variant_target_weights(
            context,
            spec,
            as_of=as_of,
            current_holdings=current_holdings,
            rebalance_dates=rebalance_dates,
        )

        if weights is not None:
            target_weights = _normalize_weights(weights, safe_haven=safe_haven)
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = tuple(
                symbol for symbol, weight in current_weights.items() if symbol != safe_haven and float(weight) > 0.0
            )
            signal_rows.append(
                {
                    "candidate_id": spec.candidate_id,
                    "as_of": as_of,
                    "next_date": next_date,
                    "signal_description": signal_description,
                    "is_emergency": bool(is_emergency),
                    "canary": canary,
                    "turnover": turnover,
                    **{f"weight_{symbol}": weight for symbol, weight in current_weights.items()},
                }
            )

        for symbol, weight in current_weights.items():
            if symbol in weights_history.columns:
                weights_history.at[next_date, symbol] = float(weight)
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol in context.returns.columns:
                value = context.returns.at[next_date, symbol]
                if pd.notna(value):
                    gross_return += float(weight) * float(value)
        cost = float(turnover_history.at[next_date]) * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    benchmark_returns = context.returns.get(
        spec.primary_benchmark_symbol.upper(), pd.Series(index=portfolio_returns.index, dtype=float)
    )
    secondary_benchmark_returns = context.returns.get(
        spec.secondary_benchmark_symbol.upper(), pd.Series(index=portfolio_returns.index, dtype=float)
    )
    summary = summarize_returns(
        portfolio_returns,
        weights_history=weights_history,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Group": spec.candidate_group,
            "Rule": spec.rule,
            "Primary Benchmark Symbol": spec.primary_benchmark_symbol.upper(),
            "Secondary Benchmark Symbol": spec.secondary_benchmark_symbol.upper(),
            "Safe Haven": safe_haven,
            "Top N": int(spec.top_n),
            "Rebalance Months": ",".join(str(month) for month in spec.rebalance_months),
            "SMA Period": int(spec.sma_period),
            "Canary Bad Threshold": int(spec.canary_bad_threshold),
            "Score Mode": str(spec.score_mode),
            "Canary Mode": str(spec.canary_mode),
            "Safe Fraction Per Bad Canary": float(spec.safe_fraction_per_bad_canary),
            "Confidence Weighting": bool(spec.confidence_weighting_enabled),
            "Confidence Volatility Gate": bool(spec.confidence_volatility_gate_enabled),
            "Ranking Pool Size": int(len(spec.ranking_pool)),
            "Ranking Pool": ",".join(spec.ranking_pool),
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history,
        "signal_history": pd.DataFrame(signal_rows),
        "benchmark_returns": benchmark_returns,
        "secondary_benchmark_returns": secondary_benchmark_returns,
    }


def _period_summary_from_result(
    result: Mapping[str, object],
    *,
    period_name: str,
    start_date: str,
    end_date: str | None,
) -> dict[str, object]:
    returns = pd.Series(result["portfolio_returns"]).copy()
    returns.index = pd.to_datetime(returns.index).tz_localize(None).normalize()
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else returns.index.max()
    period_returns = returns.loc[(returns.index >= start_ts) & (returns.index <= end_ts)]

    weights = pd.DataFrame(result.get("weights_history", pd.DataFrame())).copy()
    if not weights.empty:
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights = weights.loc[(weights.index >= start_ts) & (weights.index <= end_ts)]

    benchmark_returns = pd.Series(result.get("benchmark_returns", pd.Series(dtype=float))).copy()
    if not benchmark_returns.empty:
        benchmark_returns.index = pd.to_datetime(benchmark_returns.index).tz_localize(None).normalize()
    secondary_benchmark_returns = pd.Series(result.get("secondary_benchmark_returns", pd.Series(dtype=float))).copy()
    if not secondary_benchmark_returns.empty:
        secondary_benchmark_returns.index = (
            pd.to_datetime(secondary_benchmark_returns.index).tz_localize(None).normalize()
        )

    summary = summarize_returns(
        period_returns,
        weights_history=weights,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    base = dict(result["summary"])
    for key in (
        "Candidate",
        "Display Name",
        "Candidate Group",
        "Rule",
        "Primary Benchmark Symbol",
        "Secondary Benchmark Symbol",
        "Safe Haven",
        "Top N",
        "Rebalance Months",
        "SMA Period",
        "Canary Bad Threshold",
        "Score Mode",
        "Canary Mode",
        "Safe Fraction Per Bad Canary",
        "Confidence Weighting",
        "Confidence Volatility Gate",
        "Ranking Pool Size",
        "Ranking Pool",
        "Notes",
    ):
        summary[key] = base.get(key)
    return {"Period": period_name, **summary}


def _gate_reason(
    *,
    all_periods_available: bool,
    positive_return_all_periods: bool,
    positive_sharpe_all_periods: bool,
    long_spy_gate: bool,
    median_spy_gate: bool,
    qqq_tradeoff_gate: bool,
) -> str:
    reasons: list[str] = []
    if not all_periods_available:
        reasons.append("missing_or_too_short_period")
    if not positive_return_all_periods:
        reasons.append("non_positive_cagr_period")
    if not positive_sharpe_all_periods:
        reasons.append("non_positive_sharpe_period")
    if not long_spy_gate:
        reasons.append("long_cagr_not_above_spy")
    if not median_spy_gate:
        reasons.append("median_cagr_not_above_spy")
    if not qqq_tradeoff_gate:
        reasons.append("not_qqq_competitive_on_return_or_drawdown")
    return "pass" if not reasons else ";".join(reasons)


def _summarize_candidate_window(
    *,
    candidate_id: str,
    window_type: str,
    window: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
    secondary_benchmark_returns: pd.Series,
    min_trading_days: int,
    weights_history: pd.DataFrame | None = None,
) -> dict[str, object] | None:
    subset = pd.Series(portfolio_returns).loc[start:end].dropna()
    if len(subset) < int(min_trading_days):
        return None
    weights_subset = None
    if weights_history is not None and not weights_history.empty:
        weights = pd.DataFrame(weights_history).copy()
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights_subset = weights.loc[start:end]
    summary = summarize_returns(
        subset,
        weights_history=weights_subset,
        benchmark_returns=benchmark_returns,
        secondary_benchmark_returns=secondary_benchmark_returns,
        start_date=start.date().isoformat(),
        end_date=end.date().isoformat(),
    )
    summary["Candidate"] = candidate_id
    summary["Window Type"] = window_type
    summary["Window"] = window
    summary["Requested Start"] = start.date().isoformat()
    summary["Requested End"] = end.date().isoformat()
    return summary


def build_candidate_robustness_diagnostics(
    *,
    price_history: pd.DataFrame,
    portfolio_returns: pd.DataFrame,
    weights_by_candidate: Mapping[str, pd.DataFrame] | None = None,
    candidate_ids: Sequence[str] = DEFAULT_ROBUSTNESS_CANDIDATES,
    rolling_years: Sequence[int] = DEFAULT_ROLLING_ROBUSTNESS_YEARS,
    primary_benchmark_symbol: str = DEFAULT_PRIMARY_BENCHMARK,
    secondary_benchmark_symbol: str = DEFAULT_SECONDARY_BENCHMARK,
    min_calendar_trading_days: int = 120,
    min_rolling_trading_days_per_year: int = 180,
) -> dict[str, pd.DataFrame]:
    close = _normalize_price_history(price_history)
    daily_returns = close.pct_change(fill_method=None).fillna(0.0)
    primary_symbol = str(primary_benchmark_symbol).strip().upper()
    secondary_symbol = str(secondary_benchmark_symbol).strip().upper()
    primary_returns = daily_returns.get(primary_symbol, pd.Series(index=daily_returns.index, dtype=float))
    secondary_returns = daily_returns.get(secondary_symbol, pd.Series(index=daily_returns.index, dtype=float))
    weights_map = dict(weights_by_candidate or {})
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if portfolio.empty:
        return {"robustness_windows": pd.DataFrame(), "robustness_summary": pd.DataFrame()}
    portfolio.index = pd.to_datetime(portfolio.index).tz_localize(None).normalize()

    rows: list[dict[str, object]] = []
    for candidate_id in tuple(dict.fromkeys(str(item).strip() for item in candidate_ids if str(item).strip())):
        if candidate_id not in portfolio.columns:
            continue
        candidate_returns = pd.to_numeric(portfolio[candidate_id], errors="coerce").dropna()
        if candidate_returns.empty:
            continue
        candidate_weights = weights_map.get(candidate_id)
        for year, year_returns in candidate_returns.groupby(candidate_returns.index.year):
            start = pd.Timestamp(year_returns.index.min()).normalize()
            end = pd.Timestamp(year_returns.index.max()).normalize()
            row = _summarize_candidate_window(
                candidate_id=candidate_id,
                window_type="calendar_year",
                window=str(int(year)),
                start=start,
                end=end,
                portfolio_returns=candidate_returns,
                benchmark_returns=primary_returns,
                secondary_benchmark_returns=secondary_returns,
                min_trading_days=min_calendar_trading_days,
                weights_history=candidate_weights,
            )
            if row is not None:
                rows.append(row)

        year_end_dates = (
            pd.Series(candidate_returns.index, index=candidate_returns.index)
            .groupby(candidate_returns.index.to_period("Y"))
            .tail(1)
        )
        for years in tuple(int(value) for value in rolling_years):
            min_days = int(years) * int(min_rolling_trading_days_per_year)
            for end_value in year_end_dates:
                end = pd.Timestamp(end_value).normalize()
                start = pd.Timestamp(end - pd.DateOffset(years=int(years)) + pd.DateOffset(days=1)).normalize()
                row = _summarize_candidate_window(
                    candidate_id=candidate_id,
                    window_type=f"rolling_{years}y",
                    window=f"{start.date().isoformat()}_{end.date().isoformat()}",
                    start=start,
                    end=end,
                    portfolio_returns=candidate_returns,
                    benchmark_returns=primary_returns,
                    secondary_benchmark_returns=secondary_returns,
                    min_trading_days=min_days,
                    weights_history=candidate_weights,
                )
                if row is not None:
                    rows.append(row)

    windows = pd.DataFrame(rows)
    if windows.empty:
        return {"robustness_windows": windows, "robustness_summary": pd.DataFrame()}
    for column in (
        "CAGR",
        "Sharpe",
        "Max Drawdown",
        "Excess CAGR vs Benchmark",
        "Excess CAGR vs Secondary Benchmark",
        "Secondary Benchmark Max Drawdown",
        "Turnover/Year",
    ):
        windows[column] = pd.to_numeric(windows.get(column), errors="coerce")
    windows["Beats SPY CAGR"] = windows["Excess CAGR vs Benchmark"].gt(0.0)
    windows["Beats QQQ CAGR"] = windows["Excess CAGR vs Secondary Benchmark"].gt(0.0)
    windows["QQQ Drawdown Advantage"] = windows["Max Drawdown"] >= (
        windows["Secondary Benchmark Max Drawdown"] + DRAWNDOWN_ADVANTAGE_VS_QQQ
    )
    windows["QQQ Competitive"] = windows["Beats QQQ CAGR"] | windows["QQQ Drawdown Advantage"]

    summary_rows: list[dict[str, object]] = []
    for candidate_id, frame in windows.groupby("Candidate", sort=False):
        for window_type, subset in frame.groupby("Window Type", sort=False):
            count = int(len(subset))
            summary_rows.append(
                {
                    "Candidate": candidate_id,
                    "Window Type": window_type,
                    "Window Count": count,
                    "SPY CAGR Win Rate": float(subset["Beats SPY CAGR"].mean()) if count else float("nan"),
                    "QQQ CAGR Win Rate": float(subset["Beats QQQ CAGR"].mean()) if count else float("nan"),
                    "QQQ Competitive Rate": float(subset["QQQ Competitive"].mean()) if count else float("nan"),
                    "Median Excess CAGR vs SPY": float(subset["Excess CAGR vs Benchmark"].median()),
                    "Worst Excess CAGR vs SPY": float(subset["Excess CAGR vs Benchmark"].min()),
                    "Median Excess CAGR vs QQQ": float(subset["Excess CAGR vs Secondary Benchmark"].median()),
                    "Worst Excess CAGR vs QQQ": float(subset["Excess CAGR vs Secondary Benchmark"].min()),
                    "Worst Drawdown": float(subset["Max Drawdown"].min()),
                    "Median Sharpe": float(subset["Sharpe"].median()),
                    "Median Turnover/Year": float(subset["Turnover/Year"].median()),
                }
            )
    return {
        "robustness_windows": windows.sort_values(["Candidate", "Window Type", "Start"]).reset_index(drop=True),
        "robustness_summary": pd.DataFrame(summary_rows),
    }


def build_ranking(period_summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate, frame in period_summary.groupby("Candidate", sort=False):
        numeric = frame.copy()
        for column in (
            "CAGR",
            "Sharpe",
            "Max Drawdown",
            "Excess CAGR vs Benchmark",
            "Excess CAGR vs Secondary Benchmark",
            "Benchmark Max Drawdown",
            "Secondary Benchmark Max Drawdown",
            "Turnover/Year",
        ):
            numeric[column] = pd.to_numeric(numeric.get(column), errors="coerce")

        long_rows = numeric.loc[numeric["Period"].eq("long")]
        long_excess_spy = float(long_rows["Excess CAGR vs Benchmark"].iloc[0]) if not long_rows.empty else float("nan")
        long_excess_qqq = (
            float(long_rows["Excess CAGR vs Secondary Benchmark"].iloc[0]) if not long_rows.empty else float("nan")
        )
        min_sharpe = float(numeric["Sharpe"].min())
        median_sharpe = float(numeric["Sharpe"].median())
        worst_drawdown = float(numeric["Max Drawdown"].min())
        worst_qqq_drawdown = float(numeric["Secondary Benchmark Max Drawdown"].min())
        median_excess_spy = float(numeric["Excess CAGR vs Benchmark"].median())
        median_excess_qqq = float(numeric["Excess CAGR vs Secondary Benchmark"].median())
        median_turnover = float(numeric["Turnover/Year"].median())
        all_periods_available = (
            len(numeric) >= len(DEFAULT_PERIODS)
            and numeric["Trading Days"].fillna(0).ge(MIN_TRADING_DAYS_PER_PERIOD).all()
        )
        positive_return_all_periods = numeric["CAGR"].gt(0.0).all()
        positive_sharpe_all_periods = numeric["Sharpe"].gt(0.0).all()
        long_spy_gate = not pd.isna(long_excess_spy) and long_excess_spy > 0.0
        median_spy_gate = not pd.isna(median_excess_spy) and median_excess_spy > 0.0
        qqq_tradeoff_gate = bool(
            (not pd.isna(long_excess_qqq) and long_excess_qqq > 0.0)
            or (
                not pd.isna(worst_drawdown)
                and not pd.isna(worst_qqq_drawdown)
                and worst_drawdown >= worst_qqq_drawdown + DRAWNDOWN_ADVANTAGE_VS_QQQ
            )
        )
        research_gate_passed = bool(
            all_periods_available
            and positive_return_all_periods
            and positive_sharpe_all_periods
            and long_spy_gate
            and median_spy_gate
            and qqq_tradeoff_gate
        )
        first = frame.iloc[0]
        score = (
            min_sharpe
            + 0.50 * median_sharpe
            + 6.0 * long_excess_spy
            + 3.0 * median_excess_spy
            + 1.0 * median_excess_qqq
            + 0.75 * worst_drawdown
            - 0.03 * median_turnover
        )
        rows.append(
            {
                "Candidate": candidate,
                "Display Name": first.get("Display Name"),
                "Candidate Group": first.get("Candidate Group"),
                "Rule": first.get("Rule"),
                "Primary Benchmark Symbol": first.get("Primary Benchmark Symbol"),
                "Secondary Benchmark Symbol": first.get("Secondary Benchmark Symbol"),
                "min_sharpe": min_sharpe,
                "median_sharpe": median_sharpe,
                "median_excess_cagr_vs_spy": median_excess_spy,
                "long_excess_cagr_vs_spy": long_excess_spy,
                "median_excess_cagr_vs_qqq": median_excess_qqq,
                "long_excess_cagr_vs_qqq": long_excess_qqq,
                "worst_drawdown": worst_drawdown,
                "worst_qqq_drawdown": worst_qqq_drawdown,
                "median_turnover_per_year": median_turnover,
                "robustness_score": score,
                "research_gate_passed": research_gate_passed,
                "gate_reason": _gate_reason(
                    all_periods_available=all_periods_available,
                    positive_return_all_periods=positive_return_all_periods,
                    positive_sharpe_all_periods=positive_sharpe_all_periods,
                    long_spy_gate=long_spy_gate,
                    median_spy_gate=median_spy_gate,
                    qqq_tradeoff_gate=qqq_tradeoff_gate,
                ),
                "Top N": first.get("Top N"),
                "Rebalance Months": first.get("Rebalance Months"),
                "Canary Bad Threshold": first.get("Canary Bad Threshold"),
                "Score Mode": first.get("Score Mode"),
                "Canary Mode": first.get("Canary Mode"),
                "Safe Fraction Per Bad Canary": first.get("Safe Fraction Per Bad Canary"),
                "Confidence Weighting": first.get("Confidence Weighting"),
                "Notes": first.get("Notes"),
            }
        )

    ranking = (
        pd.DataFrame(rows)
        .sort_values(["research_gate_passed", "robustness_score"], ascending=[False, False])
        .reset_index(drop=True)
    )
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    ranking["paper_review_candidate"] = ranking["research_gate_passed"] & ranking["Candidate Group"].eq(
        "offensive_candidate"
    )
    ranking["review_action"] = "reject"
    ranking.loc[ranking["Candidate Group"].eq("current_live_baseline"), "review_action"] = "keep_current_live"
    ranking.loc[ranking["paper_review_candidate"], "review_action"] = "paper_review_only"
    return ranking


def run_offensive_research(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_PERIODS,
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, pd.DataFrame]:
    full_start = _period_start(periods)
    full_end = _period_end(periods)
    close = _normalize_price_history(price_history)
    if full_end is not None:
        close = close.loc[close.index <= pd.Timestamp(full_end).normalize()].copy()
    indicator_context = _build_indicator_context(close, variants=variants)
    rows: list[dict[str, object]] = []
    returns_by_candidate: dict[str, pd.Series] = {}
    weights_by_candidate: dict[str, pd.DataFrame] = {}
    signal_frames: list[pd.DataFrame] = []
    for spec in variants:
        result = run_variant_backtest(
            price_history,
            spec,
            start_date=full_start,
            end_date=full_end,
            turnover_cost_bps=turnover_cost_bps,
            indicator_context=indicator_context,
        )
        returns_by_candidate[spec.candidate_id] = pd.Series(result["portfolio_returns"], name=spec.candidate_id)
        weights_by_candidate[spec.candidate_id] = pd.DataFrame(result["weights_history"])
        signal_history = pd.DataFrame(result.get("signal_history", pd.DataFrame()))
        if not signal_history.empty:
            signal_frames.append(signal_history)
        for period_name, start_date, end_date in periods:
            rows.append(
                _period_summary_from_result(
                    result,
                    period_name=period_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    period_summary = pd.DataFrame(rows)
    ranking = build_ranking(period_summary)
    portfolio_returns = pd.concat(returns_by_candidate.values(), axis=1) if returns_by_candidate else pd.DataFrame()
    signal_history = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame()
    return {
        "period_summary": period_summary,
        "ranking": ranking,
        "portfolio_returns": portfolio_returns,
        "signal_history": signal_history,
        **{f"weights_{candidate_id}": weights for candidate_id, weights in weights_by_candidate.items()},
    }


def write_recommendation(output_dir: Path, *, ranking: pd.DataFrame, period_summary: pd.DataFrame) -> Path:
    path = output_dir / "recommendation.md"
    top_candidates = ranking.loc[ranking["paper_review_candidate"].astype(bool)].copy()
    if top_candidates.empty:
        recommendation = "暂不迁移到 live；保留当前 defensive baseline，进攻型候选继续 paper review 或补充样本。"
    else:
        names = ", ".join(top_candidates.head(3)["Candidate"].astype(str).tolist())
        recommendation = f"仅进入 paper review，不自动 live；优先复核候选：{names}。"

    baseline_rows = period_summary.loc[period_summary["Candidate"].eq("live_global_etf_rotation_defensive_baseline")]
    long_baseline = baseline_rows.loc[baseline_rows["Period"].eq("long")]
    baseline_text = ""
    if not long_baseline.empty:
        row = long_baseline.iloc[0]
        baseline_text = (
            f"- Baseline long CAGR: {float(row['CAGR']):.2%}; "
            f"SPY excess: {float(row['Excess CAGR vs Benchmark']):.2%}; "
            f"QQQ excess: {float(row['Excess CAGR vs Secondary Benchmark']):.2%}; "
            f"Max drawdown: {float(row['Max Drawdown']):.2%}\n"
        )

    lines = [
        "# Global ETF Offensive Rotation Research Recommendation",
        "",
        "## Recommendation",
        "",
        recommendation,
        "",
        "## Baseline Long-Window Snapshot",
        "",
        baseline_text or "- Baseline long-window row unavailable.\n",
        "## Ranking Preview",
        "",
        "```csv",
        ranking.head(10).to_csv(index=False).strip(),
        "```",
        "",
        "## Boundary",
        "",
        "This is a research-only output. It does not change the live `global_etf_rotation` manifest or runtime behavior.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest research-only offensive Global ETF rotation variants against SPY and QQQ."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--prices", help="Existing long price-history CSV/JSON/Parquet with symbol/as_of/close columns"
    )
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end")
    parser.add_argument(
        "--periods", default=",".join(f"{name}:{start}:{end or ''}" for name, start, end in DEFAULT_PERIODS)
    )
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--symbols", help="Optional comma-separated override for downloaded symbols")
    parser.add_argument(
        "--robustness-candidates",
        default=",".join(DEFAULT_ROBUSTNESS_CANDIDATES),
        help="Comma-separated candidate IDs for calendar-year and rolling robustness diagnostics.",
    )
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    periods = _parse_periods(args.periods)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        symbols = _normalize_symbols(args.symbols) or collect_required_symbols()
        prices = download_price_history(list(symbols), start=args.price_start, end=args.price_end)
        prices.to_csv(output_dir / "downloaded_price_history.csv", index=False)
    else:
        prices = read_table(args.prices)

    result = run_offensive_research(
        price_history=prices,
        periods=periods,
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    result["period_summary"].to_csv(output_dir / "period_summary.csv", index=False)
    result["ranking"].to_csv(output_dir / "ranking.csv", index=False)
    result["portfolio_returns"].to_csv(output_dir / "portfolio_returns.csv")
    result["signal_history"].to_csv(output_dir / "rebalance_events.csv", index=False)
    robustness = build_candidate_robustness_diagnostics(
        price_history=prices,
        portfolio_returns=result["portfolio_returns"],
        weights_by_candidate={
            key.removeprefix("weights_"): value for key, value in result.items() if key.startswith("weights_")
        },
        candidate_ids=_normalize_candidate_ids(args.robustness_candidates) or DEFAULT_ROBUSTNESS_CANDIDATES,
    )
    robustness["robustness_windows"].to_csv(output_dir / "candidate_robustness_windows.csv", index=False)
    robustness["robustness_summary"].to_csv(output_dir / "candidate_robustness_summary.csv", index=False)
    for key, value in result.items():
        if key.startswith("weights_"):
            pd.DataFrame(value).to_csv(output_dir / f"{key}.csv")
    write_recommendation(output_dir, ranking=result["ranking"], period_summary=result["period_summary"])
    manifest = {
        "research": "global_etf_offensive_rotation",
        "periods": [{"name": name, "start": start, "end": end} for name, start, end in periods],
        "turnover_cost_bps": float(args.turnover_cost_bps),
        "variants": [asdict(variant) for variant in GLOBAL_ETF_OFFENSIVE_VARIANTS],
        "outputs": sorted(path.name for path in output_dir.iterdir() if path.is_file()),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(result["ranking"].to_string(index=False))
    print(f"wrote global ETF offensive research outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
