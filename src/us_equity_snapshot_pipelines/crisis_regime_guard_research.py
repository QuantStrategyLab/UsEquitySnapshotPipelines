from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_backtest import DEFAULT_TURNOVER_COST_BPS, summarize_returns
from .taco_panic_rebound_overlay_compare import (
    DEFAULT_BENCHMARK_SYMBOL,
    DEFAULT_CASH_SYMBOL,
    DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    DEFAULT_SAFE_SYMBOL,
    DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    add_synthetic_attack_close,
    apply_price_crisis_guard_to_weights,
    build_price_crisis_guard_signal,
    build_tqqq_growth_income_base_weights,
)
from .taco_panic_rebound_research import price_history_to_close_matrix
from .yfinance_prices import download_price_history

DEFAULT_START_DATE = "1999-03-10"
DEFAULT_PRICE_START_DATE = "1999-03-10"
DEFAULT_ATTACK_SYMBOL = "SYNTH_TQQQ"
DEFAULT_SAFE_SYMBOL_FOR_CRISIS = "SHY"
DEFAULT_SYNTHETIC_ATTACK_MULTIPLE = 3.0
DEFAULT_DRAWDOWN_THRESHOLDS = (-0.20, -0.25, -0.30)
DEFAULT_RISK_MULTIPLIERS = (0.0, 0.25, 0.50)
CONTEXT_GATE_NONE = "none"
CONTEXT_GATE_BUBBLE = "bubble"
CONTEXT_GATE_FINANCIAL = "financial"
CONTEXT_GATE_BUBBLE_OR_FINANCIAL = "bubble_or_financial"
CONTEXT_GATE_AI_RUBRIC = "ai_rubric"
CONTEXT_GATES = frozenset(
    {
        CONTEXT_GATE_NONE,
        CONTEXT_GATE_BUBBLE,
        CONTEXT_GATE_FINANCIAL,
        CONTEXT_GATE_BUBBLE_OR_FINANCIAL,
        CONTEXT_GATE_AI_RUBRIC,
    }
)
DEFAULT_CONTEXT_GATES = (CONTEXT_GATE_NONE,)
DEFAULT_BUBBLE_LOOKBACK_DAYS = 252
DEFAULT_BUBBLE_PERSISTENCE_DAYS = 126
DEFAULT_BUBBLE_RETURN_THRESHOLD = 0.75
DEFAULT_FINANCIAL_SYMBOL = "XLF"
DEFAULT_MARKET_SYMBOL = "SPY"
DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD = -0.25
DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS = 126
DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD = -0.10

CRISIS_COMPARISON_PERIODS: tuple[tuple[str, str, str | None], ...] = (
    ("full_1999_to_date", "1999-03-10", None),
    ("dotcom_bubble_burst", "2000-03-24", "2002-10-09"),
    ("dotcom_full_cycle", "1999-03-10", "2002-10-09"),
    ("gfc_peak_to_trough", "2007-10-09", "2009-03-09"),
    ("lost_decade_2000_2009", "2000-01-03", "2009-12-31"),
    ("synthetic_tqqq_live_proxy", "2010-02-11", None),
    ("covid_crash_2020", "2020-02-18", "2020-04-30"),
    ("biden_2022_bear", "2022-01-03", "2022-12-30"),
    ("full_2015_to_date", "2015-01-02", None),
    ("trump_1_full", "2017-01-20", "2021-01-19"),
    ("trade_war_2018_2019", "2018-01-02", "2019-12-31"),
    ("biden_full", "2021-01-20", "2025-01-17"),
    ("trump_2_to_date", "2025-01-21", None),
)

SUMMARY_COLUMNS = (
    "Period",
    "Strategy",
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Trades",
    "Final Equity",
)


@dataclass(frozen=True)
class CrisisGuardSpec:
    name: str
    drawdown_threshold: float
    risk_multiplier: float
    confirm_days: int = 1
    context_gate: str = CONTEXT_GATE_NONE


def _normalize_close(close: pd.DataFrame) -> pd.DataFrame:
    frame = close.copy().sort_index()
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame.columns = frame.columns.astype(str).str.upper().str.strip()
    return frame


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): float(weight)
        for symbol, weight in weights.items()
        if pd.notna(weight) and abs(float(weight)) > 1e-12
    }
    total = sum(cleaned.values())
    if total <= 0:
        return {DEFAULT_CASH_SYMBOL: 1.0}
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def _weights_to_returns(
    returns: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    strategy_name: str,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> tuple[pd.Series, pd.DataFrame]:
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    index = returns.index
    output = pd.Series(0.0, index=index, name=strategy_name)
    rows: list[dict[str, object]] = []
    previous: dict[str, float] | None = None
    for pos, date in enumerate(index[:-1]):
        next_date = index[pos + 1]
        row = _normalize_weights(weights.loc[date].to_dict())
        turnover = 0.0
        if previous is not None:
            symbols = set(previous) | set(row)
            turnover = 0.5 * sum(abs(row.get(symbol, 0.0) - previous.get(symbol, 0.0)) for symbol in symbols)
        gross_return = 0.0
        for symbol, weight in row.items():
            if symbol == cash_symbol:
                continue
            gross_return += float(weight) * float(returns.at[next_date, symbol] if symbol in returns.columns else 0.0)
        output.at[next_date] = gross_return - turnover * (float(turnover_cost_bps) / 10_000.0)
        rows.append({"as_of": date, **row, "turnover": turnover})
        previous = row
    weights_history = pd.DataFrame(rows).set_index("as_of").sort_index()
    return output, weights_history


def _parse_float_tuple(raw: str | Sequence[float]) -> tuple[float, ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    output: list[float] = []
    for value in values:
        text = str(value).strip()
        if text:
            output.append(float(text))
    return tuple(output)


def _parse_str_tuple(raw: str | Sequence[str]) -> tuple[str, ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    output: list[str] = []
    for value in values:
        text = str(value).strip().lower()
        if text:
            output.append(text)
    return tuple(output)


def build_crisis_guard_specs(
    *,
    drawdown_thresholds: Sequence[float] = DEFAULT_DRAWDOWN_THRESHOLDS,
    risk_multipliers: Sequence[float] = DEFAULT_RISK_MULTIPLIERS,
    confirm_days: int = 1,
    context_gates: Sequence[str] = DEFAULT_CONTEXT_GATES,
) -> tuple[CrisisGuardSpec, ...]:
    specs: list[CrisisGuardSpec] = []
    cleaned_context_gates = tuple(dict.fromkeys(_parse_str_tuple(context_gates)))
    if not cleaned_context_gates:
        cleaned_context_gates = DEFAULT_CONTEXT_GATES
    invalid = [gate for gate in cleaned_context_gates if gate not in CONTEXT_GATES]
    if invalid:
        raise ValueError(f"Unsupported context gate(s): {', '.join(invalid)}")
    for context_gate in cleaned_context_gates:
        for drawdown_threshold in drawdown_thresholds:
            for risk_multiplier in risk_multipliers:
                dd_label = int(abs(float(drawdown_threshold)) * 100)
                risk_label = int(float(risk_multiplier) * 100)
                confirm_label = f"_confirm{int(confirm_days)}" if int(confirm_days) > 1 else ""
                context_label = "" if context_gate == CONTEXT_GATE_NONE else f"_{context_gate}"
                specs.append(
                    CrisisGuardSpec(
                        name=f"crisis_guard{context_label}_dd{dd_label}_risk{risk_label}{confirm_label}",
                        drawdown_threshold=float(drawdown_threshold),
                        risk_multiplier=float(risk_multiplier),
                        confirm_days=max(1, int(confirm_days)),
                        context_gate=context_gate,
                    )
                )
    return tuple(specs)


def _apply_confirm_days(signal: pd.Series, confirm_days: int) -> pd.Series:
    raw = pd.Series(signal).fillna(False).astype(bool)
    if int(confirm_days) <= 1:
        return raw
    confirmed = raw.rolling(int(confirm_days), min_periods=int(confirm_days)).sum().ge(int(confirm_days))
    return confirmed.fillna(False).rename(raw.name)


def _window_index(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
) -> pd.DatetimeIndex:
    index = close.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if index.empty:
        raise RuntimeError("No trading days in requested context window")
    return index


def build_bubble_context_gate(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    lookback_days: int = DEFAULT_BUBBLE_LOOKBACK_DAYS,
    persistence_days: int = DEFAULT_BUBBLE_PERSISTENCE_DAYS,
    return_threshold: float = DEFAULT_BUBBLE_RETURN_THRESHOLD,
) -> pd.Series:
    """Return a point-in-time bubble context proxy using only trailing price data."""
    frame = _normalize_close(close)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    if benchmark_symbol not in frame.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")
    index = _window_index(frame, start_date=start_date, end_date=end_date)
    benchmark = pd.to_numeric(frame[benchmark_symbol], errors="coerce")
    trailing_return = benchmark / benchmark.shift(int(lookback_days)) - 1.0
    raw_gate = trailing_return.ge(float(return_threshold))
    if int(persistence_days) > 0:
        raw_gate = raw_gate.rolling(int(persistence_days) + 1, min_periods=1).max().astype(bool)
    gate = raw_gate.reindex(index).fillna(False)
    return gate.rename("bubble_context_active")


def build_financial_context_gate(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    financial_symbol: str = DEFAULT_FINANCIAL_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    drawdown_threshold: float = DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    relative_lookback_days: int = DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    relative_return_threshold: float = DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
) -> pd.Series:
    """Return a financial-stress context proxy for bank/credit-led crises."""
    frame = _normalize_close(close)
    financial_symbol = str(financial_symbol).strip().upper()
    market_symbol = str(market_symbol).strip().upper()
    missing = [symbol for symbol in (financial_symbol, market_symbol) if symbol not in frame.columns]
    if missing:
        raise ValueError(f"financial context gate requires missing symbol(s): {', '.join(missing)}")
    index = _window_index(frame, start_date=start_date, end_date=end_date)
    financial = pd.to_numeric(frame[financial_symbol], errors="coerce")
    market = pd.to_numeric(frame[market_symbol], errors="coerce")
    financial_high = financial.rolling(252, min_periods=63).max()
    financial_drawdown = financial / financial_high - 1.0
    financial_return = financial / financial.shift(int(relative_lookback_days)) - 1.0
    market_return = market / market.shift(int(relative_lookback_days)) - 1.0
    relative_return = financial_return - market_return
    gate = (
        financial_drawdown.le(float(drawdown_threshold))
        & relative_return.le(float(relative_return_threshold))
    ).reindex(index).fillna(False)
    return gate.rename("financial_context_active")


def build_ai_crisis_opinions(
    close: pd.DataFrame,
    price_signal: pd.Series,
    *,
    strategy_name: str = "ai_crisis_guard",
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    bubble_lookback_days: int = DEFAULT_BUBBLE_LOOKBACK_DAYS,
    bubble_persistence_days: int = DEFAULT_BUBBLE_PERSISTENCE_DAYS,
    bubble_return_threshold: float = DEFAULT_BUBBLE_RETURN_THRESHOLD,
    financial_symbol: str = DEFAULT_FINANCIAL_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    financial_drawdown_threshold: float = DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    financial_relative_lookback_days: int = DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    financial_relative_return_threshold: float = DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
    trigger_only: bool = True,
) -> pd.DataFrame:
    """Build a deterministic two-AI crisis-opinion proxy.

    This is not a replay of historical ChatGPT calls. It is a backtestable
    rubric that mimics the intended production contract: the confirmed price
    crisis signal opens the AI scanner, proposer AI identifies the crisis type,
    auditor AI can only approve or veto protection, and the final decision is
    point-in-time and structured for later audit.
    """
    frame = _normalize_close(close)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    if benchmark_symbol not in frame.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")
    index = _window_index(frame, start_date=start_date, end_date=end_date)

    price = pd.Series(price_signal).fillna(False).astype(bool).copy()
    price.index = pd.to_datetime(price.index).tz_localize(None).normalize()
    price = price.reindex(index).ffill().fillna(False)
    opinion_index = index[price] if trigger_only else index

    benchmark = pd.to_numeric(frame[benchmark_symbol], errors="coerce")
    benchmark_high = benchmark.rolling(252, min_periods=63).max()
    benchmark_drawdown = (benchmark / benchmark_high - 1.0).reindex(index)
    benchmark_trailing_return = (
        benchmark / benchmark.shift(int(bubble_lookback_days)) - 1.0
    ).reindex(index)

    bubble = build_bubble_context_gate(
        frame,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        lookback_days=bubble_lookback_days,
        persistence_days=bubble_persistence_days,
        return_threshold=bubble_return_threshold,
    ).reindex(index).fillna(False)

    financial = build_financial_context_gate(
        frame,
        start_date=start_date,
        end_date=end_date,
        financial_symbol=financial_symbol,
        market_symbol=market_symbol,
        drawdown_threshold=financial_drawdown_threshold,
        relative_lookback_days=financial_relative_lookback_days,
        relative_return_threshold=financial_relative_return_threshold,
    ).reindex(index).fillna(False)

    financial_symbol = str(financial_symbol).strip().upper()
    market_symbol = str(market_symbol).strip().upper()
    financial_close = pd.to_numeric(frame[financial_symbol], errors="coerce")
    market_close = pd.to_numeric(frame[market_symbol], errors="coerce")
    financial_high = financial_close.rolling(252, min_periods=63).max()
    financial_drawdown = (financial_close / financial_high - 1.0).reindex(index)
    financial_relative_return = (
        financial_close / financial_close.shift(int(financial_relative_lookback_days))
        - market_close / market_close.shift(int(financial_relative_lookback_days))
    ).reindex(index)

    columns = [
        "strategy",
        "as_of",
        "price_crisis_confirmed",
        "bubble_context",
        "financial_context",
        "benchmark_drawdown_252d",
        f"benchmark_return_{int(bubble_lookback_days)}d",
        "financial_drawdown_252d",
        f"financial_relative_return_{int(financial_relative_lookback_days)}d",
        "proposer_verdict",
        "auditor_verdict",
        "crisis_type",
        "final_context_allowed",
        "confidence",
        "reason",
    ]
    rows: list[dict[str, object]] = []
    for date in opinion_index:
        price_active = bool(price.loc[date])
        bubble_active = bool(bubble.loc[date])
        financial_active = bool(financial.loc[date])
        if price_active and financial_active:
            crisis_type = "financial_crisis_risk"
            proposer_verdict = "allow_guard"
            auditor_verdict = "approve"
            final_allowed = True
            confidence = 0.88
            reason = "price crisis confirmed and financial sector stress is active"
        elif price_active and bubble_active:
            crisis_type = "bubble_burst_risk"
            proposer_verdict = "allow_guard"
            auditor_verdict = "approve"
            final_allowed = True
            confidence = 0.82
            reason = "price crisis confirmed after bubble context"
        elif price_active:
            crisis_type = "non_systemic_bear_or_policy_shock"
            proposer_verdict = "watch_only"
            auditor_verdict = "veto_missing_bubble_or_financial_context"
            final_allowed = False
            confidence = 0.72
            reason = "price crisis confirmed but systemic context is missing"
        elif financial_active:
            crisis_type = "financial_watch"
            proposer_verdict = "watch_only"
            auditor_verdict = "veto_price_not_confirmed"
            final_allowed = False
            confidence = 0.66
            reason = "financial stress context exists but price crisis is not confirmed"
        elif bubble_active:
            crisis_type = "bubble_watch"
            proposer_verdict = "watch_only"
            auditor_verdict = "veto_price_not_confirmed"
            final_allowed = False
            confidence = 0.62
            reason = "bubble context exists but price crisis is not confirmed"
        else:
            crisis_type = "normal"
            proposer_verdict = "no_action"
            auditor_verdict = "no_action"
            final_allowed = False
            confidence = 0.60
            reason = "no systemic crisis context"
        rows.append(
            {
                "strategy": strategy_name,
                "as_of": pd.Timestamp(date).date().isoformat(),
                "price_crisis_confirmed": price_active,
                "bubble_context": bubble_active,
                "financial_context": financial_active,
                "benchmark_drawdown_252d": float(benchmark_drawdown.loc[date])
                if pd.notna(benchmark_drawdown.loc[date])
                else float("nan"),
                f"benchmark_return_{int(bubble_lookback_days)}d": float(benchmark_trailing_return.loc[date])
                if pd.notna(benchmark_trailing_return.loc[date])
                else float("nan"),
                "financial_drawdown_252d": float(financial_drawdown.loc[date])
                if pd.notna(financial_drawdown.loc[date])
                else float("nan"),
                f"financial_relative_return_{int(financial_relative_lookback_days)}d": float(
                    financial_relative_return.loc[date]
                )
                if pd.notna(financial_relative_return.loc[date])
                else float("nan"),
                "proposer_verdict": proposer_verdict,
                "auditor_verdict": auditor_verdict,
                "crisis_type": crisis_type,
                "final_context_allowed": final_allowed,
                "confidence": confidence,
                "reason": reason,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_context_gate(
    close: pd.DataFrame,
    *,
    context_gate: str = CONTEXT_GATE_NONE,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    bubble_lookback_days: int = DEFAULT_BUBBLE_LOOKBACK_DAYS,
    bubble_persistence_days: int = DEFAULT_BUBBLE_PERSISTENCE_DAYS,
    bubble_return_threshold: float = DEFAULT_BUBBLE_RETURN_THRESHOLD,
    financial_symbol: str = DEFAULT_FINANCIAL_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    financial_drawdown_threshold: float = DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    financial_relative_lookback_days: int = DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    financial_relative_return_threshold: float = DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
) -> pd.Series:
    frame = _normalize_close(close)
    gate_name = str(context_gate).strip().lower()
    if gate_name not in CONTEXT_GATES:
        raise ValueError(f"Unsupported context gate: {context_gate}")
    index = _window_index(frame, start_date=start_date, end_date=end_date)
    if gate_name == CONTEXT_GATE_NONE:
        return pd.Series(True, index=index, name="no_context_gate")

    bubble = build_bubble_context_gate(
        frame,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        lookback_days=bubble_lookback_days,
        persistence_days=bubble_persistence_days,
        return_threshold=bubble_return_threshold,
    )
    if gate_name == CONTEXT_GATE_BUBBLE:
        return bubble.rename(f"{gate_name}_context_active")

    financial = build_financial_context_gate(
        frame,
        start_date=start_date,
        end_date=end_date,
        financial_symbol=financial_symbol,
        market_symbol=market_symbol,
        drawdown_threshold=financial_drawdown_threshold,
        relative_lookback_days=financial_relative_lookback_days,
        relative_return_threshold=financial_relative_return_threshold,
    )
    if gate_name == CONTEXT_GATE_FINANCIAL:
        return financial.rename(f"{gate_name}_context_active")
    return (bubble | financial).rename(f"{gate_name}_context_active")


def apply_context_gate_to_signal(price_signal: pd.Series, context_signal: pd.Series) -> pd.Series:
    """Require context on entry, then keep protection while the price signal remains active."""
    price = pd.Series(price_signal).fillna(False).astype(bool).copy()
    price.index = pd.to_datetime(price.index).tz_localize(None).normalize()
    context = pd.Series(context_signal).fillna(False).astype(bool).copy()
    context.index = pd.to_datetime(context.index).tz_localize(None).normalize()
    context = context.reindex(price.index).ffill().fillna(False)

    active = False
    values: list[bool] = []
    for date, price_active in price.items():
        if not bool(price_active):
            active = False
        elif not active and bool(context.loc[date]):
            active = True
        values.append(active)
    return pd.Series(values, index=price.index, name=price.name)


def build_guard_transition_events(signal: pd.Series, *, strategy_name: str) -> pd.DataFrame:
    active = pd.Series(signal).fillna(False).astype(bool).copy()
    active.index = pd.to_datetime(active.index).tz_localize(None).normalize()
    rows: list[dict[str, object]] = []
    previous = False
    for date, value in active.items():
        current = bool(value)
        if current == previous:
            continue
        rows.append(
            {
                "strategy": strategy_name,
                "signal_date": pd.Timestamp(date).date().isoformat(),
                "reason": "crisis_guard_on" if current else "crisis_guard_off",
                "old_exposure": 1.0 if previous else 0.0,
                "new_exposure": 1.0 if current else 0.0,
            }
        )
        previous = current
    return pd.DataFrame(rows)


def build_period_summary(
    returns_by_strategy: Mapping[str, pd.Series],
    *,
    trades_by_strategy: Mapping[str, pd.DataFrame] | None = None,
    periods: Sequence[tuple[str, str, str | None]] = CRISIS_COMPARISON_PERIODS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    global_end = max(
        pd.Timestamp(series.index.max()).normalize()
        for series in returns_by_strategy.values()
        if not series.empty
    )
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        for strategy_name, returns in returns_by_strategy.items():
            window_returns = returns.loc[(returns.index >= start) & (returns.index <= end)]
            if len(window_returns.dropna()) < 2:
                continue
            trades = (trades_by_strategy or {}).get(strategy_name)
            if trades is not None and not trades.empty and "signal_date" in trades.columns:
                trade_dates = pd.to_datetime(trades["signal_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
                trades = trades.loc[trade_dates.between(start, end)].copy()
            row = summarize_returns(window_returns, strategy_name=strategy_name, trades=trades)
            row["Period"] = period_name
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=list(SUMMARY_COLUMNS))
    return pd.DataFrame(rows).loc[:, list(SUMMARY_COLUMNS)]


def build_deltas_vs_base(summary: pd.DataFrame, *, base_strategy: str = "base") -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for period, group in summary.groupby("Period", sort=False):
        indexed = group.set_index("Strategy")
        if base_strategy not in indexed.index:
            continue
        base = indexed.loc[base_strategy]
        for strategy, row in indexed.iterrows():
            if strategy == base_strategy:
                continue
            rows.append(
                {
                    "Period": period,
                    "Strategy": strategy,
                    "Delta Total Return": float(row["Total Return"] - base["Total Return"]),
                    "Delta CAGR": float(row["CAGR"] - base["CAGR"]),
                    "Delta Max Drawdown": float(row["Max Drawdown"] - base["Max Drawdown"]),
                    "Delta Sharpe": float(row["Sharpe"] - base["Sharpe"]),
                }
            )
    return pd.DataFrame(rows)


def build_guard_diagnostics(
    signals_by_strategy: Mapping[str, pd.Series],
    *,
    periods: Sequence[tuple[str, str, str | None]] = CRISIS_COMPARISON_PERIODS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if not signals_by_strategy:
        return pd.DataFrame(columns=["Period", "Strategy", "Active Days", "Trading Days", "Active Ratio"])
    global_end = max(pd.Timestamp(signal.index.max()).normalize() for signal in signals_by_strategy.values() if not signal.empty)
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        for strategy_name, signal in signals_by_strategy.items():
            active = pd.Series(signal).fillna(False).astype(bool).copy()
            active.index = pd.to_datetime(active.index).tz_localize(None).normalize()
            window = active.loc[(active.index >= start) & (active.index <= end)]
            rows.append(
                {
                    "Period": period_name,
                    "Strategy": strategy_name,
                    "Active Days": int(window.sum()),
                    "Trading Days": int(len(window)),
                    "Active Ratio": float(window.mean()) if len(window) else float("nan"),
                }
            )
    return pd.DataFrame(rows)


def run_crisis_guard_research(
    price_history,
    *,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    safe_symbol: str = DEFAULT_SAFE_SYMBOL_FOR_CRISIS,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    synthetic_attack_from: str | None = DEFAULT_BENCHMARK_SYMBOL,
    synthetic_attack_multiple: float = DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
    synthetic_attack_expense_rate: float = DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    drawdown_thresholds: Sequence[float] = DEFAULT_DRAWDOWN_THRESHOLDS,
    risk_multipliers: Sequence[float] = DEFAULT_RISK_MULTIPLIERS,
    confirm_days: int = 1,
    context_gates: Sequence[str] = DEFAULT_CONTEXT_GATES,
    bubble_lookback_days: int = DEFAULT_BUBBLE_LOOKBACK_DAYS,
    bubble_persistence_days: int = DEFAULT_BUBBLE_PERSISTENCE_DAYS,
    bubble_return_threshold: float = DEFAULT_BUBBLE_RETURN_THRESHOLD,
    financial_symbol: str = DEFAULT_FINANCIAL_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    financial_drawdown_threshold: float = DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    financial_relative_lookback_days: int = DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    financial_relative_return_threshold: float = DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
    ma_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    ma_slope_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    close = price_history_to_close_matrix(price_history)
    close = _normalize_close(close)
    if float(synthetic_attack_multiple) > 0.0:
        close = add_synthetic_attack_close(
            close,
            attack_symbol=attack_symbol,
            source_symbol=synthetic_attack_from or benchmark_symbol,
            multiple=float(synthetic_attack_multiple),
            annual_expense_rate=float(synthetic_attack_expense_rate),
        )
    if end_date is not None:
        close = close.loc[close.index <= pd.Timestamp(end_date).normalize()].copy()
    index = close.index[close.index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough trading days for crisis guard research")

    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    financial_symbol = str(financial_symbol).strip().upper()
    market_symbol = str(market_symbol).strip().upper()

    returns = close.pct_change(fill_method=None).fillna(0.0).reindex(index).fillna(0.0)
    for symbol in {benchmark_symbol, attack_symbol, safe_symbol}:
        if symbol not in returns.columns:
            returns[symbol] = 0.0

    base_weights = build_tqqq_growth_income_base_weights(
        close,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
    ).reindex(index[:-1]).ffill().fillna(0.0)
    base_returns, base_weights_history = _weights_to_returns(
        returns,
        base_weights,
        strategy_name="base",
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    returns_by_strategy: dict[str, pd.Series] = {"base": base_returns}
    weights_by_strategy: dict[str, pd.DataFrame] = {"base": base_weights_history}
    signals_by_strategy: dict[str, pd.Series] = {}
    trades_by_strategy: dict[str, pd.DataFrame] = {}
    ai_opinions_by_strategy: dict[str, pd.DataFrame] = {}

    specs = build_crisis_guard_specs(
        drawdown_thresholds=drawdown_thresholds,
        risk_multipliers=risk_multipliers,
        confirm_days=confirm_days,
        context_gates=context_gates,
    )
    context_signals_by_gate = {
        context_gate: build_context_gate(
            close,
            context_gate=context_gate,
            start_date=start_date,
            end_date=end_date,
            benchmark_symbol=benchmark_symbol,
            bubble_lookback_days=bubble_lookback_days,
            bubble_persistence_days=bubble_persistence_days,
            bubble_return_threshold=bubble_return_threshold,
            financial_symbol=financial_symbol,
            market_symbol=market_symbol,
            financial_drawdown_threshold=financial_drawdown_threshold,
            financial_relative_lookback_days=financial_relative_lookback_days,
            financial_relative_return_threshold=financial_relative_return_threshold,
        )
        for context_gate in tuple(dict.fromkeys(spec.context_gate for spec in specs))
    }
    for spec in specs:
        raw_signal = build_price_crisis_guard_signal(
            close,
            start_date=start_date,
            end_date=end_date,
            benchmark_symbol=benchmark_symbol,
            drawdown_threshold=spec.drawdown_threshold,
            ma_days=ma_days,
            ma_slope_days=ma_slope_days,
        )
        confirmed_price_signal = _apply_confirm_days(raw_signal, spec.confirm_days)
        if spec.context_gate == CONTEXT_GATE_AI_RUBRIC:
            ai_opinions = build_ai_crisis_opinions(
                close,
                confirmed_price_signal,
                strategy_name=spec.name,
                start_date=start_date,
                end_date=end_date,
                benchmark_symbol=benchmark_symbol,
                bubble_lookback_days=bubble_lookback_days,
                bubble_persistence_days=bubble_persistence_days,
                bubble_return_threshold=bubble_return_threshold,
                financial_symbol=financial_symbol,
                market_symbol=market_symbol,
                financial_drawdown_threshold=financial_drawdown_threshold,
                financial_relative_lookback_days=financial_relative_lookback_days,
                financial_relative_return_threshold=financial_relative_return_threshold,
            )
            ai_opinions_by_strategy[spec.name] = ai_opinions
            context_signal = pd.Series(False, index=confirmed_price_signal.index, name=f"{spec.name}_ai_context_allowed")
            if not ai_opinions.empty:
                opinion_dates = pd.to_datetime(ai_opinions["as_of"]).dt.normalize()
                context_signal.loc[opinion_dates] = ai_opinions["final_context_allowed"].to_numpy(dtype=bool)
        else:
            context_signal = context_signals_by_gate[spec.context_gate]
        signal = apply_context_gate_to_signal(confirmed_price_signal, context_signal)
        guarded_weights = apply_price_crisis_guard_to_weights(
            base_weights,
            signal,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            risk_multiplier=spec.risk_multiplier,
        ).reindex(index[:-1]).ffill().fillna(0.0)
        strategy_returns, strategy_weights = _weights_to_returns(
            returns,
            guarded_weights,
            strategy_name=spec.name,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_strategy[spec.name] = strategy_returns
        weights_by_strategy[spec.name] = strategy_weights
        signals_by_strategy[spec.name] = signal
        trades_by_strategy[spec.name] = build_guard_transition_events(signal, strategy_name=spec.name)

    summary = build_period_summary(returns_by_strategy, trades_by_strategy=trades_by_strategy)
    deltas = build_deltas_vs_base(summary)
    diagnostics = build_guard_diagnostics(signals_by_strategy)
    context_diagnostics = build_guard_diagnostics(
        {f"context_{gate}": signal for gate, signal in context_signals_by_gate.items()}
    )
    guard_events = (
        pd.concat(tuple(trades_by_strategy.values()), ignore_index=True)
        if trades_by_strategy
        else pd.DataFrame()
    )
    ai_opinions = (
        pd.concat(tuple(ai_opinions_by_strategy.values()), ignore_index=True)
        if ai_opinions_by_strategy
        else pd.DataFrame()
    )
    return {
        "summary": summary,
        "deltas_vs_base": deltas,
        "guard_diagnostics": diagnostics,
        "context_diagnostics": context_diagnostics,
        "guard_events": guard_events,
        "ai_opinions": ai_opinions,
        "returns_by_strategy": returns_by_strategy,
        "weights_by_strategy": weights_by_strategy,
        "signals_by_strategy": signals_by_strategy,
        "context_signals_by_gate": context_signals_by_gate,
    }


def _format_percent_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in output.columns:
        if column in {
            "Total Return",
            "CAGR",
            "Max Drawdown",
            "Volatility",
            "Calmar",
            "Delta Total Return",
            "Delta CAGR",
            "Delta Max Drawdown",
            "Active Ratio",
        }:
            output[column] = output[column].map(lambda value: f"{float(value):.2%}" if pd.notna(value) else "")
    for column in ("Sharpe", "Delta Sharpe", "Final Equity"):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2f}" if pd.notna(value) else "")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research price-only crisis guard variants for the TQQQ profile.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--attack-symbol", default=DEFAULT_ATTACK_SYMBOL)
    parser.add_argument("--safe-symbol", default=DEFAULT_SAFE_SYMBOL_FOR_CRISIS)
    parser.add_argument("--synthetic-attack-from", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--synthetic-attack-multiple", type=float, default=DEFAULT_SYNTHETIC_ATTACK_MULTIPLE)
    parser.add_argument("--synthetic-attack-expense-rate", type=float, default=DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE)
    parser.add_argument("--drawdown-thresholds", default=",".join(str(value) for value in DEFAULT_DRAWDOWN_THRESHOLDS))
    parser.add_argument("--risk-multipliers", default=",".join(str(value) for value in DEFAULT_RISK_MULTIPLIERS))
    parser.add_argument("--confirm-days", type=int, default=1)
    parser.add_argument("--context-gates", default=",".join(DEFAULT_CONTEXT_GATES))
    parser.add_argument("--bubble-lookback-days", type=int, default=DEFAULT_BUBBLE_LOOKBACK_DAYS)
    parser.add_argument("--bubble-persistence-days", type=int, default=DEFAULT_BUBBLE_PERSISTENCE_DAYS)
    parser.add_argument("--bubble-return-threshold", type=float, default=DEFAULT_BUBBLE_RETURN_THRESHOLD)
    parser.add_argument("--financial-symbol", default=DEFAULT_FINANCIAL_SYMBOL)
    parser.add_argument("--market-symbol", default=DEFAULT_MARKET_SYMBOL)
    parser.add_argument("--financial-drawdown-threshold", type=float, default=DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD)
    parser.add_argument("--financial-relative-lookback-days", type=int, default=DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS)
    parser.add_argument(
        "--financial-relative-return-threshold",
        type=float,
        default=DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
    )
    parser.add_argument("--ma-days", type=int, default=DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS)
    parser.add_argument("--ma-slope-days", type=int, default=DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.download:
        context_gates = _parse_str_tuple(args.context_gates)
        symbols = [args.benchmark_symbol, args.safe_symbol]
        if float(args.synthetic_attack_multiple) > 0.0:
            symbols.append(args.synthetic_attack_from)
        else:
            symbols.append(args.attack_symbol)
        if any(gate in {CONTEXT_GATE_FINANCIAL, CONTEXT_GATE_BUBBLE_OR_FINANCIAL} for gate in context_gates):
            symbols.extend([args.financial_symbol, args.market_symbol])
        symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        price_history = download_price_history(symbols, start=args.price_start, end=args.price_end)
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "crisis_regime_guard_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    result = run_crisis_guard_research(
        price_history,
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        attack_symbol=args.attack_symbol,
        safe_symbol=args.safe_symbol,
        synthetic_attack_from=args.synthetic_attack_from,
        synthetic_attack_multiple=float(args.synthetic_attack_multiple),
        synthetic_attack_expense_rate=float(args.synthetic_attack_expense_rate),
        drawdown_thresholds=_parse_float_tuple(args.drawdown_thresholds),
        risk_multipliers=_parse_float_tuple(args.risk_multipliers),
        confirm_days=int(args.confirm_days),
        context_gates=_parse_str_tuple(args.context_gates),
        bubble_lookback_days=int(args.bubble_lookback_days),
        bubble_persistence_days=int(args.bubble_persistence_days),
        bubble_return_threshold=float(args.bubble_return_threshold),
        financial_symbol=args.financial_symbol,
        market_symbol=args.market_symbol,
        financial_drawdown_threshold=float(args.financial_drawdown_threshold),
        financial_relative_lookback_days=int(args.financial_relative_lookback_days),
        financial_relative_return_threshold=float(args.financial_relative_return_threshold),
        ma_days=int(args.ma_days),
        ma_slope_days=int(args.ma_slope_days),
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    summary = result["summary"]
    deltas = result["deltas_vs_base"]
    diagnostics = result["guard_diagnostics"]
    context_diagnostics = result["context_diagnostics"]
    ai_opinions = result["ai_opinions"]
    print("\nSummary:")
    print(_format_percent_columns(summary).to_string(index=False))
    print("\nDeltas vs base:")
    print(_format_percent_columns(deltas).to_string(index=False))
    print("\nGuard diagnostics:")
    print(_format_percent_columns(diagnostics).to_string(index=False))
    print("\nContext diagnostics:")
    print(_format_percent_columns(context_diagnostics).to_string(index=False))
    if isinstance(ai_opinions, pd.DataFrame) and not ai_opinions.empty:
        ai_diagnostics = (
            ai_opinions.groupby(
                ["strategy", "crisis_type", "proposer_verdict", "auditor_verdict", "final_context_allowed"],
                dropna=False,
            )
            .size()
            .reset_index(name="days")
        )
        print("\nAI opinion diagnostics:")
        print(ai_diagnostics.to_string(index=False))

    summary.to_csv(output_dir / "summary.csv", index=False)
    deltas.to_csv(output_dir / "deltas_vs_base.csv", index=False)
    diagnostics.to_csv(output_dir / "guard_diagnostics.csv", index=False)
    context_diagnostics.to_csv(output_dir / "context_diagnostics.csv", index=False)
    result["guard_events"].to_csv(output_dir / "guard_events.csv", index=False)
    if isinstance(ai_opinions, pd.DataFrame) and not ai_opinions.empty:
        ai_opinions.to_csv(output_dir / "ai_opinions.csv", index=False)
    returns_dir = output_dir / "returns"
    weights_dir = output_dir / "weights"
    signals_dir = output_dir / "signals"
    returns_dir.mkdir(exist_ok=True)
    weights_dir.mkdir(exist_ok=True)
    signals_dir.mkdir(exist_ok=True)
    for strategy, returns in result["returns_by_strategy"].items():
        returns.rename("return").to_csv(returns_dir / f"{strategy}.csv")
    for strategy, weights in result["weights_by_strategy"].items():
        weights.to_csv(weights_dir / f"{strategy}.csv")
    for strategy, signal in result["signals_by_strategy"].items():
        signal.rename("active").to_csv(signals_dir / f"{strategy}.csv")
    context_dir = output_dir / "context"
    context_dir.mkdir(exist_ok=True)
    for gate, signal in result["context_signals_by_gate"].items():
        signal.rename("active").to_csv(context_dir / f"{gate}.csv")
    print(f"wrote crisis guard research outputs -> {output_dir}")
    return 0


__all__ = [
    "CRISIS_COMPARISON_PERIODS",
    "CONTEXT_GATE_AI_RUBRIC",
    "CONTEXT_GATE_BUBBLE",
    "CONTEXT_GATE_BUBBLE_OR_FINANCIAL",
    "CONTEXT_GATE_FINANCIAL",
    "CONTEXT_GATE_NONE",
    "CrisisGuardSpec",
    "DEFAULT_ATTACK_SYMBOL",
    "DEFAULT_BUBBLE_PERSISTENCE_DAYS",
    "DEFAULT_CONTEXT_GATES",
    "DEFAULT_DRAWDOWN_THRESHOLDS",
    "DEFAULT_RISK_MULTIPLIERS",
    "apply_context_gate_to_signal",
    "build_ai_crisis_opinions",
    "build_bubble_context_gate",
    "build_context_gate",
    "build_crisis_guard_specs",
    "build_financial_context_gate",
    "build_guard_diagnostics",
    "build_guard_transition_events",
    "build_period_summary",
    "main",
    "run_crisis_guard_research",
]
