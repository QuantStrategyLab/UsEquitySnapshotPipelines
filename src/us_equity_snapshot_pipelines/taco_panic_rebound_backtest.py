from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    EVENT_KIND_SHOCK,
    EVENT_KIND_SOFTENING,
    TRADE_WAR_EVENT_SETS,
    TRADE_WAR_EVENTS_2018_TO_PRESENT,
    TradeWarEvent,
    events_to_frame,
    price_history_to_close_matrix,
    resolve_trade_war_event_set,
)
from .yfinance_prices import download_price_history

BENCHMARK_SYMBOL = "QQQ"
BROAD_BENCHMARK_SYMBOL = "SPY"
CASH_SYMBOL = "CASH"
DEFAULT_PRESET = "steady"
DEFAULT_ACCOUNT_SLEEVE_RATIO = 0.10
DEFAULT_WATCH_DAYS = 10
DEFAULT_MAX_HOLD_DAYS = 63
DEFAULT_TAKE_PROFIT_REBOUND = 0.10
DEFAULT_RUNNER_EXPOSURE = 0.35
DEFAULT_STOP_LOSS_FROM_ENTRY = -0.08
DEFAULT_TRAILING_STOP = 0.10
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_SOFTENING_EXPOSURE = 0.60
PRESIDENTIAL_PERIODS: tuple[tuple[str, str, str | None], ...] = (
    ("trump_1", "2018-01-01", "2021-01-19"),
    ("biden", "2021-01-20", "2025-01-19"),
    ("trump_2_to_date", "2025-01-20", None),
)
DEFAULT_SHOCK_DRAWDOWN_TIERS: tuple[tuple[float, float], ...] = (
    (-0.025, 0.25),
    (-0.040, 0.40),
    (-0.060, 0.55),
    (-0.080, 0.70),
)
DEFAULT_HIGH_DRAWDOWN_TIERS: tuple[tuple[float, float], ...] = (
    (-0.050, 0.25),
    (-0.080, 0.40),
    (-0.120, 0.55),
    (-0.160, 0.70),
)
BASKET_PRESETS: dict[str, dict[str, float]] = {
    "steady": {
        "QLD": 0.40,
        "TQQQ": 0.25,
        "ROM": 0.15,
        "USD": 0.10,
        "NVDA": 0.05,
        "AMD": 0.05,
    },
    "aggressive": {
        "TQQQ": 0.40,
        "TECL": 0.25,
        "SOXL": 0.20,
        "NVDA": 0.10,
        "AMD": 0.05,
    },
}
SUMMARY_COLUMNS = (
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
    "Avg Exposure",
    "Max Exposure",
    "Turnover/Year",
    "Final Equity",
)


@dataclass(frozen=True)
class OpenShock:
    event: TradeWarEvent
    signal_date: pd.Timestamp
    watch_end_date: pd.Timestamp
    baseline_close: float


def _split_weights(raw: str | Mapping[str, float] | None) -> dict[str, float] | None:
    if raw is None or isinstance(raw, Mapping):
        return dict(raw) if isinstance(raw, Mapping) else None
    weights: dict[str, float] = {}
    for item in str(raw or "").split(","):
        if not item.strip():
            continue
        if ":" not in item:
            raise ValueError("basket weights must use SYMBOL:WEIGHT comma-separated syntax")
        symbol, value = item.split(":", 1)
        symbol_text = symbol.strip().upper()
        weight = float(value)
        if symbol_text and weight > 0:
            weights[symbol_text] = weight
    return weights or None


def normalize_basket(
    preset: str = DEFAULT_PRESET,
    *,
    basket_weights: Mapping[str, float] | None = None,
) -> dict[str, float]:
    raw = dict(basket_weights or BASKET_PRESETS.get(str(preset).strip().lower(), {}))
    if not raw:
        raise ValueError(f"Unsupported basket preset: {preset!r}")
    cleaned = {str(symbol).strip().upper(): float(weight) for symbol, weight in raw.items() if float(weight) > 0}
    total = sum(cleaned.values())
    if total <= 0:
        raise ValueError("basket weights must sum to a positive value")
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def collect_required_symbols(
    *,
    preset: str = DEFAULT_PRESET,
    basket_weights: Mapping[str, float] | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
) -> tuple[str, ...]:
    basket = normalize_basket(preset, basket_weights=basket_weights)
    symbols = [benchmark_symbol, broad_benchmark_symbol, *basket.keys()]
    return tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))


def _next_index_date(index: pd.DatetimeIndex, raw_date: str | pd.Timestamp) -> pd.Timestamp | None:
    date = pd.Timestamp(raw_date).tz_localize(None).normalize()
    candidates = index[index >= date]
    if candidates.empty:
        return None
    return pd.Timestamp(candidates[0]).normalize()


def _date_after_trading_days(index: pd.DatetimeIndex, start_date: pd.Timestamp, trading_days: int) -> pd.Timestamp:
    if start_date not in index:
        maybe = _next_index_date(index, start_date)
        if maybe is None:
            return pd.Timestamp(index[-1]).normalize()
        start_date = maybe
    pos = index.get_loc(start_date)
    if not isinstance(pos, int):
        pos = int(pd.Series(range(len(index)), index=index).loc[start_date].max())
    return pd.Timestamp(index[min(len(index) - 1, pos + max(0, int(trading_days)))]).normalize()


def _drawdown_tier_exposure(drawdown: float, tiers: Sequence[tuple[float, float]]) -> float:
    exposure = 0.0
    for threshold, target in tiers:
        if pd.notna(drawdown) and float(drawdown) <= float(threshold):
            exposure = max(exposure, float(target))
    return exposure


def _shock_target_exposure(
    *,
    current_close: float,
    shock_baseline_close: float,
    high_63_close: float | None,
    shock_drawdown_tiers: Sequence[tuple[float, float]],
    high_drawdown_tiers: Sequence[tuple[float, float]],
) -> tuple[float, float, float]:
    shock_drawdown = current_close / shock_baseline_close - 1.0 if shock_baseline_close > 0 else float("nan")
    high_drawdown = current_close / high_63_close - 1.0 if high_63_close and high_63_close > 0 else float("nan")
    target = max(
        _drawdown_tier_exposure(shock_drawdown, shock_drawdown_tiers),
        _drawdown_tier_exposure(high_drawdown, high_drawdown_tiers),
    )
    return target, shock_drawdown, high_drawdown


def _target_weights_for_exposure(
    basket: Mapping[str, float],
    exposure: float,
    available_symbols: set[str],
    *,
    cash_symbol: str = CASH_SYMBOL,
) -> dict[str, float]:
    exposure = max(0.0, min(1.0, float(exposure)))
    available_basket = {symbol: weight for symbol, weight in basket.items() if symbol in available_symbols}
    total = sum(available_basket.values())
    if total <= 0 or exposure <= 0:
        return {cash_symbol: 1.0}
    weights = {symbol: exposure * weight / total for symbol, weight in available_basket.items()}
    weights[cash_symbol] = max(0.0, 1.0 - sum(weights.values()))
    return {symbol: weight for symbol, weight in weights.items() if weight > 1e-12}


def _compute_turnover(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    symbols = set(current) | set(target)
    return 0.5 * sum(abs(float(target.get(symbol, 0.0)) - float(current.get(symbol, 0.0))) for symbol in symbols)


def summarize_returns(
    returns: pd.Series,
    *,
    strategy_name: str,
    weights_history: pd.DataFrame | None = None,
    exposure_history: pd.DataFrame | None = None,
    trades: pd.DataFrame | None = None,
) -> dict[str, object]:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if clean.empty:
        raise RuntimeError("No returns to summarize")
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(clean.std(ddof=0) * math.sqrt(252))
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    turnover_per_year = float("nan")
    if weights_history is not None and not weights_history.empty:
        changes = weights_history.fillna(0.0).diff().fillna(0.0)
        if not changes.empty:
            changes.iloc[0] = 0.0
        turnover_per_year = float((0.5 * changes.abs().sum(axis=1)).sum() / years)

    avg_exposure = float("nan")
    max_exposure = float("nan")
    if exposure_history is not None and not exposure_history.empty:
        exposure = pd.to_numeric(exposure_history["target_exposure"], errors="coerce")
        avg_exposure = float(exposure.mean()) if exposure.notna().any() else float("nan")
        max_exposure = float(exposure.max()) if exposure.notna().any() else float("nan")

    return {
        "Strategy": strategy_name,
        "Start": str(clean.index[0].date()),
        "End": str(clean.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Trades": int(len(trades)) if trades is not None else 0,
        "Avg Exposure": avg_exposure,
        "Max Exposure": max_exposure,
        "Turnover/Year": turnover_per_year,
        "Final Equity": float(equity.iloc[-1]),
    }


def _filter_exposure_history(
    exposure_history: pd.DataFrame | None,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    if exposure_history is None or exposure_history.empty or "as_of" not in exposure_history.columns:
        return None
    frame = exposure_history.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"])
    return frame.loc[frame["as_of"].between(start, end)].copy()


def _filter_trades(
    trades: pd.DataFrame | None,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    if trades is None or trades.empty or "signal_date" not in trades.columns:
        return trades
    frame = trades.copy()
    frame["signal_date_ts"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["signal_date_ts"])
    return frame.loc[frame["signal_date_ts"].between(start, end)].drop(columns=["signal_date_ts"]).copy()


def build_period_summary(
    *,
    portfolio_returns: pd.Series,
    account_overlay_returns: pd.Series,
    reference_returns: pd.DataFrame,
    preset: str,
    account_sleeve_ratio: float,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    exposure_history: pd.DataFrame | None = None,
    trades: pd.DataFrame | None = None,
    periods: Sequence[tuple[str, str, str | None]] = PRESIDENTIAL_PERIODS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    strategies: list[tuple[str, pd.Series, pd.DataFrame | None, pd.DataFrame | None]] = [
        (f"taco_panic_rebound_{preset}", portfolio_returns, exposure_history, trades),
        (
            f"taco_panic_rebound_{preset}_{float(account_sleeve_ratio):.0%}_account_overlay_cash",
            account_overlay_returns,
            None,
            trades,
        ),
        (benchmark_symbol, reference_returns[benchmark_symbol], None, None),
        (broad_benchmark_symbol, reference_returns[broad_benchmark_symbol], None, None),
        (f"{preset}_basket_buy_hold", reference_returns["basket_buy_hold"], None, None),
    ]

    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        global_end = max(
            pd.Timestamp(series.index.max()).normalize()
            for _strategy_name, series, _exposure, _trades in strategies
            if not series.empty
        )
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        for strategy_name, returns, strategy_exposure, strategy_trades in strategies:
            clean = returns.loc[(returns.index >= start) & (returns.index <= end)].copy()
            if len(clean.dropna()) < 2:
                continue
            period_exposure = _filter_exposure_history(strategy_exposure, start=start, end=end)
            period_trades = _filter_trades(strategy_trades, start=start, end=end)
            row = summarize_returns(
                clean,
                strategy_name=strategy_name,
                exposure_history=period_exposure,
                trades=period_trades,
            )
            row["Period"] = period_name
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["Period", *SUMMARY_COLUMNS])
    return pd.DataFrame(rows).loc[:, ["Period", *SUMMARY_COLUMNS]]


def run_backtest(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    preset: str = DEFAULT_PRESET,
    basket_weights: Mapping[str, float] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    cash_symbol: str = CASH_SYMBOL,
    watch_days: int = DEFAULT_WATCH_DAYS,
    max_hold_days: int = DEFAULT_MAX_HOLD_DAYS,
    take_profit_rebound: float = DEFAULT_TAKE_PROFIT_REBOUND,
    runner_exposure: float = DEFAULT_RUNNER_EXPOSURE,
    stop_loss_from_entry: float = DEFAULT_STOP_LOSS_FROM_ENTRY,
    trailing_stop: float = DEFAULT_TRAILING_STOP,
    softening_exposure: float = DEFAULT_SOFTENING_EXPOSURE,
    shock_drawdown_tiers: Sequence[tuple[float, float]] = DEFAULT_SHOCK_DRAWDOWN_TIERS,
    high_drawdown_tiers: Sequence[tuple[float, float]] = DEFAULT_HIGH_DRAWDOWN_TIERS,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    account_sleeve_ratio: float = DEFAULT_ACCOUNT_SLEEVE_RATIO,
) -> dict[str, pd.DataFrame | pd.Series]:
    close = price_history_to_close_matrix(price_history)
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    close = close.sort_index()
    if start_date is not None:
        close = close.loc[close.index >= pd.Timestamp(start_date).normalize()].copy()
    if end_date is not None:
        close = close.loc[close.index <= pd.Timestamp(end_date).normalize()].copy()
    if len(close.index) < 2:
        raise RuntimeError("Not enough price history for backtest")

    benchmark_symbol = str(benchmark_symbol).strip().upper()
    broad_benchmark_symbol = str(broad_benchmark_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or CASH_SYMBOL
    if benchmark_symbol not in close.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")
    if broad_benchmark_symbol not in close.columns:
        close[broad_benchmark_symbol] = close[benchmark_symbol]
    basket = normalize_basket(preset, basket_weights=basket_weights)
    available_symbols = {symbol for symbol in close.columns if symbol in basket}
    if not available_symbols:
        raise RuntimeError("None of the basket symbols are available in price history")

    returns = close.pct_change().fillna(0.0)
    index = close.index
    high_63 = close[benchmark_symbol].rolling(63, min_periods=20).max()
    ma20 = close[benchmark_symbol].rolling(20, min_periods=5).mean()

    shock_events: list[OpenShock] = []
    softening_dates: dict[pd.Timestamp, TradeWarEvent] = {}
    for event in events:
        signal_date = _next_index_date(index, event.event_date)
        if signal_date is None:
            continue
        if event.kind == EVENT_KIND_SHOCK:
            baseline = float(close.at[signal_date, benchmark_symbol])
            shock_events.append(
                OpenShock(
                    event=event,
                    signal_date=signal_date,
                    watch_end_date=_date_after_trading_days(index, signal_date, int(watch_days)),
                    baseline_close=baseline,
                )
            )
        elif event.kind == EVENT_KIND_SOFTENING:
            softening_dates[signal_date] = event

    current_weights: dict[str, float] = {cash_symbol: 1.0}
    current_target_exposure = 0.0
    current_event_id = ""
    in_position = False
    trimmed = False
    entry_date: pd.Timestamp | None = None
    entry_benchmark_close = float("nan")
    entry_low = float("inf")
    peak_after_entry = float("nan")

    portfolio_returns = pd.Series(0.0, index=index, name="portfolio_return")
    account_overlay_returns = pd.Series(0.0, index=index, name="account_overlay_return")
    weights_rows: list[dict[str, object]] = []
    exposure_rows: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []

    def set_target(
        date: pd.Timestamp,
        next_date: pd.Timestamp,
        target_exposure: float,
        reason: str,
        event_id: str,
    ) -> None:
        nonlocal current_weights, current_target_exposure, current_event_id, in_position, entry_date
        nonlocal entry_benchmark_close, entry_low, peak_after_entry, trimmed
        target = _target_weights_for_exposure(basket, target_exposure, available_symbols, cash_symbol=cash_symbol)
        turnover = _compute_turnover(current_weights, target)
        if turnover <= 1e-12 and reason not in {"hold"}:
            return
        old_exposure = current_target_exposure
        current_weights = target
        current_target_exposure = max(0.0, min(1.0, float(target_exposure)))
        current_event_id = event_id or current_event_id
        if current_target_exposure > 0 and not in_position:
            in_position = True
            entry_date = date
            entry_benchmark_close = float(close.at[date, benchmark_symbol])
            entry_low = entry_benchmark_close
            peak_after_entry = entry_benchmark_close
            trimmed = False
        if current_target_exposure <= 0:
            in_position = False
            entry_date = None
            entry_benchmark_close = float("nan")
            entry_low = float("inf")
            peak_after_entry = float("nan")
            trimmed = False
            current_event_id = ""
        trades.append(
            {
                "signal_date": date.date().isoformat(),
                "effective_date": next_date.date().isoformat(),
                "reason": reason,
                "event_id": event_id,
                "old_exposure": old_exposure,
                "new_exposure": current_target_exposure,
                "turnover": turnover,
                "symbols": ",".join(symbol for symbol in current_weights if symbol != cash_symbol),
            }
        )

    for idx in range(len(index) - 1):
        date = pd.Timestamp(index[idx]).normalize()
        next_date = pd.Timestamp(index[idx + 1]).normalize()
        benchmark_close = float(close.at[date, benchmark_symbol])
        benchmark_high_63 = float(high_63.at[date]) if pd.notna(high_63.at[date]) else None

        if in_position:
            entry_low = min(entry_low, benchmark_close)
            peak_after_entry = benchmark_close if pd.isna(peak_after_entry) else max(peak_after_entry, benchmark_close)
            rebound_from_low = benchmark_close / entry_low - 1.0 if entry_low > 0 else 0.0
            return_from_entry = benchmark_close / entry_benchmark_close - 1.0 if entry_benchmark_close > 0 else 0.0
            hold_days = (date - entry_date).days if entry_date is not None else 0
            ma20_value = float(ma20.at[date]) if pd.notna(ma20.at[date]) else float("nan")
            trailing_drawdown = benchmark_close / peak_after_entry - 1.0 if peak_after_entry > 0 else 0.0

            if return_from_entry <= float(stop_loss_from_entry):
                set_target(date, next_date, 0.0, "stop_loss_from_entry", current_event_id)
            elif trimmed and trailing_drawdown <= -abs(float(trailing_stop)):
                set_target(date, next_date, 0.0, "runner_trailing_stop", current_event_id)
            elif hold_days >= int(max_hold_days):
                set_target(date, next_date, 0.0, "max_hold_exit", current_event_id)
            elif (
                not trimmed
                and rebound_from_low >= float(take_profit_rebound)
                and (pd.isna(ma20_value) or benchmark_close >= ma20_value)
            ):
                set_target(
                    date,
                    next_date,
                    min(current_target_exposure, float(runner_exposure)),
                    "take_profit_trim",
                    current_event_id,
                )
                trimmed = True

        best_target = current_target_exposure
        best_reason = ""
        best_event_id = current_event_id
        for shock in shock_events:
            if shock.signal_date <= date <= shock.watch_end_date:
                target, _shock_drawdown, _high_drawdown = _shock_target_exposure(
                    current_close=benchmark_close,
                    shock_baseline_close=shock.baseline_close,
                    high_63_close=benchmark_high_63,
                    shock_drawdown_tiers=shock_drawdown_tiers,
                    high_drawdown_tiers=high_drawdown_tiers,
                )
                if target > best_target:
                    best_target = target
                    best_reason = "shock_ladder_add" if in_position else "shock_ladder_entry"
                    best_event_id = shock.event.event_id
        softening_event = softening_dates.get(date)
        if softening_event is not None and float(softening_exposure) > best_target:
            best_target = float(softening_exposure)
            best_reason = "softening_event_add" if in_position else "softening_event_entry"
            best_event_id = softening_event.event_id
        if best_reason:
            set_target(date, next_date, best_target, best_reason, best_event_id)

        weights_rows.append({"as_of": date.date().isoformat(), **current_weights})
        exposure_rows.append(
            {
                "as_of": date.date().isoformat(),
                "event_id": current_event_id,
                "target_exposure": current_target_exposure,
                "benchmark_close": benchmark_close,
                "benchmark_high_63": benchmark_high_63,
                "in_position": in_position,
                "trimmed": trimmed,
            }
        )

        next_returns = returns.loc[next_date]
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol == cash_symbol:
                continue
            gross_return += float(weight) * float(next_returns.get(symbol, 0.0))
        trade_turnover = (
            float(trades[-1]["turnover"])
            if trades and trades[-1]["effective_date"] == next_date.date().isoformat()
            else 0.0
        )
        cost = trade_turnover * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost
        account_overlay_returns.at[next_date] = portfolio_returns.at[next_date] * float(account_sleeve_ratio)

    weights_history = pd.DataFrame(weights_rows).fillna(0.0)
    if not weights_history.empty:
        weights_history["as_of"] = pd.to_datetime(weights_history["as_of"])
        weights_history = weights_history.set_index("as_of").sort_index()
    exposure_history = pd.DataFrame(exposure_rows)
    trades_frame = pd.DataFrame(trades)

    basket_returns = pd.Series(0.0, index=index, name="basket_buy_hold_return")
    basket_for_returns = {symbol: weight for symbol, weight in basket.items() if symbol in returns.columns}
    total_basket_weight = sum(basket_for_returns.values())
    if total_basket_weight > 0:
        for symbol, weight in basket_for_returns.items():
            basket_returns += returns[symbol].reindex(index).fillna(0.0) * (weight / total_basket_weight)
    reference_returns = pd.DataFrame(
        {
            benchmark_symbol: returns[benchmark_symbol].reindex(index).fillna(0.0),
            broad_benchmark_symbol: returns[broad_benchmark_symbol].reindex(index).fillna(0.0),
            "basket_buy_hold": basket_returns,
        },
        index=index,
    )

    summary_rows = [
        summarize_returns(
            portfolio_returns,
            strategy_name=f"taco_panic_rebound_{preset}",
            weights_history=weights_history,
            exposure_history=exposure_history,
            trades=trades_frame,
        ),
        summarize_returns(
            account_overlay_returns,
            strategy_name=f"taco_panic_rebound_{preset}_{float(account_sleeve_ratio):.0%}_account_overlay_cash",
            trades=trades_frame,
        ),
        summarize_returns(reference_returns[benchmark_symbol], strategy_name=benchmark_symbol),
        summarize_returns(reference_returns[broad_benchmark_symbol], strategy_name=broad_benchmark_symbol),
        summarize_returns(reference_returns["basket_buy_hold"], strategy_name=f"{preset}_basket_buy_hold"),
    ]
    summary = pd.DataFrame(summary_rows).loc[:, list(SUMMARY_COLUMNS)]
    period_summary = build_period_summary(
        portfolio_returns=portfolio_returns,
        account_overlay_returns=account_overlay_returns,
        reference_returns=reference_returns,
        preset=preset,
        account_sleeve_ratio=float(account_sleeve_ratio),
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        exposure_history=exposure_history,
        trades=trades_frame,
    )

    return {
        "portfolio_returns": portfolio_returns,
        "account_overlay_returns": account_overlay_returns,
        "weights_history": weights_history,
        "exposure_history": exposure_history,
        "trades": trades_frame,
        "reference_returns": reference_returns,
        "summary": summary,
        "period_summary": period_summary,
        "event_calendar": events_to_frame(events),
    }


def _format_percent_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in (
        "Total Return",
        "CAGR",
        "Max Drawdown",
        "Volatility",
        "Avg Exposure",
        "Max Exposure",
        "Turnover/Year",
    ):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2%}" if pd.notna(value) else "")
    for column in ("Sharpe", "Calmar", "Final Equity"):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2f}" if pd.notna(value) else "")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest a research-only TACO panic rebound portfolio sleeve.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--preset", choices=tuple(sorted(BASKET_PRESETS)), default=DEFAULT_PRESET)
    parser.add_argument("--basket-weights", help="Optional SYMBOL:WEIGHT comma-separated override")
    parser.add_argument("--price-start", default=DEFAULT_START_DATE)
    parser.add_argument("--price-end", default=DEFAULT_END_DATE)
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=DEFAULT_END_DATE)
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--watch-days", type=int, default=DEFAULT_WATCH_DAYS)
    parser.add_argument("--max-hold-days", type=int, default=DEFAULT_MAX_HOLD_DAYS)
    parser.add_argument("--take-profit-rebound", type=float, default=DEFAULT_TAKE_PROFIT_REBOUND)
    parser.add_argument("--runner-exposure", type=float, default=DEFAULT_RUNNER_EXPOSURE)
    parser.add_argument("--stop-loss-from-entry", type=float, default=DEFAULT_STOP_LOSS_FROM_ENTRY)
    parser.add_argument("--trailing-stop", type=float, default=DEFAULT_TRAILING_STOP)
    parser.add_argument("--softening-exposure", type=float, default=DEFAULT_SOFTENING_EXPOSURE)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--account-sleeve-ratio", type=float, default=DEFAULT_ACCOUNT_SLEEVE_RATIO)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    basket_weights = _split_weights(args.basket_weights)
    if args.download:
        symbols = collect_required_symbols(preset=args.preset, basket_weights=basket_weights)
        price_history = download_price_history(
            list(symbols),
            start=args.price_start,
            end=args.price_end,
        )
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "taco_panic_rebound_portfolio_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    result = run_backtest(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        preset=args.preset,
        basket_weights=basket_weights,
        start_date=args.start_date,
        end_date=args.end_date,
        watch_days=args.watch_days,
        max_hold_days=args.max_hold_days,
        take_profit_rebound=args.take_profit_rebound,
        runner_exposure=args.runner_exposure,
        stop_loss_from_entry=args.stop_loss_from_entry,
        trailing_stop=args.trailing_stop,
        softening_exposure=args.softening_exposure,
        turnover_cost_bps=args.turnover_cost_bps,
        account_sleeve_ratio=args.account_sleeve_ratio,
    )
    summary = result["summary"]
    period_summary = result["period_summary"]
    print(_format_percent_columns(summary).to_string(index=False))
    print("\nPresidential-period summary:")
    print(_format_percent_columns(period_summary).to_string(index=False))
    summary.to_csv(output_dir / "summary.csv", index=False)
    period_summary.to_csv(output_dir / "period_summary.csv", index=False)
    result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
    result["account_overlay_returns"].rename("account_overlay_return").to_csv(
        output_dir / "account_overlay_returns.csv"
    )
    result["weights_history"].to_csv(output_dir / "weights_history.csv")
    result["exposure_history"].to_csv(output_dir / "exposure_history.csv", index=False)
    result["trades"].to_csv(output_dir / "trades.csv", index=False)
    result["reference_returns"].to_csv(output_dir / "reference_returns.csv")
    result["event_calendar"].to_csv(output_dir / "event_calendar.csv", index=False)
    print(f"wrote taco panic rebound portfolio outputs -> {output_dir}")
    return 0


__all__ = [
    "BASKET_PRESETS",
    "DEFAULT_PRESET",
    "PRESIDENTIAL_PERIODS",
    "build_period_summary",
    "collect_required_symbols",
    "normalize_basket",
    "run_backtest",
    "summarize_returns",
    "main",
]
