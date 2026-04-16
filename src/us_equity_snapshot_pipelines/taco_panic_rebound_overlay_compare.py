from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_backtest import DEFAULT_TURNOVER_COST_BPS, run_backtest, summarize_returns
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
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

DEFAULT_START_DATE = "2015-01-01"
DEFAULT_PRICE_START_DATE = "2014-01-01"
DEFAULT_BENCHMARK_SYMBOL = "QQQ"
DEFAULT_ATTACK_SYMBOL = "TQQQ"
DEFAULT_SAFE_SYMBOL = "BOXX"
DEFAULT_CASH_SYMBOL = "CASH"
DEFAULT_OVERLAY_SLEEVE_RATIOS = (0.05, 0.10)
DEFAULT_TRIGGER_QQQ_1D_DROP = -0.015
DEFAULT_TRIGGER_QQQ_3D_DROP = -0.030
DEFAULT_TRIGGER_TQQQ_1D_DROP = -0.045
DEFAULT_TRIGGER_MA200_NEAR_PCT = 0.01
DEFAULT_TRIGGER_SCAN_HOLD_DAYS = 2
DEFAULT_SHOCK_LOOKAHEAD_DAYS = 3
DEFAULT_SOFTENING_LOOKAHEAD_DAYS = 1
DEFAULT_ACTIVE_EVENT_DAYS = 63
DEFAULT_DUAL_DRIVE_QQQ_WEIGHT = 0.45
DEFAULT_DUAL_DRIVE_TQQQ_WEIGHT = 0.45
DEFAULT_DUAL_DRIVE_SAFE_WEIGHT = 0.08
DEFAULT_DUAL_DRIVE_CASH_WEIGHT = 0.02
DEFAULT_IDLE_SAFE_WEIGHT = 0.98
DEFAULT_IDLE_CASH_WEIGHT = 0.02
DEFAULT_SYNTHETIC_ATTACK_MULTIPLE = 0.0
DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE = 0.01
DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN = -0.20
DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS = 200
DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS = 20
DEFAULT_PRICE_CRISIS_GUARD_RISK_MULTIPLIER = 0.0
AUDIT_MODE_CRISIS_VETO = "crisis_veto"
AUDIT_MODE_OFF = "off"
AUDIT_MODES = frozenset({AUDIT_MODE_OFF, AUDIT_MODE_CRISIS_VETO})
DEFAULT_AUDIT_MODES = (AUDIT_MODE_CRISIS_VETO,)
DEFAULT_AUDIT_SYSTEMIC_WINDOWS: tuple[tuple[str, str, str], ...] = (
    ("dotcom_bust", "2000-03-24", "2002-10-09"),
    ("gfc", "2007-10-09", "2009-03-09"),
    ("covid_crisis", "2020-02-20", "2020-04-30"),
    ("regional_bank_stress", "2023-03-08", "2023-05-12"),
)

DEFAULT_COMPARISON_PERIODS: tuple[tuple[str, str, str | None], ...] = (
    ("full_1999_to_date", "1999-03-10", None),
    ("dotcom_bubble_burst", "2000-03-24", "2002-10-09"),
    ("dotcom_full_cycle", "1999-03-10", "2002-10-09"),
    ("gfc_peak_to_trough", "2007-10-09", "2009-03-09"),
    ("lost_decade_2000_2009", "2000-01-03", "2009-12-31"),
    ("synthetic_tqqq_live_proxy", "2010-02-11", None),
    ("full_2015_to_date", "2015-01-02", None),
    ("obama_pre_trump", "2015-01-02", "2017-01-19"),
    ("trump_1_full", "2017-01-20", "2021-01-19"),
    ("trade_war_2018_2019", "2018-01-02", "2019-12-31"),
    ("covid_crash_2020", "2020-02-18", "2020-04-30"),
    ("biden_full", "2021-01-20", "2025-01-17"),
    ("biden_2022_bear", "2022-01-03", "2022-12-30"),
    ("trump_2_to_date", "2025-01-21", None),
)

COMPARISON_SUMMARY_COLUMNS = (
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


def _normalize_close(close: pd.DataFrame) -> pd.DataFrame:
    frame = close.copy().sort_index()
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame.columns = frame.columns.astype(str).str.upper().str.strip()
    return frame


def add_synthetic_attack_close(
    close: pd.DataFrame,
    *,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    source_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    multiple: float = 3.0,
    annual_expense_rate: float = DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    initial_close: float = 100.0,
) -> pd.DataFrame:
    """Add a synthetic daily-reset leveraged attack asset for pre-inception stress tests."""
    frame = _normalize_close(close)
    attack_symbol = str(attack_symbol).strip().upper()
    source_symbol = str(source_symbol).strip().upper()
    if source_symbol not in frame.columns:
        raise ValueError(f"synthetic source symbol {source_symbol!r} missing from price history")
    if float(multiple) <= 0:
        return frame

    source = pd.to_numeric(frame[source_symbol], errors="coerce").ffill()
    source_returns = source.pct_change(fill_method=None).fillna(0.0)
    daily_expense = float(annual_expense_rate) / 252.0
    synthetic_returns = (source_returns * float(multiple) - daily_expense).clip(lower=-0.99)
    synthetic_close = float(initial_close) * (1.0 + synthetic_returns).cumprod()
    first_valid = source.first_valid_index()
    if first_valid is not None:
        synthetic_close.loc[synthetic_close.index < first_valid] = pd.NA
    frame[attack_symbol] = synthetic_close
    return frame


def close_matrix_to_price_history(close: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize_close(close)
    long = frame.stack().dropna().rename("close").reset_index()
    long.columns = ["as_of", "symbol", "close"]
    long["volume"] = 0
    return long.loc[:, ["symbol", "as_of", "close", "volume"]]


def build_price_stress_scan(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    qqq_1d_drop: float = DEFAULT_TRIGGER_QQQ_1D_DROP,
    qqq_3d_drop: float = DEFAULT_TRIGGER_QQQ_3D_DROP,
    tqqq_1d_drop: float = DEFAULT_TRIGGER_TQQQ_1D_DROP,
    ma200_near_pct: float = DEFAULT_TRIGGER_MA200_NEAR_PCT,
    scan_hold_days: int = DEFAULT_TRIGGER_SCAN_HOLD_DAYS,
) -> pd.Series:
    """Return days when the V1 overlay would pay for AI news classification.

    This intentionally uses only price pressure. VIX and macro data do not veto,
    reduce, or trigger positions in the V1 definition.
    """
    frame = _normalize_close(close)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    if benchmark_symbol not in frame.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")
    if attack_symbol not in frame.columns:
        raise ValueError(f"attack symbol {attack_symbol!r} missing from price history")

    index = frame.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if index.empty:
        raise RuntimeError("No trading days in requested scan window")

    benchmark = pd.to_numeric(frame[benchmark_symbol], errors="coerce")
    attack = pd.to_numeric(frame[attack_symbol], errors="coerce")
    ma200 = benchmark.rolling(200, min_periods=200).mean()
    benchmark_1d = benchmark.pct_change(1)
    benchmark_3d = benchmark.pct_change(3)
    attack_1d = attack.pct_change(1)
    ma200_cross_down = (benchmark < ma200) & (benchmark.shift(1) >= ma200.shift(1))
    near_ma200_pressure = (benchmark / ma200 - 1.0).abs().le(float(ma200_near_pct)) & benchmark_1d.lt(0.0)

    seed = (
        (benchmark_1d <= float(qqq_1d_drop))
        | (benchmark_3d <= float(qqq_3d_drop))
        | (attack_1d <= float(tqqq_1d_drop))
        | ma200_cross_down.fillna(False)
        | near_ma200_pressure.fillna(False)
    ).reindex(index).fillna(False)
    scan = seed.copy()
    for seed_date, active in seed.items():
        if not bool(active):
            continue
        end = _date_after_trading_days(index, pd.Timestamp(seed_date).normalize(), int(scan_hold_days))
        scan.loc[(scan.index >= seed_date) & (scan.index <= end)] = True
    return scan.rename("price_stress_scan")


def filter_events_by_price_stress(
    events: Sequence[TradeWarEvent],
    scan_days: pd.Series,
    *,
    shock_lookahead_days: int = DEFAULT_SHOCK_LOOKAHEAD_DAYS,
    softening_lookahead_days: int = DEFAULT_SOFTENING_LOOKAHEAD_DAYS,
    active_event_days: int = DEFAULT_ACTIVE_EVENT_DAYS,
) -> tuple[TradeWarEvent, ...]:
    """Simulate V1 AI availability: only recognize events when price stress opens the scanner."""
    scan = pd.Series(scan_days).copy().fillna(False)
    scan.index = pd.to_datetime(scan.index).tz_localize(None).normalize()
    index = pd.DatetimeIndex(scan.index).sort_values()
    recognized: list[TradeWarEvent] = []
    active_until: list[pd.Timestamp] = []

    def has_scan_window(signal_date: pd.Timestamp, lookahead_days: int) -> bool:
        end = _date_after_trading_days(index, signal_date, int(lookahead_days))
        window = scan.loc[(scan.index >= signal_date) & (scan.index <= end)]
        return bool(window.any())

    for event in sorted(events, key=lambda item: item.event_date):
        signal_date = _next_index_date(index, event.event_date)
        if signal_date is None:
            continue
        if event.kind == EVENT_KIND_SHOCK:
            if has_scan_window(signal_date, shock_lookahead_days):
                recognized.append(event)
                active_until.append(signal_date + pd.Timedelta(days=int(active_event_days)))
        elif event.kind == EVENT_KIND_SOFTENING:
            in_active_window = any(signal_date <= active_end for active_end in active_until)
            if in_active_window or has_scan_window(signal_date, softening_lookahead_days):
                recognized.append(event)
    return tuple(recognized)


def _parse_str_tuple(raw: str | Sequence[str] | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    return tuple(str(value).strip() for value in values if str(value).strip())


def _parse_crisis_windows(raw: str | Sequence[tuple[str, str, str]] | None) -> tuple[tuple[str, str, str], ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        windows: list[tuple[str, str, str]] = []
        for item in raw.split(","):
            item_text = item.strip()
            if not item_text:
                continue
            parts = [part.strip() for part in item_text.split(":")]
            if len(parts) != 3 or not all(parts):
                raise ValueError("audit crisis windows must use name:start:end comma-separated syntax")
            windows.append((parts[0], parts[1], parts[2]))
        return tuple(windows)
    return tuple((str(name), str(start), str(end)) for name, start, end in raw)


def _format_crisis_windows(windows: Sequence[tuple[str, str, str]]) -> str:
    return ",".join(f"{name}:{start}:{end}" for name, start, end in windows)


def build_dual_ai_audit_decisions(
    recognized_events: Sequence[TradeWarEvent],
    scan_days: pd.Series,
    *,
    audit_mode: str = AUDIT_MODE_CRISIS_VETO,
    systemic_windows: Sequence[tuple[str, str, str]] = DEFAULT_AUDIT_SYSTEMIC_WINDOWS,
    veto_event_ids: Sequence[str] = (),
) -> tuple[tuple[TradeWarEvent, ...], pd.DataFrame]:
    """Backtest a deterministic dual-AI review proxy over recognized events.

    Historical OpenAI calls are not replayable without archived news inputs and
    fixed model snapshots. This function therefore simulates the *decision
    contract* of the second AI: the proposer has already recognized a price-gated
    trade-war event, and the auditor can only veto events that overlap a
    predeclared systemic-crisis window or an explicit sensitivity-test event id.
    """
    mode = str(audit_mode or AUDIT_MODE_OFF).strip().lower()
    if mode not in AUDIT_MODES:
        raise ValueError(f"Unsupported audit mode: {audit_mode!r}")

    scan = pd.Series(scan_days).copy().fillna(False)
    scan.index = pd.to_datetime(scan.index).tz_localize(None).normalize()
    index = pd.DatetimeIndex(scan.index).sort_values()
    veto_ids = set(_parse_str_tuple(veto_event_ids))
    crisis_windows = tuple(
        (str(name), pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize())
        for name, start, end in systemic_windows
    )

    accepted: list[TradeWarEvent] = []
    rows: list[dict[str, object]] = []
    for event in sorted(recognized_events, key=lambda item: item.event_date):
        signal_date = _next_index_date(index, event.event_date)
        if signal_date is None:
            continue

        veto_reasons: list[str] = []
        if event.event_id in veto_ids:
            veto_reasons.append("explicit_audit_veto_event_id")
        if mode == AUDIT_MODE_CRISIS_VETO:
            for window_name, window_start, window_end in crisis_windows:
                if window_start <= signal_date <= window_end:
                    veto_reasons.append(f"systemic_window:{window_name}")

        passed = not veto_reasons
        if passed:
            accepted.append(event)
        rows.append(
            {
                "audit_mode": mode,
                "event_id": event.event_id,
                "event_date": event.event_date,
                "signal_date": signal_date.date().isoformat(),
                "kind": event.kind,
                "region": event.region,
                "title": event.title,
                "proposer_signal": "price_gated_trade_war_event",
                "auditor_verdict": "pass" if passed else "veto",
                "veto_reason": ";".join(veto_reasons),
                "final_event_included": passed,
                "backtest_note": "deterministic_dual_ai_review_proxy",
            }
        )
    columns = (
        "audit_mode",
        "event_id",
        "event_date",
        "signal_date",
        "kind",
        "region",
        "title",
        "proposer_signal",
        "auditor_verdict",
        "veto_reason",
        "final_event_included",
        "backtest_note",
    )
    decisions = pd.DataFrame(rows, columns=list(columns))
    return tuple(accepted), decisions


def build_tqqq_growth_income_base_weights(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    safe_symbol: str = DEFAULT_SAFE_SYMBOL,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    qqq_weight: float = DEFAULT_DUAL_DRIVE_QQQ_WEIGHT,
    tqqq_weight: float = DEFAULT_DUAL_DRIVE_TQQQ_WEIGHT,
    safe_weight: float = DEFAULT_DUAL_DRIVE_SAFE_WEIGHT,
    cash_weight: float = DEFAULT_DUAL_DRIVE_CASH_WEIGHT,
    idle_safe_weight: float = DEFAULT_IDLE_SAFE_WEIGHT,
    idle_cash_weight: float = DEFAULT_IDLE_CASH_WEIGHT,
) -> pd.DataFrame:
    """Approximate the current fixed dual-drive live profile for research comparison."""
    frame = _normalize_close(close)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    if benchmark_symbol not in frame.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")
    if attack_symbol not in frame.columns:
        raise ValueError(f"attack symbol {attack_symbol!r} missing from price history")

    index = frame.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough trading days for baseline comparison")

    benchmark = pd.to_numeric(frame[benchmark_symbol], errors="coerce")
    ma200 = benchmark.rolling(200, min_periods=200).mean()
    ma20 = benchmark.rolling(20, min_periods=20).mean()
    ma20_slope = ma20.diff()

    active_weights = {
        benchmark_symbol: float(qqq_weight),
        attack_symbol: float(tqqq_weight),
        safe_symbol: float(safe_weight),
        cash_symbol: float(cash_weight),
    }
    idle_weights = {
        benchmark_symbol: 0.0,
        attack_symbol: 0.0,
        safe_symbol: float(idle_safe_weight),
        cash_symbol: float(idle_cash_weight),
    }
    risk_active = False
    rows: list[dict[str, object]] = []
    for date in index[:-1]:
        close_value = float(benchmark.loc[date])
        ma200_value = ma200.loc[date]
        ma20_value = ma20.loc[date]
        slope = ma20_slope.loc[date]
        above_ma200 = pd.notna(ma200_value) and close_value > float(ma200_value)
        positive_slope = pd.notna(slope) and float(slope) > 0.0
        if risk_active and not above_ma200:
            risk_active = False
        elif (not risk_active) and above_ma200 and positive_slope:
            risk_active = True
        pullback_risk_on = (
            not above_ma200
            and pd.notna(ma20_value)
            and close_value > float(ma20_value)
            and positive_slope
        )
        weights = active_weights if risk_active or pullback_risk_on else idle_weights
        rows.append({"as_of": date, **_normalize_weights(weights)})
    return pd.DataFrame(rows).set_index("as_of").sort_index()


def build_price_crisis_guard_signal(
    close: pd.DataFrame,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    drawdown_threshold: float = DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    ma_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_DAYS,
    ma_slope_days: int = DEFAULT_PRICE_CRISIS_GUARD_MA_SLOPE_DAYS,
) -> pd.Series:
    """Return an as-of price-only crisis-regime proxy.

    This is intentionally not an AI backtest. It is a deterministic proxy that
    uses only data visible on each date, so 2000/2008 crisis stress can be
    compared without pretending today's AI model was available historically.
    """
    frame = _normalize_close(close)
    benchmark_symbol = str(benchmark_symbol).strip().upper()
    if benchmark_symbol not in frame.columns:
        raise ValueError(f"benchmark symbol {benchmark_symbol!r} missing from price history")

    index = frame.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if index.empty:
        raise RuntimeError("No trading days in requested crisis-guard window")

    benchmark = pd.to_numeric(frame[benchmark_symbol], errors="coerce")
    ma = benchmark.rolling(int(ma_days), min_periods=int(ma_days)).mean()
    ma_slope = ma.diff(int(ma_slope_days))
    high_252 = benchmark.rolling(252, min_periods=63).max()
    drawdown = benchmark / high_252 - 1.0
    crisis = (
        benchmark.lt(ma)
        & drawdown.le(float(drawdown_threshold))
        & ma_slope.lt(0.0)
    ).reindex(index).fillna(False)
    return crisis.rename("price_crisis_guard_active")


def apply_price_crisis_guard_to_weights(
    weights: pd.DataFrame,
    crisis_signal: pd.Series,
    *,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    safe_symbol: str = DEFAULT_SAFE_SYMBOL,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    risk_multiplier: float = DEFAULT_PRICE_CRISIS_GUARD_RISK_MULTIPLIER,
) -> pd.DataFrame:
    """Move growth exposure to the safe asset when the price crisis proxy is active."""
    frame = weights.copy().fillna(0.0)
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    crisis = pd.Series(crisis_signal).fillna(False).copy()
    crisis.index = pd.to_datetime(crisis.index).tz_localize(None).normalize()
    crisis = crisis.reindex(frame.index).ffill().fillna(False)

    benchmark_symbol = str(benchmark_symbol).strip().upper()
    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    multiplier = max(0.0, min(1.0, float(risk_multiplier)))
    risk_symbols = (benchmark_symbol, attack_symbol)

    rows: list[dict[str, object]] = []
    for date, row in frame.iterrows():
        row_weights = _normalize_weights(row.to_dict())
        if bool(crisis.loc[date]):
            removed = 0.0
            for symbol in risk_symbols:
                old_weight = row_weights.get(symbol, 0.0)
                new_weight = old_weight * multiplier
                row_weights[symbol] = new_weight
                removed += old_weight - new_weight
            destination = safe_symbol if safe_symbol else cash_symbol
            row_weights[destination] = row_weights.get(destination, 0.0) + removed
        rows.append({"as_of": date, **_normalize_weights(row_weights)})
    return pd.DataFrame(rows).set_index("as_of").sort_index()


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): float(weight)
        for symbol, weight in weights.items()
        if abs(float(weight)) > 1e-12
    }
    total = sum(cleaned.values())
    if total <= 0:
        return {DEFAULT_CASH_SYMBOL: 1.0}
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def _integrate_overlay_weights(
    base_row: Mapping[str, float],
    overlay_row: Mapping[str, float],
    *,
    sleeve_ratio: float,
    attack_symbol: str,
    safe_symbol: str,
    cash_symbol: str,
) -> dict[str, float]:
    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    base = _normalize_weights(base_row)
    weights = {symbol: base.get(symbol, 0.0) for symbol in set(base) | {attack_symbol, safe_symbol, cash_symbol}}

    remaining = max(0.0, min(1.0, float(sleeve_ratio)))
    safe_take = min(weights.get(safe_symbol, 0.0), remaining)
    weights[safe_symbol] = weights.get(safe_symbol, 0.0) - safe_take
    remaining -= safe_take
    cash_take = min(weights.get(cash_symbol, 0.0), remaining)
    weights[cash_symbol] = weights.get(cash_symbol, 0.0) - cash_take
    remaining -= cash_take
    if remaining > 1e-12:
        risk_symbols = [symbol for symbol in weights if symbol not in {safe_symbol, cash_symbol}]
        risk_total = sum(max(0.0, weights.get(symbol, 0.0)) for symbol in risk_symbols)
        if risk_total > 0:
            for symbol in risk_symbols:
                weights[symbol] = weights.get(symbol, 0.0) - remaining * weights.get(symbol, 0.0) / risk_total

    overlay_attack = max(0.0, float(overlay_row.get(attack_symbol, 0.0)))
    overlay_cash = max(0.0, float(overlay_row.get(cash_symbol, 0.0)))
    overlay_total = overlay_attack + overlay_cash
    if overlay_total <= 0:
        overlay_cash = 1.0
        overlay_total = 1.0
    weights[attack_symbol] = weights.get(attack_symbol, 0.0) + float(sleeve_ratio) * overlay_attack / overlay_total
    weights[safe_symbol] = weights.get(safe_symbol, 0.0) + float(sleeve_ratio) * overlay_cash / overlay_total
    return _normalize_weights(weights)


def _weights_to_returns(
    returns: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    strategy_name: str,
    safe_symbol: str = DEFAULT_SAFE_SYMBOL,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> tuple[pd.Series, pd.DataFrame]:
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL
    index = returns.index
    output = pd.Series(0.0, index=index, name=strategy_name)
    weight_rows: list[dict[str, object]] = []
    previous_weights: dict[str, float] | None = None
    for idx_pos, date in enumerate(index[:-1]):
        next_date = index[idx_pos + 1]
        row = _normalize_weights(weights.loc[date].to_dict())
        turnover = 0.0
        if previous_weights is not None:
            symbols = set(row) | set(previous_weights)
            turnover = 0.5 * sum(abs(row.get(symbol, 0.0) - previous_weights.get(symbol, 0.0)) for symbol in symbols)
        gross_return = 0.0
        for symbol, weight in row.items():
            if symbol == cash_symbol:
                continue
            gross_return += float(weight) * float(returns.at[next_date, symbol] if symbol in returns.columns else 0.0)
        output.at[next_date] = gross_return - turnover * (float(turnover_cost_bps) / 10_000.0)
        weight_rows.append({"as_of": date, **row, "turnover": turnover})
        previous_weights = row
    weights_history = pd.DataFrame(weight_rows).set_index("as_of").sort_index()
    if safe_symbol not in weights_history.columns:
        weights_history[safe_symbol] = 0.0
    return output, weights_history


def _parse_float_tuple(raw: str | Sequence[float]) -> tuple[float, ...]:
    if isinstance(raw, str):
        values = raw.split(",")
    else:
        values = list(raw)
    output: list[float] = []
    for value in values:
        value_text = str(value).strip()
        if value_text:
            output.append(float(value_text))
    return tuple(output)


def build_period_summary(
    returns_by_strategy: Mapping[str, pd.Series],
    *,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_COMPARISON_PERIODS,
    trades_by_strategy: Mapping[str, pd.DataFrame] | None = None,
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
            period_returns = returns.loc[(returns.index >= start) & (returns.index <= end)]
            if len(period_returns.dropna()) < 2:
                continue
            trades = (trades_by_strategy or {}).get(strategy_name)
            if trades is not None and not trades.empty and "signal_date" in trades.columns:
                trades = trades.copy()
                signal_dates = pd.to_datetime(trades["signal_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
                trades = trades.loc[signal_dates.between(start, end)].copy()
            row = summarize_returns(period_returns, strategy_name=strategy_name, trades=trades)
            row["Period"] = period_name
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=list(COMPARISON_SUMMARY_COLUMNS))
    frame = pd.DataFrame(rows)
    return frame.loc[:, list(COMPARISON_SUMMARY_COLUMNS)]


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


def build_diagnostics(
    *,
    scan_days: pd.Series,
    recognized_events: Sequence[TradeWarEvent],
    trades: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_COMPARISON_PERIODS,
) -> pd.DataFrame:
    scan = pd.Series(scan_days).fillna(False).copy()
    scan.index = pd.to_datetime(scan.index).tz_localize(None).normalize()
    global_end = pd.Timestamp(scan.index.max()).normalize()
    trade_frame = trades.copy()
    if not trade_frame.empty and "signal_date" in trade_frame.columns:
        trade_frame["signal_date_ts"] = pd.to_datetime(trade_frame["signal_date"], errors="coerce").dt.tz_localize(None)
    rows: list[dict[str, object]] = []
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        window = scan.loc[(scan.index >= start) & (scan.index <= end)]
        event_count = sum(start <= pd.Timestamp(event.event_date).normalize() <= end for event in recognized_events)
        trade_count = 0
        if not trade_frame.empty and "signal_date_ts" in trade_frame.columns:
            trade_count = int(trade_frame["signal_date_ts"].between(start, end).sum())
        rows.append(
            {
                "Period": period_name,
                "Stress Scan Days": int(window.sum()),
                "Trading Days": int(len(window)),
                "Scan Day Ratio": float(window.mean()) if len(window) else float("nan"),
                "Recognized Events": int(event_count),
                "Trades": trade_count,
            }
        )
    return pd.DataFrame(rows)


def build_audit_diagnostics(
    audit_decisions: pd.DataFrame,
    *,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_COMPARISON_PERIODS,
) -> pd.DataFrame:
    frame = audit_decisions.copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "Period",
                "Audit Mode",
                "Proposer Events",
                "Auditor Passed",
                "Auditor Vetoed",
                "Veto Rate",
                "Final Events",
            ]
        )
    frame["signal_date_ts"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["signal_date_ts"])
    if frame.empty:
        return pd.DataFrame()
    global_end = pd.Timestamp(frame["signal_date_ts"].max()).normalize()
    rows: list[dict[str, object]] = []
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        window = frame.loc[frame["signal_date_ts"].between(start, end)]
        for audit_mode, group in window.groupby("audit_mode", sort=False):
            proposer_events = int(len(group))
            vetoed = int(group["auditor_verdict"].eq("veto").sum())
            passed = int(group["auditor_verdict"].eq("pass").sum())
            rows.append(
                {
                    "Period": period_name,
                    "Audit Mode": audit_mode,
                    "Proposer Events": proposer_events,
                    "Auditor Passed": passed,
                    "Auditor Vetoed": vetoed,
                    "Veto Rate": float(vetoed / proposer_events) if proposer_events else float("nan"),
                    "Final Events": int(group["final_event_included"].sum()),
                }
            )
    return pd.DataFrame(rows)


def build_crisis_guard_diagnostics(
    crisis_signal: pd.Series,
    *,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_COMPARISON_PERIODS,
) -> pd.DataFrame:
    signal = pd.Series(crisis_signal).fillna(False).copy()
    if signal.empty:
        return pd.DataFrame(
            columns=["Period", "Crisis Guard Active Days", "Trading Days", "Crisis Guard Active Ratio"]
        )
    signal.index = pd.to_datetime(signal.index).tz_localize(None).normalize()
    global_end = pd.Timestamp(signal.index.max()).normalize()
    rows: list[dict[str, object]] = []
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        window = signal.loc[(signal.index >= start) & (signal.index <= end)]
        rows.append(
            {
                "Period": period_name,
                "Crisis Guard Active Days": int(window.sum()),
                "Trading Days": int(len(window)),
                "Crisis Guard Active Ratio": float(window.mean()) if len(window) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _run_taco_overlay_backtest(
    price_history,
    *,
    recognized_events: Sequence[TradeWarEvent],
    start_date: str,
    end_date: str | None,
    overlay_sleeve_ratios: Sequence[float],
    benchmark_symbol: str,
    attack_symbol: str,
    cash_symbol: str,
    turnover_cost_bps: float,
) -> dict[str, object]:
    return run_backtest(
        price_history,
        events=recognized_events,
        basket_weights={str(attack_symbol).strip().upper(): 1.0},
        start_date=start_date,
        end_date=end_date,
        turnover_cost_bps=turnover_cost_bps,
        account_sleeve_ratio=max(float(value) for value in overlay_sleeve_ratios),
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=benchmark_symbol,
        cash_symbol=cash_symbol,
    )


def _add_overlay_strategy_returns(
    *,
    scenario_prefix: str,
    taco_result: Mapping[str, object],
    returns: pd.DataFrame,
    base_weights: pd.DataFrame,
    returns_by_strategy: dict[str, pd.Series],
    weights_by_strategy: dict[str, pd.DataFrame],
    trades_by_strategy: dict[str, pd.DataFrame],
    index: pd.DatetimeIndex,
    overlay_sleeve_ratios: Sequence[float],
    attack_symbol: str,
    safe_symbol: str,
    cash_symbol: str,
    turnover_cost_bps: float,
) -> None:
    overlay_weights = taco_result["weights_history"].copy()
    if not isinstance(overlay_weights, pd.DataFrame):
        raise TypeError("taco_result['weights_history'] must be a DataFrame")
    overlay_weights.index = pd.to_datetime(overlay_weights.index).tz_localize(None).normalize()
    overlay_weights = overlay_weights.reindex(index[:-1]).ffill().fillna({cash_symbol: 1.0}).fillna(0.0)
    if attack_symbol not in overlay_weights.columns:
        overlay_weights[attack_symbol] = 0.0
    if cash_symbol not in overlay_weights.columns:
        overlay_weights[cash_symbol] = 1.0 - overlay_weights[attack_symbol]

    for sleeve_ratio in overlay_sleeve_ratios:
        strategy_name = f"{scenario_prefix}_{int(float(sleeve_ratio) * 100)}pct"
        rows = []
        for date in index[:-1]:
            rows.append(
                {
                    "as_of": date,
                    **_integrate_overlay_weights(
                        base_weights.loc[date].to_dict(),
                        overlay_weights.loc[date].to_dict(),
                        sleeve_ratio=float(sleeve_ratio),
                        attack_symbol=attack_symbol,
                        safe_symbol=safe_symbol,
                        cash_symbol=cash_symbol,
                    ),
                }
            )
        combined_weights = pd.DataFrame(rows).set_index("as_of").reindex(index[:-1]).fillna(0.0)
        strategy_returns, strategy_weights = _weights_to_returns(
            returns,
            combined_weights,
            strategy_name=strategy_name,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_strategy[strategy_name] = strategy_returns
        weights_by_strategy[strategy_name] = strategy_weights
        trades = taco_result.get("trades")
        if isinstance(trades, pd.DataFrame):
            trades_by_strategy[strategy_name] = trades


def run_overlay_comparison(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    overlay_sleeve_ratios: Sequence[float] = DEFAULT_OVERLAY_SLEEVE_RATIOS,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    attack_symbol: str = DEFAULT_ATTACK_SYMBOL,
    safe_symbol: str = DEFAULT_SAFE_SYMBOL,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    audit_modes: Sequence[str] = DEFAULT_AUDIT_MODES,
    audit_systemic_windows: Sequence[tuple[str, str, str]] = DEFAULT_AUDIT_SYSTEMIC_WINDOWS,
    audit_veto_event_ids: Sequence[str] = (),
    synthetic_attack_from: str | None = None,
    synthetic_attack_multiple: float = DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
    synthetic_attack_expense_rate: float = DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
    include_price_crisis_guard: bool = False,
    crisis_guard_drawdown: float = DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN,
    crisis_guard_risk_multiplier: float = DEFAULT_PRICE_CRISIS_GUARD_RISK_MULTIPLIER,
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
        raise RuntimeError("Not enough trading days for overlay comparison")
    comparison_price_history = close_matrix_to_price_history(close)

    scan_days = build_price_stress_scan(
        close,
        start_date=start_date,
        end_date=end_date,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
    )
    recognized_events = filter_events_by_price_stress(events, scan_days)

    taco_result = _run_taco_overlay_backtest(
        comparison_price_history,
        recognized_events=recognized_events,
        start_date=start_date,
        end_date=end_date,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        benchmark_symbol=benchmark_symbol,
        attack_symbol=attack_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    returns = close.pct_change(fill_method=None).fillna(0.0).reindex(index).fillna(0.0)
    for symbol in {benchmark_symbol, attack_symbol, safe_symbol}:
        symbol_text = str(symbol).strip().upper()
        if symbol_text not in returns.columns:
            returns[symbol_text] = 0.0

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
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    attack_symbol = str(attack_symbol).strip().upper()
    safe_symbol = str(safe_symbol).strip().upper()
    cash_symbol = str(cash_symbol).strip().upper() or DEFAULT_CASH_SYMBOL

    returns_by_strategy: dict[str, pd.Series] = {"base": base_returns}
    weights_by_strategy: dict[str, pd.DataFrame] = {"base": base_weights_history}
    trades_by_strategy: dict[str, pd.DataFrame] = {}
    _add_overlay_strategy_returns(
        scenario_prefix="price_stress_ai_taco",
        taco_result=taco_result,
        returns=returns,
        base_weights=base_weights,
        returns_by_strategy=returns_by_strategy,
        weights_by_strategy=weights_by_strategy,
        trades_by_strategy=trades_by_strategy,
        index=index,
        overlay_sleeve_ratios=overlay_sleeve_ratios,
        attack_symbol=attack_symbol,
        safe_symbol=safe_symbol,
        cash_symbol=cash_symbol,
        turnover_cost_bps=turnover_cost_bps,
    )

    crisis_signal = pd.Series(dtype=bool, name="price_crisis_guard_active")
    if bool(include_price_crisis_guard):
        crisis_signal = build_price_crisis_guard_signal(
            close,
            start_date=start_date,
            end_date=end_date,
            benchmark_symbol=benchmark_symbol,
            drawdown_threshold=float(crisis_guard_drawdown),
        )
        crisis_base_weights = apply_price_crisis_guard_to_weights(
            base_weights,
            crisis_signal,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            risk_multiplier=float(crisis_guard_risk_multiplier),
        ).reindex(index[:-1]).ffill().fillna(0.0)
        crisis_base_returns, crisis_base_weights_history = _weights_to_returns(
            returns,
            crisis_base_weights,
            strategy_name="price_crisis_guard_base",
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_strategy["price_crisis_guard_base"] = crisis_base_returns
        weights_by_strategy["price_crisis_guard_base"] = crisis_base_weights_history
        _add_overlay_strategy_returns(
            scenario_prefix="price_crisis_guard_ai_taco",
            taco_result=taco_result,
            returns=returns,
            base_weights=crisis_base_weights,
            returns_by_strategy=returns_by_strategy,
            weights_by_strategy=weights_by_strategy,
            trades_by_strategy=trades_by_strategy,
            index=index,
            overlay_sleeve_ratios=overlay_sleeve_ratios,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )

    audit_decisions_by_mode: list[pd.DataFrame] = []
    taco_trades_by_scenario = [taco_result["trades"].assign(scenario="price_stress_ai_taco")]
    taco_summaries_by_scenario = [taco_result["summary"].assign(scenario="price_stress_ai_taco")]
    taco_period_summaries_by_scenario = [
        taco_result["period_summary"].assign(scenario="price_stress_ai_taco")
    ]
    for raw_mode in _parse_str_tuple(audit_modes):
        audit_mode = str(raw_mode).strip().lower()
        if audit_mode == AUDIT_MODE_OFF:
            continue
        audited_events, audit_decisions = build_dual_ai_audit_decisions(
            recognized_events,
            scan_days,
            audit_mode=audit_mode,
            systemic_windows=audit_systemic_windows,
            veto_event_ids=audit_veto_event_ids,
        )
        audit_decisions_by_mode.append(audit_decisions)
        audit_result = _run_taco_overlay_backtest(
            comparison_price_history,
            recognized_events=audited_events,
            start_date=start_date,
            end_date=end_date,
            overlay_sleeve_ratios=overlay_sleeve_ratios,
            benchmark_symbol=benchmark_symbol,
            attack_symbol=attack_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        scenario_prefix = f"dual_ai_{audit_mode}_taco"
        _add_overlay_strategy_returns(
            scenario_prefix=scenario_prefix,
            taco_result=audit_result,
            returns=returns,
            base_weights=base_weights,
            returns_by_strategy=returns_by_strategy,
            weights_by_strategy=weights_by_strategy,
            trades_by_strategy=trades_by_strategy,
            index=index,
            overlay_sleeve_ratios=overlay_sleeve_ratios,
            attack_symbol=attack_symbol,
            safe_symbol=safe_symbol,
            cash_symbol=cash_symbol,
            turnover_cost_bps=turnover_cost_bps,
        )
        taco_trades_by_scenario.append(audit_result["trades"].assign(scenario=scenario_prefix))
        taco_summaries_by_scenario.append(audit_result["summary"].assign(scenario=scenario_prefix))
        taco_period_summaries_by_scenario.append(
            audit_result["period_summary"].assign(scenario=scenario_prefix)
        )

    summary = build_period_summary(returns_by_strategy, trades_by_strategy=trades_by_strategy)
    deltas = build_deltas_vs_base(summary)
    diagnostics = build_diagnostics(
        scan_days=scan_days,
        recognized_events=recognized_events,
        trades=taco_result["trades"],
    )
    audit_decisions = (
        pd.concat(audit_decisions_by_mode, ignore_index=True)
        if audit_decisions_by_mode
        else pd.DataFrame()
    )
    return {
        "summary": summary,
        "deltas_vs_base": deltas,
        "diagnostics": diagnostics,
        "audit_diagnostics": build_audit_diagnostics(audit_decisions),
        "crisis_guard_diagnostics": build_crisis_guard_diagnostics(crisis_signal),
        "crisis_guard_signal": crisis_signal,
        "scan_days": scan_days,
        "recognized_events": events_to_frame(recognized_events),
        "audit_decisions": audit_decisions,
        "taco_trades": taco_result["trades"],
        "taco_trades_by_scenario": pd.concat(taco_trades_by_scenario, ignore_index=True),
        "taco_sleeve_summary": taco_result["summary"],
        "taco_sleeve_summary_by_scenario": pd.concat(taco_summaries_by_scenario, ignore_index=True),
        "taco_sleeve_period_summary": taco_result["period_summary"],
        "taco_sleeve_period_summary_by_scenario": pd.concat(
            taco_period_summaries_by_scenario, ignore_index=True
        ),
        "returns_by_strategy": returns_by_strategy,
        "weights_by_strategy": weights_by_strategy,
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
            "Scan Day Ratio",
            "Veto Rate",
            "Crisis Guard Active Ratio",
        }:
            output[column] = output[column].map(lambda value: f"{float(value):.2%}" if pd.notna(value) else "")
    for column in ("Sharpe", "Delta Sharpe", "Final Equity"):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2f}" if pd.notna(value) else "")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare TQQQ growth-income with the V1 price-stress AI TACO overlay.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument(
        "--overlay-sleeve-ratios",
        default=",".join(str(value) for value in DEFAULT_OVERLAY_SLEEVE_RATIOS),
    )
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--attack-symbol", default=DEFAULT_ATTACK_SYMBOL)
    parser.add_argument("--safe-symbol", default=DEFAULT_SAFE_SYMBOL)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument(
        "--synthetic-attack-from",
        default=None,
        help="Optional source symbol used to synthesize a daily-reset leveraged attack asset.",
    )
    parser.add_argument(
        "--synthetic-attack-multiple",
        type=float,
        default=DEFAULT_SYNTHETIC_ATTACK_MULTIPLE,
        help="If positive, synthesize attack-symbol from synthetic-attack-from or benchmark-symbol.",
    )
    parser.add_argument(
        "--synthetic-attack-expense-rate",
        type=float,
        default=DEFAULT_SYNTHETIC_ATTACK_EXPENSE_RATE,
        help="Annual expense drag for the synthetic attack asset.",
    )
    parser.add_argument(
        "--include-price-crisis-guard",
        action="store_true",
        help="Add a deterministic price-only crisis-guard proxy scenario for long-horizon stress tests.",
    )
    parser.add_argument("--crisis-guard-drawdown", type=float, default=DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN)
    parser.add_argument(
        "--crisis-guard-risk-multiplier",
        type=float,
        default=DEFAULT_PRICE_CRISIS_GUARD_RISK_MULTIPLIER,
    )
    parser.add_argument(
        "--audit-modes",
        default=",".join(DEFAULT_AUDIT_MODES),
        help=f"Comma-separated dual-AI audit proxy modes. Supported: {','.join(sorted(AUDIT_MODES))}.",
    )
    parser.add_argument(
        "--audit-crisis-windows",
        default=_format_crisis_windows(DEFAULT_AUDIT_SYSTEMIC_WINDOWS),
        help="Comma-separated name:start:end windows where the auditor vetoes TACO candidates.",
    )
    parser.add_argument(
        "--audit-veto-event-ids",
        default="",
        help="Comma-separated event ids to veto for audit false-positive sensitivity tests.",
    )
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.download:
        symbols = [args.benchmark_symbol, args.safe_symbol]
        if float(args.synthetic_attack_multiple) > 0.0:
            symbols.append(args.synthetic_attack_from or args.benchmark_symbol)
        else:
            symbols.append(args.attack_symbol)
        symbols = list(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip() and str(symbol).strip().upper() != DEFAULT_CASH_SYMBOL
            )
        )
        price_history = download_price_history(symbols, start=args.price_start, end=args.price_end)
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "tqqq_taco_overlay_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    result = run_overlay_comparison(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        start_date=args.start_date,
        end_date=args.end_date,
        overlay_sleeve_ratios=_parse_float_tuple(args.overlay_sleeve_ratios),
        benchmark_symbol=args.benchmark_symbol,
        attack_symbol=args.attack_symbol,
        safe_symbol=args.safe_symbol,
        turnover_cost_bps=float(args.turnover_cost_bps),
        audit_modes=_parse_str_tuple(args.audit_modes),
        audit_systemic_windows=_parse_crisis_windows(args.audit_crisis_windows),
        audit_veto_event_ids=_parse_str_tuple(args.audit_veto_event_ids),
        synthetic_attack_from=args.synthetic_attack_from,
        synthetic_attack_multiple=float(args.synthetic_attack_multiple),
        synthetic_attack_expense_rate=float(args.synthetic_attack_expense_rate),
        include_price_crisis_guard=bool(args.include_price_crisis_guard),
        crisis_guard_drawdown=float(args.crisis_guard_drawdown),
        crisis_guard_risk_multiplier=float(args.crisis_guard_risk_multiplier),
    )
    summary = result["summary"]
    deltas = result["deltas_vs_base"]
    diagnostics = result["diagnostics"]
    audit_diagnostics = result["audit_diagnostics"]
    crisis_guard_diagnostics = result["crisis_guard_diagnostics"]
    print("\nSummary:")
    print(_format_percent_columns(summary).to_string(index=False))
    print("\nDeltas vs base:")
    print(_format_percent_columns(deltas).to_string(index=False))
    print("\nDiagnostics:")
    print(_format_percent_columns(diagnostics).to_string(index=False))
    if not audit_diagnostics.empty:
        print("\nDual-AI audit diagnostics:")
        print(_format_percent_columns(audit_diagnostics).to_string(index=False))
    if not crisis_guard_diagnostics.empty:
        print("\nPrice crisis-guard diagnostics:")
        print(_format_percent_columns(crisis_guard_diagnostics).to_string(index=False))

    summary.to_csv(output_dir / "summary.csv", index=False)
    deltas.to_csv(output_dir / "deltas_vs_base.csv", index=False)
    diagnostics.to_csv(output_dir / "diagnostics.csv", index=False)
    audit_diagnostics.to_csv(output_dir / "audit_diagnostics.csv", index=False)
    crisis_guard_diagnostics.to_csv(output_dir / "crisis_guard_diagnostics.csv", index=False)
    result["crisis_guard_signal"].to_frame().to_csv(output_dir / "crisis_guard_signal.csv")
    result["scan_days"].to_frame().to_csv(output_dir / "price_stress_scan_days.csv")
    result["recognized_events"].to_csv(output_dir / "recognized_event_calendar.csv", index=False)
    result["audit_decisions"].to_csv(output_dir / "audit_decisions.csv", index=False)
    result["taco_trades"].to_csv(output_dir / "taco_trades.csv", index=False)
    result["taco_trades_by_scenario"].to_csv(output_dir / "taco_trades_by_scenario.csv", index=False)
    result["taco_sleeve_summary"].to_csv(output_dir / "taco_sleeve_summary.csv", index=False)
    result["taco_sleeve_summary_by_scenario"].to_csv(output_dir / "taco_sleeve_summary_by_scenario.csv", index=False)
    result["taco_sleeve_period_summary"].to_csv(output_dir / "taco_sleeve_period_summary.csv", index=False)
    result["taco_sleeve_period_summary_by_scenario"].to_csv(
        output_dir / "taco_sleeve_period_summary_by_scenario.csv",
        index=False,
    )
    returns_dir = output_dir / "returns"
    weights_dir = output_dir / "weights"
    returns_dir.mkdir(exist_ok=True)
    weights_dir.mkdir(exist_ok=True)
    for strategy, returns in result["returns_by_strategy"].items():
        returns.rename("return").to_csv(returns_dir / f"{strategy}.csv")
    for strategy, weights in result["weights_by_strategy"].items():
        weights.to_csv(weights_dir / f"{strategy}.csv")
    print(f"wrote V1 TACO overlay comparison outputs -> {output_dir}")
    return 0


__all__ = [
    "AUDIT_MODE_CRISIS_VETO",
    "AUDIT_MODE_OFF",
    "AUDIT_MODES",
    "DEFAULT_AUDIT_MODES",
    "DEFAULT_AUDIT_SYSTEMIC_WINDOWS",
    "DEFAULT_COMPARISON_PERIODS",
    "DEFAULT_OVERLAY_SLEEVE_RATIOS",
    "DEFAULT_PRICE_CRISIS_GUARD_DRAWDOWN",
    "DEFAULT_PRICE_CRISIS_GUARD_RISK_MULTIPLIER",
    "add_synthetic_attack_close",
    "apply_price_crisis_guard_to_weights",
    "build_audit_diagnostics",
    "build_crisis_guard_diagnostics",
    "build_deltas_vs_base",
    "build_diagnostics",
    "build_dual_ai_audit_decisions",
    "build_period_summary",
    "build_price_crisis_guard_signal",
    "build_price_stress_scan",
    "build_tqqq_growth_income_base_weights",
    "close_matrix_to_price_history",
    "filter_events_by_price_stress",
    "main",
    "run_overlay_comparison",
]
