from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import pstdev
from typing import Iterable, Mapping


TRADING_DAYS_PER_YEAR = 252
CALENDAR_DAYS_PER_YEAR = 365.25
DEFAULT_DTE_DAYS = 730
DEFAULT_ROLL_DTE_DAYS = 365
DEFAULT_TARGET_DELTA = 0.75
DEFAULT_PREMIUM_BUDGET_RATIO = 0.03
DEFAULT_RISK_FREE_RATE = 0.035
DEFAULT_DIVIDEND_YIELD = 0.008
DEFAULT_IV_MULTIPLIER = 1.10
DEFAULT_VOL_FLOOR = 0.16
DEFAULT_VOL_CAP = 0.55
DEFAULT_MA_WINDOW = 200
DEFAULT_MOMENTUM_WINDOW = 63
DEFAULT_INITIAL_EQUITY = 100_000.0
DEFAULT_CONTRACT_MULTIPLIER = 100
DEFAULT_MIN_DTE_DAYS = 540
DEFAULT_MAX_DTE_DAYS = 930
DEFAULT_MAX_BID_ASK_SPREAD_RATIO = 0.12
SUMMARY_FIELDS = (
    "Run",
    "Underlier",
    "Data Mode",
    "Promotion Evidence",
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Final Equity",
    "Buy Hold CAGR",
    "Buy Hold Max Drawdown",
    "Option Trade Count",
    "Entry Skip Count",
    "Expired Worthless Count",
    "Recovered Principal Count",
    "Missing Quote Count",
    "Proxy Warning",
)


@dataclass(frozen=True)
class LeapsProxyConfig:
    underlier: str = "QQQ"
    run_name: str = "qqq_leaps_growth_proxy"
    initial_equity: float = DEFAULT_INITIAL_EQUITY
    premium_budget_ratio: float = DEFAULT_PREMIUM_BUDGET_RATIO
    target_delta: float = DEFAULT_TARGET_DELTA
    dte_days: int = DEFAULT_DTE_DAYS
    roll_dte_days: int = DEFAULT_ROLL_DTE_DAYS
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE
    dividend_yield: float = DEFAULT_DIVIDEND_YIELD
    iv_multiplier: float = DEFAULT_IV_MULTIPLIER
    vol_floor: float = DEFAULT_VOL_FLOOR
    vol_cap: float = DEFAULT_VOL_CAP
    realized_vol_window: int = TRADING_DAYS_PER_YEAR
    entry_gate_enabled: bool = True
    ma_window: int = DEFAULT_MA_WINDOW
    momentum_window: int = DEFAULT_MOMENTUM_WINDOW
    contract_multiplier: int = DEFAULT_CONTRACT_MULTIPLIER
    min_dte_days: int = DEFAULT_MIN_DTE_DAYS
    max_dte_days: int = DEFAULT_MAX_DTE_DAYS
    max_bid_ask_spread_ratio: float = DEFAULT_MAX_BID_ASK_SPREAD_RATIO
    min_volume: int = 0
    min_open_interest: int = 0


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _parse_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10:
        text = text[:10]
    return datetime.fromisoformat(text).date().isoformat()


def _date_value(value: object) -> datetime.date:
    return datetime.fromisoformat(_parse_date(value)).date()


def _add_calendar_days(value: object, days: int) -> str:
    return (_date_value(value) + timedelta(days=max(1, int(days)))).isoformat()


def _calendar_dte(as_of: object, expiration: object) -> int:
    return max(0, (_date_value(expiration) - _date_value(as_of)).days)


def _first_present(row: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _as_float(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: object, *, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def load_price_history(path: str | Path) -> list[dict[str, object]]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_option_chain_history(path: str | Path) -> list[dict[str, object]]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def close_series_for_symbol(
    price_history: Iterable[Mapping[str, object]],
    *,
    symbol: str,
) -> list[tuple[str, float]]:
    target = _normalize_symbol(symbol)
    rows: list[tuple[str, float]] = []
    for row in price_history:
        row_symbol = _normalize_symbol(row.get("symbol"))
        if row_symbol != target:
            continue
        try:
            close = float(row.get("close") or row.get("Close") or 0.0)
        except (TypeError, ValueError):
            continue
        if close <= 0.0:
            continue
        as_of = _parse_date(row.get("as_of") or row.get("date") or row.get("Date"))
        if not as_of:
            continue
        rows.append((as_of, close))
    return sorted(dict(rows).items())


def _normalize_option_right(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"C", "CALL"}:
        return "C"
    if text in {"P", "PUT"}:
        return "P"
    return text


def _normalize_option_quote(row: Mapping[str, object], *, underlier: str) -> dict[str, object] | None:
    target = _normalize_symbol(underlier)
    row_underlier = _normalize_symbol(
        _first_present(row, "underlier", "underlying", "root", "symbol", "underlying_symbol")
    )
    if row_underlier and row_underlier != target:
        return None

    right = _normalize_option_right(_first_present(row, "right", "option_type", "put_call", "type"))
    if right != "C":
        return None

    try:
        as_of = _parse_date(_first_present(row, "as_of", "date", "quote_date"))
        expiration = _parse_date(_first_present(row, "expiration", "expiry", "expiration_date"))
    except ValueError:
        return None

    strike = _as_float(_first_present(row, "strike", "strike_price"), default=0.0)
    bid = _as_float(_first_present(row, "bid", "best_bid"), default=-1.0)
    ask = _as_float(_first_present(row, "ask", "best_ask"), default=-1.0)
    mid = _as_float(_first_present(row, "mid", "mark", "close", "option_close"), default=0.0)
    if bid < 0.0 or ask <= 0.0:
        return None
    if ask < bid:
        return None
    if mid <= 0.0:
        mid = (bid + ask) / 2.0
    if strike <= 0.0 or mid <= 0.0:
        return None

    delta = abs(_as_float(_first_present(row, "delta", "call_delta"), default=0.0))
    dte = (_date_value(expiration) - _date_value(as_of)).days
    symbol = str(_first_present(row, "option_symbol", "contract_symbol", "occ_symbol")).strip()
    contract_key = symbol or f"{expiration}|{right}|{strike:.6f}"
    return {
        "as_of": as_of,
        "underlier": target,
        "expiration": expiration,
        "right": right,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "delta": delta,
        "dte": dte,
        "volume": _as_int(_first_present(row, "volume", "option_volume"), default=0),
        "open_interest": _as_int(_first_present(row, "open_interest", "oi"), default=0),
        "contract_key": contract_key,
    }


def _spread_ratio(quote: Mapping[str, object]) -> float:
    bid = float(quote["bid"])
    ask = float(quote["ask"])
    mid = max((bid + ask) / 2.0, 1e-9)
    return (ask - bid) / mid


def _index_option_quotes(
    option_chain_history: Iterable[Mapping[str, object]],
    *,
    underlier: str,
) -> tuple[dict[str, list[dict[str, object]]], dict[tuple[str, str], dict[str, object]]]:
    quotes_by_date: dict[str, list[dict[str, object]]] = {}
    quotes_by_contract_date: dict[tuple[str, str], dict[str, object]] = {}
    for row in option_chain_history:
        quote = _normalize_option_quote(row, underlier=underlier)
        if quote is None:
            continue
        as_of = str(quote["as_of"])
        contract_key = str(quote["contract_key"])
        quotes_by_date.setdefault(as_of, []).append(quote)
        quotes_by_contract_date[(as_of, contract_key)] = quote
    return quotes_by_date, quotes_by_contract_date


def _select_entry_quote(
    quotes: Iterable[Mapping[str, object]],
    *,
    config: LeapsProxyConfig,
) -> Mapping[str, object] | None:
    candidates: list[Mapping[str, object]] = []
    for quote in quotes:
        if not (config.min_dte_days <= int(quote["dte"]) <= config.max_dte_days):
            continue
        if float(quote["delta"]) <= 0.0:
            continue
        if _spread_ratio(quote) > config.max_bid_ask_spread_ratio:
            continue
        if int(quote["volume"]) < config.min_volume:
            continue
        if int(quote["open_interest"]) < config.min_open_interest:
            continue
        candidates.append(quote)
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda quote: (
            abs(float(quote["delta"]) - config.target_delta),
            abs(int(quote["dte"]) - config.dte_days),
            _spread_ratio(quote),
            -int(quote["open_interest"]),
        ),
    )


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _call_price(
    spot: float,
    strike: float,
    years_to_expiry: float,
    *,
    risk_free_rate: float,
    dividend_yield: float,
    volatility: float,
) -> float:
    if years_to_expiry <= 0.0:
        return max(0.0, spot - strike)
    sigma = max(0.01, float(volatility))
    root_t = math.sqrt(years_to_expiry)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate - dividend_yield + 0.5 * sigma * sigma) * years_to_expiry
    ) / (sigma * root_t)
    d2 = d1 - sigma * root_t
    return spot * math.exp(-dividend_yield * years_to_expiry) * _normal_cdf(d1) - strike * math.exp(
        -risk_free_rate * years_to_expiry
    ) * _normal_cdf(d2)


def _call_delta(
    spot: float,
    strike: float,
    years_to_expiry: float,
    *,
    risk_free_rate: float,
    dividend_yield: float,
    volatility: float,
) -> float:
    sigma = max(0.01, float(volatility))
    root_t = math.sqrt(max(years_to_expiry, 1e-6))
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate - dividend_yield + 0.5 * sigma * sigma) * years_to_expiry
    ) / (sigma * root_t)
    return math.exp(-dividend_yield * years_to_expiry) * _normal_cdf(d1)


def _strike_for_delta(
    spot: float,
    years_to_expiry: float,
    *,
    target_delta: float,
    risk_free_rate: float,
    dividend_yield: float,
    volatility: float,
) -> float:
    low = spot * 0.2
    high = spot * 2.5
    for _ in range(80):
        mid = (low + high) / 2.0
        delta = _call_delta(
            spot,
            mid,
            years_to_expiry,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            volatility=volatility,
        )
        if delta > target_delta:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def _annualized_realized_volatility(
    returns: list[float],
    *,
    index: int,
    window: int,
    floor: float,
    cap: float,
    multiplier: float,
) -> float:
    start = max(1, index - max(2, int(window)))
    samples = returns[start:index]
    if len(samples) < 20:
        return floor
    raw = pstdev(samples) * math.sqrt(TRADING_DAYS_PER_YEAR)
    return max(float(floor), min(float(cap), raw * float(multiplier)))


def _metrics(values: list[float]) -> dict[str, float]:
    if len(values) < 2 or values[0] <= 0.0:
        return {"total_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0, "volatility": 0.0}
    years = (len(values) - 1) / TRADING_DAYS_PER_YEAR
    total_return = values[-1] / values[0] - 1.0
    cagr = (values[-1] / values[0]) ** (1.0 / years) - 1.0 if years > 0.0 else 0.0
    peak = values[0]
    max_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value / peak - 1.0)
    daily_returns = [
        values[index] / values[index - 1] - 1.0
        for index in range(1, len(values))
        if values[index - 1] > 0.0
    ]
    volatility = pstdev(daily_returns) * math.sqrt(TRADING_DAYS_PER_YEAR) if len(daily_returns) > 2 else 0.0
    return {
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
    }


def _entry_gate_passed(prices: list[float], index: int, config: LeapsProxyConfig) -> bool:
    if not config.entry_gate_enabled:
        return True
    ma_window = max(2, int(config.ma_window))
    momentum_window = max(1, int(config.momentum_window))
    if index < min(ma_window, len(prices)) - 1 or index < momentum_window:
        return False
    ma_start = max(0, index - ma_window + 1)
    moving_average = sum(prices[ma_start : index + 1]) / (index - ma_start + 1)
    momentum = prices[index] / prices[index - momentum_window] - 1.0
    return prices[index] > moving_average and momentum > 0.0


def run_leaps_growth_overlay_proxy(
    price_history: Iterable[Mapping[str, object]],
    config: LeapsProxyConfig,
) -> dict[str, object]:
    series = close_series_for_symbol(price_history, symbol=config.underlier)
    if len(series) < 3:
        raise ValueError("price history must include at least three valid rows for the underlier")

    dates = [date for date, _close in series]
    prices = [close for _date, close in series]
    underlier_returns = [0.0] + [
        prices[index] / prices[index - 1] - 1.0 for index in range(1, len(prices))
    ]
    buy_hold_equity = [float(config.initial_equity)]
    overlay_equity = [float(config.initial_equity)]
    stock_value = float(config.initial_equity)
    daily_rows = [
        {
            "date": dates[0],
            "underlier": _normalize_symbol(config.underlier),
            "close": prices[0],
            "strategy_equity": overlay_equity[0],
            "buy_hold_equity": buy_hold_equity[0],
            "option_market_value": 0.0,
            "option_quantity": 0,
            "option_strike": "",
            "option_dte": 0,
            "event": "start",
        }
    ]
    trades: list[dict[str, object]] = []
    option_quantity = 0
    option_strike = 0.0
    option_expiration = ""
    option_expiry_index = -1
    option_market_value = 0.0
    option_cost_basis = 0.0
    banked_cash = 0.0
    entry_skip_count = 0
    expired_worthless_count = 0
    recovered_principal_count = 0

    for index in range(1, len(prices)):
        buy_hold_equity.append(buy_hold_equity[-1] * (1.0 + underlier_returns[index]))
        stock_value *= 1.0 + underlier_returns[index]
        event_parts: list[str] = []
        spot = prices[index]

        if option_quantity > 0:
            volatility = _annualized_realized_volatility(
                underlier_returns,
                index=index,
                window=config.realized_vol_window,
                floor=config.vol_floor,
                cap=config.vol_cap,
                multiplier=config.iv_multiplier,
            )
            remaining_dte = _calendar_dte(dates[index], option_expiration)
            years_to_expiry = remaining_dte / CALENDAR_DAYS_PER_YEAR
            option_market_value = (
                option_quantity
                * _call_price(
                    spot,
                    option_strike,
                    years_to_expiry,
                    risk_free_rate=config.risk_free_rate,
                    dividend_yield=config.dividend_yield,
                    volatility=volatility,
                )
                * config.contract_multiplier
            )

        if option_quantity > 1 and option_market_value >= 2.0 * max(option_cost_basis, 1.0):
            old_quantity = option_quantity
            option_contract_value = option_market_value / option_quantity
            recover_quantity = min(
                option_quantity - 1,
                max(1, math.ceil(option_cost_basis / max(option_contract_value, 1e-9))),
            )
            recovered = recover_quantity * option_contract_value
            banked_cash += recovered
            option_quantity -= recover_quantity
            option_market_value = option_quantity * option_contract_value
            option_cost_basis *= option_quantity / old_quantity
            recovered_principal_count += 1
            event_parts.append("recover_principal")
            trades.append(
                {
                    "date": dates[index],
                    "action": "recover_principal",
                    "quantity": recover_quantity,
                    "strike": option_strike,
                    "cash_value": round(recovered, 6),
                }
            )

        if option_quantity > 0 and index >= option_expiry_index:
            if option_market_value <= 1e-9:
                expired_worthless_count += 1
            banked_cash += option_market_value
            option_quantity = 0
            option_strike = 0.0
            option_expiration = ""
            option_market_value = 0.0
            option_cost_basis = 0.0
            event_parts.append("expire_or_settle")

        remaining_dte = _calendar_dte(dates[index], option_expiration) if option_expiration else 0
        should_open = option_quantity <= 0 or remaining_dte <= config.roll_dte_days
        if should_open and option_quantity > 0:
            banked_cash += option_market_value
            trades.append(
                {
                    "date": dates[index],
                    "action": "roll_close",
                    "quantity": option_quantity,
                    "strike": option_strike,
                    "cash_value": round(option_market_value, 6),
                }
            )
            option_quantity = 0
            option_strike = 0.0
            option_expiration = ""
            option_market_value = 0.0
            option_cost_basis = 0.0
            event_parts.append("roll_close")

        if option_quantity <= 0:
            if not _entry_gate_passed(prices, index, config):
                entry_skip_count += 1
            else:
                volatility = _annualized_realized_volatility(
                    underlier_returns,
                    index=index,
                    window=config.realized_vol_window,
                    floor=config.vol_floor,
                    cap=config.vol_cap,
                    multiplier=config.iv_multiplier,
                )
                expiration = _add_calendar_days(dates[index], config.dte_days)
                years_to_expiry = config.dte_days / CALENDAR_DAYS_PER_YEAR
                strike = _strike_for_delta(
                    spot,
                    years_to_expiry,
                    target_delta=config.target_delta,
                    risk_free_rate=config.risk_free_rate,
                    dividend_yield=config.dividend_yield,
                    volatility=volatility,
                )
                option_price = _call_price(
                    spot,
                    strike,
                    years_to_expiry,
                    risk_free_rate=config.risk_free_rate,
                    dividend_yield=config.dividend_yield,
                    volatility=volatility,
                )
                total_equity_before_open = stock_value + option_market_value + banked_cash
                premium_budget = total_equity_before_open * max(0.0, min(0.10, config.premium_budget_ratio))
                premium_budget = min(premium_budget, max(0.0, stock_value))
                quantity = (
                    int(premium_budget // (option_price * config.contract_multiplier))
                    if option_price > 0.0
                    else 0
                )
                if quantity > 0:
                    option_quantity = quantity
                    option_strike = strike
                    option_expiration = expiration
                    option_expiry_index = _expiry_index_for_dates(dates, option_expiration)
                    option_market_value = quantity * option_price * config.contract_multiplier
                    option_cost_basis = option_market_value
                    stock_value -= option_market_value
                    event_parts.append("open_leaps")
                    trades.append(
                        {
                            "date": dates[index],
                            "action": "open_leaps",
                            "quantity": option_quantity,
                            "strike": round(option_strike, 6),
                            "cash_value": round(option_market_value, 6),
                            "spot": spot,
                            "volatility": round(volatility, 6),
                        }
                    )
                else:
                    entry_skip_count += 1

        banked_cash *= math.exp(config.risk_free_rate / TRADING_DAYS_PER_YEAR)
        strategy_equity = stock_value + option_market_value + banked_cash
        overlay_equity.append(strategy_equity)
        daily_rows.append(
            {
                "date": dates[index],
                "underlier": _normalize_symbol(config.underlier),
                "close": spot,
                "strategy_equity": round(strategy_equity, 6),
                "buy_hold_equity": round(buy_hold_equity[-1], 6),
                "option_market_value": round(option_market_value, 6),
                "option_quantity": option_quantity,
                "option_strike": round(option_strike, 6) if option_strike else "",
                "option_dte": _calendar_dte(dates[index], option_expiration)
                if option_quantity > 0 and option_expiration
                else 0,
                "event": "|".join(event_parts),
            }
        )

    strategy_metrics = _metrics(overlay_equity)
    buy_hold_metrics = _metrics(buy_hold_equity)
    summary = {
        "Run": config.run_name,
        "Underlier": _normalize_symbol(config.underlier),
        "Data Mode": "black_scholes_proxy",
        "Promotion Evidence": False,
        "Start": dates[0],
        "End": dates[-1],
        "Total Return": strategy_metrics["total_return"],
        "CAGR": strategy_metrics["cagr"],
        "Max Drawdown": strategy_metrics["max_drawdown"],
        "Volatility": strategy_metrics["volatility"],
        "Final Equity": overlay_equity[-1],
        "Buy Hold CAGR": buy_hold_metrics["cagr"],
        "Buy Hold Max Drawdown": buy_hold_metrics["max_drawdown"],
        "Option Trade Count": len(trades),
        "Entry Skip Count": entry_skip_count,
        "Expired Worthless Count": expired_worthless_count,
        "Recovered Principal Count": recovered_principal_count,
        "Missing Quote Count": 0,
        "Proxy Warning": "Black-Scholes proxy; not promotion evidence without real option-chain bid/ask history.",
    }
    return {
        "config": asdict(config),
        "summary": summary,
        "daily_equity": daily_rows,
        "trades": trades,
    }


def _expiry_index_for_dates(dates: list[str], expiration: str) -> int:
    expiration_date = _date_value(expiration)
    for index, as_of in enumerate(dates):
        if _date_value(as_of) >= expiration_date:
            return index
    return len(dates)


def run_leaps_growth_overlay_option_chain_backtest(
    price_history: Iterable[Mapping[str, object]],
    option_chain_history: Iterable[Mapping[str, object]],
    config: LeapsProxyConfig,
) -> dict[str, object]:
    series = close_series_for_symbol(price_history, symbol=config.underlier)
    if len(series) < 3:
        raise ValueError("price history must include at least three valid rows for the underlier")

    quotes_by_date, quotes_by_contract_date = _index_option_quotes(
        option_chain_history,
        underlier=config.underlier,
    )
    if not quotes_by_date:
        raise ValueError("option chain history must include valid call quotes for the underlier")

    dates = [date for date, _close in series]
    prices = [close for _date, close in series]
    underlier_returns = [0.0] + [
        prices[index] / prices[index - 1] - 1.0 for index in range(1, len(prices))
    ]
    buy_hold_equity = [float(config.initial_equity)]
    overlay_equity = [float(config.initial_equity)]
    stock_value = float(config.initial_equity)
    daily_rows = [
        {
            "date": dates[0],
            "underlier": _normalize_symbol(config.underlier),
            "close": prices[0],
            "strategy_equity": overlay_equity[0],
            "buy_hold_equity": buy_hold_equity[0],
            "option_market_value": 0.0,
            "option_quantity": 0,
            "option_strike": "",
            "option_dte": 0,
            "event": "start",
        }
    ]
    trades: list[dict[str, object]] = []
    option_quantity = 0
    option_strike = 0.0
    option_expiration = ""
    option_expiry_index = -1
    option_market_value = 0.0
    option_cost_basis = 0.0
    option_contract_key = ""
    banked_cash = 0.0
    entry_skip_count = 0
    expired_worthless_count = 0
    recovered_principal_count = 0
    missing_quote_count = 0

    for index in range(1, len(prices)):
        as_of = dates[index]
        buy_hold_equity.append(buy_hold_equity[-1] * (1.0 + underlier_returns[index]))
        stock_value *= 1.0 + underlier_returns[index]
        event_parts: list[str] = []
        spot = prices[index]
        current_quote: Mapping[str, object] | None = None
        current_mark_available = False

        if option_quantity > 0:
            current_quote = quotes_by_contract_date.get((as_of, option_contract_key))
            if current_quote is not None:
                option_market_value = option_quantity * float(current_quote["bid"]) * config.contract_multiplier
                current_mark_available = True
            elif index >= option_expiry_index:
                option_market_value = option_quantity * max(0.0, spot - option_strike) * config.contract_multiplier
                current_mark_available = True
                missing_quote_count += 1
                event_parts.append("expiry_intrinsic_fallback")
            else:
                missing_quote_count += 1
                event_parts.append("missing_quote")

        if (
            option_quantity > 1
            and current_mark_available
            and option_market_value >= 2.0 * max(option_cost_basis, 1.0)
        ):
            old_quantity = option_quantity
            option_contract_value = option_market_value / option_quantity
            recover_quantity = min(
                option_quantity - 1,
                max(1, math.ceil(option_cost_basis / max(option_contract_value, 1e-9))),
            )
            recovered = recover_quantity * option_contract_value
            banked_cash += recovered
            option_quantity -= recover_quantity
            option_market_value = option_quantity * option_contract_value
            option_cost_basis *= option_quantity / old_quantity
            recovered_principal_count += 1
            event_parts.append("recover_principal")
            trades.append(
                {
                    "date": as_of,
                    "action": "recover_principal",
                    "quantity": recover_quantity,
                    "strike": option_strike,
                    "cash_value": round(recovered, 6),
                }
            )

        if option_quantity > 0 and index >= option_expiry_index:
            if option_market_value <= 1e-9:
                expired_worthless_count += 1
            banked_cash += option_market_value
            option_quantity = 0
            option_strike = 0.0
            option_expiration = ""
            option_market_value = 0.0
            option_cost_basis = 0.0
            option_contract_key = ""
            event_parts.append("expire_or_settle")

        should_roll = (
            option_quantity > 0
            and current_mark_available
            and option_expiration
            and _calendar_dte(as_of, option_expiration) <= config.roll_dte_days
        )
        if should_roll:
            banked_cash += option_market_value
            trades.append(
                {
                    "date": as_of,
                    "action": "roll_close",
                    "quantity": option_quantity,
                    "strike": option_strike,
                    "cash_value": round(option_market_value, 6),
                }
            )
            option_quantity = 0
            option_strike = 0.0
            option_expiration = ""
            option_market_value = 0.0
            option_cost_basis = 0.0
            option_contract_key = ""
            event_parts.append("roll_close")

        if option_quantity <= 0:
            if not _entry_gate_passed(prices, index, config):
                entry_skip_count += 1
            else:
                entry_quote = _select_entry_quote(
                    quotes_by_date.get(as_of, ()),
                    config=config,
                )
                if entry_quote is None:
                    entry_skip_count += 1
                else:
                    total_equity_before_open = stock_value + option_market_value + banked_cash
                    premium_budget = total_equity_before_open * max(
                        0.0,
                        min(0.10, config.premium_budget_ratio),
                    )
                    premium_budget = min(premium_budget, max(0.0, stock_value))
                    ask = float(entry_quote["ask"])
                    bid = float(entry_quote["bid"])
                    quantity = (
                        int(premium_budget // (ask * config.contract_multiplier))
                        if ask > 0.0
                        else 0
                    )
                    if quantity > 0:
                        option_quantity = quantity
                        option_strike = float(entry_quote["strike"])
                        option_expiration = str(entry_quote["expiration"])
                        option_expiry_index = _expiry_index_for_dates(dates, option_expiration)
                        option_contract_key = str(entry_quote["contract_key"])
                        option_cost_basis = quantity * ask * config.contract_multiplier
                        option_market_value = quantity * bid * config.contract_multiplier
                        stock_value -= option_cost_basis
                        event_parts.append("open_leaps")
                        trades.append(
                            {
                                "date": as_of,
                                "action": "open_leaps",
                                "quantity": option_quantity,
                                "strike": round(option_strike, 6),
                                "cash_value": round(option_cost_basis, 6),
                                "spot": spot,
                                "volatility": "",
                            }
                        )
                    else:
                        entry_skip_count += 1

        banked_cash *= math.exp(config.risk_free_rate / TRADING_DAYS_PER_YEAR)
        strategy_equity = stock_value + option_market_value + banked_cash
        overlay_equity.append(strategy_equity)
        daily_rows.append(
            {
                "date": as_of,
                "underlier": _normalize_symbol(config.underlier),
                "close": spot,
                "strategy_equity": round(strategy_equity, 6),
                "buy_hold_equity": round(buy_hold_equity[-1], 6),
                "option_market_value": round(option_market_value, 6),
                "option_quantity": option_quantity,
                "option_strike": round(option_strike, 6) if option_strike else "",
                "option_dte": _calendar_dte(as_of, option_expiration)
                if option_quantity > 0 and option_expiration
                else 0,
                "event": "|".join(event_parts),
            }
        )

    strategy_metrics = _metrics(overlay_equity)
    buy_hold_metrics = _metrics(buy_hold_equity)
    summary = {
        "Run": config.run_name,
        "Underlier": _normalize_symbol(config.underlier),
        "Data Mode": "historical_option_chain",
        "Promotion Evidence": missing_quote_count == 0 and bool(trades),
        "Start": dates[0],
        "End": dates[-1],
        "Total Return": strategy_metrics["total_return"],
        "CAGR": strategy_metrics["cagr"],
        "Max Drawdown": strategy_metrics["max_drawdown"],
        "Volatility": strategy_metrics["volatility"],
        "Final Equity": overlay_equity[-1],
        "Buy Hold CAGR": buy_hold_metrics["cagr"],
        "Buy Hold Max Drawdown": buy_hold_metrics["max_drawdown"],
        "Option Trade Count": len(trades),
        "Entry Skip Count": entry_skip_count,
        "Expired Worthless Count": expired_worthless_count,
        "Recovered Principal Count": recovered_principal_count,
        "Missing Quote Count": missing_quote_count,
        "Proxy Warning": "",
    }
    return {
        "config": asdict(config),
        "summary": summary,
        "daily_equity": daily_rows,
        "trades": trades,
    }


def write_research_outputs(result: Mapping[str, object], output_dir: str | Path) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary = result["summary"]
    daily_equity = result["daily_equity"]
    trades = result["trades"]

    with (output_path / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerow({key: summary.get(key, "") for key in SUMMARY_FIELDS})

    daily_fields = tuple(daily_equity[0]) if daily_equity else ()
    with (output_path / "daily_equity.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=daily_fields)
        writer.writeheader()
        writer.writerows(daily_equity)

    trade_fields = ("date", "action", "quantity", "strike", "cash_value", "spot", "volatility")
    with (output_path / "trades.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=trade_fields)
        writer.writeheader()
        for trade in trades:
            writer.writerow({key: trade.get(key, "") for key in trade_fields})

    manifest = {
        "schema_version": "index_leaps_growth_overlay_research.v1",
        "config": result["config"],
        "outputs": {
            "summary": "summary.csv",
            "daily_equity": "daily_equity.csv",
            "trades": "trades.csv",
        },
        "promotion_evidence": bool(summary.get("Promotion Evidence")),
        "data_mode": summary.get("Data Mode", ""),
        "warning": summary.get("Proxy Warning", ""),
    }
    (output_path / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run proxy or historical option-chain research for index LEAPS growth overlays."
    )
    parser.add_argument("--prices", required=True, help="CSV with symbol, as_of/date, close columns.")
    parser.add_argument("--option-chain", help="Optional historical option-chain CSV for real bid/ask backtests.")
    parser.add_argument("--mode", choices=("auto", "proxy", "option-chain"), default="auto")
    parser.add_argument("--underlier", default="QQQ")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-name", default="")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY)
    parser.add_argument("--premium-budget-ratio", type=float, default=DEFAULT_PREMIUM_BUDGET_RATIO)
    parser.add_argument("--target-delta", type=float, default=DEFAULT_TARGET_DELTA)
    parser.add_argument("--dte-days", type=int, default=DEFAULT_DTE_DAYS)
    parser.add_argument("--roll-dte-days", type=int, default=DEFAULT_ROLL_DTE_DAYS)
    parser.add_argument("--risk-free-rate", type=float, default=DEFAULT_RISK_FREE_RATE)
    parser.add_argument("--dividend-yield", type=float, default=DEFAULT_DIVIDEND_YIELD)
    parser.add_argument("--iv-multiplier", type=float, default=DEFAULT_IV_MULTIPLIER)
    parser.add_argument("--vol-floor", type=float, default=DEFAULT_VOL_FLOOR)
    parser.add_argument("--vol-cap", type=float, default=DEFAULT_VOL_CAP)
    parser.add_argument("--ma-window", type=int, default=DEFAULT_MA_WINDOW)
    parser.add_argument("--momentum-window", type=int, default=DEFAULT_MOMENTUM_WINDOW)
    parser.add_argument("--contract-multiplier", type=int, default=DEFAULT_CONTRACT_MULTIPLIER)
    parser.add_argument("--min-dte-days", type=int, default=DEFAULT_MIN_DTE_DAYS)
    parser.add_argument("--max-dte-days", type=int, default=DEFAULT_MAX_DTE_DAYS)
    parser.add_argument("--max-bid-ask-spread-ratio", type=float, default=DEFAULT_MAX_BID_ASK_SPREAD_RATIO)
    parser.add_argument("--min-volume", type=int, default=0)
    parser.add_argument("--min-open-interest", type=int, default=0)
    parser.add_argument("--disable-entry-gate", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    underlier = _normalize_symbol(args.underlier)
    run_name = args.run_name or f"{underlier.lower()}_leaps_growth_proxy"
    config = LeapsProxyConfig(
        underlier=underlier,
        run_name=run_name,
        initial_equity=args.initial_equity,
        premium_budget_ratio=args.premium_budget_ratio,
        target_delta=args.target_delta,
        dte_days=args.dte_days,
        roll_dte_days=args.roll_dte_days,
        risk_free_rate=args.risk_free_rate,
        dividend_yield=args.dividend_yield,
        iv_multiplier=args.iv_multiplier,
        vol_floor=args.vol_floor,
        vol_cap=args.vol_cap,
        entry_gate_enabled=not args.disable_entry_gate,
        ma_window=args.ma_window,
        momentum_window=args.momentum_window,
        contract_multiplier=args.contract_multiplier,
        min_dte_days=args.min_dte_days,
        max_dte_days=args.max_dte_days,
        max_bid_ask_spread_ratio=args.max_bid_ask_spread_ratio,
        min_volume=args.min_volume,
        min_open_interest=args.min_open_interest,
    )
    price_history = load_price_history(args.prices)
    use_option_chain = args.mode == "option-chain" or (args.mode == "auto" and args.option_chain)
    if use_option_chain:
        if not args.option_chain:
            raise ValueError("--option-chain is required when --mode=option-chain")
        result = run_leaps_growth_overlay_option_chain_backtest(
            price_history,
            load_option_chain_history(args.option_chain),
            config,
        )
    else:
        result = run_leaps_growth_overlay_proxy(price_history, config)
    write_research_outputs(result, args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
