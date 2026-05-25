from __future__ import annotations

import argparse
import inspect
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest
from us_equity_strategies.strategies.soxl_soxx_trend_income import build_rebalance_plan

from .artifacts import write_json
from .yfinance_prices import download_price_history_with_proxy_candidates, load_proxy_candidates

PROFILE = "soxl_soxx_trend_income"
MANAGED_SYMBOLS = ("SOXL", "SOXX", "BOXX", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI")
DEFAULT_INITIAL_EQUITY_USD = 100_000.0
DEFAULT_PRICE_START = "2023-01-01"
DEFAULT_BACKTEST_START = "2024-01-30"
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_RSI_WINDOW = 14
DEFAULT_BOLLINGER_WINDOW = 20
DEFAULT_BOLLINGER_STD = 2.0
DEFAULT_DOWNLOAD_SYMBOLS = list(MANAGED_SYMBOLS)
DEFAULT_OUTPUT_COLUMNS = (
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Chandelier Stops",
    "SOXL Delever Stops",
    "Final Equity",
)


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_overlay_kind(value: str | None) -> str:
    text = str(value or "none").strip().lower().replace("-", "_")
    aliases = {
        "off": "none",
        "disabled": "none",
        "chandelier_stop": "chandelier",
        "vol": "volatility",
        "realized_volatility": "volatility",
        "rolling_drawdown": "drawdown",
        "ret": "momentum",
    }
    return aliases.get(text, text)


def _clamp_ratio(value: float, *, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        result = float(default)
    if not np.isfinite(result):
        result = float(default)
    return max(0.0, min(1.0, result))


def _build_price_frame(price_history) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    for column in ("open", "high", "low", "volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[frame["symbol"].ne("")].dropna(subset=["as_of", "close"])
    output_columns = ["symbol", "as_of", "close"]
    output_columns.extend(column for column in ("open", "high", "low", "volume") if column in frame.columns)
    return (
        frame.loc[:, output_columns]
        .drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def _build_close_matrix(price_history: pd.DataFrame) -> pd.DataFrame:
    return _build_field_matrix(price_history, "close")


def _empty_matrix_index(price_history: pd.DataFrame) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(price_history["as_of"]).dropna().sort_values().unique())


def _build_field_matrix(price_history: pd.DataFrame, field: str) -> pd.DataFrame:
    if field not in price_history.columns:
        return pd.DataFrame(index=_empty_matrix_index(price_history))
    values = pd.to_numeric(price_history[field], errors="coerce")
    frame = price_history.assign(**{field: values})
    frame = frame.dropna(subset=[field])
    if frame.empty:
        return pd.DataFrame(index=_empty_matrix_index(price_history))
    field_matrix = (
        frame.pivot_table(index="as_of", columns="symbol", values=field, aggfunc="last")
        .sort_index()
        .ffill()
    )
    return field_matrix


def _build_chandelier_stop_history(
    price_history: pd.DataFrame,
    *,
    symbol: str,
    window: int,
    atr_multiple: float,
) -> pd.DataFrame:
    symbol = _normalize_symbol(symbol)
    close_matrix = _build_close_matrix(price_history)
    if symbol not in close_matrix.columns:
        return pd.DataFrame(
            columns=["close", "high", "low", "true_range", "atr", "stop_line", "triggered"],
            index=close_matrix.index,
        )
    high_matrix = _build_field_matrix(price_history, "high").reindex(close_matrix.index).ffill()
    low_matrix = _build_field_matrix(price_history, "low").reindex(close_matrix.index).ffill()
    close = close_matrix[symbol].astype(float)
    high = high_matrix[symbol].astype(float) if symbol in high_matrix.columns else close
    low = low_matrix[symbol].astype(float) if symbol in low_matrix.columns else close
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    effective_window = max(2, int(window))
    atr = true_range.rolling(effective_window, min_periods=effective_window).mean()
    rolling_high = high.rolling(effective_window, min_periods=effective_window).max()
    stop_line = rolling_high - (float(atr_multiple) * atr)
    return pd.DataFrame(
        {
            "close": close,
            "high": high,
            "low": low,
            "true_range": true_range,
            "atr": atr,
            "stop_line": stop_line,
            "triggered": close < stop_line,
        }
    )


def _build_soxl_delever_overlay_history(
    price_history: pd.DataFrame,
    *,
    kind: str,
    symbol: str,
    window: int,
    threshold: float | None,
    atr_multiple: float,
) -> pd.DataFrame:
    kind = _normalize_overlay_kind(kind)
    if kind == "chandelier":
        history = _build_chandelier_stop_history(
            price_history,
            symbol=symbol,
            window=window,
            atr_multiple=atr_multiple,
        )
        history["kind"] = "chandelier"
        history["metric"] = history["close"] - history["stop_line"]
        history["threshold"] = 0.0
        return history

    symbol = _normalize_symbol(symbol)
    close_matrix = _build_close_matrix(price_history)
    if symbol not in close_matrix.columns:
        return pd.DataFrame(
            columns=["close", "metric", "threshold", "triggered", "kind"],
            index=close_matrix.index,
        )

    close = close_matrix[symbol].astype(float)
    effective_window = max(2, int(window))
    if kind == "drawdown":
        effective_threshold = float(threshold if threshold is not None else -0.05)
        rolling_high = close.rolling(effective_window, min_periods=effective_window).max()
        metric = close / rolling_high - 1.0
        triggered = metric <= effective_threshold
    elif kind == "volatility":
        effective_threshold = float(threshold if threshold is not None else 0.45)
        metric = close.pct_change().rolling(effective_window, min_periods=effective_window).std(ddof=0) * np.sqrt(252)
        triggered = metric >= effective_threshold
    elif kind == "momentum":
        effective_threshold = float(threshold if threshold is not None else -0.06)
        metric = close.pct_change(effective_window)
        triggered = metric <= effective_threshold
    else:
        raise ValueError("unsupported SOXL delever overlay kind")

    return pd.DataFrame(
        {
            "close": close,
            "metric": metric,
            "threshold": effective_threshold,
            "triggered": triggered,
            "kind": kind,
        }
    )


def _build_indicator_history(close_matrix: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return build_indicator_history(
        close_matrix,
        rsi_window=DEFAULT_RSI_WINDOW,
        bollinger_window=DEFAULT_BOLLINGER_WINDOW,
        bollinger_std=DEFAULT_BOLLINGER_STD,
    )


def _build_rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / int(window), min_periods=int(window), adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / int(window), min_periods=int(window), adjust=False).mean()
    relative_strength = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + relative_strength))
    rsi = rsi.where(avg_loss.ne(0.0), 100.0)
    rsi = rsi.where(avg_gain.ne(0.0), 0.0)
    return rsi


def build_indicator_history(
    close_matrix: pd.DataFrame,
    *,
    rsi_window: int = DEFAULT_RSI_WINDOW,
    bollinger_window: int = DEFAULT_BOLLINGER_WINDOW,
    bollinger_std: float = DEFAULT_BOLLINGER_STD,
    dynamic_rsi_quantile_window: int | None = None,
    dynamic_rsi_quantile: float | None = None,
    dynamic_rsi_floor: float | None = None,
) -> dict[str, pd.DataFrame]:
    indicators: dict[str, pd.DataFrame] = {}
    for symbol in ("SOXL", "SOXX"):
        if symbol not in close_matrix.columns:
            continue
        close = pd.to_numeric(close_matrix[symbol], errors="coerce")
        history = pd.DataFrame(
            {
                "price": close,
                "ma_trend": close.rolling(int(soxl_soxx_trend_income_manifest.default_config["trend_ma_window"])).mean(),
            },
            index=close.index,
        )
        if symbol == "SOXX":
            ma20 = close.rolling(20).mean()
            daily_returns = close.pct_change(fill_method=None)
            realized_volatility_10 = daily_returns.rolling(10).std() * np.sqrt(252)
            realized_volatility_20 = daily_returns.rolling(20).std() * np.sqrt(252)
            history["ma20"] = ma20
            history["ma20_slope"] = ma20.diff()
            history["realized_volatility"] = realized_volatility_20
            history["realized_volatility_10"] = realized_volatility_10
            history["realized_volatility_20"] = realized_volatility_20
            rsi = _build_rsi(close, int(rsi_window))
            history["rsi14_raw"] = rsi
            history["rsi14"] = rsi
            if dynamic_rsi_quantile_window is not None or dynamic_rsi_quantile is not None:
                quantile_window = int(dynamic_rsi_quantile_window or 252)
                quantile = float(dynamic_rsi_quantile if dynamic_rsi_quantile is not None else 0.90)
                floor = float(dynamic_rsi_floor if dynamic_rsi_floor is not None else 70.0)
                min_periods = max(60, min(quantile_window, 126) // 2)
                dynamic_threshold = (
                    rsi.rolling(quantile_window, min_periods=min_periods)
                    .quantile(quantile)
                    .shift(1)
                    .fillna(floor)
                    .clip(lower=floor)
                )
                history["rsi14_dynamic_threshold"] = dynamic_threshold
            bollinger_mid = close.rolling(int(bollinger_window)).mean()
            bollinger_stddev = close.rolling(int(bollinger_window)).std(ddof=0)
            history["bb_mid"] = bollinger_mid
            history["bb_upper"] = bollinger_mid + (float(bollinger_std) * bollinger_stddev)
            history["bb_lower"] = bollinger_mid - (float(bollinger_std) * bollinger_stddev)
        indicators[symbol.lower()] = history
    return indicators


def _strategy_kwargs(overrides: Mapping[str, object] | None = None) -> dict[str, object]:
    config = soxl_soxx_trend_income_manifest.default_config
    kwargs = {
        "trend_ma_window": int(config["trend_ma_window"]),
        "cash_reserve_ratio": float(config["cash_reserve_ratio"]),
        "min_trade_ratio": float(config["min_trade_ratio"]),
        "min_trade_floor": float(config["min_trade_floor"]),
        "rebalance_threshold_ratio": float(config["rebalance_threshold_ratio"]),
        "small_account_deploy_ratio": float(config.get("small_account_deploy_ratio", 0.6)),
        "mid_account_deploy_ratio": float(config.get("mid_account_deploy_ratio", 0.57)),
        "large_account_deploy_ratio": float(config.get("large_account_deploy_ratio", 0.5)),
        "trade_layer_decay_coeff": float(config.get("trade_layer_decay_coeff", 0.04)),
        "income_layer_enabled": bool(config.get("income_layer_enabled", True)),
        "income_layer_start_usd": float(config["income_layer_start_usd"]),
        "income_layer_max_ratio": float(config["income_layer_max_ratio"]),
        "income_layer_ratio_mode": str(config.get("income_layer_ratio_mode", "linear_cap")),
        "income_layer_log_growth_factor": float(config.get("income_layer_log_growth_factor", 0.70)),
        "income_layer_stress_drawdown_ratio": float(config.get("income_layer_stress_drawdown_ratio", 0.30)),
        "income_layer_base_loss_budget_ratio": float(config.get("income_layer_base_loss_budget_ratio", 0.08)),
        "income_layer_min_loss_budget_ratio": float(config.get("income_layer_min_loss_budget_ratio", 0.06)),
        "income_layer_loss_budget_decay_per_double": float(
            config.get("income_layer_loss_budget_decay_per_double", 0.01)
        ),
        "income_layer_qqqi_weight": float(config["income_layer_qqqi_weight"]),
        "income_layer_spyi_weight": float(config["income_layer_spyi_weight"]),
        "income_layer_allocations": dict(config.get("income_layer_allocations", {})),
        "trend_entry_buffer": float(config.get("trend_entry_buffer", 0.03)),
        "trend_mid_buffer": float(config.get("trend_mid_buffer", 0.06)),
        "trend_exit_buffer": float(config.get("trend_exit_buffer", 0.03)),
        "attack_allocation_mode": str(config.get("attack_allocation_mode", "soxx_gate_tiered_blend")),
        "blend_gate_trend_source": str(config.get("blend_gate_trend_source", "SOXX")),
        "blend_gate_soxl_weight": float(config.get("blend_gate_soxl_weight", 0.75)),
        "blend_gate_mid_soxl_weight": float(config.get("blend_gate_mid_soxl_weight", 0.65)),
        "blend_gate_active_soxx_weight": float(config.get("blend_gate_active_soxx_weight", 0.20)),
        "blend_gate_defensive_soxx_weight": float(config.get("blend_gate_defensive_soxx_weight", 0.15)),
        "blend_gate_rsi_cap_enabled": bool(config.get("blend_gate_rsi_cap_enabled", False)),
        "blend_gate_rsi_threshold": float(config.get("blend_gate_rsi_threshold", 70.0)),
        "blend_gate_dynamic_rsi_threshold_enabled": bool(
            config.get("blend_gate_dynamic_rsi_threshold_enabled", False)
        ),
        "blend_gate_bollinger_cap_enabled": bool(config.get("blend_gate_bollinger_cap_enabled", False)),
        "blend_gate_overlay_stack_triggers": bool(config.get("blend_gate_overlay_stack_triggers", False)),
        "blend_gate_volatility_delever_enabled": bool(
            config.get("blend_gate_volatility_delever_enabled", False)
        ),
        "blend_gate_volatility_delever_symbol": str(
            config.get("blend_gate_volatility_delever_symbol", "SOXX")
        ),
        "blend_gate_volatility_delever_window": int(
            config.get("blend_gate_volatility_delever_window", 10)
        ),
        "blend_gate_volatility_delever_threshold": float(
            config.get("blend_gate_volatility_delever_threshold", 0.55)
        ),
        "blend_gate_volatility_delever_retention_ratio": float(
            config.get("blend_gate_volatility_delever_retention_ratio", 0.0)
        ),
        "blend_gate_volatility_delever_redirect_symbol": str(
            config.get("blend_gate_volatility_delever_redirect_symbol", "SOXX")
        ),
    }
    for key, value in dict(overrides or {}).items():
        if value is not None and key in kwargs:
            kwargs[key] = value
    return kwargs


def _call_strategy_kwargs(overrides: Mapping[str, object] | None = None) -> dict[str, object]:
    kwargs = _strategy_kwargs(overrides)
    supported = set(inspect.signature(build_rebalance_plan).parameters)
    supported.discard("indicators")
    supported.discard("account_state")
    supported.discard("translator")
    return {key: value for key, value in kwargs.items() if key in supported}


def _indicator_snapshot_at(
    indicator_history: Mapping[str, pd.DataFrame],
    as_of: pd.Timestamp,
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for symbol, frame in indicator_history.items():
        if as_of not in frame.index:
            continue
        row = frame.loc[as_of]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[-1]
        payload: dict[str, float] = {}
        for key in row.index:
            value = row[key]
            if pd.notna(value):
                payload[key] = float(value)
        result[symbol] = payload
    return result


def _build_account_state(
    *,
    weights: Mapping[str, float],
    equity: float,
    close_prices: Mapping[str, float],
    cash_symbol: str = "BOXX",
) -> dict[str, object]:
    market_values = {symbol: float(equity) * float(weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
    quantities = {}
    sellable_quantities = {}
    for symbol in MANAGED_SYMBOLS:
        price = float(close_prices.get(symbol, 0.0) or 0.0)
        market_value = market_values.get(symbol, 0.0)
        qty = market_value / price if price > 0 else 0.0
        quantities[symbol] = int(round(qty))
        sellable_quantities[symbol] = int(round(qty))
    return {
        "available_cash": float(equity),
        "market_values": market_values,
        "quantities": quantities,
        "sellable_quantities": sellable_quantities,
        "total_strategy_equity": float(equity),
        "cash_sweep_symbol": cash_symbol,
    }


def _execute_rebalance(
    *,
    current_weights: Mapping[str, float],
    target_values: Mapping[str, float],
    equity: float,
    threshold_value: float,
    current_min_trade: float,
    turnover_cost_bps: float,
) -> tuple[dict[str, float], float, float]:
    current_market_values = {symbol: float(equity) * float(current_weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
    next_market_values = dict(current_market_values)

    income_symbols = tuple(symbol for symbol in MANAGED_SYMBOLS if symbol not in {"SOXL", "SOXX", "BOXX"})
    sell_order = ("SOXL", "SOXX", *income_symbols, "BOXX")
    buy_order = (*income_symbols, "SOXL", "SOXX", "BOXX")

    cash = float(equity) - sum(current_market_values.values())
    if abs(cash) < 1e-9:
        cash = 0.0

    for symbol in sell_order:
        current = current_market_values.get(symbol, 0.0)
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff >= 0:
            continue
        if abs(diff) <= threshold_value or abs(diff) <= current_min_trade:
            continue
        next_market_values[symbol] = target
        cash -= diff

    for symbol in buy_order:
        current = next_market_values.get(symbol, current_market_values.get(symbol, 0.0))
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff <= 0:
            continue
        if diff <= threshold_value or diff <= current_min_trade:
            continue
        buy_value = min(diff, cash)
        if buy_value <= 0:
            continue
        next_market_values[symbol] = current + buy_value
        cash -= buy_value

    new_equity_before_cost = cash + sum(next_market_values.values())
    if new_equity_before_cost <= 0:
        return dict(current_weights), 0.0, 0.0

    turnover = 0.5 * sum(
        abs(float(next_market_values.get(symbol, 0.0)) - float(current_market_values.get(symbol, 0.0)))
        for symbol in MANAGED_SYMBOLS
    ) / float(equity)
    cost = float(equity) * turnover * (float(turnover_cost_bps) / 10_000.0)
    cash = max(0.0, cash - cost)
    new_equity = cash + sum(next_market_values.values())
    if new_equity <= 0:
        return dict(current_weights), 0.0, 0.0

    new_weights = {symbol: float(next_market_values.get(symbol, 0.0)) / new_equity for symbol in MANAGED_SYMBOLS}
    new_weights["__cash__"] = cash / new_equity
    return new_weights, turnover, new_equity


def _summarize_returns(
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
) -> dict[str, float | str]:
    returns = portfolio_returns.dropna()
    if returns.empty:
        raise RuntimeError("No portfolio returns to summarize")

    equity_curve = (1.0 + returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    sharpe = float(returns.mean() / std * np.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    changes = weights_history.fillna(0.0).diff().fillna(0.0)
    if not changes.empty:
        changes.iloc[0] = 0.0
    daily_turnover = 0.5 * changes.abs().sum(axis=1)
    rebalances_per_year = float((daily_turnover > 1e-12).sum() / years)
    turnover_per_year = float(daily_turnover.sum() / years)
    stock_columns = [column for column in weights_history.columns if column not in {"BOXX", "__cash__"}]
    avg_stock_exposure = (
        float(weights_history[stock_columns].fillna(0.0).sum(axis=1).mean()) if stock_columns else 0.0
    )

    return {
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Rebalances/Year": rebalances_per_year,
        "Turnover/Year": turnover_per_year,
        "Avg Stock Exposure": avg_stock_exposure,
        "Final Equity": float(equity_curve.iloc[-1]),
    }


def run_backtest(
    price_history,
    *,
    initial_equity: float = DEFAULT_INITIAL_EQUITY_USD,
    start_date: str = DEFAULT_BACKTEST_START,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    blend_gate_rsi_cap_enabled: bool | None = None,
    blend_gate_rsi_threshold: float | None = None,
    blend_gate_bollinger_cap_enabled: bool | None = None,
    blend_gate_overlay_stack_triggers: bool | None = None,
    dynamic_rsi_quantile_window: int | None = None,
    dynamic_rsi_quantile: float | None = None,
    dynamic_rsi_floor: float | None = None,
    disable_income_layer: bool = False,
    chandelier_stop_enabled: bool = False,
    chandelier_stop_symbol: str = "SOXX",
    chandelier_window: int = 22,
    chandelier_atr_multiple: float = 3.0,
    soxl_delever_overlay_kind: str = "none",
    soxl_delever_overlay_symbol: str | None = None,
    soxl_delever_overlay_window: int | None = None,
    soxl_delever_overlay_threshold: float | None = None,
    soxl_delever_overlay_atr_multiple: float | None = None,
    soxl_delever_overlay_retention_ratio: float = 0.0,
    soxl_delever_overlay_redirect_symbol: str = "BOXX",
    strategy_overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    prices = _build_price_frame(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")

    strategy_overrides = dict(strategy_overrides or {})
    if blend_gate_rsi_cap_enabled is not None:
        strategy_overrides["blend_gate_rsi_cap_enabled"] = bool(blend_gate_rsi_cap_enabled)
    if blend_gate_bollinger_cap_enabled is not None:
        strategy_overrides["blend_gate_bollinger_cap_enabled"] = bool(blend_gate_bollinger_cap_enabled)
    if blend_gate_overlay_stack_triggers is not None:
        strategy_overrides["blend_gate_overlay_stack_triggers"] = bool(blend_gate_overlay_stack_triggers)
    if disable_income_layer:
        strategy_overrides["income_layer_enabled"] = False
        strategy_overrides["income_layer_start_usd"] = 1e18
        strategy_overrides["income_layer_max_ratio"] = 0.0

    dynamic_rsi_enabled = dynamic_rsi_quantile_window is not None or dynamic_rsi_quantile is not None
    rsi_threshold = (
        float(blend_gate_rsi_threshold)
        if blend_gate_rsi_threshold is not None
        else float(soxl_soxx_trend_income_manifest.default_config.get("blend_gate_rsi_threshold", 70.0))
    )
    dynamic_floor = float(dynamic_rsi_floor) if dynamic_rsi_floor is not None else rsi_threshold
    strategy_overrides["blend_gate_rsi_threshold"] = dynamic_floor if dynamic_rsi_enabled else rsi_threshold
    if dynamic_rsi_enabled:
        strategy_overrides["blend_gate_dynamic_rsi_threshold_enabled"] = True

    close_matrix = _build_close_matrix(prices)
    overlay_kind = _normalize_overlay_kind(soxl_delever_overlay_kind)
    if chandelier_stop_enabled and overlay_kind == "none":
        overlay_kind = "chandelier"
    soxl_delever_enabled = overlay_kind != "none"
    overlay_symbol = _normalize_symbol(soxl_delever_overlay_symbol or chandelier_stop_symbol or "SOXX")
    overlay_window = int(soxl_delever_overlay_window or chandelier_window)
    overlay_atr_multiple = float(
        soxl_delever_overlay_atr_multiple
        if soxl_delever_overlay_atr_multiple is not None
        else chandelier_atr_multiple
    )
    overlay_retention_ratio = _clamp_ratio(soxl_delever_overlay_retention_ratio)
    overlay_redirect_symbol = _normalize_symbol(soxl_delever_overlay_redirect_symbol or "BOXX")
    if overlay_redirect_symbol not in MANAGED_SYMBOLS:
        expected = ", ".join(MANAGED_SYMBOLS)
        raise ValueError(f"unsupported SOXL delever redirect symbol {overlay_redirect_symbol!r}; expected one of {expected}")
    if soxl_delever_enabled:
        strategy_overrides["blend_gate_volatility_delever_enabled"] = False

    delever_history = (
        _build_soxl_delever_overlay_history(
            prices,
            kind=overlay_kind,
            symbol=overlay_symbol,
            window=overlay_window,
            threshold=soxl_delever_overlay_threshold,
            atr_multiple=overlay_atr_multiple,
        )
        if soxl_delever_enabled
        else pd.DataFrame(index=close_matrix.index)
    )
    indicator_history = build_indicator_history(
        close_matrix,
        rsi_window=DEFAULT_RSI_WINDOW,
        bollinger_window=DEFAULT_BOLLINGER_WINDOW,
        bollinger_std=DEFAULT_BOLLINGER_STD,
        dynamic_rsi_quantile_window=dynamic_rsi_quantile_window if dynamic_rsi_enabled else None,
        dynamic_rsi_quantile=dynamic_rsi_quantile if dynamic_rsi_enabled else None,
        dynamic_rsi_floor=dynamic_floor if dynamic_rsi_enabled else None,
    )
    index = close_matrix.index
    index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")

    backtest_index = index[:-1]
    weights_history = pd.DataFrame(0.0, index=index, columns=[*MANAGED_SYMBOLS, "__cash__"])
    portfolio_returns = pd.Series(index=index, dtype=float, name="portfolio_return")
    turnover_history = pd.Series(index=index, dtype=float, name="turnover")
    signal_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    soxl_delever_stop_count = 0

    first_prices = close_matrix.loc[index[0]]
    initial_boxx_price = float(first_prices.get("BOXX", np.nan))
    if not np.isfinite(initial_boxx_price) or initial_boxx_price <= 0:
        raise RuntimeError("BOXX price unavailable at backtest start")
    initial_weights = {symbol: 0.0 for symbol in MANAGED_SYMBOLS}
    initial_weights["BOXX"] = 1.0
    current_weights = dict(initial_weights)
    current_equity = float(initial_equity)

    for as_of in backtest_index:
        next_as_of = index[index.get_loc(as_of) + 1]
        close_row = close_matrix.loc[as_of]
        next_close_row = close_matrix.loc[next_as_of]
        if pd.isna(close_row.get("SOXL")) or pd.isna(close_row.get("SOXX")):
            continue

        account_state = _build_account_state(
            weights=current_weights,
            equity=current_equity,
            close_prices={symbol: float(close_row.get(symbol, np.nan)) for symbol in MANAGED_SYMBOLS},
        )
        indicators = _indicator_snapshot_at(indicator_history, as_of)
        if "soxl" not in indicators or "soxx" not in indicators:
            continue

        try:
            plan = build_rebalance_plan(
                indicators,
                account_state,
                translator=lambda key, **kwargs: key,
                **_call_strategy_kwargs(strategy_overrides),
            )
        except Exception:
            continue

        target_values = dict(plan["targets"])
        threshold_value = float(plan["threshold_value"])
        current_min_trade = float(plan["current_min_trade"])
        core_volatility_delever_triggered = bool(plan.get("blend_gate_volatility_delever_triggered"))
        if core_volatility_delever_triggered:
            soxl_delever_stop_count += 1
        delever_row = (
            delever_history.loc[as_of]
            if soxl_delever_enabled and as_of in delever_history.index
            else pd.Series(dtype=object)
        )
        delever_triggered = bool(delever_row.get("triggered", False)) if not delever_row.empty else False
        if delever_triggered and float(target_values.get("SOXL", 0.0) or 0.0) > 0.0:
            soxl_target_value = float(target_values.get("SOXL", 0.0))
            retained_value = soxl_target_value * overlay_retention_ratio
            redirected_value = soxl_target_value - retained_value
            target_values["SOXL"] = retained_value
            target_values[overlay_redirect_symbol] = (
                float(target_values.get(overlay_redirect_symbol, 0.0) or 0.0)
                + redirected_value
            )
            soxl_delever_stop_count += 1
        trend_symbol = str(plan.get("trend_symbol", "SOXX")).lower()
        trend_indicators = indicators.get(trend_symbol, {})
        trend_rsi14 = plan.get("trend_rsi14")
        if trend_rsi14 is None:
            trend_rsi14 = trend_indicators.get("rsi14")
        trend_bb_mid = plan.get("trend_bb_mid")
        if trend_bb_mid is None:
            trend_bb_mid = trend_indicators.get("bb_mid")
        trend_bb_upper = plan.get("trend_bb_upper")
        if trend_bb_upper is None:
            trend_bb_upper = trend_indicators.get("bb_upper")
        trend_bb_lower = plan.get("trend_bb_lower")
        if trend_bb_lower is None:
            trend_bb_lower = trend_indicators.get("bb_lower")
        signal_rows.append(
            {
                "as_of": as_of,
                "signal_date": as_of,
                "effective_date": next_as_of,
                "blend_tier": plan.get("blend_tier"),
                "base_blend_tier": plan.get("base_blend_tier"),
                "allocation_mode": plan.get("allocation_mode"),
                "trend_symbol": plan.get("trend_symbol"),
                "trend_price": plan.get("trend_price"),
                "trend_ma": plan.get("trend_ma"),
                "trend_entry_line": plan.get("trend_entry_line"),
                "trend_mid_line": plan.get("trend_mid_line"),
                "trend_exit_line": plan.get("trend_exit_line"),
                "trend_ma20": plan.get("trend_ma20"),
                "trend_ma20_slope": plan.get("trend_ma20_slope"),
                "trend_realized_volatility_10": trend_indicators.get("realized_volatility_10"),
                "trend_realized_volatility_20": trend_indicators.get("realized_volatility_20"),
                "trend_rsi14": trend_rsi14,
                "trend_rsi14_raw": trend_indicators.get("rsi14_raw"),
                "trend_rsi14_dynamic_threshold": trend_indicators.get("rsi14_dynamic_threshold"),
                "trend_bb_mid": trend_bb_mid,
                "trend_bb_upper": trend_bb_upper,
                "trend_bb_lower": trend_bb_lower,
                "reserved_cash": plan.get("reserved_cash"),
                "investable_cash": plan.get("investable_cash"),
                "income_layer_ratio": plan.get("income_layer_ratio"),
                "income_layer_value": plan.get("income_layer_value"),
                "income_layer_ratio_mode": plan.get("income_layer_ratio_mode"),
                "income_layer_log_ratio": plan.get("income_layer_log_ratio"),
                "income_layer_loss_budget_ratio": plan.get("income_layer_loss_budget_ratio"),
                "income_layer_loss_budget_cap_ratio": plan.get("income_layer_loss_budget_cap_ratio"),
                "income_layer_stress_drawdown_ratio": plan.get("income_layer_stress_drawdown_ratio"),
                "threshold_value": threshold_value,
                "current_min_trade": current_min_trade,
                "total_equity": current_equity,
                "overlay_trigger_count": plan.get("overlay_trigger_count"),
                "overlay_trigger_reasons": ",".join(plan.get("overlay_trigger_reasons", ())),
                "blend_gate_rsi_threshold": plan.get("blend_gate_rsi_threshold"),
                "blend_gate_rsi_cap_enabled": plan.get("blend_gate_rsi_cap_enabled"),
                "blend_gate_dynamic_rsi_threshold_enabled": plan.get(
                    "blend_gate_dynamic_rsi_threshold_enabled"
                ),
                "blend_gate_bollinger_cap_enabled": plan.get("blend_gate_bollinger_cap_enabled"),
                "blend_gate_overlay_stack_triggers": plan.get("blend_gate_overlay_stack_triggers"),
                "blend_gate_volatility_delever_enabled": plan.get("blend_gate_volatility_delever_enabled"),
                "blend_gate_volatility_delever_symbol": plan.get("blend_gate_volatility_delever_symbol"),
                "blend_gate_volatility_delever_window": plan.get("blend_gate_volatility_delever_window"),
                "blend_gate_volatility_delever_threshold": plan.get("blend_gate_volatility_delever_threshold"),
                "blend_gate_volatility_delever_metric": plan.get("blend_gate_volatility_delever_metric"),
                "blend_gate_volatility_delever_triggered": core_volatility_delever_triggered,
                "blend_gate_volatility_delever_retention_ratio": plan.get(
                    "blend_gate_volatility_delever_retention_ratio"
                ),
                "blend_gate_volatility_delever_redirect_symbol": plan.get(
                    "blend_gate_volatility_delever_redirect_symbol"
                ),
                "blend_gate_volatility_delever_removed_ratio": plan.get(
                    "blend_gate_volatility_delever_removed_ratio"
                ),
                "chandelier_stop_enabled": bool(soxl_delever_enabled and overlay_kind == "chandelier"),
                "chandelier_stop_symbol": overlay_symbol,
                "chandelier_window": overlay_window,
                "chandelier_atr_multiple": overlay_atr_multiple,
                "chandelier_stop_close": delever_row.get("close") if not delever_row.empty else None,
                "chandelier_stop_high": delever_row.get("high") if not delever_row.empty else None,
                "chandelier_stop_low": delever_row.get("low") if not delever_row.empty else None,
                "chandelier_atr": delever_row.get("atr") if not delever_row.empty else None,
                "chandelier_stop_line": delever_row.get("stop_line") if not delever_row.empty else None,
                "chandelier_stop_triggered": bool(delever_triggered and overlay_kind == "chandelier"),
                "soxl_delever_overlay_enabled": bool(soxl_delever_enabled),
                "soxl_delever_overlay_kind": overlay_kind,
                "soxl_delever_overlay_symbol": overlay_symbol,
                "soxl_delever_overlay_window": overlay_window,
                "soxl_delever_overlay_threshold": delever_row.get("threshold") if not delever_row.empty else None,
                "soxl_delever_overlay_atr_multiple": overlay_atr_multiple if overlay_kind == "chandelier" else None,
                "soxl_delever_overlay_retention_ratio": overlay_retention_ratio,
                "soxl_delever_overlay_redirect_symbol": overlay_redirect_symbol,
                "soxl_delever_overlay_metric": delever_row.get("metric") if not delever_row.empty else None,
                "soxl_delever_overlay_triggered": delever_triggered,
            }
        )

        next_weights, turnover, next_equity = _execute_rebalance(
            current_weights=current_weights,
            target_values=target_values,
            equity=current_equity,
            threshold_value=threshold_value,
            current_min_trade=current_min_trade,
            turnover_cost_bps=float(turnover_cost_bps),
        )
        if turnover > 0:
            for symbol in MANAGED_SYMBOLS:
                old = float(current_weights.get(symbol, 0.0))
                new = float(next_weights.get(symbol, 0.0))
                if abs(new - old) > 1e-12:
                    trade_rows.append(
                        {
                            "signal_date": as_of,
                            "effective_date": next_as_of,
                            "symbol": symbol,
                            "old_weight": old,
                            "new_weight": new,
                            "delta_weight": new - old,
                        }
                    )
        turnover_history.at[next_as_of] = turnover
        current_weights = {symbol: float(next_weights.get(symbol, 0.0)) for symbol in MANAGED_SYMBOLS}
        current_weights["__cash__"] = float(next_weights.get("__cash__", 0.0))
        current_equity = float(next_equity)

        next_market_values = {
            symbol: float(current_equity) * float(current_weights.get(symbol, 0.0))
            for symbol in MANAGED_SYMBOLS
        }
        next_cash = float(current_equity) * float(current_weights.get("__cash__", 0.0))
        equity_after_return = next_cash + sum(
            float(next_market_values[symbol]) * (float(next_close_row.get(symbol, np.nan)) / float(close_row.get(symbol)))
            if symbol in next_close_row and pd.notna(close_row.get(symbol)) and float(close_row.get(symbol)) > 0
            else float(next_market_values[symbol])
            for symbol in MANAGED_SYMBOLS
        )
        if current_equity > 0:
            portfolio_returns.at[next_as_of] = equity_after_return / current_equity - 1.0
        for symbol in MANAGED_SYMBOLS:
            weights_history.at[next_as_of, symbol] = float(current_weights.get(symbol, 0.0))
        weights_history.at[next_as_of, "__cash__"] = float(current_weights.get("__cash__", 0.0))
        current_equity = float(equity_after_return)

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = _summarize_returns(portfolio_returns, used_weights)
    summary["Chandelier Stops"] = float(soxl_delever_stop_count if overlay_kind == "chandelier" else 0.0)
    summary["SOXL Delever Stops"] = float(soxl_delever_stop_count)
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "trades": pd.DataFrame(trade_rows),
        "signal_history": pd.DataFrame(signal_rows),
    }


def _format_summary(summary: Mapping[str, float | str]) -> pd.DataFrame:
    return pd.DataFrame([{column: summary.get(column) for column in DEFAULT_OUTPUT_COLUMNS}])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtest for soxl_soxx_trend_income.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet)")
    input_group.add_argument("--download", action="store_true", help="Download price history with yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START, help="Download start date used with --download")
    parser.add_argument("--price-end", help="Download end date used with --download")
    parser.add_argument("--proxy", help="Proxy URL for yfinance downloads")
    parser.add_argument("--proxy-list", help="Path or URL with one HTTP(S) proxy per line")
    parser.add_argument("--proxy-list-max", type=int, default=12, help="Maximum proxy candidates to try")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_BACKTEST_START, help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--disable-rsi-cap", action="store_true", help="Disable the RSI overheat downgrade overlay")
    parser.add_argument(
        "--disable-bollinger-cap",
        action="store_true",
        help="Disable the Bollinger upper-band overheat downgrade overlay",
    )
    parser.add_argument(
        "--disable-overlay-stack-triggers",
        action="store_true",
        help="Downgrade at most one tier when multiple overheat overlays fire",
    )
    parser.add_argument("--rsi-threshold", type=float, help="Static RSI threshold for the overheat overlay")
    parser.add_argument(
        "--dynamic-rsi-quantile-window",
        type=int,
        help="Use a rolling RSI quantile threshold over this many trading days",
    )
    parser.add_argument(
        "--dynamic-rsi-quantile",
        type=float,
        help="Rolling RSI quantile to use with --dynamic-rsi-quantile-window, for example 0.90",
    )
    parser.add_argument(
        "--dynamic-rsi-floor",
        type=float,
        help="Lower bound for the dynamic RSI threshold; defaults to the static RSI threshold",
    )
    parser.add_argument(
        "--disable-income-layer",
        action="store_true",
        help="Disable the income layer for long-history core SOXL/SOXX research",
    )
    parser.add_argument(
        "--enable-chandelier-stop",
        action="store_true",
        help=(
            "Enable research-only Chandelier-style SOXL delever overlay. "
            "Uses true range when high/low are present and close-only range otherwise."
        ),
    )
    parser.add_argument(
        "--chandelier-stop-symbol",
        default="SOXX",
        help="Symbol used to compute the Chandelier stop",
    )
    parser.add_argument("--chandelier-window", type=int, default=22, help="Lookback window for the Chandelier stop")
    parser.add_argument(
        "--chandelier-atr-multiple",
        type=float,
        default=3.0,
        help="ATR multiple for the Chandelier stop",
    )
    parser.add_argument(
        "--soxl-delever-overlay",
        default="none",
        choices=("none", "chandelier", "drawdown", "volatility", "momentum"),
        help="Research-only SOXL delever overlay family",
    )
    parser.add_argument("--soxl-delever-symbol", help="Symbol used by the SOXL delever overlay")
    parser.add_argument("--soxl-delever-window", type=int, help="Lookback window for the SOXL delever overlay")
    parser.add_argument(
        "--soxl-delever-threshold",
        type=float,
        help="Overlay threshold: negative for drawdown/momentum, annualized ratio for volatility",
    )
    parser.add_argument(
        "--soxl-delever-atr-multiple",
        type=float,
        help="ATR multiple when --soxl-delever-overlay=chandelier",
    )
    parser.add_argument(
        "--soxl-delever-retention-ratio",
        type=float,
        default=0.0,
        help="Fraction of the SOXL target retained when the overlay triggers",
    )
    parser.add_argument(
        "--soxl-delever-redirect-symbol",
        default="BOXX",
        help="Managed symbol receiving the removed SOXL target value",
    )
    parser.add_argument("--output-dir", help="Optional output directory for research artifacts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir) if args.output_dir else None
    if args.download:
        if output_dir is None:
            raise EnvironmentError("--output-dir is required when --download is used")
        prices = download_price_history_with_proxy_candidates(
            DEFAULT_DOWNLOAD_SYMBOLS,
            start=args.price_start,
            end=args.price_end,
            chunk_size=25,
            proxy=args.proxy,
            proxy_candidates=load_proxy_candidates(args.proxy_list, max_candidates=args.proxy_list_max)
            if args.proxy_list
            else None,
        )
    else:
        prices = pd.read_csv(args.prices)

    result = run_backtest(
        prices,
        initial_equity=float(args.initial_equity),
        start_date=args.start_date,
        end_date=args.end_date,
        turnover_cost_bps=float(args.turnover_cost_bps),
        blend_gate_rsi_cap_enabled=False if args.disable_rsi_cap else None,
        blend_gate_rsi_threshold=args.rsi_threshold,
        blend_gate_bollinger_cap_enabled=False if args.disable_bollinger_cap else None,
        blend_gate_overlay_stack_triggers=False if args.disable_overlay_stack_triggers else None,
        dynamic_rsi_quantile_window=args.dynamic_rsi_quantile_window,
        dynamic_rsi_quantile=args.dynamic_rsi_quantile,
        dynamic_rsi_floor=args.dynamic_rsi_floor,
        disable_income_layer=bool(args.disable_income_layer),
        chandelier_stop_enabled=bool(args.enable_chandelier_stop),
        chandelier_stop_symbol=args.chandelier_stop_symbol,
        chandelier_window=int(args.chandelier_window),
        chandelier_atr_multiple=float(args.chandelier_atr_multiple),
        soxl_delever_overlay_kind=args.soxl_delever_overlay,
        soxl_delever_overlay_symbol=args.soxl_delever_symbol,
        soxl_delever_overlay_window=args.soxl_delever_window,
        soxl_delever_overlay_threshold=args.soxl_delever_threshold,
        soxl_delever_overlay_atr_multiple=args.soxl_delever_atr_multiple,
        soxl_delever_overlay_retention_ratio=float(args.soxl_delever_retention_ratio),
        soxl_delever_overlay_redirect_symbol=args.soxl_delever_redirect_symbol,
    )

    summary_frame = _format_summary(result["summary"])
    print(summary_frame.to_string(index=False))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        _build_price_frame(prices).to_csv(output_dir / "price_history.csv", index=False)
        summary_frame.to_csv(output_dir / "summary.csv", index=False)
        result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
        result["weights_history"].to_csv(output_dir / "weights_history.csv")
        result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
        result["trades"].to_csv(output_dir / "trades.csv", index=False)
        result["signal_history"].to_csv(output_dir / "signal_history.csv", index=False)
        write_json(output_dir / "backtest_config.json", {
            "profile": PROFILE,
            "initial_equity": float(args.initial_equity),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "turnover_cost_bps": float(args.turnover_cost_bps),
            "disable_rsi_cap": bool(args.disable_rsi_cap),
            "disable_bollinger_cap": bool(args.disable_bollinger_cap),
            "disable_overlay_stack_triggers": bool(args.disable_overlay_stack_triggers),
            "rsi_threshold": args.rsi_threshold,
            "dynamic_rsi_quantile_window": args.dynamic_rsi_quantile_window,
            "dynamic_rsi_quantile": args.dynamic_rsi_quantile,
            "dynamic_rsi_floor": args.dynamic_rsi_floor,
            "disable_income_layer": bool(args.disable_income_layer),
            "chandelier_stop_enabled": bool(args.enable_chandelier_stop),
            "chandelier_stop_symbol": args.chandelier_stop_symbol,
            "chandelier_window": int(args.chandelier_window),
            "chandelier_atr_multiple": float(args.chandelier_atr_multiple),
            "soxl_delever_overlay": args.soxl_delever_overlay,
            "soxl_delever_symbol": args.soxl_delever_symbol,
            "soxl_delever_window": args.soxl_delever_window,
            "soxl_delever_threshold": args.soxl_delever_threshold,
            "soxl_delever_atr_multiple": args.soxl_delever_atr_multiple,
            "soxl_delever_retention_ratio": float(args.soxl_delever_retention_ratio),
            "soxl_delever_redirect_symbol": args.soxl_delever_redirect_symbol,
        })
        print(f"wrote research backtest outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
