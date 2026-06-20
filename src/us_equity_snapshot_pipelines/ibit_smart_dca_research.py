from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from us_equity_snapshot_pipelines.ibit_zscore_exit_plugin import (
    IBIT_ZSCORE_EXIT_SCHEMA_VERSION,
    PLUGIN_IBIT_ZSCORE_EXIT,
    build_ibit_zscore_exit_signal,
)
from us_equity_snapshot_pipelines.yfinance_prices import download_price_history, normalize_price_field

SCHEMA_VERSION = "ibit_smart_dca_research.v1"
MANIFEST_TYPE = "ibit_smart_dca_research"
PARKING_ONLY_VARIANT = "parking_only"
BUY_ONLY_VARIANT = "buy_only_dca"
PLUGIN_ON_VARIANT = "plugin_on"
PLUGIN_DISABLED_ROUTE = "plugin_disabled"
MIN_ZSCORE_AVAILABLE_SIGNAL_RATIO_FOR_PROMOTION = 0.80


@dataclass(frozen=True)
class IbitDcaResearchConfig:
    ibit_symbol: str = "IBIT"
    parking_symbol: str = "BOXX"
    parking_proxy_symbol: str = ""
    price_field: str = "adjusted_close"
    primary_benchmark: str = "QQQ"
    secondary_benchmark: str = "SPY"
    btc_proxy_symbol: str = ""
    initial_parking_value: float = 0.0
    contribution_amount: float = 0.0
    rebalance_frequency: str = "MS"
    turnover_cost_bps: float = 5.0
    plugin_enabled: bool = True
    min_drawdown_improvement: float = 0.10
    max_cagr_giveup_for_drawdown: float = 0.02

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper().removesuffix(".US")


def _unique_symbols(*symbols: str) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized and normalized not in seen:
            unique.append(normalized)
            seen.add(normalized)
    return unique


def download_ibit_smart_dca_price_history(
    *,
    start: str,
    end: str | None = None,
    ibit_symbol: str = "IBIT",
    parking_symbol: str = "BOXX",
    parking_proxy_symbol: str = "",
    price_field: str = "adjusted_close",
    primary_benchmark: str = "QQQ",
    secondary_benchmark: str = "SPY",
    btc_proxy_symbol: str = "BTC",
    proxy: str | None = None,
) -> pd.DataFrame:
    symbols = _unique_symbols(
        ibit_symbol,
        parking_symbol,
        parking_proxy_symbol,
        primary_benchmark,
        secondary_benchmark,
        btc_proxy_symbol,
    )
    btc_symbol = _normalize_symbol(btc_proxy_symbol)
    symbol_aliases = {btc_symbol: ("BTC-USD",)} if btc_symbol else None
    return download_price_history(
        symbols,
        start=start,
        end=end,
        symbol_aliases=symbol_aliases,
        proxy=proxy,
        price_field=price_field,
    )


def _normalize_price_matrix(prices: pd.DataFrame, *, date_column: str = "as_of") -> pd.DataFrame:
    frame = pd.DataFrame(prices).copy()
    normalized_columns = {str(column).strip().lower(): column for column in frame.columns}
    if {"symbol", "close"}.issubset(normalized_columns):
        symbol_col = normalized_columns["symbol"]
        close_col = normalized_columns["close"]
        date_col = normalized_columns.get(date_column.lower()) or normalized_columns.get("date")
        if date_col is None:
            raise ValueError("long price history requires an as_of/date column")
        frame = frame[[date_col, symbol_col, close_col]].copy()
        frame.columns = ["as_of", "symbol", "close"]
        frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
        frame["symbol"] = frame["symbol"].map(_normalize_symbol)
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["as_of", "symbol", "close"])
        matrix = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
    else:
        date_col = normalized_columns.get(date_column.lower()) or normalized_columns.get("date")
        if date_col is not None:
            frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce").dt.tz_localize(None).dt.normalize()
            frame = frame.dropna(subset=[date_col]).set_index(date_col)
        else:
            index = pd.to_datetime(frame.index, errors="coerce")
            if getattr(index, "tz", None) is not None:
                index = index.tz_localize(None)
            frame.index = pd.DatetimeIndex(index).normalize()
            frame = frame.loc[frame.index.notna()]
        matrix = frame.copy()
        matrix.columns = [_normalize_symbol(str(column)) for column in matrix.columns]
        for column in matrix.columns:
            matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    matrix = matrix.sort_index()
    matrix.index.name = "as_of"
    return matrix.ffill().dropna(how="all")


def _apply_ibit_proxy_price(
    matrix: pd.DataFrame,
    *,
    ibit_symbol: str,
    btc_proxy_symbol: str | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    proxy_symbol = _normalize_symbol(btc_proxy_symbol or "")
    metadata: dict[str, object] = {
        "btc_proxy_symbol": proxy_symbol,
        "proxy_rows_filled": 0,
        "proxy_scale": float("nan"),
        "proxy_scale_source": "none",
        "first_actual_ibit_date": "",
    }
    if not proxy_symbol:
        return matrix, metadata
    if proxy_symbol not in matrix.columns:
        raise ValueError(f"btc_proxy_symbol {proxy_symbol} is not present in price history")

    frame = matrix.copy()
    proxy = pd.to_numeric(frame[proxy_symbol], errors="coerce")
    ibit = (
        pd.to_numeric(frame[ibit_symbol], errors="coerce")
        if ibit_symbol in frame.columns
        else pd.Series(np.nan, index=frame.index)
    )
    actual = ibit.dropna()
    valid_proxy = proxy.dropna()
    if valid_proxy.empty:
        raise ValueError(f"btc_proxy_symbol {proxy_symbol} has no valid prices")

    if actual.empty:
        scale = 100.0 / float(valid_proxy.iloc[0])
        fill_mask = ibit.isna() & proxy.notna()
        metadata["proxy_scale_source"] = "normalized_100"
    else:
        first_actual_date = pd.Timestamp(actual.index[0])
        proxy_at_inception = proxy.loc[:first_actual_date].dropna()
        if proxy_at_inception.empty:
            proxy_at_inception = proxy.loc[first_actual_date:].dropna()
        if proxy_at_inception.empty:
            raise ValueError(f"btc_proxy_symbol {proxy_symbol} has no price around first IBIT date")
        scale = float(actual.iloc[0]) / float(proxy_at_inception.iloc[-1])
        fill_mask = ibit.isna() & proxy.notna() & (frame.index < first_actual_date)
        metadata["proxy_scale_source"] = "first_actual_ibit_close"
        metadata["first_actual_ibit_date"] = str(first_actual_date.date())

    filled_values = proxy * scale
    ibit = ibit.copy()
    ibit.loc[fill_mask] = filled_values.loc[fill_mask]
    frame[ibit_symbol] = ibit
    metadata["proxy_rows_filled"] = int(fill_mask.sum())
    metadata["proxy_scale"] = float(scale)
    return frame, metadata


def _apply_parking_proxy_price(
    matrix: pd.DataFrame,
    *,
    parking_symbol: str,
    parking_proxy_symbol: str | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    proxy_symbol = _normalize_symbol(parking_proxy_symbol or "")
    metadata: dict[str, object] = {
        "parking_proxy_symbol": proxy_symbol,
        "parking_proxy_rows_filled": 0,
        "parking_proxy_scale": float("nan"),
        "parking_proxy_scale_source": "none",
        "first_actual_parking_date": "",
    }
    if not proxy_symbol or proxy_symbol == parking_symbol:
        return matrix, metadata

    frame = matrix.copy()
    parking = (
        pd.to_numeric(frame[parking_symbol], errors="coerce")
        if parking_symbol in frame.columns
        else pd.Series(np.nan, index=frame.index)
    )
    actual = parking.dropna()
    if proxy_symbol not in matrix.columns:
        if not actual.empty and not bool((frame.index < pd.Timestamp(actual.index[0])).any()):
            return frame, metadata
        raise ValueError(f"parking_proxy_symbol {proxy_symbol} is not present in price history")

    proxy = pd.to_numeric(frame[proxy_symbol], errors="coerce")
    valid_proxy = proxy.dropna()
    if valid_proxy.empty:
        raise ValueError(f"parking_proxy_symbol {proxy_symbol} has no valid prices")

    if actual.empty:
        scale = 100.0 / float(valid_proxy.iloc[0])
        fill_mask = parking.isna() & proxy.notna()
        metadata["parking_proxy_scale_source"] = "normalized_100"
    else:
        first_actual_date = pd.Timestamp(actual.index[0])
        proxy_at_inception = proxy.loc[:first_actual_date].dropna()
        if proxy_at_inception.empty:
            proxy_at_inception = proxy.loc[first_actual_date:].dropna()
        if proxy_at_inception.empty:
            raise ValueError(f"parking_proxy_symbol {proxy_symbol} has no price around first parking date")
        scale = float(actual.iloc[0]) / float(proxy_at_inception.iloc[-1])
        fill_mask = parking.isna() & proxy.notna() & (frame.index < first_actual_date)
        metadata["parking_proxy_scale_source"] = "first_actual_parking_close"
        metadata["first_actual_parking_date"] = str(first_actual_date.date())

    filled_values = proxy * scale
    parking = parking.copy()
    parking.loc[fill_mask] = filled_values.loc[fill_mask]
    frame[parking_symbol] = parking
    metadata["parking_proxy_rows_filled"] = int(fill_mask.sum())
    metadata["parking_proxy_scale"] = float(scale)
    return frame, metadata


def _rebalance_dates(index: pd.DatetimeIndex, frequency: str) -> tuple[pd.Timestamp, ...]:
    if index.empty:
        return ()
    series = pd.Series(index=index, data=index)
    if str(frequency).upper() in {"D", "DAILY"}:
        return tuple(pd.Timestamp(value) for value in index)
    grouped = series.groupby(pd.Grouper(freq=frequency)).first().dropna()
    return tuple(pd.Timestamp(value) for value in grouped.values)


def _unitized_equity(nav: pd.Series, external_cash_flow: pd.Series) -> pd.Series:
    values = pd.to_numeric(pd.Series(nav), errors="coerce")
    flows = pd.to_numeric(pd.Series(external_cash_flow), errors="coerce").reindex(values.index).fillna(0.0)
    values = values.dropna()
    if values.empty:
        return values
    unit_values: list[float] = []
    previous_nav: float | None = None
    unit = 1.0
    for as_of, nav_value in values.items():
        nav_float = float(nav_value)
        if previous_nav is not None and previous_nav > 0:
            flow = float(flows.loc[as_of])
            period_return = (nav_float - flow) / previous_nav - 1.0
            unit *= 1.0 + period_return
        unit_values.append(float(unit))
        previous_nav = nav_float
    return pd.Series(unit_values, index=values.index, name="unit_equity")


def _portfolio_stats(equity: pd.Series) -> dict[str, float | int | str]:
    series = pd.to_numeric(pd.Series(equity), errors="coerce").dropna()
    if series.empty:
        return {
            "start": "",
            "end": "",
            "observations": 0,
            "ending_value": float("nan"),
            "total_return": float("nan"),
            "cagr": float("nan"),
            "max_drawdown": float("nan"),
            "volatility": float("nan"),
            "sharpe": float("nan"),
        }
    start_value = float(series.iloc[0])
    end_value = float(series.iloc[-1])
    returns = series.pct_change().dropna()
    years = max((series.index[-1] - series.index[0]).days / 365.25, 1.0 / 365.25)
    equity_curve = series / start_value if start_value > 0 else series.copy()
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    std = float(returns.std(ddof=0)) if not returns.empty else 0.0
    return {
        "start": str(pd.Timestamp(series.index[0]).date()),
        "end": str(pd.Timestamp(series.index[-1]).date()),
        "observations": int(len(series)),
        "ending_value": end_value,
        "total_return": float(end_value / start_value - 1.0) if start_value > 0 else float("nan"),
        "cagr": float((end_value / start_value) ** (1.0 / years) - 1.0) if start_value > 0 else float("nan"),
        "max_drawdown": float(drawdown.min()),
        "volatility": float(returns.std(ddof=0) * np.sqrt(252.0)) if not returns.empty else 0.0,
        "sharpe": float(returns.mean() / std * np.sqrt(252.0)) if std else float("nan"),
    }


def _benchmark_stats(price_matrix: pd.DataFrame, symbol: str) -> dict[str, float]:
    if not symbol or symbol not in price_matrix.columns:
        return {"cagr": float("nan"), "max_drawdown": float("nan")}
    prices = pd.to_numeric(price_matrix[symbol], errors="coerce").dropna()
    if prices.empty:
        return {"cagr": float("nan"), "max_drawdown": float("nan")}
    equity = prices / float(prices.iloc[0])
    stats = _portfolio_stats(equity)
    return {"cagr": float(stats["cagr"]), "max_drawdown": float(stats["max_drawdown"])}


def _zscore_history_metadata(zscore_history: pd.DataFrame | None) -> dict[str, object]:
    if zscore_history is None or pd.DataFrame(zscore_history).empty:
        return {"rows": 0, "start": "", "end": ""}
    frame = pd.DataFrame(zscore_history).copy()
    normalized = {str(column).strip().lower(): column for column in frame.columns}
    date_column = normalized.get("as_of") or normalized.get("date") or normalized.get("timestamp")
    if date_column is None:
        return {"rows": int(len(frame)), "start": "", "end": ""}
    dates = pd.to_datetime(frame[date_column], errors="coerce").dropna().sort_values()
    if dates.empty:
        return {"rows": int(len(frame)), "start": "", "end": ""}
    return {
        "rows": int(len(dates)),
        "start": str(pd.Timestamp(dates.iloc[0]).date()),
        "end": str(pd.Timestamp(dates.iloc[-1]).date()),
    }


def _trade(
    *,
    rows: list[dict[str, object]],
    as_of: pd.Timestamp,
    variant: str,
    action: str,
    symbol: str,
    quantity: float,
    price: float,
    turnover_cost_bps: float,
) -> float:
    trade_value = float(quantity) * float(price)
    cost = abs(trade_value) * float(turnover_cost_bps) / 10_000.0
    rows.append(
        {
            "as_of": str(as_of.date()),
            "variant": variant,
            "action": action,
            "symbol": symbol,
            "quantity": float(quantity),
            "price": float(price),
            "trade_value": float(trade_value),
            "estimated_cost": float(cost),
        }
    )
    return cost


def _build_plugin_signal(
    zscore_history: pd.DataFrame | None,
    *,
    as_of: pd.Timestamp,
    plugin_config: Mapping[str, Any] | None,
    parking_symbol: str,
) -> dict[str, Any]:
    if zscore_history is None or pd.DataFrame(zscore_history).empty:
        raise ValueError("plugin_enabled=True requires non-empty zscore_history")
    config = dict(plugin_config or {})
    config["as_of"] = str(as_of.date())
    config.setdefault("parking_symbol", parking_symbol)
    try:
        signal = build_ibit_zscore_exit_signal(pd.DataFrame(zscore_history), config)
    except ValueError as exc:
        if "no valid zscore rows at or before as_of" not in str(exc):
            raise
        signal = {
            "schema_version": IBIT_ZSCORE_EXIT_SCHEMA_VERSION,
            "as_of": str(as_of.date()),
            "plugin": PLUGIN_IBIT_ZSCORE_EXIT,
            "canonical_route": "normal",
            "suggested_action": "no_action_zscore_unavailable",
            "would_trade_if_enabled": False,
            "metrics": {
                "mvrv_zscore": float("nan"),
                "zscore_history_rows": 0,
                "threshold_history_rows": 0,
                "data_status": "zscore_unavailable",
            },
            "thresholds": {},
            "position_control": {
                "final_route": "normal",
                "route_source": "zscore_unavailable",
                "suggested_action": "no_action_zscore_unavailable",
                "parking_symbol": parking_symbol,
                "target_ibit_exposure": 1.0,
                "target_parking_exposure": 0.0,
                "target_allocations": {"IBIT": 1.0, parking_symbol: 0.0},
                "reason_codes": ("zscore_unavailable_before_first_metric",),
            },
            "reason_codes": ("zscore_unavailable_before_first_metric",),
        }
    metrics = dict(signal.get("metrics", {}))
    metrics.setdefault("data_status", "available")
    signal["metrics"] = metrics
    return signal


def _simulate_variant(
    prices: pd.DataFrame,
    *,
    variant: str,
    config: IbitDcaResearchConfig,
    zscore_history: pd.DataFrame | None,
    plugin_config: Mapping[str, Any] | None,
) -> dict[str, pd.DataFrame]:
    required = [config.ibit_symbol, config.parking_symbol]
    missing = [symbol for symbol in required if symbol not in prices.columns]
    if missing:
        raise ValueError(f"price history missing required symbols: {missing}")

    dates = pd.DatetimeIndex(prices.index)
    rebalances = set(_rebalance_dates(dates, config.rebalance_frequency))
    if not rebalances:
        raise ValueError("price history does not contain rebalance dates")

    ibit_shares = 0.0
    parking_shares = 0.0
    pending_initial_parking_value = float(config.initial_parking_value)
    cash = 0.0
    trade_rows: list[dict[str, object]] = []
    holding_rows: list[dict[str, object]] = []
    signal_rows: list[dict[str, object]] = []

    for as_of, row in prices.iterrows():
        ibit_price = float(row[config.ibit_symbol])
        parking_price = float(row[config.parking_symbol])
        if pd.isna(ibit_price) or pd.isna(parking_price) or ibit_price <= 0 or parking_price <= 0:
            continue
        if pending_initial_parking_value > 0:
            parking_shares = pending_initial_parking_value / parking_price
            pending_initial_parking_value = 0.0
        external_cash_flow = 0.0

        if pd.Timestamp(as_of) in rebalances:
            external_cash_flow = float(config.contribution_amount)
            cash += external_cash_flow
            nav_before = ibit_shares * ibit_price + parking_shares * parking_price + cash
            if variant == PLUGIN_ON_VARIANT:
                signal = _build_plugin_signal(
                    zscore_history,
                    as_of=pd.Timestamp(as_of),
                    plugin_config=plugin_config,
                    parking_symbol=config.parking_symbol,
                )
                target_ibit_exposure = float(signal["position_control"]["target_ibit_exposure"])
                target_parking_exposure = float(signal["position_control"]["target_parking_exposure"])
                canonical_route = str(signal["canonical_route"])
                suggested_action = str(signal["suggested_action"])
                metrics = dict(signal.get("metrics", {}))
                signal_as_of = str(signal.get("as_of", "") or "")
                signal_data_status = str(metrics.get("data_status", "") or "available")
                mvrv_zscore = float(metrics.get("mvrv_zscore", float("nan")))
                zscore_history_rows_used = int(metrics.get("zscore_history_rows", 0) or 0)
                threshold_history_rows_used = int(metrics.get("threshold_history_rows", 0) or 0)
            elif variant == PARKING_ONLY_VARIANT:
                target_ibit_exposure = 0.0
                target_parking_exposure = 1.0
                canonical_route = PARKING_ONLY_VARIANT
                suggested_action = "hold_parking"
                signal_as_of = str(pd.Timestamp(as_of).date())
                signal_data_status = "not_applicable"
                mvrv_zscore = float("nan")
                zscore_history_rows_used = 0
                threshold_history_rows_used = 0
            else:
                target_ibit_exposure = 1.0
                target_parking_exposure = 0.0
                canonical_route = PLUGIN_DISABLED_ROUTE
                suggested_action = "buy_only_dca"
                signal_as_of = str(pd.Timestamp(as_of).date())
                signal_data_status = "plugin_disabled"
                mvrv_zscore = float("nan")
                zscore_history_rows_used = 0
                threshold_history_rows_used = 0

            signal_rows.append(
                {
                    "as_of": str(pd.Timestamp(as_of).date()),
                    "signal_as_of": signal_as_of,
                    "variant": variant,
                    "plugin_enabled": variant == PLUGIN_ON_VARIANT,
                    "canonical_route": canonical_route,
                    "suggested_action": suggested_action,
                    "signal_data_status": signal_data_status,
                    "mvrv_zscore": mvrv_zscore,
                    "zscore_history_rows_used": zscore_history_rows_used,
                    "threshold_history_rows_used": threshold_history_rows_used,
                    "target_ibit_exposure": float(target_ibit_exposure),
                    "target_parking_exposure": float(target_parking_exposure),
                }
            )

            target_ibit_value = nav_before * target_ibit_exposure
            target_parking_value = nav_before * target_parking_exposure
            current_ibit_value = ibit_shares * ibit_price
            current_parking_value = parking_shares * parking_price

            ibit_delta_value = target_ibit_value - current_ibit_value
            parking_delta_value = target_parking_value - current_parking_value

            # Sell before buy so BOXX/parking can fund scheduled IBIT purchases.
            if parking_delta_value < -1e-8:
                sell_value = min(current_parking_value, -parking_delta_value)
                quantity = sell_value / parking_price
                parking_shares -= quantity
                cash += sell_value
                cash -= _trade(
                    rows=trade_rows,
                    as_of=pd.Timestamp(as_of),
                    variant=variant,
                    action="sell",
                    symbol=config.parking_symbol,
                    quantity=quantity,
                    price=parking_price,
                    turnover_cost_bps=config.turnover_cost_bps,
                )
            if ibit_delta_value < -1e-8:
                sell_value = min(ibit_shares * ibit_price, -ibit_delta_value)
                quantity = sell_value / ibit_price
                ibit_shares -= quantity
                cash += sell_value
                cash -= _trade(
                    rows=trade_rows,
                    as_of=pd.Timestamp(as_of),
                    variant=variant,
                    action="sell",
                    symbol=config.ibit_symbol,
                    quantity=quantity,
                    price=ibit_price,
                    turnover_cost_bps=config.turnover_cost_bps,
                )
            if ibit_delta_value > 1e-8:
                buy_value = min(cash, ibit_delta_value)
                if buy_value > 1e-8:
                    quantity = buy_value / ibit_price
                    ibit_shares += quantity
                    cash -= buy_value
                    cash -= _trade(
                        rows=trade_rows,
                        as_of=pd.Timestamp(as_of),
                        variant=variant,
                        action="buy",
                        symbol=config.ibit_symbol,
                        quantity=quantity,
                        price=ibit_price,
                        turnover_cost_bps=config.turnover_cost_bps,
                    )
            if parking_delta_value > 1e-8:
                buy_value = min(cash, parking_delta_value)
                if buy_value > 1e-8:
                    quantity = buy_value / parking_price
                    parking_shares += quantity
                    cash -= buy_value
                    cash -= _trade(
                        rows=trade_rows,
                        as_of=pd.Timestamp(as_of),
                        variant=variant,
                        action="buy",
                        symbol=config.parking_symbol,
                        quantity=quantity,
                        price=parking_price,
                        turnover_cost_bps=config.turnover_cost_bps,
                    )

        ibit_value = ibit_shares * ibit_price
        parking_value = parking_shares * parking_price
        nav = ibit_value + parking_value + cash
        holding_rows.append(
            {
                "as_of": str(pd.Timestamp(as_of).date()),
                "variant": variant,
                "nav": float(nav),
                "ibit_value": float(ibit_value),
                "parking_value": float(parking_value),
                "cash": float(cash),
                "external_cash_flow": float(external_cash_flow),
                "ibit_weight": float(ibit_value / nav) if nav > 0 else 0.0,
                "parking_weight": float(parking_value / nav) if nav > 0 else 0.0,
                "cash_weight": float(cash / nav) if nav > 0 else 0.0,
            }
        )

    return {
        "trade_ledger": pd.DataFrame(trade_rows),
        "holdings_ledger": pd.DataFrame(holding_rows),
        "signal_consumption": pd.DataFrame(signal_rows),
    }


def build_ibit_smart_dca_research(
    prices: pd.DataFrame,
    *,
    zscore_history: pd.DataFrame | None = None,
    ibit_symbol: str = "IBIT",
    parking_symbol: str = "BOXX",
    parking_proxy_symbol: str = "",
    price_field: str = "adjusted_close",
    primary_benchmark: str = "QQQ",
    secondary_benchmark: str = "SPY",
    btc_proxy_symbol: str = "",
    initial_parking_value: float = 0.0,
    contribution_amount: float = 0.0,
    rebalance_frequency: str = "MS",
    turnover_cost_bps: float = 5.0,
    plugin_enabled: bool = True,
    plugin_config: Mapping[str, Any] | None = None,
    min_drawdown_improvement: float = 0.10,
    max_cagr_giveup_for_drawdown: float = 0.02,
) -> dict[str, pd.DataFrame | dict[str, object]]:
    config = IbitDcaResearchConfig(
        ibit_symbol=_normalize_symbol(ibit_symbol),
        parking_symbol=_normalize_symbol(parking_symbol),
        parking_proxy_symbol=_normalize_symbol(parking_proxy_symbol),
        price_field=normalize_price_field(price_field),
        primary_benchmark=_normalize_symbol(primary_benchmark),
        secondary_benchmark=_normalize_symbol(secondary_benchmark),
        btc_proxy_symbol=_normalize_symbol(btc_proxy_symbol),
        initial_parking_value=float(initial_parking_value),
        contribution_amount=float(contribution_amount),
        rebalance_frequency=str(rebalance_frequency),
        turnover_cost_bps=float(turnover_cost_bps),
        plugin_enabled=bool(plugin_enabled),
        min_drawdown_improvement=float(min_drawdown_improvement),
        max_cagr_giveup_for_drawdown=float(max_cagr_giveup_for_drawdown),
    )
    matrix = _normalize_price_matrix(prices)
    matrix, proxy_metadata = _apply_ibit_proxy_price(
        matrix, ibit_symbol=config.ibit_symbol, btc_proxy_symbol=config.btc_proxy_symbol
    )
    matrix, parking_proxy_metadata = _apply_parking_proxy_price(
        matrix, parking_symbol=config.parking_symbol, parking_proxy_symbol=config.parking_proxy_symbol
    )
    proxy_metadata.update(parking_proxy_metadata)
    variants = [PARKING_ONLY_VARIANT, BUY_ONLY_VARIANT]
    if config.plugin_enabled:
        variants.append(PLUGIN_ON_VARIANT)

    primary_benchmark_stats = _benchmark_stats(matrix, config.primary_benchmark)
    secondary_benchmark_stats = _benchmark_stats(matrix, config.secondary_benchmark)

    trade_frames: list[pd.DataFrame] = []
    holdings_frames: list[pd.DataFrame] = []
    signal_frames: list[pd.DataFrame] = []
    stats_rows: list[dict[str, object]] = []
    for variant in variants:
        simulated = _simulate_variant(
            matrix,
            variant=variant,
            config=config,
            zscore_history=zscore_history,
            plugin_config=plugin_config,
        )
        trade_frames.append(simulated["trade_ledger"])
        holdings = simulated["holdings_ledger"]
        holdings_frames.append(holdings)
        signal_frames.append(simulated["signal_consumption"])
        holdings_indexed = holdings.assign(as_of=pd.to_datetime(holdings["as_of"])).set_index("as_of")
        nav = holdings_indexed["nav"]
        unit_equity = _unitized_equity(nav, holdings_indexed["external_cash_flow"])
        stats = _portfolio_stats(unit_equity)
        ending_nav = float(nav.dropna().iloc[-1]) if not nav.dropna().empty else float("nan")
        stats_rows.append(
            {
                "variant": variant,
                "plugin_enabled": variant == PLUGIN_ON_VARIANT,
                **stats,
                "ending_nav": ending_nav,
                "primary_benchmark": config.primary_benchmark,
                "primary_benchmark_cagr": primary_benchmark_stats["cagr"],
                "primary_benchmark_max_drawdown": primary_benchmark_stats["max_drawdown"],
                "excess_cagr_vs_primary": float(stats["cagr"]) - primary_benchmark_stats["cagr"],
                "drawdown_delta_vs_primary": float(stats["max_drawdown"]) - primary_benchmark_stats["max_drawdown"],
                "secondary_benchmark": config.secondary_benchmark,
                "secondary_benchmark_cagr": secondary_benchmark_stats["cagr"],
                "secondary_benchmark_max_drawdown": secondary_benchmark_stats["max_drawdown"],
                "excess_cagr_vs_secondary": float(stats["cagr"]) - secondary_benchmark_stats["cagr"],
                "drawdown_delta_vs_secondary": float(stats["max_drawdown"]) - secondary_benchmark_stats["max_drawdown"],
            }
        )

    period_summary = pd.DataFrame(stats_rows)
    parking_only = period_summary.loc[period_summary["variant"].eq(PARKING_ONLY_VARIANT)].iloc[0]
    buy_only = period_summary.loc[period_summary["variant"].eq(BUY_ONLY_VARIANT)].iloc[0]
    readiness_rows: list[dict[str, object]] = []
    for _, row in period_summary.iterrows():
        cagr_delta = float(row["cagr"]) - float(buy_only["cagr"])
        drawdown_delta = float(row["max_drawdown"]) - float(buy_only["max_drawdown"])
        cagr_delta_vs_parking = float(row["cagr"]) - float(parking_only["cagr"])
        if row["variant"] == PARKING_ONLY_VARIANT:
            gate = "baseline"
            reason = "parking-only baseline"
        elif row["variant"] == BUY_ONLY_VARIANT:
            gate = "baseline"
            reason = "buy-only DCA baseline"
            cagr_delta = 0.0
            drawdown_delta = 0.0
        else:
            improves_cagr = cagr_delta > 0.0
            improves_drawdown_enough = drawdown_delta > float(config.min_drawdown_improvement)
            cagr_giveup_ok = cagr_delta >= -float(config.max_cagr_giveup_for_drawdown)
            beats_parking = cagr_delta_vs_parking > 0.0
            passes_buy_only_gate = improves_cagr or (improves_drawdown_enough and cagr_giveup_ok)
            gate = "pass" if beats_parking and passes_buy_only_gate else "fail"
            reason = (
                "plugin adds net value versus buy-only DCA and parking-only baseline"
                if gate == "pass"
                else "plugin does not add enough net value versus buy-only DCA and parking-only baseline"
            )
        readiness_rows.append(
            {
                "variant": row["variant"],
                "plugin_enabled": bool(row["plugin_enabled"]),
                "gate": gate,
                "reason": reason,
                "cagr": float(row["cagr"]),
                "max_drawdown": float(row["max_drawdown"]),
                "cagr_delta_vs_buy_only": float(cagr_delta),
                "drawdown_delta_vs_buy_only": float(drawdown_delta),
                "cagr_delta_vs_parking_only": float(cagr_delta_vs_parking),
                "primary_benchmark": row.get("primary_benchmark", ""),
                "primary_benchmark_cagr": float(row.get("primary_benchmark_cagr", float("nan"))),
                "excess_cagr_vs_primary": float(row.get("excess_cagr_vs_primary", float("nan"))),
                "secondary_benchmark": row.get("secondary_benchmark", ""),
                "secondary_benchmark_cagr": float(row.get("secondary_benchmark_cagr", float("nan"))),
                "excess_cagr_vs_secondary": float(row.get("excess_cagr_vs_secondary", float("nan"))),
            }
        )

    return {
        "ibit_dca_period_summary": period_summary,
        "ibit_dca_trade_ledger": pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame(),
        "ibit_dca_holdings_ledger": pd.concat(holdings_frames, ignore_index=True)
        if holdings_frames
        else pd.DataFrame(),
        "ibit_dca_signal_consumption": pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame(),
        "ibit_dca_live_readiness_summary": pd.DataFrame(readiness_rows),
        "manifest_inputs": {
            "schema_version": SCHEMA_VERSION,
            "config": config.to_dict(),
            "plugin_config": dict(plugin_config or {}),
            "proxy": proxy_metadata,
            "zscore_history": _zscore_history_metadata(zscore_history),
            "variants": variants,
        },
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _format_pct(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(number):
        return "n/a"
    return f"{number:.2%}"


def _markdown_table(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    rows = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for _, row in frame.iterrows():
        values = []
        for column in columns:
            value = row.get(column, "")
            if column in {
                "cagr",
                "max_drawdown",
                "cagr_delta_vs_buy_only",
                "drawdown_delta_vs_buy_only",
                "cagr_delta_vs_parking_only",
                "excess_cagr_vs_primary",
                "excess_cagr_vs_secondary",
            }:
                values.append(_format_pct(value))
            else:
                values.append(str(value))
        rows.append("| " + " | ".join(values) + " |")
    return rows


def build_ibit_dca_review_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    readiness = pd.DataFrame(result.get("ibit_dca_live_readiness_summary", pd.DataFrame()))
    signals = pd.DataFrame(result.get("ibit_dca_signal_consumption", pd.DataFrame()))
    plugin_signals = signals.loc[signals.get("variant", pd.Series(dtype=str)).eq(PLUGIN_ON_VARIANT)]
    route_counts = {
        str(route): int(count)
        for route, count in plugin_signals.get("canonical_route", pd.Series(dtype=str)).value_counts().items()
    }
    data_status_counts = {
        str(status): int(count)
        for status, count in plugin_signals.get("signal_data_status", pd.Series(dtype=str)).value_counts().items()
    }
    unavailable_signal_count = int(data_status_counts.get("zscore_unavailable", 0))
    plugin_signal_count = int(len(plugin_signals))
    available_signal_count = int(data_status_counts.get("available", 0))
    zscore_available_signal_ratio = (
        float(available_signal_count) / float(plugin_signal_count) if plugin_signal_count > 0 else 0.0
    )
    zscore_coverage_gate = (
        "pass"
        if plugin_signal_count > 0 and zscore_available_signal_ratio >= MIN_ZSCORE_AVAILABLE_SIGNAL_RATIO_FOR_PROMOTION
        else "fail"
    )
    promotion_blockers: list[str] = []
    if plugin_signal_count <= 0:
        promotion_blockers.append("plugin_signal_missing")
    if zscore_coverage_gate != "pass":
        promotion_blockers.append("zscore_coverage_below_minimum")
    non_normal_signal_count = int(sum(count for route, count in route_counts.items() if route != "normal"))
    manifest_inputs = dict(result.get("manifest_inputs", {}))
    zscore_metadata = dict(manifest_inputs.get("zscore_history", {}))
    plugin_rows = readiness.loc[readiness.get("variant", pd.Series(dtype=str)).eq(PLUGIN_ON_VARIANT)]
    if plugin_rows.empty:
        return {
            "review_status": "plugin_not_evaluated",
            "plugin_gate": "n/a",
            "plugin_reason": "plugin-on variant was not included",
            "runtime_impact": "none",
            "promotion_blockers": [*promotion_blockers, "plugin_on_variant_missing"],
            "plugin_signal_count": plugin_signal_count,
            "plugin_available_signal_count": available_signal_count,
            "plugin_route_counts": route_counts,
            "plugin_signal_data_status_counts": data_status_counts,
            "plugin_unavailable_signal_count": unavailable_signal_count,
            "plugin_non_normal_signal_count": non_normal_signal_count,
            "zscore_coverage_gate": zscore_coverage_gate,
            "zscore_available_signal_ratio": zscore_available_signal_ratio,
            "zscore_min_available_signal_ratio": MIN_ZSCORE_AVAILABLE_SIGNAL_RATIO_FOR_PROMOTION,
            "zscore_history_rows": int(zscore_metadata.get("rows", 0) or 0),
            "zscore_history_start": str(zscore_metadata.get("start", "") or ""),
            "zscore_history_end": str(zscore_metadata.get("end", "") or ""),
        }

    plugin_row = plugin_rows.iloc[0]
    plugin_gate = str(plugin_row.get("gate", "") or "")
    if plugin_gate != "pass":
        promotion_blockers.append("plugin_gate_failed")
    promotion_candidate = plugin_gate == "pass" and zscore_coverage_gate == "pass"
    return {
        "review_status": "candidate_for_live_promotion_review"
        if promotion_candidate
        else "research_reject_or_continue",
        "plugin_gate": plugin_gate,
        "plugin_reason": str(plugin_row.get("reason", "") or ""),
        "runtime_impact": "none",
        "promotion_blockers": promotion_blockers,
        "plugin_signal_count": plugin_signal_count,
        "plugin_available_signal_count": available_signal_count,
        "plugin_route_counts": route_counts,
        "plugin_signal_data_status_counts": data_status_counts,
        "plugin_unavailable_signal_count": unavailable_signal_count,
        "plugin_non_normal_signal_count": non_normal_signal_count,
        "zscore_coverage_gate": zscore_coverage_gate,
        "zscore_available_signal_ratio": zscore_available_signal_ratio,
        "zscore_min_available_signal_ratio": MIN_ZSCORE_AVAILABLE_SIGNAL_RATIO_FOR_PROMOTION,
        "zscore_history_rows": int(zscore_metadata.get("rows", 0) or 0),
        "zscore_history_start": str(zscore_metadata.get("start", "") or ""),
        "zscore_history_end": str(zscore_metadata.get("end", "") or ""),
        "cagr_delta_vs_buy_only": float(plugin_row.get("cagr_delta_vs_buy_only", float("nan"))),
        "drawdown_delta_vs_buy_only": float(plugin_row.get("drawdown_delta_vs_buy_only", float("nan"))),
        "cagr_delta_vs_parking_only": float(plugin_row.get("cagr_delta_vs_parking_only", float("nan"))),
        "excess_cagr_vs_primary": float(plugin_row.get("excess_cagr_vs_primary", float("nan"))),
        "excess_cagr_vs_secondary": float(plugin_row.get("excess_cagr_vs_secondary", float("nan"))),
    }


def render_ibit_dca_research_report(result: Mapping[str, Any]) -> str:
    readiness = pd.DataFrame(result.get("ibit_dca_live_readiness_summary", pd.DataFrame()))
    period_summary = pd.DataFrame(result.get("ibit_dca_period_summary", pd.DataFrame()))
    manifest_inputs = dict(result.get("manifest_inputs", {}))
    config = dict(manifest_inputs.get("config", {}))
    proxy = dict(manifest_inputs.get("proxy", {}))
    review_summary = build_ibit_dca_review_summary(result)

    lines = [
        "# IBIT Smart DCA Research Report",
        "",
        f"- Review status: `{review_summary['review_status']}`",
        f"- Promotion blockers: `{', '.join(review_summary.get('promotion_blockers') or []) or 'none'}`",
        f"- Plugin gate: `{review_summary['plugin_gate']}`",
        f"- Plugin reason: {review_summary['plugin_reason'] or 'n/a'}",
        f"- Z-score history: `{review_summary.get('zscore_history_start') or 'n/a'}` to "
        f"`{review_summary.get('zscore_history_end') or 'n/a'}` "
        f"({review_summary.get('zscore_history_rows', 0)} rows)",
        f"- Plugin non-normal signal count: `{review_summary.get('plugin_non_normal_signal_count', 0)}`",
        f"- Plugin unavailable z-score signal count: `{review_summary.get('plugin_unavailable_signal_count', 0)}`",
        f"- Plugin route counts: `{review_summary.get('plugin_route_counts') or {}}`",
        f"- Plugin signal data-status counts: `{review_summary.get('plugin_signal_data_status_counts') or {}}`",
        f"- Z-score coverage gate: `{review_summary.get('zscore_coverage_gate', 'n/a')}`",
        f"- Z-score available signal ratio: `{_format_pct(review_summary.get('zscore_available_signal_ratio'))}`",
        "- Runtime impact: `none` — this is research evidence only and must not change live allocation by itself.",
        "",
        "## Configuration",
        "",
        f"- IBIT symbol: `{config.get('ibit_symbol', '') or 'n/a'}`",
        f"- Parking symbol: `{config.get('parking_symbol', '') or 'n/a'}`",
        f"- Parking proxy symbol: "
        f"`{proxy.get('parking_proxy_symbol', '') or config.get('parking_proxy_symbol', '') or 'n/a'}`",
        f"- Parking proxy rows filled: `{proxy.get('parking_proxy_rows_filled', 0)}`",
        f"- Price field: `{config.get('price_field', '') or 'n/a'}`",
        f"- Primary benchmark: `{config.get('primary_benchmark', '') or 'n/a'}`",
        f"- Secondary benchmark: `{config.get('secondary_benchmark', '') or 'n/a'}`",
        f"- BTC proxy symbol: `{proxy.get('btc_proxy_symbol', '') or config.get('btc_proxy_symbol', '') or 'n/a'}`",
        f"- Proxy rows filled: `{proxy.get('proxy_rows_filled', 0)}`",
        f"- Contribution amount: `{config.get('contribution_amount', '')}`",
        f"- Rebalance frequency: `{config.get('rebalance_frequency', '')}`",
        "",
        "## Live-readiness gate summary",
        "",
        *_markdown_table(
            readiness,
            (
                "variant",
                "gate",
                "cagr",
                "max_drawdown",
                "cagr_delta_vs_buy_only",
                "drawdown_delta_vs_buy_only",
                "cagr_delta_vs_parking_only",
                "excess_cagr_vs_primary",
                "excess_cagr_vs_secondary",
            ),
        ),
        "",
        "## Period summary",
        "",
        *_markdown_table(
            period_summary,
            (
                "variant",
                "cagr",
                "max_drawdown",
                "ending_nav",
                "excess_cagr_vs_primary",
                "excess_cagr_vs_secondary",
            ),
        ),
        "",
        "## Promotion checklist",
        "",
        "- Plugin-on must beat the parking-only baseline after costs.",
        "- Plugin-on must beat buy-only DCA on net CAGR, or provide enough drawdown improvement to justify CAGR give-up.",
        "- QQQ/SPY excess-CAGR columns must be reviewed before any default enablement.",
        "- A separate promotion artifact and human approval are required before live runtime changes.",
    ]
    return "\n".join(lines).strip() + "\n"


def write_ibit_smart_dca_research_outputs(
    result: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "ibit_dca_period_summary": "ibit_dca_period_summary.csv",
        "ibit_dca_trade_ledger": "ibit_dca_trade_ledger.csv",
        "ibit_dca_holdings_ledger": "ibit_dca_holdings_ledger.csv",
        "ibit_dca_signal_consumption": "ibit_dca_signal_consumption.csv",
        "ibit_dca_live_readiness_summary": "ibit_dca_live_readiness_summary.csv",
    }
    paths: dict[str, Path] = {}
    for key, filename in artifacts.items():
        frame = pd.DataFrame(result.get(key, pd.DataFrame()))
        path = output_root / filename
        frame.to_csv(path, index=False)
        paths[key] = path
    report_path = output_root / "ibit_dca_research_report.md"
    report_path.write_text(render_ibit_dca_research_report(result), encoding="utf-8")
    paths["ibit_dca_research_report"] = report_path

    manifest_inputs = dict(result.get("manifest_inputs", {}))
    manifest = {
        "manifest_type": MANIFEST_TYPE,
        "artifact_schema_version": SCHEMA_VERSION,
        "inputs": manifest_inputs,
        "review_summary": build_ibit_dca_review_summary(result),
        "row_counts": {key: int(len(pd.DataFrame(result.get(key, pd.DataFrame())))) for key in artifacts},
        "artifacts": {
            **{key: {"path": filename} for key, filename in artifacts.items()},
            "ibit_dca_research_report": {"path": "ibit_dca_research_report.md"},
        },
        "outputs": [*artifacts.values(), "ibit_dca_research_report.md", "ibit_dca_research_manifest.json"],
    }
    manifest_path = output_root / "ibit_dca_research_manifest.json"
    manifest_path.write_text(json.dumps(_json_safe(manifest), ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    paths["manifest"] = manifest_path
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research-only IBIT Smart DCA parking/plugin-consumption backtest.")
    parser.add_argument("--prices", default="", help="Long or wide price CSV containing IBIT and parking symbols.")
    parser.add_argument(
        "--download", action="store_true", help="Download IBIT/parking/benchmark/BTC proxy prices with yfinance."
    )
    parser.add_argument("--price-start", default="2014-01-01", help="Start date for --download price history.")
    parser.add_argument("--price-end", default=None, help="Optional end date for --download price history.")
    parser.add_argument("--download-proxy", default=None, help="Optional HTTP(S)/SOCKS proxy for yfinance download.")
    parser.add_argument("--zscore-metrics", default="", help="Optional MVRV/Z-Score CSV for plugin-on variant.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--ibit-symbol", default="IBIT")
    parser.add_argument("--parking-symbol", default="BOXX")
    parser.add_argument(
        "--parking-proxy-symbol",
        default="",
        help="Optional cash-like proxy to backfill pre-parking inception prices.",
    )
    parser.add_argument("--price-field", default="adjusted_close", choices=("adjusted_close", "close"))
    parser.add_argument("--primary-benchmark", default="QQQ")
    parser.add_argument("--secondary-benchmark", default="SPY")
    parser.add_argument(
        "--btc-proxy-symbol",
        default="",
        help="Optional BTC proxy column/symbol to backfill pre-IBIT prices. Use BTC with --download.",
    )
    parser.add_argument("--initial-parking-value", type=float, default=0.0)
    parser.add_argument("--contribution-amount", type=float, default=0.0)
    parser.add_argument("--rebalance-frequency", default="MS")
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--plugin-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--plugin-config-json", default="{}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.download:
        btc_proxy_symbol = args.btc_proxy_symbol or "BTC"
        prices = download_ibit_smart_dca_price_history(
            start=args.price_start,
            end=args.price_end,
            ibit_symbol=args.ibit_symbol,
            parking_symbol=args.parking_symbol,
            parking_proxy_symbol=args.parking_proxy_symbol,
            price_field=args.price_field,
            primary_benchmark=args.primary_benchmark,
            secondary_benchmark=args.secondary_benchmark,
            btc_proxy_symbol=btc_proxy_symbol,
            proxy=args.download_proxy,
        )
    elif str(args.prices).strip():
        btc_proxy_symbol = args.btc_proxy_symbol
        prices = pd.read_csv(args.prices)
    else:
        raise ValueError("either --prices or --download is required")
    zscore_history = pd.read_csv(args.zscore_metrics) if str(args.zscore_metrics).strip() else None
    plugin_config = json.loads(args.plugin_config_json or "{}")
    result = build_ibit_smart_dca_research(
        prices,
        zscore_history=zscore_history,
        ibit_symbol=args.ibit_symbol,
        parking_symbol=args.parking_symbol,
        parking_proxy_symbol=args.parking_proxy_symbol,
        price_field=args.price_field,
        primary_benchmark=args.primary_benchmark,
        secondary_benchmark=args.secondary_benchmark,
        btc_proxy_symbol=btc_proxy_symbol,
        initial_parking_value=args.initial_parking_value,
        contribution_amount=args.contribution_amount,
        rebalance_frequency=args.rebalance_frequency,
        turnover_cost_bps=args.turnover_cost_bps,
        plugin_enabled=bool(args.plugin_enabled),
        plugin_config=plugin_config,
    )
    paths = write_ibit_smart_dca_research_outputs(result, args.output_dir)
    print(f"ibit_dca_research_manifest={paths['manifest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
