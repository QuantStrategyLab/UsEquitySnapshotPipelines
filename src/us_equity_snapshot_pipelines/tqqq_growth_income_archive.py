from __future__ import annotations

import argparse
import inspect
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from us_equity_strategies.manifests import tqqq_growth_income_manifest
from us_equity_strategies.strategies.tqqq_growth_income import build_rebalance_plan

from .artifacts import sha256_file, write_json
from .backtest_windows import build_benchmark_returns, build_window_summary
from .contracts import SOURCE_PROJECT
from .soxl_soxx_trend_income_archive import (
    _archive_proxy_used,
    _build_data_quality_report,
    _merge_symbol_aliases,
    _sanitize_argv,
    _source_ranges,
)
from .soxl_soxx_trend_income_backtest import (
    _build_close_matrix,
    _build_price_frame,
    _format_summary,
    _summarize_returns,
)
from .yfinance_prices import download_price_history_with_proxy_candidates, load_proxy_candidates

PROFILE = "tqqq_growth_income"
MANAGED_SYMBOLS = ("TQQQ", "QQQM", "BOXX", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI")
DEFAULT_INITIAL_EQUITY_USD = 100_000.0
DEFAULT_PRICE_START = "2009-01-01"
DEFAULT_BACKTEST_START = "2010-02-11"
DEFAULT_FULL_BACKTEST_START = "2024-01-31"
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_OUTPUT_ROOT = "data/output"


@dataclass(frozen=True)
class ArchiveModeSpec:
    mode: str
    description: str
    symbols: tuple[str, ...]
    price_start: str
    backtest_start: str
    symbol_aliases: Mapping[str, tuple[str, ...]]
    disable_income_layer: bool


ARCHIVE_MODE_SPECS = {
    "real-core": ArchiveModeSpec(
        mode="real-core",
        description="Real-product TQQQ/QQQM core archive with QQQ as the QQQM long-history proxy and BOXX downloaded through a cash proxy; income layer disabled for long-history cycle replay.",
        symbols=("TQQQ", "QQQM", "QQQ", "BOXX", "SPY"),
        price_start=DEFAULT_PRICE_START,
        backtest_start=DEFAULT_BACKTEST_START,
        symbol_aliases={"QQQM": ("QQQ",), "BOXX": ("BIL",)},
        disable_income_layer=True,
    ),
    "real-full": ArchiveModeSpec(
        mode="real-full",
        description="Real-product TQQQ/QQQM archive with QQQ as the QQQM long-history proxy and the production diversified income layer enabled.",
        symbols=(*MANAGED_SYMBOLS, "QQQ", "SPY"),
        price_start=DEFAULT_PRICE_START,
        backtest_start=DEFAULT_FULL_BACKTEST_START,
        symbol_aliases={"QQQM": ("QQQ",), "BOXX": ("BIL",)},
        disable_income_layer=False,
    ),
}


def _strategy_kwargs(overrides: Mapping[str, object] | None = None) -> dict[str, object]:
    config = dict(tqqq_growth_income_manifest.default_config)
    config.update(dict(overrides or {}))
    accepted_keys = {
        name
        for name, parameter in inspect.signature(build_rebalance_plan).parameters.items()
        if parameter.kind in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
    }
    return {key: value for key, value in config.items() if key in accepted_keys}


def _income_disabled_overrides() -> dict[str, object]:
    return {
        "income_threshold_usd": 1e18,
        "income_layer_enabled": False,
        "income_layer_start_usd": 1e18,
        "income_layer_max_ratio": 0.0,
    }


def _build_account_snapshot(
    *,
    weights: Mapping[str, float],
    equity: float,
    close_prices: Mapping[str, float],
) -> SimpleNamespace:
    positions = []
    for symbol in MANAGED_SYMBOLS:
        market_value = float(equity) * float(weights.get(symbol, 0.0))
        price = float(close_prices.get(symbol, 0.0) or 0.0)
        quantity = market_value / price if price > 0.0 else 0.0
        positions.append(SimpleNamespace(symbol=symbol, market_value=market_value, quantity=quantity))
    return SimpleNamespace(
        positions=tuple(positions),
        total_equity=float(equity),
        buying_power=float(equity),
        metadata={},
    )


def _execute_rebalance(
    *,
    current_weights: Mapping[str, float],
    target_values: Mapping[str, float],
    equity: float,
    threshold_value: float,
    turnover_cost_bps: float,
    sell_order: Sequence[str],
    buy_order: Sequence[str],
) -> tuple[dict[str, float], float, float]:
    current_market_values = {
        symbol: float(equity) * float(current_weights.get(symbol, 0.0))
        for symbol in MANAGED_SYMBOLS
    }
    next_market_values = dict(current_market_values)
    cash = float(equity) - sum(current_market_values.values())
    if abs(cash) < 1e-9:
        cash = 0.0

    for symbol in sell_order:
        current = current_market_values.get(symbol, 0.0)
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff >= 0.0 or abs(diff) <= threshold_value:
            continue
        next_market_values[symbol] = target
        cash -= diff

    for symbol in buy_order:
        current = next_market_values.get(symbol, current_market_values.get(symbol, 0.0))
        target = float(target_values.get(symbol, 0.0))
        diff = target - current
        if diff <= 0.0 or diff <= threshold_value:
            continue
        buy_value = min(diff, cash)
        if buy_value <= 0.0:
            continue
        next_market_values[symbol] = current + buy_value
        cash -= buy_value

    turnover = 0.5 * sum(
        abs(float(next_market_values.get(symbol, 0.0)) - float(current_market_values.get(symbol, 0.0)))
        for symbol in MANAGED_SYMBOLS
    ) / float(equity)
    cost = float(equity) * turnover * (float(turnover_cost_bps) / 10_000.0)
    cash = max(0.0, cash - cost)
    new_equity = cash + sum(next_market_values.values())
    if new_equity <= 0.0:
        return dict(current_weights), 0.0, 0.0

    next_weights = {symbol: float(next_market_values.get(symbol, 0.0)) / new_equity for symbol in MANAGED_SYMBOLS}
    next_weights["__cash__"] = cash / new_equity
    return next_weights, turnover, new_equity


def run_backtest(
    price_history,
    *,
    initial_equity: float = DEFAULT_INITIAL_EQUITY_USD,
    start_date: str = DEFAULT_BACKTEST_START,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    strategy_overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    prices = _build_price_frame(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    close_matrix = _build_close_matrix(prices)
    close_matrix = close_matrix.ffill()
    required = {"TQQQ", "QQQM", "QQQ", "BOXX"}
    missing = sorted(symbol for symbol in required if symbol not in close_matrix.columns)
    if missing:
        raise RuntimeError(f"price history missing required symbols: {', '.join(missing)}")

    index = close_matrix.dropna(subset=["TQQQ", "QQQ", "BOXX"]).index
    index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")

    qqq_history = prices.loc[prices["symbol"].eq("QQQ"), ["as_of", "close"]].sort_values("as_of").reset_index(drop=True)
    qqq_dates = pd.DatetimeIndex(qqq_history["as_of"])
    weights_history = pd.DataFrame(0.0, index=index, columns=[*MANAGED_SYMBOLS, "__cash__"])
    portfolio_returns = pd.Series(index=index, dtype=float, name="portfolio_return")
    turnover_history = pd.Series(index=index, dtype=float, name="turnover")
    signal_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    current_weights = {symbol: 0.0 for symbol in MANAGED_SYMBOLS}
    current_weights["BOXX"] = 1.0
    current_weights["__cash__"] = 0.0
    current_equity = float(initial_equity)
    strategy_kwargs = _strategy_kwargs(strategy_overrides)
    volatility_window = max(1, int(strategy_kwargs.get("dual_drive_volatility_delever_window") or 5))
    dynamic_lookback = max(1, int(strategy_kwargs.get("dual_drive_volatility_delever_dynamic_lookback") or 252))
    pullback_window = max(1, int(strategy_kwargs.get("dual_drive_pullback_rebound_window") or 20))
    strategy_history_window = max(260, dynamic_lookback + volatility_window + 10, pullback_window + 220)

    for as_of in index[:-1]:
        next_as_of = index[index.get_loc(as_of) + 1]
        close_row = close_matrix.loc[as_of]
        next_close_row = close_matrix.loc[next_as_of]
        history_end = int(qqq_dates.searchsorted(as_of, side="right"))
        if history_end < 220:
            continue
        history = qqq_history.iloc[max(0, history_end - strategy_history_window):history_end]

        snapshot = _build_account_snapshot(
            weights=current_weights,
            equity=current_equity,
            close_prices={symbol: float(close_row.get(symbol, np.nan)) for symbol in MANAGED_SYMBOLS},
        )
        plan = build_rebalance_plan(
            history,
            snapshot,
            signal_text_fn=str,
            translator=lambda key, **kwargs: key,
            **strategy_kwargs,
        )
        target_values = dict(plan["target_values"])
        next_weights, turnover, next_equity = _execute_rebalance(
            current_weights=current_weights,
            target_values=target_values,
            equity=current_equity,
            threshold_value=float(plan["threshold"]),
            turnover_cost_bps=float(turnover_cost_bps),
            sell_order=plan["sell_order_symbols"],
            buy_order=plan["buy_order_symbols"],
        )
        if turnover > 0.0:
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

        signal_row = {
            "signal_date": as_of,
            "effective_date": next_as_of,
            "signal": plan["sig_display"],
            "qqq_price": plan["qqq_p"],
            "ma200": plan["ma200"],
            "pullback_rebound": plan.get("pullback_rebound"),
            "pullback_rebound_threshold": plan.get("pullback_rebound_threshold"),
            "income_layer_ratio": plan.get("income_layer_ratio"),
            "income_layer_value": plan.get("income_layer_value"),
            "income_layer_activation_band_ratio": plan.get("income_layer_activation_band_ratio"),
            "income_layer_activation_multiplier": plan.get("income_layer_activation_multiplier"),
            "income_layer_activation_end_usd": plan.get("income_layer_activation_end_usd"),
            "income_layer_ratio_mode": plan.get("income_layer_ratio_mode"),
            "income_layer_log_ratio": plan.get("income_layer_log_ratio"),
            "income_layer_loss_budget_ratio": plan.get("income_layer_loss_budget_ratio"),
            "income_layer_loss_budget_cap_ratio": plan.get("income_layer_loss_budget_cap_ratio"),
            "income_layer_stress_drawdown_ratio": plan.get("income_layer_stress_drawdown_ratio"),
            "dual_drive_volatility_delever_threshold_mode": plan.get(
                "dual_drive_volatility_delever_threshold_mode"
            ),
            "dual_drive_volatility_delever_window": plan.get("dual_drive_volatility_delever_window"),
            "dual_drive_volatility_delever_metric": plan.get("dual_drive_volatility_delever_metric"),
            "dual_drive_volatility_delever_threshold": plan.get("dual_drive_volatility_delever_threshold"),
            "dual_drive_volatility_delever_exit_threshold": plan.get(
                "dual_drive_volatility_delever_exit_threshold"
            ),
            "dual_drive_volatility_delever_dynamic_threshold": plan.get(
                "dual_drive_volatility_delever_dynamic_threshold"
            ),
            "dual_drive_volatility_delever_dynamic_sample_count": plan.get(
                "dual_drive_volatility_delever_dynamic_sample_count"
            ),
            "dual_drive_volatility_delever_dynamic_lookback": plan.get(
                "dual_drive_volatility_delever_dynamic_lookback"
            ),
            "dual_drive_volatility_delever_dynamic_percentile": plan.get(
                "dual_drive_volatility_delever_dynamic_percentile"
            ),
            "dual_drive_volatility_delever_dynamic_floor": plan.get(
                "dual_drive_volatility_delever_dynamic_floor"
            ),
            "dual_drive_volatility_delever_dynamic_cap": plan.get(
                "dual_drive_volatility_delever_dynamic_cap"
            ),
            "dual_drive_volatility_delever_triggered": plan.get("dual_drive_volatility_delever_triggered"),
            "dual_drive_volatility_delever_applied": plan.get("dual_drive_volatility_delever_applied"),
            "dual_drive_volatility_delever_trigger_reason": plan.get(
                "dual_drive_volatility_delever_trigger_reason"
            ),
            "threshold": plan["threshold"],
            "total_equity": current_equity,
        }
        for symbol in MANAGED_SYMBOLS:
            signal_row[f"target_{symbol.lower()}"] = target_values.get(symbol, 0.0)
        signal_rows.append(signal_row)
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
            float(next_market_values[symbol])
            * (float(next_close_row.get(symbol, np.nan)) / float(close_row.get(symbol)))
            if symbol in next_close_row and pd.notna(close_row.get(symbol)) and float(close_row.get(symbol)) > 0.0
            else float(next_market_values[symbol])
            for symbol in MANAGED_SYMBOLS
        )
        portfolio_returns.at[next_as_of] = (
            equity_after_return / current_equity - 1.0 if current_equity > 0.0 else np.nan
        )
        for symbol in MANAGED_SYMBOLS:
            weights_history.at[next_as_of, symbol] = float(current_weights.get(symbol, 0.0))
        weights_history.at[next_as_of, "__cash__"] = float(current_weights.get("__cash__", 0.0))
        current_equity = float(equity_after_return)

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = _summarize_returns(portfolio_returns, used_weights)
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history.fillna(0.0),
        "trades": pd.DataFrame(trade_rows),
        "signal_history": pd.DataFrame(signal_rows),
    }


def _git_info(path: str | Path) -> dict[str, object]:
    repo = Path(path)
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
        status = subprocess.check_output(["git", "status", "--short"], cwd=repo, text=True).strip()
    except Exception:
        return {"commit": None, "dirty": None}
    return {"commit": commit, "dirty": bool(status)}


def _write_outputs(
    *,
    output_dir: Path,
    prices: pd.DataFrame,
    result: Mapping[str, object],
    config: Mapping[str, object],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    price_frame = _build_price_frame(prices)
    price_frame.to_csv(output_dir / "price_history.csv", index=False)
    _format_summary(result["summary"]).to_csv(output_dir / "summary.csv", index=False)
    build_window_summary(
        result["portfolio_returns"],
        benchmark_returns=build_benchmark_returns(price_frame),
    ).to_csv(output_dir / "window_summary.csv", index=False)
    result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
    result["weights_history"].to_csv(output_dir / "weights_history.csv")
    result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
    result["trades"].to_csv(output_dir / "trades.csv", index=False)
    result["signal_history"].to_csv(output_dir / "signal_history.csv", index=False)
    write_json(output_dir / "backtest_config.json", config)


def archive_backtest(
    *,
    mode: str,
    output_dir: str | Path,
    prices: pd.DataFrame | None = None,
    prices_path: str | Path | None = None,
    price_start: str | None = None,
    price_end: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    initial_equity: float = DEFAULT_INITIAL_EQUITY_USD,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    symbol_aliases: Mapping[str, Sequence[str]] | None = None,
    proxy: str | None = None,
    proxy_list: str | None = None,
    proxy_list_max: int = 12,
    sanitized_argv: Sequence[str] | None = None,
    download_fn: Callable | None = None,
) -> Path:
    if mode not in ARCHIVE_MODE_SPECS:
        expected = ", ".join(sorted(ARCHIVE_MODE_SPECS))
        raise ValueError(f"unsupported archive mode {mode!r}; expected one of: {expected}")
    if prices is not None and prices_path is not None:
        raise ValueError("provide either prices or prices_path, not both")

    spec = ARCHIVE_MODE_SPECS[mode]
    output_path = Path(output_dir)
    merged_aliases = _merge_symbol_aliases(spec.symbol_aliases, symbol_aliases)
    effective_price_start = price_start or spec.price_start
    effective_start = start_date or spec.backtest_start
    source_kind = "local_prices"
    source_path = str(prices_path) if prices_path is not None else None
    if prices is None:
        if prices_path is not None:
            prices = pd.read_csv(prices_path)
        else:
            source_kind = "yfinance"
            prices = download_price_history_with_proxy_candidates(
                list(spec.symbols),
                start=effective_price_start,
                end=price_end,
                chunk_size=25,
                download_fn=download_fn,
                symbol_aliases=merged_aliases,
                proxy=proxy,
                proxy_candidates=load_proxy_candidates(proxy_list, max_candidates=proxy_list_max) if proxy_list else None,
            )

    data_quality_report = _build_data_quality_report(
        prices,
        requested_symbols=spec.symbols,
        symbol_aliases=merged_aliases,
    )
    missing_symbols = sorted(data_quality_report.loc[data_quality_report["row_count"].eq(0), "symbol"].tolist())
    if missing_symbols:
        raise RuntimeError(f"archive price history missing required symbols: {', '.join(missing_symbols)}")

    result = run_backtest(
        prices,
        initial_equity=float(initial_equity),
        start_date=effective_start,
        end_date=end_date,
        turnover_cost_bps=float(turnover_cost_bps),
        strategy_overrides=_income_disabled_overrides() if spec.disable_income_layer else None,
    )
    config = {
        "profile": PROFILE,
        "archive_mode": spec.mode,
        "archive_mode_description": spec.description,
        "initial_equity": float(initial_equity),
        "price_start": effective_price_start,
        "price_end": price_end,
        "start_date": effective_start,
        "end_date": end_date,
        "turnover_cost_bps": float(turnover_cost_bps),
        "symbols": list(spec.symbols),
        "symbol_aliases": {key: list(value) for key, value in sorted(merged_aliases.items())},
        "disable_income_layer": bool(spec.disable_income_layer),
    }
    _write_outputs(output_dir=output_path, prices=prices, result=result, config=config)
    data_quality_report.to_csv(output_path / "data_quality_report.csv", index=False)

    artifact_names = (
        "price_history",
        "summary",
        "window_summary",
        "portfolio_returns",
        "weights_history",
        "turnover_history",
        "trades",
        "signal_history",
        "backtest_config",
        "data_quality_report",
    )
    artifact_paths = {
        name: output_path / f"{name}.csv"
        for name in artifact_names
        if name not in {"backtest_config"}
    }
    artifact_paths["backtest_config"] = output_path / "backtest_config.json"
    manifest = {
        "manifest_type": "research_backtest_archive",
        "source_project": SOURCE_PROJECT,
        "strategy_profile": PROFILE,
        "archive_mode": spec.mode,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_kind": source_kind,
        "price_source": {
            "provider": "yfinance" if source_kind == "yfinance" else "local_prices",
            "auto_adjust": True if source_kind == "yfinance" else None,
            "prices_path": source_path,
            "price_start": effective_price_start,
            "price_end": price_end,
            "requested_symbols": list(spec.symbols),
            "symbol_aliases": {key: list(value) for key, value in sorted(merged_aliases.items())},
            "proxy_used": _archive_proxy_used(source_kind=source_kind, proxy=proxy, proxy_list=proxy_list),
            "disable_income_layer": bool(spec.disable_income_layer),
        },
        "data_quality": _source_ranges(data_quality_report),
        "backtest": config,
        "artifacts": {
            name: {"path": str(path), "sha256": sha256_file(path)}
            for name, path in artifact_paths.items()
        },
        "code": {
            "us_equity_snapshot_pipelines": _git_info(Path(__file__).resolve().parents[2]),
            "us_equity_strategies": _git_info(Path(__file__).resolve().parents[3] / "UsEquityStrategies"),
        },
        "command": {
            "argv": list(sanitized_argv or []),
        },
    }
    write_json(output_path / "source_manifest.json", manifest)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive replayable TQQQ growth-income backtests.")
    parser.add_argument("--mode", choices=tuple(sorted(ARCHIVE_MODE_SPECS)), default="real-core")
    parser.add_argument("--prices")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--price-start")
    parser.add_argument("--price-end")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--symbol-alias", action="append", default=[])
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-list")
    parser.add_argument("--proxy-list-max", type=int, default=12)
    parser.add_argument("--output-dir")
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--archive-date")
    return parser


def _parse_symbol_aliases(values: Sequence[str] | None) -> dict[str, tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {}
    for raw_value in values or ():
        key, separator, raw_candidates = str(raw_value or "").partition("=")
        symbol = key.strip().upper()
        if not separator or not symbol:
            raise ValueError(f"invalid symbol alias {raw_value!r}; expected SYMBOL=CANDIDATE[,CANDIDATE]")
        candidates = tuple(
            candidate.strip().upper().replace(".", "-")
            for candidate in raw_candidates.split(",")
            if candidate.strip()
        )
        if not candidates:
            raise ValueError(f"invalid symbol alias {raw_value!r}; expected at least one candidate")
        aliases[symbol] = candidates
    return aliases


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.prices and args.download:
        raise SystemExit("provide either --prices or --download, not both")
    archive_date = args.archive_date or datetime.now(timezone.utc).date().isoformat()
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(args.output_root) / f"{PROFILE}_{args.mode.replace('-', '_')}_archive_{archive_date}"
    )
    archive_dir = archive_backtest(
        mode=args.mode,
        output_dir=output_dir,
        prices_path=args.prices,
        price_start=args.price_start,
        price_end=args.price_end,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_equity=float(args.initial_equity),
        turnover_cost_bps=float(args.turnover_cost_bps),
        symbol_aliases=_parse_symbol_aliases(args.symbol_alias),
        proxy=args.proxy,
        proxy_list=args.proxy_list,
        proxy_list_max=int(args.proxy_list_max),
        sanitized_argv=_sanitize_argv(sys.argv[1:] if argv is None else argv),
    )
    print(f"wrote TQQQ growth-income archive -> {archive_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARCHIVE_MODE_SPECS",
    "DEFAULT_BACKTEST_START",
    "DEFAULT_FULL_BACKTEST_START",
    "DEFAULT_INITIAL_EQUITY_USD",
    "DEFAULT_PRICE_START",
    "DEFAULT_TURNOVER_COST_BPS",
    "MANAGED_SYMBOLS",
    "PROFILE",
    "archive_backtest",
    "main",
    "run_backtest",
]
