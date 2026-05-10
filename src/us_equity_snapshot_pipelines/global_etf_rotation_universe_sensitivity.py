from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from us_equity_strategies.strategies import global_etf_rotation as strategy

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_backtest import DEFAULT_TURNOVER_COST_BPS, summarize_returns
from .taco_panic_rebound_research import price_history_to_close_matrix
from .yfinance_prices import download_price_history

DEFAULT_PRICE_START_DATE = "2015-01-01"
DEFAULT_BASE_POOL = tuple(strategy.RANKING_POOL)
DEFAULT_EXTRA_SECTOR_SYMBOLS = ("XLC", "XLY", "XLI", "XLB", "XBI")
DEFAULT_SAFE_HAVEN = strategy.SAFE_HAVEN
DEFAULT_CANARY_ASSETS = tuple(strategy.CANARY_ASSETS)
DEFAULT_REBALANCE_MONTHS = tuple(strategy.REBALANCE_MONTHS)
DEFAULT_SMA_PERIOD = strategy.SMA_PERIOD
DEFAULT_HOLD_BONUS = 0.02
DEFAULT_CANARY_BAD_THRESHOLD = 4
DEFAULT_CONFIDENCE_THRESHOLD = 1.0
DEFAULT_CONFIDENCE_TOP1_WEIGHT = 0.75
DEFAULT_CONFIDENCE_VOLATILITY_WINDOW = 126
DEFAULT_CONFIDENCE_VOLATILITY_MAX_RATIO = 1.3
DEFAULT_CONFIDENCE_METRIC = "z_gap"


@dataclass(frozen=True)
class PoolVariant:
    name: str
    ranking_pool: tuple[str, ...]


def _normalize_symbols(raw: str | Sequence[str] | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    cleaned: list[str] = []
    for value in values:
        text = str(value or "").strip().upper()
        if text and text not in cleaned:
            cleaned.append(text)
    return tuple(cleaned)


def _build_download_symbols(
    ranking_symbols: Sequence[str],
    *,
    canary_assets: Sequence[str] = DEFAULT_CANARY_ASSETS,
    safe_haven: str = DEFAULT_SAFE_HAVEN,
) -> tuple[str, ...]:
    combined = (*ranking_symbols, *canary_assets, safe_haven)
    return tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in combined if str(symbol).strip()))


def build_pool_variants(
    *,
    base_pool: Sequence[str] = DEFAULT_BASE_POOL,
    extra_sector_symbols: Sequence[str] = DEFAULT_EXTRA_SECTOR_SYMBOLS,
) -> tuple[PoolVariant, ...]:
    base = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in base_pool if str(symbol).strip()))
    extras = tuple(
        dict.fromkeys(str(symbol).strip().upper() for symbol in extra_sector_symbols if str(symbol).strip())
    )
    expanded = tuple(dict.fromkeys((*base, *extras)))
    return (
        PoolVariant(name="base", ranking_pool=base),
        PoolVariant(name="sector_expanded", ranking_pool=expanded),
    )


def _normalize_close(price_history) -> pd.DataFrame:
    close = price_history_to_close_matrix(price_history)
    frame = close.copy().sort_index()
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame.columns = frame.columns.astype(str).str.upper().str.strip()
    return frame


def _ensure_safe_haven(close: pd.DataFrame, safe_haven: str) -> pd.DataFrame:
    frame = close.copy()
    safe_haven = str(safe_haven).strip().upper()
    if safe_haven not in frame.columns:
        frame[safe_haven] = 1.0
    return frame.sort_index()


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): float(weight)
        for symbol, weight in weights.items()
        if pd.notna(weight) and abs(float(weight)) > 1e-12
    }
    total = sum(cleaned.values())
    if total <= 0:
        return {DEFAULT_SAFE_HAVEN: 1.0}
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def _run_single_variant(
    close: pd.DataFrame,
    *,
    ranking_pool: Sequence[str],
    safe_haven: str = DEFAULT_SAFE_HAVEN,
    canary_assets: Sequence[str] = DEFAULT_CANARY_ASSETS,
    rebalance_months: Sequence[int] = DEFAULT_REBALANCE_MONTHS,
    sma_period: int = DEFAULT_SMA_PERIOD,
    confidence_weighting_enabled: bool = False,
    confidence_volatility_gate_enabled: bool = False,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    frame = _ensure_safe_haven(close, safe_haven)
    returns = frame.pct_change().fillna(0.0)
    index = frame.index
    symbols = tuple(dict.fromkeys((*ranking_pool, *canary_assets, safe_haven)))
    weights_history = pd.DataFrame(0.0, index=index, columns=symbols)
    portfolio_returns = pd.Series(0.0, index=index, name="portfolio")

    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: tuple[str, ...] = ()

    for pos, date in enumerate(index[:-1]):
        cutoff = pd.Timestamp(date).normalize()

        def get_historical_close(_ib, ticker: str, *, _cutoff=cutoff) -> pd.Series:
            symbol = str(ticker).strip().upper()
            if symbol not in frame.columns:
                return pd.Series(dtype=float)
            return frame.loc[:_cutoff, symbol].dropna()

        weights, _signal, _is_emergency, _canary = strategy.compute_signals(
            None,
            current_holdings,
            get_historical_close=get_historical_close,
            as_of_date=date,
            ranking_pool=tuple(ranking_pool),
            canary_assets=tuple(canary_assets),
            safe_haven=safe_haven,
            top_n=2,
            hold_bonus=DEFAULT_HOLD_BONUS,
            canary_bad_threshold=DEFAULT_CANARY_BAD_THRESHOLD,
            rebalance_months=tuple(rebalance_months),
            translator=lambda key, **kwargs: key,
            pacing_sec=0.0,
            sma_period=int(sma_period),
            confidence_weighting_enabled=bool(confidence_weighting_enabled),
            confidence_metric=DEFAULT_CONFIDENCE_METRIC,
            confidence_threshold=DEFAULT_CONFIDENCE_THRESHOLD,
            confidence_top1_weight=DEFAULT_CONFIDENCE_TOP1_WEIGHT,
            confidence_volatility_gate_enabled=bool(confidence_volatility_gate_enabled),
            confidence_volatility_window=DEFAULT_CONFIDENCE_VOLATILITY_WINDOW,
            confidence_volatility_max_ratio=DEFAULT_CONFIDENCE_VOLATILITY_MAX_RATIO,
        )

        if weights is not None:
            current_weights = _normalize_weights(weights)
            current_holdings = tuple(
                symbol for symbol in current_weights if symbol != safe_haven and float(current_weights[symbol]) > 0.0
            )

        for symbol, weight in current_weights.items():
            if symbol in weights_history.columns:
                weights_history.at[date, symbol] = float(weight)

        next_date = index[pos + 1]
        next_returns = returns.loc[next_date]
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            gross_return += float(weight) * float(next_returns.get(symbol, 0.0))
        portfolio_returns.at[next_date] = gross_return

    if not weights_history.empty:
        for symbol, weight in current_weights.items():
            if symbol in weights_history.columns:
                weights_history.at[index[-1], symbol] = float(weight)

    summary = summarize_returns(
        portfolio_returns,
        strategy_name="global_etf_rotation",
        weights_history=weights_history.loc[:, (weights_history != 0.0).any(axis=0)],
    )
    summary["Ranking Pool Size"] = int(len(tuple(ranking_pool)))
    summary["Ranking Pool"] = ",".join(tuple(ranking_pool))
    summary["Confidence Weighting"] = bool(confidence_weighting_enabled)
    summary["Confidence Volatility Gate"] = bool(confidence_volatility_gate_enabled)
    summary["Safe Haven"] = safe_haven
    summary["Turnover Cost Bps"] = float(turnover_cost_bps)
    return summary


def run_universe_sensitivity(
    price_history,
    *,
    variants: Sequence[PoolVariant] | None = None,
    safe_haven: str = DEFAULT_SAFE_HAVEN,
    canary_assets: Sequence[str] = DEFAULT_CANARY_ASSETS,
    rebalance_months: Sequence[int] = DEFAULT_REBALANCE_MONTHS,
    sma_period: int = DEFAULT_SMA_PERIOD,
    confidence_weighting_enabled: bool = False,
    confidence_volatility_gate_enabled: bool = False,
) -> pd.DataFrame:
    close = _normalize_close(price_history)
    pool_variants = tuple(variants or build_pool_variants())
    rows: list[dict[str, object]] = []
    for variant in pool_variants:
        summary = _run_single_variant(
            close,
            ranking_pool=variant.ranking_pool,
            safe_haven=safe_haven,
            canary_assets=canary_assets,
            rebalance_months=rebalance_months,
            sma_period=sma_period,
            confidence_weighting_enabled=confidence_weighting_enabled,
            confidence_volatility_gate_enabled=confidence_volatility_gate_enabled,
        )
        rows.append({"Variant": variant.name, **summary})
    return pd.DataFrame(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare global ETF universe expansion variants.")
    parser.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--symbols", help="Comma-separated symbols to download when --prices is omitted")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--extra-sector-symbols", default=",".join(DEFAULT_EXTRA_SECTOR_SYMBOLS))
    parser.add_argument("--sma-period", type=int, default=DEFAULT_SMA_PERIOD)
    parser.add_argument("--confidence-weighting", action="store_true")
    parser.add_argument("--confidence-volatility-gate", action="store_true")
    parser.add_argument("--safe-haven", default=DEFAULT_SAFE_HAVEN)
    parser.add_argument("--output", help="Optional CSV output path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.prices:
        price_history = read_table(Path(args.prices))
    else:
        symbols = _normalize_symbols(args.symbols)
        if not symbols:
            raise SystemExit("either --prices or --symbols must be supplied")
        download_symbols = _build_download_symbols(
            symbols,
            canary_assets=DEFAULT_CANARY_ASSETS,
            safe_haven=str(args.safe_haven),
        )
        price_history = download_price_history(
            list(download_symbols),
            start=str(args.price_start),
            end=args.price_end,
        )

    variants = build_pool_variants(extra_sector_symbols=_normalize_symbols(args.extra_sector_symbols))
    result = run_universe_sensitivity(
        price_history,
        variants=variants,
        safe_haven=str(args.safe_haven),
        sma_period=int(args.sma_period),
        confidence_weighting_enabled=bool(args.confidence_weighting),
        confidence_volatility_gate_enabled=bool(args.confidence_volatility_gate),
    )
    result = result.sort_values(["CAGR", "Max Drawdown"], ascending=[False, True]).reset_index(drop=True)

    if args.output:
        Path(args.output).write_text(result.to_csv(index=False))
    else:
        print(result.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
