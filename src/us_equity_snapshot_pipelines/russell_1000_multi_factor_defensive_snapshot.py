from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

PRICE_HISTORY_REQUIRED_COLUMNS = frozenset({"symbol", "as_of", "close", "volume"})
UNIVERSE_REQUIRED_COLUMNS = frozenset({"symbol", "sector"})
FEATURE_SNAPSHOT_COLUMNS = (
    "as_of",
    "symbol",
    "sector",
    "close",
    "volume",
    "adv20_usd",
    "history_days",
    "mom_6_1",
    "mom_12_1",
    "sma200_gap",
    "vol_63",
    "maxdd_126",
    "eligible",
)


def read_table(path: str | Path) -> pd.DataFrame:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise EnvironmentError("path is required")
    table_path = Path(raw_path)
    if not table_path.exists():
        raise FileNotFoundError(f"file not found: {table_path}")

    suffix = table_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(table_path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(table_path, orient="records", lines=suffix == ".jsonl")
    if suffix == ".parquet":
        return pd.read_parquet(table_path)
    raise ValueError("Unsupported table format; expected .csv, .json, .jsonl, or .parquet")


def write_table(frame: pd.DataFrame, path: str | Path) -> None:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise EnvironmentError("path is required")
    table_path = Path(raw_path)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    suffix = table_path.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(table_path, index=False)
        return
    if suffix == ".json":
        frame.to_json(table_path, orient="records", indent=2, date_format="iso")
        return
    if suffix == ".jsonl":
        frame.to_json(table_path, orient="records", lines=True, date_format="iso")
        return
    if suffix == ".parquet":
        frame.to_parquet(table_path, index=False)
        return
    raise ValueError("Unsupported table format; expected .csv, .json, .jsonl, or .parquet")


def _require_columns(frame: pd.DataFrame, required: frozenset[str], *, name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{name} missing required columns: {missing_text}")


def _normalize_symbol_series(values: pd.Series) -> pd.Series:
    return values.astype(str).str.upper().str.strip()


def _normalize_date(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _compute_skip_return(closes: pd.Series, *, skip_days: int, lookback_days: int) -> float:
    required = skip_days + lookback_days
    if len(closes) <= required:
        return float("nan")
    end_price = closes.iloc[-1 - skip_days]
    start_price = closes.iloc[-1 - required]
    if pd.isna(end_price) or pd.isna(start_price) or start_price <= 0:
        return float("nan")
    return float(end_price / start_price - 1.0)


def _compute_window_drawdown(closes: pd.Series) -> float:
    if closes.empty:
        return float("nan")
    running_peak = closes.cummax()
    drawdown = closes / running_peak - 1.0
    return float(drawdown.min())


def _build_feature_row(
    history: pd.DataFrame,
    *,
    symbol: str,
    sector: str,
    as_of: pd.Timestamp,
    min_price_usd: float,
    min_adv20_usd: float,
    min_history_days: int,
    momentum_skip_days: int,
    momentum_6m_lookback_days: int,
    momentum_12m_lookback_days: int,
    sma_window: int,
    vol_window: int,
    drawdown_window: int,
    force_ineligible: bool = False,
) -> dict[str, object]:
    if history.empty:
        return {
            "as_of": as_of,
            "symbol": symbol,
            "sector": sector,
            "close": float("nan"),
            "volume": float("nan"),
            "adv20_usd": float("nan"),
            "history_days": 0,
            "mom_6_1": float("nan"),
            "mom_12_1": float("nan"),
            "sma200_gap": float("nan"),
            "vol_63": float("nan"),
            "maxdd_126": float("nan"),
            "eligible": False,
        }

    history = history.sort_values("as_of")
    closes = pd.to_numeric(history["close"], errors="coerce")
    volumes = pd.to_numeric(history["volume"], errors="coerce")
    dollar_volume = closes * volumes
    returns = closes.pct_change()

    latest_close = float(closes.iloc[-1])
    latest_volume = float(volumes.iloc[-1]) if not pd.isna(volumes.iloc[-1]) else float("nan")
    adv20_usd = float(dollar_volume.tail(20).mean()) if len(dollar_volume) >= 20 else float("nan")
    mom_6_1 = _compute_skip_return(
        closes,
        skip_days=momentum_skip_days,
        lookback_days=momentum_6m_lookback_days,
    )
    mom_12_1 = _compute_skip_return(
        closes,
        skip_days=momentum_skip_days,
        lookback_days=momentum_12m_lookback_days,
    )
    sma200_gap = (
        float(latest_close / closes.tail(sma_window).mean() - 1.0)
        if len(closes) >= sma_window
        else float("nan")
    )
    vol_63 = (
        float(returns.tail(vol_window).std(ddof=0) * math.sqrt(252))
        if returns.tail(vol_window).notna().sum() >= vol_window
        else float("nan")
    )
    maxdd_126 = (
        _compute_window_drawdown(closes.tail(drawdown_window))
        if len(closes) >= drawdown_window
        else float("nan")
    )

    feature_values = (mom_6_1, mom_12_1, sma200_gap, vol_63, maxdd_126)
    eligible = (
        not force_ineligible
        and len(closes) >= min_history_days
        and latest_close > min_price_usd
        and not pd.isna(adv20_usd)
        and adv20_usd >= min_adv20_usd
        and all(not pd.isna(value) for value in feature_values)
    )

    return {
        "as_of": as_of,
        "symbol": symbol,
        "sector": sector,
        "close": latest_close,
        "volume": latest_volume,
        "adv20_usd": adv20_usd,
        "history_days": int(len(closes)),
        "mom_6_1": mom_6_1,
        "mom_12_1": mom_12_1,
        "sma200_gap": sma200_gap,
        "vol_63": vol_63,
        "maxdd_126": maxdd_126,
        "eligible": bool(eligible),
    }


def _normalize_price_groups(
    price_history,
    *,
    as_of: pd.Timestamp,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    if isinstance(price_history, Mapping):
        price_groups: dict[str, pd.DataFrame] = {}
        empty_history = pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
        for raw_symbol, raw_history in price_history.items():
            history = pd.DataFrame(raw_history).copy()
            if history.empty:
                continue
            _require_columns(history, PRICE_HISTORY_REQUIRED_COLUMNS, name=f"price_history[{raw_symbol!r}]")
            history["symbol"] = _normalize_symbol_series(history["symbol"])
            history["as_of"] = pd.to_datetime(history["as_of"], utc=False).map(_normalize_date)
            history["close"] = pd.to_numeric(history["close"], errors="coerce")
            history["volume"] = pd.to_numeric(history["volume"], errors="coerce")
            history = history.dropna(subset=["symbol", "as_of", "close"])
            history = history.loc[history["as_of"] <= as_of].sort_values("as_of").reset_index(drop=True)
            if history.empty:
                continue
            price_groups[str(raw_symbol).strip().upper()] = history
            if empty_history.empty:
                empty_history = history.iloc[0:0].copy()
        return price_groups, empty_history

    prices = pd.DataFrame(price_history).copy()
    if prices.empty:
        raise ValueError("price_history must contain at least one row")
    _require_columns(prices, PRICE_HISTORY_REQUIRED_COLUMNS, name="price_history")

    prices["symbol"] = _normalize_symbol_series(prices["symbol"])
    prices["as_of"] = pd.to_datetime(prices["as_of"], utc=False).map(_normalize_date)
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices["volume"] = pd.to_numeric(prices["volume"], errors="coerce")
    prices = prices.dropna(subset=["symbol", "as_of", "close"])
    prices = prices.loc[prices["as_of"] <= as_of].copy()
    price_groups = {
        symbol: group.sort_values("as_of").reset_index(drop=True)
        for symbol, group in prices.groupby("symbol", sort=False)
    }
    return price_groups, prices.iloc[0:0].copy()


def build_feature_snapshot(
    price_history,
    universe_snapshot,
    *,
    as_of_date=None,
    benchmark_symbol: str = "SPY",
    benchmark_sector: str = "benchmark",
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 252,
    momentum_skip_days: int = 21,
    momentum_6m_lookback_days: int = 126,
    momentum_12m_lookback_days: int = 252,
    sma_window: int = 200,
    vol_window: int = 63,
    drawdown_window: int = 126,
) -> pd.DataFrame:
    universe = pd.DataFrame(universe_snapshot).copy()
    if universe.empty:
        raise ValueError("universe_snapshot must contain at least one row")
    _require_columns(universe, UNIVERSE_REQUIRED_COLUMNS, name="universe_snapshot")

    universe["symbol"] = _normalize_symbol_series(universe["symbol"])
    universe["sector"] = universe["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    universe = universe.drop_duplicates(subset=["symbol"], keep="last")

    if as_of_date is None:
        if isinstance(price_history, Mapping):
            max_dates = []
            for raw_history in price_history.values():
                history = pd.DataFrame(raw_history)
                if "as_of" not in history.columns or history.empty:
                    continue
                max_dates.append(pd.to_datetime(history["as_of"], utc=False).map(_normalize_date).max())
            if not max_dates:
                raise ValueError("price_history must contain at least one usable row")
            as_of = max(max_dates)
        else:
            prices = pd.DataFrame(price_history)
            if prices.empty or "as_of" not in prices.columns:
                raise ValueError("price_history must contain at least one usable row")
            as_of = pd.to_datetime(prices["as_of"], utc=False).map(_normalize_date).max()
    else:
        as_of = _normalize_date(as_of_date)
    price_groups, empty_history = _normalize_price_groups(price_history, as_of=as_of)
    if not price_groups and empty_history.empty:
        raise ValueError("price_history must contain at least one usable row")

    benchmark_symbol = str(benchmark_symbol or "").strip().upper()
    symbols = universe["symbol"].tolist()
    if benchmark_symbol and benchmark_symbol not in symbols:
        symbols.append(benchmark_symbol)

    sector_map = dict(zip(universe["symbol"], universe["sector"]))
    rows = []
    for symbol in symbols:
        history = price_groups.get(symbol, empty_history)
        rows.append(
            _build_feature_row(
                history,
                symbol=symbol,
                sector=sector_map.get(symbol, benchmark_sector if symbol == benchmark_symbol else "unknown"),
                as_of=as_of,
                min_price_usd=min_price_usd,
                min_adv20_usd=min_adv20_usd,
                min_history_days=min_history_days,
                momentum_skip_days=momentum_skip_days,
                momentum_6m_lookback_days=momentum_6m_lookback_days,
                momentum_12m_lookback_days=momentum_12m_lookback_days,
                sma_window=sma_window,
                vol_window=vol_window,
                drawdown_window=drawdown_window,
                force_ineligible=symbol == benchmark_symbol,
            )
        )

    snapshot = pd.DataFrame(rows)
    return snapshot.loc[:, FEATURE_SNAPSHOT_COLUMNS].sort_values(by=["symbol"]).reset_index(drop=True)
