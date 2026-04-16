from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from .crisis_regime_guard_research import CRISIS_COMPARISON_PERIODS
from .crisis_response_research import ROUTE_NO_ACTION, ROUTE_TACO, ROUTE_TRUE_CRISIS
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_research import (
    DEFAULT_EVENT_SET,
    EVENT_KIND_SHOCK,
    TRADE_WAR_EVENT_SETS,
    TRADE_WAR_EVENTS_2018_TO_PRESENT,
    TradeWarEvent,
    price_history_to_close_matrix,
    resolve_trade_war_event_set,
)
from .yfinance_prices import download_price_history

DEFAULT_START_DATE = "1999-03-10"
DEFAULT_PRICE_START_DATE = "1999-03-10"
DEFAULT_BENCHMARK_SYMBOL = "QQQ"
DEFAULT_MARKET_SYMBOL = "SPY"
DEFAULT_FINANCIAL_SYMBOLS = ("XLF", "KRE")
DEFAULT_CREDIT_PAIRS = (("HYG", "IEF"), ("LQD", "IEF"))
DEFAULT_RATE_SYMBOLS = ("IEF", "TLT")
DEFAULT_BUBBLE_LOOKBACK_DAYS = 252
DEFAULT_BUBBLE_RETURN_THRESHOLD = 0.75
DEFAULT_BUBBLE_RELATIVE_RETURN_THRESHOLD = 0.30
DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD = -0.25
DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS = 126
DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD = -0.10
DEFAULT_CREDIT_RELATIVE_LOOKBACK_DAYS = 63
DEFAULT_CREDIT_RELATIVE_RETURN_THRESHOLD = -0.08
DEFAULT_RATE_LOOKBACK_DAYS = 126
DEFAULT_RATE_RETURN_THRESHOLD = -0.08
DEFAULT_POLICY_EVENT_WINDOW_DAYS = 10
DEFAULT_EXOGENOUS_EVENT_WINDOW_DAYS = 21

CONTEXT_LABEL_VALUATION_BUBBLE = "valuation_bubble"
CONTEXT_LABEL_FINANCIAL_CRISIS = "financial_crisis"
CONTEXT_LABEL_POLICY_SHOCK = "policy_shock"
CONTEXT_LABEL_EXOGENOUS_SHOCK = "exogenous_shock"
CONTEXT_LABEL_RATE_BEAR = "rate_bear"
CONTEXT_LABEL_NORMAL = "normal"

CONTEXT_BOOL_COLUMNS = (
    "bubble_context",
    "financial_context",
    "credit_context",
    "financial_system_context",
    "rate_context",
    "policy_context",
    "exogenous_context",
)

POLICY_EVENT_KEYWORDS = (
    "tariff",
    "tariffs",
    "trade",
    "section 301",
    "sanction",
    "sanctions",
    "import",
    "export",
    "china",
    "mexico",
    "canada",
)

EXOGENOUS_EVENT_KEYWORDS = (
    "covid",
    "pandemic",
    "virus",
    "lockdown",
    "shutdown",
    "invasion",
    "war",
    "earthquake",
    "terror",
)


def _parse_str_tuple(raw: str | Sequence[str]) -> tuple[str, ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    output: list[str] = []
    for value in values:
        text = str(value).strip().upper()
        if text:
            output.append(text)
    return tuple(dict.fromkeys(output))


def _parse_credit_pairs(raw: str | Sequence[str | Sequence[str]]) -> tuple[tuple[str, str], ...]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    pairs: list[tuple[str, str]] = []
    for value in values:
        if isinstance(value, str):
            parts = value.replace("/", ":").split(":")
        else:
            parts = list(value)
        if len(parts) != 2:
            raise ValueError(f"Credit pair must use NUMERATOR:DENOMINATOR syntax: {value!r}")
        numerator = str(parts[0]).strip().upper()
        denominator = str(parts[1]).strip().upper()
        if numerator and denominator and (numerator, denominator) not in pairs:
            pairs.append((numerator, denominator))
    return tuple(pairs)


def _normalize_close(data) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        raise TypeError("price data must be a pandas DataFrame")
    if {"symbol", "as_of", "close"}.issubset(data.columns):
        frame = price_history_to_close_matrix(data)
    else:
        frame = data.copy()
    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame = frame.sort_index()
    frame.columns = frame.columns.astype(str).str.upper().str.strip()
    return frame


def _window_index(close: pd.DataFrame, *, start_date: str | None, end_date: str | None) -> pd.DatetimeIndex:
    index = close.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if end_date is not None:
        index = index[index <= pd.Timestamp(end_date).normalize()]
    if index.empty:
        raise RuntimeError("No trading days in requested context window")
    return pd.DatetimeIndex(index)


def _optional_close(close: pd.DataFrame, symbol: str, index: pd.DatetimeIndex) -> pd.Series:
    symbol = str(symbol).strip().upper()
    if symbol not in close.columns:
        return pd.Series(float("nan"), index=index, name=symbol)
    return pd.to_numeric(close[symbol], errors="coerce").reindex(index).rename(symbol)


def _trailing_return(series: pd.Series, lookback_days: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return (values / values.shift(int(lookback_days)) - 1.0).rename(series.name)


def _rolling_drawdown(series: pd.Series, lookback_days: int = 252, min_periods: int = 63) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    high = values.rolling(int(lookback_days), min_periods=int(min_periods)).max()
    return (values / high - 1.0).rename(series.name)


def _min_frame(series_by_name: dict[str, pd.Series], index: pd.DatetimeIndex, *, name: str) -> pd.Series:
    if not series_by_name:
        return pd.Series(float("nan"), index=index, name=name)
    return pd.concat(series_by_name, axis=1).min(axis=1, skipna=True).rename(name)


def _next_index_date(index: pd.DatetimeIndex, raw_date: str) -> pd.Timestamp | None:
    event_date = pd.Timestamp(raw_date).normalize()
    pos = index.searchsorted(event_date, side="left")
    if pos >= len(index):
        return None
    return pd.Timestamp(index[pos]).normalize()


def _event_text(event: TradeWarEvent) -> str:
    return " ".join(
        str(value or "").lower()
        for value in (
            event.event_id,
            event.kind,
            event.region,
            event.title,
            event.source,
        )
    )


def _matches_any(text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def build_event_context_flags(
    index: pd.DatetimeIndex,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    *,
    policy_event_window_days: int = DEFAULT_POLICY_EVENT_WINDOW_DAYS,
    exogenous_event_window_days: int = DEFAULT_EXOGENOUS_EVENT_WINDOW_DAYS,
) -> pd.DataFrame:
    rows = pd.DataFrame(
        {
            "policy_context": False,
            "exogenous_context": False,
            "policy_event_ids": "",
            "exogenous_event_ids": "",
        },
        index=index,
    )
    policy_ids: dict[pd.Timestamp, list[str]] = {pd.Timestamp(date): [] for date in index}
    exogenous_ids: dict[pd.Timestamp, list[str]] = {pd.Timestamp(date): [] for date in index}

    for event in events:
        signal_date = _next_index_date(index, event.event_date)
        if signal_date is None:
            continue
        text = _event_text(event)
        is_policy = event.kind == EVENT_KIND_SHOCK and _matches_any(text, POLICY_EVENT_KEYWORDS)
        is_exogenous = event.kind == EVENT_KIND_SHOCK and _matches_any(text, EXOGENOUS_EVENT_KEYWORDS)
        if not (is_policy or is_exogenous):
            continue

        start_pos = index.get_loc(signal_date)
        if is_policy:
            stop_pos = min(len(index), start_pos + max(1, int(policy_event_window_days)))
            for date in index[start_pos:stop_pos]:
                rows.at[date, "policy_context"] = True
                policy_ids[pd.Timestamp(date)].append(event.event_id)
        if is_exogenous:
            stop_pos = min(len(index), start_pos + max(1, int(exogenous_event_window_days)))
            for date in index[start_pos:stop_pos]:
                rows.at[date, "exogenous_context"] = True
                exogenous_ids[pd.Timestamp(date)].append(event.event_id)

    rows["policy_event_ids"] = [";".join(policy_ids[pd.Timestamp(date)]) for date in index]
    rows["exogenous_event_ids"] = [";".join(exogenous_ids[pd.Timestamp(date)]) for date in index]
    return rows


def _prepare_external_context(context: pd.DataFrame | None, index: pd.DatetimeIndex) -> pd.DataFrame:
    if context is None or context.empty:
        return pd.DataFrame(index=index)
    if "as_of" not in context.columns:
        raise ValueError("external context table must include an as_of column")
    frame = context.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"]).set_index("as_of").sort_index()
    frame = frame.loc[~frame.index.duplicated(keep="last")]
    frame.columns = [f"external_{str(column).strip()}" for column in frame.columns]
    full_index = pd.DatetimeIndex(index.union(frame.index)).sort_values()
    return frame.reindex(full_index).ffill().reindex(index)


def _suggest_label_and_route(row: pd.Series) -> tuple[str, str, str]:
    if bool(row["financial_system_context"]):
        return (
            CONTEXT_LABEL_FINANCIAL_CRISIS,
            ROUTE_TRUE_CRISIS,
            "financial-sector or credit-stress context is active",
        )
    if bool(row["bubble_context"]):
        return (
            CONTEXT_LABEL_VALUATION_BUBBLE,
            ROUTE_TRUE_CRISIS,
            "valuation-bubble proxy is active before or during a drawdown",
        )
    if bool(row["policy_context"]):
        return (
            CONTEXT_LABEL_POLICY_SHOCK,
            ROUTE_TACO,
            "policy or tariff shock context is active without systemic stress",
        )
    if bool(row["exogenous_context"]):
        return (
            CONTEXT_LABEL_EXOGENOUS_SHOCK,
            ROUTE_NO_ACTION,
            "exogenous-shock context is active and should not imply slow true-crisis defense by itself",
        )
    if bool(row["rate_context"]):
        return (
            CONTEXT_LABEL_RATE_BEAR,
            ROUTE_NO_ACTION,
            "duration or rate-stress proxy is active without financial-system stress",
        )
    return (CONTEXT_LABEL_NORMAL, ROUTE_NO_ACTION, "no active historical-crisis context")


def build_crisis_context_features(
    price_history,
    *,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    external_context: pd.DataFrame | None = None,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    financial_symbols: Sequence[str] = DEFAULT_FINANCIAL_SYMBOLS,
    credit_pairs: Sequence[tuple[str, str]] = DEFAULT_CREDIT_PAIRS,
    rate_symbols: Sequence[str] = DEFAULT_RATE_SYMBOLS,
    bubble_lookback_days: int = DEFAULT_BUBBLE_LOOKBACK_DAYS,
    bubble_return_threshold: float = DEFAULT_BUBBLE_RETURN_THRESHOLD,
    bubble_relative_return_threshold: float = DEFAULT_BUBBLE_RELATIVE_RETURN_THRESHOLD,
    financial_drawdown_threshold: float = DEFAULT_FINANCIAL_DRAWDOWN_THRESHOLD,
    financial_relative_lookback_days: int = DEFAULT_FINANCIAL_RELATIVE_LOOKBACK_DAYS,
    financial_relative_return_threshold: float = DEFAULT_FINANCIAL_RELATIVE_RETURN_THRESHOLD,
    credit_relative_lookback_days: int = DEFAULT_CREDIT_RELATIVE_LOOKBACK_DAYS,
    credit_relative_return_threshold: float = DEFAULT_CREDIT_RELATIVE_RETURN_THRESHOLD,
    rate_lookback_days: int = DEFAULT_RATE_LOOKBACK_DAYS,
    rate_return_threshold: float = DEFAULT_RATE_RETURN_THRESHOLD,
    policy_event_window_days: int = DEFAULT_POLICY_EVENT_WINDOW_DAYS,
    exogenous_event_window_days: int = DEFAULT_EXOGENOUS_EVENT_WINDOW_DAYS,
) -> pd.DataFrame:
    close = _normalize_close(price_history)
    index = _window_index(close, start_date=start_date, end_date=end_date)

    benchmark_symbol = str(benchmark_symbol).strip().upper()
    market_symbol = str(market_symbol).strip().upper()
    financial_symbols = _parse_str_tuple(financial_symbols)
    credit_pairs = _parse_credit_pairs(credit_pairs)
    rate_symbols = _parse_str_tuple(rate_symbols)

    benchmark = _optional_close(close, benchmark_symbol, index)
    market = _optional_close(close, market_symbol, index)
    benchmark_return = _trailing_return(benchmark, int(bubble_lookback_days)).reindex(index)
    market_return = _trailing_return(market, int(bubble_lookback_days)).reindex(index)
    benchmark_relative_return = (benchmark_return - market_return).rename(
        f"{benchmark_symbol}_relative_return_{int(bubble_lookback_days)}d"
    )
    benchmark_drawdown = _rolling_drawdown(benchmark).reindex(index).rename(f"{benchmark_symbol}_drawdown_252d")

    bubble_context = (
        benchmark_return.ge(float(bubble_return_threshold))
        | benchmark_relative_return.ge(float(bubble_relative_return_threshold))
    ).fillna(False)

    financial_drawdowns: dict[str, pd.Series] = {}
    financial_relative_returns: dict[str, pd.Series] = {}
    market_financial_return = _trailing_return(market, int(financial_relative_lookback_days)).reindex(index)
    for symbol in financial_symbols:
        financial = _optional_close(close, symbol, index)
        if financial.notna().any():
            financial_drawdowns[symbol] = _rolling_drawdown(financial).reindex(index)
            financial_relative_returns[symbol] = (
                _trailing_return(financial, int(financial_relative_lookback_days)).reindex(index)
                - market_financial_return
            )
    financial_drawdown_min = _min_frame(financial_drawdowns, index, name="financial_drawdown_min_252d")
    financial_relative_return_min = _min_frame(
        financial_relative_returns,
        index,
        name=f"financial_relative_return_min_{int(financial_relative_lookback_days)}d",
    )
    financial_context = (
        financial_drawdown_min.le(float(financial_drawdown_threshold))
        & financial_relative_return_min.le(float(financial_relative_return_threshold))
    ).fillna(False)

    credit_relative_returns: dict[str, pd.Series] = {}
    for numerator_symbol, denominator_symbol in credit_pairs:
        numerator = _optional_close(close, numerator_symbol, index)
        denominator = _optional_close(close, denominator_symbol, index)
        if numerator.notna().any() and denominator.notna().any():
            name = f"{numerator_symbol}_{denominator_symbol}"
            credit_relative_returns[name] = (
                _trailing_return(numerator, int(credit_relative_lookback_days)).reindex(index)
                - _trailing_return(denominator, int(credit_relative_lookback_days)).reindex(index)
            )
    credit_relative_return_min = _min_frame(
        credit_relative_returns,
        index,
        name=f"credit_relative_return_min_{int(credit_relative_lookback_days)}d",
    )
    credit_context = credit_relative_return_min.le(float(credit_relative_return_threshold)).fillna(False)
    financial_system_context = financial_context | credit_context

    rate_returns: dict[str, pd.Series] = {}
    for symbol in rate_symbols:
        rate_proxy = _optional_close(close, symbol, index)
        if rate_proxy.notna().any():
            rate_returns[symbol] = _trailing_return(rate_proxy, int(rate_lookback_days)).reindex(index)
    rate_proxy_return_min = _min_frame(rate_returns, index, name=f"rate_proxy_return_min_{int(rate_lookback_days)}d")
    rate_context = rate_proxy_return_min.le(float(rate_return_threshold)).fillna(False)

    event_flags = build_event_context_flags(
        index,
        events,
        policy_event_window_days=policy_event_window_days,
        exogenous_event_window_days=exogenous_event_window_days,
    )

    output = pd.DataFrame(
        {
            "as_of": [pd.Timestamp(date).date().isoformat() for date in index],
            f"{benchmark_symbol}_drawdown_252d": benchmark_drawdown,
            f"{benchmark_symbol}_return_{int(bubble_lookback_days)}d": benchmark_return,
            f"{benchmark_symbol}_relative_return_{int(bubble_lookback_days)}d": benchmark_relative_return,
            "financial_drawdown_min_252d": financial_drawdown_min,
            f"financial_relative_return_min_{int(financial_relative_lookback_days)}d": financial_relative_return_min,
            f"credit_relative_return_min_{int(credit_relative_lookback_days)}d": credit_relative_return_min,
            f"rate_proxy_return_min_{int(rate_lookback_days)}d": rate_proxy_return_min,
            "bubble_context": bubble_context,
            "financial_context": financial_context,
            "credit_context": credit_context,
            "financial_system_context": financial_system_context,
            "rate_context": rate_context,
            "policy_context": event_flags["policy_context"],
            "exogenous_context": event_flags["exogenous_context"],
            "policy_event_ids": event_flags["policy_event_ids"],
            "exogenous_event_ids": event_flags["exogenous_event_ids"],
        },
        index=index,
    )
    external = _prepare_external_context(external_context, index)
    if not external.empty:
        output = output.join(external)

    suggestions = output.apply(_suggest_label_and_route, axis=1, result_type="expand")
    suggestions.columns = ["suggested_context_label", "suggested_route", "suggested_reason"]
    output = output.join(suggestions)
    return output.reset_index(drop=True)


def build_context_diagnostics(
    context_features: pd.DataFrame,
    *,
    periods: Sequence[tuple[str, str, str | None]] = CRISIS_COMPARISON_PERIODS,
) -> pd.DataFrame:
    if context_features.empty:
        return pd.DataFrame(columns=["Period", "Metric", "Value", "Trading Days", "Active Ratio"])
    frame = context_features.copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["as_of"]).sort_values("as_of")
    global_end = pd.Timestamp(frame["as_of"].max()).normalize()

    rows: list[dict[str, object]] = []
    for period_name, raw_start, raw_end in periods:
        start = pd.Timestamp(raw_start).normalize()
        end = min(pd.Timestamp(raw_end).normalize(), global_end) if raw_end is not None else global_end
        if end < start:
            continue
        window = frame.loc[frame["as_of"].between(start, end)].copy()
        if window.empty:
            continue
        trading_days = int(len(window))
        for column in CONTEXT_BOOL_COLUMNS:
            if column not in window:
                continue
            active_days = int(window[column].fillna(False).astype(bool).sum())
            rows.append(
                {
                    "Period": period_name,
                    "Metric": column,
                    "Value": active_days,
                    "Trading Days": trading_days,
                    "Active Ratio": active_days / trading_days,
                }
            )
        for column in ("suggested_context_label", "suggested_route"):
            if column not in window:
                continue
            counts = window[column].fillna("").astype(str).value_counts()
            for value, count in counts.items():
                rows.append(
                    {
                        "Period": period_name,
                        "Metric": f"{column}:{value}",
                        "Value": int(count),
                        "Trading Days": trading_days,
                        "Active Ratio": int(count) / trading_days,
                    }
                )
    return pd.DataFrame(rows)


def _format_diagnostics(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "Active Ratio" in output:
        output["Active Ratio"] = output["Active Ratio"].map(
            lambda value: f"{float(value):.2%}" if pd.notna(value) else ""
        )
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build research-only historical crisis context features for AI review."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument(
        "--external-context",
        default=None,
        help="Optional point-in-time context CSV with an as_of column",
    )
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--download-proxy", default=None, help="Optional yfinance proxy URL; YFINANCE_PROXY also works")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=None)
    parser.add_argument("--benchmark-symbol", default=DEFAULT_BENCHMARK_SYMBOL)
    parser.add_argument("--market-symbol", default=DEFAULT_MARKET_SYMBOL)
    parser.add_argument("--financial-symbols", default=",".join(DEFAULT_FINANCIAL_SYMBOLS))
    parser.add_argument(
        "--credit-pairs",
        default=",".join(f"{numerator}:{denominator}" for numerator, denominator in DEFAULT_CREDIT_PAIRS),
    )
    parser.add_argument("--rate-symbols", default=",".join(DEFAULT_RATE_SYMBOLS))
    parser.add_argument("--policy-event-window-days", type=int, default=DEFAULT_POLICY_EVENT_WINDOW_DAYS)
    parser.add_argument("--exogenous-event-window-days", type=int, default=DEFAULT_EXOGENOUS_EVENT_WINDOW_DAYS)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    financial_symbols = _parse_str_tuple(args.financial_symbols)
    credit_pairs = _parse_credit_pairs(args.credit_pairs)
    rate_symbols = _parse_str_tuple(args.rate_symbols)

    if args.download:
        symbols = [
            args.benchmark_symbol,
            args.market_symbol,
            *financial_symbols,
            *rate_symbols,
        ]
        for numerator, denominator in credit_pairs:
            symbols.extend([numerator, denominator])
        symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        price_history = download_price_history(
            symbols,
            start=args.price_start,
            end=args.price_end,
            proxy=args.download_proxy,
        )
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "crisis_context_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    external_context = read_table(args.external_context) if args.external_context else None
    features = build_crisis_context_features(
        price_history,
        events=resolve_trade_war_event_set(args.event_set),
        external_context=external_context,
        start_date=args.start_date,
        end_date=args.end_date,
        benchmark_symbol=args.benchmark_symbol,
        market_symbol=args.market_symbol,
        financial_symbols=financial_symbols,
        credit_pairs=credit_pairs,
        rate_symbols=rate_symbols,
        policy_event_window_days=int(args.policy_event_window_days),
        exogenous_event_window_days=int(args.exogenous_event_window_days),
    )
    diagnostics = build_context_diagnostics(features)

    print("\nContext diagnostics:")
    print(_format_diagnostics(diagnostics).to_string(index=False))
    features.to_csv(output_dir / "crisis_context_features.csv", index=False)
    diagnostics.to_csv(output_dir / "context_diagnostics.csv", index=False)
    print(f"wrote crisis context research outputs -> {output_dir}")
    return 0


__all__ = [
    "CONTEXT_BOOL_COLUMNS",
    "CONTEXT_LABEL_EXOGENOUS_SHOCK",
    "CONTEXT_LABEL_FINANCIAL_CRISIS",
    "CONTEXT_LABEL_NORMAL",
    "CONTEXT_LABEL_POLICY_SHOCK",
    "CONTEXT_LABEL_RATE_BEAR",
    "CONTEXT_LABEL_VALUATION_BUBBLE",
    "build_context_diagnostics",
    "build_crisis_context_features",
    "build_event_context_flags",
    "main",
]
