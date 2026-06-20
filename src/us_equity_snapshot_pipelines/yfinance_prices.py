from __future__ import annotations

import json
import os
import time
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence
from urllib.parse import quote
from urllib.request import Request, build_opener, urlopen, ProxyHandler

import pandas as pd

DEFAULT_SYMBOL_ALIASES = {
    "BFA": "BF-A",
    "BFB": "BF-B",
    "BRKB": "BRK-B",
    "CWENA": "CWEN-A",
    "HEIA": "HEI-A",
    "LENB": "LEN-B",
}
PRICE_HISTORY_COLUMNS = ("symbol", "as_of", "open", "high", "low", "close", "volume")
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_USER_AGENT = "Mozilla/5.0"
HTTP_PROXY_SCHEMES = {"http", "https"}
SOCKS_PROXY_SCHEMES = {"socks4", "socks4a", "socks5", "socks5h"}
SUPPORTED_PROXY_SCHEMES = HTTP_PROXY_SCHEMES | SOCKS_PROXY_SCHEMES
PRICE_FIELD_ADJUSTED_CLOSE = "adjusted_close"
PRICE_FIELD_CLOSE = "close"
SUPPORTED_PRICE_FIELDS = {PRICE_FIELD_ADJUSTED_CLOSE, PRICE_FIELD_CLOSE}


def normalize_price_field(value: object) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"", "adjusted", "adj_close", "adjclose"}:
        return PRICE_FIELD_ADJUSTED_CLOSE
    if text in SUPPORTED_PRICE_FIELDS:
        return text
    raise ValueError(f"unsupported price_field: {value!r}")


def _normalize_symbol_alias_candidates(candidates) -> list[str]:
    if isinstance(candidates, str):
        raw_candidates = [candidates]
    elif isinstance(candidates, Sequence):
        raw_candidates = list(candidates)
    else:
        raw_candidates = []

    normalized: list[str] = []
    for candidate in raw_candidates:
        candidate_text = str(candidate or "").strip().upper().replace(".", "-")
        if candidate_text and candidate_text not in normalized:
            normalized.append(candidate_text)
    return normalized


def _normalize_input_symbols(
    symbols,
    *,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
) -> list[tuple[str, str]]:
    normalized_pairs: list[tuple[str, str]] = []
    seen_originals: set[str] = set()
    alias_map = {
        str(symbol).strip().upper(): _normalize_symbol_alias_candidates(candidates)
        for symbol, candidates in dict(symbol_aliases or {}).items()
    }

    for item in symbols:
        if isinstance(item, tuple) and len(item) == 2:
            original, download_symbol = item
        else:
            original = item
            original_text = str(item or "").strip().upper()
            alias_candidates = alias_map.get(original_text, [])
            download_symbol = (
                alias_candidates[0] if alias_candidates else DEFAULT_SYMBOL_ALIASES.get(original_text, original_text)
            )

        original_text = str(original or "").strip().upper()
        download_text = str(download_symbol or "").strip().upper().replace(".", "-")
        if not original_text or original_text in seen_originals:
            continue
        seen_originals.add(original_text)
        normalized_pairs.append((original_text, download_text or original_text))

    return normalized_pairs


def _build_download_candidates(
    symbol: str,
    *,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
) -> list[str]:
    symbol_text = str(symbol or "").strip().upper()
    candidates: list[str] = []
    for candidate in _normalize_symbol_alias_candidates(dict(symbol_aliases or {}).get(symbol_text, [])):
        if candidate not in candidates:
            candidates.append(candidate)
    for candidate in (
        DEFAULT_SYMBOL_ALIASES.get(symbol_text),
        symbol_text.replace(".", "-"),
        symbol_text,
    ):
        candidate_text = str(candidate or "").strip().upper()
        if candidate_text and candidate_text not in candidates:
            candidates.append(candidate_text)
    return candidates


def _resolve_yfinance_proxy(proxy: str | None = None) -> str | None:
    for value in (
        proxy,
        os.environ.get("YFINANCE_PROXY"),
        os.environ.get("HTTPS_PROXY"),
        os.environ.get("https_proxy"),
        os.environ.get("HTTP_PROXY"),
        os.environ.get("http_proxy"),
        os.environ.get("ALL_PROXY"),
        os.environ.get("all_proxy"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _build_yfinance_proxy_config(proxy: str | None = None) -> dict[str, str] | None:
    resolved_proxy = _resolve_yfinance_proxy(proxy)
    if resolved_proxy is None:
        return None
    return {
        "http": resolved_proxy,
        "https": resolved_proxy,
    }


def normalize_proxy_candidate(raw_value: object) -> str | None:
    text = str(raw_value or "").strip()
    if not text or text.startswith("#"):
        return None
    if "://" in text:
        scheme = text.split("://", 1)[0].lower()
        return text if scheme in SUPPORTED_PROXY_SCHEMES else None
    if ":" not in text or any(char.isspace() for char in text):
        return None
    return f"http://{text}"


def _proxy_scheme(proxy: str | None) -> str | None:
    if not proxy or "://" not in str(proxy):
        return None
    return str(proxy).split("://", 1)[0].lower()


def _is_socks_proxy(proxy: str | None) -> bool:
    return _proxy_scheme(proxy) in SOCKS_PROXY_SCHEMES


def load_proxy_candidates(source: str | Path, *, max_candidates: int | None = None) -> list[str]:
    source_text = str(source or "").strip()
    if not source_text:
        return []
    if source_text.startswith(("http://", "https://")):
        with urlopen(source_text, timeout=10) as response:  # noqa: S310 - user-supplied proxy list URL.
            payload = response.read(1_000_000).decode("utf-8", errors="replace")
    else:
        payload = Path(source_text).read_text(encoding="utf-8")

    candidates: list[str] = []
    seen: set[str] = set()
    for raw_line in payload.splitlines():
        candidate = normalize_proxy_candidate(raw_line)
        if candidate is None or candidate in seen:
            continue
        candidates.append(candidate)
        seen.add(candidate)
        if max_candidates is not None and len(candidates) >= int(max_candidates):
            break
    return candidates


def normalize_yfinance_download(data, symbols) -> pd.DataFrame:
    symbol_pairs = _normalize_input_symbols(symbols)
    if data is None or len(symbol_pairs) == 0:
        return pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)

    field_frames = {
        field: _extract_yfinance_field_frame(data, field, symbol_pairs)
        for field in ("Open", "High", "Low", "Close", "Volume")
    }
    close_frame = field_frames["Close"]

    rows: list[dict[str, object]] = []
    for original_symbol, download_symbol in symbol_pairs:
        if download_symbol not in close_frame.columns:
            continue
        closes = pd.to_numeric(close_frame[download_symbol], errors="coerce")
        values_by_field = {
            field.lower(): (
                pd.to_numeric(frame[download_symbol], errors="coerce")
                if download_symbol in frame.columns
                else pd.Series(index=closes.index, dtype=float)
            )
            for field, frame in field_frames.items()
        }
        for as_of, close in closes.dropna().items():
            rows.append(
                {
                    "symbol": original_symbol,
                    "as_of": pd.Timestamp(as_of).normalize(),
                    "open": _optional_float(values_by_field["open"].get(as_of)),
                    "high": _optional_float(values_by_field["high"].get(as_of)),
                    "low": _optional_float(values_by_field["low"].get(as_of)),
                    "close": float(close),
                    "volume": _optional_float(values_by_field["volume"].get(as_of)),
                }
            )
    return pd.DataFrame(rows, columns=PRICE_HISTORY_COLUMNS).sort_values(["as_of", "symbol"]).reset_index(drop=True)


def _optional_float(value) -> float:
    return float(value) if pd.notna(value) else float("nan")


def _extract_yfinance_field_frame(data: pd.DataFrame, field: str, symbol_pairs) -> pd.DataFrame:
    if isinstance(data.columns, pd.MultiIndex):
        if field in data.columns.get_level_values(0):
            frame = data[field].copy()
        else:
            frame = pd.DataFrame(index=data.index)
    else:
        frame = data[[field]].copy() if field in data.columns else pd.DataFrame(index=data.index)
        if len(frame.columns) == 1:
            frame.columns = [symbol_pairs[0][1]]

    frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    frame.columns = frame.columns.map(str).str.upper()
    return frame


def _period_timestamp(value: str | None, *, default_end: bool = False) -> int:
    if value is None:
        dt = datetime.now(timezone.utc)
    else:
        date_value = pd.Timestamp(value).date()
        dt = datetime.combine(date_value, dt_time.min, tzinfo=timezone.utc)
    if default_end and value is None:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())


def _fetch_yahoo_chart_payload(
    symbol: str,
    *,
    start: str,
    end: str | None,
    proxy: str | None = None,
) -> dict:
    period1 = _period_timestamp(start)
    period2 = _period_timestamp(end, default_end=True)
    if period2 <= period1:
        period2 = period1 + 86400
    url = (
        f"{YAHOO_CHART_URL.format(symbol=quote(symbol, safe=''))}"
        f"?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
    )
    request = Request(url, headers={"User-Agent": YAHOO_USER_AGENT})
    resolved_proxy = _resolve_yfinance_proxy(proxy)
    if _is_socks_proxy(resolved_proxy):
        import requests

        try:
            response = requests.get(
                url,
                headers={"User-Agent": YAHOO_USER_AGENT},
                proxies={"http": resolved_proxy, "https": resolved_proxy},
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if "SOCKS" in str(exc) or "socks" in str(exc):
                raise RuntimeError("SOCKS proxy support requires PySocks; install requests[socks] or PySocks") from exc
            raise
    if resolved_proxy:
        opener = build_opener(ProxyHandler({"http": resolved_proxy, "https": resolved_proxy}))
        with opener.open(request, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    with urlopen(request, timeout=10) as response:  # noqa: S310 - fixed Yahoo Finance chart endpoint.
        return json.loads(response.read().decode("utf-8"))


def _normalize_yahoo_chart_payload(
    payload: Mapping[str, object],
    *,
    original_symbol: str,
    price_field: str = PRICE_FIELD_ADJUSTED_CLOSE,
) -> pd.DataFrame:
    resolved_price_field = normalize_price_field(price_field)
    chart = dict(payload.get("chart") or {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo chart error for {original_symbol}: {error}")
    result = list(chart.get("result") or [])
    if not result:
        return pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)
    node = dict(result[0] or {})
    timestamps = list(node.get("timestamp") or [])
    indicators = dict(node.get("indicators") or {})
    quotes = list(indicators.get("quote") or [])
    if not timestamps or not quotes:
        return pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)
    quote_data = dict(quotes[0] or {})
    adjclose_nodes = list(indicators.get("adjclose") or [])
    adjclose_values = list(dict(adjclose_nodes[0] or {}).get("adjclose") or []) if adjclose_nodes else []

    rows: list[dict[str, object]] = []
    for idx, raw_ts in enumerate(timestamps):
        close = _optional_index_float(quote_data.get("close"), idx)
        if pd.isna(close):
            continue
        adjusted_close = _optional_index_float(adjclose_values, idx)
        adjustment_ratio = 1.0
        if resolved_price_field == PRICE_FIELD_ADJUSTED_CLOSE and pd.notna(adjusted_close) and close:
            adjustment_ratio = float(adjusted_close) / float(close)
        close_value = (
            float(adjusted_close)
            if resolved_price_field == PRICE_FIELD_ADJUSTED_CLOSE and pd.notna(adjusted_close)
            else float(close)
        )
        rows.append(
            {
                "symbol": original_symbol,
                "as_of": pd.Timestamp.fromtimestamp(int(raw_ts), tz=timezone.utc).tz_localize(None).normalize(),
                "open": _optional_adjusted_float(quote_data.get("open"), idx, adjustment_ratio),
                "high": _optional_adjusted_float(quote_data.get("high"), idx, adjustment_ratio),
                "low": _optional_adjusted_float(quote_data.get("low"), idx, adjustment_ratio),
                "close": close_value,
                "volume": _optional_index_float(quote_data.get("volume"), idx),
            }
        )
    return pd.DataFrame(rows, columns=PRICE_HISTORY_COLUMNS)


def _optional_index_float(values, idx: int) -> float:
    if values is None or idx >= len(values):
        return float("nan")
    value = values[idx]
    return float(value) if pd.notna(value) else float("nan")


def _optional_adjusted_float(values, idx: int, adjustment_ratio: float) -> float:
    value = _optional_index_float(values, idx)
    return float(value) * float(adjustment_ratio) if pd.notna(value) else float("nan")


def download_yahoo_chart_price_history(
    symbols: list[str],
    *,
    start: str,
    end: str | None = None,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
    proxy: str | None = None,
    price_field: str = PRICE_FIELD_ADJUSTED_CLOSE,
) -> pd.DataFrame:
    resolved_price_field = normalize_price_field(price_field)
    symbol_pairs = _normalize_input_symbols(symbols, symbol_aliases=symbol_aliases)
    frames: list[pd.DataFrame] = []
    for original_symbol, download_symbol in symbol_pairs:
        symbol_frames: list[pd.DataFrame] = []
        for candidate in _build_download_candidates(download_symbol, symbol_aliases=symbol_aliases):
            payload = _fetch_yahoo_chart_payload(candidate, start=start, end=end, proxy=proxy)
            normalized = _normalize_yahoo_chart_payload(
                payload,
                original_symbol=original_symbol,
                price_field=resolved_price_field,
            )
            if not normalized.empty:
                symbol_frames.append(normalized)
                break
            time.sleep(0.05)
        if symbol_frames:
            frames.extend(symbol_frames)
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)
    if frame.empty:
        raise RuntimeError("No Yahoo chart price history downloaded")
    requested_symbols = {original_symbol for original_symbol, _download_symbol in symbol_pairs}
    downloaded_symbols = set(frame["symbol"].unique())
    missing_symbols = sorted(requested_symbols - downloaded_symbols)
    if missing_symbols:
        raise RuntimeError(f"Yahoo chart price history missing symbols: {', '.join(missing_symbols)}")
    return (
        frame.drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def download_price_history(
    symbols: list[str],
    *,
    start: str,
    end: str | None = None,
    chunk_size: int = 100,
    download_fn: Callable | None = None,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
    proxy: str | None = None,
    price_field: str = PRICE_FIELD_ADJUSTED_CLOSE,
) -> pd.DataFrame:
    if not symbols:
        raise ValueError("symbols must not be empty")
    resolved_price_field = normalize_price_field(price_field)
    if download_fn is None:
        import yfinance as yf

        proxy_config = _build_yfinance_proxy_config(proxy)
        if proxy_config and hasattr(yf, "config") and hasattr(yf.config, "network"):
            yf.config.network.proxy = proxy_config
        elif proxy_config and hasattr(yf, "set_config"):
            yf.set_config(proxy=proxy_config)
        download_fn = yf.download

    symbol_pairs = _normalize_input_symbols(symbols, symbol_aliases=symbol_aliases)
    chunks = []
    for offset in range(0, len(symbol_pairs), chunk_size):
        batch_pairs = symbol_pairs[offset : offset + chunk_size]
        batch_download_symbols = [download_symbol for _original_symbol, download_symbol in batch_pairs]
        raw = download_fn(
            batch_download_symbols,
            start=start,
            end=end,
            auto_adjust=resolved_price_field == PRICE_FIELD_ADJUSTED_CLOSE,
            progress=False,
            threads=False,
        )
        normalized = normalize_yfinance_download(raw, batch_pairs)
        requested_symbols = {original_symbol for original_symbol, _download_symbol in batch_pairs}
        downloaded_symbols = set(normalized["symbol"].unique()) if not normalized.empty else set()

        retry_frames = []
        for missing_symbol in sorted(requested_symbols - downloaded_symbols):
            for candidate in _build_download_candidates(missing_symbol, symbol_aliases=symbol_aliases):
                retry_raw = download_fn(
                    [candidate],
                    start=start,
                    end=end,
                    auto_adjust=resolved_price_field == PRICE_FIELD_ADJUSTED_CLOSE,
                    progress=False,
                    threads=False,
                )
                retry_normalized = normalize_yfinance_download(retry_raw, [(missing_symbol, candidate)])
                if not retry_normalized.empty:
                    retry_frames.append(retry_normalized)
                    break
                time.sleep(0.05)

        if retry_frames:
            normalized = pd.concat([normalized, *retry_frames], ignore_index=True)
        chunks.append(normalized)

    frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)
    if frame.empty:
        raise RuntimeError("No price history downloaded")
    return (
        frame.drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def _redact_proxy(proxy: str | None) -> str:
    if not proxy:
        return "<direct>"
    scheme, separator, rest = str(proxy).partition("://")
    if not separator:
        return "<proxy>"
    host_port = rest.rsplit("@", 1)[-1]
    return f"{scheme}://{host_port}"


def _require_downloaded_symbols(
    frame: pd.DataFrame,
    symbols: list[str],
    *,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
) -> pd.DataFrame:
    symbol_pairs = _normalize_input_symbols(symbols, symbol_aliases=symbol_aliases)
    requested_symbols = {original_symbol for original_symbol, _download_symbol in symbol_pairs}
    downloaded_symbols = set(frame["symbol"].unique()) if frame is not None and not frame.empty else set()
    missing_symbols = sorted(requested_symbols - downloaded_symbols)
    if missing_symbols:
        raise RuntimeError(f"price history missing symbols: {', '.join(missing_symbols)}")
    return frame


def download_price_history_with_proxy_candidates(
    symbols: list[str],
    *,
    start: str,
    end: str | None = None,
    chunk_size: int = 100,
    download_fn: Callable | None = None,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
    proxy: str | None = None,
    proxy_candidates: Sequence[str] | None = None,
    price_field: str = PRICE_FIELD_ADJUSTED_CLOSE,
) -> pd.DataFrame:
    resolved_price_field = normalize_price_field(price_field)
    candidates: list[str | None] = []
    if proxy:
        candidates.append(proxy)
    for candidate in proxy_candidates or ():
        normalized = normalize_proxy_candidate(candidate)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    if not candidates:
        candidates.append(None)

    errors: list[str] = []
    last_error: Exception | None = None
    for candidate in candidates:
        if download_fn is None and candidate is not None:
            try:
                return _require_downloaded_symbols(
                    download_yahoo_chart_price_history(
                        symbols,
                        start=start,
                        end=end,
                        symbol_aliases=symbol_aliases,
                        proxy=candidate,
                        price_field=resolved_price_field,
                    ),
                    symbols,
                    symbol_aliases=symbol_aliases,
                )
            except Exception as chart_first_exc:
                errors.append(f"{_redact_proxy(candidate)} chart: {type(chart_first_exc).__name__}: {chart_first_exc}")
        try:
            return _require_downloaded_symbols(
                download_price_history(
                    symbols,
                    start=start,
                    end=end,
                    chunk_size=chunk_size,
                    download_fn=download_fn,
                    symbol_aliases=symbol_aliases,
                    proxy=candidate,
                    price_field=resolved_price_field,
                ),
                symbols,
                symbol_aliases=symbol_aliases,
            )
        except Exception as exc:  # pragma: no cover - exercised by integration downloads.
            if download_fn is None:
                try:
                    return _require_downloaded_symbols(
                        download_yahoo_chart_price_history(
                            symbols,
                            start=start,
                            end=end,
                            symbol_aliases=symbol_aliases,
                            proxy=candidate,
                            price_field=resolved_price_field,
                        ),
                        symbols,
                        symbol_aliases=symbol_aliases,
                    )
                except Exception as fallback_exc:
                    errors.append(
                        f"{_redact_proxy(candidate)} chart fallback: {type(fallback_exc).__name__}: {fallback_exc}"
                    )
            last_error = exc
            errors.append(f"{_redact_proxy(candidate)}: {type(exc).__name__}: {exc}")
            time.sleep(0.25)
    message = "No price history downloaded with configured yfinance proxy candidates"
    if errors:
        message = f"{message}; attempts: {' | '.join(errors[-5:])}"
    raise RuntimeError(message) from last_error
