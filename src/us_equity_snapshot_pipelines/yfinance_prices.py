from __future__ import annotations

import time
from typing import Callable, Mapping, Sequence

import pandas as pd

DEFAULT_SYMBOL_ALIASES = {
    "BFA": "BF-A",
    "BFB": "BF-B",
    "BRKB": "BRK-B",
    "CWENA": "CWEN-A",
    "HEIA": "HEI-A",
    "LENB": "LEN-B",
}


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
                alias_candidates[0]
                if alias_candidates
                else DEFAULT_SYMBOL_ALIASES.get(original_text, original_text)
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


def normalize_yfinance_download(data, symbols) -> pd.DataFrame:
    symbol_pairs = _normalize_input_symbols(symbols)
    if data is None or len(symbol_pairs) == 0:
        return pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])

    if isinstance(data.columns, pd.MultiIndex):
        close_frame = data["Close"].copy()
        volume_frame = data["Volume"].copy()
    else:
        close_frame = data[["Close"]].copy() if "Close" in data.columns else pd.DataFrame(index=data.index)
        close_frame.columns = [symbol_pairs[0][1]] if len(close_frame.columns) == 1 else close_frame.columns
        volume_frame = data[["Volume"]].copy() if "Volume" in data.columns else pd.DataFrame(index=data.index)
        volume_frame.columns = [symbol_pairs[0][1]] if len(volume_frame.columns) == 1 else volume_frame.columns

    close_frame.index = pd.to_datetime(close_frame.index).tz_localize(None).normalize()
    volume_frame.index = pd.to_datetime(volume_frame.index).tz_localize(None).normalize()
    close_frame.columns = close_frame.columns.map(str).str.upper()
    volume_frame.columns = volume_frame.columns.map(str).str.upper()

    rows: list[dict[str, object]] = []
    for original_symbol, download_symbol in symbol_pairs:
        if download_symbol not in close_frame.columns:
            continue
        closes = pd.to_numeric(close_frame[download_symbol], errors="coerce")
        volumes = (
            pd.to_numeric(volume_frame[download_symbol], errors="coerce")
            if download_symbol in volume_frame.columns
            else pd.Series(index=closes.index, dtype=float)
        )
        for as_of, close in closes.dropna().items():
            volume = volumes.get(as_of)
            rows.append(
                {
                    "symbol": original_symbol,
                    "as_of": pd.Timestamp(as_of).normalize(),
                    "close": float(close),
                    "volume": float(volume) if pd.notna(volume) else float("nan"),
                }
            )
    return pd.DataFrame(rows, columns=["symbol", "as_of", "close", "volume"]).sort_values(["as_of", "symbol"]).reset_index(drop=True)


def download_price_history(
    symbols: list[str],
    *,
    start: str,
    end: str | None = None,
    chunk_size: int = 100,
    download_fn: Callable | None = None,
    symbol_aliases: Mapping[str, Sequence[str] | str] | None = None,
) -> pd.DataFrame:
    if not symbols:
        raise ValueError("symbols must not be empty")
    if download_fn is None:
        import yfinance as yf

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
            auto_adjust=True,
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
                    auto_adjust=True,
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

    frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
    if frame.empty:
        raise RuntimeError("No price history downloaded")
    return frame.drop_duplicates(subset=["symbol", "as_of"], keep="last").sort_values(["as_of", "symbol"]).reset_index(drop=True)
