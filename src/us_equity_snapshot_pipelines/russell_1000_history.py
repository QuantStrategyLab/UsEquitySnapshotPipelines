from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from types import MappingProxyType
from zoneinfo import ZoneInfo
import csv
import hashlib
import io
import json
import re
import ssl
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import Request, urlopen

import pandas as pd

from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table, write_table

SNAPSHOT_FILENAME_DATE_RE = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})")
UNIVERSE_HISTORY_COLUMNS = ("symbol", "sector", "start_date", "end_date")
ISHARES_IWB_PRODUCT_URL = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf"
ISHARES_IWB_HOLDINGS_CSV_URL = (
    f"{ISHARES_IWB_PRODUCT_URL}/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
)
ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE = (
    f"{ISHARES_IWB_PRODUCT_URL}/1467271812596.ajax?fileType=json&tab=all&asOfDate={{as_of_date}}"
)
ISHARES_PRODUCT_DATA_API_URL = (
    "https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"
)
BLACKROCK_PRODUCT_DATA_API_URL = (
    "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"
)
BLACKROCK_PRODUCT_DATA_API_URLS = (ISHARES_PRODUCT_DATA_API_URL, BLACKROCK_PRODUCT_DATA_API_URL)
BLACKROCK_IWB_PRODUCT_ID = "239707"
BLACKROCK_PRODUCT_DATA_HOLDINGS_SOURCE_KIND = "blackrock_product_data_v2"
ISHARES_OFFICIAL_JSON_SOURCE_KIND = "official_json"
DEFAULT_IWB_HOLDINGS_SOURCE_ORDER = (BLACKROCK_PRODUCT_DATA_HOLDINGS_SOURCE_KIND, ISHARES_OFFICIAL_JSON_SOURCE_KIND)
COMPANIES_MARKETCAP_IWB_HOLDINGS_URL = "https://companiesmarketcap.com/ishares-russell-1000-etf/holdings/"
COMPANIES_MARKETCAP_TICKER_ALIASES = {
    "BRKA": "BRK.A",
    "BRKB": "BRK.B",
    "BFA": "BF.A",
    "BFB": "BF.B",
    "HEIA": "HEI.A",
}
ISHARES_SNAPSHOT_IDENTIFIER_COLUMNS = ("isin", "cusip", "sedol")
ISHARES_SNAPSHOT_OPTIONAL_COLUMN_SOURCES = (
    ("ISIN", "isin"),
    ("CUSIP", "cusip"),
    ("SEDOL", "sedol"),
    ("Market Value", "market_value"),
    ("Weight (%)", "weight"),
    ("Weight", "weight"),
    ("Notional Value", "notional_value"),
    ("Shares", "shares"),
    ("Price", "price"),
    ("Exchange", "exchange"),
    ("Location", "country"),
    ("Country", "country"),
    ("Currency", "currency"),
    ("Market Currency", "market_currency"),
)
ISHARES_SNAPSHOT_NUMERIC_COLUMNS = ("market_value", "weight", "notional_value", "shares", "price")
WAYBACK_CDX_API_URL = "https://web.archive.org/cdx/search/cdx"
DEFAULT_HTTP_USER_AGENT = "Mozilla/5.0 (compatible; UsEquitySnapshotPipelines/0.1.0)"


def parse_snapshot_date_from_path(path: str | Path) -> pd.Timestamp:
    path_text = str(path)
    match = SNAPSHOT_FILENAME_DATE_RE.search(path_text)
    if match is None:
        raise ValueError(f"Could not infer snapshot date from filename: {path_text}")
    return pd.Timestamp(match.group("date")).normalize()


def _normalize_snapshot_frame(snapshot, *, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    frame = pd.DataFrame(snapshot).copy()
    required = {"symbol", "sector"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"snapshot missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    frame["snapshot_date"] = pd.Timestamp(snapshot_date).normalize()
    return frame.loc[:, ["symbol", "sector", "snapshot_date"]].drop_duplicates(subset=["symbol"], keep="last")


def build_interval_universe_history(snapshot_tables: list[tuple[pd.Timestamp, pd.DataFrame]]) -> pd.DataFrame:
    if not snapshot_tables:
        raise ValueError("snapshot_tables must not be empty")

    normalized = [
        (
            pd.Timestamp(snapshot_date).normalize(),
            _normalize_snapshot_frame(frame, snapshot_date=pd.Timestamp(snapshot_date).normalize()),
        )
        for snapshot_date, frame in snapshot_tables
    ]
    normalized.sort(key=lambda item: item[0])

    rows: list[dict[str, object]] = []
    for index, (snapshot_date, frame) in enumerate(normalized):
        next_snapshot_date = normalized[index + 1][0] if index + 1 < len(normalized) else None
        end_date = next_snapshot_date - pd.Timedelta(days=1) if next_snapshot_date is not None else pd.NaT
        for row in frame.itertuples(index=False):
            rows.append(
                {
                    "symbol": row.symbol,
                    "sector": row.sector,
                    "start_date": snapshot_date,
                    "end_date": end_date,
                }
            )

    history = pd.DataFrame(rows)
    return history.loc[:, UNIVERSE_HISTORY_COLUMNS].sort_values(["symbol", "start_date"]).reset_index(drop=True)


def backfill_universe_history_start(history, backfill_start_date) -> pd.DataFrame:
    frame = pd.DataFrame(history).copy()
    required = {"symbol", "sector", "start_date", "end_date"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"history missing required columns: {missing_text}")
    if frame.empty:
        raise ValueError("history must not be empty")

    frame["start_date"] = pd.to_datetime(frame["start_date"]).dt.tz_localize(None).dt.normalize()
    frame["end_date"] = pd.to_datetime(frame["end_date"]).dt.tz_localize(None).dt.normalize()

    earliest_start = frame["start_date"].min()
    if pd.isna(earliest_start):
        raise ValueError("history start_date must contain at least one non-null value")

    backfill_start = pd.Timestamp(backfill_start_date).tz_localize(None).normalize()
    if backfill_start > earliest_start:
        raise ValueError("backfill_start_date must be on or before the earliest start_date")

    frame.loc[frame["start_date"] == earliest_start, "start_date"] = backfill_start
    return frame.loc[:, UNIVERSE_HISTORY_COLUMNS].sort_values(["symbol", "start_date"]).reset_index(drop=True)


def load_snapshot_tables_from_directory(input_dir: str | Path) -> list[tuple[pd.Timestamp, pd.DataFrame]]:
    root = Path(str(input_dir or "").strip())
    if not str(root):
        raise EnvironmentError("input_dir is required")
    if not root.exists():
        raise FileNotFoundError(f"input_dir not found: {root}")

    snapshot_tables: list[tuple[pd.Timestamp, pd.DataFrame]] = []
    for path in sorted(root.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".csv", ".json", ".jsonl", ".parquet"}:
            continue
        snapshot_date = parse_snapshot_date_from_path(path)
        snapshot_tables.append((snapshot_date, read_table(path)))

    if not snapshot_tables:
        raise RuntimeError(f"No supported snapshot files found in {root}")
    return snapshot_tables


def build_interval_universe_history_from_directory(input_dir: str | Path) -> pd.DataFrame:
    return build_interval_universe_history(load_snapshot_tables_from_directory(input_dir))


def _build_ssl_context() -> ssl.SSLContext:
    try:
        import certifi
    except ImportError:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


def _fetch_text(
    url: str,
    *,
    timeout: int = 60,
    user_agent: str = DEFAULT_HTTP_USER_AGENT,
    attempts: int = 2,
    retry_sleep_seconds: float = 0.5,
) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "*/*",
        },
    )
    errors: list[str] = []
    for attempt in range(max(int(attempts), 1)):
        try:
            with urlopen(request, timeout=timeout, context=_build_ssl_context()) as response:
                encoding = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(encoding, errors="replace")
        except OSError as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            if attempt + 1 >= max(int(attempts), 1):
                raise
            time.sleep(max(float(retry_sleep_seconds), 0.0))
    raise RuntimeError(f"Could not fetch {url}: {'; '.join(errors)}")


def _fetch_first_available_text(
    urls: Iterable[str],
    *,
    timeout: int = 60,
    user_agent: str = DEFAULT_HTTP_USER_AGENT,
) -> str:
    errors: list[str] = []
    for url in urls:
        try:
            return _fetch_text(url, timeout=timeout, user_agent=user_agent)
        except OSError as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
    raise OSError("; ".join(errors))


def _normalize_ishares_numeric_value(value) -> float:
    raw_value = value.get("raw") if isinstance(value, dict) else value
    if pd.isna(raw_value):
        return float("nan")
    text = str(raw_value).strip().strip('"').replace("$", "").replace(",", "").replace("%", "")
    numeric = pd.to_numeric(text, errors="coerce")
    return float(numeric) if pd.notna(numeric) else float("nan")


def _finalize_ishares_holdings_snapshot_frame(frame) -> pd.DataFrame:
    normalized = pd.DataFrame(frame).copy()
    if "Ticker" not in normalized.columns or "Sector" not in normalized.columns:
        raise ValueError("holdings frame missing required columns: Ticker, Sector")

    normalized["Ticker"] = normalized["Ticker"].astype(str).str.strip().str.strip('"').str.upper()
    normalized["Sector"] = normalized["Sector"].astype(str).str.strip().str.strip('"')
    if "Asset Class" in normalized.columns:
        normalized["Asset Class"] = normalized["Asset Class"].astype(str).str.strip().str.strip('"')
    else:
        normalized["Asset Class"] = "Equity"

    if "Name" in normalized.columns:
        normalized["Name"] = normalized["Name"].astype(str).str.strip().str.strip('"')
    else:
        normalized["Name"] = ""

    normalized = normalized.loc[
        normalized["Ticker"].ne("")
        & normalized["Ticker"].ne("-")
        & normalized["Ticker"].str.fullmatch(r"[A-Z0-9.-]+", na=False)
        & normalized["Sector"].ne("")
        & normalized["Asset Class"].eq("Equity")
        & ~normalized["Ticker"].str.startswith("THE CONTENT CONTAINED HEREIN", na=False)
    ].copy()
    normalized = normalized.drop_duplicates(subset=["Ticker"], keep="first")
    rename_map = {"Ticker": "symbol", "Sector": "sector", "Name": "name"}
    selected_columns = ["symbol", "sector", "name"]
    for source_column, target_column in ISHARES_SNAPSHOT_OPTIONAL_COLUMN_SOURCES:
        if source_column in normalized.columns and target_column not in selected_columns:
            rename_map[source_column] = target_column
            selected_columns.append(target_column)

    snapshot = normalized.rename(columns=rename_map).loc[:, selected_columns].copy()
    for column in selected_columns:
        if column in ISHARES_SNAPSHOT_NUMERIC_COLUMNS:
            snapshot[column] = snapshot[column].map(_normalize_ishares_numeric_value)
        else:
            snapshot[column] = snapshot[column].astype(str).str.strip().replace({"": pd.NA, "-": pd.NA})
    snapshot["symbol"] = snapshot["symbol"].astype(str).str.upper()
    snapshot["sector"] = snapshot["sector"].fillna("unknown")
    snapshot["name"] = snapshot["name"].fillna("")
    return snapshot.sort_values("symbol").reset_index(drop=True)


def parse_ishares_holdings_snapshot(csv_text: str) -> tuple[pd.Timestamp, pd.DataFrame]:
    if not str(csv_text or "").strip():
        raise ValueError("csv_text must not be empty")

    rows = list(csv.reader(io.StringIO(str(csv_text).lstrip("\ufeff"))))
    as_of_date = None
    for row in rows[:25]:
        if len(row) >= 2 and row[0].strip() == "Fund Holdings as of":
            as_of_date = pd.Timestamp(row[1].strip().strip('"')).normalize()
            break
    if as_of_date is None:
        raise ValueError("Could not find 'Fund Holdings as of' row in holdings file")

    header_idx = None
    for index, row in enumerate(rows):
        normalized = [cell.strip() for cell in row]
        if normalized and normalized[0] == "Ticker" and "Sector" in normalized:
            header_idx = index
            header = normalized
            break
    if header_idx is None:
        raise ValueError("Could not find holdings table header")

    holdings_rows = rows[header_idx + 1 :]
    frame = pd.DataFrame(holdings_rows, columns=header)
    frame.columns = [str(column).strip() for column in frame.columns]
    return as_of_date, _finalize_ishares_holdings_snapshot_frame(frame)


def parse_ishares_holdings_json_snapshot(json_text: str, *, as_of_date) -> tuple[pd.Timestamp, pd.DataFrame]:
    if not str(json_text or "").strip():
        raise ValueError("json_text must not be empty")

    payload_text = str(json_text).lstrip("\ufeff").strip()
    if payload_text.startswith("<"):
        raise ValueError("iShares JSON endpoint returned HTML instead of JSON")

    payload = json.loads(payload_text)
    rows = payload.get("aaData")
    if not isinstance(rows, list):
        raise ValueError("JSON payload missing aaData list")

    frame = pd.DataFrame(
        [
            {
                "Ticker": row[0] if len(row) > 0 else "",
                "Name": row[1] if len(row) > 1 else "",
                "Sector": row[2] if len(row) > 2 else "",
                "Asset Class": row[3] if len(row) > 3 else "",
                "Market Value": row[4] if len(row) > 4 else "",
                "Weight (%)": row[5] if len(row) > 5 else "",
                "Notional Value": row[6] if len(row) > 6 else "",
                "Shares": row[7] if len(row) > 7 else "",
                "CUSIP": row[8] if len(row) > 8 else "",
                "ISIN": row[9] if len(row) > 9 else "",
                "SEDOL": row[10] if len(row) > 10 else "",
                "Price": row[11] if len(row) > 11 else "",
                "Location": row[12] if len(row) > 12 else "",
                "Exchange": row[13] if len(row) > 13 else "",
                "Currency": row[14] if len(row) > 14 else "",
                "Market Currency": row[16] if len(row) > 16 else "",
            }
            for row in rows
            if isinstance(row, list)
        ]
    )
    if frame.empty:
        frame = pd.DataFrame(
            columns=[
                "Ticker",
                "Name",
                "Sector",
                "Asset Class",
                "Market Value",
                "Weight (%)",
                "Notional Value",
                "Shares",
                "CUSIP",
                "ISIN",
                "SEDOL",
                "Price",
                "Location",
                "Exchange",
                "Currency",
                "Market Currency",
            ]
        )
    return pd.Timestamp(as_of_date).normalize(), _finalize_ishares_holdings_snapshot_frame(frame)


def _parse_blackrock_product_data_date(value) -> pd.Timestamp:
    if pd.isna(value):
        raise ValueError("BlackRock product data holdings payload missing as-of date")
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return pd.to_datetime(text, format="%Y%m%d").normalize()
    return pd.Timestamp(text).normalize()


def _blackrock_data_point_values(data_points: dict, name: str) -> list:
    data_point = data_points.get(name) if isinstance(data_points, dict) else None
    if not isinstance(data_point, dict):
        return []
    values = data_point.get("value")
    if isinstance(values, list):
        return values
    formatted_values = data_point.get("formattedValue")
    if isinstance(formatted_values, list):
        return formatted_values
    return []


def _value_at(values: list, index: int):
    return values[index] if index < len(values) else ""


def parse_blackrock_product_data_holdings_snapshot(
    json_text: str,
    *,
    requested_as_of_date=None,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    if not str(json_text or "").strip():
        raise ValueError("json_text must not be empty")

    payload_text = str(json_text).lstrip("\ufeff").strip()
    if payload_text.startswith("<"):
        raise ValueError("BlackRock product data endpoint returned HTML instead of JSON")

    payload = json.loads(payload_text)
    try:
        data_points = payload["componentsByNameMap"]["holdings"]["containersByNameMap"]["all"]["dataPointsByNameMap"]
    except (KeyError, TypeError) as exc:
        raise ValueError("BlackRock product data payload missing holdings data points") from exc

    as_of_value = (data_points.get("asOfDate") or {}).get("value") or (data_points.get("asOfDate") or {}).get(
        "formattedValue"
    )
    as_of_date = _parse_blackrock_product_data_date(as_of_value or requested_as_of_date)

    tickers = _blackrock_data_point_values(data_points, "ticker")
    if not tickers:
        raise ValueError("BlackRock product data holdings payload contained no tickers")

    issue_names = _blackrock_data_point_values(data_points, "issueName")
    sectors = _blackrock_data_point_values(data_points, "sectorName")
    asset_classes = _blackrock_data_point_values(data_points, "assetClass")
    market_values = _blackrock_data_point_values(data_points, "marketValue")
    weights = _blackrock_data_point_values(data_points, "holdingPercent")
    notional_values = _blackrock_data_point_values(data_points, "notionalValue")
    shares = _blackrock_data_point_values(data_points, "unitsHeld")
    cusips = _blackrock_data_point_values(data_points, "cusip")
    isins = _blackrock_data_point_values(data_points, "isin")
    sedols = _blackrock_data_point_values(data_points, "sedol")
    prices = _blackrock_data_point_values(data_points, "unitPrice")
    countries = _blackrock_data_point_values(data_points, "countryOfRisk")
    exchanges = _blackrock_data_point_values(data_points, "exchange")
    currencies = _blackrock_data_point_values(data_points, "currencyCode")

    frame = pd.DataFrame(
        [
            {
                "Ticker": _value_at(tickers, index),
                "Name": _value_at(issue_names, index),
                "Sector": _value_at(sectors, index),
                "Asset Class": _value_at(asset_classes, index) or "Equity",
                "Market Value": _value_at(market_values, index),
                "Weight (%)": _value_at(weights, index),
                "Notional Value": _value_at(notional_values, index),
                "Shares": _value_at(shares, index),
                "CUSIP": _value_at(cusips, index),
                "ISIN": _value_at(isins, index),
                "SEDOL": _value_at(sedols, index),
                "Price": _value_at(prices, index),
                "Location": _value_at(countries, index),
                "Exchange": _value_at(exchanges, index),
                "Currency": _value_at(currencies, index),
                "Market Currency": _value_at(currencies, index),
            }
            for index in range(len(tickers))
        ]
    )
    snapshot = _finalize_ishares_holdings_snapshot_frame(frame)
    if snapshot.empty:
        raise ValueError("BlackRock product data holdings snapshot was empty after normalization")
    return as_of_date, snapshot


def _strip_html_text(value: str) -> str:
    text = re.sub(r"<[^>]*>", " ", str(value or ""))
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _normalize_companies_marketcap_ticker(value: str) -> str:
    symbol = str(value or "").strip().upper()
    return COMPANIES_MARKETCAP_TICKER_ALIASES.get(symbol, symbol)


def parse_companies_marketcap_iwb_holdings_html(html_text: str) -> tuple[pd.Timestamp, pd.DataFrame]:
    payload = str(html_text or "")
    if not payload.strip():
        raise ValueError("html_text must not be empty")

    page_text = _strip_html_text(payload)
    date_match = re.search(r"Etf holdings as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text)
    if date_match is None:
        raise ValueError("Could not find CompaniesMarketCap IWB holdings as-of date")
    as_of_date = pd.Timestamp(date_match.group(1)).normalize()

    table_match = re.search(
        r"<h2[^>]*>\s*Full holdings list\s*</h2>\s*<table\b.*?<tbody>(?P<tbody>.*?)</tbody>",
        payload,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if table_match is None:
        raise ValueError("Could not find CompaniesMarketCap IWB full holdings table")

    rows: list[dict[str, object]] = []
    for row_html in re.findall(r"<tr\b[^>]*>(.*?)</tr>", table_match.group("tbody"), flags=re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 4:
            continue
        weight = pd.to_numeric(_strip_html_text(cells[0]).replace("%", "").replace(",", ""), errors="coerce")
        name = _strip_html_text(cells[1])
        symbol = _normalize_companies_marketcap_ticker(_strip_html_text(cells[2]))
        shares = pd.to_numeric(_strip_html_text(cells[3]).replace(",", ""), errors="coerce")
        if pd.isna(weight) or not symbol or not re.fullmatch(r"[A-Z0-9.-]+", symbol):
            continue
        rows.append(
            {
                "symbol": symbol,
                "sector": "unknown",
                "name": name,
                "weight": float(weight),
                "shares": float(shares) if pd.notna(shares) else float("nan"),
            }
        )

    if not rows:
        raise ValueError("CompaniesMarketCap IWB holdings table did not contain parseable rows")
    snapshot = pd.DataFrame(rows)
    return (
        as_of_date,
        snapshot.drop_duplicates(subset=["symbol"], keep="first")
        .sort_values(["weight", "symbol"], ascending=[False, True])
        .reset_index(drop=True),
    )


def download_companies_marketcap_iwb_holdings_snapshot(
    url: str = COMPANIES_MARKETCAP_IWB_HOLDINGS_URL,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    as_of_date, snapshot = parse_companies_marketcap_iwb_holdings_html(_fetch_text(url))
    if len(snapshot) < 500:
        raise RuntimeError(f"CompaniesMarketCap IWB holdings snapshot too small: row_count={len(snapshot)}")
    return as_of_date, snapshot


def build_ishares_holdings_json_url(
    as_of_date,
    *,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
) -> str:
    normalized = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    return str(holdings_url_template).format(as_of_date=f"{normalized:%Y%m%d}")


def build_blackrock_product_data_holdings_url(
    as_of_date=None,
    *,
    product_id: str = BLACKROCK_IWB_PRODUCT_ID,
    api_url: str = ISHARES_PRODUCT_DATA_API_URL,
) -> str:
    params = {
        "appType": "PRODUCT_PAGE",
        "appSubType": "ISHARES",
        "targetSite": "us-ishares",
        "locale": "en_US",
        "portfolioId": str(product_id),
        "userType": "individual",
        "component": "holdings",
    }
    if as_of_date is not None and not pd.isna(as_of_date):
        normalized = pd.Timestamp(as_of_date).tz_localize(None).normalize()
        params["asOfDate"] = f"{normalized:%Y%m%d}"
    return f"{api_url}?{urlencode(params)}"


def download_blackrock_product_data_holdings_snapshot_for_date(
    as_of_date,
    *,
    holdings_url_template: str | None = None,
    api_urls: Iterable[str] = BLACKROCK_PRODUCT_DATA_API_URLS,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    del holdings_url_template
    snapshot_date = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    source_urls = [
        build_blackrock_product_data_holdings_url(snapshot_date, api_url=str(api_url)) for api_url in api_urls
    ]
    return parse_blackrock_product_data_holdings_snapshot(
        _fetch_first_available_text(source_urls),
        requested_as_of_date=snapshot_date,
    )


def download_ishares_holdings_snapshot_for_date(
    as_of_date,
    *,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    snapshot_date = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    source_url = build_ishares_holdings_json_url(snapshot_date, holdings_url_template=holdings_url_template)
    return parse_ishares_holdings_json_snapshot(_fetch_text(source_url), as_of_date=snapshot_date)


def _build_snapshot_source_url(source_url_fn, as_of_date, holdings_url_template: str | None) -> str:
    try:
        return source_url_fn(as_of_date, holdings_url_template=holdings_url_template)
    except TypeError:
        return source_url_fn(as_of_date)


def build_monthly_snapshot_request_dates(start_date, end_date=None) -> list[pd.Timestamp]:
    start = pd.Timestamp(start_date).tz_localize(None).normalize()
    end = pd.Timestamp(end_date or pd.Timestamp.now(tz="UTC")).tz_localize(None).normalize()
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    request_dates = [
        pd.Timestamp(timestamp).normalize() for timestamp in pd.date_range(start=start, end=end, freq="ME")
    ]
    if not request_dates or request_dates[-1] != end:
        request_dates.append(end)
    return sorted(dict.fromkeys(request_dates))


def resolve_ishares_holdings_snapshot(
    requested_date,
    *,
    max_lookback_days: int = 7,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
    download_fn=download_ishares_holdings_snapshot_for_date,
    source_url_fn=build_ishares_holdings_json_url,
    source_kind: str = ISHARES_OFFICIAL_JSON_SOURCE_KIND,
) -> dict[str, object]:
    requested = pd.Timestamp(requested_date).tz_localize(None).normalize()
    errors: list[str] = []
    for lookback_days in range(max(int(max_lookback_days), 0) + 1):
        candidate_date = requested - pd.Timedelta(days=lookback_days)
        try:
            as_of_date, snapshot = download_fn(candidate_date, holdings_url_template=holdings_url_template)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{candidate_date:%Y-%m-%d}: {type(exc).__name__}: {exc}")
            continue
        if not snapshot.empty:
            return {
                "requested_date": requested,
                "as_of_date": pd.Timestamp(as_of_date).normalize(),
                "lookback_days": lookback_days,
                "source_kind": source_kind,
                "source_url": _build_snapshot_source_url(
                    source_url_fn,
                    as_of_date,
                    holdings_url_template,
                ),
                "snapshot": snapshot,
            }
        errors.append(f"{candidate_date:%Y-%m-%d}: empty snapshot")
    detail = f"; attempts: {'; '.join(errors[-5:])}" if errors else ""
    raise RuntimeError(
        "Could not resolve a non-empty iShares holdings snapshot "
        f"within {max_lookback_days} day(s) before {requested:%Y-%m-%d}{detail}"
    )


def resolve_iwb_holdings_snapshot(
    requested_date,
    *,
    max_lookback_days: int = 7,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
    source_order: tuple[str, ...] = DEFAULT_IWB_HOLDINGS_SOURCE_ORDER,
) -> dict[str, object]:
    source_specs = {
        BLACKROCK_PRODUCT_DATA_HOLDINGS_SOURCE_KIND: {
            "download_fn": download_blackrock_product_data_holdings_snapshot_for_date,
            "source_url_fn": build_blackrock_product_data_holdings_url,
            "holdings_url_template": None,
        },
        ISHARES_OFFICIAL_JSON_SOURCE_KIND: {
            "download_fn": download_ishares_holdings_snapshot_for_date,
            "source_url_fn": build_ishares_holdings_json_url,
            "holdings_url_template": holdings_url_template,
        },
    }
    errors: list[str] = []
    for source_kind in source_order:
        if source_kind not in source_specs:
            raise ValueError(f"unsupported IWB holdings source: {source_kind}")
        source = source_specs[source_kind]
        try:
            return resolve_ishares_holdings_snapshot(
                requested_date,
                max_lookback_days=max_lookback_days,
                holdings_url_template=source["holdings_url_template"],
                download_fn=source["download_fn"],
                source_url_fn=source["source_url_fn"],
                source_kind=source_kind,
            )
        except RuntimeError as exc:
            errors.append(f"{source_kind}: {exc}")
    detail = f"; sources: {' | '.join(errors)}" if errors else ""
    requested = pd.Timestamp(requested_date).tz_localize(None).normalize()
    raise RuntimeError(
        "Could not resolve a non-empty IWB holdings snapshot "
        f"within {max_lookback_days} day(s) before {requested:%Y-%m-%d}{detail}"
    )


def download_ishares_historical_universe_snapshots(
    *,
    start_date,
    end_date=None,
    max_lookback_days: int = 7,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
) -> tuple[list[tuple[pd.Timestamp, pd.DataFrame]], pd.DataFrame]:
    records: list[dict[str, object]] = []
    for requested_date in build_monthly_snapshot_request_dates(start_date, end_date):
        record = resolve_iwb_holdings_snapshot(
            requested_date,
            max_lookback_days=max_lookback_days,
            holdings_url_template=holdings_url_template,
        )
        record["row_count"] = int(len(record["snapshot"]))
        records.append(record)

    if not records:
        raise RuntimeError("No iShares Russell 1000 historical holdings snapshots were downloaded")

    deduped_by_date: dict[pd.Timestamp, dict[str, object]] = {}
    for record in records:
        deduped_by_date[pd.Timestamp(record["as_of_date"]).normalize()] = record

    ordered_records = [deduped_by_date[key] for key in sorted(deduped_by_date)]
    snapshots = [(pd.Timestamp(record["as_of_date"]).normalize(), record["snapshot"]) for record in ordered_records]
    metadata = pd.DataFrame(
        [
            {
                "requested_date": pd.Timestamp(record["requested_date"]).normalize(),
                "as_of_date": pd.Timestamp(record["as_of_date"]).normalize(),
                "source_kind": record["source_kind"],
                "lookback_days": int(record["lookback_days"]),
                "source_url": record["source_url"],
                "row_count": int(record["row_count"]),
            }
            for record in ordered_records
        ]
    )
    return snapshots, metadata


def list_wayback_timestamps(
    url: str,
    *,
    from_year: int = 2020,
    to_year: int | None = None,
    limit: int = 200,
) -> list[str]:
    to_year = to_year or pd.Timestamp.now(tz="UTC").year
    quoted_url = quote(url, safe="")
    cdx_url = (
        f"{WAYBACK_CDX_API_URL}?url={quoted_url}"
        "&output=json"
        "&fl=timestamp"
        "&filter=statuscode:200"
        f"&from={int(from_year)}"
        f"&to={int(to_year)}"
        f"&limit={int(limit)}"
    )
    payload = _fetch_text(cdx_url, timeout=120)
    rows = json.loads(payload)
    return [str(row[0]).strip() for row in rows[1:] if row]


def build_wayback_snapshot_url(timestamp: str, *, holdings_url: str = ISHARES_IWB_HOLDINGS_CSV_URL) -> str:
    return f"https://web.archive.org/web/{timestamp}id_/{holdings_url}"


def download_ishares_holdings_snapshot(url: str) -> tuple[pd.Timestamp, pd.DataFrame]:
    return parse_ishares_holdings_snapshot(_fetch_text(url))


def download_ishares_universe_snapshots(
    *,
    holdings_url: str = ISHARES_IWB_HOLDINGS_CSV_URL,
    from_year: int = 2020,
    to_year: int | None = None,
    include_live: bool = True,
) -> tuple[list[tuple[pd.Timestamp, pd.DataFrame]], pd.DataFrame]:
    records: list[dict[str, object]] = []

    for timestamp in list_wayback_timestamps(holdings_url, from_year=from_year, to_year=to_year):
        source_url = build_wayback_snapshot_url(timestamp, holdings_url=holdings_url)
        as_of_date, snapshot = download_ishares_holdings_snapshot(source_url)
        records.append(
            {
                "as_of_date": as_of_date,
                "source_kind": "wayback",
                "capture_timestamp": timestamp,
                "source_url": source_url,
                "row_count": int(len(snapshot)),
                "snapshot": snapshot,
            }
        )

    if include_live:
        as_of_date, snapshot = download_ishares_holdings_snapshot(holdings_url)
        records.append(
            {
                "as_of_date": as_of_date,
                "source_kind": "live",
                "capture_timestamp": "",
                "source_url": holdings_url,
                "row_count": int(len(snapshot)),
                "snapshot": snapshot,
            }
        )

    if not records:
        raise RuntimeError("No iShares Russell 1000 holdings snapshots were downloaded")

    records.sort(
        key=lambda item: (
            pd.Timestamp(item["as_of_date"]),
            1 if item["source_kind"] == "live" else 0,
            str(item["capture_timestamp"]),
        )
    )

    deduped_by_date: dict[pd.Timestamp, dict[str, object]] = {}
    for record in records:
        deduped_by_date[pd.Timestamp(record["as_of_date"]).normalize()] = record

    ordered_records = [deduped_by_date[key] for key in sorted(deduped_by_date)]
    snapshots = [(pd.Timestamp(record["as_of_date"]).normalize(), record["snapshot"]) for record in ordered_records]
    metadata = pd.DataFrame(
        [
            {
                "as_of_date": pd.Timestamp(record["as_of_date"]).normalize(),
                "source_kind": record["source_kind"],
                "capture_timestamp": record["capture_timestamp"],
                "source_url": record["source_url"],
                "row_count": record["row_count"],
            }
            for record in ordered_records
        ]
    )
    return snapshots, metadata


def _normalize_identifier_value(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "<NA>"}:
        return ""
    return text


def build_symbol_alias_candidates(
    snapshot_tables: list[tuple[pd.Timestamp, pd.DataFrame]],
) -> dict[str, list[str]]:
    if not snapshot_tables:
        raise ValueError("snapshot_tables must not be empty")

    records: list[dict[str, object]] = []
    token_to_indices: dict[str, list[int]] = defaultdict(list)

    for snapshot_date, snapshot in snapshot_tables:
        frame = pd.DataFrame(snapshot).copy()
        if "symbol" not in frame.columns:
            raise ValueError("snapshot missing required columns: symbol")
        frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
        if "name" not in frame.columns:
            frame["name"] = ""
        frame["name"] = frame["name"].fillna("").astype(str).str.strip()
        for column in ISHARES_SNAPSHOT_IDENTIFIER_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.NA

        normalized_date = pd.Timestamp(snapshot_date).normalize()
        for row in frame.itertuples(index=False):
            tokens = [
                f"{column}:{value}"
                for column in ISHARES_SNAPSHOT_IDENTIFIER_COLUMNS
                if (value := _normalize_identifier_value(getattr(row, column, "")))
            ]
            if not tokens:
                continue
            record_index = len(records)
            records.append(
                {
                    "snapshot_date": normalized_date,
                    "symbol": str(getattr(row, "symbol", "")).strip().upper(),
                    "name": str(getattr(row, "name", "")).strip(),
                    "tokens": tokens,
                }
            )
            for token in tokens:
                token_to_indices[token].append(record_index)

    if not records:
        return {}

    parents = list(range(len(records)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for indices in token_to_indices.values():
        if len(indices) <= 1:
            continue
        base = indices[0]
        for other in indices[1:]:
            union(base, other)

    components: dict[int, list[dict[str, object]]] = defaultdict(list)
    for index, record in enumerate(records):
        components[find(index)].append(record)

    alias_candidates: dict[str, list[str]] = {}
    for component_records in components.values():
        symbol_stats: dict[str, dict[str, object]] = {}
        for record in component_records:
            symbol = str(record["symbol"]).strip().upper()
            snapshot_date = pd.Timestamp(record["snapshot_date"]).normalize()
            stats = symbol_stats.setdefault(
                symbol,
                {
                    "first_seen": snapshot_date,
                    "last_seen": snapshot_date,
                },
            )
            stats["first_seen"] = min(pd.Timestamp(stats["first_seen"]).normalize(), snapshot_date)
            stats["last_seen"] = max(pd.Timestamp(stats["last_seen"]).normalize(), snapshot_date)

        ordered_symbols = [
            symbol
            for symbol, _stats in sorted(
                symbol_stats.items(),
                key=lambda item: (
                    -pd.Timestamp(item[1]["last_seen"]).value,
                    -pd.Timestamp(item[1]["first_seen"]).value,
                    item[0],
                ),
            )
        ]
        if len(ordered_symbols) <= 1:
            continue
        for original_symbol in ordered_symbols:
            alias_candidates[original_symbol] = ordered_symbols.copy()

    return alias_candidates


def build_symbol_alias_candidates_from_directory(input_dir: str | Path) -> dict[str, list[str]]:
    return build_symbol_alias_candidates(load_snapshot_tables_from_directory(input_dir))


def build_symbol_alias_table(symbol_aliases: dict[str, list[str]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol in sorted(symbol_aliases):
        for priority, candidate in enumerate(symbol_aliases[symbol], start=1):
            rows.append(
                {
                    "symbol": symbol,
                    "download_candidate": candidate,
                    "priority": priority,
                }
            )
    return pd.DataFrame(rows, columns=["symbol", "download_candidate", "priority"])


def collect_symbol_universe(
    universe_history,
    *,
    benchmark_symbol: str = "SPY",
    safe_haven: str = "BOXX",
) -> list[str]:
    frame = pd.DataFrame(universe_history).copy()
    if "symbol" not in frame.columns:
        raise ValueError("universe_history missing required columns: symbol")
    symbols = frame["symbol"].astype(str).str.upper().str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
    for extra in (benchmark_symbol, safe_haven):
        symbol = str(extra or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def write_interval_universe_history(history: pd.DataFrame, output_path: str | Path) -> None:
    write_table(history, output_path)


# --- Offline IWB SEC N-PORT filing-index/XML adapter (path B preparation) ---
#
# Pure helpers over caller-supplied raw bytes. Synthetic fixtures cover only the
# field subset listed in IWB_SEC_COVERED_NPORT_FIELDS. A minimal structural
# equivalent also covers fields inspected in the 2026-03-31 public submission;
# this is not full SEC schema validation or full raw-sample capture. Event/terminal-price evidence is not
# supplied here; those business requirements stay explicitly incomplete.

IWB_SEC_FILING_CIK = "0001100663"
IWB_SEC_FILING_SERIES_ID = "S000004347"
IWB_SEC_FILING_CLASS_ID = "C000012077"
IWB_SEC_FILING_TICKER = "IWB"
IWB_SEC_NPORT_NAMESPACE = "http://www.sec.gov/edgar/nport"
# Preserve only the namespace of the already committed offline test fixtures;
# it is not an SEC namespace or a live-source/schema-validation assertion.
IWB_SEC_LEGACY_SYNTHETIC_NPORT_NAMESPACE = "http://example.invalid/synthetic-nport-subset"
IWB_SEC_FILING_MAX_INDEX_BYTES = 1_048_576
IWB_SEC_FILING_MAX_XML_BYTES = 8_388_608
IWB_SEC_FILING_SOURCE_ID = "iwb_sec_nport_public_holdings_proxy"
IWB_SEC_FILING_UNIVERSE_ID = "iwb_sec_nport_public_holdings_proxy"
IWB_SEC_SUPPORTED_FORMS = frozenset({"NPORT-P", "NPORT-P/A"})
IWB_SEC_COVERED_NPORT_FIELDS = (
    "headerData",
    "filerInfo",
    "filer",
    "issuerCredentials",
    "cik",
    "seriesClassInfo",
    "seriesId",
    "classId",
    "ticker",
    "formData",
    "genInfo",
    "repPdDate",
    "invstOrSecs",
    "invstOrSec",
    "name",
    "lei",
    "title",
    "identifiers",
    "other",
    "tickers",
    "assetCat",
    "issuerCat",
    "issuerConditional",
    "submissionType",
    "accessionNumber",
    "regCik",
    "repPdEnd",
    "cusip",
    "isin",
    "curCd",
    "valUSD",
    "balance",
    "units",
    "pctVal",
)
# Official N-PORT 1.13 lexical maxima; no checksum or issuer/security-ID validation.
IWB_SEC_ISSUER_IDENTIFIER_MAX_CHARS = 20
IWB_SEC_TITLE_MAX_CHARS = 150
IWB_SEC_OTHER_IDENTIFIER_MAX_CHARS = 150
IWB_SEC_IDENTIFIER_MAX_COUNT = 100
# Scope these names to holdings: official signature/title is nportcommon.
IWB_SEC_HOLDING_LEXICAL_FIELDS = frozenset({"lei", "title", "other"})
IWB_SEC_EQUITY_ASSET_CATS = frozenset({"EC", "EP"})
IWB_SEC_NON_EQUITY_ASSET_HINTS = frozenset(
    {
        "ABS",
        "ABS-MBS",
        "ABS-O",
        "COMM",
        "DBT",
        "DER",
        "DIR",
        "LON",
        "RA",
        "RE",
        "SN",
        "STIV",
        "UST",
        "CASH",
    }
)
_IWB_SEC_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_IWB_SEC_CIK_RE = re.compile(r"^[0-9]{1,10}$")
_IWB_SEC_ACCEPTED_OFFSET_RE = re.compile(
    r"^(?P<naive>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})"
    r"(?P<frac>\.\d+)?"
    r"(?P<offset>Z|[+-]\d{2}:?\d{2})$"
)
_IWB_SEC_ACCEPTED_NAIVE_RE = re.compile(
    r"^(?P<naive>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(?P<frac>\.\d+)?$"
)
_IWB_SEC_FORBIDDEN_XML_TEXT_RE = re.compile(
    r"(?is)<!DOCTYPE\b|<!ENTITY\b|SYSTEM\s+(['\"])[^'\"]+\1|PUBLIC\s+(['\"])[^'\"]+\2|"
    r"<\?xml-stylesheet\b"
)
_IWB_SEC_XML_DECL_ENCODING_RE = re.compile(
    rb"""(?is)<\?xml\b[^>]*\bencoding\s*=\s*(['"])\s*([^'"]+)\s*\1"""
)
class IwbSecFilingAdapterError(ValueError):
    """Raised when offline IWB SEC filing bytes cannot be bound safely."""


@dataclass(frozen=True)
class IwbSecFilingIndexRecord:
    accession_number: str
    form_type: str
    report_period: date
    accepted_at: datetime
    cik: str
    index_sha256: str


@dataclass(frozen=True)
class IwbSecOtherIdentifierRecord:
    """Source-declared opaque value and qualifier, never a tradable ticker alias."""

    description: str
    value: str


@dataclass(frozen=True)
class IwbSecHoldingRecord:
    """Parsed holding with optional, unvalidated source lexical evidence.

    Appended evidence fields are keyword-only and excluded from equality/hash
    to preserve existing positional constructors and record identity semantics.
    Record equality/hash therefore does not establish full evidence equality;
    compare the evidence explicitly and retain the binding's raw XML SHA256.
    ``dataclasses.asdict`` adds issuer_identifier, security_title and
    other_identifiers keys; it is not an unchanged serialized schema.
    """

    name: str
    ticker: str | None
    cusip: str | None
    isin: str | None
    asset_cat: str | None
    issuer_cat: str | None
    status: str
    reasons: tuple[str, ...]
    # Source lexical values, not prices, weights, FX conversions or event evidence.
    currency: str | None = None
    value_usd: str | None = None
    balance: str | None = None
    units: str | None = None
    percent_value: str | None = None
    issuer_category_description: str | None = None
    # Source <lei> permits LEI, RSSD or N/A; issuer-level, not security identity.
    issuer_identifier: str | None = field(default=None, kw_only=True, compare=False)
    security_title: str | None = field(default=None, kw_only=True, compare=False)
    other_identifiers: tuple[IwbSecOtherIdentifierRecord, ...] = field(default=(), kw_only=True, compare=False)


@dataclass(frozen=True)
class IwbSecFilingInputVersion:
    accession_number: str
    version_id: str
    form_type: str
    report_period: date
    accepted_at: datetime
    observed_at: datetime
    cik: str
    series_id: str
    class_id: str
    ticker: str
    index_sha256: str
    xml_sha256: str
    raw_binding_sha256: str
    holdings: tuple[IwbSecHoldingRecord, ...]
    incomplete_items: tuple[Mapping[str, object], ...]
    qualification: str
    trading_eligible: bool
    covered_nport_fields: tuple[str, ...]
    schema_claim: str


def _iwb_sec_fail(message: str) -> None:
    raise IwbSecFilingAdapterError(message)


def _iwb_sec_sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _iwb_sec_require_bytes(raw: object, *, label: str, max_bytes: int) -> bytes:
    if not isinstance(raw, (bytes, bytearray)):
        _iwb_sec_fail(f"{label} must be raw bytes")
    payload = bytes(raw)
    if not payload:
        _iwb_sec_fail(f"{label} is empty")
    if len(payload) > max_bytes:
        _iwb_sec_fail(f"{label} exceeds size limit ({max_bytes} bytes)")
    return payload


def _iwb_sec_require_aware(value: object, label: str) -> datetime:
    if not isinstance(value, datetime):
        _iwb_sec_fail(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        _iwb_sec_fail(f"{label} must be timezone-aware")
    return value


def _iwb_sec_local_name(tag: str) -> str:
    if not isinstance(tag, str):
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _iwb_sec_find_all(root: ET.Element, local_name: str) -> list[ET.Element]:
    return [element for element in root.iter() if _iwb_sec_local_name(element.tag) == local_name]


def _iwb_sec_direct_children(parent: ET.Element, local_name: str) -> list[ET.Element]:
    return [child for child in list(parent) if _iwb_sec_local_name(child.tag) == local_name]


def _iwb_sec_require_exact_one_direct_child(parent: ET.Element, local_name: str) -> ET.Element:
    nodes = _iwb_sec_direct_children(parent, local_name)
    if not nodes:
        _iwb_sec_fail(f"N-PORT XML missing {local_name}")
    if len(nodes) != 1:
        _iwb_sec_fail(f"N-PORT XML duplicate structural container: {local_name}")
    # Reject the same container also nested elsewhere under this parent.
    nested = _iwb_sec_find_all(parent, local_name)
    if len(nested) != 1:
        _iwb_sec_fail(f"N-PORT XML misplaced or nested structural container: {local_name}")
    return nodes[0]


def _iwb_sec_require_exact_one_container(root: ET.Element, local_name: str) -> ET.Element:
    nodes = _iwb_sec_find_all(root, local_name)
    if not nodes:
        _iwb_sec_fail(f"N-PORT XML missing {local_name}")
    if len(nodes) != 1:
        _iwb_sec_fail(f"N-PORT XML duplicate structural container: {local_name}")
    return nodes[0]


def _iwb_sec_singleton_text(
    scope: ET.Element, local_name: str, *, required: bool = True, direct_only: bool = False
) -> str | None:
    """Require exact-one element node; blank text is malformed; identical duplicates reject."""
    nodes = _iwb_sec_direct_children(scope, local_name) if direct_only else _iwb_sec_find_all(scope, local_name)
    if not nodes:
        if required:
            _iwb_sec_fail(f"missing required N-PORT field: {local_name}")
        return None
    if len(nodes) != 1:
        _iwb_sec_fail(f"duplicate required singleton N-PORT field: {local_name}")
    text = "".join(nodes[0].itertext()).strip()
    if not text:
        _iwb_sec_fail(f"blank N-PORT field: {local_name}")
    return text


def _iwb_sec_parse_report_period(value: str) -> date:
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise IwbSecFilingAdapterError("invalid report period") from exc


def _iwb_sec_normalize_cik(value: str) -> str:
    text = (value or "").strip()
    if not _IWB_SEC_CIK_RE.fullmatch(text):
        _iwb_sec_fail("invalid CIK grammar")
    return text.zfill(10)


def _iwb_sec_normalize_form(value: str) -> str:
    form_type = unescape(value or "").strip().upper().replace(" ", "")
    if form_type.startswith("FORM"):
        form_type = form_type[4:].lstrip()
    if form_type not in IWB_SEC_SUPPORTED_FORMS:
        _iwb_sec_fail(f"unsupported form type: {form_type or '<empty>'}")
    return form_type


def _iwb_sec_parse_accepted_at(
    raw_value: str,
    *,
    accepted_timezone: str | timezone | ZoneInfo | None,
) -> datetime:
    text = unescape(raw_value).strip()
    candidate = text
    if "T" not in candidate[:19] and " " in candidate:
        candidate = candidate.replace(" ", "T", 1)
    offset_match = _IWB_SEC_ACCEPTED_OFFSET_RE.fullmatch(candidate)
    naive_match = _IWB_SEC_ACCEPTED_NAIVE_RE.fullmatch(candidate)
    if offset_match is not None:
        frac = offset_match.group("frac")
        offset = offset_match.group("offset")
        naive = datetime.fromisoformat(offset_match.group("naive") + (frac or ""))
        if offset == "Z":
            return naive.replace(tzinfo=timezone.utc)
        sign = 1 if offset[0] == "+" else -1
        hhmm = offset[1:].replace(":", "")
        hours = int(hhmm[:2])
        minutes = int(hhmm[2:] or "0")
        return naive.replace(tzinfo=timezone(sign * timedelta(hours=hours, minutes=minutes)))
    if naive_match is None:
        _iwb_sec_fail("invalid Accepted timestamp")
    if accepted_timezone is None:
        _iwb_sec_fail("Accepted lacks timezone/offset; caller must supply accepted_timezone")
    if isinstance(accepted_timezone, str):
        try:
            tzinfo = ZoneInfo(accepted_timezone)
        except Exception as exc:  # noqa: BLE001 - surface as adapter error
            raise IwbSecFilingAdapterError("invalid accepted_timezone") from exc
    else:
        tzinfo = accepted_timezone
    naive = datetime.fromisoformat(naive_match.group("naive") + (naive_match.group("frac") or ""))
    if naive.tzinfo is not None:
        _iwb_sec_fail("Accepted timestamp already timezone-aware but mismatched parser path")
    return _iwb_sec_localize_strict(naive, tzinfo)


def _iwb_sec_localize_strict(naive: datetime, tzinfo: timezone | ZoneInfo) -> datetime:
    if not isinstance(tzinfo, ZoneInfo):
        return naive.replace(tzinfo=tzinfo)
    fold0 = naive.replace(tzinfo=tzinfo, fold=0)
    fold1 = naive.replace(tzinfo=tzinfo, fold=1)
    round_trip = fold0.astimezone(timezone.utc).astimezone(tzinfo)
    if round_trip.replace(tzinfo=None) != naive:
        _iwb_sec_fail("Accepted timestamp does not exist in the supplied timezone")
    if fold0.utcoffset() != fold1.utcoffset():
        _iwb_sec_fail("Accepted timestamp is ambiguous in the supplied timezone")
    return fold0


class _IwbSecIndexHTMLExtractor(HTMLParser):
    """Bounded labeled-field/tableFile extractor; not a transport validator."""

    _VOID_TAGS = frozenset({
        "area", "base", "basefont", "br", "col", "frame", "hr", "img", "input", "isindex", "link", "meta", "param",
    })
    _OPTIONAL_CONTENT_TAGS = frozenset({"p", "li", "dt", "dd", "colgroup", "thead", "tbody", "tfoot", "option"})

    _LABEL_MAP = {
        "accession number": "accession_number",
        "period of report": "report_period",
        "accepted": "accepted",
        "filing date": "filing_date",
        "form": "form_type",
        "form type": "form_type",
        "cik": "cik",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fields: dict[str, list[str]] = defaultdict(list)
        self.primary_document_types: list[str] = []
        self.saw_table_file = False
        self._capture: str | None = None
        self._capture_depth = 0
        self._buffer: list[str] = []
        self._pending_info_head: str | None = None
        self._in_table_file = False
        self._in_tr = False
        self._in_cell = False
        self._cell_buffer: list[str] = []
        self._row_cells: list[str] = []
        self._header_cells: list[str] = []
        self.text_fragments: list[str] = []
        self._open_required_tags: list[str] = []
        self._saw_html = False
        self._saw_body = False
        self._body_closed = False
        self._html_closed = False
        self._saw_declaration = False
        self._optional_wrapper_ends = False

    def handle_decl(self, decl: str) -> None:
        if self._saw_html or self._saw_declaration:
            _iwb_sec_fail("filing index HTML is malformed or truncated")
        self._saw_declaration = True
        # Only this explicitly supported HTML 4.01 declaration permits the
        # optional HTML/BODY end tags. All consumed fields/containers still close.
        self._optional_wrapper_ends = re.fullmatch(
            r'''(?i)DOCTYPE\s+HTML\s+PUBLIC\s+(['"])-//W3C//DTD HTML 4\.01 Transitional//EN\1\s+'''
            r'''(['"])http://www\.w3\.org/TR/html4/loose\.dtd\2\s*''', decl,
        ) is not None

    def _requires_end_tag(self, tag: str) -> bool:
        # Other tables (e.g. tableSeries) are not parsed as document records.
        # HTML4 allows their row/cell ends to be omitted; tableFile is stricter.
        return tag not in self._VOID_TAGS and tag not in self._OPTIONAL_CONTENT_TAGS and (
            tag not in {"tr", "td", "th"} or self._in_table_file
        )

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._body_closed or self._html_closed:
            _iwb_sec_fail("filing index HTML is malformed or truncated")
        if tag == "html":
            if self._saw_html or self._open_required_tags:
                _iwb_sec_fail("filing index HTML is malformed or truncated")
            self._saw_html = True
        elif tag == "body":
            if self._saw_body or self._open_required_tags != ["html"]:
                _iwb_sec_fail("filing index HTML is malformed or truncated")
            self._saw_body = True
        if self._requires_end_tag(tag):
            self._open_required_tags.append(tag)
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        classes = tuple(attr_map.get("class", "").lower().split())
        href = attr_map.get("href", "")
        for name, value in parse_qsl(urlsplit(href).query, keep_blank_values=True):
            if name.lower() != "cik":
                continue
            if _IWB_SEC_CIK_RE.fullmatch(value):
                self.fields["cik"].append(value.zfill(10))
            elif re.fullmatch(r"[SC][0-9]{9}", value) is None:
                _iwb_sec_fail("invalid CIK query value")
        if tag == "table" and "tablefile" in classes:
            if self._in_table_file:
                _iwb_sec_fail("filing index HTML is malformed or truncated")
            self._in_table_file = True
            self.saw_table_file = True
            self._header_cells = []
            return
        if self._in_table_file and tag == "tr":
            self._in_tr = True
            self._row_cells = []
            return
        if self._in_table_file and self._in_tr and tag in {"td", "th"}:
            self._in_cell = True
            self._cell_buffer = []
            return
        if tag == "div" and "infohead" in classes:
            self._start_capture("info_head")
        elif tag == "div" and "info" in classes and "infohead" not in classes:
            self._start_capture("info")
        elif tag in {"div", "span"} and "formheader" in classes:
            self._start_capture("form_header")

    def handle_endtag(self, tag: str) -> None:
        if self._html_closed or (self._body_closed and tag != "html"):
            _iwb_sec_fail("filing index HTML is malformed or truncated")
        if tag == "html" and self._optional_wrapper_ends and self._open_required_tags == ["html", "body"]:
            self._open_required_tags.pop()
            self._body_closed = True
        if self._requires_end_tag(tag):
            if not self._open_required_tags or self._open_required_tags[-1] != tag:
                _iwb_sec_fail("filing index HTML is incomplete or truncated")
            self._open_required_tags.pop()
        if tag == "body":
            self._body_closed = True
        elif tag == "html":
            self._html_closed = True
        if self._in_table_file and self._in_cell and tag in {"td", "th"}:
            self._in_cell = False
            self._row_cells.append(unescape("".join(self._cell_buffer)).strip())
            self._cell_buffer = []
            return
        if self._in_table_file and self._in_tr and tag == "tr":
            self._in_tr = False
            self._finish_table_row(self._row_cells)
            self._row_cells = []
            return
        if self._in_table_file and tag == "table":
            self._in_table_file = False
            return
        if self._capture is None or len(self._open_required_tags) != self._capture_depth - 1:
            return
        text = unescape("".join(self._buffer)).strip()
        capture = self._capture
        self._capture = None
        self._buffer = []
        if not text:
            return
        if capture == "info_head":
            self._pending_info_head = text
            return
        if capture == "info":
            if self._pending_info_head is None:
                return
            key = self._LABEL_MAP.get(self._pending_info_head.strip().lower())
            self._pending_info_head = None
            if key is not None:
                self.fields[key].append(text)
            return
        if capture == "form_header":
            form_match = re.search(r"(?i)\bForm\s+([A-Za-z0-9/-]+)", text)
            if form_match is not None:
                self.fields["form_type"].append(form_match.group(1))
            elif text.upper().startswith("NPORT"):
                self.fields["form_type"].append(text)

    def handle_data(self, data: str) -> None:
        if (self._body_closed or self._html_closed) and data.strip():
            _iwb_sec_fail("filing index HTML is malformed or truncated")
        self.text_fragments.append(data)
        if self._in_cell:
            self._cell_buffer.append(data)
            return
        if self._capture is not None:
            self._buffer.append(data)
            return
        text = data.strip()
        if not text:
            return
        cik_match = re.search(r"(?i)\bCIK\b\s*[#:]?\s*([^\s<]+)", text)
        if cik_match is not None:
            self.fields["cik"].append(cik_match.group(1).strip("()[]"))
        accession_match = re.search(
            r"(?i)\bAccession\s+(?:Number\b|No\.)\s*[#:]?\s*([0-9]{10}-[0-9]{2}-[0-9]{6})", text
        )
        if accession_match is not None:
            self.fields["accession_number"].append(accession_match.group(1))

    def _start_capture(self, kind: str) -> None:
        if self._capture is not None:
            _iwb_sec_fail("filing index HTML is malformed or truncated")
        self._capture = kind
        self._capture_depth = len(self._open_required_tags)
        self._buffer = []

    def _finish_table_row(self, cells: list[str]) -> None:
        if not cells:
            return
        lowered = [cell.strip().lower() for cell in cells]
        header_tokens = {"seq", "type", "form type", "description", "document", "form", "size"}
        if any(cell in header_tokens for cell in lowered) and all(
            cell in header_tokens or cell == "" for cell in lowered
        ):
            self._header_cells = lowered
            return
        if not self._header_cells:
            # Minimal Form Type | value row support.
            if len(cells) >= 2 and cells[0].strip().lower() in {"form type", "form"}:
                self.primary_document_types.append(cells[1].strip())
            return
        row = {name: cells[index].strip() for index, name in enumerate(self._header_cells) if index < len(cells)}
        seq = row.get("seq", "")
        doc_type = row.get("type") or row.get("form type") or ""
        description = row.get("description", "").lower()
        if seq == "1" or description == "primary document":
            if not doc_type:
                _iwb_sec_fail("filing index primary document row is malformed")
            self.primary_document_types.append(doc_type)

    def unfinished(self) -> bool:
        # This checks supported HTML shape only. The caller's collection receipt,
        # not this parser, is the evidence for HTTP response completion.
        allowed_open = ([], ["html"], ["html", "body"]) if self._optional_wrapper_ends else ([],)
        return (
            not self._saw_html
            or not self._saw_body
            or bool(self.rawdata)
            or self._open_required_tags not in allowed_open
            or self._capture is not None
            or self._pending_info_head is not None
            or self._in_cell
            or self._in_tr
            or self._in_table_file
        )


def _iwb_sec_extract_colon_fields(html_text: str) -> dict[str, list[str]]:
    patterns = {
        "cik": re.compile(r"(?is)\bCIK\b\s*:\s*([^\s<]+)"),
        "accession_number": re.compile(r"(?is)\bAccession\s+Number\b\s*:\s*([0-9]{10}-[0-9]{2}-[0-9]{6})"),
        "form_type": re.compile(r"(?is)\bForm(?:\s+Type)?\b\s*:\s*([A-Za-z0-9/-]+)"),
        "report_period": re.compile(r"(?is)\bPeriod\s+of\s+Report\b\s*:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})"),
        "accepted": re.compile(r"(?is)\bAccepted\b\s*:\s*([^\s<][^<\r\n]*)"),
    }
    fields: dict[str, list[str]] = defaultdict(list)
    for key, pattern in patterns.items():
        fields[key].extend(match.group(1).strip() for match in pattern.finditer(html_text))
    return fields


def _iwb_sec_extract_index_fields(html_text: str) -> dict[str, str]:
    extractor = _IwbSecIndexHTMLExtractor()
    try:
        extractor.feed(html_text)
        if extractor.unfinished():
            _iwb_sec_fail("filing index HTML is incomplete or truncated")
        extractor.close()
    except IwbSecFilingAdapterError:
        raise
    except Exception as exc:  # noqa: BLE001 - malformed HTML is an adapter failure
        raise IwbSecFilingAdapterError("filing index HTML is malformed or truncated") from exc
    merged: dict[str, list[str]] = defaultdict(list)
    # Official "SEC Accession No." may split label/value across strong/div
    # tags. Match the accumulated text, rather than assume a labeled info div.
    accession_pattern = r"(?i)\bAccession\s+(?:Number\b|No\.)\s*[#:]?\s*([0-9]{10}-[0-9]{2}-[0-9]{6})"
    merged["accession_number"].extend(re.findall(accession_pattern, " ".join(extractor.text_fragments)))
    for source in (extractor.fields, _iwb_sec_extract_colon_fields(html_text)):
        for key, values in source.items():
            merged[key].extend(values)
    header_forms = {value.strip() for value in merged.get("form_type", []) if value and value.strip()}
    primary_types = {value.strip() for value in extractor.primary_document_types if value and value.strip()}
    if extractor.saw_table_file and not primary_types:
        _iwb_sec_fail("filing index primary document row is missing or malformed")
    if primary_types:
        if len(primary_types) != 1:
            _iwb_sec_fail("duplicate conflicting filing index primary document type")
        primary_form = next(iter(primary_types))
        try:
            normalized_primary = _iwb_sec_normalize_form(primary_form)
        except IwbSecFilingAdapterError as exc:
            raise IwbSecFilingAdapterError(f"unsupported primary document type: {primary_form}") from exc
        if header_forms:
            normalized_headers: set[str] = set()
            for raw_header in header_forms:
                try:
                    normalized_headers.add(_iwb_sec_normalize_form(raw_header))
                except IwbSecFilingAdapterError as exc:
                    raise IwbSecFilingAdapterError(
                        "filing index has conflicting or unsupported header form"
                    ) from exc
            # Require exact agreement; do not drop conflicting headers that happen to
            # include the primary type.
            if normalized_headers != {normalized_primary}:
                _iwb_sec_fail("filing index header form disagrees with primary document type")
        merged["form_type"] = [normalized_primary]
    # Filing Date is collected for ambiguity checks but is not Accepted/availability.
    required = ("cik", "accession_number", "form_type", "report_period", "accepted")
    missing = [key for key in required if not merged.get(key)]
    if missing:
        _iwb_sec_fail(f"filing index missing fields: {', '.join(missing)}")
    singleton: dict[str, str] = {}
    for key in required:
        unique = {value.strip() for value in merged[key] if value and value.strip()}
        if len(unique) != 1:
            _iwb_sec_fail(f"duplicate conflicting filing index field: {key}")
        singleton[key] = next(iter(unique))
    return singleton


def parse_iwb_sec_filing_index_html(
    raw_index_html: bytes,
    *,
    accepted_timezone: str | timezone | ZoneInfo | None = None,
) -> IwbSecFilingIndexRecord:
    """Parse caller-supplied SEC filing-index HTML bytes for the IWB series."""
    payload = _iwb_sec_require_bytes(
        raw_index_html, label="filing index HTML", max_bytes=IWB_SEC_FILING_MAX_INDEX_BYTES
    )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IwbSecFilingAdapterError("filing index HTML is not valid UTF-8") from exc
    if "<" not in text:
        _iwb_sec_fail("filing index HTML is malformed or truncated")
    fields = _iwb_sec_extract_index_fields(text)
    cik = _iwb_sec_normalize_cik(fields["cik"])
    if cik != IWB_SEC_FILING_CIK:
        _iwb_sec_fail(f"filing index CIK is not IWB trust CIK {IWB_SEC_FILING_CIK}")
    accession = fields["accession_number"]
    if not _IWB_SEC_ACCESSION_RE.fullmatch(accession):
        _iwb_sec_fail("invalid accession number")
    form_type = _iwb_sec_normalize_form(fields["form_type"])
    accepted_at = _iwb_sec_parse_accepted_at(fields["accepted"], accepted_timezone=accepted_timezone)
    return IwbSecFilingIndexRecord(
        accession_number=accession,
        form_type=form_type,
        report_period=_iwb_sec_parse_report_period(fields["report_period"]),
        accepted_at=accepted_at,
        cik=cik,
        index_sha256=_iwb_sec_sha256(payload),
    )


def _iwb_sec_reject_unsafe_xml_text(text: str) -> None:
    if _IWB_SEC_FORBIDDEN_XML_TEXT_RE.search(text):
        _iwb_sec_fail("N-PORT XML rejects DTD/entity/external subset constructs")


def _iwb_sec_decode_xml_utf8_only(payload: bytes) -> bytes:
    """Reject non-UTF-8 / BOM / UTF-16/32 payloads; return screened UTF-8 bytes only."""
    if payload.startswith((b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf")):
        _iwb_sec_fail("N-PORT XML rejects BOM/UTF-16/UTF-32 encodings")
    if b"\x00" in payload:
        _iwb_sec_fail("N-PORT XML rejects NUL-bearing alternate encodings")
    decl = _IWB_SEC_XML_DECL_ENCODING_RE.search(payload)
    if decl is not None:
        encoding_name = decl.group(2).decode("ascii", errors="replace").strip().lower()
        if encoding_name not in {"utf-8", "utf8"}:
            _iwb_sec_fail(f"N-PORT XML encoding is not UTF-8: {encoding_name}")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IwbSecFilingAdapterError("N-PORT XML is not valid UTF-8") from exc
    _iwb_sec_reject_unsafe_xml_text(text)
    return text.encode("utf-8")


def _iwb_sec_parse_xml_root(payload: bytes) -> ET.Element:
    screened = _iwb_sec_decode_xml_utf8_only(payload)
    try:
        root = ET.fromstring(screened)
    except ET.ParseError as exc:
        raise IwbSecFilingAdapterError("N-PORT XML is malformed or truncated") from exc
    if _iwb_sec_local_name(root.tag) != "edgarSubmission":
        _iwb_sec_fail("N-PORT XML root must be edgarSubmission")
    namespace = next(
        (
            candidate
            for candidate in (IWB_SEC_NPORT_NAMESPACE, IWB_SEC_LEGACY_SYNTHETIC_NPORT_NAMESPACE)
            if root.tag == f"{{{candidate}}}edgarSubmission"
        ),
        None,
    )
    if namespace is None:
        _iwb_sec_fail("N-PORT XML root namespace is not supported")
    # Both allowed roots enforce the same rule. New lexical fields are checked
    # at their consumed holding paths, since signature/title uses nportcommon.
    globally_covered_fields = set(IWB_SEC_COVERED_NPORT_FIELDS) - IWB_SEC_HOLDING_LEXICAL_FIELDS
    for node in root.iter():
        local = _iwb_sec_local_name(node.tag)
        if local in globally_covered_fields and node.tag != f"{{{namespace}}}{local}":
            _iwb_sec_fail("N-PORT consumed field namespace mismatch")
    return root


def _iwb_sec_identifier_value(node: ET.Element) -> str | None:
    text = "".join(node.itertext()).strip()
    attribute = node.attrib.get("value", "").strip()
    if text and attribute and text != attribute:
        _iwb_sec_fail(f"conflicting identifier text/value: {_iwb_sec_local_name(node.tag)}")
    return text or attribute or None


def _iwb_sec_has_security_identifier_evidence(value: str | None) -> bool:
    """Exclude only absent values and official NA_TYPE's exact N/A placeholder.

    Presence is lexical evidence, not checksum validity or a security-master
    match. Do not guess additional sentinels or normalize opaque source codes.
    """
    return bool(value) and value != "N/A"


def _iwb_sec_optional_bounded_lexical_text(
    holding: ET.Element, local_name: str, *, max_chars: int
) -> str | None:
    nodes = _iwb_sec_direct_children(holding, local_name)
    if not nodes:
        return None
    expected_tag = holding.tag.removesuffix("invstOrSec") + local_name
    if any(node.tag != expected_tag for node in nodes):
        _iwb_sec_fail("N-PORT consumed field namespace mismatch")
    _iwb_sec_singleton_text(holding, local_name, direct_only=True)
    if list(nodes[0]) or nodes[0].attrib:
        _iwb_sec_fail(f"invalid N-PORT lexical field structure: {local_name}")
    value = nodes[0].text or ""
    if not 1 <= len(value) <= max_chars:
        _iwb_sec_fail(f"invalid N-PORT lexical field length: {local_name}")
    return value


def _iwb_sec_qualified_other_identifiers(identifiers: ET.Element | None) -> tuple[IwbSecOtherIdentifierRecord, ...]:
    if identifiers is None:
        return ()
    if len(list(identifiers)) > IWB_SEC_IDENTIFIER_MAX_COUNT:
        _iwb_sec_fail("N-PORT identifier count exceeds official schema maximum")
    nodes = _iwb_sec_direct_children(identifiers, "other")
    if len(nodes) != len(_iwb_sec_find_all(identifiers, "other")):
        _iwb_sec_fail("N-PORT misplaced or nested other identifier")
    result: list[IwbSecOtherIdentifierRecord] = []
    for node in nodes:
        if node.tag != identifiers.tag.removesuffix("identifiers") + "other":
            _iwb_sec_fail("N-PORT consumed field namespace mismatch")
        if list(node) or (node.text or "").strip() or set(node.attrib) != {"otherDesc", "value"}:
            _iwb_sec_fail("invalid N-PORT qualified other identifier structure")
        description = node.attrib["otherDesc"]
        value = node.attrib["value"]
        if not all(text.strip() and 1 <= len(text) <= IWB_SEC_OTHER_IDENTIFIER_MAX_CHARS
                   for text in (description, value)):
            _iwb_sec_fail("invalid N-PORT qualified other identifier length")
        # Retain parsed lexical characters, case, source order and repetitions without merging
        # namespaces, asserting canonical identity or synthesizing a ticker.
        result.append(IwbSecOtherIdentifierRecord(description=description, value=value))
    return tuple(result)


def _iwb_sec_classify_holding(holding: ET.Element) -> IwbSecHoldingRecord:
    name_nodes = [child for child in list(holding) if _iwb_sec_local_name(child.tag) == "name"]
    if len(name_nodes) > 1:
        _iwb_sec_fail("duplicate required singleton N-PORT field: name")
    name = "".join(name_nodes[0].itertext()).strip() if name_nodes else ""
    identifiers_nodes = [child for child in list(holding) if _iwb_sec_local_name(child.tag) == "identifiers"]
    if len(identifiers_nodes) > 1:
        _iwb_sec_fail("duplicate required singleton N-PORT field: identifiers")
    identifiers = identifiers_nodes[0] if identifiers_nodes else None
    issuer_identifier = _iwb_sec_optional_bounded_lexical_text(
        holding, "lei", max_chars=IWB_SEC_ISSUER_IDENTIFIER_MAX_CHARS
    )
    security_title = _iwb_sec_optional_bounded_lexical_text(holding, "title", max_chars=IWB_SEC_TITLE_MAX_CHARS)
    other_identifiers = _iwb_sec_qualified_other_identifiers(identifiers)
    cusip_nodes = _iwb_sec_direct_children(holding, "cusip")
    isin_nodes: list[ET.Element] = []
    if identifiers is not None:
        cusip_nodes.extend(_iwb_sec_find_all(identifiers, "cusip"))
        isin_nodes = _iwb_sec_find_all(identifiers, "isin")
    if len(cusip_nodes) > 1:
        _iwb_sec_fail("duplicate required singleton N-PORT field: cusip")
    if len(isin_nodes) > 1:
        _iwb_sec_fail("duplicate required singleton N-PORT field: isin")
    cusip = _iwb_sec_identifier_value(cusip_nodes[0]) if cusip_nodes else None
    isin = _iwb_sec_identifier_value(isin_nodes[0]) if isin_nodes else None
    ticker_candidates: list[str] = []
    search_roots = [node for node in (identifiers, holding) if node is not None]
    seen_ticker_nodes: set[int] = set()
    for root in search_roots:
        for tickers_node in _iwb_sec_find_all(root, "tickers"):
            for child in list(tickers_node):
                if _iwb_sec_local_name(child.tag) != "ticker":
                    continue
                node_id = id(child)
                if node_id in seen_ticker_nodes:
                    continue
                seen_ticker_nodes.add(node_id)
                text = (_iwb_sec_identifier_value(child) or "").upper()
                if text:
                    ticker_candidates.append(text)
        for child in list(root):
            if _iwb_sec_local_name(child.tag) != "ticker":
                continue
            node_id = id(child)
            if node_id in seen_ticker_nodes:
                continue
            seen_ticker_nodes.add(node_id)
            text = (_iwb_sec_identifier_value(child) or "").upper()
            if text:
                ticker_candidates.append(text)
    unique_tickers = list(dict.fromkeys(ticker_candidates))
    asset_cat = _iwb_sec_singleton_text(holding, "assetCat", required=False, direct_only=True)
    issuer_cat = _iwb_sec_singleton_text(holding, "issuerCat", required=False, direct_only=True)
    issuer_description = None
    conditional = _iwb_sec_direct_children(holding, "issuerConditional")
    if len(conditional) > 1 or (conditional and issuer_cat is not None):
        _iwb_sec_fail("duplicate required singleton N-PORT field: issuer category")
    if conditional:
        issuer_cat = conditional[0].attrib.get("issuerCat", "").strip() or None
        issuer_description = conditional[0].attrib.get("desc", "").strip() or None
    reasons: list[str] = []
    ticker: str | None
    if not unique_tickers:
        ticker = None
        reasons.append("missing_ticker")
    elif len(unique_tickers) > 1:
        ticker = None
        reasons.append("conflicting_ticker_identity")
    else:
        ticker = unique_tickers[0]
    asset_upper = (asset_cat or "").strip().upper()
    name_upper = name.upper()
    if not asset_upper:
        reasons.append("missing_asset_cat")
    elif asset_upper in IWB_SEC_NON_EQUITY_ASSET_HINTS or asset_upper not in IWB_SEC_EQUITY_ASSET_CATS:
        reasons.append(f"non_equity_or_unsupported_asset_cat:{asset_upper or 'unknown'}")
    # Conservative identity qualification: a declared ticker plus issuer/title
    # or opaque other ID does not supply security-code evidence for an equity.
    if asset_upper in IWB_SEC_EQUITY_ASSET_CATS and not (
        _iwb_sec_has_security_identifier_evidence(cusip) or _iwb_sec_has_security_identifier_evidence(isin)
    ):
        reasons.append("security_identifier_evidence_not_supplied")
    if "CONTINGENT" in name_upper:
        reasons.append("contingent_consideration_unresolved")
    if "SPINOFF" in name_upper or "SPIN-OFF" in name_upper:
        reasons.append("spinoff_unresolved")
    if "CASH" in name_upper and asset_upper in {"CASH", "STIV"}:
        reasons.append("cash_consideration_unresolved")
    reasons.extend(
        (
            "event_evidence_not_supplied",
            "terminal_price_evidence_not_supplied",
            "corporate_action_consideration_unresolved",
        )
    )
    blocking = {
        "missing_ticker",
        "conflicting_ticker_identity",
        "missing_asset_cat",
        "contingent_consideration_unresolved",
        "spinoff_unresolved",
        "cash_consideration_unresolved",
        "security_identifier_evidence_not_supplied",
    }
    if any(reason in blocking or reason.startswith("non_equity_or_unsupported_asset_cat:") for reason in reasons):
        if any(reason in reasons for reason in (
            "missing_ticker", "conflicting_ticker_identity", "security_identifier_evidence_not_supplied"
        )):
            status = "unresolved"
        else:
            status = "unsupported"
    else:
        status = "resolved_equity"
    return IwbSecHoldingRecord(
        name=name,
        ticker=ticker,
        cusip=cusip,
        isin=isin,
        asset_cat=asset_cat,
        issuer_cat=issuer_cat,
        status=status,
        reasons=tuple(dict.fromkeys(reasons)),
        currency=_iwb_sec_singleton_text(holding, "curCd", required=False, direct_only=True),
        value_usd=_iwb_sec_singleton_text(holding, "valUSD", required=False, direct_only=True),
        balance=_iwb_sec_singleton_text(holding, "balance", required=False, direct_only=True),
        units=_iwb_sec_singleton_text(holding, "units", required=False, direct_only=True),
        percent_value=_iwb_sec_singleton_text(holding, "pctVal", required=False, direct_only=True),
        issuer_category_description=issuer_description,
        issuer_identifier=issuer_identifier,
        security_title=security_title,
        other_identifiers=other_identifiers,
    )


def parse_iwb_sec_nport_xml_bytes(
    raw_xml: bytes,
    *,
    expected_index: IwbSecFilingIndexRecord | None = None,
) -> tuple[dict[str, object], tuple[IwbSecHoldingRecord, ...]]:
    """Parse an inspected field subset; this is not full N-PORT XSD validation."""
    payload = _iwb_sec_require_bytes(raw_xml, label="N-PORT XML", max_bytes=IWB_SEC_FILING_MAX_XML_BYTES)
    root = _iwb_sec_parse_xml_root(payload)
    header = _iwb_sec_require_exact_one_direct_child(root, "headerData")
    form_data = _iwb_sec_require_exact_one_direct_child(root, "formData")
    series_info = _iwb_sec_require_exact_one_container(header, "seriesClassInfo")
    gen_info = _iwb_sec_require_exact_one_direct_child(form_data, "genInfo")
    holdings_parent = _iwb_sec_require_exact_one_direct_child(form_data, "invstOrSecs")

    cik_text = _iwb_sec_singleton_text(header, "cik")
    assert cik_text is not None
    cik = _iwb_sec_normalize_cik(cik_text)
    if cik != IWB_SEC_FILING_CIK:
        _iwb_sec_fail(f"N-PORT CIK is not IWB trust CIK {IWB_SEC_FILING_CIK}")
    series_id = _iwb_sec_singleton_text(series_info, "seriesId")
    class_id = _iwb_sec_singleton_text(series_info, "classId")
    declared_ticker = _iwb_sec_singleton_text(series_info, "ticker", required=False)
    if series_id != IWB_SEC_FILING_SERIES_ID:
        _iwb_sec_fail(
            f"N-PORT seriesId {series_id!r} is not IWB series {IWB_SEC_FILING_SERIES_ID}; "
            "wrong growth/other series rejected"
        )
    if class_id != IWB_SEC_FILING_CLASS_ID:
        _iwb_sec_fail(f"N-PORT classId {class_id!r} is not IWB class {IWB_SEC_FILING_CLASS_ID}")
    if declared_ticker is not None and declared_ticker.upper() != IWB_SEC_FILING_TICKER:
        _iwb_sec_fail(f"N-PORT ticker {declared_ticker!r} is not {IWB_SEC_FILING_TICKER}")
    # The public XML omits the fund ticker. This fixed fund mapping is valid
    # only after exact CIK/series/class checks; it never maps holding tickers.
    ticker = IWB_SEC_FILING_TICKER
    gen_cik = _iwb_sec_singleton_text(gen_info, "regCik", required=False)
    if gen_cik is not None and _iwb_sec_normalize_cik(gen_cik) != cik:
        _iwb_sec_fail("N-PORT genInfo regCik mismatch")
    gen_series = _iwb_sec_singleton_text(gen_info, "seriesId", required=False)
    if gen_series is not None and gen_series != series_id:
        _iwb_sec_fail("N-PORT genInfo seriesId mismatch")
    report_period_text = _iwb_sec_singleton_text(gen_info, "repPdDate")
    assert report_period_text is not None
    report_period = _iwb_sec_parse_report_period(report_period_text)
    report_end_text = _iwb_sec_singleton_text(gen_info, "repPdEnd", required=False)
    accession = _iwb_sec_singleton_text(header, "accessionNumber", required=False)
    # Official submissionType is in headerData; retain the legacy synthetic
    # placement while rejecting duplicates/conflicts anywhere in the document.
    submission_type = _iwb_sec_singleton_text(root, "submissionType", required=False)
    if submission_type is not None:
        submission_type = _iwb_sec_normalize_form(submission_type)
    if expected_index is not None:
        if expected_index.cik != cik:
            _iwb_sec_fail("index/XML CIK mismatch")
        if expected_index.report_period != report_period:
            _iwb_sec_fail("index/XML report period mismatch")
        if accession is not None and accession != expected_index.accession_number:
            _iwb_sec_fail("index/XML accession mismatch")
        if submission_type is not None and submission_type != expected_index.form_type:
            _iwb_sec_fail("index/XML submissionType mismatch")
        if expected_index.form_type not in IWB_SEC_SUPPORTED_FORMS:
            _iwb_sec_fail(f"unsupported form type: {expected_index.form_type}")
    holding_nodes: list[ET.Element] = []
    for child in list(holdings_parent):
        local = _iwb_sec_local_name(child.tag)
        if local != "invstOrSec":
            _iwb_sec_fail(f"unrecognized invstOrSecs child: {local}")
        holding_nodes.append(child)
    if not holding_nodes:
        _iwb_sec_fail("N-PORT XML invstOrSecs is empty")
    nested_holdings = _iwb_sec_find_all(holdings_parent, "invstOrSec")
    if len(nested_holdings) != len(holding_nodes):
        _iwb_sec_fail("malformed nested or misplaced invstOrSec placement")
    holdings = tuple(_iwb_sec_classify_holding(node) for node in holding_nodes)
    ticker_to_cusips: dict[str, set[str]] = defaultdict(set)
    ticker_to_isins: dict[str, set[str]] = defaultdict(set)
    cusip_to_tickers: dict[str, set[str]] = defaultdict(set)
    isin_to_tickers: dict[str, set[str]] = defaultdict(set)
    for holding in holdings:
        if not holding.ticker:
            continue
        if _iwb_sec_has_security_identifier_evidence(holding.cusip):
            ticker_to_cusips[holding.ticker].add(holding.cusip)
            cusip_to_tickers[holding.cusip].add(holding.ticker)
        if _iwb_sec_has_security_identifier_evidence(holding.isin):
            ticker_to_isins[holding.ticker].add(holding.isin)
            isin_to_tickers[holding.isin].add(holding.ticker)
    conflict_tickers = {
        symbol
        for symbol in set(ticker_to_cusips) | set(ticker_to_isins)
        if len(ticker_to_cusips.get(symbol, ())) > 1 or len(ticker_to_isins.get(symbol, ())) > 1
    }
    conflict_cusips = {value for value, symbols in cusip_to_tickers.items() if len(symbols) > 1}
    conflict_isins = {value for value, symbols in isin_to_tickers.items() if len(symbols) > 1}
    if conflict_tickers or conflict_cusips or conflict_isins:
        rebuilt: list[IwbSecHoldingRecord] = []
        for holding in holdings:
            reasons = list(holding.reasons)
            if (
                (holding.ticker and holding.ticker in conflict_tickers)
                or (holding.cusip and holding.cusip in conflict_cusips)
                or (holding.isin and holding.isin in conflict_isins)
            ):
                reasons.append("identity_or_code_reuse_conflict")
                rebuilt.append(
                    IwbSecHoldingRecord(
                        name=holding.name,
                        ticker=holding.ticker,
                        cusip=holding.cusip,
                        isin=holding.isin,
                        asset_cat=holding.asset_cat,
                        issuer_cat=holding.issuer_cat,
                        status="unresolved",
                        reasons=tuple(dict.fromkeys(reasons)),
                        currency=holding.currency,
                        value_usd=holding.value_usd,
                        balance=holding.balance,
                        units=holding.units,
                        percent_value=holding.percent_value,
                        issuer_category_description=holding.issuer_category_description,
                        issuer_identifier=holding.issuer_identifier,
                        security_title=holding.security_title,
                        other_identifiers=holding.other_identifiers,
                    )
                )
            else:
                rebuilt.append(holding)
        holdings = tuple(rebuilt)
    meta: dict[str, object] = {
        "cik": cik,
        "series_id": series_id,
        "class_id": class_id,
        "ticker": ticker,
        "ticker_origin": "xml_declared" if declared_ticker else "pinned_cik_series_class_identity",
        "report_period": report_period,
        "report_period_end": _iwb_sec_parse_report_period(report_end_text) if report_end_text else None,
        "accession_number": accession,
        "submission_type": submission_type,
        "xml_sha256": _iwb_sec_sha256(payload),
        "covered_nport_fields": IWB_SEC_COVERED_NPORT_FIELDS,
        "schema_claim": "synthetic_subset_not_verified_sec_sample",
    }
    return meta, holdings


def _iwb_sec_incomplete_items(holdings: Sequence[IwbSecHoldingRecord]) -> tuple[Mapping[str, object], ...]:
    items: list[Mapping[str, object]] = [
        MappingProxyType(
            {
                "kind": "holding_incomplete",
                "name": holding.name,
                "ticker": holding.ticker,
                "status": holding.status,
                "reasons": holding.reasons,
            }
        )
        for holding in holdings
    ]
    items.append(
        MappingProxyType(
            {
                "kind": "adapter_incomplete",
                "reasons": (
                    "event_evidence_not_supplied",
                    "terminal_price_evidence_not_supplied",
                    "not_full_equity_universe_claim",
                    "trading_qualification_false",
                ),
            }
        )
    )
    return tuple(items)


def bind_iwb_sec_filing_input_version(
    *,
    index_html_bytes: bytes,
    nport_xml_bytes: bytes,
    observed_at: datetime,
    version_id: str,
    accepted_timezone: str | timezone | ZoneInfo | None = None,
    qualification: str = "synthetic",
) -> IwbSecFilingInputVersion:
    """Bind index+XML bytes to one immutable observed input version.

    ``observed_at`` must be the caller-supplied observation-completed aware
    timestamp after the complete response was in hand. Accepted/report period
    never become availability. No clock/lag is fabricated here.
    """
    if not isinstance(version_id, str) or not version_id.strip():
        _iwb_sec_fail("version_id is required")
    if not isinstance(qualification, str) or not qualification.strip():
        _iwb_sec_fail("qualification is required")
    observed = _iwb_sec_require_aware(observed_at, "observed_at")
    index_payload = _iwb_sec_require_bytes(
        index_html_bytes, label="filing index HTML", max_bytes=IWB_SEC_FILING_MAX_INDEX_BYTES
    )
    xml_payload = _iwb_sec_require_bytes(
        nport_xml_bytes, label="N-PORT XML", max_bytes=IWB_SEC_FILING_MAX_XML_BYTES
    )
    index_record = parse_iwb_sec_filing_index_html(
        index_payload, accepted_timezone=accepted_timezone
    )
    meta, holdings = parse_iwb_sec_nport_xml_bytes(xml_payload, expected_index=index_record)
    if observed <= index_record.accepted_at:
        _iwb_sec_fail("observed_at must be after Accepted and after complete response")
    return IwbSecFilingInputVersion(
        accession_number=index_record.accession_number,
        version_id=version_id.strip(),
        form_type=index_record.form_type,
        report_period=index_record.report_period,
        accepted_at=index_record.accepted_at,
        observed_at=observed,
        cik=str(meta["cik"]),
        series_id=str(meta["series_id"]),
        class_id=str(meta["class_id"]),
        ticker=str(meta["ticker"]),
        index_sha256=index_record.index_sha256,
        xml_sha256=str(meta["xml_sha256"]),
        raw_binding_sha256=_iwb_sec_sha256(index_payload + b"\0" + xml_payload),
        holdings=holdings,
        incomplete_items=_iwb_sec_incomplete_items(holdings),
        qualification=qualification.strip(),
        trading_eligible=False,
        covered_nport_fields=IWB_SEC_COVERED_NPORT_FIELDS,
        schema_claim="synthetic_subset_not_verified_sec_sample",
    )


def select_iwb_sec_filing_input_version_at_cutoff(
    versions: Sequence[IwbSecFilingInputVersion],
    *,
    decision_at: datetime,
) -> IwbSecFilingInputVersion:
    """Select the unique known input version available at ``decision_at``.

    All supplied versions must share one report-period/fund revision family.
    Accepted orders source filings but never establishes availability. Same
    accession revisions then use observed time; equal-time conflicts fail.
    """
    deadline = _iwb_sec_require_aware(decision_at, "decision_at")
    if not versions:
        _iwb_sec_fail("no filing input versions supplied")
    periods = {version.report_period for version in versions}
    if len(periods) != 1:
        _iwb_sec_fail("mixed report-period filing versions are not one revision family")
    families = {(version.cik, version.series_id, version.class_id, version.ticker) for version in versions}
    if len(families) != 1:
        _iwb_sec_fail("mixed fund-identity filing versions are not one revision family")
    known = [version for version in versions if version.observed_at <= deadline]
    if not known:
        _iwb_sec_fail("no filing input version known at decision cutoff")
    newest_accepted = max(version.accepted_at for version in known)
    accepted_newest = [version for version in known if version.accepted_at == newest_accepted]
    latest_observed = max(version.observed_at for version in accepted_newest)
    latest = [version for version in accepted_newest if version.observed_at == latest_observed]
    digests = {version.raw_binding_sha256 for version in latest}
    identities = {(version.accession_number, version.version_id) for version in latest}
    if len(latest) != 1 or len(digests) != 1 or len(identities) != 1:
        _iwb_sec_fail("equal-time conflicting filing versions at decision cutoff")
    return latest[0]


def _iwb_sec_ceil_utc_second(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    if utc_value.microsecond:
        return utc_value.replace(microsecond=0) + timedelta(seconds=1)
    return utc_value.replace(microsecond=0)


def _iwb_sec_floor_utc_second(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iwb_sec_mss_observation_available_at(observed_at: datetime) -> str:
    """Convert observation time to MSS second-resolution UTC without rounding earlier."""
    observed = _iwb_sec_require_aware(observed_at, "observed_at")
    return _iwb_sec_ceil_utc_second(observed).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iwb_sec_mss_decision_at(decision_at: datetime) -> str:
    """Floor decision precision for MSS seconds; never ceil a decision later."""
    deadline = _iwb_sec_require_aware(decision_at, "decision_at")
    return _iwb_sec_floor_utc_second(deadline).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iwb_sec_lazy_mss():
    try:
        from market_signal_sources.artifacts.point_in_time_universe import (
            build_point_in_time_universe_snapshot,
            validate_universe_snapshot_for_decision,
        )
    except ImportError as exc:  # pragma: no cover - depends on local editable install
        raise IwbSecFilingAdapterError(
            "MarketSignalSources point_in_time_universe is not importable"
        ) from exc
    return build_point_in_time_universe_snapshot, validate_universe_snapshot_for_decision


def _iwb_sec_require_bridgeable_membership(version: IwbSecFilingInputVersion) -> tuple[str, ...]:
    if not version.holdings:
        _iwb_sec_fail("canonical bridge rejected: empty holdings")
    blocking = [holding for holding in version.holdings if holding.status != "resolved_equity"]
    if blocking:
        _iwb_sec_fail(
            "canonical bridge rejected: unresolved/unsupported/identity-conflicting membership rows present"
        )
    symbols: list[str] = []
    for holding in version.holdings:
        if not holding.ticker:
            _iwb_sec_fail("canonical bridge rejected: resolved equity missing ticker")
        if holding.ticker not in symbols:
            symbols.append(holding.ticker)
    if not symbols:
        _iwb_sec_fail("canonical bridge rejected: no resolved equity membership")
    return tuple(symbols)


def iwb_sec_research_candidate_symbols(version: IwbSecFilingInputVersion) -> tuple[str, ...]:
    """Explicit partial research helper; not an ordinary complete contract input."""
    symbols: list[str] = []
    for holding in version.holdings:
        if holding.ticker and holding.status == "resolved_equity" and holding.ticker not in symbols:
            symbols.append(holding.ticker)
    return tuple(symbols)


def build_iwb_sec_point_in_time_universe_snapshot(
    version: IwbSecFilingInputVersion,
    *,
    license_scope: str = "sec_public_edgar_synthetic_offline",
) -> dict[str, object]:
    """Bridge one fully resolved membership version into the existing MSS constructor."""
    symbols = _iwb_sec_require_bridgeable_membership(version)
    build_snapshot, _validate_for_decision = _iwb_sec_lazy_mss()
    return build_snapshot(
        universe_id=IWB_SEC_FILING_UNIVERSE_ID,
        effective_date=version.report_period.isoformat(),
        available_at=_iwb_sec_mss_observation_available_at(version.observed_at),
        source_id=IWB_SEC_FILING_SOURCE_ID,
        raw_artifact_sha256=version.raw_binding_sha256,
        license_scope=license_scope,
        constituents=symbols,
    )


def validate_iwb_sec_universe_snapshot_for_decision(
    snapshot: Mapping[str, object],
    *,
    decision_at: datetime,
) -> dict[str, object]:
    """Validate MSS snapshot with floored decision precision and direct causal check."""
    _build_snapshot, validate_for_decision = _iwb_sec_lazy_mss()
    deadline = _iwb_sec_require_aware(decision_at, "decision_at")
    available_text = snapshot.get("available_at")
    if not isinstance(available_text, str):
        _iwb_sec_fail("invalid universe snapshot available_at")
    try:
        available_at = datetime.strptime(available_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise IwbSecFilingAdapterError("invalid universe snapshot available_at") from exc
    decision_floor = _iwb_sec_floor_utc_second(deadline)
    if available_at > decision_floor:
        _iwb_sec_fail("universe snapshot was unavailable at decision time")
    return validate_for_decision(snapshot, decision_at=_iwb_sec_mss_decision_at(deadline))


def iwb_sec_universe_rows_for_ues(
    version: IwbSecFilingInputVersion,
) -> tuple[Mapping[str, object], ...]:
    """UES-facing rows for fully resolved membership only; fail closed otherwise."""
    symbols = _iwb_sec_require_bridgeable_membership(version)
    return tuple(
        MappingProxyType({"symbol": symbol, "visible_at": version.observed_at}) for symbol in symbols
    )
