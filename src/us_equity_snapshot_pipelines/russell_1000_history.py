from __future__ import annotations

from collections import defaultdict
import csv
from html import unescape
import io
import json
import re
import ssl
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

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
    end = pd.Timestamp(end_date or pd.Timestamp.utcnow()).tz_localize(None).normalize()
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
    to_year = to_year or pd.Timestamp.utcnow().year
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
