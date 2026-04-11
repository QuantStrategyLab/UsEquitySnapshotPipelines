from __future__ import annotations

from collections import defaultdict
import csv
import io
import json
import re
import ssl
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

SNAPSHOT_FILENAME_DATE_RE = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})")
UNIVERSE_HISTORY_COLUMNS = ("symbol", "sector", "start_date", "end_date")
ISHARES_IWB_PRODUCT_URL = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf"
ISHARES_IWB_HOLDINGS_CSV_URL = (
    f"{ISHARES_IWB_PRODUCT_URL}/1467271812596.ajax"
    "?fileType=csv&fileName=IWB_holdings&dataType=fund"
)
ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE = (
    f"{ISHARES_IWB_PRODUCT_URL}/1467271812596.ajax"
    "?fileType=json&tab=all&asOfDate={as_of_date}"
)
ISHARES_SNAPSHOT_IDENTIFIER_COLUMNS = ("isin", "cusip", "sedol")
ISHARES_SNAPSHOT_OPTIONAL_COLUMN_SOURCES = (
    ("ISIN", "isin"),
    ("CUSIP", "cusip"),
    ("SEDOL", "sedol"),
    ("Exchange", "exchange"),
    ("Location", "country"),
    ("Country", "country"),
    ("Currency", "currency"),
    ("Market Currency", "market_currency"),
)
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
        (pd.Timestamp(snapshot_date).normalize(), _normalize_snapshot_frame(frame, snapshot_date=pd.Timestamp(snapshot_date).normalize()))
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


def _fetch_text(url: str, *, timeout: int = 60, user_agent: str = DEFAULT_HTTP_USER_AGENT) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "*/*",
        },
    )
    with urlopen(request, timeout=timeout, context=_build_ssl_context()) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(encoding, errors="replace")


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

    payload = json.loads(str(json_text).lstrip("\ufeff"))
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
                "CUSIP": row[8] if len(row) > 8 else "",
                "ISIN": row[9] if len(row) > 9 else "",
                "SEDOL": row[10] if len(row) > 10 else "",
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
                "CUSIP",
                "ISIN",
                "SEDOL",
                "Location",
                "Exchange",
                "Currency",
                "Market Currency",
            ]
        )
    return pd.Timestamp(as_of_date).normalize(), _finalize_ishares_holdings_snapshot_frame(frame)


def build_ishares_holdings_json_url(
    as_of_date,
    *,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
) -> str:
    normalized = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    return str(holdings_url_template).format(as_of_date=f"{normalized:%Y%m%d}")


def download_ishares_holdings_snapshot_for_date(
    as_of_date,
    *,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    snapshot_date = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    source_url = build_ishares_holdings_json_url(snapshot_date, holdings_url_template=holdings_url_template)
    return parse_ishares_holdings_json_snapshot(_fetch_text(source_url), as_of_date=snapshot_date)


def build_monthly_snapshot_request_dates(start_date, end_date=None) -> list[pd.Timestamp]:
    start = pd.Timestamp(start_date).tz_localize(None).normalize()
    end = pd.Timestamp(end_date or pd.Timestamp.utcnow()).tz_localize(None).normalize()
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    request_dates = [pd.Timestamp(timestamp).normalize() for timestamp in pd.date_range(start=start, end=end, freq="ME")]
    if not request_dates or request_dates[-1] != end:
        request_dates.append(end)
    return sorted(dict.fromkeys(request_dates))


def resolve_ishares_holdings_snapshot(
    requested_date,
    *,
    max_lookback_days: int = 7,
    holdings_url_template: str = ISHARES_IWB_HOLDINGS_JSON_URL_TEMPLATE,
    download_fn=download_ishares_holdings_snapshot_for_date,
) -> dict[str, object]:
    requested = pd.Timestamp(requested_date).tz_localize(None).normalize()
    for lookback_days in range(max(int(max_lookback_days), 0) + 1):
        candidate_date = requested - pd.Timedelta(days=lookback_days)
        as_of_date, snapshot = download_fn(candidate_date, holdings_url_template=holdings_url_template)
        if not snapshot.empty:
            return {
                "requested_date": requested,
                "as_of_date": pd.Timestamp(as_of_date).normalize(),
                "lookback_days": lookback_days,
                "source_url": build_ishares_holdings_json_url(
                    as_of_date,
                    holdings_url_template=holdings_url_template,
                ),
                "snapshot": snapshot,
            }
    raise RuntimeError(
        "Could not resolve a non-empty iShares holdings snapshot "
        f"within {max_lookback_days} day(s) before {requested:%Y-%m-%d}"
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
        record = resolve_ishares_holdings_snapshot(
            requested_date,
            max_lookback_days=max_lookback_days,
            holdings_url_template=holdings_url_template,
        )
        record["source_kind"] = "official_json"
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
    symbols = (
        frame["symbol"].astype(str).str.upper().str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
    )
    for extra in (benchmark_symbol, safe_haven):
        symbol = str(extra or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def write_interval_universe_history(history: pd.DataFrame, output_path: str | Path) -> None:
    write_table(history, output_path)
