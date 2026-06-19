from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from io import StringIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests


DATE_ALIASES = ("as_of", "date", "timestamp", "time")
ZSCORE_ALIASES = ("mvrv_zscore", "mvrv_z_score", "mvrv_z", "zscore", "z_score")
RECORD_CONTAINER_KEYS = ("data", "records", "result", "results", "values", "rows")


def _normalized_key_map(frame: pd.DataFrame) -> dict[str, str]:
    return {str(column).strip().lower(): str(column) for column in frame.columns}


def _find_column(frame: pd.DataFrame, aliases: Sequence[str], *, label: str) -> str:
    normalized = _normalized_key_map(frame)
    for alias in aliases:
        column = normalized.get(alias)
        if column:
            return column
    raise ValueError(f"IBIT zscore metrics require a {label} column")


def normalize_ibit_zscore_metrics(
    frame: pd.DataFrame, *, start: object | None = None, end: object | None = None
) -> pd.DataFrame:
    date_column = _find_column(frame, DATE_ALIASES, label="date/as_of")
    zscore_column = _find_column(frame, ZSCORE_ALIASES, label="mvrv_zscore")
    normalized = frame[[date_column, zscore_column]].copy()
    normalized.columns = ["as_of", "mvrv_zscore"]
    normalized["as_of"] = pd.to_datetime(normalized["as_of"], errors="coerce")
    normalized["mvrv_zscore"] = pd.to_numeric(normalized["mvrv_zscore"], errors="coerce")
    normalized = normalized.dropna(subset=["as_of", "mvrv_zscore"]).sort_values("as_of")
    if start:
        normalized = normalized[normalized["as_of"] >= pd.Timestamp(start)]
    if end:
        normalized = normalized[normalized["as_of"] <= pd.Timestamp(end)]
    if normalized.empty:
        raise ValueError("IBIT zscore metrics produced no valid rows")
    normalized["as_of"] = normalized["as_of"].dt.date.astype(str)
    return normalized.reset_index(drop=True)


def _records_from_tabular_payload(payload: object) -> list[object]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, Mapping):
        raise ValueError("unsupported IBIT zscore JSON payload")

    columns = payload.get("columns")
    data = payload.get("data")
    if isinstance(columns, Sequence) and not isinstance(columns, (str, bytes, bytearray)) and isinstance(data, list):
        return [dict(zip(columns, row, strict=False)) if isinstance(row, Sequence) else row for row in data]

    for key in RECORD_CONTAINER_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, Mapping):
            try:
                return _records_from_tabular_payload(value)
            except ValueError:
                continue

    if any(str(key).strip().lower() in {*DATE_ALIASES, *ZSCORE_ALIASES} for key in payload):
        return [payload]

    array_values = {key: value for key, value in payload.items() if isinstance(value, list)}
    if array_values:
        row_count = min(len(value) for value in array_values.values())
        return [{key: value[idx] for key, value in array_values.items()} for idx in range(row_count)]

    raise ValueError("unsupported IBIT zscore JSON payload shape")


def frame_from_payload(payload: object) -> pd.DataFrame:
    records = _records_from_tabular_payload(payload)
    if records and isinstance(records[0], Sequence) and not isinstance(records[0], (Mapping, str, bytes, bytearray)):
        return pd.DataFrame({"as_of": [row[0] for row in records], "mvrv_zscore": [row[1] for row in records]})
    return pd.DataFrame(records)


def _frame_from_response(response: requests.Response) -> pd.DataFrame:
    content_type = response.headers.get("content-type", "").lower()
    text = response.text
    if "json" in content_type or text.lstrip().startswith(("{", "[")):
        return frame_from_payload(json.loads(text))
    return pd.read_csv(StringIO(text))


def download_ibit_zscore_metrics(
    *,
    url: str,
    output_path: Path,
    start: object | None = None,
    end: object | None = None,
    bearer_token: str | None = None,
    api_key: str | None = None,
) -> Path:
    headers = {"Accept": "application/json,text/csv;q=0.9,*/*;q=0.1"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    if api_key:
        headers["X-API-Key"] = api_key
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    metrics = normalize_ibit_zscore_metrics(_frame_from_response(response), start=start, end=end)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(output_path, index=False)
    return output_path


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value and value.strip() else None


def _first_env_value(names: Iterable[str]) -> str | None:
    for name in names:
        value = _env_value(name)
        if value:
            return value
    return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download and normalize IBIT MVRV Z-Score metrics.")
    parser.add_argument("--url", required=True, help="HTTP(S) endpoint returning CSV or JSON zscore history.")
    parser.add_argument("--output", required=True, help="Output CSV path with as_of,mvrv_zscore columns.")
    parser.add_argument("--start", default=None, help="Optional inclusive start date.")
    parser.add_argument("--end", default=None, help="Optional inclusive end date.")
    parser.add_argument(
        "--bearer-token-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_BEARER_TOKEN", "NEW_HEDGE_API_TOKEN"],
        help="Environment variable containing a bearer token. Can be passed multiple times.",
    )
    parser.add_argument(
        "--api-key-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_API_KEY"],
        help="Environment variable containing an API key for the X-API-Key header. Can be passed multiple times.",
    )
    args = parser.parse_args(argv)

    output_path = Path(args.output)
    bearer_token = _first_env_value(args.bearer_token_env)
    api_key = _first_env_value(args.api_key_env)
    result_path = download_ibit_zscore_metrics(
        url=args.url,
        output_path=output_path,
        start=args.start,
        end=args.end,
        bearer_token=bearer_token,
        api_key=api_key,
    )
    host = urlparse(args.url).netloc or "<local>"
    row_count = len(pd.read_csv(result_path))
    print(f"downloaded {row_count} IBIT zscore metric rows from {host} -> {result_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
