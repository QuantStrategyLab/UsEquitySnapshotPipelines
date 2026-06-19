from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests


DATE_ALIASES = ("as_of", "date", "timestamp", "time", "day", "d")
ZSCORE_ALIASES = ("mvrv_zscore", "mvrv_z_score", "mvrv_z", "mvrvzscore", "capmvrvz", "zscore", "z_score")
RECORD_CONTAINER_KEYS = ("data", "records", "result", "results", "values", "rows")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SENSITIVE_QUERY_PARAMS = frozenset({"access_token", "api_key", "api_token", "apikey", "key", "token"})


@dataclass(frozen=True)
class MetricsAuth:
    newhedge_query_token: str | None = None
    bgeometrics_query_token: str | None = None
    generic_query_token: str | None = None
    generic_query_token_param: str = "api_token"
    bearer_token: str | None = None
    api_key: str | None = None


@dataclass(frozen=True)
class MetricsDownloadResult:
    output_path: Path
    source_url: str
    row_count: int
    source_type: str = "source"
    latest_as_of: str | None = None
    source_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class MetricsQualityConfig:
    min_rows: int = 1
    max_age_days: int | None = None
    max_fallback_age_days: int | None = None
    max_gap_days: int | None = None
    max_abs_zscore: float | None = None
    max_daily_zscore_change: float | None = None


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
    normalized["as_of"] = _parse_dates(normalized["as_of"])
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


def validate_ibit_zscore_metrics(
    metrics: pd.DataFrame,
    *,
    quality: MetricsQualityConfig | None = None,
    today: object | None = None,
    label: str = "IBIT zscore metrics",
) -> None:
    quality = quality or MetricsQualityConfig()
    if len(metrics) < quality.min_rows:
        raise ValueError(f"{label} require at least {quality.min_rows} rows, got {len(metrics)}")

    as_of = pd.to_datetime(metrics["as_of"], errors="coerce")
    if as_of.isna().any():
        raise ValueError(f"{label} contain invalid as_of values")
    zscore = pd.to_numeric(metrics["mvrv_zscore"], errors="coerce")
    if zscore.isna().any():
        raise ValueError(f"{label} contain invalid mvrv_zscore values")

    ordered = pd.DataFrame({"as_of": as_of, "mvrv_zscore": zscore}).sort_values("as_of")
    if quality.max_gap_days is not None and len(as_of) > 1:
        max_gap = int(ordered["as_of"].diff().dt.days.max())
        if max_gap > quality.max_gap_days:
            raise ValueError(f"{label} max date gap {max_gap}d exceeds {quality.max_gap_days}d")

    latest = ordered["as_of"].max().normalize()
    if quality.max_age_days is not None:
        current_day = _today_timestamp(today)
        age_days = int((current_day - latest).days)
        if age_days > quality.max_age_days:
            raise ValueError(f"{label} latest row is {age_days}d old, exceeds {quality.max_age_days}d")

    if quality.max_abs_zscore is not None and float(ordered["mvrv_zscore"].abs().max()) > quality.max_abs_zscore:
        raise ValueError(f"{label} zscore magnitude exceeds {quality.max_abs_zscore}")
    if quality.max_daily_zscore_change is not None and len(ordered) > 1:
        max_change = float(ordered["mvrv_zscore"].diff().abs().max())
        if max_change > quality.max_daily_zscore_change:
            raise ValueError(f"{label} daily zscore change {max_change:.4f} exceeds {quality.max_daily_zscore_change}")


def _today_timestamp(today: object | None = None) -> pd.Timestamp:
    if today is not None:
        return pd.Timestamp(today).normalize()
    return pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()


def _parse_dates(values: pd.Series) -> pd.Series:
    numeric_values = pd.to_numeric(values, errors="coerce")
    if numeric_values.notna().all() and not numeric_values.empty:
        median_abs = numeric_values.abs().median()
        if median_abs >= 100_000_000_000:
            return pd.to_datetime(numeric_values, unit="ms", errors="coerce")
        if median_abs >= 1_000_000_000:
            return pd.to_datetime(numeric_values, unit="s", errors="coerce")
    return pd.to_datetime(values, errors="coerce")


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


def _latest_as_of(metrics: pd.DataFrame) -> str:
    return str(pd.to_datetime(metrics["as_of"], errors="coerce").max().date())


def _fallback_quality_config(quality: MetricsQualityConfig) -> MetricsQualityConfig:
    max_age_days = quality.max_fallback_age_days
    if max_age_days is None:
        max_age_days = quality.max_age_days
    return MetricsQualityConfig(
        min_rows=quality.min_rows,
        max_age_days=max_age_days,
        max_fallback_age_days=quality.max_fallback_age_days,
        max_gap_days=quality.max_gap_days,
        max_abs_zscore=quality.max_abs_zscore,
        max_daily_zscore_change=quality.max_daily_zscore_change,
    )


def _split_url_values(values: Iterable[str | None]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        for raw_url in value.replace(",", "\n").splitlines():
            url = raw_url.strip()
            if not url or url in seen:
                continue
            urls.append(url)
            seen.add(url)
    return urls


def _source_host(url: str) -> str:
    return urlparse(url).netloc or "<local>"


def _is_newhedge_url(url: str) -> bool:
    return _source_host(url).lower().endswith("newhedge.io")


def _is_bgeometrics_url(url: str) -> bool:
    host = _source_host(url).lower()
    return host.endswith("bitcoin-data.com") or host.endswith("bgeometrics.com")


def _url_with_query_param(url: str, *, name: str, value: str | None) -> str:
    if not value:
        return url
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if any(key == name for key, _ in query):
        return url
    query.append((name, value))
    return urlunparse(parsed._replace(query=urlencode(query)))


def _url_without_sensitive_query_params(url: str) -> str:
    parsed = urlparse(url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.strip().lower() not in SENSITIVE_QUERY_PARAMS
    ]
    return urlunparse(parsed._replace(query=urlencode(query)))


def _public_proxy_auth(auth: MetricsAuth) -> MetricsAuth:
    return MetricsAuth(generic_query_token_param=auth.generic_query_token_param)


def _query_contains_sensitive_params(url: str) -> bool:
    return any(
        key.strip().lower() in SENSITIVE_QUERY_PARAMS
        for key, _ in parse_qsl(urlparse(url).query, keep_blank_values=True)
    )


def _request_url_and_headers(url: str, auth: MetricsAuth) -> tuple[str, dict[str, str]]:
    headers = {"Accept": "application/json,text/csv;q=0.9,*/*;q=0.1"}
    request_url = url
    if _is_newhedge_url(url):
        request_url = _url_with_query_param(request_url, name="api_token", value=auth.newhedge_query_token)
    elif _is_bgeometrics_url(url):
        request_url = _url_with_query_param(request_url, name="token", value=auth.bgeometrics_query_token)
    else:
        request_url = _url_with_query_param(
            request_url, name=auth.generic_query_token_param, value=auth.generic_query_token
        )
    if auth.bearer_token:
        headers["Authorization"] = f"Bearer {auth.bearer_token}"
    if auth.api_key:
        headers["X-API-Key"] = auth.api_key
    return request_url, headers


def _request_has_credentials(url: str, auth: MetricsAuth) -> bool:
    request_url, headers = _request_url_and_headers(url, auth)
    if _query_contains_sensitive_params(request_url):
        return True
    return "Authorization" in headers or "X-API-Key" in headers


def _proxies_from_url(proxy_url: str | None) -> dict[str, str] | None:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _truthy_env_value(value: str | None) -> bool:
    return bool(value and value.strip().lower() in {"1", "true", "yes", "on"})


def _retry_wait_seconds(response: requests.Response | None, *, retry_wait_seconds: float, attempt_index: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
    return max(retry_wait_seconds, 0.0) * (2 ** max(attempt_index - 1, 0))


def _safe_error(exc: BaseException) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return f"HTTP {exc.response.status_code}"
    return exc.__class__.__name__


def download_ibit_zscore_metrics_from_sources(
    *,
    urls: Sequence[str],
    output_path: Path,
    start: object | None = None,
    end: object | None = None,
    auth: MetricsAuth | None = None,
    proxy_url: str | None = None,
    public_proxy_urls: Sequence[str] | None = None,
    allow_public_proxy: bool = False,
    fallback_csv_path: Path | None = None,
    quality: MetricsQualityConfig | None = None,
    attempts: int = 3,
    retry_wait_seconds: float = 2.0,
) -> MetricsDownloadResult:
    if not urls:
        raise ValueError("at least one IBIT zscore metrics URL is required")

    attempts = max(attempts, 1)
    auth = auth or MetricsAuth()
    quality = quality or MetricsQualityConfig()
    public_proxy_urls = public_proxy_urls or []
    errors: list[str] = []

    for url in urls:
        host = _source_host(url)
        routes: list[tuple[str, str, MetricsAuth, str | None]] = [("direct", url, auth, proxy_url)]
        if allow_public_proxy and not _request_has_credentials(url, auth):
            public_url = _url_without_sensitive_query_params(url)
            public_auth = _public_proxy_auth(auth)
            routes.extend(
                (f"public-proxy-{idx}", public_url, public_auth, public_proxy_url)
                for idx, public_proxy_url in enumerate(public_proxy_urls, start=1)
            )

        for route_label, route_url, route_auth, route_proxy_url in routes:
            proxies = _proxies_from_url(route_proxy_url)
            for attempt in range(1, attempts + 1):
                response: requests.Response | None = None
                try:
                    request_url, headers = _request_url_and_headers(route_url, route_auth)
                    response = requests.get(request_url, headers=headers, timeout=60, proxies=proxies)
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        if attempt < attempts:
                            time.sleep(
                                _retry_wait_seconds(
                                    response, retry_wait_seconds=retry_wait_seconds, attempt_index=attempt
                                )
                            )
                            continue
                    response.raise_for_status()
                    metrics = normalize_ibit_zscore_metrics(_frame_from_response(response), start=start, end=end)
                    validate_ibit_zscore_metrics(metrics, quality=quality)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    metrics.to_csv(output_path, index=False)
                    return MetricsDownloadResult(
                        output_path=output_path,
                        source_url=url,
                        row_count=len(metrics),
                        source_type="source",
                        latest_as_of=_latest_as_of(metrics),
                        source_errors=tuple(errors),
                    )
                except requests.RequestException as exc:
                    should_retry = response is None or response.status_code in RETRYABLE_STATUS_CODES
                    if should_retry and attempt < attempts:
                        time.sleep(
                            _retry_wait_seconds(response, retry_wait_seconds=retry_wait_seconds, attempt_index=attempt)
                        )
                        continue
                    errors.append(f"{host} via {route_label}: {_safe_error(exc)}")
                    break
                except (ValueError, json.JSONDecodeError, pd.errors.ParserError) as exc:
                    errors.append(f"{host} via {route_label}: {exc.__class__.__name__}")
                    break

    if fallback_csv_path:
        try:
            metrics = normalize_ibit_zscore_metrics(pd.read_csv(fallback_csv_path), start=start, end=end)
            validate_ibit_zscore_metrics(
                metrics, quality=_fallback_quality_config(quality), label="fallback IBIT zscore metrics"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            metrics.to_csv(output_path, index=False)
            return MetricsDownloadResult(
                output_path=output_path,
                source_url=str(fallback_csv_path),
                row_count=len(metrics),
                source_type="fallback",
                latest_as_of=_latest_as_of(metrics),
                source_errors=tuple(errors),
            )
        except (OSError, ValueError, pd.errors.ParserError) as exc:
            errors.append(f"fallback {_source_host(str(fallback_csv_path))}: {exc.__class__.__name__}")

    raise RuntimeError("failed to download IBIT zscore metrics from configured sources: " + "; ".join(errors))


def download_ibit_zscore_metrics(
    *,
    url: str,
    output_path: Path,
    start: object | None = None,
    end: object | None = None,
    bearer_token: str | None = None,
    api_key: str | None = None,
) -> Path:
    result = download_ibit_zscore_metrics_from_sources(
        urls=[url],
        output_path=output_path,
        start=start,
        end=end,
        auth=MetricsAuth(bearer_token=bearer_token, api_key=api_key),
    )
    return result.output_path


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value and value.strip() else None


def _first_env_value(names: Iterable[str]) -> str | None:
    for name in names:
        value = _env_value(name)
        if value:
            return value
    return None


def _env_values(names: Iterable[str]) -> list[str]:
    return [value for name in names if (value := _env_value(name))]


def _env_flag(names: Iterable[str]) -> bool:
    return _truthy_env_value(_first_env_value(names))


def _env_int(name: str, default: int | None = None) -> int | None:
    value = _env_value(name)
    return int(value) if value is not None else default


def _env_float(name: str, default: float | None = None) -> float | None:
    value = _env_value(name)
    return float(value) if value is not None else default


def _result_metadata(result: MetricsDownloadResult) -> dict[str, object]:
    return {
        "source_type": result.source_type,
        "source": result.source_url,
        "row_count": result.row_count,
        "latest_as_of": result.latest_as_of,
        "output_path": str(result.output_path),
        "source_errors": list(result.source_errors),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download and normalize IBIT MVRV Z-Score metrics.")
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="HTTP(S) endpoint returning CSV or JSON zscore history. Can be passed multiple times or comma-separated.",
    )
    parser.add_argument(
        "--urls-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_URLS", "IBIT_ZSCORE_METRICS_URL"],
        help="Environment variable containing one or more metrics URLs. Can be passed multiple times.",
    )
    parser.add_argument("--output", required=True, help="Output CSV path with as_of,mvrv_zscore columns.")
    parser.add_argument("--metadata-output", default=None, help="Optional JSON metadata output path.")
    parser.add_argument(
        "--fallback-csv",
        default=_first_env_value(["IBIT_ZSCORE_METRICS_FALLBACK_CSV"]),
        help="Optional last-good fallback CSV path used only after all sources fail.",
    )
    parser.add_argument("--start", default=None, help="Optional inclusive start date.")
    parser.add_argument("--end", default=None, help="Optional inclusive end date.")
    parser.add_argument("--min-rows", type=int, default=None, help="Minimum valid normalized rows.")
    parser.add_argument("--max-age-days", type=int, default=None, help="Maximum age of the latest source row.")
    parser.add_argument(
        "--max-fallback-age-days",
        type=int,
        default=None,
        help="Maximum age of the latest fallback row. Defaults to --max-age-days when unset.",
    )
    parser.add_argument("--max-gap-days", type=int, default=None, help="Maximum gap between consecutive rows.")
    parser.add_argument("--max-abs-zscore", type=float, default=None, help="Maximum absolute zscore value.")
    parser.add_argument(
        "--max-daily-zscore-change",
        type=float,
        default=None,
        help="Maximum absolute change between consecutive zscore rows.",
    )
    parser.add_argument("--attempts", type=int, default=3, help="Attempts per source for transient HTTP errors.")
    parser.add_argument(
        "--retry-wait-seconds",
        type=float,
        default=2.0,
        help="Initial wait before retrying a transient source failure.",
    )
    parser.add_argument(
        "--newhedge-token-env",
        action="append",
        default=["NEW_HEDGE_API_TOKEN", "IBIT_ZSCORE_METRICS_NEW_HEDGE_TOKEN"],
        help="Environment variable containing a Newhedge query token. Can be passed multiple times.",
    )
    parser.add_argument(
        "--bgeometrics-token-env",
        action="append",
        default=["BGEOMETRICS_API_TOKEN", "IBIT_ZSCORE_METRICS_BGEOMETRICS_TOKEN"],
        help="Environment variable containing a BGeometrics query token. Can be passed multiple times.",
    )
    parser.add_argument(
        "--query-token-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_QUERY_TOKEN"],
        help="Environment variable containing a generic query token for non-provider-specific URLs.",
    )
    parser.add_argument(
        "--query-token-param",
        default="api_token",
        help="Query parameter name for the generic query token.",
    )
    parser.add_argument(
        "--bearer-token-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_BEARER_TOKEN"],
        help="Environment variable containing a bearer token. Can be passed multiple times.",
    )
    parser.add_argument(
        "--api-key-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_API_KEY"],
        help="Environment variable containing an API key for the X-API-Key header. Can be passed multiple times.",
    )
    parser.add_argument(
        "--proxy-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_PROXY"],
        help="Environment variable containing a trusted HTTP(S) proxy URL. Can be passed multiple times.",
    )
    parser.add_argument(
        "--public-proxies-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_PUBLIC_PROXIES"],
        help="Environment variable containing comma/newline-separated public proxy URLs.",
    )
    parser.add_argument(
        "--allow-public-proxy-env",
        action="append",
        default=["IBIT_ZSCORE_METRICS_ALLOW_PUBLIC_PROXY"],
        help="Environment variable enabling public proxy fallback when set to true/1/yes/on.",
    )
    args = parser.parse_args(argv)

    urls = _split_url_values(args.url) or _split_url_values(_env_values(args.urls_env))
    if not urls:
        parser.error("at least one --url or configured URLs env value is required")

    output_path = Path(args.output)
    auth = MetricsAuth(
        newhedge_query_token=_first_env_value(args.newhedge_token_env),
        bgeometrics_query_token=_first_env_value(args.bgeometrics_token_env),
        generic_query_token=_first_env_value(args.query_token_env),
        generic_query_token_param=args.query_token_param,
        bearer_token=_first_env_value(args.bearer_token_env),
        api_key=_first_env_value(args.api_key_env),
    )
    quality = MetricsQualityConfig(
        min_rows=args.min_rows if args.min_rows is not None else (_env_int("IBIT_ZSCORE_METRICS_MIN_ROWS", 1) or 1),
        max_age_days=args.max_age_days
        if args.max_age_days is not None
        else _env_int("IBIT_ZSCORE_METRICS_MAX_AGE_DAYS"),
        max_fallback_age_days=(
            args.max_fallback_age_days
            if args.max_fallback_age_days is not None
            else _env_int("IBIT_ZSCORE_METRICS_MAX_FALLBACK_AGE_DAYS")
        ),
        max_gap_days=args.max_gap_days
        if args.max_gap_days is not None
        else _env_int("IBIT_ZSCORE_METRICS_MAX_GAP_DAYS"),
        max_abs_zscore=(
            args.max_abs_zscore if args.max_abs_zscore is not None else _env_float("IBIT_ZSCORE_METRICS_MAX_ABS_ZSCORE")
        ),
        max_daily_zscore_change=(
            args.max_daily_zscore_change
            if args.max_daily_zscore_change is not None
            else _env_float("IBIT_ZSCORE_METRICS_MAX_DAILY_ZSCORE_CHANGE")
        ),
    )
    result = download_ibit_zscore_metrics_from_sources(
        urls=urls,
        output_path=output_path,
        start=args.start,
        end=args.end,
        auth=auth,
        proxy_url=_first_env_value(args.proxy_env),
        public_proxy_urls=_split_url_values(_env_values(args.public_proxies_env)),
        allow_public_proxy=_env_flag(args.allow_public_proxy_env),
        fallback_csv_path=Path(args.fallback_csv) if args.fallback_csv else None,
        quality=quality,
        attempts=args.attempts,
        retry_wait_seconds=args.retry_wait_seconds,
    )
    if args.metadata_output:
        metadata_path = Path(args.metadata_output)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(_result_metadata(result), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(
        f"downloaded {result.row_count} IBIT zscore metric rows from {result.source_type} "
        f"{_source_host(result.source_url)} latest_as_of={result.latest_as_of} -> {result.output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
