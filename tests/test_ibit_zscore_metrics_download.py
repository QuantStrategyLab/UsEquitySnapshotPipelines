from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import requests


SCRIPT_PATH = Path("scripts/download_ibit_zscore_metrics.py")


def _load_script_module():
    spec = importlib.util.spec_from_file_location("download_ibit_zscore_metrics", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_normalize_ibit_zscore_metrics_accepts_common_aliases() -> None:
    module = _load_script_module()

    normalized = module.normalize_ibit_zscore_metrics(
        pd.DataFrame(
            {
                "date": ["2026-01-02", "2026-01-01", "bad"],
                "mvrv_z": ["2.7", "2.5", "bad"],
            }
        )
    )

    assert normalized.to_dict("records") == [
        {"as_of": "2026-01-01", "mvrv_zscore": 2.5},
        {"as_of": "2026-01-02", "mvrv_zscore": 2.7},
    ]


def test_normalize_ibit_zscore_metrics_accepts_bgeometrics_aliases() -> None:
    module = _load_script_module()

    normalized = module.normalize_ibit_zscore_metrics(
        pd.DataFrame(
            {
                "d": ["2026-01-01", "2026-01-02"],
                "mvrvZscore": [1.1064, 1.1839],
            }
        )
    )

    assert normalized.to_dict("records") == [
        {"as_of": "2026-01-01", "mvrv_zscore": 1.1064},
        {"as_of": "2026-01-02", "mvrv_zscore": 1.1839},
    ]


def test_normalize_ibit_zscore_metrics_accepts_coinmetrics_alias() -> None:
    module = _load_script_module()

    normalized = module.normalize_ibit_zscore_metrics(
        pd.DataFrame(
            {
                "time": ["2026-01-01T00:00:00Z"],
                "CapMVRVZ": [1.1064],
            }
        )
    )

    assert normalized.to_dict("records") == [{"as_of": "2026-01-01", "mvrv_zscore": 1.1064}]


def test_frame_from_payload_accepts_nested_records_and_filters_dates() -> None:
    module = _load_script_module()

    frame = module.frame_from_payload(
        {
            "data": [
                {"timestamp": "2026-01-01", "z_score": 2.5},
                {"timestamp": "2026-01-02", "z_score": 2.7},
                {"timestamp": "2026-01-03", "z_score": 2.9},
            ]
        }
    )
    normalized = module.normalize_ibit_zscore_metrics(frame, start="2026-01-02", end="2026-01-02")

    assert normalized.to_dict("records") == [{"as_of": "2026-01-02", "mvrv_zscore": 2.7}]


def test_frame_from_payload_accepts_columns_and_rows_shape() -> None:
    module = _load_script_module()

    frame = module.frame_from_payload(
        {
            "columns": ["time", "mvrv_zscore"],
            "data": [["2026-01-01", 2.5], ["2026-01-02", 2.7]],
        }
    )
    normalized = module.normalize_ibit_zscore_metrics(frame)

    assert normalized.to_dict("records") == [
        {"as_of": "2026-01-01", "mvrv_zscore": 2.5},
        {"as_of": "2026-01-02", "mvrv_zscore": 2.7},
    ]


def test_frame_from_payload_accepts_newhedge_millisecond_timestamps() -> None:
    module = _load_script_module()

    normalized = module.normalize_ibit_zscore_metrics(module.frame_from_payload([[1767225600000, 1.1064]]))

    assert normalized.to_dict("records") == [{"as_of": "2026-01-01", "mvrv_zscore": 1.1064}]


def test_url_with_query_param_preserves_existing_query() -> None:
    module = _load_script_module()

    assert (
        module._url_with_query_param("https://newhedge.io/api/v2/metrics/mvrv_z?foo=1", name="api_token", value="abc")
        == "https://newhedge.io/api/v2/metrics/mvrv_z?foo=1&api_token=abc"
    )
    assert (
        module._url_with_query_param(
            "https://newhedge.io/api/v2/metrics/mvrv_z?api_token=existing", name="api_token", value="abc"
        )
        == "https://newhedge.io/api/v2/metrics/mvrv_z?api_token=existing"
    )


def test_url_without_sensitive_query_params_strips_tokens() -> None:
    module = _load_script_module()

    assert (
        module._url_without_sensitive_query_params(
            "https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01&token=secret&api_key=secret2"
        )
        == "https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01"
    )


def _json_response(payload: object, *, status_code: int, url: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = json.dumps(payload).encode("utf-8")
    response.headers["content-type"] = "application/json"
    return response


def test_validate_metrics_rejects_stale_sparse_or_extreme_data() -> None:
    module = _load_script_module()
    quality = module.MetricsQualityConfig(
        min_rows=3,
        max_age_days=2,
        max_gap_days=2,
        max_abs_zscore=10.0,
        max_daily_zscore_change=5.0,
    )

    valid = pd.DataFrame(
        {
            "as_of": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "mvrv_zscore": [1.0, 1.5, 2.0],
        }
    )
    module.validate_ibit_zscore_metrics(valid, quality=quality, today="2026-01-04")

    for frame in [
        valid.head(2),
        pd.DataFrame({"as_of": ["2025-12-01", "2025-12-02", "2025-12-03"], "mvrv_zscore": [1.0, 1.5, 2.0]}),
        pd.DataFrame({"as_of": ["2026-01-01", "2026-01-05", "2026-01-06"], "mvrv_zscore": [1.0, 1.5, 2.0]}),
        pd.DataFrame({"as_of": ["2026-01-01", "2026-01-02", "2026-01-03"], "mvrv_zscore": [1.0, 11.0, 2.0]}),
        pd.DataFrame({"as_of": ["2026-01-01", "2026-01-02", "2026-01-03"], "mvrv_zscore": [1.0, 7.0, 2.0]}),
    ]:
        try:
            module.validate_ibit_zscore_metrics(frame, quality=quality, today="2026-01-04")
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected quality validation failure for {frame!r}")


def test_download_metrics_falls_back_across_sources_without_leaking_newhedge_token(monkeypatch, tmp_path) -> None:
    module = _load_script_module()
    calls = []

    def fake_get(url, *, headers, timeout, proxies):
        calls.append({"url": url, "headers": headers, "timeout": timeout, "proxies": proxies})
        if len(calls) == 1:
            return _json_response({"error": "Token is required"}, status_code=401, url=url)
        return _json_response([{"d": "2026-01-01", "mvrvZscore": 1.1064}], status_code=200, url=url)

    monkeypatch.setattr(module.requests, "get", fake_get)
    output_path = tmp_path / "metrics.csv"

    result = module.download_ibit_zscore_metrics_from_sources(
        urls=[
            "https://newhedge.io/api/v2/metrics/mvrv-z-score/mvrv_z",
            "https://api.bitcoin-data.com/v1/mvrv-zscore",
        ],
        output_path=output_path,
        auth=module.MetricsAuth(newhedge_query_token="newhedge-token"),
        attempts=1,
    )

    assert result.output_path == output_path
    assert result.source_url == "https://api.bitcoin-data.com/v1/mvrv-zscore"
    assert result.row_count == 1
    assert calls[0]["url"].endswith("api_token=newhedge-token")
    assert "newhedge-token" not in calls[1]["url"]
    assert "Authorization" not in calls[1]["headers"]


def test_download_metrics_retries_rate_limited_source(monkeypatch, tmp_path) -> None:
    module = _load_script_module()
    calls = []
    sleeps = []

    def fake_get(url, *, headers, timeout, proxies):
        calls.append(url)
        if len(calls) == 1:
            response = _json_response({"error": "rate limited"}, status_code=429, url=url)
            response.headers["Retry-After"] = "0"
            return response
        return _json_response([["2026-01-01", 1.1064]], status_code=200, url=url)

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = module.download_ibit_zscore_metrics_from_sources(
        urls=["https://example.com/mvrv-zscore"],
        output_path=tmp_path / "metrics.csv",
        attempts=2,
    )

    assert result.row_count == 1
    assert len(calls) == 2
    assert sleeps == [0.0]


def test_public_proxy_fallback_skips_credentialed_requests(monkeypatch, tmp_path) -> None:
    module = _load_script_module()
    calls = []
    sleeps = []

    def fake_get(url, *, headers, timeout, proxies):
        calls.append({"url": url, "headers": headers, "timeout": timeout, "proxies": proxies})
        response = _json_response({"error": "rate limited"}, status_code=429, url=url)
        response.headers["Retry-After"] = "0"
        return response

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.time, "sleep", lambda seconds: sleeps.append(seconds))

    try:
        module.download_ibit_zscore_metrics_from_sources(
            urls=["https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01&token=embedded-token"],
            output_path=tmp_path / "metrics.csv",
            auth=module.MetricsAuth(
                bgeometrics_query_token="provider-token",
                bearer_token="bearer-token",
                api_key="api-key",
            ),
            public_proxy_urls=["http://public-proxy.example:8080"],
            allow_public_proxy=True,
            attempts=1,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("credentialed public proxy fallback should not succeed")

    assert len(calls) == 1
    assert calls[0]["url"].endswith("token=embedded-token")
    assert calls[0]["headers"]["Authorization"] == "Bearer bearer-token"
    assert calls[0]["headers"]["X-API-Key"] == "api-key"
    assert calls[0]["proxies"] is None
    assert sleeps == []


def test_public_proxy_fallback_allows_non_credentialed_requests(monkeypatch, tmp_path) -> None:
    module = _load_script_module()
    calls = []
    sleeps = []

    def fake_get(url, *, headers, timeout, proxies):
        calls.append({"url": url, "headers": headers, "timeout": timeout, "proxies": proxies})
        if len(calls) == 1:
            response = _json_response({"error": "rate limited"}, status_code=429, url=url)
            response.headers["Retry-After"] = "0"
            return response
        return _json_response([{"d": "2026-01-01", "mvrvZscore": 1.1064}], status_code=200, url=url)

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = module.download_ibit_zscore_metrics_from_sources(
        urls=["https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01"],
        output_path=tmp_path / "metrics.csv",
        public_proxy_urls=["http://public-proxy.example:8080"],
        allow_public_proxy=True,
        attempts=1,
    )

    assert result.row_count == 1
    assert calls[0]["url"] == "https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01"
    assert "Authorization" not in calls[0]["headers"]
    assert "X-API-Key" not in calls[0]["headers"]
    assert calls[0]["proxies"] is None
    assert calls[1]["url"] == "https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01"
    assert "Authorization" not in calls[1]["headers"]
    assert "X-API-Key" not in calls[1]["headers"]
    assert calls[1]["proxies"] == {
        "http": "http://public-proxy.example:8080",
        "https": "http://public-proxy.example:8080",
    }
    assert sleeps == []


def test_download_metrics_uses_fresh_last_good_fallback_after_source_failures(monkeypatch, tmp_path) -> None:
    module = _load_script_module()

    def fake_get(url, *, headers, timeout, proxies):
        return _json_response({"error": "rate limited"}, status_code=429, url=url)

    fallback_path = tmp_path / "last_good.csv"
    pd.DataFrame(
        {
            "as_of": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "mvrv_zscore": [1.0, 1.5, 2.0],
        }
    ).to_csv(fallback_path, index=False)
    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module, "_today_timestamp", lambda today=None: pd.Timestamp("2026-01-04"))

    result = module.download_ibit_zscore_metrics_from_sources(
        urls=["https://api.bitcoin-data.com/v1/mvrv-zscore"],
        output_path=tmp_path / "metrics.csv",
        fallback_csv_path=fallback_path,
        quality=module.MetricsQualityConfig(min_rows=3, max_fallback_age_days=5),
        attempts=1,
    )

    assert result.source_type == "fallback"
    assert result.source_url == str(fallback_path)
    assert result.latest_as_of == "2026-01-03"
    assert result.row_count == 3
    assert result.source_errors == ("api.bitcoin-data.com via direct: HTTP 429",)


def test_download_metrics_rejects_stale_last_good_fallback(monkeypatch, tmp_path) -> None:
    module = _load_script_module()

    def fake_get(url, *, headers, timeout, proxies):
        return _json_response({"error": "rate limited"}, status_code=429, url=url)

    fallback_path = tmp_path / "last_good.csv"
    pd.DataFrame(
        {
            "as_of": ["2025-12-01", "2025-12-02", "2025-12-03"],
            "mvrv_zscore": [1.0, 1.5, 2.0],
        }
    ).to_csv(fallback_path, index=False)
    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module, "_today_timestamp", lambda today=None: pd.Timestamp("2026-01-04"))

    try:
        module.download_ibit_zscore_metrics_from_sources(
            urls=["https://api.bitcoin-data.com/v1/mvrv-zscore"],
            output_path=tmp_path / "metrics.csv",
            fallback_csv_path=fallback_path,
            quality=module.MetricsQualityConfig(min_rows=3, max_fallback_age_days=5),
            attempts=1,
        )
    except RuntimeError as exc:
        assert "fallback <local>: ValueError" in str(exc)
    else:
        raise AssertionError("stale fallback should fail")


def test_main_writes_download_metadata(monkeypatch, tmp_path) -> None:
    module = _load_script_module()

    def fake_get(url, *, headers, timeout, proxies):
        return _json_response([{"d": "2026-01-01", "mvrvZscore": 1.1064}], status_code=200, url=url)

    monkeypatch.setattr(module.requests, "get", fake_get)
    output_path = tmp_path / "metrics.csv"
    metadata_path = tmp_path / "metadata.json"

    assert (
        module.main(
            [
                "--url",
                "https://api.bitcoin-data.com/v1/mvrv-zscore",
                "--output",
                str(output_path),
                "--metadata-output",
                str(metadata_path),
            ]
        )
        == 0
    )

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["source_type"] == "source"
    assert metadata["row_count"] == 1
    assert metadata["latest_as_of"] == "2026-01-01"
