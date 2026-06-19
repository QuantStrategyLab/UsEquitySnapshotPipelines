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


def test_public_proxy_fallback_strips_credentials(monkeypatch, tmp_path) -> None:
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

    assert result.row_count == 1
    assert calls[0]["url"].endswith("token=embedded-token")
    assert calls[0]["headers"]["Authorization"] == "Bearer bearer-token"
    assert calls[0]["headers"]["X-API-Key"] == "api-key"
    assert calls[0]["proxies"] is None
    assert calls[1]["url"] == "https://api.bitcoin-data.com/v1/mvrv-zscore?startday=2026-01-01"
    assert "Authorization" not in calls[1]["headers"]
    assert "X-API-Key" not in calls[1]["headers"]
    assert calls[1]["proxies"] == {
        "http": "http://public-proxy.example:8080",
        "https": "http://public-proxy.example:8080",
    }
    assert sleeps == []
