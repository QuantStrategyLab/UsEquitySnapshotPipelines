from __future__ import annotations

from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.input_sources import (
    DEFAULT_REMOTE_COPY_TIMEOUT_SECONDS,
    _default_http_copy,
    resolve_input_source,
    resolve_input_sources,
    source_needs_gcloud,
)


def test_resolves_local_input_without_copying(tmp_path) -> None:
    prices = tmp_path / "prices.csv"
    prices.write_text("symbol,as_of,close,volume\nQQQ,2026-04-01,100,1\n", encoding="utf-8")

    resolved = resolve_input_source(prices, output_dir=tmp_path / "resolved", stem="prices")

    assert resolved == prices


def test_rejects_unsupported_local_suffix(tmp_path) -> None:
    prices = tmp_path / "prices.txt"
    prices.write_text("not a table", encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported prices file suffix"):
        resolve_input_source(prices, output_dir=tmp_path / "resolved", stem="prices")


def test_resolves_gcs_inputs_with_supplied_copy_function(tmp_path) -> None:
    calls: list[tuple[str, Path]] = []

    def fake_copy(source: str, target: Path) -> None:
        calls.append((source, target))
        target.write_text("symbol,as_of,close,volume\nQQQ,2026-04-01,100,1\n", encoding="utf-8")

    resolved = resolve_input_sources(
        prices_source="gs://bucket/raw/prices.csv",
        universe_source="gs://bucket/raw/universe.csv",
        config_source="gs://bucket/raw/config.json",
        product_map_source="gs://bucket/raw/product_map.csv",
        source_input_manifest_source="gs://bucket/raw/r1000_source_input_manifest.json",
        output_dir=tmp_path / "resolved",
        gcs_copy=fake_copy,
    )

    assert resolved.prices_path == tmp_path / "resolved" / "prices.csv"
    assert resolved.universe_path == tmp_path / "resolved" / "universe.csv"
    assert resolved.config_path == tmp_path / "resolved" / "config.json"
    assert resolved.product_map_path == tmp_path / "resolved" / "product_map.csv"
    assert resolved.source_input_manifest_path == tmp_path / "resolved" / "source_input_manifest.json"
    assert [source for source, _target in calls] == [
        "gs://bucket/raw/prices.csv",
        "gs://bucket/raw/universe.csv",
        "gs://bucket/raw/config.json",
        "gs://bucket/raw/product_map.csv",
        "gs://bucket/raw/r1000_source_input_manifest.json",
    ]


def test_source_needs_gcloud_only_for_gcs() -> None:
    assert source_needs_gcloud("gs://bucket/path.csv") is True
    assert source_needs_gcloud("https://example.com/path.csv") is False
    assert source_needs_gcloud("data/prices.csv") is False


def test_resolve_input_source_rejects_secret_like_remote_uri(tmp_path) -> None:
    with pytest.raises(ValueError, match="must not contain token"):
        resolve_input_source(
            "https://example.com/prices.csv?token=abc",
            output_dir=tmp_path / "resolved",
            stem="prices",
        )


def test_default_http_copy_uses_timeout_and_streams_response(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, float]] = []

    class FakeResponse:
        def __init__(self) -> None:
            self._payload = b"symbol,close\nQQQ,100\n"

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            payload, self._payload = self._payload, b""
            return payload

    def fake_urlopen(request, *, timeout: float):
        calls.append((request.full_url, timeout))
        return FakeResponse()

    monkeypatch.setattr("us_equity_snapshot_pipelines.input_sources.urlopen", fake_urlopen)
    target = tmp_path / "prices.csv"

    _default_http_copy("https://example.com/prices.csv", target)

    assert target.read_text(encoding="utf-8") == "symbol,close\nQQQ,100\n"
    assert calls == [("https://example.com/prices.csv", DEFAULT_REMOTE_COPY_TIMEOUT_SECONDS)]
