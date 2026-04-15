from __future__ import annotations

from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.input_sources import resolve_input_source, resolve_input_sources, source_needs_gcloud


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
        output_dir=tmp_path / "resolved",
        gcs_copy=fake_copy,
    )

    assert resolved.prices_path == tmp_path / "resolved" / "prices.csv"
    assert resolved.universe_path == tmp_path / "resolved" / "universe.csv"
    assert resolved.config_path == tmp_path / "resolved" / "config.json"
    assert resolved.product_map_path == tmp_path / "resolved" / "product_map.csv"
    assert [source for source, _target in calls] == [
        "gs://bucket/raw/prices.csv",
        "gs://bucket/raw/universe.csv",
        "gs://bucket/raw/config.json",
        "gs://bucket/raw/product_map.csv",
    ]


def test_source_needs_gcloud_only_for_gcs() -> None:
    assert source_needs_gcloud("gs://bucket/path.csv") is True
    assert source_needs_gcloud("https://example.com/path.csv") is False
    assert source_needs_gcloud("data/prices.csv") is False
