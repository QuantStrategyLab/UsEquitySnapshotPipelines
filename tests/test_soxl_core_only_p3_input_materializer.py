from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p1_binding as p1
from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p3_input_materializer as materializer


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "soxl_core_only_p3_materializer_test",
        "tool_version": "v1",
    }


def _dates(count: int = 400) -> list[str]:
    current = date(2025, 1, 2)
    result: list[str] = []
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _member_payload(count: int = 400) -> dict[str, object]:
    series = {"SOXL": [], "SOXX": [], "BOXX": []}
    for index, session_date in enumerate(_dates(count)):
        base = 100.0 + (index * 0.14) + ((index % 11) * 0.03)
        for symbol, multiplier in (("SOXL", 1.2), ("SOXX", 1.0), ("BOXX", 0.8)):
            close = (base * multiplier) + ((index % 7) * 0.04)
            if symbol == "BOXX" and index < 120:
                continue
            series[symbol].append(
                {
                    "session_date": session_date,
                    "bar": {
                        "open": close - 0.15,
                        "high": close + 0.25,
                        "low": close - 0.30,
                        "close": close,
                        "volume": float(1_000_000 + index),
                    },
                }
            )
    return {"schema_version": materializer.BARS_SCHEMA, "series": series}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _bound_input(count: int = 400):
    payload = _member_payload(count)
    member_bytes = _canonical(payload)
    source_digests = {
        symbol: hashlib.sha256(
            materializer.canonical_soxl_core_only_source_series_bytes(
                symbol=symbol,
                series=payload["series"][symbol],
            )
        ).hexdigest()
        for symbol in ("SOXL", "SOXX", "BOXX")
    }
    binding = p1.build_soxl_core_only_p1_binding(
        date_cutoff=payload["series"]["SOXL"][-1]["session_date"]
    )
    manifest = p1.build_soxl_core_only_input_manifest(
        binding,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=member_bytes,
        source_content_sha256=source_digests,
    )
    return binding, manifest, member_bytes


def test_materializer_binds_canonical_p1_bars_and_derives_runner_sessions() -> None:
    binding, manifest, member_bytes = _bound_input()

    result = materializer.materialize_soxl_core_only_p3_input(
        binding=binding,
        manifest=manifest,
        member_bytes=member_bytes,
    )

    assert result["schema_version"] == materializer.MATERIALIZED_INPUT_SCHEMA
    assert result["p1_identity"]["date_cutoff"] == binding["data_identity"]["date_cutoff"]
    assert result["p2_identity"]["candidate_id"] == "soxl_soxx_core_only_p2_v2"
    assert result["sessions"][0]["as_of"].endswith("T00:00:00+00:00")
    assert result["sessions"][0]["as_of"] == f"{_dates()[251]}T00:00:00+00:00"
    assert _dates()[120] < _dates()[251]
    assert len(result["sessions"]) > 2
    indicators = result["sessions"][-1]["market_data"]["derived_indicators"]
    assert indicators["SOXL"]["ma_trend"] > 0.0
    assert 0.50 <= indicators["SOXX"]["realized_volatility_10_dynamic_threshold"] <= 0.75
    material = {key: value for key, value in result.items() if key != "materialized_input_sha256"}
    assert result["materialized_input_sha256"] == hashlib.sha256(_canonical(material)).hexdigest()


def test_materializer_rejects_tampered_or_noncanonical_p1_member() -> None:
    binding, manifest, member_bytes = _bound_input()
    payload = json.loads(member_bytes)
    payload["series"]["SOXX"][5]["bar"]["close"] += 1.0
    tampered = _canonical(payload)
    with pytest.raises(materializer.SoxlCoreOnlyP3MaterializerError):
        materializer.materialize_soxl_core_only_p3_input(
            binding=binding,
            manifest=manifest,
            member_bytes=tampered,
        )

    with pytest.raises(materializer.SoxlCoreOnlyP3MaterializerError):
        materializer.materialize_soxl_core_only_p3_input(
            binding=binding,
            manifest=manifest,
            member_bytes=member_bytes + b"\n",
        )


def test_materializer_rejects_missing_asset_or_insufficient_history() -> None:
    binding, manifest, member_bytes = _bound_input()
    payload = json.loads(member_bytes)
    payload["series"].pop("BOXX")
    with pytest.raises(materializer.SoxlCoreOnlyP3MaterializerError):
        materializer.materialize_soxl_core_only_p3_input(
            binding=binding,
            manifest=manifest,
            member_bytes=_canonical(payload),
        )

    short_binding, short_manifest, short_member = _bound_input(251)
    with pytest.raises(materializer.SoxlCoreOnlyP3MaterializerError):
        materializer.materialize_soxl_core_only_p3_input(
            binding=short_binding,
            manifest=short_manifest,
            member_bytes=short_member,
        )
