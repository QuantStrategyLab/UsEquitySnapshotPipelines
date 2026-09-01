from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest
from quant_platform_kit.data import (
    DECISION_PRICE_SERIES_MEMBER_PATH,
    read_decision_price_series_artifact_json,
)
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
)

from us_equity_snapshot_pipelines.lifecycle.decision_data_projection import (
    DecisionDataProjectionError,
    extract_soxl_bars_daily_series,
    extract_tqqq_bars_daily_series,
    publish_verified_daily_price_series_projection,
    verify_decision_price_series_projection_root,
)


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "decision_data_projection",
        "tool_version": "v1",
    }


def _parent_root(tmp_path: Path, *, bars: bytes, profile: str) -> tuple[Path, str]:
    root = tmp_path / "parent"
    root.mkdir(mode=0o700)
    manifest = canonical_research_input_manifest_bytes(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": "native-p1-root",
            "research_input_contract_id": "native_daily_p1.v1",
            "domain": "us_equity",
            "profile": profile,
            "artifact_type": "immutable_adjusted_ohlcv_etf_only",
            "observed_at": "2026-08-28T20:00:00Z",
            "effective_at": "2026-08-28T20:00:00Z",
            "as_of": "2026-08-28T20:00:00Z",
            "producer": _producer(),
            "calendar": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "session_date": "2026-08-28",
                "source": "exchange_calendars",
                "source_revision": "v1",
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "alpaca_sip",
                "source_revision": "v1",
            },
            "sources": [
                {
                    "source_id": "alpaca_sip_1day_adjustment_all:QQQ",
                    "revision": "v1",
                    "observed_at": "2026-08-28T20:00:00Z",
                    "content_sha256": "c" * 64,
                },
                {
                    "source_id": "alpaca_sip_1day_adjustment_all:TQQQ",
                    "revision": "v1",
                    "observed_at": "2026-08-28T20:00:00Z",
                    "content_sha256": "d" * 64,
                },
            ],
            "members": [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(bars),
                    "sha256": sha256(bars).hexdigest(),
                }
            ],
        }
    )
    root.joinpath("bars.json").write_bytes(bars)
    root.joinpath("manifest.json").write_bytes(manifest)
    return root, research_input_manifest_sha256(json.loads(manifest))


def _verified_parent(expected_sha256: str):
    def verify(_: Path) -> str:
        return expected_sha256

    return verify


def test_tqqq_projection_binds_to_verified_parent_and_uses_only_portable_series(tmp_path: Path) -> None:
    bars = b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{"QQQ":{"bars":[{"date":"2026-08-27","close":100.0,"volume":10},{"date":"2026-08-28","close":101.0,"volume":11}]},"TQQQ":{"bars":[{"date":"2026-08-27","close":50.0,"volume":20},{"date":"2026-08-28","close":51.0,"volume":21}]}}}'
    parent, parent_sha256 = _parent_root(tmp_path, bars=bars, profile="tqqq_core_only_p2_v5")
    output = tmp_path / "projection"

    result = publish_verified_daily_price_series_projection(
        parent_root=parent,
        output_root=output,
        parent_verifier=_verified_parent(parent_sha256),
        strategy_scope="tqqq_growth_income",
        series_extractor=extract_tqqq_bars_daily_series,
        producer=_producer(),
    )

    assert result["parent_manifest_sha256"] == parent_sha256
    assert result["manifest_sha256"] == verify_decision_price_series_projection_root(output)
    projection = read_decision_price_series_artifact_json(
        output.joinpath(DECISION_PRICE_SERIES_MEMBER_PATH).read_bytes()
    )
    assert projection["strategy_scope"] == "tqqq_growth_income"
    assert projection["as_of"] == "2026-08-28"
    assert projection["adjustment_basis"] == "total_return_adjusted"
    assert set(projection["series"]) == {"QQQ", "TQQQ"}
    assert projection["series"]["QQQ"]["points"][-1]["close"] == 101.0


def test_soxl_extractor_normalizes_native_session_rows(tmp_path: Path) -> None:
    root = tmp_path / "soxl"
    root.mkdir()
    root.joinpath("bars.json").write_bytes(
        b'{"schema_version":"qsl.soxl_soxx_core_only_private_bars.v1","series":{"SOXL":[{"session_date":"2026-08-28","bar":{"close":11.0,"volume":5}}]}}'
    )

    assert extract_soxl_bars_daily_series(root) == {
        "SOXL": [{"as_of": "2026-08-28", "close": 11.0, "volume": 5}]
    }


def test_projection_rejects_parent_member_changes_after_native_verification(tmp_path: Path) -> None:
    bars = b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{"QQQ":{"bars":[{"date":"2026-08-28","close":100.0,"volume":10}]}}}'
    parent, parent_sha256 = _parent_root(tmp_path, bars=bars, profile="tqqq_core_only_p2_v5")
    parent.joinpath("bars.json").write_bytes(bars + b" ")

    with pytest.raises(DecisionDataProjectionError):
        publish_verified_daily_price_series_projection(
            parent_root=parent,
            output_root=tmp_path / "projection",
            parent_verifier=_verified_parent(parent_sha256),
            strategy_scope="tqqq_growth_income",
            series_extractor=extract_tqqq_bars_daily_series,
            producer=_producer(),
        )


def test_projection_rechecks_parent_member_after_series_extraction(tmp_path: Path) -> None:
    bars = b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{"QQQ":{"bars":[{"date":"2026-08-28","close":100.0,"volume":10}]}}}'
    parent, parent_sha256 = _parent_root(tmp_path, bars=bars, profile="tqqq_core_only_p2_v5")

    def mutating_extractor(root: Path) -> dict[str, list[dict[str, object]]]:
        extracted = extract_tqqq_bars_daily_series(root)
        root.joinpath("bars.json").write_bytes(bars + b" ")
        return extracted

    with pytest.raises(DecisionDataProjectionError):
        publish_verified_daily_price_series_projection(
            parent_root=parent,
            output_root=tmp_path / "projection",
            parent_verifier=_verified_parent(parent_sha256),
            strategy_scope="tqqq_growth_income",
            series_extractor=mutating_extractor,
            producer=_producer(),
        )
