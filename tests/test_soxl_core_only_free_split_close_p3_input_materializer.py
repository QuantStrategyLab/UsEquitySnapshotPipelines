from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_free_split_close_p1 as p1
from us_equity_snapshot_pipelines.lifecycle import (
    soxl_core_only_free_split_close_p3_input_materializer as materializer,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import (
    expected_soxl_core_only_sessions,
)

_CUTOFF = "2026-08-18"


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "soxl_core_only_free_split_close_p3_materializer_test",
        "tool_version": "v1",
    }


class _Observer:
    def observe_daily_bars(self, *, source_id: str, symbol: str, start_date: str, date_cutoff: str):
        assert date_cutoff == _CUTOFF
        assert start_date == expected_soxl_core_only_sessions(_CUTOFF)[symbol][0].isoformat()
        offset = {"SOXL": 10.0, "SOXX": 100.0, "BOXX": 20.0}[symbol]
        bars = tuple(
            DailyBar(
                session_date=session.isoformat(),
                open=offset + index,
                high=offset + index,
                low=offset + index,
                close=offset + index,
                volume=1000.0 + index,
            )
            for index, session in enumerate(expected_soxl_core_only_sessions(_CUTOFF)[symbol], start=1)
        )
        return DailyBarSourceObservation(
            source_id=source_id,
            status=SOURCE_OBSERVATION_READY,
            snapshot=DailyBarSourceSnapshot(
                source_id=source_id,
                symbol=symbol,
                date_cutoff=_CUTOFF,
                adjustment_basis="split_adjusted",
                source_artifact_sha256=hashlib.sha256(f"{source_id}:{symbol}".encode()).hexdigest(),
                bars=bars,
            ),
        )


def _bound_root(tmp_path: Path) -> Path:
    root = tmp_path / "p1"
    p1.publish_soxl_core_only_free_split_close_p1_inputs(
        _Observer(),
        output_root=root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )
    return root


def _materialize(root: Path) -> dict[str, object]:
    return materializer.materialize_soxl_core_only_free_split_close_p3_input(
        binding=json.loads((root / "binding.json").read_bytes()),
        manifest=json.loads((root / "manifest.json").read_bytes()),
        closes_bytes=(root / "closes.json").read_bytes(),
        assurance_bytes=(root / "assurance.json").read_bytes(),
    )


def test_materializer_binds_v4_close_and_assurance_members_before_deriving_sessions(tmp_path: Path) -> None:
    result = _materialize(_bound_root(tmp_path))

    assert result["schema_version"] == materializer.MATERIALIZED_INPUT_SCHEMA
    assert result["p2_identity"]["candidate_id"] == "soxl_soxx_core_only_p2_v4_free_split_close"
    assert set(result["p1_identity"]) == {
        "input_manifest_sha256",
        "binding_sha256",
        "closes_member_sha256",
        "assurance_member_sha256",
        "date_cutoff",
    }
    assert result["indicator_spec"]["price_field"] == "split_adjusted_close"
    assert len(result["sessions"]) >= 252
    assert result["sessions"][-1]["as_of"] == f"{_CUTOFF}T00:00:00+00:00"
    material = {key: value for key, value in result.items() if key != "materialized_input_sha256"}
    assert result["materialized_input_sha256"] == hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def test_materializer_rejects_tampered_verifier_receipt(tmp_path: Path) -> None:
    root = _bound_root(tmp_path)
    assurance = json.loads((root / "assurance.json").read_bytes())
    assurance["assurances"]["SOXX"]["diagnostic"]["source_snapshot_sha256"][
        "yahoo_finance_chart_1day_split_adjusted"
    ] = "0" * 64

    with pytest.raises(materializer.SoxlCoreOnlyFreeSplitCloseP3MaterializerError):
        materializer.materialize_soxl_core_only_free_split_close_p3_input(
            binding=json.loads((root / "binding.json").read_bytes()),
            manifest=json.loads((root / "manifest.json").read_bytes()),
            closes_bytes=(root / "closes.json").read_bytes(),
            assurance_bytes=json.dumps(assurance, sort_keys=True, separators=(",", ":")).encode(),
        )
