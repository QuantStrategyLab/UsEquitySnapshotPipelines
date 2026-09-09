from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

import pytest

from test_soxl_v7_nonlive_forward_observation import _record
from us_equity_snapshot_pipelines.lifecycle import soxl_v7_nonlive_forward_observation as v7


def test_cli_projects_only_existing_record_with_closed_output_fields(
    tmp_path: Path, monkeypatch
) -> None:
    record = tmp_path / "record.json"
    output = tmp_path / "source.json"
    record.write_text(json.dumps(_record()), encoding="utf-8")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("observation execution is forbidden")

    monkeypatch.setattr(v7, "build_soxl_v7_nonlive_forward_inputs", forbidden)
    monkeypatch.setattr(v7, "build_soxl_v7_nonlive_forward_record", forbidden)
    script = Path(__file__).parents[1] / "scripts/build_soxl_v7_forward_control_plane_source.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--record",
            str(record),
            "--output",
            str(output),
            "--generated-at",
            "2026-09-09T03:00:00Z",
        ],
    )
    runpy.run_path(str(script), run_name="__main__")

    source = json.loads(output.read_text(encoding="utf-8"))
    assert set(source) == {
        "schema_version",
        "source_id",
        "generated_at",
        "computed_at",
        "data_status",
        "candidates",
        "errors",
    }
    assert set(source["candidates"][0]) == {
        "candidate_id",
        "candidate_kind",
        "domain",
        "lifecycle",
        "evidence",
        "recommendation",
        "freshness",
        "forward_observation",
    }
    assert source["computed_at"] == _record()["observed_at"]
    assert source["candidates"][0]["freshness"]["status"] == "fresh"
    assert source["candidates"][0]["forward_observation"]["no_order"] is True
    assert source["candidates"][0]["forward_observation"]["live_authority_granted"] is False


def test_cli_stops_with_fixed_error_for_invalid_record(
    tmp_path: Path, monkeypatch
) -> None:
    record = tmp_path / "record.json"
    output = tmp_path / "source.json"
    record.write_text("{}", encoding="utf-8")
    script = Path(__file__).parents[1] / "scripts/build_soxl_v7_forward_control_plane_source.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--record",
            str(record),
            "--output",
            str(output),
            "--generated-at",
            "2026-09-09T03:00:00Z",
        ],
    )

    with pytest.raises(SystemExit, match="invalid SOXL V7 forward control-plane source"):
        runpy.run_path(str(script), run_name="__main__")
    assert not output.exists()
