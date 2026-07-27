from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.soxl_tqqq_clean_cutover_snapshot import *

ROWS = [
    {"session": "2026-07-27", "symbol": "QQQ", "adjusted_close": "1.0"},
    {"session": "2026-07-27", "symbol": "TQQQ", "adjusted_close": "2.0"},
]


def test_quarantine_is_provider_free():
    q = quarantine_raw_payload({"rows": ROWS}, {"retrieved_at": "synthetic"})
    assert q["offline_fixture"] is True


def test_materialize_and_readback(tmp_path: Path):
    p = tmp_path / "snap.json"
    a = materialize_clean_cutover_snapshot(
        destination=p,
        rows=ROWS,
        sessions=["2026-07-27"],
        source_sha256="s",
        calendar_sha256="c",
        external_manifest_sha256="m",
    )
    assert (
        strict_readback_clean_cutover_snapshot(
            path=p, expected_source_sha256="s", expected_calendar_sha256="c", expected_manifest_sha256="m"
        )["snapshot_identity"]
        == a["snapshot_identity"]
    )
    with pytest.raises(SnapshotValidationError):
        materialize_clean_cutover_snapshot(
            destination=p,
            rows=ROWS,
            sessions=["2026-07-27"],
            source_sha256="s",
            calendar_sha256="c",
            external_manifest_sha256="m",
        )


def test_rows_require_pair_and_canonical(tmp_path):
    with pytest.raises(SnapshotValidationError):
        materialize_clean_cutover_snapshot(
            destination=tmp_path / "x",
            rows=ROWS[:1],
            sessions=["2026-07-27"],
            source_sha256="s",
            calendar_sha256="c",
            external_manifest_sha256="m",
        )
