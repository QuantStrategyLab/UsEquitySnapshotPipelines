from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.soxl_tqqq_clean_cutover_snapshot import (
    SnapshotValidationError,
    materialize_clean_cutover_snapshot,
    quarantine_raw_payload,
    strict_readback_clean_cutover_snapshot,
)

SOURCE = "1" * 64
CALENDAR = "2" * 64
MANIFEST = "3" * 64
ROWS = [
    {"session": "2026-07-24", "symbol": "TQQQ", "adjusted_close": 12.5},
    {"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": "45.25"},
]


def quarantined():
    payload = json.dumps(ROWS, separators=(",", ":")).encode()
    return quarantine_raw_payload(payload, {"source_sha256": hashlib.sha256(payload).hexdigest(), "retrieved_at": "2026-07-27T00:00:00Z", "source_identity": "synthetic"})


def test_materialize_and_strict_readback(tmp_path: Path):
    result = materialize_clean_cutover_snapshot(quarantined(), tmp_path / "snapshot.json", source_sha256=quarantined().receipt.source_sha256, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST, sessions=["2026-07-24"])
    assert result.snapshot_identity == f"sha256-{MANIFEST}"
    checked = strict_readback_clean_cutover_snapshot(result.path, expected_source_sha256=result.path.read_text() and quarantined().receipt.source_sha256, expected_calendar_sha256=CALENDAR, expected_external_manifest_sha256=MANIFEST)
    assert checked == result


def test_no_clobber_and_digest_binding(tmp_path: Path):
    path = tmp_path / "snapshot.json"
    q = quarantined()
    materialize_clean_cutover_snapshot(q, path, source_sha256=q.receipt.source_sha256, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST)
    with pytest.raises(SnapshotValidationError, match="destination already exists"):
        materialize_clean_cutover_snapshot(q, path, source_sha256=q.receipt.source_sha256, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST)
    with pytest.raises(SnapshotValidationError, match="source digest mismatch"):
        materialize_clean_cutover_snapshot(q, tmp_path / "other.json", source_sha256=SOURCE, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST)


@pytest.mark.parametrize("rows", [
    [{"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": 1}],
    ROWS + [{"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": 1}],
    [{"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": 0}, {"session": "2026-07-24", "symbol": "TQQQ", "adjusted_close": 1}],
])
def test_rows_fail_closed(tmp_path: Path, rows):
    payload = json.dumps(rows).encode()
    q = quarantine_raw_payload(payload, {"source_sha256": hashlib.sha256(payload).hexdigest(), "retrieved_at": "now", "source_identity": "synthetic"})
    with pytest.raises(SnapshotValidationError):
        materialize_clean_cutover_snapshot(q, tmp_path / "snapshot.json", source_sha256=q.receipt.source_sha256, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST)


def test_readback_rejects_symlink(tmp_path: Path):
    target = tmp_path / "target.json"
    q = quarantined()
    materialize_clean_cutover_snapshot(q, target, source_sha256=q.receipt.source_sha256, calendar_sha256=CALENDAR, external_manifest_sha256=MANIFEST)
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(SnapshotValidationError, match="symlink"):
        strict_readback_clean_cutover_snapshot(link, expected_source_sha256=q.receipt.source_sha256, expected_calendar_sha256=CALENDAR, expected_external_manifest_sha256=MANIFEST)
