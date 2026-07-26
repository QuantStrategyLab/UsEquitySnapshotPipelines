from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

_spec = importlib.util.spec_from_file_location(
    "soxl_tqqq_clean_cutover_snapshot",
    Path(__file__).parents[1] / "src/us_equity_snapshot_pipelines/soxl_tqqq_clean_cutover_snapshot.py",
)
assert _spec and _spec.loader
snapshot = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = snapshot
_spec.loader.exec_module(snapshot)


CALENDAR_SHA256 = "c" * 64


def _rows(pair_id: str) -> list[dict[str, object]]:
    if pair_id == "QQQ_TQQQ":
        return [
            {"session": "2026-01-02", "symbol": "TQQQ", "adjusted_close": 42.0},
            {"session": "2026-01-02", "symbol": "QQQ", "adjusted_close": 500.0},
            {"session": "2026-01-05", "symbol": "TQQQ", "adjusted_close": 43.0},
            {"session": "2026-01-05", "symbol": "QQQ", "adjusted_close": 501.0},
        ]
    return [
        {"session": "2026-02-03", "symbol": "SOXL", "adjusted_close": 18.0},
        {"session": "2026-02-03", "symbol": "SOXX", "adjusted_close": 220.0},
        {"session": "2026-02-04", "symbol": "SOXL", "adjusted_close": 18.5},
        {"session": "2026-02-04", "symbol": "SOXX", "adjusted_close": 221.0},
    ]


def _materialize(tmp_path: Path, pair_id: str) -> snapshot.SnapshotResult:
    return snapshot.materialize_clean_cutover_snapshot(
        pair_id=pair_id,
        rows=_rows(pair_id),
        output_dir=tmp_path / pair_id.lower(),
        calendar_sha256=CALENDAR_SHA256,
        source_identity="offline-fixture-v1",
        producer_identity="unit-test",
        materialized_at="2026-07-26T00:00:00Z",
    )


def _manifest_sha256(output_dir: Path) -> str:
    return hashlib.sha256((output_dir / "manifest.json").read_bytes()).hexdigest()


def test_independent_pair_snapshots_bind_external_digests_without_common_sessions(tmp_path: Path) -> None:
    tqqq = _materialize(tmp_path, "QQQ_TQQQ")
    soxl = _materialize(tmp_path, "SOXX_SOXL")

    assert tqqq.snapshot_id == f"sha256-{_manifest_sha256(tqqq.output_dir)}"
    assert soxl.snapshot_id == f"sha256-{_manifest_sha256(soxl.output_dir)}"
    assert tqqq.snapshot_id != soxl.snapshot_id
    assert snapshot.verify_clean_cutover_snapshot(
        tqqq.output_dir,
        expected_manifest_sha256=_manifest_sha256(tqqq.output_dir),
        expected_calendar_sha256=CALENDAR_SHA256,
    ) == tqqq
    assert snapshot.verify_clean_cutover_snapshot(
        soxl.output_dir,
        expected_manifest_sha256=_manifest_sha256(soxl.output_dir),
        expected_calendar_sha256=CALENDAR_SHA256,
    ) == soxl


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: rows.pop(),
        lambda rows: rows.append({"session": "2026-01-05", "symbol": "TQQQ", "adjusted_close": 44.0}),
        lambda rows: rows.append({"session": "2026-01-05", "symbol": "SPY", "adjusted_close": 600.0}),
    ],
)
def test_materializer_rejects_partial_duplicate_or_extra_pair_rows(tmp_path: Path, mutate) -> None:
    rows = _rows("QQQ_TQQQ")
    mutate(rows)

    with pytest.raises(snapshot.SnapshotValidationError, match="exactly the pair symbols"):
        snapshot.materialize_clean_cutover_snapshot(
            pair_id="QQQ_TQQQ",
            rows=rows,
            output_dir=tmp_path / "snapshot",
            calendar_sha256=CALENDAR_SHA256,
            source_identity="offline-fixture-v1",
            producer_identity="unit-test",
            materialized_at="2026-07-26T00:00:00Z",
        )


def test_materializer_rejects_noncanonical_generation_plugin_and_legacy_modes(tmp_path: Path) -> None:
    common = {
        "pair_id": "QQQ_TQQQ",
        "rows": _rows("QQQ_TQQQ"),
        "calendar_sha256": CALENDAR_SHA256,
        "source_identity": "offline-fixture-v1",
        "producer_identity": "unit-test",
        "materialized_at": "2026-07-26T00:00:00Z",
    }
    for field, value in (("evidence_generation", "legacy_v3"), ("plugin", "ENABLED"), ("legacy_read", "dual_read")):
        with pytest.raises(snapshot.SnapshotValidationError, match="INVALID_EVIDENCE"):
            snapshot.materialize_clean_cutover_snapshot(output_dir=tmp_path / field, **common, **{field: value})


def test_verifier_rejects_missing_external_digest_and_tampered_calendar_binding(tmp_path: Path) -> None:
    result = _materialize(tmp_path, "QQQ_TQQQ")

    with pytest.raises(snapshot.SnapshotValidationError, match="expected manifest"):
        snapshot.verify_clean_cutover_snapshot(
            result.output_dir,
            expected_manifest_sha256="",
            expected_calendar_sha256=CALENDAR_SHA256,
        )
    with pytest.raises(snapshot.SnapshotValidationError, match="calendar"):
        snapshot.verify_clean_cutover_snapshot(
            result.output_dir,
            expected_manifest_sha256=_manifest_sha256(result.output_dir),
            expected_calendar_sha256="d" * 64,
        )


def test_verifier_rejects_unsorted_payload_and_existing_identity(tmp_path: Path) -> None:
    result = _materialize(tmp_path, "QQQ_TQQQ")
    payload_path = result.output_dir / "payload.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["rows"] = list(reversed(payload["rows"]))
    payload_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid manifest binding"):
        snapshot.verify_clean_cutover_snapshot(
            result.output_dir,
            expected_manifest_sha256=_manifest_sha256(result.output_dir),
            expected_calendar_sha256=CALENDAR_SHA256,
        )
    with pytest.raises(snapshot.SnapshotValidationError, match="immutable output"):
        _materialize(tmp_path, "QQQ_TQQQ")
