from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.soxl_tqqq_clean_cutover_snapshot import (
    ExternalBindings,
    SnapshotValidationError,
    TrustedSnapshotPackage,
)

SOURCE = "1" * 64
CALENDAR = "2" * 64
MANIFEST = "3" * 64


def payload() -> dict:
    return {
        "schema_version": "soxl_tqqq_clean_cutover_snapshot.v1",
        "evidence_generation": "clean_cutover_v1",
        "pair_id": "QQQ_TQQQ",
        "plugin_state": "ABSENT_DISABLED",
        "size_zero": True,
        "source_sha256": SOURCE,
        "calendar_sha256": CALENDAR,
        "external_manifest_sha256": MANIFEST,
        "adjusted_price_field": "adjusted_close",
        "timezone": "UTC",
        "sessions": ["2026-07-24"],
        "rows": [
            {"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": "45.25"},
            {"session": "2026-07-24", "symbol": "TQQQ", "adjusted_close": "12.5"},
        ],
        "snapshot_id": f"sha256-{MANIFEST}",
    }


def write_snapshot(path: Path, value: dict | None = None) -> None:
    value = value or payload()
    unsigned = (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    value = {**value, "content_sha256": hashlib.sha256(unsigned).hexdigest()}
    path.write_bytes((json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode())


def bindings(path: Path) -> ExternalBindings:
    return ExternalBindings(source_sha256=SOURCE, calendar_sha256=CALENDAR, manifest_sha256=MANIFEST, content_sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def test_readback_and_canonical_serializer(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    write_snapshot(path)
    package = TrustedSnapshotPackage.read(path, root=tmp_path, bindings=bindings(path))
    assert package.snapshot_id == f"sha256-{MANIFEST}"
    assert package.to_bytes() == path.read_bytes()


def test_external_digest_binding_and_file_safety(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    write_snapshot(path)
    with pytest.raises(SnapshotValidationError, match="source_sha256"):
        TrustedSnapshotPackage.read(path, root=tmp_path, bindings=ExternalBindings("9" * 64, CALENDAR, MANIFEST, hashlib.sha256(path.read_bytes()).hexdigest()))
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(SnapshotValidationError, match="symlink"):
        TrustedSnapshotPackage.read(link, root=tmp_path, bindings=bindings(path))


def test_content_digest_is_external_binding(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    write_snapshot(path)
    supplied = ExternalBindings(SOURCE, CALENDAR, MANIFEST, "9" * 64)
    with pytest.raises(SnapshotValidationError, match="content digest"):
        TrustedSnapshotPackage.read(path, root=tmp_path, bindings=supplied)


def test_invalid_root_and_row_identity_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(SnapshotValidationError, match="must be a file"):
        TrustedSnapshotPackage.read(tmp_path, root=tmp_path, bindings=ExternalBindings(SOURCE, CALENDAR, MANIFEST, "0" * 64))
    path = tmp_path / "snapshot.json"
    value = payload()
    value["rows"][0]["symbol"] = []
    write_snapshot(path, value)
    with pytest.raises(SnapshotValidationError, match="row identity"):
        TrustedSnapshotPackage.read(path, root=tmp_path, bindings=bindings(path))


def test_duplicate_keys_and_noncanonical_bytes_rejected(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    raw = json.dumps(payload(), sort_keys=True, separators=(",", ":")).replace("\"pair_id\":\"QQQ_TQQQ\"", "\"pair_id\":\"QQQ_TQQQ\",\"pair_id\":\"QQQ_TQQQ\"")
    path.write_text(raw)
    with pytest.raises(SnapshotValidationError, match="duplicate"):
        TrustedSnapshotPackage.read(path, root=tmp_path, bindings=bindings(path))
    write_snapshot(path)
    path.write_bytes(path.read_bytes().replace(b"\"12.5\"", b"12.50"))
    with pytest.raises(SnapshotValidationError, match="canonical"):
        TrustedSnapshotPackage.read(path, root=tmp_path, bindings=bindings(path))


def test_public_boundary_rejects_raw_inputs() -> None:
    with pytest.raises(TypeError):
        TrustedSnapshotPackage(b"raw")  # type: ignore[arg-type]
