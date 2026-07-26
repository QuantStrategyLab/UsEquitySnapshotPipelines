import hashlib
import json
import threading
from types import MappingProxyType
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines import soxl_tqqq_clean_cutover_snapshot as snapshot


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _calendar(tmp_path: Path, sessions: list[str] | None = None) -> tuple[Path, str]:
    value = {
        "exchange": "XNYS",
        "schema": "xnys_calendar_v1",
        "sessions": sessions or ["2026-01-02", "2026-01-05"],
        "timezone": "America/New_York",
    }
    path = tmp_path / "calendar.json"
    raw = _canonical(value)
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


def _rows(pair_id: str = "QQQ_TQQQ") -> list[dict[str, object]]:
    symbols = snapshot.PAIR_SYMBOLS[pair_id]
    return [
        {"session": session, "symbol": symbol, "adjusted_close": 100.0 + ordinal}
        for session in ("2026-01-02", "2026-01-05")
        for ordinal, symbol in enumerate(symbols)
    ]


def _materialize(tmp_path: Path, pair_id: str = "QQQ_TQQQ") -> snapshot.SnapshotResult:
    calendar, digest = _calendar(tmp_path)
    return snapshot.materialize_clean_cutover_snapshot(
        pair_id,
        _rows(pair_id),
        tmp_path / "snapshot",
        calendar_path=calendar,
        calendar_sha256=digest,
        source_identity="synthetic-source",
        producer_identity="synthetic-producer",
        generated_at="2026-01-05T12:00:00Z",
    )


def _readback(result: snapshot.SnapshotResult, tmp_path: Path) -> snapshot.SnapshotResult:
    return snapshot.strict_readback_clean_cutover_snapshot(
        result.path,
        calendar_path=tmp_path / "calendar.json",
        calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest(),
    )


def _resign_manifest(result: snapshot.SnapshotResult, manifest: dict[str, object]) -> None:
    raw = _canonical(manifest)
    (result.path / "manifest.json").write_bytes(raw)
    marker = json.loads((result.path / "publication.json").read_text())
    marker["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
    (result.path / "publication.json").write_bytes(_canonical(marker))


def _resign_publication(result: snapshot.SnapshotResult, payload: dict[str, object], manifest: dict[str, object]) -> None:
    payload_raw = _canonical(payload)
    manifest["payload_sha256"] = hashlib.sha256(payload_raw).hexdigest()
    manifest_raw = _canonical(manifest)
    marker = {
        "complete": True,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "payload_sha256": hashlib.sha256(payload_raw).hexdigest(),
        "snapshot_id": manifest["snapshot_id"],
    }
    (result.path / "payload.json").write_bytes(payload_raw)
    (result.path / "manifest.json").write_bytes(manifest_raw)
    (result.path / "publication.json").write_bytes(_canonical(marker))


def test_schema_binds_each_pair_to_exact_ordered_symbols() -> None:
    schema = json.loads(Path("schemas/soxl_tqqq_clean_cutover_snapshot.v1.schema.json").read_text())
    assert schema["allOf"]
    assert "format" not in json.dumps(schema)


@pytest.mark.parametrize("pair_id", ["QQQ_TQQQ", "SOXX_SOXL"])
def test_materializer_and_readback_preserve_pair_order_for_both_pairs(tmp_path: Path, pair_id: str) -> None:
    result = _materialize(tmp_path, pair_id)
    assert snapshot.strict_readback_clean_cutover_snapshot(
        result.path, calendar_path=tmp_path / "calendar.json", calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest()
    ) == result


def test_rejects_swapped_cross_pair_extra_and_duplicate_symbols(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    for rows in (_rows()[::-1], _rows() + [_rows()[0]], [{**row, "symbol": "SOXX"} for row in _rows()]):
        with pytest.raises(snapshot.SnapshotValidationError):
            snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", rows, tmp_path / str(len(rows)), calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_readback_rejects_bool_or_integer_lookalikes_in_manifest_coverage_and_marker(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    manifest = json.loads((result.path / "manifest.json").read_text())
    manifest["size"] = False
    _resign_manifest(result, manifest)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


@pytest.mark.parametrize("bad", [False, True, 0, -1, float("inf"), float("nan"), 10**1000])
def test_materializer_normalizes_scalar_and_numeric_failures(tmp_path: Path, bad: object) -> None:
    rows = _rows()
    rows[0]["adjusted_close"] = bad
    calendar, digest = _calendar(tmp_path)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", rows, tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_materializer_rejects_non_lossless_integer_adjusted_close(tmp_path: Path) -> None:
    rows = _rows()
    rows[0]["adjusted_close"] = 9_007_199_254_740_993
    calendar, digest = _calendar(tmp_path)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", rows, tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_materializer_accepts_and_normalizes_declared_mapping_rows(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    rows = [MappingProxyType(row) for row in _rows()]
    result = snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", rows, tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")
    assert _readback(result, tmp_path) == result


def test_materializer_bounds_incremental_row_ingestion(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)

    def unbounded_rows() -> object:
        for index in range(20_000):
            symbol = snapshot.PAIR_SYMBOLS["QQQ_TQQQ"][index % 2]
            yield {"session": "2026-01-02", "symbol": symbol, "adjusted_close": 100.0}
        raise AssertionError("rows iterable was consumed without a bound")

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", unbounded_rows(), tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_calendar_requires_regular_nonsymlink_file_and_exact_external_sha(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), tmp_path / "snapshot", calendar_path=calendar, calendar_sha256="0" * 64, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")
    link = tmp_path / "calendar-link.json"
    link.symlink_to(calendar)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), tmp_path / "snapshot-link", calendar_path=link, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_materializer_rejects_weekday_holiday_absent_from_calendar(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path, ["2026-01-02"])
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")


def test_manifest_marks_fixture_only_size_zero_plugin_absent(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    manifest = json.loads((result.path / "manifest.json").read_text())
    assert manifest["evidence_generation"] == "clean_cutover_v1"
    assert manifest["offline_fixture"] is True and manifest["plugin"] == "ABSENT_DISABLED" and manifest["size"] == 0


def test_materializer_emits_canonical_json_for_all_members(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    for name in ("manifest.json", "payload.json", "publication.json"):
        raw = (result.path / name).read_bytes()
        assert raw == _canonical(json.loads(raw))


@pytest.mark.parametrize("raw", [b"{", b'{"x":1,"x":2}\n', b'{"x":NaN}\n', b"\xff"])
def test_readback_normalizes_invalid_utf8_duplicate_keys_and_json_syntax(tmp_path: Path, raw: bytes) -> None:
    result = _materialize(tmp_path)
    (result.path / "payload.json").write_bytes(raw)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


def test_readback_rejects_noncanonical_but_logically_equivalent_bytes(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    value = json.loads((result.path / "payload.json").read_text())
    (result.path / "payload.json").write_text(json.dumps(value, indent=2) + "\n")
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


def test_readback_normalizes_deep_json_recursion(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    (result.path / "payload.json").write_bytes(b"[" * 2_000 + b"]" * 2_000)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


def test_python_rejects_impossible_dates_and_noncanonical_utc_timestamps(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), tmp_path / "snapshot", calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00.1Z")


def test_readback_rejects_partial_publication_and_extra_member(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    (result.path / "extra.json").write_text("{}")
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.strict_readback_clean_cutover_snapshot(result.path, calendar_path=tmp_path / "calendar.json", calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest())


def test_materializer_rejects_oversized_manifest_before_reservation(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    destination = tmp_path / "snapshot"
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), destination, calendar_path=calendar, calendar_sha256=digest, source_identity="x" * snapshot.MAX_MEMBER_BYTES, producer_identity="producer", generated_at="2026-01-05T12:00:00Z")
    assert not destination.exists()


def test_preexisting_destination_is_never_clobbered_and_two_publishers_have_one_winner(tmp_path: Path) -> None:
    calendar, digest = _calendar(tmp_path)
    destination = tmp_path / "snapshot"
    destination.write_text("sentinel")
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), destination, calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")
    assert destination.read_text() == "sentinel"
    destination.unlink()
    outcomes: list[object] = []
    def publish() -> None:
        try:
            outcomes.append(snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), destination, calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z"))
        except snapshot.SnapshotValidationError as error:
            outcomes.append(error)
    threads = [threading.Thread(target=publish), threading.Thread(target=publish)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    assert sum(isinstance(outcome, snapshot.SnapshotResult) for outcome in outcomes) == 1


def test_readback_rejects_symlink_root_and_member(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    link = tmp_path / "snapshot-link"
    link.symlink_to(result.path, target_is_directory=True)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.strict_readback_clean_cutover_snapshot(link, calendar_path=tmp_path / "calendar.json", calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest())
    member = result.path / "manifest.json"
    saved = member.read_bytes()
    member.unlink()
    member.symlink_to(tmp_path / "calendar.json")
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)
    member.unlink()
    member.write_bytes(saved)


def test_readback_rechecks_calendar_membership_and_external_manifest_digest(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    calendar, digest = _calendar(tmp_path, ["2026-01-02"])
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.strict_readback_clean_cutover_snapshot(result.path, calendar_path=calendar, calendar_sha256=digest)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.strict_readback_clean_cutover_snapshot(result.path, calendar_path=tmp_path / "calendar.json", calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest(), expected_manifest_sha256="0" * 64)


def test_readback_recomputes_and_binds_canonical_snapshot_id(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    payload = json.loads((result.path / "payload.json").read_text())
    manifest = json.loads((result.path / "manifest.json").read_text())
    payload["snapshot_id"] = "clean_cutover_bogus"
    manifest["snapshot_id"] = "clean_cutover_bogus"
    _resign_publication(result, payload, manifest)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


def test_readback_rejects_integer_spelling_for_canonical_float(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    payload = json.loads((result.path / "payload.json").read_text())
    manifest = json.loads((result.path / "manifest.json").read_text())
    payload["rows"][0]["adjusted_close"] = 100
    _resign_publication(result, payload, manifest)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)


def test_readback_consumes_declared_member_size_across_short_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _materialize(tmp_path)
    original_read = snapshot.os.read

    def short_read(fd: int, size: int) -> bytes:
        return original_read(fd, max(1, size // 2))

    monkeypatch.setattr(snapshot.os, "read", short_read)
    assert _readback(result, tmp_path) == result


def test_materializer_fsyncs_parent_after_reserving_destination(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_fsync = snapshot.os.fsync
    parent_inode = tmp_path.stat().st_ino
    synced_inodes: list[int] = []

    def record_fsync(fd: int) -> None:
        synced_inodes.append(snapshot.os.fstat(fd).st_ino)
        original_fsync(fd)

    monkeypatch.setattr(snapshot.os, "fsync", record_fsync)
    _materialize(tmp_path)
    assert parent_inode in synced_inodes


def test_materializer_cleans_up_its_own_partial_publication_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calendar, digest = _calendar(tmp_path)
    destination = tmp_path / "snapshot"

    def fail_write(_: int, __: str, ___: bytes) -> None:
        raise snapshot.SnapshotValidationError("injected write failure")

    monkeypatch.setattr(snapshot, "_write_member", fail_write)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_clean_cutover_snapshot("QQQ_TQQQ", _rows(), destination, calendar_path=calendar, calendar_sha256=digest, source_identity="source", producer_identity="producer", generated_at="2026-01-05T12:00:00Z")
    assert not destination.exists()


def test_readback_rejects_mismatched_coverage_and_completion_marker(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    manifest = json.loads((result.path / "manifest.json").read_text())
    manifest["coverage"]["row_count"] = 3
    _resign_manifest(result, manifest)
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, tmp_path)
    again = tmp_path / "again"
    again.mkdir()
    result = _materialize(again)
    (result.path / "publication.json").unlink()
    with pytest.raises(snapshot.SnapshotValidationError):
        _readback(result, again)


def test_rejects_legacy_dual_read_fallback_plugin_or_nonzero_size_modes(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    manifest = json.loads((result.path / "manifest.json").read_text())
    manifest["plugin"] = "ENABLED"
    (result.path / "manifest.json").write_bytes(_canonical(manifest))
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.strict_readback_clean_cutover_snapshot(result.path, calendar_path=tmp_path / "calendar.json", calendar_sha256=hashlib.sha256((tmp_path / "calendar.json").read_bytes()).hexdigest())
