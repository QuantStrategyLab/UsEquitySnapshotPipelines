from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot


def _fixture_prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "TQQQ", "adjusted_close": 10.5},
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.25},
            {"session": "2010-01-05", "symbol": "TQQQ", "adjusted_close": 11.0},
            {"session": "2010-01-05", "symbol": "QQQ", "adjusted_close": 46.0},
        ]
    )


def _request(tmp_path: Path, prices: pd.DataFrame | None = None, **overrides: object) -> object:
    return snapshot.SnapshotRequest(prices=prices if prices is not None else _fixture_prices(), output_dir=tmp_path / "snapshot", **overrides)


def test_materialize_accepts_only_typed_request_and_writes_one_canonical_envelope(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))

    assert tuple(path.name for path in result.output_dir.iterdir()) == ("snapshot.json",)
    envelope = json.loads((result.output_dir / "snapshot.json").read_text(encoding="utf-8"))
    assert envelope["contract_version"] == "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
    assert envelope["calendar"] == "XNYS.regular.v1"
    assert envelope["request"] == {"mode": "core_only", "plugin": "ABSENT_DISABLED", "size": 0}
    assert envelope["snapshot_identity"] == result.snapshot_identity
    assert snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_snapshot_sha256=result.snapshot_sha256) == result


def test_materialize_rejects_untyped_legacy_arguments(tmp_path: Path) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="SnapshotRequest"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")


def test_admission_rejects_invalid_request_before_destination_io(tmp_path: Path) -> None:
    output_dir = tmp_path / "not-created" / "snapshot"

    with pytest.raises(snapshot.SnapshotValidationError, match="size must be zero"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path / "not-created", size=1))

    assert not output_dir.parent.exists()


def test_admission_rejects_non_xnys_regular_session(tmp_path: Path) -> None:
    prices = _fixture_prices().replace("2010-01-05", "2010-04-02")

    with pytest.raises(snapshot.SnapshotValidationError, match="XNYS regular session"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path, prices))

    prices = _fixture_prices().replace("2010-01-05", "2010-12-31")
    with pytest.raises(snapshot.SnapshotValidationError, match="XNYS regular session"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path, prices))


@pytest.mark.parametrize("closed_session", ["2012-10-29", "2012-10-30", "2018-12-05", "2025-01-09"])
def test_admission_rejects_exceptional_xnys_closure(tmp_path: Path, closed_session: str) -> None:
    prices = _fixture_prices().replace("2010-01-05", closed_session)

    with pytest.raises(snapshot.SnapshotValidationError, match="XNYS regular session"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path, prices))


@pytest.mark.parametrize("dtype", ["datetime", "timedelta"])
def test_admission_rejects_datetime_like_adjusted_close(tmp_path: Path, dtype: str) -> None:
    values = pd.to_datetime(["2010-01-04", "2010-01-04", "2010-01-05", "2010-01-05"])
    adjusted_close = values if dtype == "datetime" else values - values[0]
    prices = _fixture_prices().assign(adjusted_close=adjusted_close)

    with pytest.raises(snapshot.SnapshotValidationError, match="datetime-like adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path, prices))


def test_admission_rejects_row_count_above_explicit_bound(tmp_path: Path) -> None:
    prices = pd.concat([_fixture_prices()] * (snapshot.MAX_ROW_COUNT // 4 + 1), ignore_index=True)

    with pytest.raises(snapshot.SnapshotValidationError, match="row count"):
        snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path, prices))


def test_readback_recomputes_snapshot_identity_even_with_new_trusted_file_hash(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))
    envelope_path = result.output_dir / "snapshot.json"
    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    envelope["records"][0]["adjusted_close"] = "999"
    envelope_path.write_text(json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    forged_file_hash = hashlib.sha256(envelope_path.read_bytes()).hexdigest()

    with pytest.raises(snapshot.SnapshotValidationError, match="snapshot identity mismatch"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_snapshot_sha256=forged_file_hash)


@pytest.mark.parametrize("field_value", [0.0, False])
def test_readback_rejects_non_exact_request_size_type(tmp_path: Path, field_value: object) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))
    envelope_path = result.output_dir / "snapshot.json"
    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    envelope["request"]["size"] = field_value
    content = json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
    envelope_path.write_text(content, encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid snapshot envelope"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir, expected_snapshot_sha256=hashlib.sha256(content.encode()).hexdigest()
        )


def test_readback_rejects_non_exact_row_count_type(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))
    envelope_path = result.output_dir / "snapshot.json"
    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    envelope["row_count"] = float(envelope["row_count"])
    content = json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
    envelope_path.write_text(content, encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid snapshot envelope"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir, expected_snapshot_sha256=hashlib.sha256(content.encode()).hexdigest()
        )


def test_readback_requires_exact_single_regular_file_and_size_bound(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))
    (result.output_dir / "extra").write_text("not allowed", encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="unexpected output files"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_snapshot_sha256=result.snapshot_sha256)


@pytest.mark.parametrize("kind", ["symlink", "oversized"])
def test_readback_rejects_non_regular_or_oversized_envelope(tmp_path: Path, kind: str) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))
    envelope_path = result.output_dir / "snapshot.json"
    if kind == "symlink":
        target = tmp_path / "target.json"
        target.write_bytes(envelope_path.read_bytes())
        envelope_path.unlink()
        envelope_path.symlink_to(target)
    else:
        envelope_path.write_bytes(b"x" * (snapshot.MAX_ENVELOPE_BYTES + 1))

    with pytest.raises(snapshot.SnapshotValidationError, match="bounded regular non-symlink file"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_snapshot_sha256=result.snapshot_sha256)


def test_publish_fsyncs_temporary_file_and_parent_directory_before_and_after_atomic_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fsync_calls: list[int] = []
    replace_calls: list[tuple[Path, Path]] = []
    original_replace = snapshot.os.replace

    monkeypatch.setattr(snapshot.os, "fsync", lambda fd: fsync_calls.append(fd))

    def recording_replace(source: str | Path, destination: str | Path) -> None:
        replace_calls.append((Path(source), Path(destination)))
        original_replace(source, destination)

    monkeypatch.setattr(snapshot.os, "replace", recording_replace)
    snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))

    assert len(fsync_calls) >= 3
    assert replace_calls[0][0].parent == replace_calls[0][1].parent


def test_publish_atomically_renames_sibling_staging_root_and_fsyncs_destination_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    replace_calls: list[tuple[Path, Path]] = []
    fsync_directories: list[Path] = []
    original_replace = snapshot.os.replace
    original_fsync_directory = snapshot._fsync_directory

    def recording_replace(source: str | Path, destination: str | Path) -> None:
        replace_calls.append((Path(source), Path(destination)))
        original_replace(source, destination)

    def recording_fsync_directory(directory: Path) -> None:
        fsync_directories.append(directory)
        original_fsync_directory(directory)

    monkeypatch.setattr(snapshot.os, "replace", recording_replace)
    monkeypatch.setattr(snapshot, "_fsync_directory", recording_fsync_directory)
    result = snapshot.materialize_tqqq_r1_snapshot(_request(tmp_path))

    source, destination = replace_calls[0]
    assert source.parent == result.output_dir.parent
    assert destination == result.output_dir
    assert result.output_dir.parent in fsync_directories
