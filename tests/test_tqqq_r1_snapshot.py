from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot


def _prices(*, session: str = "2010-01-04", value: object = 10.5) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"session": session, "symbol": "QQQ", "adjusted_close": value},
            {"session": session, "symbol": "TQQQ", "adjusted_close": value},
        ]
    )


def _request(**changes: object) -> object:
    values: dict[str, object] = {"prices": _prices()}
    values.update(changes)
    return snapshot.TqqqR1SnapshotRequest(**values)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_envelope(path: Path) -> dict[str, object]:
    return json.loads((path / "snapshot.json").read_text(encoding="utf-8"))


def _rewrite_envelope(path: Path, envelope: dict[str, object]) -> str:
    without_identity = dict(envelope)
    without_identity.pop("snapshot_identity", None)
    envelope["snapshot_identity"] = hashlib.sha256(_canonical_bytes(without_identity)).hexdigest()
    raw = _canonical_bytes(envelope) + b"\n"
    (path / "snapshot.json").write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


# 1. Typed, bounded local request admission.
def test_admission_requires_exact_typed_bounded_request_before_io(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(snapshot.tempfile, "mkdtemp", lambda **_: pytest.fail("allocation must not occur"))

    with pytest.raises(snapshot.SnapshotValidationError, match="typed request"):
        snapshot.materialize_tqqq_r1_snapshot(_prices(), tmp_path / "snapshot")
    with pytest.raises(snapshot.SnapshotValidationError, match="row limit"):
        snapshot.materialize_tqqq_r1_snapshot(_request(prices=pd.concat([_prices()] * 5001)), tmp_path / "snapshot")


# 2. The legacy multi-file layout is not a valid compatibility path.
def test_publish_and_readback_use_only_the_canonical_envelope_and_reject_legacy(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(), tmp_path / "snapshot")

    assert [member.name for member in result.output_dir.iterdir()] == ["snapshot.json"]
    assert snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256) == result
    (result.output_dir / "manifest.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(snapshot.SnapshotValidationError, match="canonical envelope"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


# 3. XNYS recurring holidays and exceptional closures are both rejected.
@pytest.mark.parametrize("closed_session", ["2012-10-29", "2012-10-30", "2018-12-05", "2025-01-09"])
def test_admission_rejects_exceptional_xnys_closures(tmp_path: Path, closed_session: str) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="XNYS regular"):
        snapshot.materialize_tqqq_r1_snapshot(_request(prices=_prices(session=closed_session)), tmp_path / "snapshot")


def test_admission_accepts_friday_before_saturday_new_years_day(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(prices=_prices(session="2010-12-31")), tmp_path / "snapshot")
    assert result.output_dir.is_dir()


# 4. Datetime/timedelta values are rejected before pd.to_numeric can convert them.
@pytest.mark.parametrize("value", [pd.Timestamp("2026-01-01"), pd.Timedelta("1 day")])
def test_admission_rejects_datetime_like_adjusted_close(tmp_path: Path, value: object) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="datetime-like adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(_request(prices=_prices(value=value)), tmp_path / "snapshot")


# 5. Envelope scalars and containers have exact, not merely equality-compatible, types.
@pytest.mark.parametrize("field,value", [("size", False), ("row_count", 2.0), ("symbols", {"QQQ": "TQQQ"})])
def test_readback_rejects_non_exact_envelope_scalar_and_container_types(
    tmp_path: Path, field: str, value: object
) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(), tmp_path / "snapshot")
    envelope = _read_envelope(result.output_dir)
    target = envelope["request"] if field in {"size", "symbols"} else envelope
    assert isinstance(target, dict)
    target[field] = value
    digest = _rewrite_envelope(result.output_dir, envelope)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid canonical envelope"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=digest)


# 6. Invalid admission must happen before filesystem allocation or I/O.
def test_invalid_admission_does_not_create_destination_or_staging_root(tmp_path: Path) -> None:
    destination = tmp_path / "new" / "snapshot"
    with pytest.raises(snapshot.SnapshotValidationError, match="mode"):
        snapshot.materialize_tqqq_r1_snapshot(_request(mode="live"), destination)
    assert not destination.parent.exists()


# 7. Publication stages under a sibling and atomically renames the root only once complete.
def test_publish_uses_sibling_staging_directory_and_atomic_root_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / "snapshot"
    observed: list[tuple[Path, Path]] = []
    original_replace = os.replace

    def record_replace(source: str | bytes | os.PathLike[str] | os.PathLike[bytes], target: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> None:
        observed.append((Path(source), Path(target)))
        original_replace(source, target)

    monkeypatch.setattr(snapshot.os, "replace", record_replace)
    snapshot.materialize_tqqq_r1_snapshot(_request(), destination)

    assert observed == [(observed[0][0], destination)]
    assert observed[0][0].parent == destination.parent
    assert observed[0][0].name.startswith(".snapshot.")


# 8. Durability covers staged file/dir, installed parent, and newly-created ancestors.
def test_publish_fsyncs_staging_file_directory_destination_parent_and_new_ancestors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "new" / "nested" / "snapshot"
    fsync_calls: list[int] = []
    monkeypatch.setattr(snapshot.os, "fsync", lambda fd: fsync_calls.append(fd))

    snapshot.materialize_tqqq_r1_snapshot(_request(), destination)

    assert len(fsync_calls) >= 5


# 9. Readback uses no-follow regular-file descriptor, bounds bytes, and validates identity/digest canonicalization.
def test_readback_opens_canonical_file_no_follow_and_rejects_symlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(), tmp_path / "snapshot")
    flags: list[int] = []
    original_open = os.open

    def record_open(path: str | bytes | os.PathLike[str] | os.PathLike[bytes], flag: int, *args: object) -> int:
        if Path(path).name == "snapshot.json":
            flags.append(flag)
        return original_open(path, flag, *args)

    monkeypatch.setattr(snapshot.os, "open", record_open)
    snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)
    assert flags and flags[-1] & os.O_NOFOLLOW

    target = tmp_path / "elsewhere.json"
    target.write_bytes((result.output_dir / "snapshot.json").read_bytes())
    (result.output_dir / "snapshot.json").unlink()
    (result.output_dir / "snapshot.json").symlink_to(target)
    with pytest.raises(snapshot.SnapshotValidationError, match="regular"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_readback_rejects_bad_identity_and_oversized_envelope(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_request(), tmp_path / "snapshot")
    envelope = _read_envelope(result.output_dir)
    envelope["snapshot_identity"] = "0" * 64
    raw = _canonical_bytes(envelope) + b"\n"
    (result.output_dir / "snapshot.json").write_bytes(raw)

    with pytest.raises(snapshot.SnapshotValidationError, match="identity"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=hashlib.sha256(raw).hexdigest())

    oversized = b"x" * (snapshot.MAX_SNAPSHOT_BYTES + 1)
    (result.output_dir / "snapshot.json").write_bytes(oversized)
    with pytest.raises(snapshot.SnapshotValidationError, match="bounded regular"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=hashlib.sha256(oversized).hexdigest())
