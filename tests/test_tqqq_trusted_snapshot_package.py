from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot
from us_equity_snapshot_pipelines import tqqq_trusted_snapshot_package as package


def _prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.25},
            {"session": "2010-01-04", "symbol": "TQQQ", "adjusted_close": 10.5},
            {"session": "2010-01-05", "symbol": "QQQ", "adjusted_close": 46.0},
            {"session": "2010-01-05", "symbol": "TQQQ", "adjusted_close": 11.0},
        ]
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bound_package(tmp_path: Path, *, session: str = "2010-01-05") -> tuple[Path, Path, Path, Path, str]:
    snapshot_result = snapshot.materialize_tqqq_r1_snapshot(_prices(), tmp_path / "snapshot")
    calendar_path = tmp_path / "calendar.json"
    package.write_strict_json(
        calendar_path,
        {
            "contract_version": "tqqq_offline_calendar_evidence.v1",
            "calendar": "XNYS",
            "sessions": ["2010-01-04", session],
        },
    )
    receipt_path = tmp_path / "receipt.json"
    package.write_strict_json(
        receipt_path,
        {
            "contract_version": "tqqq_snapshot_receipt.v1",
            "snapshot_manifest_sha256": snapshot_result.manifest_sha256,
            "calendar_sha256": _sha256(calendar_path),
            "session": session,
        },
    )
    manifest_path = tmp_path / "package-manifest.json"
    package.write_strict_json(
        manifest_path,
        {
            "contract_version": "tqqq_trusted_snapshot_package.v1",
            "snapshot_manifest_sha256": snapshot_result.manifest_sha256,
            "receipt_sha256": _sha256(receipt_path),
            "calendar_sha256": _sha256(calendar_path),
            "session": session,
        },
    )
    return snapshot_result.output_dir, manifest_path, receipt_path, calendar_path, snapshot_result.manifest_sha256


def _load(
    snapshot_dir: Path,
    manifest_path: Path,
    receipt_path: Path,
    calendar_path: Path,
    snapshot_manifest_sha256: str,
    *,
    package_manifest_sha256: str | None = None,
    receipt_sha256: str | None = None,
) -> package.TrustedSnapshotPackage:
    return package.load_verified_trusted_snapshot_package(
        snapshot_dir,
        manifest_path,
        receipt_path,
        calendar_path,
        expected_snapshot_manifest_sha256=snapshot_manifest_sha256,
        expected_package_manifest_sha256=package_manifest_sha256 or _sha256(manifest_path),
        expected_receipt_sha256=receipt_sha256 or _sha256(receipt_path),
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_strict_json_writer_rejects_non_finite_numbers(tmp_path: Path, value: float) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="non-finite"):
        package.write_strict_json(tmp_path / "payload.json", {"value": value})


@pytest.mark.parametrize("payload", [b'{"value":NaN}', b'{"value":Infinity}', b'{"value":-Infinity}'])
def test_strict_json_reader_rejects_non_finite_numbers(payload: bytes) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid strict JSON"):
        package.read_strict_json(payload, "payload")


def test_strict_json_reader_rejects_overflowed_float() -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid strict JSON"):
        package.read_strict_json(b'{"value":1e400}', "x")


def test_verified_loader_preserves_existing_weekend_rejection(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path, session="2010-01-03")

    with pytest.raises(package.TrustedSnapshotPackageError, match="weekday"):
        _load(*args)


def test_public_verified_loader_returns_only_verified_package(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)

    trusted = _load(*args)

    assert isinstance(trusted, package.TrustedSnapshotPackage)
    assert trusted.session == "2010-01-05"
    assert trusted.snapshot_dir.is_absolute()
    assert trusted.snapshot_manifest_sha256 == snapshot.verify_tqqq_r1_snapshot(
        args[0],
        expected_manifest_sha256=trusted.snapshot_manifest_sha256,
    ).manifest_sha256
    with pytest.raises(TypeError):
        package.TrustedSnapshotPackage()  # type: ignore[call-arg]


def test_loader_requires_external_package_manifest_and_receipt_anchors(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    snapshot_dir, manifest_path, receipt_path, calendar_path, snapshot_manifest_sha256 = args
    package_manifest_sha256 = _sha256(manifest_path)
    receipt_sha256 = _sha256(receipt_path)
    package.write_strict_json(
        calendar_path,
        {
            "contract_version": "tqqq_offline_calendar_evidence.v1",
            "calendar": "XNYS",
            "sessions": ["2010-01-04", "2010-01-05", "2010-01-06"],
        },
    )
    receipt = package.read_strict_json(receipt_path.read_bytes(), "receipt")
    assert isinstance(receipt, dict)
    receipt["calendar_sha256"] = _sha256(calendar_path)
    package.write_strict_json(receipt_path, receipt)
    manifest = package.read_strict_json(manifest_path.read_bytes(), "package manifest")
    assert isinstance(manifest, dict)
    manifest["calendar_sha256"] = _sha256(calendar_path)
    manifest["receipt_sha256"] = _sha256(receipt_path)
    package.write_strict_json(manifest_path, manifest)

    with pytest.raises(package.TrustedSnapshotPackageError, match="package manifest hash"):
        _load(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            snapshot_manifest_sha256,
            package_manifest_sha256=package_manifest_sha256,
            receipt_sha256=receipt_sha256,
        )


def test_loader_hashes_the_same_evidence_bytes_it_validates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _write_bound_package(tmp_path)
    reads: dict[Path, int] = {}
    read_regular = package._read_regular

    def count_evidence_reads(path: str | Path, name: str) -> bytes:
        source = Path(path)
        if source in set(args[1:4]):
            reads[source] = reads.get(source, 0) + 1
            if reads[source] > 1:
                raise AssertionError(f"evidence reread: {source.name}")
        return read_regular(path, name)

    monkeypatch.setattr(package, "_read_regular", count_evidence_reads)

    _load(*args)

    assert reads == {path: 1 for path in args[1:4]}


def test_verified_package_stores_resolved_snapshot_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _write_bound_package(tmp_path)
    monkeypatch.chdir(tmp_path)

    trusted = _load(Path("snapshot"), *args[1:])

    assert trusted.snapshot_dir.is_absolute()
    assert b"45.25" in trusted.read_snapshot_member("prices.csv")


def test_snapshot_validation_failures_are_normalized(tmp_path: Path) -> None:
    _, manifest_path, receipt_path, calendar_path, _ = _write_bound_package(tmp_path)

    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid verified snapshot"):
        _load(tmp_path / "missing-snapshot", manifest_path, receipt_path, calendar_path, "0" * 64)


def test_strict_json_reader_normalizes_oversized_integer(tmp_path: Path) -> None:
    payload = b'{"value":' + (b"7" * 5000) + b"}"

    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid strict JSON"):
        package.read_strict_json(payload, "oversized")


def test_loader_authenticates_evidence_before_decoding(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    snapshot_dir, manifest_path, receipt_path, calendar_path, snapshot_manifest_sha256 = args
    receipt_sha256 = _sha256(receipt_path)
    receipt_path.write_bytes(b'{"value":' + (b"8" * 5000) + b"}")

    with pytest.raises(package.TrustedSnapshotPackageError, match="receipt hash binding mismatch"):
        _load(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            snapshot_manifest_sha256,
            receipt_sha256=receipt_sha256,
        )


def test_loader_keeps_verified_snapshot_directory_stable_after_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    snapshot_dir, manifest_path, receipt_path, calendar_path, snapshot_manifest_sha256 = args
    verify = package.tqqq_r1_snapshot.verify_tqqq_r1_snapshot_fd

    def verify_then_replace(directory_fd: int, *, expected_manifest_sha256: str):
        result = verify(directory_fd, expected_manifest_sha256=expected_manifest_sha256)
        original = Path(snapshot_dir)
        original.rename(tmp_path / "verified-original")
        replacement = original
        replacement.mkdir()
        (replacement / "prices.csv").write_text(
            "session,symbol,adjusted_close\n2010-01-05,QQQ,999\n",
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(package.tqqq_r1_snapshot, "verify_tqqq_r1_snapshot_fd", verify_then_replace)

    trusted = _load(args[0], *args[1:])

    assert trusted.snapshot_dir.is_absolute()
    assert b"999" not in trusted.read_snapshot_member("prices.csv")


def test_package_preserves_verified_member_bytes_after_member_replacement(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    trusted = _load(*args)
    (args[0] / "prices.csv").write_text(
        "session,symbol,adjusted_close\n2010-01-05,QQQ,999\n",
        encoding="utf-8",
    )

    assert b"999" not in trusted.read_snapshot_member("prices.csv")


def test_snapshot_members_are_opened_nonblocking(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _write_bound_package(tmp_path)
    open_file = snapshot.os.open

    def require_nonblocking(path: str | bytes | Path, flags: int, *values: object, **options: object) -> int:
        if path in snapshot.OUTPUT_FILENAMES:
            assert flags & snapshot.os.O_NONBLOCK
        return open_file(path, flags, *values, **options)

    monkeypatch.setattr(snapshot.os, "open", require_nonblocking)

    _load(*args)


def test_strict_json_writer_does_not_follow_raced_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "target.json"
    target.write_text("unchanged\n", encoding="utf-8")
    destination = tmp_path / "destination.json"
    destination.symlink_to(target)
    monkeypatch.setattr(Path, "is_symlink", lambda _self: False)

    with pytest.raises(package.TrustedSnapshotPackageError, match="unable to write strict JSON"):
        package.write_strict_json(destination, {"value": 1})

    assert target.read_text(encoding="utf-8") == "unchanged\n"
