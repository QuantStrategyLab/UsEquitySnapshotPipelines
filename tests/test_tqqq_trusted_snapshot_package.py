from __future__ import annotations

import copy
import hashlib
import os
import pickle
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
        {"contract_version": "tqqq_offline_calendar_evidence.v1", "calendar": "XNYS", "sessions": ["2010-01-04", session]},
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


@pytest.mark.parametrize("payload", [b'{"value":NaN}', b'{"value":1e400}', b'{"value":' + b"7" * 5000 + b"}"])
def test_strict_json_reader_normalizes_nonfinite_and_huge_numbers(payload: bytes) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid strict JSON"):
        package.read_strict_json(payload, "payload")


def test_loader_authenticates_evidence_before_decoding(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    receipt_sha256 = _sha256(args[2])
    args[2].write_bytes(b'{"value":' + b"8" * 5000 + b"}")

    with pytest.raises(package.TrustedSnapshotPackageError, match="receipt hash binding mismatch"):
        _load(*args, receipt_sha256=receipt_sha256)


def test_loader_keeps_verified_bytes_after_member_and_directory_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _write_bound_package(tmp_path)
    verified = package.tqqq_r1_snapshot.verify_tqqq_r1_snapshot_fd

    def verify_then_replace(directory_fd: int, *, expected_manifest_sha256: str):
        result = verified(directory_fd, expected_manifest_sha256=expected_manifest_sha256)
        args[0].rename(tmp_path / "verified-original")
        args[0].mkdir()
        (args[0] / "prices.csv").write_text("replaced\n", encoding="utf-8")
        return result

    monkeypatch.setattr(package.tqqq_r1_snapshot, "verify_tqqq_r1_snapshot_fd", verify_then_replace)
    trusted = _load(*args)

    assert b"45.25" in trusted.read_snapshot_member("prices.csv")
    assert "45.25" not in repr(trusted)


def test_loader_rejects_fifo_member_without_blocking(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    prices_path = args[0] / "prices.csv"
    prices_path.unlink()
    os.mkfifo(prices_path)

    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid verified snapshot"):
        _load(*args)


def test_loader_rejects_symlinked_evidence(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    target = tmp_path / "target.json"
    target.write_bytes(args[1].read_bytes())
    args[1].unlink()
    args[1].symlink_to(target)

    with pytest.raises(package.TrustedSnapshotPackageError, match="regular non-symlink"):
        _load(*args)


def test_verified_package_owns_descriptor_and_rejects_copy_pickle_and_hash(tmp_path: Path) -> None:
    trusted = _load(*_write_bound_package(tmp_path))
    before_close = trusted

    with pytest.raises(TypeError):
        hash(trusted)
    with pytest.raises(package.TrustedSnapshotPackageError):
        copy.copy(trusted)
    with pytest.raises(package.TrustedSnapshotPackageError):
        copy.deepcopy(trusted)
    with pytest.raises(package.TrustedSnapshotPackageError):
        pickle.dumps(trusted)

    trusted.close()
    trusted.close()
    assert trusted == before_close
    with pytest.raises(package.TrustedSnapshotPackageError, match="closed"):
        trusted.read_snapshot_member("prices.csv")


def test_loader_uses_single_nonblocking_regular_evidence_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _write_bound_package(tmp_path)
    open_file = package.os.open
    evidence_names = {os.fspath(path) for path in args[1:4]}
    observed: list[int] = []

    def observe(path: str | bytes | Path, flags: int, *values: object, **options: object) -> int:
        if os.fspath(path) in evidence_names:
            observed.append(flags)
        return open_file(path, flags, *values, **options)

    monkeypatch.setattr(package.os, "open", observe)
    _load(*args)

    assert len(observed) == 3
    assert all(flags & os.O_NOFOLLOW and flags & os.O_NONBLOCK for flags in observed)
