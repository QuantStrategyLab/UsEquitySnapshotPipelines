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


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_strict_json_writer_rejects_non_finite_numbers(tmp_path: Path, value: float) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="non-finite"):
        package.write_strict_json(tmp_path / "payload.json", {"value": value})


@pytest.mark.parametrize("payload", [b'{"value":NaN}', b'{"value":Infinity}', b'{"value":-Infinity}'])
def test_strict_json_reader_rejects_non_finite_numbers(payload: bytes) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError, match="invalid strict JSON"):
        package.read_strict_json(payload, "payload")


def test_verified_loader_preserves_existing_weekend_rejection(tmp_path: Path) -> None:
    snapshot_dir, manifest_path, receipt_path, calendar_path, trusted_manifest_sha256 = _write_bound_package(tmp_path, session="2010-01-03")

    with pytest.raises(package.TrustedSnapshotPackageError, match="weekday"):
        package.load_verified_trusted_snapshot_package(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            expected_snapshot_manifest_sha256=trusted_manifest_sha256,
        )


def test_public_verified_loader_returns_only_verified_package(tmp_path: Path) -> None:
    snapshot_dir, manifest_path, receipt_path, calendar_path, trusted_manifest_sha256 = _write_bound_package(tmp_path)

    trusted = package.load_verified_trusted_snapshot_package(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            expected_snapshot_manifest_sha256=trusted_manifest_sha256,
        )

    assert isinstance(trusted, package.TrustedSnapshotPackage)
    assert trusted.session == "2010-01-05"
    assert trusted.snapshot_manifest_sha256 == snapshot.verify_tqqq_r1_snapshot(
        snapshot_dir,
        expected_manifest_sha256=trusted.snapshot_manifest_sha256,
    ).manifest_sha256
    with pytest.raises(TypeError):
        package.TrustedSnapshotPackage()  # type: ignore[call-arg]


def test_caller_cannot_substitute_calendar_provenance_after_receipt_is_accepted(tmp_path: Path) -> None:
    snapshot_dir, manifest_path, receipt_path, calendar_path, trusted_manifest_sha256 = _write_bound_package(tmp_path)
    package.write_strict_json(
        calendar_path,
        {
            "contract_version": "tqqq_offline_calendar_evidence.v1",
            "calendar": "XNYS",
            "sessions": ["2010-01-04", "2010-01-05", "2010-01-06"],
        },
    )

    with pytest.raises(package.TrustedSnapshotPackageError, match="calendar hash"):
        package.load_verified_trusted_snapshot_package(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            expected_snapshot_manifest_sha256=trusted_manifest_sha256,
        )


def test_accepted_offline_calendar_evidence_is_bound_to_session_and_package(tmp_path: Path) -> None:
    snapshot_dir, manifest_path, receipt_path, calendar_path, trusted_manifest_sha256 = _write_bound_package(tmp_path)
    package.write_strict_json(
        calendar_path,
        {
            "contract_version": "tqqq_offline_calendar_evidence.v1",
            "calendar": "XNYS",
            "sessions": ["2010-01-04"],
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

    with pytest.raises(package.TrustedSnapshotPackageError, match="calendar session binding"):
        package.load_verified_trusted_snapshot_package(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            expected_snapshot_manifest_sha256=trusted_manifest_sha256,
        )


def test_caller_cannot_substitute_receipt_snapshot_provenance(tmp_path: Path) -> None:
    snapshot_dir, manifest_path, receipt_path, calendar_path, trusted_manifest_sha256 = _write_bound_package(tmp_path)
    receipt = package.read_strict_json(receipt_path.read_bytes(), "receipt")
    assert isinstance(receipt, dict)
    receipt["snapshot_manifest_sha256"] = "0" * 64
    package.write_strict_json(receipt_path, receipt)
    manifest = package.read_strict_json(manifest_path.read_bytes(), "package manifest")
    assert isinstance(manifest, dict)
    manifest["receipt_sha256"] = _sha256(receipt_path)
    package.write_strict_json(manifest_path, manifest)

    with pytest.raises(package.TrustedSnapshotPackageError, match="snapshot manifest hash binding"):
        package.load_verified_trusted_snapshot_package(
            snapshot_dir,
            manifest_path,
            receipt_path,
            calendar_path,
            expected_snapshot_manifest_sha256=trusted_manifest_sha256,
        )
