from __future__ import annotations

import copy
import hashlib
import os
import pickle
import threading
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


def _write_bound_package(
    tmp_path: Path,
    *,
    session: str = "2010-01-05",
) -> tuple[Path, Path, Path, Path, str]:
    snapshot_result = snapshot.materialize_tqqq_r1_snapshot(
        _prices(),
        tmp_path / "snapshot",
    )
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
    return (
        snapshot_result.output_dir,
        manifest_path,
        receipt_path,
        calendar_path,
        snapshot_result.manifest_sha256,
    )


def _load(
    *args: object,
    package_manifest_sha256: str | None = None,
    receipt_sha256: str | None = None,
) -> package.TrustedSnapshotPackage:
    (
        snapshot_dir,
        manifest_path,
        receipt_path,
        calendar_path,
        snapshot_manifest_sha256,
    ) = args
    return package.load_verified_trusted_snapshot_package(
        snapshot_dir,
        manifest_path,
        receipt_path,
        calendar_path,
        expected_snapshot_manifest_sha256=snapshot_manifest_sha256,
        expected_package_manifest_sha256=(package_manifest_sha256 or _sha256(manifest_path)),
        expected_receipt_sha256=receipt_sha256 or _sha256(receipt_path),
    )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"x":NaN}',
        b'{"x":1e400}',
        b'{"x":' + b"7" * 5000 + b"}",
        b'{"x":1,"x":2}',
        b"\xff",
    ],
)
def test_strict_json_normalizes_adversarial_inputs(payload: bytes) -> None:
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid strict JSON",
    ):
        package.read_strict_json(payload, "payload")


def test_strict_json_rejects_oversized_bytes_before_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parser_called = False

    def observe(*_args: object, **_kwargs: object) -> object:
        nonlocal parser_called
        parser_called = True
        return {}

    monkeypatch.setattr(package.json, "loads", observe)
    payload = b" " * (package.MAX_EVIDENCE_BYTES + 1)

    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="exceeds size limit",
    ):
        package.read_strict_json(payload, "oversized payload")

    assert parser_called is False


def test_strict_json_accepts_exact_size_boundary() -> None:
    payload = b" " * (package.MAX_EVIDENCE_BYTES - 2) + b"{}"

    assert len(payload) == package.MAX_EVIDENCE_BYTES
    assert package.read_strict_json(payload, "boundary payload") == {}


def test_authenticated_deep_json_recursion_error_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def recurse(*_args: object, **_kwargs: object) -> object:
        raise RecursionError("deep JSON")

    monkeypatch.setattr(package.json, "loads", recurse)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid strict JSON",
    ):
        package.read_strict_json(b'{"calendar":[]}', "calendar evidence")


def test_calendar_digest_mismatch_never_calls_calendar_parser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    args[3].write_bytes(
        b'{"deep":' + b"[" * 1200 + b"0" + b"]" * 1200 + b"}",
    )
    parse = package.read_strict_json
    parsed_names: list[str] = []

    def observe(payload: bytes, name: str) -> object:
        parsed_names.append(name)
        return parse(payload, name)

    monkeypatch.setattr(package, "read_strict_json", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="calendar hash binding mismatch",
    ):
        _load(*args)
    assert "calendar evidence" not in parsed_names


def test_package_keeps_caller_path_and_verified_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    verify = package._verify_snapshot_fd

    def replace_after_verify(
        directory_fd: int,
        *,
        expected_manifest_sha256: str,
    ) -> tuple[str, tuple[tuple[str, bytes], ...]]:
        result = verify(
            directory_fd,
            expected_manifest_sha256=expected_manifest_sha256,
        )
        (args[0] / "prices.csv").write_text("replaced\n", encoding="utf-8")
        return result

    monkeypatch.setattr(
        package,
        "_verify_snapshot_fd",
        replace_after_verify,
    )
    trusted = _load(*args)

    assert trusted.snapshot_dir == args[0]
    assert not str(trusted.snapshot_dir).startswith("/proc/self/fd/")
    assert b"45.25" in trusted.read_snapshot_member("prices.csv")
    trusted.close()


def test_loader_accepts_relative_snapshot_root_and_preserves_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    relative_snapshot_dir = Path("snapshot")

    trusted = _load(relative_snapshot_dir, *args[1:])

    assert trusted.snapshot_dir == relative_snapshot_dir
    trusted.close()


@pytest.mark.parametrize(
    "root_form",
    [
        lambda path: path,
        lambda path: os.fspath(path) + os.sep,
        lambda path: os.fspath(path) + os.sep + ".",
    ],
    ids=["path", "trailing-slash", "trailing-dot"],
)
def test_loader_rejects_all_final_symlink_snapshot_root_forms(
    tmp_path: Path,
    root_form: object,
) -> None:
    args = _write_bound_package(tmp_path)
    link = tmp_path / "snapshot-link"
    link.symlink_to(args[0], target_is_directory=True)

    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="stable snapshot directory",
    ):
        _load(root_form(link), *args[1:])


def test_concurrent_close_claims_descriptor_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted = _load(*_write_bound_package(tmp_path))
    descriptor = trusted._snapshot_fd
    close = package.os.close
    calls: list[int] = []

    def observe(value: int) -> None:
        if value == descriptor:
            calls.append(value)
        close(value)

    monkeypatch.setattr(package.os, "close", observe)
    threads = [threading.Thread(target=trusted.close) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert calls == [descriptor]


def test_close_read_race_is_verified_bytes_or_closed_error(
    tmp_path: Path,
) -> None:
    trusted = _load(*_write_bound_package(tmp_path))
    outcome: list[bytes | Exception] = []
    start = threading.Barrier(2)

    def read() -> None:
        start.wait()
        try:
            outcome.append(trusted.read_snapshot_member("prices.csv"))
        except package.TrustedSnapshotPackageError as exc:
            outcome.append(exc)

    reader = threading.Thread(target=read)
    reader.start()
    start.wait()
    trusted.close()
    reader.join()
    assert len(outcome) == 1
    assert isinstance(
        outcome[0],
        (bytes, package.TrustedSnapshotPackageError),
    )


def test_package_close_is_idempotent_and_not_transferable(
    tmp_path: Path,
) -> None:
    trusted = _load(*_write_bound_package(tmp_path))

    assert "prices.csv" not in repr(trusted)
    assert b"45.25" not in repr(trusted).encode()
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
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="closed",
    ):
        trusted.read_snapshot_member("prices.csv")


def test_loader_rejects_symlink_fifo_and_oversized_evidence(
    tmp_path: Path,
) -> None:
    args = _write_bound_package(tmp_path)
    target = tmp_path / "target.json"
    target.write_bytes(args[1].read_bytes())
    args[1].unlink()
    args[1].symlink_to(target)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="regular non-symlink",
    ):
        _load(*args)

    args = _write_bound_package(tmp_path / "fifo")
    args[3].unlink()
    os.mkfifo(args[3])
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="regular non-symlink",
    ):
        _load(*args)

    args = _write_bound_package(tmp_path / "huge")
    args[3].write_bytes(b"x" * (package.MAX_EVIDENCE_BYTES + 1))
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="exceeds size limit",
    ):
        _load(*args)


def test_snapshot_member_size_limit_rejects_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = tmp_path / "prices.csv"
    oversized.touch()
    os.truncate(
        oversized,
        package._MAX_SNAPSHOT_MEMBER_BYTES + 1,
    )
    directory_fd = package._open_snapshot_directory(tmp_path)

    def fail_if_read(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError(
            "oversized snapshot member must not be read",
        )

    monkeypatch.setattr(package.os, "read", fail_if_read)
    try:
        with pytest.raises(
            package.TrustedSnapshotPackageError,
            match="snapshot member exceeds size limit",
        ):
            package._read_snapshot_member(
                directory_fd,
                "prices.csv",
            )
    finally:
        os.close(directory_fd)


def test_loader_rejects_snapshot_member_symlink(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path)
    target = tmp_path / "prices-target.csv"
    prices_path = args[0] / "prices.csv"
    target.write_bytes(prices_path.read_bytes())
    prices_path.unlink()
    prices_path.symlink_to(target)

    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid verified snapshot",
    ):
        _load(*args)


def test_reader_rejects_device_and_toctou_size_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="regular non-symlink",
    ):
        package._read_regular("/dev/null", "device evidence")

    evidence = tmp_path / "evidence.json"
    evidence.write_bytes(b"{}")
    fstat = package.os.fstat
    calls = 0

    def changed_size(descriptor: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        actual = fstat(descriptor)
        if calls == 2:
            values = list(actual)
            values[6] += 1
            return os.stat_result(values)
        return actual

    monkeypatch.setattr(package.os, "fstat", changed_size)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="changed while reading",
    ):
        package._read_regular(evidence, "evidence")


def test_unsupported_platform_capability_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(package.os, "name", "nt")
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="capability",
    ):
        package._safe_open_flags()
