from __future__ import annotations

import copy
import hashlib
import inspect
import json
import os
import pickle
import stat
import threading
from pathlib import Path
from typing import Any

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


def _canonical_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


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
    package_manifest_path = tmp_path / "package-manifest.json"
    package.write_strict_json(
        package_manifest_path,
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
        package_manifest_path,
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
        package_manifest_path,
        receipt_path,
        calendar_path,
        snapshot_manifest_sha256,
    ) = args
    assert isinstance(package_manifest_path, Path)
    assert isinstance(receipt_path, Path)
    return package.load_verified_trusted_snapshot_package(
        snapshot_dir,
        package_manifest_path,
        receipt_path,
        calendar_path,
        expected_snapshot_manifest_sha256=snapshot_manifest_sha256,
        expected_package_manifest_sha256=(
            package_manifest_sha256 or _sha256(package_manifest_path)
        ),
        expected_receipt_sha256=receipt_sha256 or _sha256(receipt_path),
    )


def _rewrite_with_implicit_index_session_split(
    args: tuple[Path, Path, Path, Path, str],
) -> tuple[Path, Path, Path, Path, str]:
    snapshot_dir, package_manifest_path, receipt_path, calendar_path, _ = args
    prices_path = snapshot_dir / "prices.csv"
    prices_path.write_bytes(
        b"session,symbol,adjusted_close\n"
        b"2010-01-05,2010-01-04,QQQ,45.25\n"
        b"2010-01-05,2010-01-04,TQQQ,10.5\n"
    )
    validation_path = snapshot_dir / "validation.json"
    _canonical_json(
        validation_path,
        {"valid": True, "row_count": 2, "symbols": ["QQQ", "TQQQ"]},
    )
    manifest_path = snapshot_dir / "manifest.json"
    _canonical_json(
        manifest_path,
        {
            "contract_version": snapshot.CONTRACT_VERSION,
            "symbols": list(snapshot.SYMBOLS),
            "requested_lower_bound": snapshot.REQUESTED_LOWER_BOUND,
            "price_field": snapshot.PRICE_FIELD,
            "plugin": snapshot.PLUGIN,
            "mode": snapshot.MODE,
            "size": 0,
            "row_count": 2,
            "prices_sha256": _sha256(prices_path),
        },
    )
    _canonical_json(
        snapshot_dir / "sha256sums.json",
        {
            "prices.csv": _sha256(prices_path),
            "manifest.json": _sha256(manifest_path),
            "validation.json": _sha256(validation_path),
        },
    )
    snapshot_manifest_sha256 = _sha256(manifest_path)
    package.write_strict_json(
        receipt_path,
        {
            "contract_version": "tqqq_snapshot_receipt.v1",
            "snapshot_manifest_sha256": snapshot_manifest_sha256,
            "calendar_sha256": _sha256(calendar_path),
            "session": "2010-01-05",
        },
    )
    package.write_strict_json(
        package_manifest_path,
        {
            "contract_version": "tqqq_trusted_snapshot_package.v1",
            "snapshot_manifest_sha256": snapshot_manifest_sha256,
            "receipt_sha256": _sha256(receipt_path),
            "calendar_sha256": _sha256(calendar_path),
            "session": "2010-01-05",
        },
    )
    return (
        snapshot_dir,
        package_manifest_path,
        receipt_path,
        calendar_path,
        snapshot_manifest_sha256,
    )


def test_receipt_session_comes_from_single_verified_pandas_parse(
    tmp_path: Path,
) -> None:
    args = _rewrite_with_implicit_index_session_split(
        _write_bound_package(tmp_path),
    )

    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="snapshot session binding mismatch",
    ):
        _load(*args)


def test_raw_parent_component_is_rejected_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "base"
    args = _write_bound_package(base)
    target_parent = tmp_path / "target-parent"
    (target_parent / "nested").mkdir(parents=True)
    (base / "link").symlink_to(target_parent / "nested", target_is_directory=True)
    raw = os.fspath(base / "link") + os.sep + ".." + os.sep + "snapshot"
    opened: list[str] = []
    real_open = package.os.open

    def observe(path: str, flags: int, *open_args: object, **kwargs: object) -> int:
        opened.append(os.fspath(path))
        return real_open(path, flags, *open_args, **kwargs)

    monkeypatch.setattr(package.os, "open", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid snapshot directory",
    ):
        _load(raw, *args[1:])

    assert raw not in opened


def test_verified_snapshot_returns_canonical_session_frozenset(
    tmp_path: Path,
) -> None:
    args = _write_bound_package(tmp_path)
    descriptor = package._open_snapshot_directory(args[0])
    try:
        manifest_sha256, members, sessions = package._verify_snapshot_fd(
            descriptor,
            expected_manifest_sha256=args[4],
        )
    finally:
        os.close(descriptor)

    assert manifest_sha256 == args[4]
    assert dict(members)["prices.csv"].startswith(b"session,symbol")
    assert sessions == frozenset({"2010-01-04", "2010-01-05"})
    assert type(sessions) is frozenset


def test_loader_performs_one_pandas_parse_and_no_independent_csv_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    calls = 0
    read_csv = package.pd.read_csv

    def observe(*args: object, **kwargs: object) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return read_csv(*args, **kwargs)

    monkeypatch.setattr(package.pd, "read_csv", observe)
    trusted = _load(*args)
    try:
        assert calls == 1
    finally:
        trusted.close()

    source = inspect.getsource(package)
    assert "csv.DictReader" not in source
    assert "import csv" not in source


@pytest.mark.parametrize(
    "raw",
    [
        "../snapshot",
        "base/../snapshot",
        "base//../snapshot",
        "base/..",
        "base/../../snapshot",
        "base/.././snapshot",
    ],
)
def test_all_parent_components_fail_before_open(
    raw: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    def observe(*_args: object, **_kwargs: object) -> int:
        nonlocal opened
        opened = True
        raise AssertionError("ambiguous path must be rejected before open")

    monkeypatch.setattr(package.os, "open", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid snapshot directory",
    ):
        package._open_snapshot_directory(raw)
    assert opened is False


@pytest.mark.parametrize("suffix", [os.sep, os.sep + "."])
def test_terminal_path_ambiguity_fails_before_open(
    suffix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    def observe(*_args: object, **_kwargs: object) -> int:
        nonlocal opened
        opened = True
        raise AssertionError("terminal ambiguity must be rejected before open")

    monkeypatch.setattr(package.os, "open", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid snapshot directory",
    ):
        package._open_snapshot_directory("snapshot" + suffix)
    assert opened is False


def test_allowed_relative_path_is_forwarded_without_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "snapshot").mkdir()
    monkeypatch.chdir(tmp_path)
    raw = "." + os.sep + "snapshot"
    observed: list[str] = []
    real_open = package.os.open

    def observe(path: str, flags: int, *args: object, **kwargs: object) -> int:
        observed.append(os.fspath(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(package.os, "open", observe)
    descriptor = package._open_snapshot_directory(raw)
    os.close(descriptor)
    assert observed == [raw]
    source = inspect.getsource(package)
    assert "normpath" not in source
    assert "realpath" not in source
    assert ".resolve(" not in source


def test_loader_accepts_relative_snapshot_root_and_preserves_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    relative = Path("snapshot")

    trusted = _load(relative, *args[1:])
    try:
        assert trusted.snapshot_dir == relative
        assert not str(trusted.snapshot_dir).startswith("/proc/self/fd/")
    finally:
        trusted.close()


@pytest.mark.parametrize(
    "root_form",
    [
        lambda path: path,
        lambda path: os.fspath(path) + os.sep,
        lambda path: os.fspath(path) + os.sep + ".",
    ],
    ids=["plain", "trailing-separator", "trailing-dot"],
)
def test_loader_rejects_all_final_symlink_snapshot_root_forms(
    tmp_path: Path,
    root_form: Any,
) -> None:
    args = _write_bound_package(tmp_path)
    link = tmp_path / "snapshot-link"
    link.symlink_to(args[0], target_is_directory=True)

    with pytest.raises(package.TrustedSnapshotPackageError):
        _load(root_form(link), *args[1:])


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


def test_strict_json_exact_size_boundary_and_deep_recursion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b" " * (package.MAX_EVIDENCE_BYTES - 2) + b"{}"
    assert package.read_strict_json(payload, "boundary") == {}

    def recurse(*_args: object, **_kwargs: object) -> object:
        raise RecursionError("deep JSON")

    monkeypatch.setattr(package.json, "loads", recurse)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="invalid strict JSON",
    ):
        package.read_strict_json(b"{}", "deep")


def test_write_strict_json_rejects_nonfinite_and_destination_symlink(
    tmp_path: Path,
) -> None:
    with pytest.raises(package.TrustedSnapshotPackageError):
        package.write_strict_json(tmp_path / "nan.json", {"x": float("nan")})

    target = tmp_path / "target.json"
    target.write_text("unchanged", encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(package.TrustedSnapshotPackageError):
        package.write_strict_json(link, {"x": 1})
    assert target.read_text(encoding="utf-8") == "unchanged"


def test_external_snapshot_anchor_is_checked_before_snapshot_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    descriptor = package._open_snapshot_directory(args[0])

    def fail_if_parsed(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("untrusted snapshot must not be parsed")

    monkeypatch.setattr(package.pd, "read_csv", fail_if_parsed)
    try:
        with pytest.raises(
            package.TrustedSnapshotPackageError,
            match="manifest hash mismatch",
        ):
            package._verify_snapshot_fd(
                descriptor,
                expected_manifest_sha256="0" * 64,
            )
    finally:
        os.close(descriptor)


def test_package_and_receipt_anchors_are_checked_before_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    parsed_names: list[str] = []
    parser = package.read_strict_json

    def observe(payload: bytes, name: str) -> object:
        parsed_names.append(name)
        return parser(payload, name)

    monkeypatch.setattr(package, "read_strict_json", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="package manifest hash binding mismatch",
    ):
        _load(*args, package_manifest_sha256="0" * 64)
    assert "package manifest" not in parsed_names
    assert "receipt" not in parsed_names
    parsed_names.clear()

    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="receipt hash binding mismatch",
    ):
        _load(*args, receipt_sha256="0" * 64)
    assert "package manifest" not in parsed_names
    assert "receipt" not in parsed_names


def test_calendar_digest_mismatch_never_calls_calendar_parser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    args[3].write_bytes(
        b'{"deep":' + b"[" * 1200 + b"0" + b"]" * 1200 + b"}",
    )
    parser = package.read_strict_json
    parsed_names: list[str] = []

    def observe(payload: bytes, name: str) -> object:
        parsed_names.append(name)
        return parser(payload, name)

    monkeypatch.setattr(package, "read_strict_json", observe)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="calendar hash binding mismatch",
    ):
        _load(*args)
    assert "calendar evidence" not in parsed_names


def test_package_keeps_descriptor_stable_verified_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _write_bound_package(tmp_path)
    verify = package._verify_snapshot_fd

    def replace_after_verify(
        directory_fd: int,
        *,
        expected_manifest_sha256: str,
    ) -> tuple[str, tuple[tuple[str, bytes], ...], frozenset[str]]:
        result = verify(
            directory_fd,
            expected_manifest_sha256=expected_manifest_sha256,
        )
        original = args[0].with_name("snapshot-original")
        args[0].rename(original)
        args[0].mkdir()
        (args[0] / "prices.csv").write_text("replaced\n", encoding="utf-8")
        return result

    monkeypatch.setattr(package, "_verify_snapshot_fd", replace_after_verify)
    trusted = _load(*args)
    try:
        assert trusted.snapshot_dir == args[0]
        assert b"45.25" in trusted.read_snapshot_member("prices.csv")
    finally:
        trusted.close()


def test_concurrent_close_claims_descriptor_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted = _load(*_write_bound_package(tmp_path))
    descriptor = trusted._snapshot_fd
    real_close = package.os.close
    calls: list[int] = []

    def observe(value: int) -> None:
        if value == descriptor:
            calls.append(value)
        real_close(value)

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
    outcomes: list[bytes | Exception] = []
    start = threading.Barrier(2)

    def read() -> None:
        start.wait()
        try:
            outcomes.append(trusted.read_snapshot_member("prices.csv"))
        except package.TrustedSnapshotPackageError as exc:
            outcomes.append(exc)

    reader = threading.Thread(target=read)
    reader.start()
    start.wait()
    trusted.close()
    reader.join()
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], (bytes, package.TrustedSnapshotPackageError))


def test_package_lifecycle_is_idempotent_nontransferable_and_repr_safe(
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
    with pytest.raises(package.TrustedSnapshotPackageError, match="closed"):
        trusted.read_snapshot_member("prices.csv")


def test_unverified_direct_construction_is_rejected() -> None:
    with pytest.raises(package.TrustedSnapshotPackageError):
        package.TrustedSnapshotPackage(
            _verified=object(),
            snapshot_dir=Path("snapshot"),
            session="2010-01-05",
            snapshot_manifest_sha256="0" * 64,
            receipt_sha256="0" * 64,
            calendar_sha256="0" * 64,
            snapshot_fd=-1,
            snapshot_members=(),
        )


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
            values[stat.ST_SIZE] += 1
            return os.stat_result(values)
        return actual

    monkeypatch.setattr(package.os, "fstat", changed_size)
    with pytest.raises(
        package.TrustedSnapshotPackageError,
        match="changed while reading",
    ):
        package._read_regular(evidence, "evidence")


def test_snapshot_member_size_limit_rejects_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = tmp_path / "prices.csv"
    oversized.touch()
    os.truncate(oversized, package._MAX_SNAPSHOT_MEMBER_BYTES + 1)
    directory_fd = package._open_snapshot_directory(tmp_path)

    def fail_if_read(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("oversized snapshot member must not be read")

    monkeypatch.setattr(package.os, "read", fail_if_read)
    try:
        with pytest.raises(
            package.TrustedSnapshotPackageError,
            match="snapshot member exceeds size limit",
        ):
            package._read_snapshot_member(directory_fd, "prices.csv")
    finally:
        os.close(directory_fd)


def test_loader_rejects_snapshot_member_symlink_and_fifo(tmp_path: Path) -> None:
    args = _write_bound_package(tmp_path / "symlink")
    target = tmp_path / "prices-target.csv"
    prices_path = args[0] / "prices.csv"
    target.write_bytes(prices_path.read_bytes())
    prices_path.unlink()
    prices_path.symlink_to(target)
    with pytest.raises(package.TrustedSnapshotPackageError):
        _load(*args)

    args = _write_bound_package(tmp_path / "fifo")
    prices_path = args[0] / "prices.csv"
    prices_path.unlink()
    os.mkfifo(prices_path)
    with pytest.raises(package.TrustedSnapshotPackageError):
        _load(*args)


def test_posix_linux_macos_capability_matrix() -> None:
    if os.name != "posix":
        pytest.skip("Linux/macOS capability check")
    assert getattr(os, "O_NOFOLLOW", 0)
    assert getattr(os, "O_NONBLOCK", 0)
    assert getattr(os, "O_DIRECTORY", 0)
    assert package._safe_open_flags(directory=True) & os.O_DIRECTORY
    assert package._safe_open_flags() & os.O_NOFOLLOW


@pytest.mark.parametrize(
    ("attribute", "directory"),
    [("O_NOFOLLOW", False), ("O_NONBLOCK", False), ("O_DIRECTORY", True)],
)
def test_missing_descriptor_capability_fails_closed(
    attribute: str,
    directory: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(package.os, attribute, 0)
    with pytest.raises(package.TrustedSnapshotPackageError, match="capability"):
        package._safe_open_flags(directory=directory)


def test_unsupported_platform_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(package.os, "name", "nt")
    with pytest.raises(package.TrustedSnapshotPackageError, match="capability"):
        package._safe_open_flags()


def test_module_has_no_provider_data_or_live_dependencies() -> None:
    source = inspect.getsource(package)
    forbidden = (
        "import requests",
        "import yfinance",
        "from google.cloud",
        "submit_order",
        "broker_client",
        "live_position",
    )
    assert all(name not in source for name in forbidden)
