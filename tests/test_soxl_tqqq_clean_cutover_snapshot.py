from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines import soxl_tqqq_clean_cutover_snapshot as snapshot


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
SESSIONS = ("2024-01-02", "2024-01-03")


def _bindings() -> snapshot.ExternalBindings:
    return snapshot.ExternalBindings(
        source_sha256=HASH_A,
        calendar_sha256=HASH_B,
        manifest_sha256=HASH_C,
        content_sha256=HASH_D,
        expected_sessions=SESSIONS,
    )


def _payload(*, sessions: tuple[str, ...] = SESSIONS) -> dict[str, object]:
    rows = [
        {"session": session, "symbol": symbol, "adjusted_close": price}
        for session, price_qqq, price_tqqq in (("2024-01-02", "1.25", "2.5"), ("2024-01-03", "1.5", "3.25"))
        if session in sessions
        for symbol, price in (("QQQ", price_qqq), ("TQQQ", price_tqqq))
    ]
    return {
        "calendar_timezone": "America/New_York",
        "generation": "clean_cutover_v1",
        "offline_fixture": True,
        "pair_id": "QQQ_TQQQ",
        "plugin": "ABSENT_DISABLED",
        "rows": rows,
        "schema_version": "soxl_tqqq_clean_cutover_snapshot.v1",
        "sessions": list(sessions),
        "size": 0,
        "symbols": ["QQQ", "TQQQ"],
    }


def _canonical(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _write_package(tmp_path: Path, payload: object | None = None) -> tuple[Path, snapshot.ExternalBindings]:
    path = tmp_path / "fixture.json"
    raw = _canonical(_payload() if payload is None else payload)
    path.write_bytes(raw)
    bindings = snapshot.ExternalBindings(
        source_sha256=HASH_A,
        calendar_sha256=HASH_B,
        manifest_sha256=HASH_C,
        content_sha256=hashlib.sha256(raw).hexdigest(),
        expected_sessions=SESSIONS,
    )
    return path, bindings


def _build(tmp_path: Path, payload: object | None = None) -> snapshot.TrustedSnapshotPackage:
    path, bindings = _write_package(tmp_path, payload)
    return snapshot.build_trusted_snapshot_package(
        trusted_root=tmp_path,
        relative_path=path.name,
        bindings=bindings,
    )


def test_schema_artifact_is_generated_from_runtime_contract() -> None:
    schema_path = Path(__file__).parents[1] / "schemas" / "soxl_tqqq_clean_cutover_snapshot.v1.schema.json"
    assert schema_path.read_bytes() == snapshot.canonical_json_bytes(snapshot.public_json_schema()) + b"\n"


def test_factory_returns_frozen_digest_bound_offline_package(tmp_path: Path) -> None:
    package = _build(tmp_path)
    assert package.snapshot_id == f"sha256-{HASH_C}"
    assert package.external_bindings.source_sha256 == HASH_A
    assert package.canonical_bytes == _canonical(_payload())
    with pytest.raises((AttributeError, TypeError)):
        package.snapshot_id = "changed"  # type: ignore[misc]
    assert snapshot.validate_trusted_snapshot_package(package) == package


@pytest.mark.parametrize("raw", [b"{}", b"[]", b'{"a":1}', [{"session": "2024-01-02"}]] )
def test_public_boundary_rejects_raw_inputs(raw: bytes) -> None:
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.validate_trusted_snapshot_package(raw)


def test_package_cannot_be_directly_constructed_or_given_free_form_digest() -> None:
    with pytest.raises(TypeError):
        snapshot.TrustedSnapshotPackage()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        snapshot.TrustedSnapshotPackage(  # type: ignore[call-arg]
            _token=object(), canonical_bytes=b"", external_bindings=_bindings()
        )
    with pytest.raises(TypeError):
        snapshot.build_trusted_snapshot_package(  # type: ignore[call-arg]
            trusted_root=Path("."), relative_path="fixture.json", content_sha256=HASH_A
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update({"generation": "legacy"}),
        lambda value: value.update({"plugin": "PRESENT"}),
        lambda value: value.update({"offline_fixture": False}),
        lambda value: value.update({"size": 1}),
        lambda value: value.update({"symbols": ["TQQQ", "QQQ"]}),
        lambda value: value.update({"sessions": ["2024-01-03", "2024-01-02"]}),
        lambda value: value.update({"sessions": ["2024-01-02"]}),
    ],
)
def test_exact_constants_and_calendar_coverage_are_required(tmp_path: Path, mutate) -> None:
    payload = _payload()
    mutate(payload)
    with pytest.raises(snapshot.SnapshotValidationError):
        _build(tmp_path, payload)


def test_decimal_contract_accepts_canonical_sub_unit_and_shared_length_boundary(tmp_path: Path) -> None:
    payload = _payload()
    payload["rows"][0]["adjusted_close"] = "0.5"  # type: ignore[index]
    assert _build(tmp_path, payload)
    payload = _payload()
    payload["rows"][0]["adjusted_close"] = "9" * snapshot.MAX_DECIMAL_LENGTH  # type: ignore[index]
    assert _build(tmp_path, payload)
    schema = snapshot.public_json_schema()
    decimal_schema = schema["properties"]["rows"]["items"]["properties"]["adjusted_close"]  # type: ignore[index]
    assert decimal_schema["maxLength"] == snapshot.MAX_DECIMAL_LENGTH  # type: ignore[index]
    assert decimal_schema["pattern"] == rf"^{snapshot.CANONICAL_DECIMAL_PATTERN}$"  # type: ignore[index]


@pytest.mark.parametrize("decimal", [".5", "0", "0.0", "1.0", "01", "1e2", "NaN", "Infinity", "9" * 33])
def test_decimal_contract_rejects_noncanonical_or_unbounded_values(tmp_path: Path, decimal: str) -> None:
    payload = _payload()
    payload["rows"][0]["adjusted_close"] = decimal  # type: ignore[index]
    with pytest.raises(snapshot.SnapshotValidationError):
        _build(tmp_path, payload)


def test_rows_must_be_pair_complete_sorted_and_unique(tmp_path: Path) -> None:
    payload = _payload()
    payload["rows"].reverse()  # type: ignore[index]
    with pytest.raises(snapshot.SnapshotValidationError):
        _build(tmp_path, payload)
    payload = _payload()
    payload["rows"].append(payload["rows"][0])  # type: ignore[index]
    with pytest.raises(snapshot.SnapshotValidationError):
        _build(tmp_path, payload)
    payload = _payload()
    payload["rows"].pop()  # type: ignore[index]
    with pytest.raises(snapshot.SnapshotValidationError):
        _build(tmp_path, payload)


def test_digest_mismatch_and_noncanonical_or_duplicate_json_fail_before_trust(tmp_path: Path) -> None:
    path, bindings = _write_package(tmp_path)
    bad_bindings = snapshot.ExternalBindings(
        source_sha256=HASH_A,
        calendar_sha256=HASH_B,
        manifest_sha256=HASH_C,
        content_sha256=HASH_D,
        expected_sessions=SESSIONS,
    )
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=path.name, bindings=bad_bindings)
    duplicate = b'{"rows":{"session":"2024-01-02","session":"2024-01-02"}}'
    path.write_bytes(duplicate)
    duplicate_bindings = snapshot.ExternalBindings(
        source_sha256=HASH_A,
        calendar_sha256=HASH_B,
        manifest_sha256=HASH_C,
        content_sha256=hashlib.sha256(duplicate).hexdigest(),
        expected_sessions=SESSIONS,
    )
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(
            trusted_root=tmp_path, relative_path=path.name, bindings=duplicate_bindings
        )


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlink support required")
def test_strict_readback_rejects_root_leaf_and_ancestor_symlinks_and_escapes(tmp_path: Path) -> None:
    path, bindings = _write_package(tmp_path)
    link_root = tmp_path / "root-link"
    link_root.symlink_to(tmp_path, target_is_directory=True)
    for root, candidate in ((link_root, path.name), (tmp_path, "missing/../fixture.json"), (tmp_path, "/tmp/fixture.json")):
        with pytest.raises(snapshot.SnapshotValidationError):
            snapshot.build_trusted_snapshot_package(trusted_root=root, relative_path=candidate, bindings=bindings)
    leaf = tmp_path / "leaf-link.json"
    leaf.symlink_to(path)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=leaf.name, bindings=bindings)
    nested = tmp_path / "nested"
    nested.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path="nested/fixture.json", bindings=bindings)
    real_parent = tmp_path / "real-parent"
    trusted_root = real_parent / "trusted-root"
    trusted_root.mkdir(parents=True)
    source_path, root_bindings = _write_package(trusted_root)
    parent_link = tmp_path / "parent-link"
    parent_link.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(
            trusted_root=parent_link / trusted_root.name,
            relative_path=source_path.name,
            bindings=root_bindings,
        )


def test_strict_readback_rejects_missing_root_root_file_nonregular_and_oversize(monkeypatch, tmp_path: Path) -> None:
    path, bindings = _write_package(tmp_path)
    for root in (tmp_path / "missing", path):
        with pytest.raises(snapshot.SnapshotValidationError):
            snapshot.build_trusted_snapshot_package(trusted_root=root, relative_path=path.name, bindings=bindings)
    fifo = tmp_path / "input.fifo"
    os.mkfifo(fifo)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=fifo.name, bindings=bindings)
    monkeypatch.setattr(snapshot, "MAX_READ_BYTES", 32)
    path.write_bytes(b"x" * 33)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=path.name, bindings=bindings)


def test_serializer_and_readback_share_the_same_byte_limit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "MAX_READ_BYTES", 8)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.canonical_json_bytes({"oversized": "fixture"})
    path, bindings = _write_package(tmp_path)
    monkeypatch.setattr(snapshot.os, "read", lambda *_: b"")
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=path.name, bindings=bindings)


def test_missing_o_nofollow_fails_closed(monkeypatch, tmp_path: Path) -> None:
    path, bindings = _write_package(tmp_path)
    monkeypatch.delattr(snapshot.os, "O_NOFOLLOW", raising=False)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path=path.name, bindings=bindings)


def test_rejects_huge_json_integer_before_conversion(tmp_path: Path) -> None:
    raw = b'{"size":' + b"9" * (snapshot.MAX_JSON_INT_DIGITS + 1) + b"}"
    (tmp_path / "fixture.json").write_bytes(raw)
    bindings = snapshot.ExternalBindings(
        source_sha256=HASH_A,
        calendar_sha256=HASH_B,
        manifest_sha256=HASH_C,
        content_sha256=hashlib.sha256(raw).hexdigest(),
        expected_sessions=SESSIONS,
    )
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.build_trusted_snapshot_package(trusted_root=tmp_path, relative_path="fixture.json", bindings=bindings)


def test_validator_rechecks_mutable_escape_hatches_against_external_bindings(tmp_path: Path) -> None:
    package = _build(tmp_path)
    object.__setattr__(package, "canonical_bytes", package.canonical_bytes + b" ")
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.validate_trusted_snapshot_package(package)
    package = _build(tmp_path)
    object.__setattr__(package.external_bindings, "content_sha256", HASH_D)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.validate_trusted_snapshot_package(package)
    package = _build(tmp_path)
    object.__setattr__(package.external_bindings, "expected_sessions", ("not-a-date",))
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.validate_trusted_snapshot_package(package)


def test_external_bindings_are_typed_and_exact() -> None:
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.ExternalBindings(HASH_A, HASH_B, HASH_C, HASH_D, ["2024-01-02"])  # type: ignore[arg-type]
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.ExternalBindings(HASH_A, HASH_B, HASH_C, HASH_D, ("2024-01-02", "2024-01-02"))
