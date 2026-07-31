from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot

_COMMIT = "9dd3077eef2cd6abd72f4bea94dcc88d4018fb8d"
_TREE = "47364a77a44ee88b52ba55eab8585d9ffd589c3a"
_OBSERVED = datetime(2026, 7, 30, 6, 5, 39, tzinfo=UTC)


def _fixture_prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "TQQQ", "adjusted_close": 10.5},
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.25},
            {"session": "2010-01-05", "symbol": "TQQQ", "adjusted_close": 11.0},
            {"session": "2010-01-05", "symbol": "QQQ", "adjusted_close": 46.0},
        ]
    )


def _proof(tmp_path: Path) -> snapshot.SnapshotResult:
    return snapshot.materialize_tqqq_r1_research_input_proof(
        _fixture_prices(),
        tmp_path / "proof",
        producer_commit_sha=_COMMIT,
        producer_tree_sha=_TREE,
        observed_at=_OBSERVED,
        as_of=_OBSERVED,
    )


def test_research_input_proof_apis_are_public() -> None:
    assert callable(snapshot.materialize_tqqq_r1_research_input_proof)
    assert callable(snapshot.verify_tqqq_r1_research_input_proof)


def test_materialize_writes_deterministic_qpk_manifest_and_exact_provenance(tmp_path: Path) -> None:
    result = _proof(tmp_path)
    manifest_path = result.output_dir / "research-input-manifest.json"
    raw = manifest_path.read_bytes()
    manifest = snapshot.read_research_input_manifest_json(raw)

    assert tuple(sorted(path.name for path in result.output_dir.iterdir())) == (
        "research-input-manifest.json",
        "tqqq_r1_snapshot",
    )
    assert raw == snapshot.canonical_research_input_manifest_bytes(manifest)
    assert result.manifest_sha256 == snapshot.research_input_manifest_sha256(manifest)
    assert manifest["producer"] == {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": _COMMIT,
        "tree_sha": _TREE,
        "tool": "us_equity_snapshot_pipelines.tqqq_r1_snapshot.materialize_tqqq_r1_research_input_proof",
        "tool_version": "tqqq_r1_research_input_proof.v1",
    }
    assert manifest["calendar"] == {
        "calendar_id": "UESP_TQQQ_R1_SYNTHETIC_FIXTURE_V1",
        "timezone": "America/New_York",
        "session_date": "2010-01-05",
        "source": "tqqq_r1_snapshot.weekday_session_contract",
        "source_revision": _COMMIT,
    }
    assert manifest["adjustment"]["policy"] == "total_return_adjusted"
    assert [member["path"] for member in manifest["members"]] == list(snapshot._PROOF_FILENAMES)
    prices = result.output_dir / "tqqq_r1_snapshot" / "prices.csv"
    assert manifest["sources"] == [{
        "source_id": "uesp:tqqq-r1:canonical-prices:v1",
        "revision": snapshot.CONTRACT_VERSION,
        "observed_at": "2026-07-30T06:05:39Z",
        "content_sha256": hashlib.sha256(prices.read_bytes()).hexdigest(),
    }]
    assert snapshot.verify_tqqq_r1_research_input_proof(
        result.output_dir, expected_manifest_sha256=result.manifest_sha256
    ) == result


def test_verify_authenticates_trusted_digest_before_parsing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _proof(tmp_path)
    monkeypatch.setattr(snapshot, "read_research_input_manifest_json", lambda _: pytest.fail("parser reached"))

    with pytest.raises(snapshot.SnapshotValidationError, match="trusted research input manifest hash mismatch"):
        snapshot.verify_tqqq_r1_research_input_proof(result.output_dir, expected_manifest_sha256="0" * 64)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":"research_input_manifest.v1","schema_version":"research_input_manifest.v1"}',
        b'{"schema_version":NaN}',
        b"\xff",
        b"{} trailing",
    ],
    ids=["duplicate-keys", "nonfinite", "invalid-utf8", "trailing-json"],
)
def test_verify_normalizes_qpk_strict_readback_failures(tmp_path: Path, payload: bytes) -> None:
    result = _proof(tmp_path)
    manifest = result.output_dir / "research-input-manifest.json"
    manifest.write_bytes(payload)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid research input proof"):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=hashlib.sha256(payload).hexdigest()
        )


def test_verify_rejects_noncanonical_manifest_bytes(tmp_path: Path) -> None:
    result = _proof(tmp_path)
    manifest = result.output_dir / "research-input-manifest.json"
    payload = b" \n" + manifest.read_bytes()
    manifest.write_bytes(payload)

    with pytest.raises(snapshot.SnapshotValidationError, match="not canonical"):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=hashlib.sha256(payload).hexdigest()
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest.update(research_input_contract_id="other_contract.v1"),
        lambda manifest: manifest.update(manifest_id="uesp.tqqq-r1.synthetic-proof." + "0" * 64 + ".v1"),
        lambda manifest: manifest.update(profile="other_synthetic_fixture_proof.v1"),
        lambda manifest: manifest.update(artifact_type="other_immutable_snapshot"),
        lambda manifest: manifest["calendar"].update(session_date="2010-01-04"),
        lambda manifest: manifest.update(effective_at="2010-01-04T00:00:00-05:00"),
        lambda manifest: manifest["sources"][0].update(content_sha256="0" * 64),
    ],
    ids=["contract", "identity", "profile", "artifact", "calendar-session", "effective-session", "source-digest"],
)
def test_verify_rejects_manifest_claims_inconsistent_with_enclosed_snapshot(tmp_path: Path, mutate) -> None:
    result = _proof(tmp_path)
    manifest_path = result.output_dir / "research-input-manifest.json"
    manifest = snapshot.read_research_input_manifest_json(manifest_path.read_bytes())
    mutate(manifest)
    payload = snapshot.canonical_research_input_manifest_bytes(manifest)
    manifest_path.write_bytes(payload)

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=hashlib.sha256(payload).hexdigest()
        )


@pytest.mark.parametrize("kind", ["missing", "extra", "symlink", "nonregular", "oversized"])
def test_verify_rejects_invalid_outer_members(tmp_path: Path, kind: str) -> None:
    result = _proof(tmp_path)
    root = result.output_dir
    if kind == "missing":
        (root / "tqqq_r1_snapshot" / "validation.json").unlink()
    elif kind == "extra":
        (root / "unexpected").write_text("x", encoding="utf-8")
    elif kind == "symlink":
        (root / "research-input-manifest.json").unlink()
        os.symlink("tqqq_r1_snapshot/manifest.json", root / "research-input-manifest.json")
    elif kind == "nonregular":
        (root / "research-input-manifest.json").unlink()
        (root / "research-input-manifest.json").mkdir()
    else:
        (root / "research-input-manifest.json").write_bytes(b"x" * (snapshot._PROOF_MANIFEST_BYTE_LIMIT + 1))

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_research_input_proof(root, expected_manifest_sha256=result.manifest_sha256)


def test_verify_rejects_inner_total_limit_before_semantic_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _proof(tmp_path)
    root = result.output_dir
    inner = root / "tqqq_r1_snapshot"
    oversized_members = {
        "prices.csv": b"p" * snapshot._MEMBER_BYTE_LIMITS["prices.csv"],
        "manifest.json": b"m" * snapshot._MEMBER_BYTE_LIMITS["manifest.json"],
        "validation.json": b"v" * snapshot._MEMBER_BYTE_LIMITS["validation.json"],
        "sha256sums.json": b"s",
    }
    for name, raw in oversized_members.items():
        (inner / name).write_bytes(raw)

    manifest_path = root / "research-input-manifest.json"
    manifest = snapshot.read_research_input_manifest_json(manifest_path.read_bytes())
    for member in manifest["members"]:
        raw = oversized_members[Path(member["path"]).name]
        member.update(size_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    manifest_raw = snapshot.canonical_research_input_manifest_bytes(manifest)
    manifest_path.write_bytes(manifest_raw)

    monkeypatch.setattr(
        snapshot,
        "_validate_tqqq_r1_research_input_proof_claims",
        lambda *_: pytest.fail("semantic parsing reached"),
    )
    with pytest.raises(snapshot.SnapshotValidationError, match="snapshot members exceed total size limit"):
        snapshot.verify_tqqq_r1_research_input_proof(
            root, expected_manifest_sha256=hashlib.sha256(manifest_raw).hexdigest()
        )


def test_verify_rejects_member_size_hash_and_identity_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _proof(tmp_path)
    prices = result.output_dir / "tqqq_r1_snapshot" / "prices.csv"
    prices.write_bytes(prices.read_bytes() + b"tamper\n")
    with pytest.raises(snapshot.SnapshotValidationError, match="member hash mismatch"):
        snapshot.verify_tqqq_r1_research_input_proof(result.output_dir, expected_manifest_sha256=result.manifest_sha256)

    result = _proof(tmp_path / "second")
    identities = iter([(1,), (2,)])
    monkeypatch.setattr(snapshot, "_stable_member_identity", lambda _: next(identities))
    with pytest.raises(snapshot.SnapshotValidationError, match="changed during read"):
        snapshot.verify_tqqq_r1_research_input_proof(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


class _ScandirEntries:
    def __init__(self, names: list[str], *, fail_after_entries: bool) -> None:
        self._names = names
        self._fail_after_entries = fail_after_entries

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def __iter__(self):
        for name in self._names:
            yield type("Entry", (), {"name": name})()
        if self._fail_after_entries:
            pytest.fail("directory scan continued after the exact entry limit")


@pytest.mark.parametrize(
    "directories",
    [
        ((["research-input-manifest.json", "tqqq_r1_snapshot", "extra"], True),),
        ((["research-input-manifest.json", "tqqq_r1_snapshot"], False), ([*snapshot.OUTPUT_FILENAMES, "extra"], True)),
    ],
    ids=["outer", "nested"],
)
def test_verify_stops_proof_directory_enumeration_at_exact_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, directories: tuple[tuple[list[str], bool], ...]
) -> None:
    result = _proof(tmp_path)
    scans = iter(directories)

    def bounded_scandir(_: int) -> _ScandirEntries:
        names, fail_after_entries = next(scans)
        return _ScandirEntries(names, fail_after_entries=fail_after_entries)

    monkeypatch.setattr(snapshot.os, "scandir", bounded_scandir)
    monkeypatch.setattr(snapshot.os, "supports_fd", {*snapshot.os.supports_fd, bounded_scandir})

    with pytest.raises(snapshot.SnapshotValidationError, match="unexpected research input"):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=result.manifest_sha256
        )


def test_materialize_is_no_clobber_and_verify_is_detached(tmp_path: Path) -> None:
    result = _proof(tmp_path)
    verified = snapshot.verify_tqqq_r1_research_input_proof(
        result.output_dir, expected_manifest_sha256=result.manifest_sha256
    )
    assert verified == result
    with pytest.raises(snapshot.SnapshotValidationError, match="immutable output already exists"):
        snapshot.materialize_tqqq_r1_research_input_proof(
            _fixture_prices(), result.output_dir, producer_commit_sha=_COMMIT, producer_tree_sha=_TREE,
            observed_at=_OBSERVED, as_of=_OBSERVED,
        )

@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest["adjustment"].update(policy="split_adjusted"),
        lambda manifest: manifest["adjustment"].update(source="other.adjusted_close"),
        lambda manifest: manifest["adjustment"].update(source_revision="f" * 40),
    ],
    ids=["policy", "source", "source-revision"],
)
def test_verify_rejects_recanonicalized_adjustment_claim_tampering(tmp_path: Path, mutate) -> None:
    result = _proof(tmp_path)
    manifest_path = result.output_dir / "research-input-manifest.json"
    manifest = snapshot.read_research_input_manifest_json(manifest_path.read_bytes())
    mutate(manifest)
    payload = snapshot.canonical_research_input_manifest_bytes(manifest)
    manifest_path.write_bytes(payload)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid research input proof claims"):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=hashlib.sha256(payload).hexdigest()
        )


@pytest.mark.parametrize("path", snapshot._PROOF_FILENAMES)
def test_verify_rejects_recanonicalized_wrong_member_media_type(tmp_path: Path, path: str) -> None:
    result = _proof(tmp_path)
    manifest_path = result.output_dir / "research-input-manifest.json"
    manifest = snapshot.read_research_input_manifest_json(manifest_path.read_bytes())
    member = next(member for member in manifest["members"] if member["path"] == path)
    member["media_type"] = "application/json" if path.endswith("prices.csv") else "text/csv"
    payload = snapshot.canonical_research_input_manifest_bytes(manifest)
    manifest_path.write_bytes(payload)

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_research_input_proof(
            result.output_dir, expected_manifest_sha256=hashlib.sha256(payload).hexdigest()
        )


@pytest.mark.parametrize("name", ["validation.json", "sha256sums.json"])
def test_verify_uses_captured_bytes_after_inner_path_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    result = _proof(tmp_path)
    root = result.output_dir
    manifest_path = root / "research-input-manifest.json"
    valid_manifest = manifest_path.read_bytes()
    inner = root / "tqqq_r1_snapshot"
    valid_members = {member.name: member.read_bytes() for member in inner.iterdir()}
    malformed = dict(valid_members)
    malformed[name] = b"{malformed"
    if name == "validation.json":
        malformed["sha256sums.json"] = snapshot.json.dumps(
            {
                member: hashlib.sha256(malformed[member]).hexdigest()
                for member in ("prices.csv", "manifest.json", "validation.json")
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode() + b"\n"
    for member, raw in malformed.items():
        (inner / member).write_bytes(raw)
    manifest = snapshot.read_research_input_manifest_json(valid_manifest)
    for member in manifest["members"]:
        raw = malformed[Path(member["path"]).name]
        member.update(size_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    captured_manifest = snapshot.canonical_research_input_manifest_bytes(manifest)
    manifest_path.write_bytes(captured_manifest)
    original_read = snapshot._read_tqqq_r1_research_input_proof

    def capture_then_replace(output: Path) -> tuple[bytes, dict[str, bytes]]:
        captured = original_read(output)
        inner.rename(root / "captured-malformed")
        replacement = root / "replacement"
        replacement.mkdir()
        for member, raw in valid_members.items():
            (replacement / member).write_bytes(raw)
        replacement.rename(inner)
        manifest_path.write_bytes(valid_manifest)
        return captured

    monkeypatch.setattr(snapshot, "_read_tqqq_r1_research_input_proof", capture_then_replace)
    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_research_input_proof(
            root, expected_manifest_sha256=hashlib.sha256(captured_manifest).hexdigest()
        )
