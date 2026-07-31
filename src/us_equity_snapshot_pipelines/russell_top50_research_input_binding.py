"""Offline-only Russell Top50 synthetic research-input binding."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping

import pandas as pd
from quant_platform_kit.data.research_input import (
    InvalidResearchInputEvidence,
    canonical_research_input_manifest_bytes,
    read_research_input_manifest_json,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

from .pipelines.mega_cap_leader_rotation_snapshot import build_artifacts
from .tqqq_r1_snapshot import SnapshotValidationError, _publish_noreplace, _read_member_from_root, _require_descriptor_capabilities

PROFILE = "russell_top50_leader_rotation"
CONTRACT_VERSION = "russell_top50_leader_rotation.research_input_binding.v1"
_SOURCE_ID = "uesp:russell-top50:frozen-synthetic-fixture:v1"
_SOURCE_IDENTITY = "repository_owned_frozen_synthetic_fixture"
_MEMBERS = (
    "inputs/prices.csv",
    "inputs/universe.csv",
    "snapshot/feature_snapshot.csv",
    "snapshot/feature_snapshot.manifest.json",
    "snapshot/ranking.csv",
)
_MANIFEST = "research-input-manifest.json"
_MAX_MEMBER_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_BYTES = 8 * 1024 * 1024
_BASE = "b5306a972724105273efebbd22834afde1294e4e"
_TREE = "9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c"
_PRICES_SHA256 = "1e2c1532ab665a57936643e7b2242d5767ec954af6ce2aa4534a133f5fc70e71"
_UNIVERSE_SHA256 = "e7e09faef385b31dbc3b7adfdea3ed60bf4b9793c8d03f8eb5ceb96885c11a91"
_MISSING_SOURCE_IDENTITY = object()


class ResearchInputBindingError(ValueError):
    """Raised for every rejected local binding package."""


@dataclass(frozen=True)
class RussellTop50ResearchInput:
    output_dir: Path
    manifest_sha256: str
    members: Mapping[str, bytes]


def _invalid() -> None:
    raise ResearchInputBindingError("invalid Russell Top50 research input")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_csv(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _safe_identity(value: object) -> str:
    if type(value) is not str or value != _SOURCE_IDENTITY:
        _invalid()
    return value


def _safe_timestamp(value: datetime, name: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ResearchInputBindingError(f"{name} must be canonical UTC")
    if value.utcoffset() != timedelta(0) or value.microsecond:
        raise ResearchInputBindingError(f"{name} must be canonical UTC")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strict_json_mapping(raw: bytes) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                _invalid()
            result[key] = value
        return result

    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _: _invalid(),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        _invalid()
    if type(parsed) is not dict:
        _invalid()
    return parsed


def _canonical_inner_manifest_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(dict(payload), indent=2, sort_keys=True).encode("utf-8")


def _read_package(output_dir: str | Path) -> tuple[bytes, dict[str, bytes]]:
    root = Path(output_dir)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        _require_descriptor_capabilities()
        root_fd = os.open(root, flags)
        try:
            if {entry.name for entry in os.scandir(root_fd)} != {_MANIFEST, "inputs", "snapshot"}:
                _invalid()
            manifest_raw = _read_member_from_root(root_fd, _MANIFEST, _MAX_MEMBER_BYTES)
            members: dict[str, bytes] = {}
            total = len(manifest_raw)
            for directory, names in (
                ("inputs", ("prices.csv", "universe.csv")),
                ("snapshot", ("feature_snapshot.csv", "feature_snapshot.manifest.json", "ranking.csv")),
            ):
                child_fd = os.open(directory, flags, dir_fd=root_fd)
                try:
                    if {entry.name for entry in os.scandir(child_fd)} != set(names):
                        _invalid()
                    for name in names:
                        raw = _read_member_from_root(child_fd, name, min(_MAX_MEMBER_BYTES, _MAX_TOTAL_BYTES - total))
                        total += len(raw)
                        members[f"{directory}/{name}"] = raw
                finally:
                    os.close(child_fd)
            if total > _MAX_TOTAL_BYTES:
                _invalid()
            return manifest_raw, members
        finally:
            os.close(root_fd)
    except (OSError, SnapshotValidationError, ResearchInputBindingError):
        raise ResearchInputBindingError("invalid Russell Top50 research input") from None


def _outer_manifest(
    members: Mapping[str, bytes], *, producer_commit_sha: str, producer_tree_sha: str, observed_at: str, as_of: str
) -> dict[str, object]:
    return validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"uesp.russell-top50.synthetic.{_sha256(members['snapshot/feature_snapshot.csv'])}.v1",
            "research_input_contract_id": CONTRACT_VERSION,
            "domain": "us_equity",
            "profile": PROFILE,
            "artifact_type": "feature_snapshot",
            "observed_at": observed_at,
            "effective_at": as_of,
            "as_of": as_of,
            "producer": {
                "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
                "commit_sha": producer_commit_sha,
                "tree_sha": producer_tree_sha,
                "tool": "us_equity_snapshot_pipelines.russell_top50_research_input_binding",
                "tool_version": CONTRACT_VERSION,
            },
            "calendar": {
                "calendar_id": "UESP_RUSSELL_TOP50_SYNTHETIC_FIXTURE_V1",
                "timezone": "America/New_York",
                "session_date": as_of[:10],
                "source": "mega_cap_leader_rotation_snapshot.fixture",
                "source_revision": producer_commit_sha,
            },
            "adjustment": {"policy": "raw", "source": _SOURCE_IDENTITY, "source_revision": producer_commit_sha},
            "sources": [{"source_id": _SOURCE_ID, "revision": CONTRACT_VERSION, "observed_at": observed_at,
                         "content_sha256": _sha256(members["inputs/prices.csv"])}],
            "members": [
                {"path": name, "media_type": "application/json" if name.endswith(".json") else "text/csv",
                 "size_bytes": len(members[name]), "sha256": _sha256(members[name])}
                for name in _MEMBERS
            ],
        }
    )


def _validate_claims(manifest: Mapping[str, object], members: Mapping[str, bytes]) -> None:
    try:
        producer = manifest["producer"]
        observed_at = manifest["observed_at"]
        as_of = manifest["as_of"]
        expected_inner = {
            "config_name": PROFILE,
            "config_path": "strategy_manifest_default",
            "config_sha256": "a1f685e85f780e4e8c6fd9d2769a5a4125ea57567640372ad256cf3969e5cbca",
            "contract_version": "russell_top50_leader_rotation.feature_snapshot.v1",
            "generated_at": observed_at,
            "input_artifacts": {
                "prices": {"as_of": as_of[:10], "path": "inputs/prices.csv", "row_count": 2880, "sha256": _PRICES_SHA256},
                "universe": {"as_of": None, "path": "inputs/universe.csv", "row_count": 6, "sha256": _UNIVERSE_SHA256},
            },
            "manifest_type": "feature_snapshot",
            "price_as_of": as_of[:10],
            "row_count": 9,
            "snapshot_as_of": as_of[:10],
            "snapshot_path": "snapshot/feature_snapshot.csv",
            "snapshot_sha256": _sha256(members["snapshot/feature_snapshot.csv"]),
            "source_project": "UsEquitySnapshotPipelines",
            "strategy_profile": PROFILE,
        }
        if (
            manifest["manifest_id"] != f"uesp.russell-top50.synthetic.{_sha256(members['snapshot/feature_snapshot.csv'])}.v1"
            or
            _sha256(members["inputs/prices.csv"]) != _PRICES_SHA256
            or _sha256(members["inputs/universe.csv"]) != _UNIVERSE_SHA256
            or manifest["research_input_contract_id"] != CONTRACT_VERSION
            or manifest["domain"] != "us_equity"
            or manifest["profile"] != PROFILE
            or manifest["artifact_type"] != "feature_snapshot"
            or manifest["sources"] != [{"source_id": _SOURCE_ID, "revision": CONTRACT_VERSION,
                                          "observed_at": manifest["observed_at"],
                                          "content_sha256": _sha256(members["inputs/prices.csv"])}]
            or not isinstance(producer, Mapping)
            or producer.get("repository") != "QuantStrategyLab/UsEquitySnapshotPipelines"
            or producer.get("commit_sha") != _BASE
            or producer.get("tree_sha") != _TREE
            or producer.get("tool") != "us_equity_snapshot_pipelines.russell_top50_research_input_binding"
            or producer.get("tool_version") != CONTRACT_VERSION
            or manifest["adjustment"] != {"policy": "raw", "source": _SOURCE_IDENTITY,
                                             "source_revision": producer["commit_sha"]}
            or manifest["calendar"] != {
                "calendar_id": "UESP_RUSSELL_TOP50_SYNTHETIC_FIXTURE_V1",
                "timezone": "America/New_York",
                "session_date": as_of[:10],
                "source": "mega_cap_leader_rotation_snapshot.fixture",
                "source_revision": _BASE,
            }
        ):
            _invalid()
        inner_raw = members["snapshot/feature_snapshot.manifest.json"]
        inner = _strict_json_mapping(inner_raw)
        if inner != expected_inner or inner_raw != _canonical_inner_manifest_bytes(expected_inner):
            _invalid()
        pd.read_csv(__import__("io").BytesIO(members["inputs/prices.csv"]))
        pd.read_csv(__import__("io").BytesIO(members["inputs/universe.csv"]))
        pd.read_csv(__import__("io").BytesIO(members["snapshot/feature_snapshot.csv"]))
        pd.read_csv(__import__("io").BytesIO(members["snapshot/ranking.csv"]))
    except (KeyError, TypeError, ValueError):
        _invalid()


def verify_russell_top50_research_input(output_dir: str | Path, *, expected_manifest_sha256: str) -> RussellTop50ResearchInput:
    if (
        type(expected_manifest_sha256) is not str
        or len(expected_manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_manifest_sha256)
    ):
        _invalid()
    try:
        manifest_raw, members = _read_package(output_dir)
        if _sha256(manifest_raw) != expected_manifest_sha256:
            _invalid()
        manifest = read_research_input_manifest_json(manifest_raw)
        if manifest_raw != canonical_research_input_manifest_bytes(manifest) or research_input_manifest_sha256(manifest) != expected_manifest_sha256:
            _invalid()
        declared = {item["path"]: item for item in manifest["members"]}
        if list(declared) != list(_MEMBERS):
            _invalid()
        for name, raw in members.items():
            item = declared[name]
            expected_media_type = "application/json" if name.endswith(".json") else "text/csv"
            if item["media_type"] != expected_media_type or item["size_bytes"] != len(raw) or item["sha256"] != _sha256(raw):
                _invalid()
        _validate_claims(manifest, members)
    except (InvalidResearchInputEvidence, ResearchInputBindingError):
        raise ResearchInputBindingError("invalid Russell Top50 research input") from None
    return RussellTop50ResearchInput(Path(output_dir), expected_manifest_sha256, dict(members))


def materialize_russell_top50_research_input(
    prices: pd.DataFrame, universe: pd.DataFrame, output_dir: str | Path, *, producer_commit_sha: str,
    producer_tree_sha: str, observed_at: datetime, as_of: datetime, source_identity: object = _MISSING_SOURCE_IDENTITY
) -> RussellTop50ResearchInput:
    """Publish a deterministic local package from already-local frozen synthetic frames."""
    _safe_identity(source_identity)
    observed, cutoff = _safe_timestamp(observed_at, "observed_at"), _safe_timestamp(as_of, "as_of")
    if producer_commit_sha != _BASE or producer_tree_sha != _TREE:
        _invalid()
    if _sha256(_canonical_csv(prices)) != _PRICES_SHA256 or _sha256(_canonical_csv(universe)) != _UNIVERSE_SHA256:
        _invalid()
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid()
    temporary: Path | None = None
    try:
        temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
        inputs = temporary / "inputs"
        snapshot = temporary / "snapshot"
        inputs.mkdir()
        snapshot.mkdir()
        (inputs / "prices.csv").write_bytes(_canonical_csv(prices))
        (inputs / "universe.csv").write_bytes(_canonical_csv(universe))
        produced = build_artifacts(prices_path=inputs / "prices.csv", universe_path=inputs / "universe.csv", output_dir=snapshot,
                                   as_of_date=cutoff[:10], min_adv20_usd=1_000_000.0)
        shutil.move(str(produced.snapshot_path), snapshot / "feature_snapshot.csv")
        shutil.move(str(produced.manifest_path), snapshot / "feature_snapshot.manifest.json")
        shutil.move(str(produced.ranking_path), snapshot / "ranking.csv")
        produced.release_summary_path.unlink()
        inner_path = snapshot / "feature_snapshot.manifest.json"
        inner = _strict_json_mapping(inner_path.read_bytes())
        if (
            inner.get("manifest_type") != "feature_snapshot"
            or inner.get("strategy_profile") != PROFILE
            or inner.get("contract_version") != "russell_top50_leader_rotation.feature_snapshot.v1"
            or inner.get("snapshot_sha256") != _sha256((snapshot / "feature_snapshot.csv").read_bytes())
            or inner.get("input_artifacts", {}).get("prices", {}).get("sha256") != _PRICES_SHA256
            or inner.get("input_artifacts", {}).get("universe", {}).get("sha256") != _UNIVERSE_SHA256
        ):
            _invalid()
        inner_path.write_bytes(
            _canonical_inner_manifest_bytes(
                {
                    "config_name": PROFILE,
                    "config_path": "strategy_manifest_default",
                    "config_sha256": "a1f685e85f780e4e8c6fd9d2769a5a4125ea57567640372ad256cf3969e5cbca",
                    "contract_version": "russell_top50_leader_rotation.feature_snapshot.v1",
                    "generated_at": observed,
                    "input_artifacts": {
                        "prices": {"as_of": cutoff[:10], "path": "inputs/prices.csv", "row_count": 2880, "sha256": _PRICES_SHA256},
                        "universe": {"as_of": None, "path": "inputs/universe.csv", "row_count": 6, "sha256": _UNIVERSE_SHA256},
                    },
                    "manifest_type": "feature_snapshot",
                    "price_as_of": cutoff[:10],
                    "row_count": 9,
                    "snapshot_as_of": cutoff[:10],
                    "snapshot_path": "snapshot/feature_snapshot.csv",
                    "snapshot_sha256": _sha256((snapshot / "feature_snapshot.csv").read_bytes()),
                    "source_project": "UsEquitySnapshotPipelines",
                    "strategy_profile": PROFILE,
                }
            )
        )
        members = {name: (temporary / name).read_bytes() for name in _MEMBERS}
        manifest = _outer_manifest(members, producer_commit_sha=producer_commit_sha, producer_tree_sha=producer_tree_sha,
                                   observed_at=observed, as_of=cutoff)
        digest = research_input_manifest_sha256(manifest)
        (temporary / _MANIFEST).write_bytes(canonical_research_input_manifest_bytes(manifest))
        verify_russell_top50_research_input(temporary, expected_manifest_sha256=digest)
        _publish_noreplace(temporary, destination)
    except (OSError, SnapshotValidationError, ResearchInputBindingError, UnicodeDecodeError, ValueError, pd.errors.ParserError):
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)
        raise ResearchInputBindingError("invalid Russell Top50 research input") from None
    return RussellTop50ResearchInput(destination, digest, dict(members))


def bind_russell_top50_research_input(output_dir: str | Path, *, expected_manifest_sha256: str, context: Any) -> Any:
    """Evaluate the existing UES entrypoint against a detached verified feature snapshot only."""
    from quant_platform_kit.strategy_contracts import StrategyContext

    if type(context) is not StrategyContext:
        _invalid()
    as_of = object.__getattribute__(context, "as_of")
    inert_fields = ("market_data", "state", "runtime_config", "capabilities", "artifacts")
    if type(as_of) is not str or object.__getattribute__(context, "portfolio") is not None or any(
        type(value := object.__getattribute__(context, name)) is not dict or len(value) != 0 for name in inert_fields
    ):
        _invalid()
    verified = verify_russell_top50_research_input(output_dir, expected_manifest_sha256=expected_manifest_sha256)
    raw = verified.members["snapshot/feature_snapshot.csv"]
    frame = pd.read_csv(__import__("io").BytesIO(raw)).copy(deep=True)
    from us_equity_strategies.entrypoints import russell_top50_leader_rotation_entrypoint
    local_context = StrategyContext(as_of=as_of, market_data={"feature_snapshot": frame})
    return russell_top50_leader_rotation_entrypoint.evaluate(local_context)
