from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .artifacts import resolve_snapshot_as_of, sha256_file
from .contracts import get_profile_contract


@dataclass(frozen=True)
class PublishItem:
    source: Path
    destination: str
    sha256: str | None = None


@dataclass(frozen=True)
class AtomicGenerationPublishPlan:
    artifacts: tuple[PublishItem, ...]
    pointer_destination: str
    pointer_bytes: bytes
    validation: dict[str, object]


def build_publish_plan(*, profile: str, artifact_dir: str | Path, gcs_prefix: str) -> tuple[PublishItem, ...]:
    contract = get_profile_contract(profile)
    paths = contract.artifact_paths(artifact_dir)
    normalized_prefix = str(gcs_prefix).rstrip("/")
    return tuple(
        PublishItem(source=path, destination=f"{normalized_prefix}/{path.name}")
        for path in (
            paths["snapshot"],
            paths["manifest"],
            paths["ranking"],
            paths["release_summary"],
        )
    )


def build_candidate_publish_plan(plan: tuple[PublishItem, ...], *, candidate_prefix: str) -> tuple[PublishItem, ...]:
    normalized_prefix = str(candidate_prefix).rstrip("/")
    return tuple(
        PublishItem(source=item.source, destination=f"{normalized_prefix}/{item.source.name}", sha256=item.sha256)
        for item in plan
    )


def _validate_generation_id(generation_id: str) -> str:
    normalized = str(generation_id or "").strip()
    if normalized in {"", ".", ".."} or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", normalized) is None:
        raise ValueError("generation_id must be 1-128 characters using only letters, digits, dot, underscore, or hyphen")
    return normalized


def _normalize_gcs_prefix(gcs_prefix: str) -> str:
    normalized = str(gcs_prefix or "").strip().rstrip("/")
    if not normalized.startswith("gs://") or any(character.isspace() or character == "\\" for character in normalized):
        raise ValueError("gcs_prefix must be a gs:// URI without whitespace or backslashes")
    bucket_and_path = normalized.removeprefix("gs://")
    bucket, separator, object_prefix = bucket_and_path.partition("/")
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", bucket) is None:
        raise ValueError("gcs_prefix must contain a valid GCS bucket name")
    if separator and any(segment in {"", ".", ".."} for segment in object_prefix.split("/")):
        raise ValueError("gcs_prefix object path must not contain empty, dot, or parent segments")
    return normalized


def _load_manifest(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"snapshot manifest must contain a JSON object: {path}")
    return payload


def _load_snapshot(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, orient="records", lines=suffix == ".jsonl")
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"unsupported snapshot format for publish validation: {path}")


def _coerce_date(value: object) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    else:
        timestamp = timestamp.tz_localize(None)
    return timestamp.normalize()


def validate_publish_artifacts(
    *,
    profile: str,
    artifact_dir: str | Path,
    min_row_count: int = 1,
    max_source_fallback_streak: int = 1,
) -> dict[str, object]:
    contract = get_profile_contract(profile)
    paths = contract.artifact_paths(artifact_dir)
    for name, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"{name} artifact not found: {path}")

    manifest = _load_manifest(paths["manifest"])
    snapshot = _load_snapshot(paths["snapshot"])
    row_count = int(len(snapshot))
    if row_count < int(min_row_count):
        raise ValueError(f"snapshot row_count below minimum: row_count={row_count} min={int(min_row_count)}")
    if int(manifest.get("row_count") or -1) != row_count:
        raise ValueError(f"manifest row_count mismatch: manifest={manifest.get('row_count')} snapshot={row_count}")
    actual_snapshot_sha256 = sha256_file(paths["snapshot"])
    if str(manifest.get("snapshot_sha256") or "").strip() != actual_snapshot_sha256:
        raise ValueError("manifest snapshot_sha256 does not match snapshot file")
    snapshot_as_of = resolve_snapshot_as_of(snapshot)
    if not snapshot_as_of:
        raise ValueError("snapshot_as_of could not be resolved from snapshot rows")
    if str(manifest.get("snapshot_as_of") or "").strip() != snapshot_as_of:
        raise ValueError(
            f"manifest snapshot_as_of mismatch: manifest={manifest.get('snapshot_as_of')} snapshot={snapshot_as_of}"
        )
    price_as_of = _coerce_date(manifest.get("price_as_of"))
    snapshot_as_of_ts = _coerce_date(snapshot_as_of)
    if price_as_of is not None and snapshot_as_of_ts is not None and price_as_of < snapshot_as_of_ts:
        raise ValueError(f"price_as_of is older than snapshot_as_of: price_as_of={price_as_of.date()} snapshot_as_of={snapshot_as_of}")

    source_fallback_used = bool(manifest.get("source_input_fallback_used"))
    source_fallback_streak = int(manifest.get("source_input_fallback_streak") or 0)
    if source_fallback_used and source_fallback_streak > int(max_source_fallback_streak):
        raise ValueError(
            "source input fallback streak exceeds publish limit: "
            f"streak={source_fallback_streak} max={int(max_source_fallback_streak)}"
        )
    return {
        "profile": profile,
        "snapshot_as_of": snapshot_as_of,
        "price_as_of": str(manifest.get("price_as_of") or ""),
        "universe_as_of": str(manifest.get("universe_as_of") or ""),
        "row_count": row_count,
        "source_input_status": str(manifest.get("source_input_status") or ""),
        "source_input_fallback_used": source_fallback_used,
        "source_input_fallback_streak": source_fallback_streak,
    }


def build_atomic_generation_publish_plan(
    *,
    profile: str,
    artifact_dir: str | Path,
    gcs_prefix: str,
    generation_id: str,
    min_row_count: int = 1,
    max_source_fallback_streak: int = 1,
) -> AtomicGenerationPublishPlan:
    normalized_generation_id = _validate_generation_id(generation_id)
    normalized_prefix = _normalize_gcs_prefix(gcs_prefix)
    contract = get_profile_contract(profile)
    validation = validate_publish_artifacts(
        profile=contract.profile,
        artifact_dir=artifact_dir,
        min_row_count=min_row_count,
        max_source_fallback_streak=max_source_fallback_streak,
    )
    paths = contract.artifact_paths(artifact_dir)
    immutable_prefix = f"{normalized_prefix}/generations/{normalized_generation_id}"
    ordered_names = ("snapshot", "manifest", "ranking", "release_summary")
    artifact_sha256 = {name: sha256_file(paths[name]) for name in ordered_names}
    artifacts = tuple(
        PublishItem(
            source=paths[name],
            destination=f"{immutable_prefix}/{paths[name].name}",
            sha256=artifact_sha256[name],
        )
        for name in ordered_names
    )
    payload = {
        "schema": "current_generation.v1",
        "profile": contract.profile,
        "generation_id": normalized_generation_id,
        "immutable_prefix": immutable_prefix,
        "snapshot_as_of": validation["snapshot_as_of"],
        "objects": {
            name: {
                "basename": paths[name].name,
                "sha256": artifact_sha256[name],
            }
            for name in ordered_names
        },
    }
    pointer_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    return AtomicGenerationPublishPlan(
        artifacts=artifacts,
        pointer_destination=f"{normalized_prefix}/current_generation.json",
        pointer_bytes=pointer_bytes,
        validation=validation,
    )


def publish_artifacts(plan: tuple[PublishItem, ...], *, dry_run: bool) -> None:
    for item in plan:
        if not item.source.exists():
            raise FileNotFoundError(f"artifact not found: {item.source}")
        command = ["gcloud", "storage", "cp", str(item.source), item.destination]
        if dry_run:
            print("DRY-RUN " + " ".join(command))
            continue
        subprocess.run(command, check=True)


def publish_atomic_generation(
    plan: AtomicGenerationPublishPlan,
    *,
    expected_pointer_generation: int,
    dry_run: bool,
) -> None:
    expected_generation = int(expected_pointer_generation)
    if expected_generation < 0:
        raise ValueError("expected_pointer_generation must be zero or a positive GCS object generation")
    with tempfile.TemporaryDirectory(prefix="snapshot-generation-pointer-") as temporary_dir:
        frozen_artifacts: list[PublishItem] = []
        for index, item in enumerate(plan.artifacts):
            if not item.sha256:
                raise ValueError(f"atomic publish item is missing its verified sha256: {item.source.name}")
            frozen_path = Path(temporary_dir) / f"{index:02d}-{item.source.name}"
            shutil.copyfile(item.source, frozen_path)
            if sha256_file(frozen_path) != item.sha256:
                raise ValueError(f"artifact changed after validation: {item.source.name}")
            frozen_artifacts.append(PublishItem(source=frozen_path, destination=item.destination, sha256=item.sha256))

        for item in frozen_artifacts:
            command = [
                "gcloud",
                "storage",
                "cp",
                str(item.source),
                item.destination,
                "--if-generation-match=0",
            ]
            if dry_run:
                print("DRY-RUN " + " ".join(command))
                continue
            subprocess.run(command, check=True)

        pointer_path = Path(temporary_dir) / "current_generation.json"
        pointer_path.write_bytes(plan.pointer_bytes)
        pointer_command = [
            "gcloud",
            "storage",
            "cp",
            str(pointer_path),
            plan.pointer_destination,
            f"--if-generation-match={expected_generation}",
        ]
        if dry_run:
            print("DRY-RUN " + " ".join(pointer_command))
            return
        subprocess.run(pointer_command, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish US equity snapshot artifacts to GCS.")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--gcs-prefix", required=True)
    parser.add_argument("--execute", action="store_true", help="Actually run gcloud storage cp. Default is dry-run.")
    parser.add_argument("--candidate-prefix", help="Optional GCS prefix for candidate artifacts before latest publish.")
    parser.add_argument("--generation-id", help="Opt in to immutable generation publish and pointer-only activation.")
    parser.add_argument(
        "--expected-pointer-generation",
        type=int,
        help="Expected current_generation.json GCS generation; use 0 only for initial creation.",
    )
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument("--max-source-fallback-streak", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    atomic_requested = args.generation_id is not None or args.expected_pointer_generation is not None
    if atomic_requested and (args.generation_id is None or args.expected_pointer_generation is None):
        parser.error("--generation-id and --expected-pointer-generation must be provided together")
    if atomic_requested and args.candidate_prefix:
        parser.error("--candidate-prefix cannot be combined with atomic generation publish")
    if args.execute and not atomic_requested and not args.candidate_prefix:
        parser.error("--execute requires atomic generation arguments or an explicit --candidate-prefix")
    if atomic_requested:
        atomic_plan = build_atomic_generation_publish_plan(
            profile=args.profile,
            artifact_dir=args.artifact_dir,
            gcs_prefix=args.gcs_prefix,
            generation_id=args.generation_id,
            min_row_count=args.min_row_count,
            max_source_fallback_streak=args.max_source_fallback_streak,
        )
        print("validated snapshot publish artifacts: " + json.dumps(atomic_plan.validation, sort_keys=True))
        publish_atomic_generation(
            atomic_plan,
            expected_pointer_generation=args.expected_pointer_generation,
            dry_run=not args.execute,
        )
        return 0

    plan = build_publish_plan(profile=args.profile, artifact_dir=args.artifact_dir, gcs_prefix=args.gcs_prefix)
    validation = validate_publish_artifacts(
        profile=args.profile,
        artifact_dir=args.artifact_dir,
        min_row_count=args.min_row_count,
        max_source_fallback_streak=args.max_source_fallback_streak,
    )
    print("validated snapshot publish artifacts: " + json.dumps(validation, sort_keys=True))
    if args.candidate_prefix:
        publish_artifacts(
            build_candidate_publish_plan(plan, candidate_prefix=args.candidate_prefix),
            dry_run=not args.execute,
        )
    publish_artifacts(plan, dry_run=not args.execute)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
