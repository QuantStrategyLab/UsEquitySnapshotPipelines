from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .artifacts import resolve_snapshot_as_of, sha256_file
from .contracts import get_profile_contract


@dataclass(frozen=True)
class PublishItem:
    source: Path
    destination: str


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
    return tuple(PublishItem(source=item.source, destination=f"{normalized_prefix}/{item.source.name}") for item in plan)


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


def publish_artifacts(plan: tuple[PublishItem, ...], *, dry_run: bool) -> None:
    for item in plan:
        if not item.source.exists():
            raise FileNotFoundError(f"artifact not found: {item.source}")
        command = ["gcloud", "storage", "cp", str(item.source), item.destination]
        if dry_run:
            print("DRY-RUN " + " ".join(command))
            continue
        subprocess.run(command, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish US equity snapshot artifacts to GCS.")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--gcs-prefix", required=True)
    parser.add_argument("--execute", action="store_true", help="Actually run gcloud storage cp. Default is dry-run.")
    parser.add_argument("--candidate-prefix", help="Optional GCS prefix for candidate artifacts before latest publish.")
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument("--max-source-fallback-streak", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
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
