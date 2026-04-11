from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path

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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    plan = build_publish_plan(profile=args.profile, artifact_dir=args.artifact_dir, gcs_prefix=args.gcs_prefix)
    publish_artifacts(plan, dry_run=not args.execute)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
