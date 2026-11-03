"""Collect independent snapshot publications; never publish or trigger a workflow."""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROFILES = ("global_etf_rotation", "russell_top50_leader_rotation")
WORKFLOW = ".github/workflows/publish-snapshot-artifacts.yml"


class GitHub:
    def __init__(self, repo: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
            raise ValueError("Invalid repository")
        self.repo = repo

    def _run(self, arguments: list[str]) -> str:
        try:
            return subprocess.run(["gh", *arguments], check=True, capture_output=True,
                                  text=True, timeout=300).stdout
        except (subprocess.SubprocessError, OSError):
            # Do not forward CLI output, which can contain account or transport details.
            raise RuntimeError("GitHub read failed") from None

    def pages(self, endpoint: str, key: str) -> list[dict]:
        pages = json.loads(self._run(["api", "--paginate", "--slurp", f"repos/{self.repo}/{endpoint}"]))
        return [item for page in pages for item in page[key]]

    def download(self, run_id: int, name: str, target: Path) -> None:
        self._run(["run", "download", str(run_id), "--repo", self.repo,
                   "--name", name, "--dir", str(target)])


def positive_id(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("Invalid source identifier")
    return value


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Source timestamp needs a timezone")
    return result.astimezone(UTC)


def eligible(run: dict) -> bool:
    return (run["path"] == WORKFLOW and run["head_branch"] == "main"
            and run["status"] == "completed" and run["conclusion"] == "success")


def artifacts_for(gh: GitHub, run_id: int) -> list[dict]:
    artifacts = gh.pages(f"actions/runs/{run_id}/artifacts?per_page=100", "artifacts")
    for item in artifacts:
        positive_id(item["id"])
        if type(item["expired"]) is not bool or not isinstance(item["name"], str):
            raise ValueError("Invalid artifact metadata")
    return artifacts


def validate_snapshot(directory: Path, profile: str) -> None:
    required = (f"{profile}_feature_snapshot_latest.csv",
                f"{profile}_feature_snapshot_latest.csv.manifest.json",
                f"{profile}_ranking_latest.csv", "release_status_summary.json")
    summaries = list(directory.rglob("release_status_summary.json"))
    if len(summaries) != 1:
        raise ValueError("Missing or ambiguous release summary")
    root = summaries[0].parent
    if any(not (root / name).is_file() or (root / name).stat().st_size == 0 for name in required):
        raise ValueError("Incomplete snapshot publication")
    if json.loads(summaries[0].read_text(encoding="utf-8"))["strategy_profile"] != profile:
        raise ValueError("Snapshot profile mismatch")
    manifest = json.loads((root / required[1]).read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("Invalid snapshot manifest")
    for name in (required[0], required[2]):
        with (root / name).open(encoding="utf-8", newline="") as stream:
            rows = csv.reader(stream, strict=True)
            header = next(rows)
            if not header or any(not column.strip() for column in header):
                raise ValueError("Invalid snapshot CSV header")
            for row in rows:
                if len(row) != len(header):
                    raise ValueError("Invalid snapshot CSV row")


def publication_time(item: dict, now: datetime) -> datetime:
    created = timestamp(item["created_at"])
    published = timestamp(item.get("updated_at", item["created_at"]))
    if published < created or published > now:
        raise ValueError("Invalid publication timestamp")
    return published


def collect(gh: GitHub, output: Path, report: Path, *, now: datetime,
            source_run_id: int | None = None, max_age_days: int = 7) -> int:
    # Exclusive creation refuses both prior partial evidence and competing writers.
    # Each workflow run already has an isolated runner and workflow concurrency.
    output.mkdir(parents=True, exist_ok=False)
    report.mkdir(parents=True, exist_ok=True)
    coverage = {profile: {"status": "MISSING"} for profile in PROFILES}
    manifest = {"profiles": coverage, "max_publication_age_days": max_age_days,
                "checked_at": now.isoformat(), "status": "UNKNOWN"}
    exit_code = 1
    try:
        if max_age_days <= 0:
            raise ValueError("Invalid maximum age")
        runs = gh.pages("actions/workflows/publish-snapshot-artifacts.yml/runs"
                        "?branch=main&status=success&per_page=100", "workflow_runs")
        candidates = {profile: [] for profile in PROFILES}
        selected = {}
        trigger_artifacts = None
        # Run creation order cannot bound artifact publication order after reruns.
        for run in runs:
            if not eligible(run):
                continue
            run_id = positive_id(run["id"])
            artifacts = artifacts_for(gh, run_id)
            if run_id == source_run_id:
                trigger_artifacts = (run_id, artifacts)
            for profile in PROFILES:
                matches = [a for a in artifacts if a["name"] == f"us-equity-snapshot-{profile}-{run_id}"]
                if len(matches) > 1:
                    raise ValueError("Ambiguous snapshot artifacts")
                if matches:
                    item = matches[0]
                    candidates[profile].append((publication_time(item, now), run_id, item["id"], item))
        for profile, sources in candidates.items():
            skipped = []
            for published, run_id, artifact_id, item in sorted(sources, key=lambda source: source[:3], reverse=True):
                if item["expired"]:
                    skipped.append({"reason": "EXPIRED", "run_id": run_id, "artifact_id": artifact_id,
                                    "published_at": published.isoformat()})
                    coverage[profile] = {"status": "EXPIRED", "skipped_sources": skipped}
                    continue
                age = now - published
                selected[profile] = (run_id, item)
                coverage[profile] = {"status": "STALE" if age > timedelta(days=max_age_days) else "AVAILABLE",
                                     "run_id": run_id, "artifact_id": artifact_id,
                                     "published_at": published.isoformat(),
                                     "fallback": bool(skipped), "skipped_sources": skipped}
                break
        if source_run_id is not None and trigger_artifacts is None:
            raise ValueError("Trigger is not an eligible publication")
        if all(item["status"] == "AVAILABLE" for item in coverage.values()):
            for profile, (run_id, item) in selected.items():
                coverage[profile]["status"] = "UNKNOWN"
                gh.download(run_id, item["name"], output / profile)
                try:
                    validate_snapshot(output / profile, profile)
                except Exception:
                    coverage[profile]["status"] = "INVALID"
                    # Do not replace unreadable evidence with an older snapshot.
                    raise
                coverage[profile]["status"] = "COLLECTED"
            # Keep ancillary evidence from the trigger without importing an older
            # strategy snapshot that could win the bundle's recursive discovery.
            if trigger_artifacts:
                run_id, artifacts = trigger_artifacts
                for item in artifacts:
                    if item["name"].startswith("us-equity-snapshot-"):
                        continue
                    if item["expired"]:
                        raise ValueError("Trigger ancillary evidence expired")
                    gh.download(run_id, item["name"], output / f"ancillary-{item['id']}")
            manifest["status"] = "COLLECTED"
            exit_code = 0
        else:
            manifest["status"] = "INCOMPLETE"
    except Exception:
        # Fixed diagnostic only: metadata, parser and transport exceptions are untrusted.
        manifest["status"] = "UNKNOWN"
        for item in coverage.values():
            if item["status"] not in {"COLLECTED", "INVALID"}:
                item["status"] = "UNKNOWN"
    (report / "source_coverage.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    lines = ["## Snapshot source coverage", "", f"Collection: {manifest['status']}",
             f"Maximum publication age: {max_age_days} days (not snapshot/PIT or live validation).", ""]
    for profile, item in coverage.items():
        skipped = item.get("skipped_sources", [])
        detail = f"; fallback after {len(skipped)} newer EXPIRED publication(s)" if item.get("fallback") else ""
        lines.append(f"- {profile}: {item['status']}{detail}")
    (report / "source_coverage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--source-run-id", default="")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--max-age-days", type=int, default=7)
    args = parser.parse_args()
    try:
        return collect(GitHub(args.repo), args.output_dir, args.report_dir, now=datetime.now(UTC),
                       source_run_id=positive_id(int(args.source_run_id)) if args.source_run_id else None,
                       max_age_days=args.max_age_days)
    except Exception:
        print("Snapshot collection failed; existing output or invalid configuration. No review authorized.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
