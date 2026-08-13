"""Build a versioned, offline-first TQQQ replay runtime without replaying it."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib
from typing import Callable


_MANIFEST_SCHEMA = "qsl.tqqq.offline-replay-runtime.v1"
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_VCS_REQUIREMENT = re.compile(
    r"^git\+https://github\.com/QuantStrategyLab/(?P<repository>[A-Za-z0-9]+)"
    r"\.git@(?P<revision>[0-9a-f]{40})$"
)
_LOCKED_VCS_SOURCE = re.compile(
    r"^https://github\.com/QuantStrategyLab/(?P<repository>[A-Za-z0-9]+)"
    r"\.git\?rev=(?P<requested>[0-9a-f]{40})#(?P<resolved>[0-9a-f]{40})$"
)
_REQUIRED_REPOSITORIES = {
    "quant-platform-kit": "QuantPlatformKit",
    "quant-strategy-plugins": "QuantStrategyPlugins",
    "us-equity-strategies": "UsEquityStrategies",
}


class TqqqOfflineReplayRuntimeError(RuntimeError):
    """The replay runtime cannot be reproduced from the committed inputs."""

    def __init__(self, message: str, *, diagnostic: dict[str, str | None] | None = None) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


@dataclass(frozen=True)
class TqqqOfflineReplayRuntimeManifest:
    schema_version: str
    uesp_revision: str
    lockfile_sha256: str
    qpk_revision: str
    ues_revision: str
    python_major_minor: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _clean_git_revision(project_root: Path) -> str:
    try:
        top_level = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if Path(top_level).resolve() != project_root.resolve():
            raise TqqqOfflineReplayRuntimeError("version-controlled source root is invalid")
        status = subprocess.run(
            ["git", "-C", str(project_root), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        revision = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise TqqqOfflineReplayRuntimeError("version-controlled source is unavailable") from exc
    if status or not _REVISION.fullmatch(revision):
        raise TqqqOfflineReplayRuntimeError("version-controlled source must be clean and pinned")
    return revision


def _project_vcs_revisions(pyproject_path: Path) -> dict[str, str]:
    try:
        document = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        dependencies = document["project"]["dependencies"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise TqqqOfflineReplayRuntimeError("committed project dependency identity is unavailable") from exc
    if not isinstance(dependencies, list):
        raise TqqqOfflineReplayRuntimeError("committed project dependency identity is unavailable")

    revisions: dict[str, str] = {}
    for dependency in dependencies:
        if not isinstance(dependency, str) or " @ " not in dependency:
            continue
        distribution, source = dependency.split(" @ ", 1)
        repository = _REQUIRED_REPOSITORIES.get(distribution)
        if repository is None:
            continue
        match = _VCS_REQUIREMENT.fullmatch(source)
        if match is None or match["repository"] != repository:
            raise TqqqOfflineReplayRuntimeError("dependencies must use pinned VCS identities")
        revisions[distribution] = match["revision"]

    if set(revisions) != set(_REQUIRED_REPOSITORIES):
        raise TqqqOfflineReplayRuntimeError("dependencies must use pinned VCS identities")
    return revisions


def _verify_lockfile_revisions(lockfile_path: Path, expected: dict[str, str]) -> None:
    try:
        document = tomllib.loads(lockfile_path.read_text(encoding="utf-8"))
        packages = document["package"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise TqqqOfflineReplayRuntimeError("committed lockfile identity is unavailable") from exc
    if not isinstance(packages, list):
        raise TqqqOfflineReplayRuntimeError("committed lockfile identity is unavailable")

    observed: dict[str, str] = {}
    for package in packages:
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        if name not in expected:
            continue
        source = package.get("source")
        source_url = source.get("git") if isinstance(source, dict) else None
        match = _LOCKED_VCS_SOURCE.fullmatch(source_url) if isinstance(source_url, str) else None
        if (
            match is None
            or match["repository"] != _REQUIRED_REPOSITORIES[name]
            or match["requested"] != match["resolved"]
        ):
            raise TqqqOfflineReplayRuntimeError("lockfile identity mismatch")
        observed[name] = match["resolved"]

    if observed != expected:
        raise TqqqOfflineReplayRuntimeError("lockfile identity mismatch")


def _python_major_minor(value: str | None) -> str:
    result = value or f"{sys.version_info.major}.{sys.version_info.minor}"
    if not re.fullmatch(r"\d+\.\d+", result):
        raise TqqqOfflineReplayRuntimeError("Python identity is invalid")
    return result


def _offline_sync_diagnostic(
    stderr: str | bytes | None, manifest: TqqqOfflineReplayRuntimeManifest
) -> dict[str, str | None]:
    """Return a fixed, non-sensitive cache-miss category without retaining uv output."""
    message = stderr.lower() if isinstance(stderr, str) else ""
    failure_category = "offline_sync_failed"
    source_category = "unclassified"
    artifact_kind: str | None = None
    cache_miss_markers = (
        "not found in the cache",
        "not available in the cache",
        "no cache entry",
        "requested data wasn't found in the cache",
        "remote git fetches are not allowed because network connectivity is disabled",
    )
    has_cache_miss = any(marker in message for marker in cache_miss_markers)
    if has_cache_miss and "failed to resolve requirements from" in message and "build-system.requires" in message:
        failure_category = "offline_cache_miss"
        source_category = "build_requirement"
        artifact_kind = "build_requirement"
    elif has_cache_miss:
        failure_category = "offline_cache_miss"
    if failure_category == "offline_cache_miss" and artifact_kind is None and (
        "git+" in message or ("github.com/" in message and ".git" in message)
    ):
        source_category = "locked_vcs_source"
        artifact_kind = "source"
    elif failure_category == "offline_cache_miss" and artifact_kind is None and (".whl" in message or "wheel" in message):
        source_category = "third_party_artifact"
        artifact_kind = "wheel"
    elif failure_category == "offline_cache_miss" and artifact_kind is None and (".tar.gz" in message or "sdist" in message):
        source_category = "third_party_artifact"
        artifact_kind = "sdist"
    return {
        "failure_category": failure_category,
        "source_category": source_category,
        "target_python": manifest.python_major_minor,
        "artifact_kind": artifact_kind,
    }


def derive_tqqq_offline_replay_runtime_manifest(
    project_root: Path, *, python_major_minor: str | None = None
) -> TqqqOfflineReplayRuntimeManifest:
    """Derive provenance only from a clean UESP checkout and its committed lockfile."""
    project_root = project_root.resolve()
    pyproject_path = project_root / "pyproject.toml"
    lockfile_path = project_root / "uv.lock"
    if not pyproject_path.is_file() or not lockfile_path.is_file():
        raise TqqqOfflineReplayRuntimeError(
            "legacy script-only identity is unusable; version-controlled source and lockfile are required"
        )

    revisions = _project_vcs_revisions(pyproject_path)
    _verify_lockfile_revisions(lockfile_path, revisions)
    return TqqqOfflineReplayRuntimeManifest(
        schema_version=_MANIFEST_SCHEMA,
        uesp_revision=_clean_git_revision(project_root),
        lockfile_sha256=hashlib.sha256(lockfile_path.read_bytes()).hexdigest(),
        qpk_revision=revisions["quant-platform-kit"],
        ues_revision=revisions["us-equity-strategies"],
        python_major_minor=_python_major_minor(python_major_minor),
    )


def _write_manifest(destination: Path, manifest: TqqqOfflineReplayRuntimeManifest) -> None:
    destination.write_text(
        json.dumps(manifest.to_dict(), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _verify_target_python_identity(
    runtime_python: Path,
    expected: str,
    *,
    project_root: Path,
    environment: dict[str, str],
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    result = run(
        [
            str(runtime_python),
            "-I",
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=project_root,
        env=environment,
    )
    observed = result.stdout.strip() if isinstance(result.stdout, str) else ""
    if observed != expected:
        raise TqqqOfflineReplayRuntimeError("target Python identity mismatch")


def build_tqqq_offline_replay_runtime(
    project_root: Path,
    target_directory: Path,
    *,
    allow_network: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> TqqqOfflineReplayRuntimeManifest:
    """Build and import-preflight an isolated runtime; replay remains a separate authority gate.

    `allow_network=True` is intentionally opt-in for an externally authorized installer.
    The default fails closed from local locked sources when the required uv cache is absent.
    """
    manifest = derive_tqqq_offline_replay_runtime_manifest(project_root)
    project_root = project_root.resolve()
    target_directory = target_directory.resolve()
    try:
        target_directory.relative_to(project_root)
    except ValueError:
        pass
    else:
        raise TqqqOfflineReplayRuntimeError("target directory must be outside source checkout")
    if target_directory.exists() or not target_directory.parent.is_dir():
        raise TqqqOfflineReplayRuntimeError("target directory must be clean and have an existing parent")

    target_directory.mkdir()
    try:
        _write_manifest(target_directory / "manifest.json", manifest)
        environment = os.environ.copy()
        environment["UV_PROJECT_ENVIRONMENT"] = str(target_directory / ".venv")
        command: list[str] = [
            "uv",
            "sync",
            "--project",
            str(project_root),
            "--locked",
            "--python",
            manifest.python_major_minor,
        ]
        if not allow_network:
            command.append("--offline")
        command.append("--no-editable")
        offline_diagnostic: dict[str, str | None] | None = None
        sync_options: dict[str, object] = {"check": True, "cwd": project_root, "env": environment}
        if not allow_network:
            sync_options.update(capture_output=True, text=True)
        try:
            run(command, **sync_options)
        except subprocess.CalledProcessError as exc:
            if allow_network:
                raise
            offline_diagnostic = _offline_sync_diagnostic(exc.stderr, manifest)
        if offline_diagnostic is not None:
            raise TqqqOfflineReplayRuntimeError(
                "offline replay runtime build failed", diagnostic=offline_diagnostic
            )
        runtime_python = target_directory / ".venv" / "bin" / "python"
        _verify_target_python_identity(
            runtime_python,
            manifest.python_major_minor,
            project_root=project_root,
            environment=environment,
            run=run,
        )
        run(
            [
                str(runtime_python),
                "-I",
                "-c",
                "from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner "
                "import run_tqqq_promotion_research",
            ],
            check=True,
            cwd=project_root,
            env=environment,
        )
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(target_directory, ignore_errors=True)
        raise TqqqOfflineReplayRuntimeError("offline replay runtime build failed") from exc
    except OSError as exc:
        shutil.rmtree(target_directory, ignore_errors=True)
        raise TqqqOfflineReplayRuntimeError("offline replay runtime build failed") from exc
    except Exception:
        shutil.rmtree(target_directory, ignore_errors=True)
        raise
    return manifest
