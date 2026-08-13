from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest


def _git_requirement(repository: str, revision: str) -> str:
    return f"git+https://github.com/QuantStrategyLab/{repository}.git@{revision}"


def _write_project(root: Path, *, qpk_revision: str = "a" * 40, ues_revision: str = "b" * 40) -> None:
    qsp_revision = "c" * 40
    root.mkdir()
    root.joinpath("pyproject.toml").write_text(
        "\n".join(
            (
                "[project]",
                'name = "us-equity-snapshot-pipelines"',
                'requires-python = ">=3.11"',
                "dependencies = [",
                "  \"quant-platform-kit @ "
                f"{_git_requirement('QuantPlatformKit', qpk_revision)}\",",
                "  \"quant-strategy-plugins @ "
                f"{_git_requirement('QuantStrategyPlugins', qsp_revision)}\",",
                "  \"us-equity-strategies @ "
                f"{_git_requirement('UsEquityStrategies', ues_revision)}\",",
                "]",
                "",
            )
        ),
        encoding="utf-8",
    )
    root.joinpath("uv.lock").write_text(
        "\n".join(
            (
                'version = 1',
                "",
                "[[package]]",
                'name = "quant-platform-kit"',
                "source = { git = "
                f'"https://github.com/QuantStrategyLab/QuantPlatformKit.git?rev={qpk_revision}#{qpk_revision}" }}',
                "",
                "[[package]]",
                'name = "quant-strategy-plugins"',
                "source = { git = "
                f'"https://github.com/QuantStrategyLab/QuantStrategyPlugins.git?rev={qsp_revision}#{qsp_revision}" }}',
                "",
                "[[package]]",
                'name = "us-equity-strategies"',
                "source = { git = "
                f'"https://github.com/QuantStrategyLab/UsEquityStrategies.git?rev={ues_revision}#{ues_revision}" }}',
                "",
            )
        ),
        encoding="utf-8",
    )


def test_manifest_derives_identity_from_versioned_source_and_current_lock(monkeypatch, tmp_path: Path) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    _write_project(project)
    monkeypatch.setattr(runtime, "_clean_git_revision", lambda _: "d" * 40)

    manifest = runtime.derive_tqqq_offline_replay_runtime_manifest(
        project, python_major_minor="3.12"
    )

    assert manifest.to_dict() == {
        "schema_version": "qsl.tqqq.offline-replay-runtime.v1",
        "uesp_revision": "d" * 40,
        "lockfile_sha256": hashlib.sha256((project / "uv.lock").read_bytes()).hexdigest(),
        "qpk_revision": "a" * 40,
        "ues_revision": "b" * 40,
        "python_major_minor": "3.12",
    }


@pytest.mark.parametrize(
    ("dependency", "message"),
    (
        ("quant-platform-kit @ file:///tmp/QuantPlatformKit", "pinned VCS"),
        ("quant-platform-kit @ git+https://github.com/QuantStrategyLab/QuantPlatformKit.git@main", "pinned VCS"),
    ),
)
def test_manifest_rejects_mutable_or_editable_dependency_identity(
    monkeypatch, tmp_path: Path, dependency: str, message: str
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    _write_project(project)
    pyproject = project / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            _git_requirement("QuantPlatformKit", "a" * 40), dependency
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "_clean_git_revision", lambda _: "d" * 40)

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match=message):
        runtime.derive_tqqq_offline_replay_runtime_manifest(project)


def test_manifest_rejects_script_only_or_lock_mismatched_identity(monkeypatch, tmp_path: Path) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    script_only = tmp_path / "script-only"
    script_only.mkdir()
    script_only.joinpath("repair.py").write_text("print('legacy')\n", encoding="utf-8")
    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="source and lockfile"):
        runtime.derive_tqqq_offline_replay_runtime_manifest(script_only)

    project = tmp_path / "project"
    _write_project(project, qpk_revision="a" * 40)
    lock = project / "uv.lock"
    lock.write_text(lock.read_text(encoding="utf-8").replace("a" * 40, "e" * 40), encoding="utf-8")
    monkeypatch.setattr(runtime, "_clean_git_revision", lambda _: "d" * 40)
    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="lockfile identity mismatch"):
        runtime.derive_tqqq_offline_replay_runtime_manifest(project)


def test_offline_builder_uses_manifest_python_not_ci_python_and_preflights_runner_import(
    monkeypatch, tmp_path: Path
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)
    monkeypatch.setattr(runtime.sys, "version_info", SimpleNamespace(major=3, minor=13))
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        if command[:2] == ["uv", "sync"]:
            environment = kwargs["env"]
            assert isinstance(environment, dict)
            Path(environment["UV_PROJECT_ENVIRONMENT"]).joinpath("bin").mkdir(parents=True)
        if command[0] == str(target / ".venv" / "bin" / "python") and command[2] == "-c":
            return subprocess.CompletedProcess(command, 0, "3.12\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    manifest = runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert target.joinpath("manifest.json").is_file()
    assert manifest.uesp_revision == "d" * 40
    sync_command, sync_kwargs = calls[0]
    assert sync_command == [
        "uv",
        "sync",
        "--project",
        str(project),
        "--locked",
        "--python",
        "3.12",
        "--offline",
        "--no-editable",
    ]
    assert sync_kwargs["cwd"] == project
    identity_command, identity_kwargs = calls[1]
    assert identity_command == [
        str(target / ".venv" / "bin" / "python"),
        "-I",
        "-c",
        "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
    ]
    assert identity_kwargs["capture_output"] is True
    assert identity_kwargs["text"] is True
    preflight_command, _ = calls[2]
    assert preflight_command[:3] == [str(target / ".venv" / "bin" / "python"), "-I", "-c"]
    assert preflight_command[3] == (
        "from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner "
        "import run_tqqq_promotion_research"
    )
    assert "provider" not in preflight_command[3]
    assert "run_tqqq_promotion_research(" not in preflight_command[3]


def test_offline_builder_rejects_mismatched_target_interpreter_and_cleans_up(
    monkeypatch, tmp_path: Path
) -> None:
    """The CI/builder Python must not stand in for the target runtime Python."""
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)
    calls: list[list[str]] = []
    provider_calls = 0

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal provider_calls
        calls.append(command)
        if "provider" in " ".join(command):
            provider_calls += 1
        if command[:2] == ["uv", "sync"]:
            environment = kwargs["env"]
            assert isinstance(environment, dict)
            Path(environment["UV_PROJECT_ENVIRONMENT"]).joinpath("bin").mkdir(parents=True)
        if command[0] == str(target / ".venv" / "bin" / "python") and command[2] == "-c":
            return subprocess.CompletedProcess(command, 0, "3.13\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="target Python identity mismatch"):
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert not target.exists()
    assert provider_calls == 0
    assert all("provider" not in " ".join(command) for command in calls)
    assert all("run_tqqq_promotion_research(" not in " ".join(command) for command in calls)


def test_offline_builder_rejects_missing_target_interpreter_and_cleans_up(
    monkeypatch, tmp_path: Path
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["uv", "sync"]:
            environment = kwargs["env"]
            assert isinstance(environment, dict)
            Path(environment["UV_PROJECT_ENVIRONMENT"]).mkdir(parents=True)
            return subprocess.CompletedProcess(command, 0, "", "")
        raise FileNotFoundError(command[0])

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="offline replay runtime build failed"):
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert not target.exists()
    assert all("provider" not in " ".join(command) for command in calls)


def test_offline_builder_exposes_only_safe_locked_vcs_cache_miss_diagnostic(
    monkeypatch, tmp_path: Path
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)
    raw_stderr = "Remote Git fetches are not allowed because network connectivity is disabled: git+locked-vcs-source"

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            assert kwargs["capture_output"] is True
            assert kwargs["text"] is True
            raise subprocess.CalledProcessError(1, command, output="unsafe stdout", stderr=raw_stderr)
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="offline replay runtime build failed") as error:
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert error.value.diagnostic == {
        "failure_category": "offline_cache_miss",
        "source_category": "locked_vcs_source",
        "target_python": "3.12",
        "artifact_kind": "source",
    }
    assert raw_stderr not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert not target.exists()


def test_offline_builder_keeps_non_vcs_cache_miss_safe_when_local_vcs_source_exists(
    monkeypatch, tmp_path: Path
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    local_vcs_source = tmp_path / "QuantPlatformKit"
    _write_project(project)
    local_vcs_source.mkdir()
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)
    raw_stderr = "requested data wasn't found in the cache: private-package-1.0.0-py3-none-any.whl"

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            raise subprocess.CalledProcessError(1, command, output="unsafe stdout", stderr=raw_stderr)
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="offline replay runtime build failed") as error:
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert local_vcs_source.is_dir()
    assert error.value.diagnostic == {
        "failure_category": "offline_cache_miss",
        "source_category": "third_party_artifact",
        "target_python": "3.12",
        "artifact_kind": "wheel",
    }
    assert raw_stderr not in str(error.value)
    assert local_vcs_source.name not in str(error.value)
    assert not target.exists()


def test_offline_builder_does_not_call_unknown_sync_failure_a_cache_miss(monkeypatch, tmp_path: Path) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            raise subprocess.CalledProcessError(1, command, stderr="failed to resolve selected interpreter")
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError) as error:
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert error.value.diagnostic == {
        "failure_category": "offline_sync_failed",
        "source_category": "unclassified",
        "target_python": "3.12",
        "artifact_kind": None,
    }


def test_offline_builder_recognizes_build_system_cache_miss_before_wheel(monkeypatch, tmp_path: Path) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            raise subprocess.CalledProcessError(
                1,
                command,
                stderr=(
                    "Failed to resolve requirements from `build-system.requires`: "
                    "requested data wasn't found in the cache: wheel"
                ),
            )
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError) as error:
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert error.value.diagnostic == {
        "failure_category": "offline_cache_miss",
        "source_category": "build_requirement",
        "target_python": "3.12",
        "artifact_kind": "build_requirement",
    }


def test_offline_builder_does_not_call_unsatisfiable_build_requirements_a_cache_miss(
    monkeypatch, tmp_path: Path
) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            raise subprocess.CalledProcessError(
                1,
                command,
                stderr="Failed to resolve requirements from `build-system.requires`: incompatible requirements",
            )
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError) as error:
        runtime.build_tqqq_offline_replay_runtime(project, target, run=fake_run)

    assert error.value.diagnostic == {
        "failure_category": "offline_sync_failed",
        "source_category": "unclassified",
        "target_python": "3.12",
        "artifact_kind": None,
    }


def test_network_enabled_sync_failure_does_not_capture_output(monkeypatch, tmp_path: Path) -> None:
    from us_equity_snapshot_pipelines import tqqq_offline_replay_runtime as runtime

    project = tmp_path / "project"
    target = tmp_path / "runtime"
    _write_project(project)
    manifest = runtime.TqqqOfflineReplayRuntimeManifest(
        schema_version="qsl.tqqq.offline-replay-runtime.v1",
        uesp_revision="d" * 40,
        lockfile_sha256="e" * 64,
        qpk_revision="a" * 40,
        ues_revision="b" * 40,
        python_major_minor="3.12",
    )
    monkeypatch.setattr(runtime, "derive_tqqq_offline_replay_runtime_manifest", lambda _: manifest)

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["uv", "sync"]:
            assert "capture_output" not in kwargs
            assert "text" not in kwargs
            raise subprocess.CalledProcessError(1, command)
        raise AssertionError("the failed sync must stop the builder")

    with pytest.raises(runtime.TqqqOfflineReplayRuntimeError, match="offline replay runtime build failed"):
        runtime.build_tqqq_offline_replay_runtime(project, target, allow_network=True, run=fake_run)

    assert not target.exists()
