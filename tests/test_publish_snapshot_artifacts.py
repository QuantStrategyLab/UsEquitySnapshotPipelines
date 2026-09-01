from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.artifacts import sha256_file
from us_equity_snapshot_pipelines.contracts import get_profile_contract
from us_equity_snapshot_pipelines.publish import (
    build_atomic_generation_publish_plan,
    build_candidate_publish_plan,
    build_publish_plan,
    main,
    publish_atomic_generation,
    validate_publish_artifacts,
)


PROFILE = "russell_top50_leader_rotation"


def _write_artifacts(tmp_path, *, fallback_streak: int = 0) -> None:
    contract = get_profile_contract(PROFILE)
    paths = contract.artifact_paths(tmp_path)
    snapshot = pd.DataFrame(
        [
            {"as_of": "2026-06-01", "symbol": "AAPL", "close": 100.0},
            {"as_of": "2026-06-01", "symbol": "MSFT", "close": 200.0},
        ]
    )
    snapshot.to_csv(paths["snapshot"], index=False)
    paths["ranking"].write_text("rank,symbol\n1,AAPL\n", encoding="utf-8")
    paths["release_summary"].write_text('{"release_status":"ready"}\n', encoding="utf-8")
    paths["manifest"].write_text(
        json.dumps(
            {
                "manifest_type": "feature_snapshot",
                "contract_version": contract.contract_version,
                "strategy_profile": PROFILE,
                "config_name": PROFILE,
                "config_path": "strategy_manifest_default",
                "config_sha256": "abc",
                "snapshot_path": str(paths["snapshot"]),
                "snapshot_sha256": sha256_file(paths["snapshot"]),
                "snapshot_as_of": "2026-06-01",
                "row_count": len(snapshot),
                "price_as_of": "2026-06-01",
                "source_input_fallback_used": fallback_streak > 0,
                "source_input_fallback_streak": fallback_streak,
            }
        ),
        encoding="utf-8",
    )


def test_validate_publish_artifacts_accepts_consistent_manifest(tmp_path) -> None:
    _write_artifacts(tmp_path, fallback_streak=1)

    validation = validate_publish_artifacts(profile=PROFILE, artifact_dir=tmp_path)

    assert validation["snapshot_as_of"] == "2026-06-01"
    assert validation["row_count"] == 2
    assert validation["source_input_fallback_used"] is True


def test_validate_publish_artifacts_blocks_stale_repeated_fallback(tmp_path) -> None:
    _write_artifacts(tmp_path, fallback_streak=2)

    try:
        validate_publish_artifacts(profile=PROFILE, artifact_dir=tmp_path, max_source_fallback_streak=1)
    except ValueError as exc:
        assert "fallback streak exceeds publish limit" in str(exc)
    else:
        raise AssertionError("expected stale repeated fallback to be blocked")


def test_build_candidate_publish_plan_uses_candidate_prefix(tmp_path) -> None:
    _write_artifacts(tmp_path)

    plan = build_publish_plan(profile=PROFILE, artifact_dir=tmp_path, gcs_prefix="gs://bucket/latest")
    candidate = build_candidate_publish_plan(plan, candidate_prefix="gs://bucket/candidates/123")

    assert candidate[0].destination.startswith("gs://bucket/candidates/123/")
    assert candidate[0].source == plan[0].source


@pytest.mark.parametrize("generation_id", ["", ".", "..", "../run", "/run", "run/1", "run\\1"])
def test_atomic_generation_publish_rejects_unsafe_generation_id(tmp_path, generation_id) -> None:
    _write_artifacts(tmp_path)

    with pytest.raises(ValueError, match="generation_id"):
        build_atomic_generation_publish_plan(
            profile=PROFILE,
            artifact_dir=tmp_path,
            gcs_prefix="gs://bucket/latest",
            generation_id=generation_id,
        )


def test_atomic_generation_pointer_is_canonical_and_generation_scoped(tmp_path) -> None:
    _write_artifacts(tmp_path)

    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest/",
        generation_id="123-2",
    )
    payload = json.loads(plan.pointer_bytes)

    assert plan.pointer_bytes == json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    assert payload["schema"] == "current_generation.v1"
    assert payload["profile"] == PROFILE
    assert payload["generation_id"] == "123-2"
    assert payload["immutable_prefix"] == "gs://bucket/latest/generations/123-2"
    assert payload["snapshot_as_of"] == "2026-06-01"
    assert set(payload["objects"]) == {"manifest", "ranking", "release_summary", "snapshot"}
    assert all("/" not in item["basename"] for item in payload["objects"].values())
    assert all(len(item["sha256"]) == 64 for item in payload["objects"].values())
    assert plan.pointer_destination == "gs://bucket/latest/current_generation.json"
    assert all(item.destination.startswith(payload["immutable_prefix"] + "/") for item in plan.artifacts)


@pytest.mark.parametrize("artifact_name", ["snapshot", "manifest", "ranking", "release_summary"])
def test_atomic_generation_pointer_requires_complete_verified_artifacts(tmp_path, artifact_name) -> None:
    _write_artifacts(tmp_path)
    get_profile_contract(PROFILE).artifact_paths(tmp_path)[artifact_name].unlink()

    with pytest.raises(FileNotFoundError, match=rf"{artifact_name} artifact not found"):
        build_atomic_generation_publish_plan(
            profile=PROFILE,
            artifact_dir=tmp_path,
            gcs_prefix="gs://bucket/latest",
            generation_id="123-2",
        )


def test_atomic_generation_pointer_rejects_manifest_snapshot_digest_mismatch(tmp_path) -> None:
    _write_artifacts(tmp_path)
    paths = get_profile_contract(PROFILE).artifact_paths(tmp_path)
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    manifest["snapshot_sha256"] = "0" * 64
    paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest snapshot_sha256 does not match"):
        build_atomic_generation_publish_plan(
            profile=PROFILE,
            artifact_dir=tmp_path,
            gcs_prefix="gs://bucket/latest",
            generation_id="123-2",
        )


def test_atomic_generation_pointer_uses_canonical_profile(tmp_path) -> None:
    _write_artifacts(tmp_path)

    plan = build_atomic_generation_publish_plan(
        profile="russell-top50-leader-rotation",
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )

    assert json.loads(plan.pointer_bytes)["profile"] == PROFILE


@pytest.mark.parametrize("gcs_prefix", ["", "bucket/latest", "https://bucket/latest", "gs:///latest", "gs://bucket/../latest"])
def test_atomic_generation_publish_rejects_unsafe_gcs_prefix(tmp_path, gcs_prefix) -> None:
    _write_artifacts(tmp_path)

    with pytest.raises(ValueError, match="gcs_prefix"):
        build_atomic_generation_publish_plan(
            profile=PROFILE,
            artifact_dir=tmp_path,
            gcs_prefix=gcs_prefix,
            generation_id="123-2",
        )


def test_execute_publishes_immutable_generation_before_single_cas_pointer(tmp_path, monkeypatch) -> None:
    _write_artifacts(tmp_path)
    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )
    calls: list[tuple[list[str], bytes | None]] = []

    def fake_run(command, *, check):
        source = command[3]
        pointer_bytes = None
        if command[4] == plan.pointer_destination:
            pointer_bytes = Path(source).read_bytes()
        calls.append((command, pointer_bytes))

    monkeypatch.setattr("us_equity_snapshot_pipelines.publish.subprocess.run", fake_run)

    publish_atomic_generation(plan, expected_pointer_generation=7, dry_run=False)

    assert len(calls) == 5
    assert all(call[0][-1] == "--if-generation-match=0" for call in calls[:4])
    assert all("/generations/123-2/" in call[0][4] for call in calls[:4])
    assert calls[-1][0][4] == plan.pointer_destination
    assert calls[-1][0][-1] == "--if-generation-match=7"
    assert calls[-1][1] == plan.pointer_bytes
    assert [call[0][4] for call in calls[:4]] == [item.destination for item in plan.artifacts]


def test_execute_freezes_and_rechecks_artifacts_before_upload(tmp_path, monkeypatch) -> None:
    _write_artifacts(tmp_path)
    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )
    get_profile_contract(PROFILE).artifact_paths(tmp_path)["ranking"].write_text("rank,symbol\n1,MSFT\n", encoding="utf-8")
    calls: list[list[str]] = []
    monkeypatch.setattr("us_equity_snapshot_pipelines.publish.subprocess.run", lambda command, *, check: calls.append(command))

    with pytest.raises(ValueError, match="changed after validation"):
        publish_atomic_generation(plan, expected_pointer_generation=7, dry_run=False)

    assert calls == []


def test_failed_immutable_upload_never_writes_pointer(tmp_path, monkeypatch) -> None:
    _write_artifacts(tmp_path)
    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )
    calls: list[list[str]] = []

    def fail_second_upload(command, *, check):
        calls.append(command)
        if len(calls) == 2:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr("us_equity_snapshot_pipelines.publish.subprocess.run", fail_second_upload)

    with pytest.raises(subprocess.CalledProcessError):
        publish_atomic_generation(plan, expected_pointer_generation=7, dry_run=False)

    assert len(calls) == 2
    assert all(call[4] != plan.pointer_destination for call in calls)


def test_pointer_cas_conflict_fails_closed(tmp_path, monkeypatch) -> None:
    _write_artifacts(tmp_path)
    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )

    def fail_pointer_upload(command, *, check):
        if command[4] == plan.pointer_destination:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr("us_equity_snapshot_pipelines.publish.subprocess.run", fail_pointer_upload)

    with pytest.raises(subprocess.CalledProcessError):
        publish_atomic_generation(plan, expected_pointer_generation=7, dry_run=False)


def test_atomic_generation_dry_run_never_calls_subprocess(tmp_path, monkeypatch, capsys) -> None:
    _write_artifacts(tmp_path)
    plan = build_atomic_generation_publish_plan(
        profile=PROFILE,
        artifact_dir=tmp_path,
        gcs_prefix="gs://bucket/latest",
        generation_id="123-2",
    )

    monkeypatch.setattr(
        "us_equity_snapshot_pipelines.publish.subprocess.run",
        lambda *args, **kwargs: pytest.fail("dry-run must not call subprocess"),
    )

    publish_atomic_generation(plan, expected_pointer_generation=7, dry_run=True)

    output = capsys.readouterr().out
    assert output.count("DRY-RUN gcloud storage cp") == 5
    assert output.rstrip().endswith("--if-generation-match=7")


def test_main_requires_complete_atomic_activation_arguments(tmp_path) -> None:
    _write_artifacts(tmp_path)
    base = ["--profile", PROFILE, "--artifact-dir", str(tmp_path), "--gcs-prefix", "gs://bucket/latest"]

    with pytest.raises(SystemExit):
        main([*base, "--generation-id", "123-2"])
    with pytest.raises(SystemExit):
        main([*base, "--expected-pointer-generation", "7"])
    with pytest.raises(SystemExit):
        main([*base, "--generation-id", "123-2", "--expected-pointer-generation", "7", "--candidate-prefix", "gs://candidate"])
    with pytest.raises(SystemExit):
        main([*base, "--execute"])
