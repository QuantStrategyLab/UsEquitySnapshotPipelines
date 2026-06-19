from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.artifacts import sha256_file
from us_equity_snapshot_pipelines.contracts import get_profile_contract
from us_equity_snapshot_pipelines.publish import build_candidate_publish_plan, build_publish_plan, validate_publish_artifacts


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
