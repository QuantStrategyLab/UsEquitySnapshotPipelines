from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.global_etf_rotation_shadow_review import (
    SHADOW_REVIEW_ARTIFACT_SCHEMA_VERSION,
    build_shadow_review_artifacts,
    extract_shadow_review_rows,
    main,
)


ROW_FIELDS = [
    "schema_version",
    "candidate",
    "active_candidate",
    "shadow_candidate",
    "selected_count",
    "offensive_weight",
    "safe_haven_weight",
    "turnover_delta_vs_active",
    "shadow_review_passed",
    "review_note",
]


def _diagnostics_payload() -> dict[str, object]:
    return {
        "strategy_profile": "global_etf_rotation",
        "diagnostics": {
            "global_etf_shadow_review_schema_version": "global_etf_shadow_review.v1",
            "global_etf_shadow_review_row_fields": ROW_FIELDS,
            "global_etf_shadow_review_rows": [
                {
                    "schema_version": "global_etf_shadow_review.v1",
                    "candidate": "liveable_blend_baseline90_fast10",
                    "active_candidate": "live_global_etf_rotation_defensive_baseline",
                    "shadow_candidate": "liveable_blend_baseline90_fast10",
                    "selected_count": 2,
                    "offensive_weight": 0.10,
                    "safe_haven_weight": 0.90,
                    "turnover_delta_vs_active": 0.04,
                    "shadow_review_passed": True,
                    "review_note": "shadow candidate stable and turnover delta acceptable",
                    "account_id": "SHOULD_NOT_LEAK",
                }
            ],
        },
    }


def test_extract_shadow_review_rows_accepts_release_summary_and_filters_allowed_fields() -> None:
    extracted = extract_shadow_review_rows(_diagnostics_payload())

    assert extracted.schema_version == "global_etf_shadow_review.v1"
    assert extracted.row_fields == tuple(ROW_FIELDS)
    assert len(extracted.rows) == 1
    assert tuple(extracted.rows[0]) == tuple(ROW_FIELDS)
    assert "account_id" not in extracted.rows[0]
    assert extracted.rows[0]["candidate"] == "liveable_blend_baseline90_fast10"


def test_build_shadow_review_artifacts_writes_csv_json_and_manifest(tmp_path: Path) -> None:
    diagnostics_path = tmp_path / "operator_review.json"
    diagnostics_path.write_text(json.dumps(_diagnostics_payload()), encoding="utf-8")

    outputs = build_shadow_review_artifacts(
        diagnostics_path,
        output_dir=tmp_path / "out",
        profile="global_etf_rotation",
        snapshot_as_of="2026-06-30",
    )

    assert outputs.csv_path.exists()
    assert outputs.json_path.exists()
    assert outputs.manifest_path.exists()
    rows = pd.read_csv(outputs.csv_path)
    manifest = json.loads(outputs.manifest_path.read_text(encoding="utf-8"))
    payload = json.loads(outputs.json_path.read_text(encoding="utf-8"))
    assert list(rows.columns) == ROW_FIELDS
    assert manifest["artifact_schema_version"] == SHADOW_REVIEW_ARTIFACT_SCHEMA_VERSION
    assert manifest["profile"] == "global_etf_rotation"
    assert manifest["snapshot_as_of"] == "2026-06-30"
    assert manifest["row_count"] == 1
    assert manifest["shadow_review_schema_version"] == "global_etf_shadow_review.v1"
    assert "sha256" in manifest["artifacts"]["csv"]
    assert payload["rows"][0]["candidate"] == "liveable_blend_baseline90_fast10"


def test_shadow_review_rejects_sensitive_review_note() -> None:
    payload = _diagnostics_payload()
    rows = payload["diagnostics"]["global_etf_shadow_review_rows"]
    rows[0]["review_note"] = "account=U123 candidate=liveable_blend_baseline90_fast10"

    try:
        extract_shadow_review_rows(payload)
    except ValueError as exc:
        assert "sensitive" in str(exc)
    else:
        raise AssertionError("expected sensitive review note rejection")


def test_shadow_review_cli_writes_outputs(tmp_path: Path) -> None:
    diagnostics_path = tmp_path / "diagnostics.json"
    diagnostics_path.write_text(json.dumps(_diagnostics_payload()), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(
        [
            "--diagnostics-json",
            str(diagnostics_path),
            "--output-dir",
            str(output_dir),
            "--profile",
            "global_etf_rotation",
            "--snapshot-as-of",
            "2026-06-30",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "global_etf_rotation_shadow_review_rows.csv").exists()
    assert (output_dir / "global_etf_rotation_shadow_review_rows.json").exists()
    assert (output_dir / "global_etf_rotation_shadow_review_manifest.json").exists()
