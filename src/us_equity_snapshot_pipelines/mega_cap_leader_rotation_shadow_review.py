from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import timezone, datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .artifacts import sha256_file, write_json
from .contracts import RUSSELL_TOP50_LEADER_ROTATION_PROFILE, SOURCE_PROJECT

SHADOW_REVIEW_ARTIFACT_SCHEMA_VERSION = "russell_top50_shadow_review_artifact.v1"
RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION = "russell_top50_shadow_review.v1"
SHADOW_REVIEW_ROW_FIELDS = (
    "schema_version",
    "active_variant",
    "shadow_variant",
    "selected_count",
    "realized_stock_weight",
    "safe_haven_weight",
    "turnover_delta_vs_active",
    "largest_increase_symbol",
    "largest_increase_delta",
    "largest_decrease_symbol",
    "largest_decrease_delta",
    "review_note",
)
SENSITIVE_TOKENS = (
    "account",
    "acct",
    "token",
    "secret",
    "password",
    "cookie",
    "jwt",
    "authorization",
)


@dataclass(frozen=True)
class ShadowReviewExtraction:
    schema_version: str
    row_fields: tuple[str, ...]
    rows: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class ShadowReviewArtifactOutputs:
    csv_path: Path
    json_path: Path
    manifest_path: Path


def _load_json_object(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object expected: {path}")
    return dict(payload)


def _diagnostics_mapping(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    diagnostics = payload.get("diagnostics")
    return diagnostics if isinstance(diagnostics, Mapping) else payload


def _contains_sensitive_text(value: object) -> bool:
    text = str(value or "").lower()
    return any(token in text for token in SENSITIVE_TOKENS)


def _sanitize_row(row: Mapping[str, Any], *, row_fields: tuple[str, ...]) -> dict[str, object]:
    missing = [field for field in row_fields if field not in row]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"shadow review row missing required fields: {missing_text}")
    sanitized = {field: row[field] for field in row_fields}
    if _contains_sensitive_text(sanitized.get("review_note")):
        raise ValueError("shadow review row contains sensitive text in review_note")
    return sanitized


def extract_shadow_review_rows(payload: Mapping[str, Any]) -> ShadowReviewExtraction:
    diagnostics = _diagnostics_mapping(payload)
    schema_version = str(
        diagnostics.get("leader_rotation_shadow_review_schema_version") or RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION
    ).strip()
    row_fields_raw = diagnostics.get("leader_rotation_shadow_review_row_fields") or SHADOW_REVIEW_ROW_FIELDS
    row_fields = tuple(str(field) for field in row_fields_raw)
    if row_fields != SHADOW_REVIEW_ROW_FIELDS:
        raise ValueError(
            "unsupported shadow review row fields: "
            f"expected={SHADOW_REVIEW_ROW_FIELDS!r} actual={row_fields!r}"
        )
    if any(_contains_sensitive_text(field) for field in row_fields):
        raise ValueError("shadow review row fields include a sensitive field name")

    rows_raw = diagnostics.get("leader_rotation_shadow_review_rows") or ()
    if not isinstance(rows_raw, (list, tuple)):
        raise ValueError("leader_rotation_shadow_review_rows must be a list")
    rows: list[dict[str, object]] = []
    for row in rows_raw:
        if not isinstance(row, Mapping):
            raise ValueError("each shadow review row must be an object")
        rows.append(_sanitize_row(row, row_fields=row_fields))
    return ShadowReviewExtraction(
        schema_version=schema_version,
        row_fields=row_fields,
        rows=tuple(rows),
    )


def _artifact_payload(
    *,
    profile: str,
    snapshot_as_of: str,
    source_path: Path,
    extraction: ShadowReviewExtraction,
) -> dict[str, object]:
    return {
        "artifact_schema_version": SHADOW_REVIEW_ARTIFACT_SCHEMA_VERSION,
        "profile": profile,
        "snapshot_as_of": snapshot_as_of,
        "shadow_review_schema_version": extraction.schema_version,
        "row_fields": list(extraction.row_fields),
        "row_count": len(extraction.rows),
        "rows": list(extraction.rows),
        "source_path": str(source_path),
        "source_sha256": sha256_file(source_path),
        "source_project": SOURCE_PROJECT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _manifest_payload(
    *,
    profile: str,
    snapshot_as_of: str,
    source_path: Path,
    extraction: ShadowReviewExtraction,
    csv_path: Path,
    json_path: Path,
) -> dict[str, object]:
    return {
        "manifest_type": "shadow_review_artifact",
        "artifact_schema_version": SHADOW_REVIEW_ARTIFACT_SCHEMA_VERSION,
        "profile": profile,
        "snapshot_as_of": snapshot_as_of,
        "shadow_review_schema_version": extraction.schema_version,
        "row_count": len(extraction.rows),
        "row_fields": list(extraction.row_fields),
        "source_path": str(source_path),
        "source_sha256": sha256_file(source_path),
        "source_project": SOURCE_PROJECT,
        "artifacts": {
            "csv": {"path": str(csv_path), "sha256": sha256_file(csv_path)},
            "json": {"path": str(json_path), "sha256": sha256_file(json_path)},
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_shadow_review_artifacts(
    diagnostics_json: str | Path,
    *,
    output_dir: str | Path,
    profile: str = RUSSELL_TOP50_LEADER_ROTATION_PROFILE,
    snapshot_as_of: str = "",
) -> ShadowReviewArtifactOutputs:
    source_path = Path(diagnostics_json)
    extraction = extract_shadow_review_rows(_load_json_object(source_path))
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    prefix = str(profile or RUSSELL_TOP50_LEADER_ROTATION_PROFILE).strip() or RUSSELL_TOP50_LEADER_ROTATION_PROFILE
    csv_path = root / f"{prefix}_shadow_review_rows.csv"
    json_path = root / f"{prefix}_shadow_review_rows.json"
    manifest_path = root / f"{prefix}_shadow_review_manifest.json"

    frame = pd.DataFrame(list(extraction.rows), columns=list(extraction.row_fields))
    frame.to_csv(csv_path, index=False)
    write_json(
        json_path,
        _artifact_payload(
            profile=prefix,
            snapshot_as_of=str(snapshot_as_of or ""),
            source_path=source_path,
            extraction=extraction,
        ),
    )
    write_json(
        manifest_path,
        _manifest_payload(
            profile=prefix,
            snapshot_as_of=str(snapshot_as_of or ""),
            source_path=source_path,
            extraction=extraction,
            csv_path=csv_path,
            json_path=json_path,
        ),
    )
    return ShadowReviewArtifactOutputs(csv_path=csv_path, json_path=json_path, manifest_path=manifest_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Russell Top50 shadow review CSV/JSON artifacts from runtime diagnostics.")
    parser.add_argument("--diagnostics-json", required=True, help="Runtime diagnostics JSON or release_status_summary.json")
    parser.add_argument("--output-dir", required=True, help="Output directory for shadow review artifacts")
    parser.add_argument("--profile", default=RUSSELL_TOP50_LEADER_ROTATION_PROFILE)
    parser.add_argument("--snapshot-as-of", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    outputs = build_shadow_review_artifacts(
        args.diagnostics_json,
        output_dir=args.output_dir,
        profile=args.profile,
        snapshot_as_of=args.snapshot_as_of,
    )
    print(f"shadow_review_csv={outputs.csv_path}")
    print(f"shadow_review_json={outputs.json_path}")
    print(f"shadow_review_manifest={outputs.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
