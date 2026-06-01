from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import date
from pathlib import Path


def _materialize_manifest(reference: str) -> Path:
    raw = str(reference or "").strip()
    if not raw:
        raise ValueError("manifest reference is required")
    if not raw.startswith("gs://"):
        path = Path(raw)
        if not path.exists():
            raise FileNotFoundError(f"manifest not found: {path}")
        return path
    target = Path(tempfile.mkdtemp(prefix="snapshot-health-")) / Path(raw).name
    subprocess.run(["gcloud", "storage", "cp", raw, str(target)], check=True)
    return target


def _month_lag(snapshot_as_of: date, run_as_of: date) -> int:
    return (run_as_of.year - snapshot_as_of.year) * 12 + (run_as_of.month - snapshot_as_of.month)


def _parse_date(value: object, *, field: str) -> date:
    if not value:
        raise ValueError(f"{field} is required")
    return date.fromisoformat(str(value)[:10])


def check_manifest_health(*, manifest_reference: str, run_as_of: str | None, max_month_lag: int) -> dict[str, object]:
    manifest_path = _materialize_manifest(manifest_reference)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("snapshot manifest must contain a JSON object")

    required_fields = (
        "manifest_type",
        "contract_version",
        "strategy_profile",
        "snapshot_as_of",
        "snapshot_sha256",
        "row_count",
    )
    missing = [field for field in required_fields if not payload.get(field)]
    if missing:
        raise ValueError(f"manifest missing required fields: {','.join(missing)}")
    row_count = int(payload.get("row_count") or 0)
    if row_count <= 0:
        raise ValueError(f"manifest row_count must be positive: {row_count}")

    snapshot_as_of = _parse_date(payload.get("snapshot_as_of"), field="snapshot_as_of")
    effective_run_as_of = date.fromisoformat(run_as_of) if run_as_of else date.today()
    lag = _month_lag(snapshot_as_of, effective_run_as_of)
    if lag > int(max_month_lag):
        raise ValueError(
            "snapshot manifest is stale: "
            f"snapshot_as_of={snapshot_as_of.isoformat()} run_as_of={effective_run_as_of.isoformat()} "
            f"max_month_lag={int(max_month_lag)}"
        )

    price_as_of = payload.get("price_as_of")
    if price_as_of and _parse_date(price_as_of, field="price_as_of") < snapshot_as_of:
        raise ValueError(f"price_as_of is older than snapshot_as_of: price_as_of={price_as_of}")

    return {
        "manifest": manifest_reference,
        "strategy_profile": payload.get("strategy_profile"),
        "snapshot_as_of": snapshot_as_of.isoformat(),
        "price_as_of": price_as_of,
        "universe_as_of": payload.get("universe_as_of"),
        "row_count": row_count,
        "source_input_status": payload.get("source_input_status"),
        "source_input_fallback_used": bool(payload.get("source_input_fallback_used")),
        "source_input_fallback_streak": int(payload.get("source_input_fallback_streak") or 0),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check a published feature snapshot manifest for freshness.")
    parser.add_argument("--manifest", required=True, help="Local or gs:// manifest path")
    parser.add_argument("--run-as-of", help="Run date in YYYY-MM-DD format; defaults to today")
    parser.add_argument("--max-month-lag", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_manifest_health(
        manifest_reference=args.manifest,
        run_as_of=args.run_as_of,
        max_month_lag=args.max_month_lag,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
