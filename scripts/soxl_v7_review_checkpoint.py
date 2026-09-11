"""Persist one completed V7 result before exposing its observation receipt.

Only existing record/summary/ticket objects are bundled. This recovery file is
not a new approval or evidence policy. No input acquisition or replay runs here.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BUCKET = "qsl-runtime-logs-shared"
PREFIX = "strategy-lifecycle/v1/us_equity/soxl-v7-research-review"


class CreateOnlyStore:
    def __init__(self, bucket):
        self.bucket = bucket

    def read(self, name):
        from google.api_core.exceptions import NotFound
        try:
            return json.loads(self.bucket.blob(f"{PREFIX}/{name}").download_as_bytes())
        except NotFound:
            return None

    def create(self, name, value):
        from google.api_core.exceptions import PreconditionFailed
        blob = self.bucket.blob(f"{PREFIX}/{name}")
        try:
            blob.upload_from_string(
                json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False),
                content_type="application/json", if_generation_match=0, retry=None,
            )
        except PreconditionFailed:
            pass
        # Unknown writes escape to PARK. A later invocation only reads objects;
        # no in-call write retries and no inferred success from HTTP status.
        if self.read(name) != value:
            raise ValueError("durable V7 checkpoint mismatch")



def validate_completion(value):
    from us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation import build_soxl_v7_forward_control_plane_source
    from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_v7_forward_confirmation_p4_evidence import _sha256, FORWARD_CONFIRMATION_SUMMARY_SCHEMA
    if not isinstance(value, dict) or set(value) != {"record", "financial_summary", "ticket"}:
        raise ValueError("invalid V7 completion")
    record, summary, ticket = value["record"], value["financial_summary"], value["ticket"]
    build_soxl_v7_forward_control_plane_source(record, generated_at=record["observed_at"])
    if (
        record["controller"]["state"] != "FORWARD_COMPLETE_HUMAN_REVIEW"
        or summary["schema_version"] != FORWARD_CONFIRMATION_SUMMARY_SCHEMA
        or summary["evidence_summary_sha256"] != _sha256({k: v for k, v in summary.items() if k != "evidence_summary_sha256"})
        or summary["p1_identity"]["input_manifest_sha256"] != record["p1_manifest_sha256"]
        or summary["p2_identity"] != {"candidate_id": record["candidate_id"], "config_sha256": record["candidate_config_sha256"]}
    ):
        raise ValueError("mismatched V7 completion")
    admitted = summary["forward_confirmation_policy"]["forward_confirmation_satisfied"]
    if admitted is not True and admitted is not False:
        raise ValueError("invalid V7 financial verdict")
    if (ticket is not None) is not admitted:
        raise ValueError("V7 ticket does not match financial verdict")
    if ticket is not None and (
        ticket["state"] != "awaiting_human" or ticket["live_authority_granted"] is not False
        or ticket["strategy_profile"] != record["candidate_id"]
        or f"forward_record_sha256={record['record_sha256']}" not in ticket["notes"]
        or f"financial_evidence_sha256={summary['evidence_summary_sha256']}" not in ticket["notes"]
    ):
        raise ValueError("mismatched V7 admitted ticket")
    return value


def preserve_completion(root, store):
    record = json.loads((root / "record.json").read_text())
    if record["controller"]["state"] != "FORWARD_COMPLETE_HUMAN_REVIEW":
        return False
    ticket_path = root / "review/ticket.json"
    value = validate_completion({
        "record": record,
        "financial_summary": json.loads((root / "review/financial-summary.json").read_text()),
        "ticket": json.loads(ticket_path.read_text()) if ticket_path.exists() else None,
    })
    store.create("completed-review.json", value)
    return True


def restore_completion(root, store, requested_date):
    value = store.read("completed-review.json")
    if value is None:
        return False
    value = validate_completion(value)
    if value["record"]["last_observed_session"] > requested_date:
        return False
    root.mkdir(parents=True, exist_ok=True)
    (root / "review").mkdir(exist_ok=True)
    for name, payload in (("record.json", value["record"]), ("review/financial-summary.json", value["financial_summary"]), ("review/ticket.json", value["ticket"])):
        if payload is not None:
            (root / name).write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False))
    return True


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["restore", "preserve"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--requested-date")
    args = parser.parse_args(argv)
    try:
        from google.cloud import storage
        store = CreateOnlyStore(storage.Client().bucket(BUCKET))
        if args.operation == "restore":
            if not args.requested_date:
                raise ValueError("requested date is required")
            restored = restore_completion(args.root, store, args.requested_date)
            print("restored=" + str(restored).lower())
        else:
            preserve_completion(args.root, store)
        return 0
    except Exception:
        print("PARKED_V7_COMPLETION_UNAVAILABLE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
