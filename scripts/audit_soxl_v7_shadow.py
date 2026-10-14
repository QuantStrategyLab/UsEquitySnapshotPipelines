#!/usr/bin/env python3
"""Read existing V7 receipts in cloud CI; publish only a bounded aggregate."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

BUCKET = "qsl-runtime-logs-shared"
PREFIX = "strategy-lifecycle/v1/us_equity/soxl-v7-nonlive-forward/"
COMPLETION = "strategy-lifecycle/v1/us_equity/soxl-v7-research-review/completed-review.json"
CANDIDATE = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
CONFIG_SHA256 = "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"
UES_REVISION = "07b164d95f2ab4d4c54fd993f6f2040bd207d664"
QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
MAX_OBJECT_BYTES = 2 * 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()


def _unavailable(reason):
    return {"status": "UNCOMPUTABLE", "reason": reason, "value": None}


def summarize_records(records, *, validate_record, expected_sessions, references):
    """Receipt coverage is not proof of contemporaneous signals or executable fills."""
    if not records:
        raise ValueError("NO_EXISTING_RECEIPTS")
    records = sorted(records, key=lambda item: item["last_observed_session"])
    latest = records[-1]
    dates = latest["observation_sessions"]
    if dates != list(expected_sessions) or len(records) != len(dates):
        raise ValueError("INCOMPLETE_RECEIPT_CHAIN")
    healthy = 0
    late = 0
    previous = None
    for index, record in enumerate(records, 1):
        validate_record(record)
        receipt = record["forward_observation_receipt"]
        if (
            record["candidate_id"] != CANDIDATE
            or record["candidate_config_sha256"] != CONFIG_SHA256
            or record["observation_sessions"] != dates[:index]
            or record["controller"]["observations_completed"] != index
        ):
            raise ValueError("MISMATCHED_RECEIPT_CHAIN")
        if previous is not None and receipt["previous_receipt_sha256"] != previous["receipt_sha256"]:
            raise ValueError("BROKEN_RECEIPT_LINK")
        if previous is None and receipt["previous_receipt_sha256"] is not None:
            raise ValueError("INVALID_FIRST_RECEIPT_LINK")
        previous = receipt
        if record["controller"]["state"] in {"FORWARD_ACTIVE", "FORWARD_COMPLETE_HUMAN_REVIEW"}:
            healthy += 1
        # A conservative diagnostic, not an execution deadline or parity acceptance.
        if datetime.fromisoformat(record["observed_at"].replace("Z", "+00:00")).date().isoformat() > dates[index - 1]:
            late += 1
    return {
        "schema_version": "qsl.soxl-v7-readonly-shadow-audit.v1",
        "status": "OBSERVATION_IDENTITY_VERIFIED_METRICS_INCOMPLETE",
        "candidate_id": CANDIDATE,
        "config_sha256": CONFIG_SHA256,
        "ues_revision": UES_REVISION,
        "qpk_replay_revision": QPK_REVISION,
        "window_start_session": dates[0],
        "first_receipt_observed_at": records[0]["observed_at"],
        "last_observed_session": dates[-1],
        "last_receipt_observed_at": latest["observed_at"],
        "effective_observation_sessions": len(dates),
        "healthy_receipt_sessions": healthy,
        "receipts_observed_after_session_utc_date": late,
        "required_observation_sessions": 252,
        "controller_state": latest["controller"]["state"],
        "execution_parity_qualified_days": _unavailable("RECEIPTS_CONTAIN_DIGESTS_NOT_SIGNAL_OR_FILL_PATHS"),
        "shadow_vs_backtest_tracking_error": _unavailable("PAIRED_DAILY_NET_RETURNS_NOT_PRESERVED_IN_RECEIPTS"),
        "shadow_pnl_and_mdd_vs_live_baseline": _unavailable("NO_FROZEN_LIVE_BASELINE_OR_PAIRED_LEDGER_IN_V7_RECEIPTS"),
        "signal_match_and_fill_executability": _unavailable("SHADOW_DECISION_AND_FRACTIONAL_NEXT_CLOSE_SIMULATION_ONLY"),
        "completed_financial_review": {"status": "NOT_PRESENT"},
        "cloud_readback": {"object_count": len(references), "reference_set_sha256": _digest(references)},
        "latest_record_sha256": latest["record_sha256"],
        "research_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "no_order": True,
        "budget_effect": "NONE",
    }


def _read_cloud():
    from google.api_core.exceptions import NotFound
    from google.cloud import storage

    from scripts.soxl_v7_review_checkpoint import validate_completion
    from us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation import (
        _expected_sessions,
        build_soxl_v7_forward_control_plane_source,
    )

    client = storage.Client(project=os.environ["GCP_PROJECT_ID"])
    bucket = client.bucket(BUCKET)
    blobs = list(client.list_blobs(BUCKET, prefix=PREFIX, max_results=253))
    if not 1 <= len(blobs) <= 252:
        raise ValueError("INVALID_RECEIPT_OBJECT_COUNT")
    references = []
    total = 0

    def read(blob):
        nonlocal total
        if blob.generation is None or not 0 < int(blob.size or 0) <= MAX_OBJECT_BYTES:
            raise ValueError("INVALID_OBJECT_METADATA")
        total += int(blob.size)
        if total > MAX_TOTAL_BYTES:
            raise ValueError("READ_BUDGET_EXCEEDED")
        # Listed/reloaded generations are immutable; never read a mutable latest.
        raw = blob.download_as_bytes(if_generation_match=int(blob.generation))
        if len(raw) != int(blob.size):
            raise ValueError("OBJECT_SIZE_MISMATCH")
        references.append({"name": blob.name, "generation": str(blob.generation), "sha256": hashlib.sha256(raw).hexdigest()})
        return json.loads(raw)

    records = []
    for blob in sorted(blobs, key=lambda item: item.name):
        if re.fullmatch(re.escape(PREFIX) + r"\d{4}-\d{2}-\d{2}\.json", blob.name) is None:
            raise ValueError("UNEXPECTED_RECEIPT_OBJECT")
        record = read(blob)
        if blob.name != PREFIX + record["last_observed_session"] + ".json":
            raise ValueError("OBJECT_SESSION_MISMATCH")
        records.append(record)
    latest = max(records, key=lambda item: item["last_observed_session"])
    dates = _expected_sessions("2026-08-26", latest["last_observed_session"])
    report = summarize_records(
        records,
        validate_record=lambda record: build_soxl_v7_forward_control_plane_source(record, generated_at=record["observed_at"]),
        expected_sessions=dates,
        references=references,
    )
    completion = bucket.blob(COMPLETION)
    try:
        completion.reload()
    except NotFound:
        pass
    else:
        value = validate_completion(read(completion))
        if value["record"]["record_sha256"] != latest["record_sha256"]:
            raise ValueError("COMPLETION_RECORD_MISMATCH")
        summary = value["financial_summary"]
        # This is against unlevered SOXX, never relabelled as the live baseline.
        report["completed_financial_review"] = {
            "status": "EXISTING_VALIDATED_REVIEW",
            "evidence_summary_sha256": summary["evidence_summary_sha256"],
            "forward_confirmation_satisfied": summary["forward_confirmation_policy"]["forward_confirmation_satisfied"],
            "comparison": "SOXX_BUY_AND_HOLD_NOT_LIVE_BASELINE",
            "runs": [
                {"cost_bps": run["cost_bps"], "candidate_mdd": run["metrics"]["max_drawdown"],
                 "soxx_mdd": run["benchmark"]["max_drawdown"]}
                for run in summary["runs"]
            ],
        }
    report["cloud_readback"] = {"object_count": len(references), "reference_set_sha256": _digest(references)}
    return report


def main():
    # No caller-selected source, URL, account, command, raw output or local input mode.
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_OS") != "Linux":
        print('{"status":"CLOUD_CI_ONLY"}')
        return 2
    try:
        report = _read_cloud()
    except Exception as exc:
        # Storage/validation exception text may contain private material.
        reason = {
            "Forbidden": "CLOUD_READ_PERMISSION_DENIED",
            "PermissionDenied": "CLOUD_READ_PERMISSION_DENIED",
            "Unauthorized": "CLOUD_AUTHENTICATION_FAILED",
            "DefaultCredentialsError": "CLOUD_AUTHENTICATION_FAILED",
        }.get(type(exc).__name__, "READ_OR_VALIDATION_FAILED")
        report = {"schema_version": "qsl.soxl-v7-readonly-shadow-audit.v1", "status": "BLOCKED_READ_OR_VALIDATION",
                  "reason": reason, "no_order": True, "promotion_eligible": False, "live_ready": False}
        code = 2
    else:
        code = 0
    report["audit_uesp_revision"] = os.environ["GITHUB_SHA"]
    report["audited_at"] = datetime.now(UTC).isoformat()
    destination = Path(os.environ["RUNNER_TEMP"]) / "soxl-v7-readonly-audit" / "summary.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    print(json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
