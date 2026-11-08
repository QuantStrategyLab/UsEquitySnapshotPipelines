"""Emit aggregate company-action reconciliation for the fixed SOXL group."""
import json
import os
from pathlib import Path

from soxl_reconcile import reconcile


READ_MEMBERS = frozenset({
    "registration.json",
    "manifest.json",
    "actions/SOXL/page-001.json",
    "actions/SOXX/page-001.json",
    "actions/BOXX/page-001.json",
})


def reconcile_soxl_gcs(client, data, issuer, exact_read):
    bucket = client.bucket(data["input_root"].split("/")[2])
    prefix = f'research/v2/input/{data["dataset"]}/'
    remaining = set(READ_MEMBERS)

    def read_member(name, reference):
        if name not in remaining:
            raise ValueError("SOXL_READ_SCOPE_EXCEEDED")
        remaining.remove(name)
        return exact_read(bucket, prefix + name, reference)

    result = reconcile(data, read_member, issuer)
    if result["status"] == "SOXL_COMPARED" and remaining:
        return {"status": "PARKED", "reason_code": "SOXL_READ_SCOPE_INCOMPLETE"}
    return result


def main():
    if os.environ.get("RUNNER_OS") != "Linux" or not os.environ.get("RUNNER_TEMP"):
        print('{"status":"PARKED","reason_code":"APPROVED_LINUX_RUNNER_REQUIRED"}')
        return 2

    try:
        from google.cloud import storage
        from inspect_gcs import binding, exact_read

        data = binding()
        with Path(__file__).with_name("issuer-soxl.csv").open(encoding="utf-8", newline="") as issuer:
            result = reconcile_soxl_gcs(storage.Client(), data, issuer, exact_read)
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else ""
        if code not in {"PROTECTED_BINDING_MISSING", "PROTECTED_BINDING_INVALID"}:
            code = "SOXL_RECONCILIATION_SETUP_FAILED"
        result = {"status": "PARKED", "reason_code": code}

    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0 if result["status"] == "SOXL_COMPARED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
