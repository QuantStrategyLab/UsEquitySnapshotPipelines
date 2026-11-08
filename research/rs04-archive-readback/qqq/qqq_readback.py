"""Return a count-only reconciliation from the previously fixed QQQ archive."""
import json
import os
from pathlib import Path

from qqq_archive import reconcile


def reconcile_qqq_gcs(client, data, issuer, exact_read):
    bucket = client.bucket(data["input_root"].split("/")[2])
    prefix = f'research/v2/input/{data["dataset"]}/'

    def read_member(name, reference):
        return exact_read(bucket, prefix + name, reference)

    return reconcile(data, read_member, issuer)


def main():
    if os.environ.get("RUNNER_OS") != "Linux" or not os.environ.get("RUNNER_TEMP"):
        print('{"status":"PARKED","reason_code":"APPROVED_LINUX_RUNNER_REQUIRED"}')
        return 2

    try:
        from google.cloud import storage
        from inspect_gcs import binding, exact_read

        data = binding()
        with Path(__file__).with_name("issuer-qqq.csv").open(encoding="utf-8", newline="") as issuer:
            result = reconcile_qqq_gcs(storage.Client(), data, issuer, exact_read)
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else ""
        if code not in {"PROTECTED_BINDING_MISSING", "PROTECTED_BINDING_INVALID"}:
            code = "QQQ_RECONCILIATION_SETUP_FAILED"
        result = {"status": "PARKED", "reason_code": code}

    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0 if result["status"] == "QQQ_COMPARED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
