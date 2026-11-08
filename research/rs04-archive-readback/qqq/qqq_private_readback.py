"""Deliver the fixed QQQ residuals without publishing protected fields."""

import contextlib
import json
import os
import re
from pathlib import Path

from qqq_archive import archive_events, issuer_events
from qqq_residual import locate_residuals


RESULT_OBJECT = "research/v2/qqq-reconciliation/42467625448/residuals.json"


def destination(data, uri):
    """Only the one proposed result object in the original private bucket."""
    bucket = data["input_root"].split("/")[2]
    if uri != f"gs://{bucket}/{RESULT_OBJECT}":
        raise ValueError("QQQ_RESULT_BINDING_INVALID")
    return bucket


def deliver(client, data, result_uri, issuer_csv, exact_read):
    try:
        bucket = client.bucket(destination(data, result_uri))
    except Exception:
        return "QQQ_RESULT_BINDING_INVALID"

    try:
        issuer = issuer_events(issuer_csv)
        prefix = f'research/v2/input/{data["dataset"]}/'
        archive = archive_events(data, lambda name, ref: exact_read(bucket, prefix + name, ref))
        result = locate_residuals(issuer, archive)
    except Exception:
        return "QQQ_PRIVATE_READBACK_STOPPED"
    if result["status"] != "QQQ_RESIDUALS_LOCATED":
        return "QQQ_PRIOR_RESULT_NOT_REPRODUCED"

    try:
        bucket.blob(RESULT_OBJECT).upload_from_string(
            json.dumps(result, allow_nan=False, sort_keys=True),
            content_type="application/json", if_generation_match=0, retry=None, timeout=30,
        )
    except Exception:
        # A timeout can occur after a successful creation. Do not retry or overwrite.
        return "QQQ_RESULT_WRITE_UNCONFIRMED"
    return "QQQ_PRIVATE_RESULT_CREATED"


def main():
    if (os.environ.get("RUNNER_OS") != "Linux"
            or os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_RUN_ATTEMPT") != "1"
            or not re.fullmatch(r"[a-f0-9]{40}", os.environ.get("GITHUB_SHA", ""))
            or os.environ.get("GITHUB_SHA") != os.environ.get("RS04_QQQ_EXPECTED_SHA")):
        code = "QQQ_APPROVED_RUNNER_REQUIRED"
    else:
        with open(os.devnull, "w", encoding="utf-8") as sink:
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                try:
                    from google.cloud import storage
                    from inspect_gcs import binding, exact_read

                    data = binding()
                    uri = os.environ.get("RS04_QQQ_PRIVATE_RESULT_URI", "")
                    destination(data, uri)
                    with Path(__file__).with_name("issuer-qqq.csv").open(encoding="utf-8", newline="") as issuer:
                        code = deliver(storage.Client(), data, uri, issuer, exact_read)
                except ValueError:
                    code = "QQQ_PRIVATE_BINDING_INVALID"
                except Exception:
                    code = "QQQ_PRIVATE_SETUP_STOPPED"
    created = code == "QQQ_PRIVATE_RESULT_CREATED"
    print(json.dumps({"status": "CREATED" if created else "PARKED", "reason_code": code}))
    return 0 if created else 2


if __name__ == "__main__":
    raise SystemExit(main())
