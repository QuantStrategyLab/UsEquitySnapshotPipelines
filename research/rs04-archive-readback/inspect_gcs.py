"""Check one registered RS-04 archive using existing Linux research credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from inspect_registered_archive import inspect_members, registered_calendar


IDENTITY_FIELDS = {
    "workload_identity_provider": "RS04_WIF_PROVIDER",
    "service_account": "RS04_WIF_SERVICE_ACCOUNT",
    "project_id": "RS04_GCP_PROJECT",
}


def binding():
    """Read the operator-installed binding without echoing its values."""
    raw = os.environ.get("RS04_HIST01_BINDING")
    if not raw:
        raise ValueError("PROTECTED_BINDING_MISSING")
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError
        patterns = {
            "input_root": r"gs://[a-z0-9._-]+/research/v2/input/?",
            "dataset": r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}",
            "producer_revision": r"[a-f0-9]{40}",
            "workload_identity_provider": (
                r"projects/[0-9]+/locations/global/workloadIdentityPools/"
                r"[a-z0-9-]+/providers/[a-z0-9-]+"
            ),
            "service_account": r"[a-z0-9._-]+@[a-z0-9.-]+\.gserviceaccount\.com",
            "project_id": r"[a-z][a-z0-9-]{4,28}[a-z0-9]",
        }
        if any(not isinstance(data[key], str) or not re.fullmatch(pattern, data[key])
               for key, pattern in patterns.items()):
            raise ValueError
        for key in ("registration", "manifest"):
            ref = data[key]
            generation, size, digest = reference(ref)
            if (not re.fullmatch(r"[1-9][0-9]*", generation) or type(size) is not int
                    or not 0 < size <= 128 * 1024 or not re.fullmatch(r"[a-f0-9]{64}", digest)):
                raise ValueError
    except (ValueError, TypeError, KeyError):
        raise ValueError("PROTECTED_BINDING_INVALID") from None
    return data


def prepare_identity(data):
    """Mask component values before the authentication action can log inputs."""
    values = {env: data[key] for key, env in IDENTITY_FIELDS.items()}
    values["RS04_HIST01_ROOT"] = data["input_root"]
    for value in (*values.values(), data["dataset"],
                  data["registration"]["generation"], data["registration"]["sha256"],
                  data["manifest"]["generation"], data["manifest"]["sha256"]):
        print(f"::add-mask::{value}", flush=True)
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as stream:
        for name, value in values.items():
            stream.write(f"{name}={value}\n")


def exact_read(bucket, name, ref):
    generation, size, digest = ref
    blob = bucket.blob(name, generation=int(generation))
    blob.reload(if_generation_match=int(generation), retry=None, timeout=30)
    if blob.size != size or str(blob.generation) != generation:
        raise ValueError("OBJECT_METADATA_MISMATCH")
    body = blob.download_as_bytes(if_generation_match=int(generation), raw_download=True, retry=None, timeout=30)
    if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("OBJECT_BYTES_MISMATCH")
    return body


def reference(row):
    return row["generation"], row["bytes"], row["sha256"]


def inspect_registered_gcs(client, data, scratch):
    bucket = client.bucket(data["input_root"].split("/")[2])
    prefix = f'research/v2/input/{data["dataset"]}/'
    registration_ref = reference(data["registration"])
    manifest_ref = reference(data["manifest"])
    registration = json.loads(exact_read(bucket, prefix + "registration.json", registration_ref))
    if (registration["schema_version"] != "rs04.archive-registration.v1"
            or registration["producer_revision"] != data["producer_revision"]
            or registration["no_order"] is not True
            or registration["manifest"]["name"] != "manifest.json"
            or reference(registration["manifest"]) != manifest_ref):
        raise ValueError("REGISTRATION_BINDING_MISMATCH")
    body = exact_read(bucket, prefix + "manifest.json", manifest_ref)
    manifest = json.loads(body)
    members = {row["name"]: row for row in registration["members"]}
    if len(members) != len(registration["members"]):
        raise ValueError("DUPLICATE_REGISTERED_MEMBER")
    expected, calendar_version = registered_calendar()
    entries = {(item["kind"], item["symbol"]): item for item in manifest["inputs"]}
    if (len(manifest["inputs"]) != 12
            or entries.keys() != {(kind, symbol) for kind in ("bars", "actions") for symbol in expected}):
        raise ValueError("REGISTERED_MEMBER_SET_MISMATCH")
    pages = []
    for (kind, symbol), item in entries.items():
        if item["complete_pagination"] is not True or not 1 <= len(item["pages"]) <= 20:
            raise ValueError("REGISTERED_PAGINATION_MISMATCH")
        for index, page in enumerate(item["pages"], 1):
            name = f"{kind}/{symbol}/page-{index:03d}.json"
            generation, size, digest = reference(members[name])
            if (page["name"] != name or (size, digest) != (page["bytes"], page["sha256"])
                    or not re.fullmatch(r"[1-9][0-9]*", generation) or not 0 < size <= 4 * 1024 * 1024):
                raise ValueError("REGISTERED_MEMBER_IDENTITY_MISMATCH")
            pages.append((name, (generation, size, digest)))
    if len(pages) > 60 or sum(ref[1] for _, ref in pages) > 128 * 1024 * 1024:
        raise ValueError("REGISTERED_READ_BUDGET_EXCEEDED")
    (scratch / "manifest.json").write_bytes(body)
    for name, ref in pages:
        path = scratch / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(exact_read(bucket, prefix + name, ref))
    result = inspect_members(scratch, manifest_ref[2], expected)
    result.update(checked_member_count=len(pages), calendar="XNYS", calendar_version=calendar_version)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-identity", action="store_true")
    args = parser.parse_args()
    if not os.environ.get("RUNNER_TEMP") or os.environ.get("RUNNER_OS") != "Linux":
        print('{"status":"PARKED","reason_code":"APPROVED_LINUX_RUNNER_REQUIRED"}')
        return 2
    os.umask(0o077)
    try:
        data = binding()
        if args.prepare_identity:
            prepare_identity(data)
            return 0
        from google.cloud import storage

        with TemporaryDirectory(prefix="rs04-readback-", dir=os.environ["RUNNER_TEMP"]) as tmp:
            result = inspect_registered_gcs(storage.Client(), data, Path(tmp))
    except Exception as exc:
        code, _, detail = str(exc).partition(":")
        if not isinstance(exc, ValueError) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", code):
            code = {403: "GCS_OBJECT_FORBIDDEN", 404: "GCS_OBJECT_MISSING",
                    412: "GCS_GENERATION_PRECONDITION_FAILED"}.get(getattr(exc, "code", None), "READBACK_FAILED")
        result = {"status": "PARKED", "reason_code": code}
        symbol = detail.partition(":")[0]
        if symbol in ("BOXX", "QQQ", "QQQM", "SOXL", "SOXX", "TQQQ"):
            result["symbol"] = symbol
    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0 if result["status"] == "STRUCTURE_CHECKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
