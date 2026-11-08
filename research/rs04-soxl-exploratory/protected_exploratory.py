"""Review candidate: one private SOXL replay and conditional result creation."""

from contextlib import redirect_stderr, redirect_stdout
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import exploratory as local

RESULT_OBJECT = "research/v2/soxl-exploratory/rs04e-b1b2-v1/result.json"
CREATED = "SOXL_EXPLORATORY_RESULT_CREATED"
ARCHIVE_KEYS = (
    "input_root",
    "dataset",
    "producer_revision",
    "registration",
    "manifest",
    "workload_identity_provider",
    "service_account",
    "project_id",
)
MAX_PLAN_BYTES = 48 * 1024
MAX_RESULT_BYTES = 1024 * 1024


def runner_ok(env):
    sha = env.get("GITHUB_SHA", "")
    return (
        env.get("RUNNER_OS") == "Linux"
        and env.get("GITHUB_REPOSITORY") == "QuantStrategyLab/UsEquitySnapshotPipelines"
        and env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
        and env.get("GITHUB_REF") == "refs/heads/main"
        and env.get("GITHUB_RUN_ATTEMPT") == "1"
        and re.fullmatch(r"[0-9a-f]{40}", sha) is not None
        and env.get("RS04_SOXL_EXPLORATORY_EXPECTED_SHA") == sha
        and bool(env.get("RUNNER_TEMP"))
    )


def qualify(plan_bytes, plan_digest, source_sha, data, config_bytes, issuer_bytes):
    if (
        len(plan_bytes) > MAX_PLAN_BYTES
        or not re.fullmatch(r"[0-9a-f]{64}", plan_digest)
        or sha256(plan_bytes).hexdigest() != plan_digest
    ):
        raise ValueError("PLAN_UNBOUND")
    plan = json.loads(plan_bytes)
    root = data["input_root"]
    if (
        not re.fullmatch(r"gs://[a-z0-9._-]+/research/v2/input/?", root)
        or not re.fullmatch(r"[0-9a-f]{40}", source_sha)
        or plan["source_sha"] != source_sha
        or not re.fullmatch(r"[0-9a-f]{64}", plan.get("manifest_sha256", ""))
        or plan["manifest_sha256"] != data["manifest"]["sha256"]
        or plan["archive_binding"] != {key: data[key] for key in ARCHIVE_KEYS}
        or plan["result_uri"] != f"gs://{root.split('/')[2]}/{RESULT_OBJECT}"
    ):
        raise ValueError("BINDING_MISMATCH")
    local.qualify(plan_bytes, plan_digest, config_bytes, issuer_bytes)
    return plan, root.split("/")[2]


def deliver(client_factory, data, *, plan_bytes, plan_digest, source_sha, config_bytes, issuer_bytes, scratch):
    try:
        plan, bucket_name = qualify(plan_bytes, plan_digest, source_sha, data, config_bytes, issuer_bytes)
    except Exception:
        return "SOXL_EXPLORATORY_BINDINGS_STOPPED"
    try:
        client = client_factory(plan["archive_binding"]["project_id"])
        result = local.run_local(
            plan_bytes=plan_bytes,
            reviewed_digest=plan_digest,
            config_bytes=config_bytes,
            issuer_bytes=issuer_bytes,
            data=data,
            client_factory=lambda: local.gcs_reader(client, data),
            scratch=scratch,
            source_sha=source_sha,
        )
        scope = result.get("scope", {})
        if (scope.get("classification") != "EXPLORATORY"
                or scope.get("independent_full_split_coverage") != "UNPROVEN"
                or scope.get("eligible_for_promotion") is not False
                or scope.get("strict_pit_verified") is not False
                or scope.get("production_use_authorized") is not False):
            raise ValueError("EXPLORATORY_RESULT_SCOPE_INVALID")
        body = json.dumps(result, sort_keys=True, allow_nan=False).encode()
        if len(body) > MAX_RESULT_BYTES:
            raise ValueError("EXPLORATORY_RESULT_TOO_LARGE")
    except Exception:
        return "SOXL_EXPLORATORY_REPLAY_STOPPED"
    try:
        client.bucket(bucket_name).blob(RESULT_OBJECT).upload_from_string(
            body, content_type="application/json", if_generation_match=0, retry=None, timeout=30
        )
    except Exception:
        return "SOXL_EXPLORATORY_WRITE_UNCONFIRMED"
    return CREATED


def main():
    env = os.environ
    code = "SOXL_EXPLORATORY_APPROVED_RUNNER_REQUIRED"
    if runner_ok(env):
        os.umask(0o077)
        with open(os.devnull, "w", encoding="utf-8") as sink, redirect_stdout(sink), redirect_stderr(sink):
            try:
                from inspect_gcs import IDENTITY_FIELDS, binding

                data = binding()
                if any(
                    data[key] != env.get(name)
                    for key, name in {**IDENTITY_FIELDS, "input_root": "RS04_HIST01_ROOT"}.items()
                ):
                    raise ValueError("IDENTITY_MISMATCH")
                package = Path(__file__).parent
                plan_bytes = env["RS04_SOXL_EXPLORATORY_PLAN"].encode()
                plan_digest = env["RS04_SOXL_EXPLORATORY_PLAN_SHA256"]
                config = (package / "research-baseline-defaults.json").read_bytes()
                issuer = (package / "issuer-soxl.csv").read_bytes()
                qualify(plan_bytes, plan_digest, env["GITHUB_SHA"], data, config, issuer)
                from google.cloud import storage

                with TemporaryDirectory(prefix="rs04-soxl-", dir=env["RUNNER_TEMP"]) as temp:
                    code = deliver(
                        lambda project: storage.Client(project=project),
                        data,
                        plan_bytes=plan_bytes,
                        plan_digest=plan_digest,
                        source_sha=env["GITHUB_SHA"],
                        config_bytes=config,
                        issuer_bytes=issuer,
                        scratch=Path(temp),
                    )
            except Exception:
                code = "SOXL_EXPLORATORY_BINDINGS_STOPPED"
    print(json.dumps({"status": "CREATED" if code == CREATED else "PARKED", "reason_code": code, "classification": "EXPLORATORY"}))
    return 0 if code == CREATED else 2


if __name__ == "__main__":
    raise SystemExit(main())
