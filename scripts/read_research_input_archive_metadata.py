"""GET-only research archive metadata / integrity reader for original RAW + R6 pins.

metadata_only: fixed RAW metadata + bounded complete index + four P1 metadata.
integrity_only: fixed-generation whole-byte hashes for RAW/complete/P1 plus three
contract metadata observations. No provider calls, listing, uploads, body dumps,
or research qualification claims.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Callable
from urllib.parse import urlparse

OVERALL_BUDGET_S = 30.0
ROOT_ENV = "SOXL_V7_R6_PRIVATE_ROOT"
ROOT_SHA256 = "274610e59a8d654b412af9155ee4c3bd59b90a53fc5f8bbc56091d346f60070c"
ALLOWED_REPOSITORY = "QuantStrategyLab/UsEquitySnapshotPipelines"
STORAGE_READONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
# SDK 3.12+ otherwise queues a background bucket metadata GET from Blob spans.
DISABLE_OTEL_BUCKET_METADATA_ENV = "DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA"

RAW_BUCKET = "qsl-research-evidence-831478360303"
RAW_OBJECT = "research/v2/input/qqqm-boxx-raw-20260925-001/manifest.json"
RAW_PREFIX = "research/v2/input/qqqm-boxx-raw-20260925-001/"
RAW_GENERATION = 1790338090501279
RAW_SIZE_BYTES = 7580
RAW_EXPECTED_SHA256 = "cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182"

R6_COMPLETE_NAME = "complete.json"
R6_COMPLETE_MAX_BYTES = 64 * 1024
R6_COMPLETE_GENERATION = 1790408062686989
R6_COMPLETE_SIZE_BYTES = 3390
R6_COMPLETE_SHA256 = "15bfb9d59a884e79614f555b959d888df895377f4924c7639480b106752fee46"
R6_COMPLETE_OBSERVED_AT = "2026-09-26T07:34:09Z"
R6_LICENSE_SHA256 = "c11f174c833e93443df6dd210e4d965dfb4aaa9a86406a95f434728c4ac07cdb"

P1_MAX_BYTES = 16 * 1024 * 1024
P1_KEYS = ("binding.json", "manifest.json", "closes.json", "assurance.json")
P1_NAME_MAP = {key: f"p1/{key}" for key in P1_KEYS}
P1_MANIFEST_SHA256 = "86fa48cba3459ad9ff228671cd5b5e574e0e68c1637eaa40d7487c541ff36795"
P1_PINS: dict[str, dict[str, object]] = {
    "binding.json": {
        "generation": 1790408051987526,
        "size_bytes": 832,
        "sha256": "769ab7fd151d7576755ef1ab3a01540cd1229ae671f07faeb348a59fec484f27",
    },
    "manifest.json": {
        "generation": 1790408052165082,
        "size_bytes": 2199,
        "sha256": P1_MANIFEST_SHA256,
    },
    "closes.json": {
        "generation": 1790408052331620,
        "size_bytes": 147981,
        "sha256": "e5761ee4def3b42237f605bf04e65056b2556e3bb4be7ae1b376a925b7008072",
    },
    "assurance.json": {
        "generation": 1790408052509816,
        "size_bytes": 1097,
        "sha256": "ad2b8232ea2b4f4b22f1a8c0d4e34ddfa2170798fb2fceab52afb4fbb998048b",
    },
}
INTEGRITY_MAX_BODY_BYTES = (
    RAW_SIZE_BYTES
    + R6_COMPLETE_SIZE_BYTES
    + int(P1_PINS["binding.json"]["size_bytes"])
    + int(P1_PINS["manifest.json"]["size_bytes"])
    + int(P1_PINS["closes.json"]["size_bytes"])
    + int(P1_PINS["assurance.json"]["size_bytes"])
)

STUDY_ID = "soxl_v7_twelve_basic_split_close_development_v1"
INPUT_CONTRACT_ID = "qsl.soxl-v7-twelve-single-source-r6-input.v1"
COMPLETION_SCHEMA = "soxl-v7-r6-twelve-single-completion.v1"
SIGNAL_CANDIDATE_ID = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
PRODUCER_REVISION = "0ae8ac4eb886431f9f9695702d9dd60982919dae"
DATE_CUTOFF = "2026-08-25"
SOURCE_ASSURANCE = "single_source_structural_only_no_cross_provider_verification"
BINDING_SHA256 = str(P1_PINS["binding.json"]["sha256"])

CONTRACT_SPECS: tuple[tuple[str, str], ...] = (
    (
        "tqqq_qqq_guard_cash_contract.v1.json",
        "7b603312762262ebb2fe90c86bef2ed0b7914ab26586b0b9db93ee39b4d1e60b",
    ),
    (
        "boxx_outer_cash_policy.v1.json",
        "cfed32767cb367ccce7ef880c3c15f82d50b99fe805d542e85839fc67270c905",
    ),
    (
        "s4_budget_policy.v1.json",
        "8c7a4410717c52222bb09c91a9c5d6774524625b8b2ad3226ffb2cd28dd31bbd",
    ),
)

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SHA_NAME = re.compile(r"^(?:\.\./|/|gs:|https?:)", re.IGNORECASE)


class MetadataError(Exception):
    """Sanitized group failure with a fixed reason class."""

    def __init__(self, reason_class: str) -> None:
        self.reason_class = reason_class
        super().__init__(reason_class)


def _digest(value: bytes | str) -> str:
    data = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _require_actions_identity() -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise MetadataError("ENV_REFUSED")
    if os.environ.get("GITHUB_REPOSITORY") != ALLOWED_REPOSITORY:
        raise MetadataError("ENV_REFUSED")


def _parse_root(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str) or _digest(uri) != ROOT_SHA256:
        raise MetadataError("ROOT_MISMATCH")
    if not uri.startswith("gs://") or not uri.endswith("/"):
        raise MetadataError("ROOT_INVALID")
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or parsed.query or parsed.params or parsed.fragment:
        raise MetadataError("ROOT_INVALID")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket or not prefix or ".." in prefix.split("/") or "//" in prefix:
        raise MetadataError("ROOT_INVALID")
    return bucket, prefix


def _strict_positive_int(value: object, *, reason: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataError(reason)
    if value <= 0:
        raise MetadataError(reason)
    return value


def _strict_nonneg_int(value: object, *, reason: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataError(reason)
    if value < 0:
        raise MetadataError(reason)
    return value


def _require_hex64(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise MetadataError(reason)
    return value


def _remaining(deadline: float, clock: Callable[[], float]) -> float:
    left = deadline - clock()
    if left <= 0:
        raise MetadataError("DEADLINE_EXCEEDED")
    return left


def _object_pairs_hook(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise MetadataError("COMPLETE_JSON_INVALID")
        result[key] = value
    return result


def _parse_complete_json(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object_pairs_hook,
            parse_constant=lambda _c: (_ for _ in ()).throw(MetadataError("COMPLETE_JSON_INVALID")),
        )
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001 - redact parser/transport detail
        raise MetadataError("COMPLETE_JSON_INVALID") from exc
    if not isinstance(payload, dict):
        raise MetadataError("COMPLETE_JSON_INVALID")

    def _reject_nonfinite(node: object) -> None:
        if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
            raise MetadataError("COMPLETE_JSON_INVALID")
        if isinstance(node, dict):
            for value in node.values():
                _reject_nonfinite(value)
        elif isinstance(node, list):
            for value in node:
                _reject_nonfinite(value)

    _reject_nonfinite(payload)
    return payload


def _validate_complete_identity(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != COMPLETION_SCHEMA:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("study_id") != STUDY_ID:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("producer_revision") != PRODUCER_REVISION:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("date_cutoff") != DATE_CUTOFF:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("signal_candidate_id") != SIGNAL_CANDIDATE_ID:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("source_assurance") != SOURCE_ASSURANCE:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("historical_point_in_time_certified") is not False:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    manifest_sha = _require_hex64(payload.get("p1_manifest_sha256"), reason="COMPLETE_IDENTITY_MISMATCH")
    if manifest_sha != P1_MANIFEST_SHA256:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    license_sha = _require_hex64(payload.get("license_evidence_sha256"), reason="COMPLETE_IDENTITY_MISMATCH")
    observed_at = payload.get("observed_at")
    if not isinstance(observed_at, str):
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    try:
        parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH") from exc
    if parsed.tzinfo is None:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    return {
        "schema": COMPLETION_SCHEMA,
        "study_id": STUDY_ID,
        "producer_revision": PRODUCER_REVISION,
        "date_cutoff": DATE_CUTOFF,
        "signal_candidate_id": SIGNAL_CANDIDATE_ID,
        "source_assurance": SOURCE_ASSURANCE,
        "historical_point_in_time_certified": False,
        "p1_manifest_sha256": manifest_sha,
        "license_evidence_sha256": license_sha,
        "observed_at": observed_at,
    }


def _validate_p1_receipts(payload: dict[str, Any]) -> dict[str, dict[str, object]]:
    receipts = payload.get("p1")
    if not isinstance(receipts, dict) or set(receipts) != set(P1_KEYS):
        raise MetadataError("P1_SET_INVALID")
    validated: dict[str, dict[str, object]] = {}
    for key in P1_KEYS:
        receipt = receipts[key]
        if not isinstance(receipt, dict):
            raise MetadataError("P1_RECEIPT_INVALID")
        name = receipt.get("name")
        if not isinstance(name, str) or name != P1_NAME_MAP[key]:
            raise MetadataError("P1_RECEIPT_INVALID")
        if "/" not in name or name.startswith("/") or ".." in name.split("/") or "//" in name:
            raise MetadataError("P1_RECEIPT_INVALID")
        if _SHA_NAME.search(name) is not None or "?" in name or "#" in name:
            raise MetadataError("P1_RECEIPT_INVALID")
        generation = _strict_positive_int(receipt.get("generation"), reason="P1_RECEIPT_INVALID")
        size = _strict_nonneg_int(receipt.get("size_bytes"), reason="P1_RECEIPT_INVALID")
        if size > P1_MAX_BYTES:
            raise MetadataError("P1_RECEIPT_INVALID")
        sha256 = _require_hex64(receipt.get("sha256"), reason="P1_RECEIPT_INVALID")
        if key == "manifest.json" and sha256 != P1_MANIFEST_SHA256:
            raise MetadataError("P1_RECEIPT_INVALID")
        if set(receipt) - {"name", "generation", "size_bytes", "sha256"}:
            raise MetadataError("P1_RECEIPT_INVALID")
        validated[key] = {
            "name": name,
            "generation": generation,
            "size_bytes": size,
            "sha256": sha256,
        }
    return validated


def _require_receipts_match_pins(receipts: dict[str, dict[str, object]]) -> None:
    for key in P1_KEYS:
        pin = P1_PINS[key]
        receipt = receipts[key]
        if (
            receipt["generation"] != pin["generation"]
            or receipt["size_bytes"] != pin["size_bytes"]
            or receipt["sha256"] != pin["sha256"]
        ):
            raise MetadataError("P1_RECEIPT_PIN_MISMATCH")


def _reload_metadata(
    client: Any,
    *,
    bucket_name: str,
    object_name: str,
    generation: int | None,
    expected_generation: int | None,
    expected_size: int | None,
    deadline: float,
    clock: Callable[[], float],
    mismatch_reason: str,
    unavailable_reason: str,
) -> tuple[int, int]:
    timeout = _remaining(deadline, clock)
    try:
        blob = client.bucket(bucket_name).blob(object_name, generation=generation)
        blob.reload(retry=None, timeout=timeout)
        observed_generation = blob.generation
        observed_size = blob.size
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MetadataError(unavailable_reason) from exc
    _remaining(deadline, clock)
    generation_i = _strict_positive_int(observed_generation, reason=mismatch_reason)
    size_i = _strict_nonneg_int(observed_size, reason=mismatch_reason)
    if expected_generation is not None and generation_i != expected_generation:
        raise MetadataError(mismatch_reason)
    if expected_size is not None and size_i != expected_size:
        raise MetadataError(mismatch_reason)
    return generation_i, size_i


def _download_bytes(
    client: Any,
    *,
    bucket_name: str,
    object_name: str,
    generation: int,
    size: int,
    max_size: int,
    deadline: float,
    clock: Callable[[], float],
    oversize_reason: str,
    fail_reason: str,
) -> bytes:
    if size > max_size:
        raise MetadataError(oversize_reason)
    timeout = _remaining(deadline, clock)
    try:
        blob = client.bucket(bucket_name).blob(object_name, generation=generation)
        content = blob.download_as_bytes(
            start=0,
            end=size,
            if_generation_match=generation,
            retry=None,
            timeout=timeout,
            raw_download=True,
        )
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MetadataError(fail_reason) from exc
    _remaining(deadline, clock)
    if not isinstance(content, (bytes, bytearray)) or len(content) != size:
        raise MetadataError(fail_reason)
    return bytes(content)


def _download_complete(
    client: Any,
    *,
    bucket_name: str,
    prefix: str,
    generation: int,
    size: int,
    deadline: float,
    clock: Callable[[], float],
) -> bytes:
    return _download_bytes(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=generation,
        size=size,
        max_size=R6_COMPLETE_MAX_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="COMPLETE_OVERSIZE",
        fail_reason="COMPLETE_DOWNLOAD_FAILED",
    )


def read_raw_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    generation, size = _reload_metadata(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=RAW_GENERATION,
        expected_generation=RAW_GENERATION,
        expected_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="RAW_METADATA_MISMATCH",
        unavailable_reason="RAW_METADATA_UNAVAILABLE",
    )
    _remaining(deadline, clock)
    return {
        "status": "METADATA_MATCHED",
        "content_verified": False,
        "bucket": RAW_BUCKET,
        "object": RAW_OBJECT,
        "generation": generation,
        "size_bytes": size,
        "expected_sha256": RAW_EXPECTED_SHA256,
        "note": "source_declared_sha_not_stored_in_object_metadata",
    }


def read_r6_group(
    client: Any,
    *,
    root_uri: str,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    bucket_name, prefix = _parse_root(root_uri)
    object_name = prefix + R6_COMPLETE_NAME

    # Observe actual positive generation/size once, then pin the index download.
    generation, size = _reload_metadata(
        client,
        bucket_name=bucket_name,
        object_name=object_name,
        generation=None,
        expected_generation=None,
        expected_size=None,
        deadline=deadline,
        clock=clock,
        mismatch_reason="COMPLETE_METADATA_MISMATCH",
        unavailable_reason="COMPLETE_METADATA_UNAVAILABLE",
    )
    if size > R6_COMPLETE_MAX_BYTES:
        raise MetadataError("COMPLETE_OVERSIZE")
    saved_generation, saved_size = generation, size
    body = _download_complete(
        client,
        bucket_name=bucket_name,
        prefix=prefix,
        generation=saved_generation,
        size=saved_size,
        deadline=deadline,
        clock=clock,
    )
    _remaining(deadline, clock)
    payload = _parse_complete_json(body)
    _remaining(deadline, clock)
    identity = _validate_complete_identity(payload)
    receipts = _validate_p1_receipts(payload)
    p1_meta: dict[str, object] = {}
    for key, receipt in receipts.items():
        observed_generation, observed_size = _reload_metadata(
            client,
            bucket_name=bucket_name,
            object_name=prefix + str(receipt["name"]),
            generation=int(receipt["generation"]),
            expected_generation=int(receipt["generation"]),
            expected_size=int(receipt["size_bytes"]),
            deadline=deadline,
            clock=clock,
            mismatch_reason="P1_METADATA_MISMATCH",
            unavailable_reason="P1_METADATA_UNAVAILABLE",
        )
        p1_meta[key] = {
            "name": receipt["name"],
            "generation": observed_generation,
            "size_bytes": observed_size,
            "sha256": receipt["sha256"],
            "body_read": False,
        }
    _remaining(deadline, clock)
    return {
        "status": "METADATA_READY",
        "content_verified": False,
        "completion_identity_authenticated": False,
        "complete": {
            "name": R6_COMPLETE_NAME,
            "generation": saved_generation,
            "size_bytes": saved_size,
            "sha256": _digest(body),
            **identity,
        },
        "p1": p1_meta,
    }


def _validate_manifest_binding_links(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != "research_input_manifest.v1":
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if manifest.get("profile") != STUDY_ID:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if manifest.get("research_input_contract_id") != INPUT_CONTRACT_ID:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    producer = manifest.get("producer")
    if not isinstance(producer, dict) or producer.get("commit_sha") != PRODUCER_REVISION:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    calendar = manifest.get("calendar")
    adjustment = manifest.get("adjustment")
    if not isinstance(calendar, dict) or calendar.get("source_revision") != BINDING_SHA256:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if not isinstance(adjustment, dict) or adjustment.get("source_revision") != BINDING_SHA256:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    members = manifest.get("members")
    if not isinstance(members, list) or len(members) != 2:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    by_path: dict[str, dict[str, Any]] = {}
    for item in members:
        if not isinstance(item, dict):
            raise MetadataError("MANIFEST_BINDING_MISMATCH")
        path = item.get("path")
        if not isinstance(path, str) or path in by_path:
            raise MetadataError("MANIFEST_BINDING_MISMATCH")
        by_path[path] = item
    if set(by_path) != {"closes.json", "assurance.json"}:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    for path in ("closes.json", "assurance.json"):
        pin = P1_PINS[path]
        item = by_path[path]
        size = item.get("size_bytes")
        sha256 = item.get("sha256")
        if size != pin["size_bytes"] or sha256 != pin["sha256"]:
            raise MetadataError("MANIFEST_BINDING_MISMATCH")


def read_raw_integrity_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    generation, size = _reload_metadata(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=RAW_GENERATION,
        expected_generation=RAW_GENERATION,
        expected_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="RAW_METADATA_MISMATCH",
        unavailable_reason="RAW_METADATA_UNAVAILABLE",
    )
    body = _download_bytes(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=generation,
        size=size,
        max_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="RAW_OVERSIZE",
        fail_reason="RAW_DOWNLOAD_FAILED",
    )
    digest = _digest(body)
    if digest != RAW_EXPECTED_SHA256:
        raise MetadataError("RAW_HASH_MISMATCH")
    _remaining(deadline, clock)
    return {
        "status": "INTEGRITY_MATCHED",
        "content_hash_verified": True,
        "generation": generation,
        "size_bytes": size,
        "sha256": digest,
    }


def read_r6_integrity_group(
    client: Any,
    *,
    root_uri: str,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    bucket_name, prefix = _parse_root(root_uri)
    generation, size = _reload_metadata(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=R6_COMPLETE_GENERATION,
        expected_generation=R6_COMPLETE_GENERATION,
        expected_size=R6_COMPLETE_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="COMPLETE_METADATA_MISMATCH",
        unavailable_reason="COMPLETE_METADATA_UNAVAILABLE",
    )
    body = _download_bytes(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=generation,
        size=size,
        max_size=R6_COMPLETE_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="COMPLETE_OVERSIZE",
        fail_reason="COMPLETE_DOWNLOAD_FAILED",
    )
    complete_digest = _digest(body)
    if complete_digest != R6_COMPLETE_SHA256:
        raise MetadataError("COMPLETE_HASH_MISMATCH")
    payload = _parse_complete_json(body)
    identity = _validate_complete_identity(payload)
    if identity["license_evidence_sha256"] != R6_LICENSE_SHA256:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if identity["observed_at"] not in {R6_COMPLETE_OBSERVED_AT, "2026-09-26T07:34:09+00:00"}:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    receipts = _validate_p1_receipts(payload)
    _require_receipts_match_pins(receipts)

    bodies: dict[str, bytes] = {}
    # Manifest whole-bytes first; binding links are extracted only after that hash.
    for key in ("manifest.json", "binding.json", "closes.json", "assurance.json"):
        pin = P1_PINS[key]
        object_name = prefix + P1_NAME_MAP[key]
        observed_generation, observed_size = _reload_metadata(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=int(pin["generation"]),
            expected_generation=int(pin["generation"]),
            expected_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            mismatch_reason="P1_METADATA_MISMATCH",
            unavailable_reason="P1_METADATA_UNAVAILABLE",
        )
        member = _download_bytes(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=observed_generation,
            size=observed_size,
            max_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            oversize_reason="P1_OVERSIZE",
            fail_reason="P1_DOWNLOAD_FAILED",
        )
        digest = _digest(member)
        if digest != pin["sha256"]:
            raise MetadataError("P1_HASH_MISMATCH")
        bodies[key] = member
        if key == "manifest.json":
            try:
                manifest = json.loads(
                    member.decode("utf-8"),
                    object_pairs_hook=_object_pairs_hook,
                    parse_constant=lambda _c: (_ for _ in ()).throw(
                        MetadataError("MANIFEST_JSON_INVALID")
                    ),
                )
            except MetadataError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise MetadataError("MANIFEST_JSON_INVALID") from exc
            if not isinstance(manifest, dict):
                raise MetadataError("MANIFEST_JSON_INVALID")
            _validate_manifest_binding_links(manifest)

    if _digest(bodies["binding.json"]) != BINDING_SHA256:
        raise MetadataError("P1_HASH_MISMATCH")
    _remaining(deadline, clock)
    return {
        "status": "INTEGRITY_MATCHED",
        "content_hash_verified": True,
        "completion_identity_authenticated": False,
        "complete": {
            "generation": generation,
            "size_bytes": size,
            "sha256": complete_digest,
            "p1_manifest_sha256": P1_MANIFEST_SHA256,
            "license_evidence_sha256": R6_LICENSE_SHA256,
            "observed_at": identity["observed_at"],
            "historical_point_in_time_certified": False,
        },
        "p1": {
            key: {
                "generation": int(P1_PINS[key]["generation"]),
                "size_bytes": int(P1_PINS[key]["size_bytes"]),
                "sha256": str(P1_PINS[key]["sha256"]),
                "content_hash_verified": True,
                "body_read": True,
            }
            for key in P1_KEYS
        },
    }


def read_contracts_metadata_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    contracts: dict[str, object] = {}
    for name, expected_sha in CONTRACT_SPECS:
        object_name = RAW_PREFIX + name
        try:
            generation, size = _reload_metadata(
                client,
                bucket_name=RAW_BUCKET,
                object_name=object_name,
                generation=None,
                expected_generation=None,
                expected_size=None,
                deadline=deadline,
                clock=clock,
                mismatch_reason="CONTRACT_METADATA_MISMATCH",
                unavailable_reason="CONTRACT_METADATA_UNAVAILABLE",
            )
        except MetadataError:
            raise
        if size > P1_MAX_BYTES:
            raise MetadataError("CONTRACT_OVERSIZE")
        contracts[name] = {
            "location_present": True,
            "generation": generation,
            "size_bytes": size,
            "expected_sha256": expected_sha,
            "expected_sha_status": "unverified",
            "body_read": False,
            "content_hash_verified": False,
        }
    _remaining(deadline, clock)
    return {"status": "METADATA_MATCHED", "contracts": contracts}


def _lazy_storage_client() -> Any:
    # Force the exact SDK knob before imports/ctor. Caller "false" must not re-enable
    # background bucket metadata GETs; getenv is consulted dynamically per span.
    os.environ[DISABLE_OTEL_BUCKET_METADATA_ENV] = "true"
    # Lazy imports keep ADC/client construction behind the Actions identity gate.
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import storage

    credentials, project = google.auth.default(scopes=(STORAGE_READONLY_SCOPE,))
    session = AuthorizedSession(credentials, max_refresh_attempts=0)
    return storage.Client(project=project, credentials=credentials, _http=session)


def _base_result() -> dict[str, object]:
    return {
        "status": "BLOCKED",
        "research_qualification": False,
        "trading_rights": False,
        "license_verified": False,
        "historical_point_in_time_certified": False,
        "completion_identity_authenticated": False,
        "content_verified": False,
        "content_integrity_verified": False,
    }


def run_metadata_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    root: str | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "raw": None,
        "r6": None,
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result

    root_uri = root if root is not None else os.environ.get(ROOT_ENV, "")

    raw_error: str | None = None
    r6_error: str | None = None
    try:
        _remaining(deadline, mono)
        result["raw"] = read_raw_group(client, deadline=deadline, clock=mono)
        _remaining(deadline, mono)
    except MetadataError as exc:
        raw_error = exc.reason_class
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}
    except Exception:  # noqa: BLE001
        raw_error = "RAW_METADATA_UNAVAILABLE"
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}

    try:
        _remaining(deadline, mono)
        if not isinstance(root_uri, str) or not root_uri:
            raise MetadataError("ROOT_MISMATCH")
        # Reject malformed root before any private R6 object access.
        _parse_root(root_uri)
        result["r6"] = read_r6_group(
            client,
            root_uri=root_uri,
            deadline=deadline,
            clock=mono,
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        r6_error = exc.reason_class
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}
    except Exception:  # noqa: BLE001
        r6_error = "COMPLETE_METADATA_UNAVAILABLE"
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}

    if raw_error is None and r6_error is None:
        try:
            _remaining(deadline, mono)
        except MetadataError as exc:
            result["status"] = "BLOCKED"
            result["reason_class"] = exc.reason_class
            return 2, result
        result["status"] = "METADATA_READY"
        return 0, result
    result["status"] = "BLOCKED"
    if raw_error and r6_error:
        result["reason_class"] = "GROUP_MISMATCH"
    else:
        result["reason_class"] = raw_error or r6_error
    return 2, result


def run_integrity_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    root: str | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "raw": None,
        "r6": None,
        "contracts": None,
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result

    root_uri = root if root is not None else os.environ.get(ROOT_ENV, "")
    raw_error: str | None = None
    r6_error: str | None = None
    contracts_error: str | None = None

    try:
        _remaining(deadline, mono)
        result["raw"] = read_raw_integrity_group(client, deadline=deadline, clock=mono)
        _remaining(deadline, mono)
    except MetadataError as exc:
        raw_error = exc.reason_class
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}
    except Exception:  # noqa: BLE001
        raw_error = "RAW_METADATA_UNAVAILABLE"
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}

    try:
        _remaining(deadline, mono)
        if not isinstance(root_uri, str) or not root_uri:
            raise MetadataError("ROOT_MISMATCH")
        _parse_root(root_uri)
        result["r6"] = read_r6_integrity_group(
            client,
            root_uri=root_uri,
            deadline=deadline,
            clock=mono,
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        r6_error = exc.reason_class
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}
    except Exception:  # noqa: BLE001
        r6_error = "COMPLETE_METADATA_UNAVAILABLE"
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}

    try:
        _remaining(deadline, mono)
        result["contracts"] = read_contracts_metadata_group(
            client, deadline=deadline, clock=mono
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        contracts_error = exc.reason_class
        result["contracts"] = {"status": "BLOCKED", "reason_class": contracts_error}
    except Exception:  # noqa: BLE001
        contracts_error = "CONTRACT_METADATA_UNAVAILABLE"
        result["contracts"] = {"status": "BLOCKED", "reason_class": contracts_error}

    if raw_error is None and r6_error is None and contracts_error is None:
        try:
            _remaining(deadline, mono)
        except MetadataError as exc:
            result["status"] = "BLOCKED"
            result["reason_class"] = exc.reason_class
            return 2, result
        result["status"] = "INTEGRITY_READY"
        result["content_integrity_verified"] = True
        return 0, result

    result["status"] = "BLOCKED"
    # Incomplete contracts keep overall BLOCKED even when RAW/P1 integrity matched.
    errors = [code for code in (raw_error, r6_error, contracts_error) if code]
    if len(errors) > 1:
        result["reason_class"] = "GROUP_MISMATCH"
    else:
        result["reason_class"] = errors[0]
    if raw_error is None and r6_error is None:
        result["content_integrity_verified"] = False
    return 2, result


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv == ["--integrity-only"]:
        code, payload = run_integrity_read()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return code
    if argv:
        # Root/URI must come from the protected env, never argv.
        payload = {
            **_base_result(),
            "reason_class": "ARGV_REFUSED",
        }
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 2
    code, payload = run_metadata_read()
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
