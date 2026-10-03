"""Synthetic GET-only checks for research archive metadata entry."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import google.auth.credentials
import pytest
import requests
import urllib3

from scripts import read_research_input_archive_metadata as reader

WORKFLOW = Path(".github/workflows/soxl-v7-r6-twelve-single-source.yml")
REPO_ROOT = Path(__file__).resolve().parents[1]

SYN_ROOT = "gs://synthetic-r6-private-bucket/exact-study-root/"
SYN_COMPLETE_GEN = 9001
P1_GEN = {
    "binding.json": 11,
    "manifest.json": 12,
    "closes.json": 13,
    "assurance.json": 14,
}
P1_SIZE = {
    "binding.json": 21,
    "manifest.json": 22,
    "closes.json": 23,
    "assurance.json": 24,
}


def _sha(data: bytes | str) -> str:
    raw = data if isinstance(data, bytes) else data.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _actions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_REPOSITORY", reader.ALLOWED_REPOSITORY)
    monkeypatch.setenv(reader.ROOT_ENV, SYN_ROOT)
    monkeypatch.setattr(reader, "ROOT_SHA256", _sha(SYN_ROOT))


class FakeBlob:
    def __init__(
        self,
        store: dict[tuple[str, str], dict[str, Any]],
        bucket: str,
        name: str,
        generation: int | None,
        events: list[tuple[str, object]],
    ) -> None:
        self.store = store
        self.bucket = bucket
        self.name = name
        self.generation = generation
        self.events = events
        self.size: int | None = None

    def reload(self, *, retry: object, timeout: float) -> None:
        assert retry is None
        assert timeout > 0
        key = (self.bucket, self.name)
        self.events.append(("reload", (self.bucket, self.name, self.generation, timeout)))
        if key not in self.store:
            raise RuntimeError("missing")
        item = self.store[key]
        self.generation = item["generation"]
        self.size = item["size"]

    def download_as_bytes(
        self,
        *,
        start: int,
        end: int,
        if_generation_match: int,
        retry: object,
        timeout: float,
        raw_download: bool = False,
    ) -> bytes:
        assert retry is None
        assert timeout > 0
        assert start == 0
        assert raw_download is True
        self.events.append(
            (
                "download",
                (self.bucket, self.name, if_generation_match, start, end, timeout, raw_download),
            )
        )
        item = self.store[(self.bucket, self.name)]
        body = item["body"]
        if item["generation"] != if_generation_match:
            raise RuntimeError("generation precondition")
        if not raw_download and body[:2] == b"\x1f\x8b":
            return gzip.decompress(body)
        return body[start : end + 1]

    def upload_from_string(self, *_a: object, **_k: object) -> None:
        raise AssertionError("upload unreachable")

    def delete(self, *_a: object, **_k: object) -> None:
        raise AssertionError("delete unreachable")


class FakeBucket:
    def __init__(
        self,
        store: dict[tuple[str, str], dict[str, Any]],
        name: str,
        events: list[tuple[str, object]],
    ) -> None:
        self.store = store
        self.name = name
        self.events = events

    def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
        return FakeBlob(self.store, self.name, name, generation, self.events)

    def list_blobs(self, *_a: object, **_k: object) -> None:
        raise AssertionError("list unreachable")

    def copy_blob(self, *_a: object, **_k: object) -> None:
        raise AssertionError("copy unreachable")


class FakeClient:
    def __init__(self, store: dict[tuple[str, str], dict[str, Any]]) -> None:
        self.store = store
        self.events: list[tuple[str, object]] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, name, self.events)

    def list_buckets(self, *_a: object, **_k: object) -> None:
        raise AssertionError("list_buckets unreachable")

    def create_bucket(self, *_a: object, **_k: object) -> None:
        raise AssertionError("create_bucket unreachable")


class SequencedClock:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)
        self.i = 0

    def __call__(self) -> float:
        if self.i >= len(self.values):
            return self.values[-1]
        value = self.values[self.i]
        self.i += 1
        return value


class MutableClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class FakeCredentials(google.auth.credentials.Credentials):
    def __init__(self) -> None:
        super().__init__()
        self.token = "synthetic-access-token"
        self.refresh_calls = 0

    def refresh(self, request: object) -> None:  # noqa: ANN001
        self.refresh_calls += 1
        raise AssertionError("401 must not trigger credential refresh")


_BUCKET_ONLY_PATH = re.compile(r"^/storage/v1/b/[^/]+$")


def _is_bucket_metadata_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    return _BUCKET_ONLY_PATH.fullmatch(path) is not None


def _patch_lazy_auth(monkeypatch: pytest.MonkeyPatch, creds: FakeCredentials) -> None:
    monkeypatch.setattr(
        "google.auth.default",
        lambda scopes=None, **_kwargs: (creds, "synthetic-project"),
    )


def _complete_body() -> bytes:
    p1 = {
        key: {
            "name": f"p1/{key}",
            "generation": P1_GEN[key],
            "size_bytes": P1_SIZE[key],
            "sha256": reader.P1_MANIFEST_SHA256 if key == "manifest.json" else _sha(f"syn-{key}"),
        }
        for key in reader.P1_KEYS
    }
    payload = {
        "schema": reader.COMPLETION_SCHEMA,
        "study_id": reader.STUDY_ID,
        "signal_candidate_id": reader.SIGNAL_CANDIDATE_ID,
        "source_assurance": reader.SOURCE_ASSURANCE,
        "historical_point_in_time_certified": False,
        "date_cutoff": reader.DATE_CUTOFF,
        "producer_revision": reader.PRODUCER_REVISION,
        "license_evidence_sha256": "a" * 64,
        "observed_at": "2026-09-26T12:00:00+00:00",
        "p1_manifest_sha256": reader.P1_MANIFEST_SHA256,
        "p1": p1,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _store(complete: bytes | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    body = complete if complete is not None else _complete_body()
    prefix = "exact-study-root/"
    store: dict[tuple[str, str], dict[str, Any]] = {
        (reader.RAW_BUCKET, reader.RAW_OBJECT): {
            "generation": reader.RAW_GENERATION,
            "size": reader.RAW_SIZE_BYTES,
            "body": b"RAW-BODY-MUST-NOT-BE-READ",
        },
        ("synthetic-r6-private-bucket", prefix + reader.R6_COMPLETE_NAME): {
            "generation": SYN_COMPLETE_GEN,
            "size": len(body),
            "body": body,
        },
    }
    for key in reader.P1_KEYS:
        store[("synthetic-r6-private-bucket", prefix + f"p1/{key}")] = {
            "generation": P1_GEN[key],
            "size": P1_SIZE[key],
            "body": b"P1-BODY-MUST-NOT-BE-READ",
        }
    return store


def test_successful_exact_seven_requests_and_sanitized_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()
    client = FakeClient(_store(body))
    code, payload = reader.run_metadata_read(client=client)
    assert code == 0
    assert payload["status"] == "METADATA_READY"
    assert payload["research_qualification"] is False
    assert payload["trading_rights"] is False
    assert payload["content_verified"] is False
    raw = payload["raw"]
    assert isinstance(raw, dict)
    assert raw["status"] == "METADATA_MATCHED"
    assert raw["content_verified"] is False
    assert raw["expected_sha256"] == reader.RAW_EXPECTED_SHA256
    assert raw["generation"] == reader.RAW_GENERATION
    assert raw["size_bytes"] == reader.RAW_SIZE_BYTES
    r6 = payload["r6"]
    assert isinstance(r6, dict)
    assert r6["status"] == "METADATA_READY"
    assert r6["completion_identity_authenticated"] is False
    assert r6["complete"]["generation"] == SYN_COMPLETE_GEN
    assert r6["complete"]["size_bytes"] == len(body)
    assert r6["complete"]["sha256"] == _sha(body)
    assert r6["complete"]["p1_manifest_sha256"] == reader.P1_MANIFEST_SHA256
    ops = [event[0] for event in client.events]
    assert ops.count("reload") == 6
    assert ops.count("download") == 1
    assert len(client.events) == 7
    download = next(event for event in client.events if event[0] == "download")
    assert download[1][1].endswith("complete.json")
    assert download[1][2] == SYN_COMPLETE_GEN
    assert download[1][4] == len(body)
    assert download[1][6] is True
    for event in client.events:
        if event[0] == "reload":
            assert event[1][3] > 0
            if event[1][1] == reader.RAW_OBJECT:
                assert event[1][2] == reader.RAW_GENERATION
        if event[0] == "download":
            assert event[1][5] > 0
    text = json.dumps(payload)
    assert "RAW-BODY" not in text
    assert "P1-BODY" not in text


def test_raw_and_p1_bodies_never_read(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()
    store = _store(body)

    class GuardBlob(FakeBlob):
        def download_as_bytes(self, **kwargs: Any) -> bytes:  # type: ignore[override]
            if self.name != "exact-study-root/complete.json":
                raise AssertionError(f"unexpected download {self.name}")
            return super().download_as_bytes(**kwargs)

    class GuardBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return GuardBlob(self.store, self.name, name, generation, self.events)

    class GuardClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return GuardBucket(self.store, name, self.events)

    client = GuardClient(store)
    code, _payload = reader.run_metadata_read(client=client)
    assert code == 0


def test_complete_download_raw_download_no_gzip_expansion(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    plain = (_complete_body()[:-1] + b" ") * 2000
    assert len(plain) > 1024 * 1024
    compressed = gzip.compress(plain)
    assert len(compressed) < reader.R6_COMPLETE_MAX_BYTES
    store = _store(compressed)
    store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")] = {
        "generation": SYN_COMPLETE_GEN,
        "size": len(compressed),
        "body": compressed,
    }
    client = FakeClient(store)
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_JSON_INVALID"
    download = next(event for event in client.events if event[0] == "download")
    assert download[1][6] is True
    assert download[1][4] == len(compressed)
    assert not any(len(gzip.decompress(compressed)) == event[1][4] for event in client.events if event[0] == "download")


def test_cli_refuses_before_client_outside_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", reader.ALLOWED_REPOSITORY)
    created = {"client": False}

    def boom() -> FakeClient:
        created["client"] = True
        raise AssertionError("client must not be created")

    code, payload = reader.run_metadata_read(create_client=boom)
    assert code == 2
    assert payload["reason_class"] == "ENV_REFUSED"
    assert created["client"] is False


def test_cli_refuses_wrong_repository_before_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_REPOSITORY", "QuantStrategyLab/Other")
    created = {"client": False}

    def boom() -> FakeClient:
        created["client"] = True
        raise AssertionError("client must not be created")

    code, payload = reader.run_metadata_read(create_client=boom)
    assert code == 2
    assert payload["reason_class"] == "ENV_REFUSED"
    assert created["client"] is False


def test_main_refuses_argv(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _actions(monkeypatch)
    assert reader.main(["gs://evil"]) == 2
    out = json.loads(capsys.readouterr().out)
    assert out["reason_class"] == "ARGV_REFUSED"
    assert "gs://" not in json.dumps(out)


def test_module_main_argv_refused_subprocess() -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"GITHUB_ACTIONS", "GITHUB_REPOSITORY", reader.ROOT_ENV}
    }
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.read_research_input_archive_metadata", "gs://evil-root"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["reason_class"] == "ARGV_REFUSED"
    assert "gs://" not in proc.stdout
    assert "evil-root" not in proc.stdout
    assert "evil-root" not in proc.stderr


def test_malformed_root_blocks_r6_before_private_access(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    monkeypatch.setenv(reader.ROOT_ENV, "gs://synthetic-r6-private-bucket/other/")
    body = _complete_body()
    client = FakeClient(_store(body))
    code, payload = reader.run_metadata_read(client=client)
    assert code == 2
    assert payload["raw"]["status"] == "METADATA_MATCHED"
    assert payload["r6"]["reason_class"] == "ROOT_MISMATCH"
    private_ops = [e for e in client.events if e[0] in {"reload", "download"} and e[1][0] != reader.RAW_BUCKET]
    assert private_ops == []


def test_complete_identity_and_json_invariants(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()
    payload = json.loads(body.decode())
    payload["historical_point_in_time_certified"] = 0
    bad = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    client = FakeClient(_store(bad))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = len(bad)
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_IDENTITY_MISMATCH"

    dup = b'{"schema":"x","schema":"y"}'
    client = FakeClient(_store(dup))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = len(dup)
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_JSON_INVALID"

    nonfinite = b'{"schema":"soxl-v7-r6-twelve-single-completion.v1","x":NaN}'
    client = FakeClient(_store(nonfinite))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = len(nonfinite)
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_JSON_INVALID"


def test_p1_path_set_int_hash_size_invariants(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)

    def run_with_mutator(mutate: Any, reason: str) -> None:
        payload = json.loads(_complete_body().decode())
        mutate(payload)
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        client = FakeClient(_store(body))
        client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = len(body)
        code, result = reader.run_metadata_read(client=client)
        assert code == 2
        assert result["r6"]["reason_class"] == reason

    run_with_mutator(lambda p: p["p1"].pop("closes.json"), "P1_SET_INVALID")
    run_with_mutator(lambda p: p["p1"].__setitem__("extra.json", p["p1"]["binding.json"]), "P1_SET_INVALID")
    run_with_mutator(lambda p: p["p1"]["binding.json"].__setitem__("name", "../binding.json"), "P1_RECEIPT_INVALID")
    run_with_mutator(lambda p: p["p1"]["binding.json"].__setitem__("name", "p1/binding.json?x=1"), "P1_RECEIPT_INVALID")
    run_with_mutator(lambda p: p["p1"]["binding.json"].__setitem__("generation", True), "P1_RECEIPT_INVALID")
    run_with_mutator(lambda p: p["p1"]["binding.json"].__setitem__("size_bytes", -1), "P1_RECEIPT_INVALID")
    run_with_mutator(
        lambda p: p["p1"]["manifest.json"].__setitem__("sha256", "b" * 64),
        "P1_RECEIPT_INVALID",
    )
    run_with_mutator(
        lambda p: p["p1"]["closes.json"].__setitem__("size_bytes", reader.P1_MAX_BYTES + 1),
        "P1_RECEIPT_INVALID",
    )


def test_invalid_observed_complete_metadata_and_generation_race(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()

    client = FakeClient(_store(body))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["generation"] = True
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_METADATA_MISMATCH"
    assert not any(event[0] == "download" for event in client.events)

    client = FakeClient(_store(body))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = -1
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_METADATA_MISMATCH"

    class RaceBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            super().reload(**kwargs)
            if self.name.endswith("complete.json"):
                item = self.store[(self.bucket, self.name)]
                item["generation"] = int(item["generation"]) + 1

    class RaceBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return RaceBlob(self.store, self.name, name, generation, self.events)

    class RaceClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return RaceBucket(self.store, name, self.events)

    client = RaceClient(_store(body))
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_DOWNLOAD_FAILED"

    client = FakeClient(_store(body))
    client.store[(reader.RAW_BUCKET, reader.RAW_OBJECT)]["size"] = reader.RAW_SIZE_BYTES + 1
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["raw"]["reason_class"] == "RAW_METADATA_MISMATCH"

    huge = b"x" * (reader.R6_COMPLETE_MAX_BYTES + 1)
    client = FakeClient(_store(huge))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")] = {
        "generation": SYN_COMPLETE_GEN,
        "size": len(huge),
        "body": huge,
    }
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_OVERSIZE"
    assert not any(event[0] == "download" for event in client.events)


def test_deadline_last_request_overrun_and_shrinking_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()
    clock = MutableClock(0.0)

    class OverrunBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            super().reload(**kwargs)
            if self.name.endswith("p1/assurance.json"):
                clock.now = 31.0

    class OverrunBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return OverrunBlob(self.store, self.name, name, generation, self.events)

    class OverrunClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return OverrunBucket(self.store, name, self.events)

    client = OverrunClient(_store(body))
    code, result = reader.run_metadata_read(client=client, clock=clock, budget_s=30.0)
    assert code == 2
    assert result["raw"]["status"] == "METADATA_MATCHED"
    assert result["r6"]["reason_class"] == "DEADLINE_EXCEEDED"
    assert result["reason_class"] == "DEADLINE_EXCEEDED"

    # Shrinking timeouts across the seven GETs without sleeps.
    tick = MutableClock(0.0)
    timeouts: list[float] = []

    class TickBlob(FakeBlob):
        def reload(self, *, retry: object, timeout: float) -> None:
            timeouts.append(float(timeout))
            super().reload(retry=retry, timeout=timeout)
            tick.now += 1.0

        def download_as_bytes(self, **kwargs: Any) -> bytes:  # type: ignore[override]
            timeouts.append(float(kwargs["timeout"]))
            content = super().download_as_bytes(**kwargs)
            tick.now += 1.0
            return content

    class TickBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return TickBlob(self.store, self.name, name, generation, self.events)

    class TickClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return TickBucket(self.store, name, self.events)

    client = TickClient(_store(body))
    code, result = reader.run_metadata_read(client=client, clock=tick, budget_s=30.0)
    assert code == 0
    assert len(timeouts) == 7
    assert timeouts == sorted(timeouts, reverse=True)
    assert timeouts[0] > timeouts[-1]

    # Early deadline still blocks the first group.
    client = FakeClient(_store(body))
    early = SequencedClock([0.0, 0.0, 30.0])
    code, result = reader.run_metadata_read(client=client, clock=early, budget_s=30.0)
    assert code == 2
    assert result["raw"]["reason_class"] == "DEADLINE_EXCEEDED"

    # RAW fails first; R6 still reported independently when time remains.
    client = FakeClient(_store(body))
    client.store[(reader.RAW_BUCKET, reader.RAW_OBJECT)]["generation"] = 1
    code, result = reader.run_metadata_read(client=client)
    assert code == 2
    assert result["raw"]["reason_class"] == "RAW_METADATA_MISMATCH"
    assert result["r6"]["status"] == "METADATA_READY"


def test_lazy_client_401_does_not_refresh_or_resend(monkeypatch: pytest.MonkeyPatch) -> None:
    # Prove FORCE override: caller false must not leave background bucket GETs enabled.
    monkeypatch.setenv(reader.DISABLE_OTEL_BUCKET_METADATA_ENV, "false")
    fake_creds = FakeCredentials()
    _patch_lazy_auth(monkeypatch, fake_creds)
    http_calls: list[tuple[str, str]] = []

    def fake_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
        http_calls.append((method.upper(), url))
        response = requests.Response()
        response.status_code = 401
        response._content = b'{"error":{"message":"unauthorized"}}'
        response.headers["Content-Type"] = "application/json"
        response.url = url
        response.request = requests.Request(method=method, url=url).prepare()
        return response

    monkeypatch.setattr(requests.Session, "request", fake_request)
    client = reader._lazy_storage_client()
    assert os.environ[reader.DISABLE_OTEL_BUCKET_METADATA_ENV] == "true"
    assert client.project == "synthetic-project"
    blob = client.bucket("synthetic-bucket").blob("synthetic-object")
    with pytest.raises(Exception):  # noqa: BLE001 - SDK raises transport/API error
        blob.reload(retry=None, timeout=5)
    get_calls = [call for call in http_calls if call[0] == "GET"]
    assert len(get_calls) == 1
    assert len(http_calls) == 1
    assert fake_creds.refresh_calls == 0
    assert not any(_is_bucket_metadata_url(url) for _method, url in http_calls)


def test_lazy_client_real_sdk_no_bucket_metadata_get_and_raw_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(reader.DISABLE_OTEL_BUCKET_METADATA_ENV, "false")
    fake_creds = FakeCredentials()
    _patch_lazy_auth(monkeypatch, fake_creds)
    plain = b'{"schema":"synthetic-complete","payload":"' + (b"x" * 4000) + b'"}'
    compressed = gzip.compress(plain)
    assert len(compressed) < reader.R6_COMPLETE_MAX_BYTES
    assert len(plain) > len(compressed)
    generation = 9001
    object_name = "exact-study-root/complete.json"
    http_calls: list[tuple[str, str]] = []

    def fake_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
        http_calls.append((method.upper(), url))
        parsed = urlparse(url)
        path = parsed.path
        response = requests.Response()
        response.url = url
        response.request = requests.Request(method=method, url=url).prepare()
        if _is_bucket_metadata_url(url):
            raise AssertionError(f"unexpected bucket metadata GET: {url}")
        if "/download/storage/v1/b/" in path and "alt=media" in parsed.query:
            response.status_code = 200
            response.headers["Content-Type"] = "application/octet-stream"
            response.headers["Content-Length"] = str(len(compressed))
            # Real SDK media path reads response.raw.stream(...); preload must stay false.
            response.raw = urllib3.HTTPResponse(
                body=BytesIO(compressed),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(compressed)),
                },
                status=200,
                preload_content=False,
                decode_content=False,
            )
            response._content = False
            response._content_consumed = False
            return response
        if "/o/" in path and "exact-study-root" in path and "complete.json" in path:
            response.status_code = 200
            response.headers["Content-Type"] = "application/json"
            response._content = json.dumps(
                {
                    "kind": "storage#object",
                    "name": object_name,
                    "bucket": "synthetic-bucket",
                    "generation": str(generation),
                    "size": str(len(compressed)),
                    "metageneration": "1",
                }
            ).encode("utf-8")
            return response
        raise AssertionError(f"unexpected request {method} {url}")

    monkeypatch.setattr(requests.Session, "request", fake_request)
    client = reader._lazy_storage_client()
    assert os.environ[reader.DISABLE_OTEL_BUCKET_METADATA_ENV] == "true"

    blob = client.bucket("synthetic-bucket").blob(object_name, generation=generation)
    blob.reload(retry=None, timeout=5)
    assert int(blob.generation) == generation
    assert int(blob.size) == len(compressed)

    clock = MutableClock(0.0)
    body = reader._download_complete(
        client,
        bucket_name="synthetic-bucket",
        prefix="exact-study-root/",
        generation=generation,
        size=len(compressed),
        deadline=30.0,
        clock=clock,
    )
    assert body == compressed
    assert body != plain
    assert body[:2] == b"\x1f\x8b"

    get_urls = [url for method, url in http_calls if method == "GET"]
    assert get_urls
    assert not any(_is_bucket_metadata_url(url) for url in get_urls)
    assert any("/download/storage/v1/b/" in url and "alt=media" in url for url in get_urls)
    assert any("/o/" in urlparse(url).path for url in get_urls)
    assert fake_creds.refresh_calls == 0
    assert len(http_calls) == len(get_urls)


def test_exception_redaction_in_public_output(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    body = _complete_body()

    class ExplodingBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            raise RuntimeError("secret-token gs://private/path credentials=xyz")

    class ExplodingBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return ExplodingBlob(self.store, self.name, name, generation, self.events)

    class ExplodingClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return ExplodingBucket(self.store, name, self.events)

    client = ExplodingClient(_store(body))
    code, payload = reader.run_metadata_read(client=client)
    assert code == 2
    text = json.dumps(payload)
    assert "secret-token" not in text
    assert "credentials=" not in text
    assert "gs://private/path" not in text
    assert payload["raw"]["reason_class"] == "RAW_METADATA_UNAVAILABLE"


def test_workflow_mode_job_and_secret_isolation() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    assert (
        "          - preflight\n"
        "          - execute\n"
        "          - metadata_only\n"
        "          - integrity_only\n"
    ) in raw
    research_marker = "  r6-research:\n"
    meta_marker = "  research-input-archive-metadata:\n"
    integrity_marker = "  research-input-archive-integrity:\n"
    assert research_marker in raw
    assert meta_marker in raw
    assert integrity_marker in raw
    research_start = raw.index(research_marker)
    meta_start = raw.index(meta_marker)
    integrity_start = raw.index(integrity_marker)
    assert research_start < meta_start < integrity_start
    research_block = raw[research_start:meta_start]
    meta_block = raw[meta_start:integrity_start]
    integrity_block = raw[integrity_start:]
    assert "    if: ${{ inputs.mode == 'preflight' || inputs.mode == 'execute' }}\n" in research_block
    assert "    if: ${{ inputs.mode == 'metadata_only' }}\n" in meta_block
    assert "    if: ${{ inputs.mode == 'integrity_only' }}\n" in integrity_block
    assert "environment: market-data-nonlive" in research_block
    assert "environment: market-data-nonlive" in meta_block
    assert "environment: market-data-nonlive" in integrity_block
    for block in (meta_block, integrity_block):
        assert "TWELVE_DATA_API_KEY" not in block
        assert "SOXL_V7_R6_LICENSE_EVIDENCE_SHA256" not in block
        assert "UsEquityStrategies" not in block
        assert "ALPACA" not in block
        assert "SOXL_V7_R6_PRIVATE_ROOT: ${{ secrets.SOXL_V7_R6_PRIVATE_ROOT }}" in block
        assert "DEADLINE_EXCEEDED" in block
        assert "124" in block
        assert "2>/dev/null" in block
    assert (
        "timeout 30s uv run --no-sync python -m scripts.read_research_input_archive_metadata 2>/dev/null"
        in meta_block
    )
    assert (
        "timeout 30s uv run --no-sync python -m scripts.read_research_input_archive_metadata --integrity-only 2>/dev/null"
        in integrity_block
    )
    assert "content_integrity_verified" in integrity_block
    assert "if: inputs.mode == 'preflight'" in research_block
    assert "if: inputs.mode == 'execute'" in research_block
    assert "TWELVE_DATA_API_KEY: ${{ secrets.TWELVE_DATA_API_KEY }}" in research_block
    assert "SOXL_V7_R6_LICENSE_EVIDENCE_SHA256: ${{ secrets.SOXL_V7_R6_LICENSE_EVIDENCE_SHA256 }}" in research_block
    assert "uv==0.11.19" in raw
    assert "uv sync --locked --no-dev --no-editable --python 3.11" in raw
    assert "google-github-actions/auth@7c6bc770dae815cd3e89ee6cdf493a5fab2cc093" in raw
    assert "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065" in raw
    assert "actions/checkout@11d5960a326750d5838078e36cf38b85af677262" in raw


def test_unreachable_mutators_on_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    client = FakeClient(_store())
    bucket = client.bucket("x")
    with pytest.raises(AssertionError, match="list unreachable"):
        bucket.list_blobs()
    with pytest.raises(AssertionError, match="upload unreachable"):
        bucket.blob("n").upload_from_string(b"x")
    with pytest.raises(AssertionError, match="create_bucket unreachable"):
        client.create_bucket("x")


def _pad_to(prefix: bytes, size: int) -> bytes:
    if len(prefix) > size:
        raise AssertionError(f"prefix {len(prefix)} exceeds target {size}")
    return prefix + (b" " * (size - len(prefix)))


def _integrity_bodies() -> dict[str, bytes]:
    raw_body = b"SYN-RAW-" + (b"r" * (reader.RAW_SIZE_BYTES - 8))
    closes_body = b"SYN-CLOSES-" + (b"c" * (int(reader.P1_PINS["closes.json"]["size_bytes"]) - 11))
    assurance_body = b"SYN-ASSURE-" + (b"a" * (int(reader.P1_PINS["assurance.json"]["size_bytes"]) - 11))
    binding_body = _pad_to(b'{"synthetic":"binding-body"}', int(reader.P1_PINS["binding.json"]["size_bytes"]))
    bind_sha = _sha(binding_body)
    manifest_core = json.dumps(
        {
            "schema_version": "research_input_manifest.v1",
            "profile": reader.STUDY_ID,
            "research_input_contract_id": reader.INPUT_CONTRACT_ID,
            "producer": {"commit_sha": reader.PRODUCER_REVISION},
            "calendar": {"source_revision": bind_sha},
            "adjustment": {"source_revision": bind_sha},
            "members": [
                {
                    "path": "closes.json",
                    "size_bytes": len(closes_body),
                    "sha256": _sha(closes_body),
                },
                {
                    "path": "assurance.json",
                    "size_bytes": len(assurance_body),
                    "sha256": _sha(assurance_body),
                },
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    manifest_body = _pad_to(manifest_core, int(reader.P1_PINS["manifest.json"]["size_bytes"]))
    return {
        "raw": raw_body,
        "binding": binding_body,
        "manifest": manifest_body,
        "closes": closes_body,
        "assurance": assurance_body,
    }


def _integrity_complete_body(bodies: dict[str, bytes]) -> bytes:
    body_by_key = {
        "binding.json": bodies["binding"],
        "manifest.json": bodies["manifest"],
        "closes.json": bodies["closes"],
        "assurance.json": bodies["assurance"],
    }
    p1 = {
        key: {
            "name": f"p1/{key}",
            "generation": int(reader.P1_PINS[key]["generation"]),
            "size_bytes": int(reader.P1_PINS[key]["size_bytes"]),
            "sha256": _sha(body_by_key[key]),
        }
        for key in reader.P1_KEYS
    }
    payload = {
        "schema": reader.COMPLETION_SCHEMA,
        "study_id": reader.STUDY_ID,
        "signal_candidate_id": reader.SIGNAL_CANDIDATE_ID,
        "source_assurance": reader.SOURCE_ASSURANCE,
        "historical_point_in_time_certified": False,
        "date_cutoff": reader.DATE_CUTOFF,
        "producer_revision": reader.PRODUCER_REVISION,
        "license_evidence_sha256": reader.R6_LICENSE_SHA256,
        "observed_at": reader.R6_COMPLETE_OBSERVED_AT,
        "p1_manifest_sha256": _sha(bodies["manifest"]),
        "p1": p1,
    }
    core = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return _pad_to(core, reader.R6_COMPLETE_SIZE_BYTES)


def _patch_integrity_hashes(monkeypatch: pytest.MonkeyPatch, bodies: dict[str, bytes], complete: bytes) -> None:
    bind_sha = _sha(bodies["binding"])
    manifest_sha = _sha(bodies["manifest"])
    pins = {key: dict(value) for key, value in reader.P1_PINS.items()}
    pins["binding.json"]["sha256"] = bind_sha
    pins["manifest.json"]["sha256"] = manifest_sha
    pins["closes.json"]["sha256"] = _sha(bodies["closes"])
    pins["assurance.json"]["sha256"] = _sha(bodies["assurance"])
    monkeypatch.setattr(reader, "P1_PINS", pins)
    monkeypatch.setattr(reader, "RAW_EXPECTED_SHA256", _sha(bodies["raw"]))
    monkeypatch.setattr(reader, "R6_COMPLETE_SHA256", _sha(complete))
    monkeypatch.setattr(reader, "P1_MANIFEST_SHA256", manifest_sha)
    monkeypatch.setattr(reader, "BINDING_SHA256", bind_sha)


def _integrity_store(bodies: dict[str, bytes], complete: bytes) -> dict[tuple[str, str], dict[str, Any]]:
    prefix = "exact-study-root/"
    store: dict[tuple[str, str], dict[str, Any]] = {
        (reader.RAW_BUCKET, reader.RAW_OBJECT): {
            "generation": reader.RAW_GENERATION,
            "size": reader.RAW_SIZE_BYTES,
            "body": bodies["raw"],
        },
        ("synthetic-r6-private-bucket", prefix + reader.R6_COMPLETE_NAME): {
            "generation": reader.R6_COMPLETE_GENERATION,
            "size": reader.R6_COMPLETE_SIZE_BYTES,
            "body": complete,
        },
    }
    for key, body_key in (
        ("binding.json", "binding"),
        ("manifest.json", "manifest"),
        ("closes.json", "closes"),
        ("assurance.json", "assurance"),
    ):
        store[("synthetic-r6-private-bucket", prefix + f"p1/{key}")] = {
            "generation": int(reader.P1_PINS[key]["generation"]),
            "size": int(reader.P1_PINS[key]["size_bytes"]),
            "body": bodies[body_key],
        }
    for name, _sha256 in reader.CONTRACT_SPECS:
        store[(reader.RAW_BUCKET, reader.RAW_PREFIX + name)] = {
            "generation": 7000 + len(name),
            "size": 128 + len(name),
            "body": b"CONTRACT-BODY-MUST-NOT-BE-READ",
        }
    return store


def test_integrity_success_exact_fifteen_gets_and_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    assert reader.INTEGRITY_MAX_BODY_BYTES == 163079
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    # Real frozen hashes must never be reused for synthetic bodies.
    assert reader.RAW_EXPECTED_SHA256 != "cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182"
    assert reader.R6_COMPLETE_SHA256 != "15bfb9d59a884e79614f555b959d888df895377f4924c7639480b106752fee46"
    client = FakeClient(_integrity_store(bodies, complete))
    code, payload = reader.run_integrity_read(client=client)
    assert code == 0
    assert payload["status"] == "INTEGRITY_READY"
    assert payload["content_integrity_verified"] is True
    assert payload["research_qualification"] is False
    assert payload["trading_rights"] is False
    assert payload["license_verified"] is False
    assert payload["historical_point_in_time_certified"] is False
    assert payload["completion_identity_authenticated"] is False
    assert payload["content_verified"] is False
    raw = payload["raw"]
    assert isinstance(raw, dict)
    assert raw["status"] == "INTEGRITY_MATCHED"
    assert raw["content_hash_verified"] is True
    assert raw["generation"] == reader.RAW_GENERATION
    assert raw["size_bytes"] == reader.RAW_SIZE_BYTES
    r6 = payload["r6"]
    assert isinstance(r6, dict)
    assert r6["status"] == "INTEGRITY_MATCHED"
    assert r6["completion_identity_authenticated"] is False
    assert r6["complete"]["generation"] == reader.R6_COMPLETE_GENERATION
    assert r6["complete"]["size_bytes"] == reader.R6_COMPLETE_SIZE_BYTES
    contracts = payload["contracts"]
    assert isinstance(contracts, dict)
    assert contracts["status"] == "METADATA_MATCHED"
    for name, expected in reader.CONTRACT_SPECS:
        item = contracts["contracts"][name]
        assert item["location_present"] is True
        assert item["body_read"] is False
        assert item["content_hash_verified"] is False
        assert item["expected_sha_status"] == "unverified"
        assert item["expected_sha256"] == expected
        assert item["generation"] > 0
        assert 0 <= item["size_bytes"] <= reader.P1_MAX_BYTES
    ops = [event[0] for event in client.events]
    assert ops.count("reload") == 9
    assert ops.count("download") == 6
    assert len(client.events) == 15
    requested = sum(event[1][4] for event in client.events if event[0] == "download")
    assert requested == 163079
    assert not any(event[0] == "download" and "contract" in event[1][1] for event in client.events)
    text = json.dumps(payload)
    assert "SYN-RAW" not in text
    assert "SYN-CLOSES" not in text
    assert "CONTRACT-BODY" not in text
    assert "gs://" not in text
    assert "synthetic-r6-private-bucket" not in text
    assert reader.RAW_BUCKET not in text


def test_integrity_hash_gen_size_and_receipt_widening(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)

    client = FakeClient(_integrity_store(bodies, complete))
    client.store[(reader.RAW_BUCKET, reader.RAW_OBJECT)]["body"] = b"x" * reader.RAW_SIZE_BYTES
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["raw"]["reason_class"] == "RAW_HASH_MISMATCH"

    client = FakeClient(_integrity_store(bodies, complete))
    client.store[(reader.RAW_BUCKET, reader.RAW_OBJECT)]["generation"] = reader.RAW_GENERATION + 1
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["raw"]["reason_class"] == "RAW_METADATA_MISMATCH"

    client = FakeClient(_integrity_store(bodies, complete))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/complete.json")]["size"] = (
        reader.R6_COMPLETE_SIZE_BYTES + 1
    )
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "COMPLETE_METADATA_MISMATCH"

    # Receipt widening must not retarget pinned P1 objects.
    widened = json.loads(complete.decode())
    widened["p1"]["closes.json"]["generation"] = int(reader.P1_PINS["closes.json"]["generation"]) + 99
    widened["p1"]["closes.json"]["size_bytes"] = int(reader.P1_PINS["closes.json"]["size_bytes"]) + 50
    widened["p1"]["closes.json"]["sha256"] = "f" * 64
    bad = _pad_to(
        json.dumps(widened, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        reader.R6_COMPLETE_SIZE_BYTES,
    )
    monkeypatch.setattr(reader, "R6_COMPLETE_SHA256", _sha(bad))
    client = FakeClient(_integrity_store(bodies, bad))
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "P1_RECEIPT_PIN_MISMATCH"
    p1_downloads = [
        event
        for event in client.events
        if event[0] == "download" and "/p1/" in event[1][1]
    ]
    assert p1_downloads == []


def test_integrity_manifest_binding_link_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    bad_manifest = json.loads(bodies["manifest"].decode())
    bad_manifest["calendar"]["source_revision"] = "0" * 64
    bodies = dict(bodies)
    bodies["manifest"] = _pad_to(
        json.dumps(bad_manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        int(reader.P1_PINS["manifest.json"]["size_bytes"]),
    )
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    client = FakeClient(_integrity_store(bodies, complete))
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "MANIFEST_BINDING_MISMATCH"

    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    bad_manifest = json.loads(bodies["manifest"].decode())
    bad_manifest["members"][0]["sha256"] = "1" * 64
    bodies = dict(bodies)
    bodies["manifest"] = _pad_to(
        json.dumps(bad_manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        int(reader.P1_PINS["manifest.json"]["size_bytes"]),
    )
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    client = FakeClient(_integrity_store(bodies, complete))
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["r6"]["reason_class"] == "MANIFEST_BINDING_MISMATCH"


def test_integrity_no_tempfile_or_path_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)

    def boom_write(*_a: object, **_k: object) -> None:
        raise AssertionError("path write unreachable")

    monkeypatch.setattr(Path, "write_bytes", boom_write)
    monkeypatch.setattr(Path, "write_text", boom_write)
    import tempfile

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", boom_write)
    monkeypatch.setattr(tempfile, "mkstemp", boom_write)
    client = FakeClient(_integrity_store(bodies, complete))
    code, _payload = reader.run_integrity_read(client=client)
    assert code == 0


def test_integrity_contracts_metadata_only_and_first_error_stops_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    store = _integrity_store(bodies, complete)
    first_name = reader.CONTRACT_SPECS[0][0]
    del store[(reader.RAW_BUCKET, reader.RAW_PREFIX + first_name)]
    client = FakeClient(store)
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["raw"]["status"] == "INTEGRITY_MATCHED"
    assert result["r6"]["status"] == "INTEGRITY_MATCHED"
    assert result["contracts"]["reason_class"] == "CONTRACT_METADATA_UNAVAILABLE"
    assert result["content_integrity_verified"] is False
    assert result["status"] == "BLOCKED"
    contract_reloads = [
        event
        for event in client.events
        if event[0] == "reload" and event[1][1].startswith(reader.RAW_PREFIX) and event[1][1] != reader.RAW_OBJECT
    ]
    assert len(contract_reloads) == 1
    assert contract_reloads[0][1][1].endswith(first_name)
    assert not any(event[0] == "download" and first_name in event[1][1] for event in client.events)


def test_integrity_403_404_ambiguous_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)

    class StatusBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            if self.name.endswith(reader.CONTRACT_SPECS[0][0]):
                raise RuntimeError("403 Forbidden / 404 Not Found for object")
            super().reload(**kwargs)

    class StatusBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return StatusBlob(self.store, self.name, name, generation, self.events)

    class StatusClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return StatusBucket(self.store, name, self.events)

    client = StatusClient(_integrity_store(bodies, complete))
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["contracts"]["reason_class"] == "CONTRACT_METADATA_UNAVAILABLE"
    text = json.dumps(result)
    assert "403" not in text
    assert "404" not in text
    assert "NOT_FOUND" not in text
    assert "nonexistent" not in text.lower()


def test_integrity_independent_raw_r6_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)

    client = FakeClient(_integrity_store(bodies, complete))
    client.store[(reader.RAW_BUCKET, reader.RAW_OBJECT)]["generation"] = 1
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["raw"]["reason_class"] == "RAW_METADATA_MISMATCH"
    assert result["r6"]["status"] == "INTEGRITY_MATCHED"
    assert result["contracts"]["status"] == "METADATA_MATCHED"
    assert result["reason_class"] == "RAW_METADATA_MISMATCH"

    client = FakeClient(_integrity_store(bodies, complete))
    client.store[("synthetic-r6-private-bucket", "exact-study-root/p1/manifest.json")]["body"] = (
        b"y" * int(reader.P1_PINS["manifest.json"]["size_bytes"])
    )
    code, result = reader.run_integrity_read(client=client)
    assert code == 2
    assert result["raw"]["status"] == "INTEGRITY_MATCHED"
    assert result["r6"]["reason_class"] == "P1_HASH_MISMATCH"
    assert result["contracts"]["status"] == "METADATA_MATCHED"


def test_integrity_deadline_after_last_get(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    clock = MutableClock(0.0)
    last_contract = reader.CONTRACT_SPECS[-1][0]

    class OverrunBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            super().reload(**kwargs)
            if self.name.endswith(last_contract):
                clock.now = 31.0

    class OverrunBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return OverrunBlob(self.store, self.name, name, generation, self.events)

    class OverrunClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return OverrunBucket(self.store, name, self.events)

    client = OverrunClient(_integrity_store(bodies, complete))
    code, result = reader.run_integrity_read(client=client, clock=clock, budget_s=30.0)
    assert code == 2
    assert result["raw"]["status"] == "INTEGRITY_MATCHED"
    assert result["r6"]["status"] == "INTEGRITY_MATCHED"
    assert result["contracts"]["reason_class"] == "DEADLINE_EXCEEDED"
    assert result["reason_class"] == "DEADLINE_EXCEEDED"
    assert result["content_integrity_verified"] is False


def test_integrity_raw_gzip_not_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    plain = bodies["raw"]
    compressed = gzip.compress(plain)
    assert len(compressed) < reader.RAW_SIZE_BYTES
    padded = compressed + (b"\x00" * (reader.RAW_SIZE_BYTES - len(compressed)))
    bodies = dict(bodies)
    bodies["raw"] = padded
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    client = FakeClient(_integrity_store(bodies, complete))
    code, result = reader.run_integrity_read(client=client)
    assert code == 0
    download = next(
        event
        for event in client.events
        if event[0] == "download" and event[1][1] == reader.RAW_OBJECT
    )
    assert download[1][6] is True
    assert download[1][4] == reader.RAW_SIZE_BYTES
    assert result["raw"]["sha256"] == _sha(padded)
    assert result["raw"]["sha256"] != _sha(plain)


def test_integrity_output_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    _actions(monkeypatch)
    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)

    class ExplodingBlob(FakeBlob):
        def reload(self, **kwargs: Any) -> None:  # type: ignore[override]
            raise RuntimeError("secret-token gs://private/path credentials=xyz amount=12.34")

    class ExplodingBucket(FakeBucket):
        def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
            return ExplodingBlob(self.store, self.name, name, generation, self.events)

    class ExplodingClient(FakeClient):
        def bucket(self, name: str) -> FakeBucket:
            return ExplodingBucket(self.store, name, self.events)

    client = ExplodingClient(_integrity_store(bodies, complete))
    code, payload = reader.run_integrity_read(client=client)
    assert code == 2
    text = json.dumps(payload)
    assert "secret-token" not in text
    assert "credentials=" not in text
    assert "gs://private/path" not in text
    assert "12.34" not in text
    assert payload["raw"]["reason_class"] == "RAW_METADATA_UNAVAILABLE"


def test_integrity_cli_env_and_argv_guards(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", reader.ALLOWED_REPOSITORY)
    created = {"client": False}

    def boom() -> FakeClient:
        created["client"] = True
        raise AssertionError("client must not be created")

    code, payload = reader.run_integrity_read(create_client=boom)
    assert code == 2
    assert payload["reason_class"] == "ENV_REFUSED"
    assert created["client"] is False

    _actions(monkeypatch)
    assert reader.main(["--integrity-only", "--root", "gs://x"]) == 2
    out = json.loads(capsys.readouterr().out)
    assert out["reason_class"] == "ARGV_REFUSED"
    assert "gs://" not in json.dumps(out)

    bodies = _integrity_bodies()
    complete = _integrity_complete_body(bodies)
    _patch_integrity_hashes(monkeypatch, bodies, complete)
    client = FakeClient(_integrity_store(bodies, complete))
    monkeypatch.setattr(reader, "_lazy_storage_client", lambda: client)
    assert reader.main(["--integrity-only"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "INTEGRITY_READY"
    assert out["content_integrity_verified"] is True


def test_integrity_lazy_client_body_helper_no_bucket_get_or_401_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(reader.DISABLE_OTEL_BUCKET_METADATA_ENV, "false")
    fake_creds = FakeCredentials()
    _patch_lazy_auth(monkeypatch, fake_creds)
    plain = b"SYN-BODY-" + (b"z" * 200)
    compressed = gzip.compress(plain)
    generation = 4242
    object_name = "exact-study-root/p1/assurance.json"
    http_calls: list[tuple[str, str]] = []

    def fake_request(self: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
        http_calls.append((method.upper(), url))
        parsed = urlparse(url)
        path = parsed.path
        response = requests.Response()
        response.url = url
        response.request = requests.Request(method=method, url=url).prepare()
        if _is_bucket_metadata_url(url):
            raise AssertionError(f"unexpected bucket metadata GET: {url}")
        if "/download/storage/v1/b/" in path and "alt=media" in parsed.query:
            response.status_code = 200
            response.headers["Content-Type"] = "application/octet-stream"
            response.headers["Content-Length"] = str(len(compressed))
            response.raw = urllib3.HTTPResponse(
                body=BytesIO(compressed),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(compressed)),
                },
                status=200,
                preload_content=False,
                decode_content=False,
            )
            response._content = False
            response._content_consumed = False
            return response
        if "/o/" in path and "assurance.json" in path:
            response.status_code = 200
            response.headers["Content-Type"] = "application/json"
            response._content = json.dumps(
                {
                    "kind": "storage#object",
                    "name": object_name,
                    "bucket": "synthetic-bucket",
                    "generation": str(generation),
                    "size": str(len(compressed)),
                    "metageneration": "1",
                }
            ).encode("utf-8")
            return response
        raise AssertionError(f"unexpected request {method} {url}")

    monkeypatch.setattr(requests.Session, "request", fake_request)
    client = reader._lazy_storage_client()
    assert os.environ[reader.DISABLE_OTEL_BUCKET_METADATA_ENV] == "true"
    clock = MutableClock(0.0)
    body = reader._download_bytes(
        client,
        bucket_name="synthetic-bucket",
        object_name=object_name,
        generation=generation,
        size=len(compressed),
        max_size=len(compressed),
        deadline=30.0,
        clock=clock,
        oversize_reason="P1_OVERSIZE",
        fail_reason="P1_DOWNLOAD_FAILED",
    )
    assert body == compressed
    assert body != plain
    get_urls = [url for method, url in http_calls if method == "GET"]
    assert get_urls
    assert not any(_is_bucket_metadata_url(url) for url in get_urls)
    assert any("/download/storage/v1/b/" in url and "alt=media" in url for url in get_urls)
    assert fake_creds.refresh_calls == 0
    assert len(http_calls) == len(get_urls)


def test_integrity_module_main_only_flag_subprocess() -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"GITHUB_ACTIONS", "GITHUB_REPOSITORY", reader.ROOT_ENV}
    }
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.read_research_input_archive_metadata",
            "--integrity-only",
            "gs://evil",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["reason_class"] == "ARGV_REFUSED"
