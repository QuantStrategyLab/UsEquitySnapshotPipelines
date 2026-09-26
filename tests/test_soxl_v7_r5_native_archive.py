"""Synthetic checks for the one-shot V7 private archive boundary."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from urllib.request import Request

import pytest

from scripts import soxl_v7_r5_native_archive as r5


class FakeBlob:
    def __init__(self, objects: dict[str, tuple[int, bytes]], name: str, generation: int | None) -> None:
        self.objects = objects
        self.name = name
        self.generation = generation

    def upload_from_string(self, content: bytes, *, if_generation_match: int, **_kwargs) -> None:
        assert if_generation_match == 0
        if self.name in self.objects:
            raise ValueError("exists")
        self.generation = 17
        self.objects[self.name] = (self.generation, content)

    def reload(self, **_kwargs) -> None:
        self.generation = self.objects[self.name][0]
        self.size = len(self.objects[self.name][1])

    def download_as_bytes(self, *, if_generation_match: int, start: int, end: int, **_kwargs) -> bytes:
        stored_generation, content = self.objects[self.name]
        assert self.generation == if_generation_match == stored_generation
        return content[start : end + 1]


class FakeBucket:
    def __init__(self, objects: dict[str, tuple[int, bytes]]) -> None:
        self.objects = objects

    def blob(self, name: str, *, generation: int | None = None) -> FakeBlob:
        return FakeBlob(self.objects, name, generation)


class FakeClient:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[int, bytes]] = {}

    def bucket(self, _name: str) -> FakeBucket:
        return FakeBucket(self.objects)


def test_create_only_and_fixed_generation_readback(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "gs://synthetic-private-bucket/one-study/"
    monkeypatch.setattr(r5, "ROOT_SHA256", hashlib.sha256(uri.encode()).hexdigest())
    client = FakeClient()
    archive = r5.FixedArchive(client, uri)
    receipt = archive.create_probe()
    assert archive.read(receipt) == r5.canonical({"schema": "soxl_v7_r5_probe.v1", "candidate_id": r5.candidate_id()})
    assert archive.verify_probe() == receipt
    with pytest.raises(r5.R5ArchiveError, match="create-only write failed"):
        archive.create_probe()
    assert client.objects["one-study/probe.json"][0] == receipt["generation"]


def test_object_budget_blocks_before_write(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "gs://synthetic-private-bucket/one-study/"
    monkeypatch.setattr(r5, "ROOT_SHA256", hashlib.sha256(uri.encode()).hexdigest())
    monkeypatch.setattr(r5, "MAX_OBJECT_OPERATIONS", 1)
    client = FakeClient()
    with pytest.raises(r5.R5ArchiveError, match="budget exhausted"):
        r5.FixedArchive(client, uri).create("p1/binding.json", b"{}")
    assert client.objects == {}


def test_root_and_name_cannot_expand_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "gs://synthetic-private-bucket/one-study/"
    monkeypatch.setattr(r5, "ROOT_SHA256", hashlib.sha256(uri.encode()).hexdigest())
    with pytest.raises(r5.R5ArchiveError, match="frozen authorization"):
        r5.FixedArchive(FakeClient(), "gs://synthetic-private-bucket/other/")
    archive = r5.FixedArchive(FakeClient(), uri)
    with pytest.raises(r5.R5ArchiveError, match="invalid private object name"):
        archive.create("../outside.json", b"{}")


class FakeResponse:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, body: bytes) -> None:
        self.body = body

    def read(self, size: int = -1) -> bytes:
        result, self.body = self.body[:size], self.body[size:]
        return result


def test_http_budget_and_host_before_network(monkeypatch: pytest.MonkeyPatch) -> None:
    meter = r5.HttpMeter()
    calls: list[str] = []

    class Opener:
        def open(self, request: Request, *, timeout: int) -> FakeResponse:
            calls.append(request.full_url)
            return FakeResponse(b"abc")

    meter.opener = Opener()
    with pytest.raises(r5.R5ArchiveError, match="host outside"):
        meter.open(Request("https://elsewhere.invalid/data"))
    assert calls == []
    monkeypatch.setattr(r5, "MAX_HTTP_BYTES", 2)
    with pytest.raises(r5.R5ArchiveError, match="body size ambiguous"):
        meter.open(Request("https://api.twelvedata.com/time_series")).read()
    assert meter.requests == 1
    monkeypatch.setattr(r5, "MAX_HTTP_REQUESTS", 1)
    with pytest.raises(r5.R5ArchiveError, match="request budget exhausted"):
        meter.open(Request("https://query1.finance.yahoo.com/chart/SOXL"))
    assert len(calls) == 1


def test_yahoo_duplicate_session_rejected_before_downstream_dedup() -> None:
    timestamp = int(datetime(2024, 1, 2, tzinfo=UTC).timestamp())
    with pytest.raises(r5.R5ArchiveError, match="duplicate session"):
        r5._check_yahoo_duplicate_dates({"chart": {"result": [{"timestamp": [timestamp, timestamp]}]}})


def test_license_binding_must_be_present_before_market_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    class NeverArchive:
        def verify_probe(self) -> None:
            raise AssertionError("should not touch store")

    with pytest.raises(r5.R5ArchiveError, match="license precheck"):
        r5.execute(
            NeverArchive(), api_key="synthetic", ues_project=None, producer_revision="0" * 40,
            license_evidence_sha256="",
        )


def test_second_execute_cannot_repeat_market_acquisition(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "gs://synthetic-private-bucket/one-study/"
    monkeypatch.setattr(r5, "ROOT_SHA256", hashlib.sha256(uri.encode()).hexdigest())
    monkeypatch.setattr(r5, "_verify_revisions", lambda *_args: None)
    client = FakeClient()
    r5.FixedArchive(client, uri).create_probe()
    entered = 0

    class StopBeforeMarket:
        def __enter__(self):
            nonlocal entered
            entered += 1
            raise RuntimeError("synthetic stop")

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(r5, "MarketTransport", StopBeforeMarket)
    kwargs = {
        "api_key": "synthetic",
        "ues_project": None,
        "producer_revision": "0" * 40,
        "license_evidence_sha256": "a" * 64,
    }
    with pytest.raises(RuntimeError, match="synthetic stop"):
        r5.execute(r5.FixedArchive(client, uri), **kwargs)
    with pytest.raises(r5.R5ArchiveError, match="create-only write failed"):
        r5.execute(r5.FixedArchive(client, uri), **kwargs)
    assert entered == 1
