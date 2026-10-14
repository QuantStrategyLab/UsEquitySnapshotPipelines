"""Synthetic only; no provider, GCS or live configuration access."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "rs04_archive", Path(__file__).resolve().parents[1] / "scripts/rs04_cloud_archive.py")
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


def test_mac_is_rejected_even_with_copied_ci_environment(monkeypatch):
    for key, value in {"GITHUB_ACTIONS": "true", "RUNNER_OS": "Linux", "GITHUB_DEFAULT_BRANCH": "main",
                       "GITHUB_REF": "refs/heads/main", "GITHUB_RUN_ATTEMPT": "1"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(archive.platform, "system", lambda: "Darwin")
    with pytest.raises(archive.ArchiveError, match="CLOUD_DEFAULT_BRANCH"):
        archive.require_cloud()
    monkeypatch.setattr(archive.platform, "system", lambda: "Linux")
    archive.require_cloud()
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    with pytest.raises(archive.ArchiveError):
        archive.require_cloud()


@pytest.mark.parametrize("value", ["", "gs://demo-bucket/live/", "gs://demo-bucket/research/v2/input/../",
                                  "gs://demo-bucket/research/v2/input/x"])
def test_root_is_required_and_bounded(value):
    with pytest.raises(archive.ArchiveError):
        archive.input_root(value)


def test_metadata_scan_never_reads_bodies_or_claims_missing_content():
    calls = []
    root = "gs://demo-bucket/research/v2/input/"
    def command(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout=root + "fixture/manifest.json#123:\n"
                               "    Generation: 123\n    Content-Length: 70\n"
                               "    Metadata:\n      unrelated-private-field: do-not-export\n")
    result = archive.inventory(root, command)
    assert calls == [["gsutil", "ls", "-L", "-a", root + "**"]]
    assert result["objects_without_sha256_metadata"] == 1
    assert result["legacy_metadata_matches"] == []
    assert result["digest_absence_is_not_content_absence"] and not result["real_data_read"]
    assert "do-not-export" not in json.dumps(result)


def test_metadata_failure_suppresses_native_diagnostics():
    def command(*args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="secret-like-native-detail")
    with pytest.raises(archive.ArchiveError, match="METADATA_LIST_DENIED_OR_UNAVAILABLE") as error:
        archive.inventory("gs://demo-bucket/research/v2/input/", command)
    assert "secret-like" not in str(error.value)


def test_config_snapshot_contains_types_and_names_no_values_or_scalar_hashes():
    source = {"secret": "do-not-export", "cash_fraction": 0.03,
              "enabled": True, "targets": [{"symbol": "PRIVATE-NAME"}], "absent": None}
    result = archive.structure(source)
    rendered = json.dumps(result)
    assert "do-not-export" not in rendered and "PRIVATE-NAME" not in rendered and "0.03" not in rendered
    assert "sha256" not in rendered
    assert result["properties"]["enabled"] == {"type": "boolean"}


class MemoryStore:
    def __init__(self):
        self.objects, self.receipts = {}, []
    def create(self, name, body):
        if name in self.objects:
            raise archive.ArchiveError("CREATE_OR_READBACK_UNKNOWN_NO_RETRY")
        self.objects[name] = body
        identity = {"name": name, "bytes": len(body), "sha256": archive.sha(body),
                    "generation": str(len(self.objects))}
        self.receipts.append(identity)
        return identity
    def config_structure(self, uri):
        raise archive.ArchiveError("CONFIG_READ_UNAVAILABLE")


def test_same_inputs_have_same_manifest_and_legacy_digest_is_not_forged(monkeypatch):
    def collect(provider, store, symbol, kind):
        body = archive.canonical({"fixture": symbol, "kind": kind})
        name = f"{kind}/{symbol}/page-001.json"
        store.create(name, body)
        return {"symbol": symbol, "kind": kind, "pages": [{"sha256": archive.sha(body)}]}
    monkeypatch.setattr(archive, "collect", collect)
    stores = [MemoryStore(), MemoryStore()]
    results = [archive.regenerate(None, store) for store in stores]
    assert results[0]["manifest"]["sha256"] == results[1]["manifest"]["sha256"]
    assert results[0]["manifest"]["sha256"] != archive.LEGACY_DIGEST
    assert results[0]["live_config_snapshot"] == "LIVE_CONFIG_URI_UNAVAILABLE"
    assert list(stores[0].objects)[-1] == "registration.json"
    assert json.loads(stores[0].objects["manifest.json"])["historical_pit_attested"] is False


def test_failure_never_registers_or_retries(monkeypatch):
    calls = []
    def denied(*args):
        calls.append(1)
        raise archive.ArchiveError("PROVIDER_READ_FAILED_NO_RETRY")
    monkeypatch.setattr(archive, "collect", denied)
    store = MemoryStore()
    with pytest.raises(archive.ArchiveError):
        archive.regenerate(None, store)
    assert calls == [1] and "manifest.json" not in store.objects and "registration.json" not in store.objects


def test_unavailable_supplied_config_stops_before_any_provider_read(monkeypatch):
    def forbidden(*args):
        raise AssertionError("provider must not be called")
    monkeypatch.setattr(archive, "collect", forbidden)
    store = MemoryStore()
    with pytest.raises(archive.ArchiveError, match="CONFIG_READ_UNAVAILABLE"):
        archive.regenerate(None, store, "gs://demo-bucket/research/v2/input/config.json")
    assert store.objects == {}


def test_actions_pagination_repeat_is_rejected():
    class Provider:
        def get(self, path, params):
            return {"corporate_actions": {"cash_dividends": [{"symbol": "SOXL"}]}, "next_page_token": "same"}
    with pytest.raises(archive.ArchiveError, match="PAGINATION_INVALID"):
        archive.collect(Provider(), MemoryStore(), "SOXL", "actions")


def test_store_enforces_create_only_and_exact_generation_readback():
    calls = []
    class Blob:
        generation = 321
        def upload_from_string(self, body, **kwargs):
            self.body = body
            calls.append(kwargs)
        def reload(self, **kwargs):
            calls.append(kwargs)
        def download_as_bytes(self, **kwargs):
            calls.append(kwargs)
            return self.body
    blob = Blob()
    client = SimpleNamespace(bucket=lambda name: SimpleNamespace(blob=lambda name: blob))
    result = archive.Store(client, "gs://demo-bucket/research/v2/input/fixture/").create("manifest.json", b"{}")
    assert calls[0]["if_generation_match"] == 0 and calls[0]["retry"] is None
    assert calls[2]["if_generation_match"] == 321 and calls[2]["retry"] is None
    assert result["sha256"] == blob.metadata["sha256"] == archive.sha(b"{}")


def test_registered_calendar_coverage_and_pagination_are_validated():
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS", start="2021-12-01", end="2026-08-25")
    rows = [{"t": str(s.date()) + "T05:00:00Z", "o": 10., "h": 11., "l": 9., "c": 10., "v": 1}
            for s in calendar.sessions_in_range("2021-12-01", "2026-08-25")]
    class Provider:
        def get(self, path, params):
            return {"symbol": "SOXL", "bars": rows[500:] if "page_token" in params else rows[:500],
                    "next_page_token": None if "page_token" in params else "synthetic-token"}
    store = MemoryStore()
    result = archive.collect(Provider(), store, "SOXL", "bars")
    assert result["count"] == 1187 and len(result["pages"]) == 2
    assert all(b"synthetic-token" not in body for body in store.objects.values())


def test_one_missing_session_is_rejected_before_page_write():
    class Provider:
        def get(self, path, params):
            return {"symbol": "SOXL", "bars": [{"t": "2021-12-02T05:00:00Z",
                    "o": 10., "h": 11., "l": 9., "c": 10., "v": 1}]}
    store = MemoryStore()
    with pytest.raises(archive.ArchiveError, match="BAR_VALIDATION_FAILED"):
        archive.collect(Provider(), store, "SOXL", "bars")
    assert store.objects == {}
