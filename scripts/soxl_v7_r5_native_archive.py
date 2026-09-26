"""Bounded one-shot SOXL V7 P1/P3 private archive, without schedule or orders.

The private destination is supplied only by the approved non-live environment.
This module never prints object addresses, credentials or market observations.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from scripts.acquire_soxl_core_only_free_split_close_p1 import TwelveYahooSplitAdjustedCloseObserver
from scripts.run_soxl_core_only_free_split_close_p3_evidence import (
    _load_isolated_replay,
    run_soxl_core_only_free_split_close_p3_offline_evidence,
)
from us_equity_snapshot_pipelines import twelve_data_daily, yfinance_prices
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p1 import (
    SoxlCoreOnlyFreeSplitCloseP1Error,
    SoxlCoreOnlyFreeSplitCloseP1UnavailableError,
    publish_soxl_core_only_free_split_close_p1_inputs,
    verify_soxl_core_only_free_split_close_p1_input_root,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)

ROOT_SHA256 = "8ce3d6c09b09a36d82baffd12bb31d85be2e82b81ed8c551cb0d7eda39ca4bbc"
DATE_CUTOFF = "2026-08-25"
UES_REVISION = "07b164d95f2ab4d4c54fd993f6f2040bd207d664"
CONFIG_SHA256 = "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"
P1_FILES = ("binding.json", "manifest.json", "closes.json", "assurance.json")
MAX_HTTP_REQUESTS = 24
MAX_HTTP_BYTES = 100 * 1024 * 1024
MAX_OBJECT_OPERATIONS = 80
MAX_OBJECT_BYTES = 250 * 1024 * 1024
MAX_SINGLE_UPLOAD_BYTES = 8 * 1024 * 1024
PROFILE = "v7_longterm_compounding_cash_reserve"
CONFIG = Path("config/soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve.json")


class R5ArchiveError(RuntimeError):
    """Sanitized terminal state for this fixed research run."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _root_parts(uri: str) -> tuple[str, str]:
    if digest(uri.encode()) != ROOT_SHA256 or not uri.startswith("gs://") or not uri.endswith("/"):
        raise R5ArchiveError("private root does not match frozen authorization")
    bucket, sep, prefix = uri[5:].partition("/")
    if not bucket or not sep or not prefix:
        raise R5ArchiveError("private root unavailable")
    return bucket, prefix


class FixedArchive:
    """Exact names only; create-only upload and generation-pinned readback."""

    def __init__(self, client: storage.Client, uri: str) -> None:
        bucket, prefix = _root_parts(uri)
        self.bucket = client.bucket(bucket)
        self.prefix = prefix
        self.operations = 0
        self.bytes_transferred = 0

    def _charge(self, *, operations: int = 1, bytes_transferred: int = 0) -> None:
        if (
            self.operations + operations > MAX_OBJECT_OPERATIONS
            or self.bytes_transferred + bytes_transferred > MAX_OBJECT_BYTES
        ):
            raise R5ArchiveError("private object budget exhausted")
        self.operations += operations
        self.bytes_transferred += bytes_transferred

    def _blob(self, name: str, *, generation: int | None = None):  # noqa: ANN202 - vendor blob type
        if not name or name.startswith("/") or ".." in Path(name).parts or "//" in name:
            raise R5ArchiveError("invalid private object name")
        return self.bucket.blob(self.prefix + name, generation=generation)

    def create(self, name: str, content: bytes) -> dict[str, object]:
        blob = self._blob(name)
        if len(content) > MAX_SINGLE_UPLOAD_BYTES:
            raise R5ArchiveError("private object exceeds bounded upload size")
        # Reserve two write requests conservatively, including possible upload
        # initiation. The following fixed-version read is charged separately.
        self._charge(operations=2, bytes_transferred=len(content))
        try:
            blob.upload_from_string(content, content_type="application/json", if_generation_match=0, retry=None)
            generation = int(blob.generation)
        except Exception as exc:  # noqa: BLE001 - do not expose object/identity details
            raise R5ArchiveError("private create-only write failed or outcome unknown") from exc
        if generation <= 0:
            raise R5ArchiveError("private generation unavailable after write")
        receipt = {"name": name, "generation": generation, "size_bytes": len(content), "sha256": digest(content)}
        if self.read(receipt) != content:
            raise R5ArchiveError("private fixed-version readback mismatch")
        return receipt

    def read(self, receipt: dict[str, object]) -> bytes:
        name, generation = str(receipt["name"]), int(receipt["generation"])
        size, expected_sha = int(receipt["size_bytes"]), str(receipt["sha256"])
        if generation <= 0 or size < 0:
            raise R5ArchiveError("invalid private object receipt")
        # Request one extra byte: a larger object then cannot masquerade as a
        # receipt for its matching prefix, and the transfer remains bounded.
        self._charge(bytes_transferred=size + 1)
        try:
            content = self._blob(name, generation=generation).download_as_bytes(
                start=0, end=size, if_generation_match=generation, retry=None
            )
        except Exception as exc:  # noqa: BLE001 - fail closed without GCS error body
            raise R5ArchiveError("private fixed-version read failed") from exc
        if len(content) != size or digest(content) != expected_sha:
            raise R5ArchiveError("private fixed-version readback mismatch")
        return content

    def verify_probe(self) -> dict[str, object]:
        content = canonical({"schema": "soxl_v7_r5_probe.v1", "candidate_id": candidate_id()})
        blob = self._blob("probe.json")
        self._charge()
        try:
            blob.reload(retry=None)
            generation = int(blob.generation)
            if int(blob.size) != len(content):
                raise R5ArchiveError("private probe size mismatch")
        except Exception as exc:  # noqa: BLE001
            raise R5ArchiveError("private probe unavailable") from exc
        receipt = {"name": "probe.json", "generation": generation, "size_bytes": len(content), "sha256": digest(content)}
        if self.read(receipt) != content:
            raise R5ArchiveError("private probe mismatch")
        return receipt

    def create_probe(self) -> dict[str, object]:
        content = canonical({"schema": "soxl_v7_r5_probe.v1", "candidate_id": candidate_id()})
        try:
            return self.create("probe.json", content)
        except R5ArchiveError as exc:
            if not isinstance(exc.__cause__, PreconditionFailed):
                raise
            return self.verify_probe()


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):  # noqa: ANN202
        return None


class _MeteredResponse:
    def __init__(self, response, meter: "HttpMeter") -> None:  # noqa: ANN001
        self.response = response
        self.meter = meter
        self.status = response.status

    def __enter__(self):  # noqa: ANN204
        self.response.__enter__()
        return self

    def __exit__(self, *args):  # noqa: ANN002, ANN204
        return self.response.__exit__(*args)

    def read(self, size: int = -1) -> bytes:
        remaining = MAX_HTTP_BYTES - self.meter.bytes_read
        if remaining < 1:
            raise R5ArchiveError("market body budget exhausted")
        header = self.response.headers.get("Content-Length")
        if header is not None and int(header) > remaining:
            raise R5ArchiveError("market body budget exhausted")
        content = self.response.read(remaining if size < 0 else min(size, remaining))
        self.meter.bytes_read += len(content)
        if len(content) == remaining and header is None:
            raise R5ArchiveError("market body size ambiguous at budget")
        return content


class HttpMeter:
    """One HTTP request per fixed source; no redirect, proxy or hidden retry."""

    def __init__(self) -> None:
        self.requests = 0
        self.bytes_read = 0
        self.opener = build_opener(ProxyHandler({}), _NoRedirect())

    def open(self, request, timeout: int = 30):  # noqa: ANN001, ANN202
        if urlparse(request.full_url).hostname not in {"api.twelvedata.com", "query1.finance.yahoo.com"}:
            raise R5ArchiveError("market request host outside frozen sources")
        if self.requests >= MAX_HTTP_REQUESTS:
            raise R5ArchiveError("market request budget exhausted")
        self.requests += 1
        return _MeteredResponse(self.opener.open(request, timeout=timeout), self)


def _check_yahoo_duplicate_dates(payload: dict[str, object]) -> None:
    try:
        result = payload["chart"]["result"][0]  # type: ignore[index]
        timestamps = result["timestamp"]
        dates = [datetime.fromtimestamp(int(value), UTC).date() for value in timestamps]
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise R5ArchiveError("Yahoo source dates unavailable before normalization") from exc
    if len(dates) != len(set(dates)):
        raise R5ArchiveError("Yahoo source has duplicate session dates")


class MarketTransport:
    """Apply one budget to both existing adapters without changing their math."""

    def __init__(self) -> None:
        self.meter = HttpMeter()
        self._originals = None

    def __enter__(self) -> HttpMeter:
        if yfinance_prices._resolve_yfinance_proxy() is not None:
            raise R5ArchiveError("unmetered Yahoo proxy configured")
        original_fetch = yfinance_prices._fetch_yahoo_chart_payload

        def checked_fetch(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            payload = original_fetch(*args, **kwargs)
            _check_yahoo_duplicate_dates(payload)
            return payload

        self._originals = (twelve_data_daily.urlopen, yfinance_prices.urlopen, original_fetch)
        twelve_data_daily.urlopen = self.meter.open
        yfinance_prices.urlopen = self.meter.open
        yfinance_prices._fetch_yahoo_chart_payload = checked_fetch
        return self.meter

    def __exit__(self, *_args) -> None:  # noqa: ANN002
        if self._originals is not None:
            twelve_data_daily.urlopen, yfinance_prices.urlopen, yfinance_prices._fetch_yahoo_chart_payload = (
                self._originals
            )


class ArchivingObserver(TwelveYahooSplitAdjustedCloseObserver):
    def __init__(self, api_key: str, archive: FixedArchive) -> None:
        super().__init__(api_key)
        self.archive = archive
        self.receipts: dict[str, dict[str, object]] = {}
        self.statuses: list[dict[str, object]] = []
        self.archive_failure: str | None = None

    def observe_daily_bars(self, *, source_id: str, symbol: str, start_date: str, date_cutoff: str):  # noqa: ANN201
        observation = super().observe_daily_bars(
            source_id=source_id, symbol=symbol, start_date=start_date, date_cutoff=date_cutoff
        )
        self.statuses.append({
            "source_id": source_id, "symbol": symbol,
            "status": observation.status, "reason_codes": list(observation.reason_codes),
        })
        if observation.snapshot is not None:
            name = f"source/{symbol}/{source_id}.json"
            content = canonical(observation.snapshot.to_dict())
            if digest(content) != observation.snapshot.snapshot_sha256:
                raise R5ArchiveError("source snapshot identity mismatch")
            try:
                self.receipts[name] = self.archive.create(name, content)
            except R5ArchiveError as exc:
                self.archive_failure = str(exc)
                raise
        return observation


def candidate_id() -> str:
    return P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id


def _verify_revisions(ues_project: Path, producer_revision: str) -> None:
    if len(producer_revision) != 40 or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() != producer_revision:
        raise R5ArchiveError("producer revision mismatch")
    if subprocess.run(["git", "merge-base", "--is-ancestor", "6ced4b83b3f450f8d77d20a17637881e55b454bb", "HEAD"], check=False).returncode:
        raise R5ArchiveError("cash-cost correction absent")
    if subprocess.check_output(["git", "-C", str(ues_project), "rev-parse", "HEAD"], text=True).strip() != UES_REVISION:
        raise R5ArchiveError("frozen UES revision mismatch")
    if digest(canonical(json.loads(CONFIG.read_bytes()))) != CONFIG_SHA256:
        raise R5ArchiveError("frozen V7 config mismatch")


def _readback_p1(archive: FixedArchive, receipts: dict[str, dict[str, object]], root: Path) -> str:
    root.mkdir(mode=0o700)
    for name in P1_FILES:
        content = archive.read(receipts[name])
        destination = root / name
        destination.write_bytes(content)
        destination.chmod(0o600)
    return verify_soxl_core_only_free_split_close_p1_input_root(
        root, p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
    )


def _p3_from_archived_replays(
    p1_root: Path, ues_project: Path, replays: list[dict[str, object]]
) -> dict[str, object]:
    indexed = {digest(canonical(item["input"])): item["output"] for item in replays}
    if len(indexed) != 15:
        raise R5ArchiveError("archived native replay count mismatch")

    def read_replay(*, input_path: Path, **_kwargs):  # noqa: ANN003, ANN202
        key = digest(canonical(json.loads(input_path.read_bytes())))
        if key not in indexed:
            raise R5ArchiveError("archived replay input mismatch")
        return indexed[key]

    return run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding=json.loads((p1_root / "binding.json").read_bytes()),
        manifest=json.loads((p1_root / "manifest.json").read_bytes()),
        closes_bytes=(p1_root / "closes.json").read_bytes(),
        assurance_bytes=(p1_root / "assurance.json").read_bytes(),
        ues_project=ues_project,
        p2_candidate_path=CONFIG,
        isolated_replay=read_replay,
        p2_profile=PROFILE,
    )


def execute(
    archive: FixedArchive,
    *,
    api_key: str,
    ues_project: Path,
    producer_revision: str,
    license_evidence_sha256: str,
) -> dict[str, object]:
    if not re.fullmatch(r"[0-9a-f]{64}", license_evidence_sha256):
        raise R5ArchiveError("supplier license precheck not bound")
    _verify_revisions(ues_project, producer_revision)
    if not api_key:
        raise R5ArchiveError("existing provider credential unavailable")
    # The prior preflight consumed one create and one fixed-generation read.
    # Reserve its conservative three-operation allowance in this run.
    archive._charge(operations=3)
    archive.verify_probe()
    archive.create(
        "attempt.json",
        canonical({
            "schema": "soxl_v7_r5_single_attempt.v1",
            "candidate_id": candidate_id(),
            "date_cutoff": DATE_CUTOFF,
            "producer_revision": producer_revision,
            "started_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        }),
    )
    with tempfile.TemporaryDirectory(prefix="soxl-v7-r5-") as directory:
        root = Path(directory)
        if root.is_symlink() or root.stat().st_mode & 0o777 != 0o700:
            raise R5ArchiveError("private runtime directory unavailable")
        observer = ArchivingObserver(api_key, archive)
        with MarketTransport() as meter:
            p1 = root / "new-p1"
            try:
                published = publish_soxl_core_only_free_split_close_p1_inputs(
                    observer,
                    output_root=p1,
                    observed_at=datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    producer={
                        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
                        "commit_sha": producer_revision,
                        "tree_sha": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip(),
                        "tool": "soxl_core_only_v7_longterm_compounding_cash_reserve_p1",
                        "tool_version": "v7",
                    },
                    date_cutoff=DATE_CUTOFF,
                    p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
                )
            except SoxlCoreOnlyFreeSplitCloseP1UnavailableError as exc:
                raise R5ArchiveError(
                    f"new P1 unavailable: {exc.reason_code}; observations={canonical(observer.statuses).decode()}"
                ) from None
            except SoxlCoreOnlyFreeSplitCloseP1Error:
                if observer.archive_failure is not None:
                    raise R5ArchiveError(observer.archive_failure) from None
                raise R5ArchiveError(
                    f"new P1 contract failed; observations={canonical(observer.statuses).decode()}"
                ) from None
        if meter.requests > MAX_HTTP_REQUESTS or meter.bytes_read > MAX_HTTP_BYTES or len(observer.receipts) != 6:
            raise R5ArchiveError("new P1 request or source coverage incomplete")
        manifest_sha = verify_soxl_core_only_free_split_close_p1_input_root(
            p1, p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
        )
        if manifest_sha != published["manifest_sha256"]:
            raise R5ArchiveError("new P1 manifest mismatch")
        p1_receipts = {name: archive.create(f"p1/{name}", (p1 / name).read_bytes()) for name in P1_FILES}
        config_receipt = archive.create("metadata/candidate_config.json", CONFIG.read_bytes())
        readback_root = root / "readback-p1"
        if _readback_p1(archive, p1_receipts, readback_root) != manifest_sha:
            raise R5ArchiveError("archived P1 manifest mismatch")

        frozen_replay = _load_isolated_replay(p2_profile=PROFILE)
        replays: list[dict[str, object]] = []

        def recording_replay(*, input_path: Path, **kwargs):  # noqa: ANN003, ANN202
            replay_input = json.loads(input_path.read_bytes())
            result = frozen_replay(input_path=input_path, **kwargs)
            replays.append({"input": replay_input, "output": result})
            return result

        summary = run_soxl_core_only_free_split_close_p3_offline_evidence(
            binding=json.loads((readback_root / "binding.json").read_bytes()),
            manifest=json.loads((readback_root / "manifest.json").read_bytes()),
            closes_bytes=(readback_root / "closes.json").read_bytes(),
            assurance_bytes=(readback_root / "assurance.json").read_bytes(),
            ues_project=ues_project,
            p2_candidate_path=CONFIG,
            isolated_replay=recording_replay,
            p2_profile=PROFILE,
        )
        if summary.get("status") != "SUCCESS" or len(summary.get("runs", [])) != 15 or len(replays) != 15:
            raise R5ArchiveError("native P3 fixed suite incomplete")
        replay_receipt = archive.create("p3/replays.json", canonical(replays))
        summary_receipt = archive.create("p3/summary.json", canonical(summary))
        archived_replays = json.loads(archive.read(replay_receipt))
        archived_summary = json.loads(archive.read(summary_receipt))
        recomputed = _p3_from_archived_replays(readback_root, ues_project, archived_replays)
        if canonical(recomputed) != canonical(archived_summary):
            raise R5ArchiveError("archived native P3 numerical readback mismatch")
        completion = {
            "schema_version": "qsl.soxl-v7-r5-native-completion.v1",
            "candidate_id": candidate_id(),
            "date_cutoff": DATE_CUTOFF,
            "producer_revision": producer_revision,
            "ues_revision": UES_REVISION,
            "candidate_config_sha256": CONFIG_SHA256,
            "candidate_config": config_receipt,
            "producer_git_tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip(),
            "producer_lockfile_sha256": digest(Path("uv.lock").read_bytes()),
            "qpk_dependency": next(
                dependency for dependency in tomllib.loads(Path("pyproject.toml").read_text())["project"]["dependencies"]
                if dependency.startswith("quant-platform-kit @ ")
            ),
            "supplier_license_evidence_sha256": license_evidence_sha256,
            "p1_manifest_sha256": manifest_sha,
            "sources": observer.receipts,
            "p1": p1_receipts,
            "replays": replay_receipt,
            "p3": summary_receipt,
            "http_requests": meter.requests,
            "http_bytes": meter.bytes_read,
        }
        complete_receipt = archive.create("complete.json", canonical(completion))
        if archive.read(complete_receipt) != canonical(completion):
            raise R5ArchiveError("native completion readback mismatch")
        return {
            "status": "COMPLETE",
            "candidate_id": candidate_id(),
            "p1_manifest_sha256": manifest_sha,
            "p3_summary_sha256": summary_receipt["sha256"],
            "http_requests": meter.requests,
            "http_bytes": meter.bytes_read,
            "object_operations": archive.operations,
            "object_bytes": archive.bytes_transferred,
        }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("preflight", "execute"))
    parser.add_argument("--ues-project", type=Path)
    parser.add_argument("--license-evidence-sha256", default="")
    args = parser.parse_args()
    archive = None
    try:
        archive = FixedArchive(storage.Client(), os.environ.get("SOXL_V7_R5_PRIVATE_ROOT", ""))
        if args.mode == "preflight":
            archive.create_probe()
            result = {"status": "PREFLIGHT_COMPLETE", "object_operations": archive.operations}
        else:
            if args.ues_project is None:
                raise R5ArchiveError("frozen UES checkout unavailable")
            result = execute(
                archive,
                api_key=os.environ.get("TWELVE_DATA_API_KEY", ""),
                ues_project=args.ues_project,
                producer_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                license_evidence_sha256=args.license_evidence_sha256,
            )
    except Exception as exc:  # noqa: BLE001 - no private error detail in public logs
        print(json.dumps({
            "status": "BLOCKED", "failure_class": type(exc).__name__,
            "reason": str(exc) if isinstance(exc, R5ArchiveError) else "private operation failed",
            "object_operations": 0 if archive is None else archive.operations,
        }))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
