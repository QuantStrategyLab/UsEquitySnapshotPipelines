"""One bounded R6 Twelve-only source archive and unchanged V7 numerical replay."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from google.cloud import storage

from scripts.soxl_v7_r5_native_archive import (
    FixedArchive,
    HttpMeter,
    R5ArchiveError,
    _load_isolated_replay,
    _verify_revisions,
    canonical,
    digest,
)
from us_equity_snapshot_pipelines import twelve_data_daily
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence import (
    build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan,
    build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_v7_twelve_single_source_r6 import (
    DATE_CUTOFF,
    FILES,
    STUDY_ID,
    SYMBOLS,
    R6InputError,
    build_input,
    materialize_input,
    read_input,
    verify_input,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import expected_soxl_core_only_sessions

ROOT_SHA256 = "274610e59a8d654b412af9155ee4c3bd59b90a53fc5f8bbc56091d346f60070c"
MAX_HTTP_REQUESTS = 12
MAX_HTTP_BYTES = 100 * 1024 * 1024
MAX_OBJECT_OPERATIONS = 160
MAX_OBJECT_BYTES = 512 * 1024 * 1024
PROFILE = "v7_longterm_compounding_cash_reserve"
CONFIG = Path("config/soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve.json")


class R6ArchiveError(RuntimeError):
    """Sanitized terminal outcome for the separate R6 execution."""


def _validate_twelve_payload(payload: object) -> None:
    """Reject metadata drift and duplicates before the existing normalizer sorts."""
    try:
        if not isinstance(payload, dict) or not isinstance(payload.get("meta"), dict):
            raise ValueError
        meta = payload["meta"]
        if meta.get("currency") != "USD" or meta.get("interval") != "1day" or meta.get("type") != "ETF":
            raise ValueError
        values = payload["values"]
        if not isinstance(values, list) or not values:
            raise ValueError
        dates = [str(item["datetime"]) for item in values]
        if len(dates) != len(set(dates)) or not (dates == sorted(dates) or dates == sorted(dates, reverse=True)):
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise R6ArchiveError("Twelve Data metadata or raw session order invalid") from exc


class _RawCapture:
    """Persist the bounded provider body before it is parsed or normalized."""

    def __init__(self, response, *, archive: FixedArchive, symbol: str, receipts: dict[str, dict[str, object]]) -> None:  # noqa: ANN001
        self.response = response
        self.archive = archive
        self.symbol = symbol
        self.receipts = receipts
        self.status = response.status
        self.read_once = False

    def __enter__(self):  # noqa: ANN204
        self.response.__enter__()
        return self

    def __exit__(self, *args):  # noqa: ANN002, ANN204
        return self.response.__exit__(*args)

    def read(self, size: int = -1) -> bytes:
        if self.read_once or size != -1:
            raise R6ArchiveError("R6 raw response read contract invalid")
        self.read_once = True
        body = self.response.read()
        self.receipts[self.symbol] = self.archive.create(f"source_raw/{self.symbol}.json", body)
        return body


class TwelveTransport:
    """Meter only the approved Twelve endpoint; no Yahoo source is reachable."""

    def __init__(self, archive: FixedArchive) -> None:
        self.archive = archive
        self.raw_receipts: dict[str, dict[str, object]] = {}
        self.meter = HttpMeter(
            allowed_hosts=frozenset({"api.twelvedata.com"}),
            max_requests=MAX_HTTP_REQUESTS,
            max_bytes=MAX_HTTP_BYTES,
        )
        self.originals = None

    def __enter__(self) -> HttpMeter:
        original_normalize = twelve_data_daily._normalize_daily_bars

        def checked_open(request, timeout=30):  # noqa: ANN001, ANN202
            query = parse_qs(urlparse(request.full_url).query)
            symbols = query.get("symbol", [])
            if len(symbols) != 1 or symbols[0] not in SYMBOLS or symbols[0] in self.raw_receipts:
                raise R6ArchiveError("R6 raw source request identity invalid")
            return _RawCapture(
                self.meter.open(request, timeout=timeout),
                archive=self.archive,
                symbol=symbols[0],
                receipts=self.raw_receipts,
            )

        def checked_normalize(payload, *, symbol, date_cutoff):  # noqa: ANN001, ANN202
            _validate_twelve_payload(payload)
            if payload["meta"].get("symbol") != symbol:
                raise R6ArchiveError("Twelve Data symbol mismatch")
            return original_normalize(payload, symbol=symbol, date_cutoff=date_cutoff)

        self.originals = (twelve_data_daily.urlopen, original_normalize)
        twelve_data_daily.urlopen = checked_open
        twelve_data_daily._normalize_daily_bars = checked_normalize
        return self.meter

    def __exit__(self, *_args) -> None:  # noqa: ANN002
        if self.originals is not None:
            twelve_data_daily.urlopen, twelve_data_daily._normalize_daily_bars = self.originals


def _replay_suite(members: dict[str, bytes], ues_project: Path, replay) -> dict[str, object]:  # noqa: ANN001
    materialized = materialize_input(members)
    plan = build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan(materialized)
    return build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=replay,
    )


def _r6_report(numeric_component: dict[str, object], *, manifest_sha: str, observed_at: str) -> dict[str, object]:
    """Bind unchanged V7 numbers to the new, weaker R6 evaluation identity."""
    return {
        "schema_version": "qsl.soxl-v7-r6-twelve-single-source-report.v1",
        "study_id": STUDY_ID,
        "input_contract_id": "qsl.soxl-v7-twelve-single-source-r6-input.v1",
        "input_manifest_sha256": manifest_sha,
        "source_assurance": "single_source_structural_only_no_cross_provider_verification",
        "observed_at": observed_at,
        "date_cutoff": DATE_CUTOFF,
        "historical_point_in_time_certified": False,
        "window_classification": "retrospective_development_including_legacy_oos_named_windows",
        "original_dual_source_p1_p3_verified": False,
        "joint_account_admission": "NOT_ADMITTED",
        "numeric_component": numeric_component,
    }


def _verify_replay_accounting(replays: list[dict[str, object]]) -> dict[str, object]:
    """Recompute archived fills, cash, equity, turnover and fees without a signal call."""
    if len(replays) != 15:
        raise R6ArchiveError("R6 replay count invalid")
    checked = []
    for record in replays:
        try:
            replay_input, envelope = record["input"], record["output"]
            output = envelope["replay"]
            bps = int(replay_input["cost_bps"])
            sessions = replay_input["sessions"]
            decisions = output["decisions"]
            if bps not in {5, 10, 15} or output["cost_bps"] != bps or len(decisions) != len(sessions):
                raise ValueError
            cash = float(replay_input["initial_equity"])
            quantities = {symbol: 0.0 for symbol in SYMBOLS}
            pending_weights = None
            pending_cash_weight = None
            turnover_total = 0.0
            cost_total = 0.0
            for session, decision in zip(sessions, decisions, strict=True):
                prices = {symbol: float(session["prices"][symbol]) for symbol in SYMBOLS}
                if any(not math.isfinite(price) or price <= 0 for price in prices.values()):
                    raise ValueError
                market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in SYMBOLS}
                before = cash + sum(market_values.values())
                turnover = 0.0
                fee = 0.0
                if pending_weights is not None:
                    if pending_cash_weight is None or before <= 0:
                        raise ValueError
                    turnover = 0.5 * (
                        sum(abs(pending_weights[symbol] - market_values[symbol] / before) for symbol in SYMBOLS)
                        + abs(pending_cash_weight - cash / before)
                    )
                    fee = before * turnover * bps / 10_000.0
                    after = before - fee
                    if after <= 0:
                        raise ValueError
                    quantities = {symbol: pending_weights[symbol] * after / prices[symbol] for symbol in SYMBOLS}
                    cash = pending_cash_weight * after
                    market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in SYMBOLS}
                    turnover_total += turnover
                    cost_total += fee
                equity = cash + sum(market_values.values())
                if (
                    not math.isclose(turnover, float(decision["executed_one_way_turnover"]), rel_tol=1e-10, abs_tol=1e-7)
                    or not math.isclose(fee, float(decision["executed_cost"]), rel_tol=1e-10, abs_tol=1e-7)
                    or not math.isclose(equity, float(decision["equity_before_signal"]), rel_tol=1e-10, abs_tol=1e-7)
                    or decision["signal_as_of"] != session["as_of"]
                ):
                    raise ValueError
                pending_weights = decision["pending_target_weights"]
                pending_cash_weight = decision["pending_cash_weight"]
                if pending_weights is not None:
                    weights = [float(pending_weights[symbol]) for symbol in SYMBOLS]
                    pending_cash_weight = float(pending_cash_weight)
                    if (
                        any(not math.isfinite(weight) or weight < 0 for weight in weights)
                        or not math.isfinite(pending_cash_weight)
                        or pending_cash_weight < 0
                        or not math.isclose(sum(weights) + pending_cash_weight, 1.0, abs_tol=1e-9)
                    ):
                        raise ValueError
                elif pending_cash_weight is not None:
                    raise ValueError
            if (
                output["executed_signal_count"] != len(decisions) - 1
                or not math.isclose(turnover_total, float(output["one_way_turnover"]), rel_tol=1e-10, abs_tol=1e-7)
                or not math.isclose(cost_total, float(output["cost_total"]), rel_tol=1e-10, abs_tol=1e-7)
                or not math.isclose(equity, float(output["final_equity"]), rel_tol=1e-10, abs_tol=1e-7)
            ):
                raise ValueError
            checked.append({
                "replay_input_sha256": digest(canonical(replay_input)),
                "replay_output_sha256": digest(canonical(envelope)),
                "recomputed_one_way_turnover": turnover_total,
                "recomputed_cost_total": cost_total,
                "recomputed_final_equity": equity,
                "recomputed_cash": cash,
            })
        except (KeyError, TypeError, ValueError, IndexError, AttributeError, OverflowError) as exc:
            raise R6ArchiveError("R6 archived fee or equity accounting invalid") from exc
    return {"schema_version": "qsl.soxl-v7-r6-accounting-check.v1", "runs": checked}


def _verify_archived_sources(
    archive: FixedArchive,
    raw_receipts: dict[str, dict[str, object]],
    snapshot_receipts: dict[str, dict[str, object]],
    members: dict[str, bytes],
) -> dict[str, object]:
    """Rebuild each normalized observation from its generation-pinned raw body."""
    _manifest_sha, close_series = verify_input(members)
    report = json.loads(members["assurance.json"])
    if set(raw_receipts) != set(SYMBOLS) or set(snapshot_receipts) != set(SYMBOLS):
        raise R6ArchiveError("R6 source receipt set incomplete")
    trace = {}
    for symbol in SYMBOLS:
        try:
            raw_bytes = archive.read(raw_receipts[symbol])
            snapshot_bytes = archive.read(snapshot_receipts[symbol])
            payload = json.loads(raw_bytes)
            _validate_twelve_payload(payload)
            if payload["meta"].get("symbol") != symbol:
                raise ValueError
            bars = twelve_data_daily._normalize_daily_bars(payload, symbol=symbol, date_cutoff=DATE_CUTOFF)
            start = expected_soxl_core_only_sessions(DATE_CUTOFF)[symbol][0].isoformat()
            source_artifact_sha256 = digest(canonical({
                "source_id": twelve_data_daily.TWELVE_DATA_DAILY_SOURCE_ID,
                "symbol": symbol,
                "start_date": start,
                "date_cutoff": DATE_CUTOFF,
                "adjustment_basis": "split_adjusted",
                "bars": [bar.to_dict() for bar in bars],
            }))
            rebuilt = {
                "source_id": twelve_data_daily.TWELVE_DATA_DAILY_SOURCE_ID,
                "symbol": symbol,
                "date_cutoff": DATE_CUTOFF,
                "adjustment_basis": "split_adjusted",
                "source_artifact_sha256": source_artifact_sha256,
                "bars": [bar.to_dict() for bar in bars],
            }
            if canonical(rebuilt) != snapshot_bytes:
                raise ValueError
            assurance = report["symbols"][symbol]
            if assurance["source_snapshot_sha256"] != digest(snapshot_bytes):
                raise ValueError
            closes = [{"session_date": bar.session_date, "close": bar.close} for bar in bars]
            if closes != close_series[symbol]:
                raise ValueError
            trace[symbol] = {
                "raw_response_sha256": digest(raw_bytes),
                "normalized_snapshot_sha256": digest(snapshot_bytes),
                "normalized_close_sha256": assurance["canonical_close_series_sha256"],
                "session_count": len(bars),
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise R6ArchiveError("R6 raw-to-normalized source readback invalid") from exc
    return {"schema_version": "qsl.soxl-v7-r6-source-trace.v1", "symbols": trace}


def execute(
    archive: FixedArchive,
    *,
    api_key: str,
    ues_project: Path,
    producer_revision: str,
    license_evidence_sha256: str,
) -> dict[str, object]:
    if not re.fullmatch(r"[0-9a-f]{64}", license_evidence_sha256):
        raise R6ArchiveError("Twelve Data rights record unbound")
    _verify_revisions(ues_project, producer_revision)
    if not api_key:
        raise R6ArchiveError("existing provider credential unavailable")
    archive._charge(operations=3)  # the one earlier probe write/read
    archive.verify_probe()
    archive.create("attempt.json", canonical({
        "schema": "soxl-v7-r6-single-attempt.v1", "study_id": STUDY_ID,
        "date_cutoff": DATE_CUTOFF, "producer_revision": producer_revision,
        "started_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
    }))
    with tempfile.TemporaryDirectory(prefix="soxl-v7-r6-") as directory:
        root = Path(directory)
        if root.is_symlink() or root.stat().st_mode & 0o777 != 0o700:
            raise R6ArchiveError("private runtime directory unavailable")
        observed_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        observations = {}
        source_receipts = {}
        transport = TwelveTransport(archive)
        with transport as meter:
            for symbol in SYMBOLS:
                start = expected_soxl_core_only_sessions(DATE_CUTOFF)[symbol][0].isoformat()
                observation = twelve_data_daily.observe_twelve_data_adjusted_daily_bars(
                    api_key=api_key, symbol=symbol, start_date=start, date_cutoff=DATE_CUTOFF
                )
                if observation.status != "READY" or observation.snapshot is None:
                    reason = ",".join(observation.reason_codes) or "SOURCE_UNAVAILABLE"
                    raise R6ArchiveError(f"Twelve Data {symbol} unavailable: {reason}")
                if symbol not in transport.raw_receipts:
                    raise R6ArchiveError("R6 provider response not archived")
                observations[symbol] = observation
                source_receipts[symbol] = archive.create(
                    f"source/{symbol}.json", canonical(observation.snapshot.to_dict())
                )
        if meter.requests != 3 or meter.bytes_read > MAX_HTTP_BYTES or set(transport.raw_receipts) != set(SYMBOLS):
            raise R6ArchiveError("R6 source request count incomplete")
        members = build_input(
            observations,
            observed_at=observed_at,
            producer={
                "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
                "commit_sha": producer_revision,
                "tree_sha": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip(),
                "tool": "soxl_v7_r6_twelve_single_source", "tool_version": "r6.v1",
            },
        )
        manifest_sha, _series = verify_input(members)
        p1_receipts = {name: archive.create(f"p1/{name}", members[name]) for name in FILES}
        config_receipt = archive.create("metadata/candidate_config.json", CONFIG.read_bytes())
        readback = root / "readback-p1"
        readback.mkdir(mode=0o700)
        for name in FILES:
            file = readback / name
            file.write_bytes(archive.read(p1_receipts[name]))
            file.chmod(0o600)
        frozen_members = read_input(readback)
        if verify_input(frozen_members)[0] != manifest_sha:
            raise R6ArchiveError("R6 archived input mismatch")
        source_trace = _verify_archived_sources(
            archive, transport.raw_receipts, source_receipts, frozen_members
        )
        trace_receipt = archive.create("metadata/source_trace.json", canonical(source_trace))

        native_replay = _load_isolated_replay(p2_profile=PROFILE)
        replay_records: list[dict[str, object]] = []

        def recording_replay(replay_input):  # noqa: ANN001, ANN202
            with tempfile.TemporaryDirectory(prefix="soxl-r6-replay-") as replay_directory:
                input_path = Path(replay_directory) / "input.json"
                input_path.write_bytes(canonical(replay_input))
                output = native_replay(
                    ues_project=ues_project, input_path=input_path, p2_candidate_path=CONFIG
                )
            replay_records.append({"input": replay_input, "output": output})
            return output

        numeric_component = _replay_suite(frozen_members, ues_project, recording_replay)
        if numeric_component.get("status") != "SUCCESS" or len(numeric_component.get("runs", [])) != 15 or len(replay_records) != 15:
            raise R6ArchiveError("R6 fixed numerical suite incomplete")
        _verify_replay_accounting(replay_records)
        summary = _r6_report(numeric_component, manifest_sha=manifest_sha, observed_at=observed_at)
        replay_receipt = archive.create("p3/replays.json", canonical(replay_records))
        summary_receipt = archive.create("p3/summary.json", canonical(summary))
        archived_records = json.loads(archive.read(replay_receipt))
        archived_summary = json.loads(archive.read(summary_receipt))
        accounting = _verify_replay_accounting(archived_records)
        accounting_receipt = archive.create("p3/accounting_check.json", canonical(accounting))
        indexed = {digest(canonical(item["input"])): item["output"] for item in archived_records}
        if len(indexed) != 15:
            raise R6ArchiveError("R6 archived replay inputs incomplete")

        def archived_replay(replay_input):  # noqa: ANN001, ANN202
            key = digest(canonical(replay_input))
            if key not in indexed:
                raise R6ArchiveError("R6 archived replay identity mismatch")
            return indexed[key]

        recomputed = _r6_report(
            _replay_suite(frozen_members, ues_project, archived_replay),
            manifest_sha=manifest_sha,
            observed_at=observed_at,
        )
        if canonical(recomputed) != canonical(archived_summary):
            raise R6ArchiveError("R6 numerical readback mismatch")
        complete = {
            "schema": "soxl-v7-r6-twelve-single-completion.v1",
            "study_id": STUDY_ID,
            "signal_candidate_id": json.loads(frozen_members["binding.json"])["signal_candidate_id"],
            "source_assurance": "single_source_structural_only_no_cross_provider_verification",
            "historical_point_in_time_certified": False,
            "date_cutoff": DATE_CUTOFF,
            "producer_revision": producer_revision,
            "producer_tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip(),
            "producer_lock_sha256": digest(Path("uv.lock").read_bytes()),
            "license_evidence_sha256": license_evidence_sha256,
            "observed_at": observed_at,
            "sources": source_receipts,
            "raw_responses": transport.raw_receipts,
            "source_trace": trace_receipt,
            "p1": p1_receipts,
            "p1_manifest_sha256": manifest_sha,
            "candidate_config": config_receipt,
            "replays": replay_receipt,
            "summary": summary_receipt,
            "accounting_check": accounting_receipt,
            "http_requests": meter.requests,
            "time_series_credits": meter.requests,
            "http_bytes": meter.bytes_read,
        }
        receipt = archive.create("complete.json", canonical(complete))
        if archive.read(receipt) != canonical(complete):
            raise R6ArchiveError("R6 completion readback mismatch")
        return {
            "status": "COMPLETE", "study_id": STUDY_ID,
            "p1_manifest_sha256": manifest_sha,
            "p3_summary_sha256": summary_receipt["sha256"],
            "http_requests": meter.requests,
            "time_series_credits": meter.requests,
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
        archive = FixedArchive(
            storage.Client(), os.environ.get("SOXL_V7_R6_PRIVATE_ROOT", ""),
            root_sha256=ROOT_SHA256,
            max_operations=MAX_OBJECT_OPERATIONS,
            max_bytes=MAX_OBJECT_BYTES,
            probe_content=canonical({"schema": "soxl_v7_r6_probe.v1", "study_id": STUDY_ID}),
        )
        if args.mode == "preflight":
            archive.create_probe()
            result = {"status": "PREFLIGHT_COMPLETE", "object_operations": archive.operations}
        else:
            if args.ues_project is None:
                raise R6ArchiveError("frozen UES checkout unavailable")
            result = execute(
                archive,
                api_key=os.environ.get("TWELVE_DATA_API_KEY", ""),
                ues_project=args.ues_project,
                producer_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                license_evidence_sha256=args.license_evidence_sha256,
            )
    except Exception as exc:  # noqa: BLE001 - never expose private error bodies
        print(json.dumps({
            "status": "BLOCKED", "failure_class": type(exc).__name__,
            "reason": str(exc) if isinstance(exc, (R5ArchiveError, R6ArchiveError, R6InputError)) else "private operation failed",
            "object_operations": 0 if archive is None else archive.operations,
        }, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
