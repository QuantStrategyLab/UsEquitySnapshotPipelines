#!/usr/bin/env python3
"""Run offline P3 evidence for one explicit SOXL v4 P1 root.

This façade reads only explicit local P1 members, materializes the separately
assured close-only input, and delegates fixed replays to the v4 isolated UES
runner.  It acquires no data and never accesses cloud storage, credentials,
workflows, brokers, accounts, or orders.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p3_evidence import (
    SoxlCoreOnlyFreeSplitCloseP3EvidenceError,
    build_soxl_core_only_free_split_close_p3_evidence_plan,
    build_soxl_core_only_free_split_close_p3_evidence_summary,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p3_input_materializer import (
    SoxlCoreOnlyFreeSplitCloseP3MaterializerError,
    materialize_soxl_core_only_free_split_close_p3_input,
)

RUN_SCHEMA = "qsl.soxl-soxx-core-only-free-split-close-p3-offline-run.v1"


class SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError(ValueError):
    """Sanitized error at the v4 offline P3 orchestration boundary."""


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("invalid SOXL free-source offline P3 arguments")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("invalid SOXL free-source offline P3 value") from exc


def _read_json(path: Path) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("invalid SOXL free-source offline P3 input") from exc
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("invalid SOXL free-source offline P3 input")
    return dict(value)


def _read_member(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("invalid SOXL free-source offline P3 input") from exc


def _load_isolated_replay() -> Callable[..., Mapping[str, object]]:
    runner_path = Path(__file__).with_name("run_soxl_core_only_free_split_close_p3_isolated.py")
    spec = importlib.util.spec_from_file_location("qsl_soxl_core_only_free_split_close_p3_isolated", runner_path)
    if spec is None or spec.loader is None:
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("isolated SOXL v4 runner unavailable")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - defensive import boundary
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("isolated SOXL v4 runner unavailable") from exc
    replay = getattr(module, "run_isolated_replay", None)
    if not callable(replay):
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("isolated SOXL v4 runner unavailable")
    return replay


def run_soxl_core_only_free_split_close_p3_offline_evidence(
    *,
    binding: Mapping[str, object],
    manifest: Mapping[str, object],
    closes_bytes: bytes,
    assurance_bytes: bytes,
    ues_project: Path,
    p2_candidate_path: Path,
    isolated_replay: Callable[..., Mapping[str, object]],
) -> dict[str, object]:
    """Return a metrics-only P3 summary after every fixed replay succeeds."""
    if not callable(isolated_replay):
        raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("isolated SOXL v4 runner unavailable")
    materialized = materialize_soxl_core_only_free_split_close_p3_input(
        binding=binding,
        manifest=manifest,
        closes_bytes=closes_bytes,
        assurance_bytes=assurance_bytes,
    )
    plan = build_soxl_core_only_free_split_close_p3_evidence_plan(materialized)

    def execute(replay_input: Mapping[str, object]) -> Mapping[str, object]:
        with tempfile.TemporaryDirectory(prefix="qsl-soxl-v4-p3-") as directory:
            input_path = Path(directory) / "replay-input.json"
            input_path.write_bytes(_canonical(replay_input))
            result = isolated_replay(
                ues_project=ues_project,
                input_path=input_path,
                p2_candidate_path=p2_candidate_path,
            )
        if not isinstance(result, Mapping):
            raise SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError("isolated SOXL v4 runner unavailable")
        return result

    return build_soxl_core_only_free_split_close_p3_evidence_summary(
        materialized=materialized,
        evidence_plan=plan,
        replay_executor=execute,
    )


def _arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--p1-binding", required=True, type=Path)
    parser.add_argument("--input-manifest", required=True, type=Path)
    parser.add_argument("--closes-member", required=True, type=Path)
    parser.add_argument("--assurance-member", required=True, type=Path)
    parser.add_argument("--ues-project", required=True, type=Path)
    parser.add_argument("--p2-candidate", required=True, type=Path)
    return parser.parse_args(argv)


def _parked(failure_class: str) -> dict[str, str]:
    return {"schema_version": RUN_SCHEMA, "status": "PARKED", "failure_class": failure_class}


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(sys.argv[1:] if argv is None else argv)
        result = run_soxl_core_only_free_split_close_p3_offline_evidence(
            binding=_read_json(args.p1_binding),
            manifest=_read_json(args.input_manifest),
            closes_bytes=_read_member(args.closes_member),
            assurance_bytes=_read_member(args.assurance_member),
            ues_project=args.ues_project,
            p2_candidate_path=args.p2_candidate,
            isolated_replay=_load_isolated_replay(),
        )
    except (
        SoxlCoreOnlyFreeSplitCloseP3OfflineEvidenceError,
        SoxlCoreOnlyFreeSplitCloseP3MaterializerError,
        SoxlCoreOnlyFreeSplitCloseP3EvidenceError,
        ValueError,
        OSError,
    ):
        result = _parked("p1_p2_p3_contract_or_runtime_unavailable")
    except Exception:  # pragma: no cover - final no-leak process boundary
        result = _parked("p3_internal_failure")
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
