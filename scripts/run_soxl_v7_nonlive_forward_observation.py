#!/usr/bin/env python3
"""Run one V7 Shadow/simulated-Paper observation without broker access."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path

from scripts.run_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_isolated import (
    run_isolated_replay,
    run_isolated_source,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p3_input_materializer import (
    materialize_soxl_core_only_free_split_close_p3_input,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation import (
    SoxlV7NonliveForwardObservationError,
    build_soxl_v7_nonlive_forward_inputs,
    build_soxl_v7_nonlive_forward_record,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SoxlV7NonliveForwardObservationError("invalid non-live observation input") from exc


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--p1-root", type=Path, required=True)
    parser.add_argument("--p2-candidate", type=Path, required=True)
    parser.add_argument("--ues-project", type=Path, required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous-record", type=Path)
    return parser.parse_args(argv)


def _sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )


def _run(args: argparse.Namespace) -> dict[str, object]:
    root = args.p1_root
    materialized = materialize_soxl_core_only_free_split_close_p3_input(
        binding=_read_json(root / "binding.json"),
        manifest=_read_json(root / "manifest.json"),
        closes_bytes=(root / "closes.json").read_bytes(),
        assurance_bytes=(root / "assurance.json").read_bytes(),
        p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
    )
    inputs = build_soxl_v7_nonlive_forward_inputs(materialized)
    previous = _read_json(args.previous_record) if args.previous_record else None
    with tempfile.TemporaryDirectory(prefix="soxl-v7-nonlive-") as raw_temp:
        temp = Path(raw_temp)
        shadow_input = temp / "shadow-context.json"
        _write_json(shadow_input, inputs.shadow_source_context)
        shadow = run_isolated_source(
            ues_project=args.ues_project,
            input_path=shadow_input,
            p2_candidate_path=args.p2_candidate,
        )
        shadow_digest = str(shadow.get("result_sha256") or "")
        if len(inputs.observation_sessions) == 1:
            paper_digest = _sha256(
                {
                    "kind": "initial_cash_baseline",
                    "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
                    "session": inputs.observation_sessions[0],
                }
            )
        else:
            paper_input = temp / "simulated-paper-replay.json"
            _write_json(paper_input, inputs.simulated_paper_replay_input)
            paper = run_isolated_replay(
                ues_project=args.ues_project,
                input_path=paper_input,
                p2_candidate_path=args.p2_candidate,
            )
            paper_digest = str(paper.get("result_sha256") or "")
    return build_soxl_v7_nonlive_forward_record(
        observed_at=args.observed_at,
        inputs=inputs,
        shadow_observation_sha256=shadow_digest,
        simulated_paper_observation_sha256=paper_digest,
        previous_record=previous if isinstance(previous, dict) else None,
    )


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        record = _run(args)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    except (OSError, ValueError, SoxlV7NonliveForwardObservationError):
        print('{"stage":"P4","status":"PARKED"}')
        return 2
    controller = record["controller"]
    assert isinstance(controller, dict)
    print(
        json.dumps(
            {
                "state": controller["state"],
                "observations_completed": controller["observations_completed"],
                "record_sha256": record["record_sha256"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
