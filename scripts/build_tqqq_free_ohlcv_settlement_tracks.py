"""Build redacted T+0/T+1/T+2 source-settlement tracks from run artifacts."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_free_ohlcv_p1 import (
    TqqqCoreOnlyFreeOhlcvP1Error,
    build_tqqq_core_only_free_ohlcv_settlement_track,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    P2_V9_CONTRACT,
)

_BATCH_SCHEMA = "qsl.tqqq-free-ohlcv-settlement-observation-batch.v1"
_TRACK_BATCH_SCHEMA = "qsl.tqqq-free-ohlcv-settlement-track-batch.v1"


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _load_observation_batch(path: Path) -> list[tuple[int, dict[str, object]]]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid settlement observation batch") from exc
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != _BATCH_SCHEMA
        or value.get("candidate")
        != {
            "candidate_id": P2_V9_CONTRACT.candidate_id,
            "config_sha256": P2_V9_CONTRACT.config_sha256,
        }
        or value.get("no_order") is not True
        or value.get("automatic_promotion") is not False
        or not isinstance(value.get("observations"), list)
    ):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid settlement observation batch")
    observations: list[tuple[int, dict[str, object]]] = []
    for entry in value["observations"]:
        if (
            not isinstance(entry, Mapping)
            or not isinstance(entry.get("age_sessions"), int)
            or entry["age_sessions"] not in {0, 1, 2}
            or not isinstance(entry.get("observation"), Mapping)
        ):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid settlement observation batch")
        observations.append((entry["age_sessions"], dict(entry["observation"])))
    if len(observations) != 3 or {age for age, _observation in observations} != {0, 1, 2}:
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid settlement observation batch")
    return observations


def _semantic_bytes(observation: Mapping[str, object]) -> bytes:
    value = dict(observation)
    value.pop("observed_at", None)
    return _canonical(value)


def build_settlement_tracks(
    *, current_path: Path, prior_paths: Sequence[Path]
) -> dict[str, object]:
    """Return evidence-only tracks, failing closed on conflicting same-age probes."""
    grouped: dict[str, dict[int, dict[str, object]]] = {}
    ambiguous_cutoffs: set[str] = set()
    for path in (*prior_paths, current_path):
        for age, observation in _load_observation_batch(path):
            cutoff = observation.get("date_cutoff")
            if not isinstance(cutoff, str):
                raise TqqqCoreOnlyFreeOhlcvP1Error("invalid settlement observation batch")
            existing = grouped.setdefault(cutoff, {}).get(age)
            if existing is None:
                grouped[cutoff][age] = observation
            elif _semantic_bytes(existing) != _semantic_bytes(observation):
                ambiguous_cutoffs.add(cutoff)
            elif str(existing.get("observed_at")) < str(observation.get("observed_at")):
                grouped[cutoff][age] = observation
    tracks: dict[str, object] = {}
    for cutoff in sorted(grouped):
        if cutoff in ambiguous_cutoffs:
            tracks[cutoff] = {
                "schema_version": "qsl.tqqq-free-ohlcv-settlement-track.v1",
                "candidate": {
                    "candidate_id": P2_V9_CONTRACT.candidate_id,
                    "config_sha256": P2_V9_CONTRACT.config_sha256,
                },
                "date_cutoff": cutoff,
                "observations": {},
                "settlement_state": "AMBIGUOUS_REPEATED_PROBE",
                "revision_detected": True,
                "no_order": True,
                "automatic_promotion": False,
            }
            continue
        tracks[cutoff] = build_tqqq_core_only_free_ohlcv_settlement_track(
            grouped[cutoff], contract=P2_V9_CONTRACT
        )
    return {
        "schema_version": _TRACK_BATCH_SCHEMA,
        "candidate": {
            "candidate_id": P2_V9_CONTRACT.candidate_id,
            "config_sha256": P2_V9_CONTRACT.config_sha256,
        },
        "tracks": tracks,
        "ambiguous_cutoff_count": len(ambiguous_cutoffs),
        "no_order": True,
        "automatic_promotion": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--prior", action="append", type=Path, default=[])
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    result = build_settlement_tracks(current_path=args.current, prior_paths=args.prior)
    args.output.write_bytes(_canonical(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
