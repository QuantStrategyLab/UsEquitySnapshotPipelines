#!/usr/bin/env python3
"""Write TQQQ P3 evidence from one preserved immutable snapshot."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
    run_tqqq_promotion_evidence,
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--personal-attestation", type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--mandate-receipt-sha256", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid immutable snapshot") from exc


def _snapshot_payload(
    snapshot_root: Path, personal_attestation: Path | None
) -> dict[str, object]:
    payload = {
        "input_manifest": _read_json(snapshot_root / "input-manifest.json"),
        "bars": _read_json(snapshot_root / "bars.json"),
    }
    if personal_attestation is not None:
        payload["provenance"] = _read_json(personal_attestation)
    return payload


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        result = run_tqqq_promotion_evidence(
            input_payload=_snapshot_payload(args.snapshot_root, args.personal_attestation),
            config_payload=_read_json(args.config),
            mandate_receipt_sha256=args.mandate_receipt_sha256,
            output_dir=args.output_dir,
        )
    except (OSError, TypeError, ValueError, TqqqPromotionEvidenceError):
        print('{"status":"PARKED"}')
        return 2
    print(
        json.dumps(
            {
                "evidence_sha256": result["evidence_sha256"],
                "status": "EVIDENCE_V2_COMPLETE",
                "verdict": result["verdict"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
