from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_existing_soxl_snapshot_promotion.py"


def _module():
    spec = importlib.util.spec_from_file_location("run_existing_soxl_snapshot_promotion", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _argv() -> list[str]:
    return [
        "--snapshot",
        "/private/snapshot",
        "--snapshot-digest",
        "a" * 64,
        "--authority-receipt-sha256",
        "b" * 64,
        "--entitlement-receipt-sha256",
        "c" * 64,
        "--license-receipt-sha256",
        "d" * 64,
        "--retention-expires-at",
        "2026-12-31T00:00:00Z",
        "--risk-standard-id",
        "soxl_p3_candidate_bound_v1",
        "--risk-standard-sha256",
        "e" * 64,
        "--input-license",
        "authority-bound private internal research",
        "--input-usage-scope",
        "non-commercial internal research",
    ]


def test_snapshot_only_cli_emits_only_sanitized_terminal(monkeypatch, capsys) -> None:
    module = _module()
    monkeypatch.setattr(module, "_require_filevault", lambda: None)
    monkeypatch.setattr(module, "resolve_soxl_runtime_identity", lambda: ("f" * 40, "1" * 40))
    monkeypatch.setattr(
        module,
        "orchestrate_existing_soxl_snapshot",
        lambda *_args, **_kwargs: {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": 9,
            "snapshot_digest": "a" * 64,
            "evidence_digest": "2" * 64,
            "mandate_receipt_digest": "3" * 64,
            "rerun_count": 1,
        },
    )

    assert module.main(_argv()) == 0

    assert json.loads(capsys.readouterr().out) == {
        "asset_count": 9,
        "evidence_digest": "2" * 64,
        "mandate_receipt_digest": "3" * 64,
        "rerun_count": 1,
        "snapshot_digest": "a" * 64,
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
    }


def test_snapshot_only_cli_has_no_provider_or_broker_call_surface() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    for forbidden in (
        "ibapi",
        "connect(",
        "run_exact_acquisition",
        "reqaccount",
        "reqpositions",
        "reqopenorders",
        "placeorder",
        "cancelorder",
        "reqexecutions",
    ):
        assert forbidden not in source.lower()
