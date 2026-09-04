"""Synthetic-only P2 v2 adapter evidence (never promotion or market evidence)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from quant_platform_kit.common.models import PortfolioSnapshot
from quant_platform_kit.common.strategy_contracts import StrategyContext
from us_equity_strategies.entrypoints import build_tqqq_core_only_p2_v2_research_decision


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def run_synthetic_p2_v2_evidence(
    *, input_payload: Mapping[str, object], candidate: Mapping[str, object], output_dir: Path
) -> dict[str, object]:
    """Call the public adapter with in-memory synthetic bars and publish bound proof."""
    manifest = input_payload["input_manifest"]
    bars = input_payload["bars"]
    if not isinstance(manifest, Mapping) or not isinstance(bars, Mapping):
        raise ValueError("invalid synthetic input")
    producer = manifest.get("producer")
    if not isinstance(producer, Mapping) or producer.get("tool") != "synthetic_fixture":
        raise ValueError("synthetic_fixture provenance required")
    symbols = bars.get("symbols")
    if not isinstance(symbols, Mapping) or not isinstance(symbols.get("QQQ"), Mapping):
        raise ValueError("synthetic QQQ bars required")
    qqq_bars = symbols["QQQ"].get("bars")
    if not isinstance(qqq_bars, list) or not qqq_bars:
        raise ValueError("synthetic QQQ history required")
    history = [
        {"close": float(row["close"]), "high": float(row["high"]), "low": float(row["low"])}
        for row in qqq_bars
        if isinstance(row, Mapping)
    ]
    if len(history) < 252:
        raise ValueError("synthetic history is too short")
    runtime = candidate.get("runtime_config")
    if not isinstance(runtime, Mapping):
        raise ValueError("candidate runtime config required")
    as_of = datetime.now(timezone.utc)
    context = StrategyContext(
        as_of=as_of,
        portfolio=PortfolioSnapshot(as_of=as_of, total_equity=100_000.0, buying_power=100_000.0, cash_balance=100_000.0),
        market_data={"benchmark_history": history},
        runtime_config=dict(runtime),
    )
    decision = build_tqqq_core_only_p2_v2_research_decision(context)
    manifest_sha = hashlib.sha256(_canonical(manifest)).hexdigest()
    candidate_sha = hashlib.sha256(_canonical(candidate)).hexdigest()
    package = {
        "schema_version": "qsl.tqqq-p2-v2-synthetic-adapter-evidence.v1",
        "status": "SYNTHETIC_ONLY_VERIFIED",
        "authority_scope": "SYNTHETIC_CONTRACT_ONLY",
        "no_order": True,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_config_sha256": candidate_sha,
        "source": candidate.get("source"),
        "input_manifest_sha256": manifest_sha,
        "adapter": "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision",
        "decision": asdict(decision),
    }
    output_dir.mkdir(parents=True, exist_ok=False)
    artifact = output_dir / "synthetic-adapter-evidence.json"
    artifact.write_bytes(_canonical(package))
    return {"status": package["status"], "evidence_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest()}
