from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .universe_audit_contracts import HardGateSpec, ScoreTermSpec, SelectionRuleSpec, WATCHLIST
from .universe_audit_engine import DEFAULT_ALLOWED_METRICS

AI_HARD_GATE_KEY_TO_METRIC = {
    "min_trading_days": "trading_days",
    "min_month_end_closes": "month_end_closes",
    "min_data_completeness": "data_completeness",
    "min_adv20_usd": "adv20_usd",
    "max_vol_63": "vol_63",
    "max_drawdown_126": "maxdd_126",
}
AI_HARD_GATE_OPERATORS = {
    "min_trading_days": ">=",
    "min_month_end_closes": ">=",
    "min_data_completeness": ">=",
    "min_adv20_usd": ">=",
    "max_vol_63": "<=",
    "max_drawdown_126": ">=",
}


def _load_payload(source: str | Path | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        return source
    path = Path(source)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("AI rule proposal must contain a JSON object")
    return payload


def _metric_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        raw = value
    else:
        raw = ()
    return tuple(dict.fromkeys(str(item or "").strip() for item in raw if str(item or "").strip()))


def build_rule_spec_from_ai_proposal(
    source: str | Path | Mapping[str, Any],
    *,
    rule_id: str | None = None,
    rule_version: str = "v1",
    universe_id: str,
    allowed_metrics: set[str] | frozenset[str] = DEFAULT_ALLOWED_METRICS,
) -> SelectionRuleSpec:
    payload = _load_payload(source)
    proposal_id = str(payload.get("proposal_id") or payload.get("id") or "").strip()
    resolved_rule_id = str(rule_id or payload.get("rule_id") or proposal_id or "").strip()
    if not resolved_rule_id:
        raise ValueError("AI proposal must include proposal_id or rule_id")

    raw_gates = payload.get("suggested_hard_gates") or payload.get("hard_gates") or {}
    if not isinstance(raw_gates, Mapping):
        raise ValueError("AI proposal hard gates must be a JSON object")
    hard_gates: list[HardGateSpec] = []
    for key, threshold in raw_gates.items():
        gate_key = str(key or "").strip()
        metric = AI_HARD_GATE_KEY_TO_METRIC.get(gate_key)
        if metric is None:
            raise ValueError(f"unknown AI hard gate {gate_key!r}")
        if metric not in allowed_metrics:
            raise ValueError(f"AI hard gate metric {metric!r} is not allowed")
        hard_gates.append(
            HardGateSpec(
                name=gate_key,
                metric=metric,
                operator=AI_HARD_GATE_OPERATORS[gate_key],
                threshold=threshold,
                failure_action=WATCHLIST,
                description="AI-proposed hard gate converted to deterministic rule spec.",
            )
        )

    suggested_metrics = _metric_list(payload.get("suggested_metrics") or payload.get("candidate_metrics"))
    unknown_metrics = tuple(metric for metric in suggested_metrics if metric not in allowed_metrics)
    if unknown_metrics:
        raise ValueError(f"AI proposal references unsupported metrics: {', '.join(unknown_metrics)}")
    score_terms = tuple(
        ScoreTermSpec(
            metric=metric,
            weight=1.0,
            higher_is_better=metric not in {"vol_63", "maxdd_126"},
            description="AI-proposed transparent score term.",
        )
        for metric in suggested_metrics
        if metric not in {gate.metric for gate in hard_gates}
    )

    return SelectionRuleSpec(
        rule_id=resolved_rule_id,
        rule_version=str(payload.get("rule_version") or rule_version),
        universe_id=universe_id,
        hard_gates=tuple(hard_gates),
        score_terms=score_terms,
        benchmark_symbols=tuple(_metric_list(payload.get("benchmark_symbols"))),
        notes=str(payload.get("hypothesis") or payload.get("notes") or "").strip(),
        ai_proposal_id=proposal_id or None,
    )
