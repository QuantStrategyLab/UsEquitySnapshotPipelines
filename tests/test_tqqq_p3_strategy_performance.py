from __future__ import annotations

import hashlib

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_p3_strategy_performance as performance
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import P2_V5_CONTRACT


NOW = "2026-08-19T04:00:00Z"
PRODUCER_REVISION = "a" * 40
STRATEGY_REVISION = "b" * 40
MANIFEST = "c" * 64


def _evidence() -> dict[str, object]:
    return {
        "strategy": {
            "profile": P2_V5_CONTRACT.candidate_id,
            "domain": "us_equity",
            "source_revision": STRATEGY_REVISION,
        },
        "lifecycle_claims": {
            "learning_only": True,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
        },
        "input_provenance": {
            "manifest_sha256": MANIFEST,
            "range": {"end": "2026-08-18"},
        },
        "digests": {"config_sha256": P2_V5_CONTRACT.config_sha256},
        "metrics": {
            "sharpe_ratio": 1.2,
            "annualized_return": 0.25,
            "calmar_ratio": 1.5,
            "win_rate": 0.55,
            "max_drawdown": 0.12,
        },
    }


def _expected_digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        performance.canonical_evidence_package_v2_bytes(value)
    ).hexdigest()


def _build(monkeypatch: pytest.MonkeyPatch, **overrides: object) -> dict[str, object]:
    evidence = _evidence()
    evidence.update(overrides)
    monkeypatch.setattr(performance, "validate_evidence_package_v2", lambda *_args, **_kwargs: ())
    return performance.build_tqqq_p3_strategy_performance(
        evidence_package=evidence,
        expected_evidence_sha256=_expected_digest(evidence),
        producer_revision=PRODUCER_REVISION,
        computed_at=NOW,
    )


def test_builds_only_the_bounded_research_metrics_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _build(monkeypatch)

    assert payload == {
        "schema_version": "strategy_performance.v2",
        "metrics_kind": "performance",
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "strategy_profile": "tqqq_core_only_p2_v5",
        "candidate_kind": "individual",
        "domain": "us_equity",
        "generated_at": NOW,
        "as_of": "2026-08-18",
        "current_metrics": {
            "sharpe": 1.2,
            "cagr": 0.25,
            "calmar": 1.5,
            "win_rate": 0.55,
            "max_dd": 0.12,
        },
        "evidence": {
            "p1_input_digest": MANIFEST,
            "p2_config_digest": P2_V5_CONTRACT.config_sha256,
            "p3_evidence_id": _expected_digest(_evidence()),
            "strategy_revision": STRATEGY_REVISION,
            "producer_revision": PRODUCER_REVISION,
        },
        "lifecycle": {"stage": "P3", "status": "verified"},
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }
    encoded = performance.canonical_tqqq_p3_strategy_performance_bytes(payload).decode("utf-8")
    assert "bars" not in encoded
    assert '"orders"' not in encoded
    assert "path" not in encoded


def test_rejects_misbound_or_non_research_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _evidence()
    monkeypatch.setattr(performance, "validate_evidence_package_v2", lambda *_args, **_kwargs: ())

    with pytest.raises(performance.TqqqP3StrategyPerformanceError, match="P3 evidence digest mismatch"):
        performance.build_tqqq_p3_strategy_performance(
            evidence_package=evidence,
            expected_evidence_sha256="d" * 64,
            producer_revision=PRODUCER_REVISION,
            computed_at=NOW,
        )

    evidence["lifecycle_claims"] = {"learning_only": True}
    with pytest.raises(performance.TqqqP3StrategyPerformanceError, match="not research-only"):
        performance.build_tqqq_p3_strategy_performance(
            evidence_package=evidence,
            expected_evidence_sha256=_expected_digest(evidence),
            producer_revision=PRODUCER_REVISION,
            computed_at=NOW,
        )


def test_rejects_non_numeric_watcher_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = _evidence()
    metrics = dict(evidence["metrics"])
    metrics["sharpe_ratio"] = "not-a-number"
    evidence["metrics"] = metrics
    monkeypatch.setattr(performance, "validate_evidence_package_v2", lambda *_args, **_kwargs: ())

    with pytest.raises(performance.TqqqP3StrategyPerformanceError, match="invalid sharpe"):
        performance.build_tqqq_p3_strategy_performance(
            evidence_package=evidence,
            expected_evidence_sha256=_expected_digest(evidence),
            producer_revision=PRODUCER_REVISION,
            computed_at=NOW,
        )
