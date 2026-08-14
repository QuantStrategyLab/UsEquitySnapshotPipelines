from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from quant_platform_kit.data.research_input import canonical_research_input_manifest_bytes
from quant_platform_kit.strategy_lifecycle import validate_evidence_package_v2

from us_equity_snapshot_pipelines.lifecycle import tqqq_p3_direct as direct

_SHA = "a" * 64
_REV = "b" * 40


def _authority() -> dict[str, str]:
    return {
        "authority_receipt_sha256": _SHA,
        "entitlement_receipt_sha256": "c" * 64,
        "license_receipt_sha256": "d" * 64,
        "retention_expires_at": "2030-01-01T00:00:00Z",
        "risk_standard_id": "qpk.strategy_promotion_risk_standard.zh-CN.v2",
        "risk_standard_sha256": "e" * 64,
        "platform_execution_revision": _REV,
        "input_license": direct.INPUT_LICENSE,
        "input_usage_scope": direct.INPUT_USAGE_SCOPE,
    }


def _snapshot(
    root: Path, *, observations: dict[str, bool] | None = None, declining: bool = False
) -> Path:
    root.mkdir(parents=True)
    sessions = []
    current = date(2018, 1, 2)
    end = date(2025, 7, 2)
    while current <= end:
        if current.weekday() < 5:
            offset = len(sessions)
            level = 100.0 * (0.999**offset) if declining else 100.0 + offset / 100
            sessions.append(
                {
                    "date": current.isoformat(),
                    "open": level,
                    "high": level + 1.0,
                    "low": level - 1.0,
                    "close": level + 0.5,
                    "volume": 1_000_000.0,
                }
            )
        current += timedelta(days=1)
    bars = {
        "schema_version": "tqqq_etf_only_private_bars.v1",
        "symbols": {symbol: sessions for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")},
        "observations": observations or {},
    }
    raw = json.dumps(bars, sort_keys=True, separators=(",", ":")).encode()
    (root / "bars.json").write_bytes(raw)
    manifest = {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": "synthetic-tqqq-direct-p3",
        "research_input_contract_id": "tqqq_etf_only_ibkr_adjusted_last.v1",
        "domain": "us_equity",
        "profile": "tqqq_core_parity_v1",
        "artifact_type": "immutable_adjusted_ohlcv_etf_only",
        "observed_at": "2025-07-02T20:00:00Z",
        "effective_at": "2025-07-02T20:00:00Z",
        "as_of": "2025-07-02T20:00:00Z",
        "producer": {"repository": "QuantStrategyLab/UsEquitySnapshotPipelines", "commit_sha": _REV, "tree_sha": _REV, "tool": "synthetic", "tool_version": "v1"},
        "calendar": {"calendar_id": "XNYS", "timezone": "America/New_York", "session_date": "2025-07-02", "source": "exchange_calendars", "source_revision": "synthetic"},
        "adjustment": {"policy": "total_return_adjusted", "source": "IBKR_ADJUSTED_LAST", "source_revision": "synthetic"},
        "sources": [{"source_id": "ibkr:QQQ", "revision": "synthetic", "observed_at": "2025-07-02T20:00:00Z", "content_sha256": hashlib.sha256(raw).hexdigest()}],
        "members": [{"path": "bars.json", "media_type": "application/json", "size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}],
    }
    (root / "manifest.json").write_bytes(canonical_research_input_manifest_bytes(manifest))
    return root


def _approve(*_args: object, **_kwargs: object) -> object:
    return SimpleNamespace(
        assessment=SimpleNamespace(outcome="APPROVE", execution_authorized=False, reason_codes=()),
        decision=SimpleNamespace(positions=(SimpleNamespace(symbol="BOXX", target_weight=0.5),)),
    )


def test_structural_failure_happens_before_orchestrator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(tmp_path / "snapshot")
    (snapshot / "bars.json").write_text("{}")
    monkeypatch.setattr(direct, "BacktestOrchestrator", lambda **_kwargs: pytest.fail("orchestrator called"))

    with pytest.raises(direct.TqqqP3ContractError):
        direct.run_tqqq_p3(snapshot, _authority(), tmp_path / "out")


def test_freezes_plan_before_orchestrator_and_writes_valid_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(tmp_path / "snapshot")
    monkeypatch.setattr(direct, "evaluate_tqqq_growth_income_promotion_research", _approve)
    captured: list[object] = []
    original = direct.BacktestOrchestrator

    class CapturingOrchestrator(original):
        def __init__(self, **kwargs: object) -> None:
            assert direct._FROZEN_PLAN is not None
            captured.append(direct._FROZEN_PLAN)
            super().__init__(**kwargs)

    monkeypatch.setattr(direct, "BacktestOrchestrator", CapturingOrchestrator)
    root = direct.run_tqqq_p3(snapshot, _authority(), tmp_path / "out")

    evidence = json.loads((root / "strategy-evidence-package.v2.json").read_text())
    assert captured
    assert evidence["backtest"]["orchestrator"] == "BacktestOrchestrator"
    assert evidence["cost_stress"]["scenarios"] == [
        {"multiplier": 1, "total_cost_bps": 5.0},
        {"multiplier": 2, "total_cost_bps": 10.0},
        {"multiplier": 3, "total_cost_bps": 15.0},
    ]
    assert evidence["backtest"]["promotion_run"]["locked_oos_start"] == "2024-07-01"
    assert validate_evidence_package_v2(evidence, base_dir=root) == ()


def test_each_decision_calls_risk_once_and_has_no_order_surface(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(tmp_path / "snapshot")
    calls = 0

    def evaluate(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return _approve(*args, **kwargs)

    monkeypatch.setattr(direct, "evaluate_tqqq_growth_income_promotion_research", evaluate)
    root = direct.run_tqqq_p3(snapshot, _authority(), tmp_path / "out")
    risk = json.loads((root / "risk.json").read_text())

    assert risk["risk_assessment_count"] == risk["decision_count"] == calls
    assert risk["order_calls"] == 0
    assert "place_order" not in Path(direct.__file__).read_text()


def test_right_censor_drift_risk_rejection_and_negative_return_produce_verdict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(tmp_path / "snapshot", observations={"right_censored_cooldown": True, "observed_drift": True})
    monkeypatch.setattr(
        direct,
        "evaluate_tqqq_growth_income_promotion_research",
        lambda *_args, **_kwargs: SimpleNamespace(
            assessment=SimpleNamespace(outcome="REJECT", execution_authorized=False, reason_codes=("synthetic",)),
            decision=SimpleNamespace(positions=()),
        ),
    )
    root = direct.run_tqqq_p3(snapshot, _authority(), tmp_path / "out")
    risk = json.loads((root / "risk.json").read_text())

    assert set(risk["verdicts"]) >= {"INCONCLUSIVE_OBSERVED_DRIFT", "INCONCLUSIVE_RIGHT_CENSORED_COOLDOWN", "INCONCLUSIVE_RISK_REJECTION"}


def test_negative_return_and_drawdown_are_evidence_verdicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot", declining=True)
    monkeypatch.setattr(direct, "evaluate_tqqq_growth_income_promotion_research", _approve)

    root = direct.run_tqqq_p3(snapshot, _authority(), tmp_path / "out")
    risk = json.loads((root / "risk.json").read_text())

    assert "REJECT_NEGATIVE_RETURN" in risk["verdicts"]
    assert "INCONCLUSIVE_DRAWDOWN_PARK" in risk["verdicts"]
