from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.strategy_contracts import StrategyContext
from us_equity_strategies.entrypoints import (
    build_tqqq_core_only_p2_benchmark_guard_research_decision,
)

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    P2_V9_CONTRACT,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    _Bar,
    _candidate_contract,
    _v9_market_regime_artifact,
    _validate_config,
)

_CONFIG_PATH = (
    Path(__file__).parents[1]
    / "config"
    / "tqqq_core_only_p2_v9_benchmark_drawdown_guard.json"
)


def _candidate() -> dict[str, object]:
    return json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))


def _history(last_close: float) -> tuple[_Bar, ...]:
    start = date(2025, 1, 2)
    rows = tuple(
        _Bar(
            start + timedelta(days=index),
            100.0 + index * 0.1,
            100.1 + index * 0.1,
            99.9 + index * 0.1,
            100.0 + index * 0.1,
            1_000.0,
        )
        for index in range(260)
    )
    return (*rows[:-1], _Bar(rows[-1].session, last_close, last_close + 0.1, last_close - 0.1, last_close, 1_000.0))


def _decision(candidate: dict[str, object], bars: tuple[_Bar, ...]):
    artifact = _v9_market_regime_artifact(candidate, bars, bars[-1].session)
    assert artifact is not None
    context = StrategyContext(
        as_of=datetime.combine(
            bars[-1].session, time(16, 0), tzinfo=ZoneInfo("America/New_York")
        ),
        portfolio=PortfolioSnapshot(
            as_of=datetime.now(UTC),
            total_equity=100_000.0,
            buying_power=100_000.0,
            cash_balance=100_000.0,
            positions=(Position(symbol="TQQQ", quantity=100.0, market_value=45_000.0),),
            metadata={"market_regime_control": artifact},
        ),
        market_data={
            "benchmark_history": tuple(
                {
                    "date": row.session.isoformat(),
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                }
                for row in bars
            )
        },
        runtime_config=candidate["runtime_config"],
    )
    return artifact, build_tqqq_core_only_p2_benchmark_guard_research_decision(context)


def test_v9_binds_the_frozen_config_and_research_only_entrypoint() -> None:
    candidate = _candidate()

    assert _candidate_contract(candidate) == P2_V9_CONTRACT
    assert _validate_config(candidate)["candidate"]["candidate_id"] == P2_V9_CONTRACT.candidate_id

    artifact, decision = _decision(candidate, _history(119.0))
    assert artifact == _v9_market_regime_artifact(candidate, _history(119.0), _history(119.0)[-1].session)
    assert artifact["canonical_route"] == "risk_reduced"
    assert "generated_at" not in artifact
    assert artifact["execution_controls"]["broker_order_allowed"] is False
    assert artifact["execution_controls"]["consumption_evidence_status"] == "research_backtest_approved"
    assert decision.diagnostics["market_regime_control_route"] == "risk_reduced"
    weights = {
        position.symbol: position.target_value / 100_000.0
        for position in decision.positions
        if position.target_value
    }
    assert weights.get("TQQQ", 0.0) <= 0.1125
    assert weights.get("TQQQ", 0.0) + weights.get("QQQM", 0.0) <= 0.45
    assert weights["BOXX"] >= 0.53


def test_v9_hard_drawdown_routes_an_existing_risk_sleeve_to_boxx() -> None:
    artifact, decision = _decision(_candidate(), _history(110.0))

    assert artifact["canonical_route"] == "risk_off"
    weights = {
        position.symbol: position.target_value / 100_000.0
        for position in decision.positions
        if position.target_value
    }
    assert weights == {"BOXX": 0.98}
