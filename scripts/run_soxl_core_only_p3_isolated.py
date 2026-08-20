"""Execute the frozen SOXL core-only P2 v3 candidate in its exact UES runtime.

This is a P3 execution primitive, not a P3 evidence verifier.  It accepts an
already materialized research context, validates a local UES checkout against
the revision and lockfile frozen by P2, then runs the public strategy adapter
inside that checkout's locked environment.  It has no provider, storage,
workflow, credential, risk-engine, sizing, or order integration.

The later P3 verifier must validate the immutable P1 manifest and the complete
P2 candidate before it invokes this executable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

P2_UES_REVISION = "7756fe32585e85cf1d09a163203a02e3eee39fe1"
P2_QPK_REVISION = "3acab1923a97b805b077c85c6c19657be0143bac"
P2_UES_UV_LOCK_SHA256 = "6c12df9b3412681829295f15de7e2ce7fc5b708d1de815f72d654fc16b7848e6"
P2_CANDIDATE_ID = "soxl_soxx_core_only_p2_v3"
P2_CONFIG_SHA256 = "ff8fa0acf4f175a7c40c3e1e6a3304ea2748b6b81c3797342085a4df3810ab4d"
INPUT_SCHEMA = "qsl.soxl-core-only-p3-strategy-context.v1"
DECISION_SCHEMA = "qsl.soxl-core-only-p3-decision.v1"
ISOLATED_RESULT_SCHEMA = "qsl.soxl-core-only-p3-isolated-result.v1"
BATCH_INPUT_SCHEMA = "qsl.soxl-core-only-p3-strategy-context-batch.v1"
BATCH_DECISION_SCHEMA = "qsl.soxl-core-only-p3-decision-batch.v1"
ISOLATED_BATCH_RESULT_SCHEMA = "qsl.soxl-core-only-p3-isolated-batch-result.v1"
STATEFUL_REPLAY_INPUT_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-input.v1"
STATEFUL_REPLAY_RESULT_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-result.v1"
ISOLATED_REPLAY_RESULT_SCHEMA = "qsl.soxl-core-only-p3-isolated-replay-result.v1"
MAX_BATCH_CONTEXTS = 1024
ENTRYPOINT = (
    "us_equity_strategies.entrypoints."
    "build_soxl_soxx_core_only_p2_v2_research_decision"
)
_SYMBOLS = ("SOXL", "SOXX", "BOXX")
_INDICATOR_FIELDS = {
    "SOXL": frozenset({"price", "ma_trend"}),
    "SOXX": frozenset(
        {
            "price",
            "ma_trend",
            "ma20",
            "ma20_slope",
            "rsi14",
            "bb_upper",
            "realized_volatility_10",
            "realized_volatility_10_dynamic_threshold",
            "realized_volatility_10_dynamic_sample_count",
        }
    ),
}
_DIAGNOSTIC_FIELDS = (
    "blend_tier",
    "base_blend_tier",
    "active_risk_asset",
    "blend_gate_volatility_delever_triggered",
    "blend_gate_volatility_delever_redirect_symbol",
    "market_regime_control_enabled",
    "market_regime_control_applied",
)


class SoxlCoreOnlyP3IsolatedRunnerError(ValueError):
    """Fail-closed error with no raw source material."""


def _fail() -> None:
    raise SoxlCoreOnlyP3IsolatedRunnerError("invalid SOXL core-only isolated P3 input")


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail()
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0) or (nonnegative and result < 0.0):
        _fail()
    return 0.0 if result == 0.0 else result


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        _fail()
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        _fail()
    if result.tzinfo is None or result.utcoffset() is None:
        _fail()
    return result


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _finite(value)
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in _mapping(value).items()}
    _fail()


def _validate_positions(value: object) -> None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail()
    expected = {"symbol", "quantity", "market_value", "average_cost", "currency", "account_id"}
    seen: set[str] = set()
    for raw in value:
        row = _mapping(raw)
        if set(row) != expected:
            _fail()
        symbol = row["symbol"]
        if not isinstance(symbol, str) or symbol not in _SYMBOLS or symbol in seen:
            _fail()
        seen.add(symbol)
        _finite(row["quantity"])
        _finite(row["market_value"], nonnegative=True)
        if row["average_cost"] is not None:
            _finite(row["average_cost"], positive=True)
        if row["currency"] != "USD" or row["account_id"] is not None:
            _fail()


def _validate_portfolio(value: object, as_of: datetime) -> dict[str, object]:
    portfolio = _mapping(value)
    expected = {"as_of", "total_equity", "buying_power", "cash_balance", "positions", "metadata"}
    if set(portfolio) != expected or _timestamp(portfolio["as_of"]) != as_of:
        _fail()
    _finite(portfolio["total_equity"], positive=True)
    if portfolio["buying_power"] is not None:
        _finite(portfolio["buying_power"], nonnegative=True)
    if portfolio["cash_balance"] is not None:
        _finite(portfolio["cash_balance"], nonnegative=True)
    _validate_positions(portfolio["positions"])
    metadata = _mapping(portfolio["metadata"])
    if set(metadata) - {"observed_effective_exposure", "sellable_quantities"}:
        _fail()
    if "observed_effective_exposure" in metadata:
        _finite(metadata["observed_effective_exposure"], nonnegative=True)
    if "sellable_quantities" in metadata:
        quantities = _mapping(metadata["sellable_quantities"])
        position_symbols = {_mapping(row)["symbol"] for row in portfolio["positions"]}
        if set(quantities) != position_symbols:
            _fail()
        for quantity in quantities.values():
            _finite(quantity, nonnegative=True)
    return portfolio


def _validate_market_data(value: object) -> dict[str, object]:
    market_data = _mapping(value)
    if set(market_data) != {"derived_indicators"}:
        _fail()
    indicators = _mapping(market_data["derived_indicators"])
    if set(indicators) != {"SOXL", "SOXX"}:
        _fail()
    for symbol, required in _INDICATOR_FIELDS.items():
        row = _mapping(indicators[symbol])
        if set(row) != required:
            _fail()
        parsed = {field: _finite(row[field]) for field in required}
        if parsed["price"] <= 0.0 or parsed["ma_trend"] <= 0.0:
            _fail()
        if symbol == "SOXX" and (
            parsed["ma20"] <= 0.0
            or parsed["bb_upper"] <= 0.0
            or not 0.0 <= parsed["rsi14"] <= 100.0
            or parsed["realized_volatility_10"] < 0.0
            or parsed["realized_volatility_10_dynamic_threshold"] <= 0.0
            or parsed["realized_volatility_10_dynamic_sample_count"] <= 0.0
        ):
            _fail()
    return market_data


def validate_source_context(value: object) -> dict[str, object]:
    """Validate the JSON-only source input, without importing UES."""
    payload = _mapping(value)
    expected = {"schema_version", "as_of", "portfolio", "market_data"}
    if set(payload) != expected or payload["schema_version"] != INPUT_SCHEMA:
        _fail()
    as_of = _timestamp(payload["as_of"])
    _validate_portfolio(payload["portfolio"], as_of)
    _validate_market_data(payload["market_data"])
    return payload


def validate_p2_candidate(value: object) -> dict[str, object]:
    """Bind the full P2 file before it supplies the strategy runtime config."""
    candidate = _mapping(value)
    source = _mapping(candidate.get("source"))
    if (
        _sha256(candidate) != P2_CONFIG_SHA256
        or candidate.get("candidate_id") != P2_CANDIDATE_ID
        or source.get("repository") != "QuantStrategyLab/UsEquityStrategies"
        or source.get("revision") != P2_UES_REVISION
        or source.get("entrypoint") != ENTRYPOINT
        or _mapping(source.get("dependency_lock")).get("quant_platform_kit_revision") != P2_QPK_REVISION
    ):
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL P2 identity mismatch")
    runtime_config = _mapping(_json_value(candidate.get("runtime_config")))
    return {
        "candidate_id": P2_CANDIDATE_ID,
        "config_sha256": P2_CONFIG_SHA256,
        "runtime_config": runtime_config,
    }


def _source_decision(value: object, candidate: object) -> dict[str, object]:
    """Run only when this script is already in the P2-pinned UES environment."""
    payload = validate_source_context(value)
    p2 = validate_p2_candidate(candidate)
    try:
        from quant_platform_kit.common.models import PortfolioSnapshot, Position
        from quant_platform_kit.strategy_contracts import StrategyContext
        from us_equity_strategies.entrypoints import (
            build_soxl_soxx_core_only_p2_v2_research_decision,
        )
    except ImportError as exc:  # pragma: no cover - protected by outer identity gate
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc

    as_of = _timestamp(payload["as_of"])
    portfolio_payload = _mapping(payload["portfolio"])
    positions = tuple(
        Position(
            symbol=str(row["symbol"]),
            quantity=_finite(row["quantity"]),
            market_value=_finite(row["market_value"], nonnegative=True),
            average_cost=(
                None if row["average_cost"] is None else _finite(row["average_cost"], positive=True)
            ),
            currency="USD",
            account_id=None,
        )
        for row in portfolio_payload["positions"]
    )
    decision = build_soxl_soxx_core_only_p2_v2_research_decision(
        StrategyContext(
            as_of=as_of,
            portfolio=PortfolioSnapshot(
                as_of=as_of,
                total_equity=_finite(portfolio_payload["total_equity"], positive=True),
                buying_power=(
                    None
                    if portfolio_payload["buying_power"] is None
                    else _finite(portfolio_payload["buying_power"], nonnegative=True)
                ),
                cash_balance=(
                    None
                    if portfolio_payload["cash_balance"] is None
                    else _finite(portfolio_payload["cash_balance"], nonnegative=True)
                ),
                positions=positions,
                metadata=dict(_mapping(portfolio_payload["metadata"])),
            ),
            market_data=_mapping(payload["market_data"]),
            runtime_config=_mapping(p2["runtime_config"]),
        )
    )
    return _summarize_source_decision(decision, as_of=as_of)


def _summarize_source_decision(decision: object, *, as_of: datetime) -> dict[str, object]:
    positions = getattr(decision, "positions", ())
    targets = {
        position.symbol: _finite(position.target_value, nonnegative=True)
        for position in positions
        if position.target_value is not None
    }
    if (
        not set(_SYMBOLS).issubset(targets)
        or any(target != 0.0 for symbol, target in targets.items() if symbol not in _SYMBOLS)
    ):
        _fail()
    diagnostics = _mapping(getattr(decision, "diagnostics", None))
    summary = {field: diagnostics.get(field) for field in _DIAGNOSTIC_FIELDS}
    if (
        summary["blend_tier"] not in {"full", "mid", "defensive"}
        or summary["base_blend_tier"] not in {"full", "mid", "defensive"}
        or summary["active_risk_asset"] not in {"SOXL", "SOXX", "SOXX+SOXL"}
        or not isinstance(summary["blend_gate_volatility_delever_triggered"], bool)
        or summary["blend_gate_volatility_delever_redirect_symbol"] not in {"SOXX", None}
        or summary["market_regime_control_enabled"] is not False
        or summary["market_regime_control_applied"] is not False
    ):
        _fail()
    result: dict[str, object] = {
        "schema_version": DECISION_SCHEMA,
        "entrypoint": ENTRYPOINT,
        "as_of": as_of.isoformat(),
        "target_values": {symbol: targets[symbol] for symbol in _SYMBOLS},
        "diagnostics": summary,
    }
    result["output_sha256"] = _sha256(result)
    return result


def _source_decision_batch(value: object, candidate: object) -> dict[str, object]:
    """Replay an ordered bounded context batch in one pinned UES process."""
    payload = _mapping(value)
    if set(payload) != {"schema_version", "contexts"} or payload["schema_version"] != BATCH_INPUT_SCHEMA:
        _fail()
    contexts = payload["contexts"]
    if not isinstance(contexts, list) or not contexts or len(contexts) > MAX_BATCH_CONTEXTS:
        _fail()
    decisions = [_source_decision(context, candidate) for context in contexts]
    as_of = [str(decision["as_of"]) for decision in decisions]
    if as_of != sorted(as_of) or len(as_of) != len(set(as_of)):
        _fail()
    result: dict[str, object] = {
        "schema_version": BATCH_DECISION_SCHEMA,
        "entrypoint": ENTRYPOINT,
        "count": len(decisions),
        "decisions": decisions,
    }
    result["output_sha256"] = _sha256(result)
    return result


def _validate_replay_input(value: object) -> dict[str, object]:
    payload = _mapping(value)
    if set(payload) != {"schema_version", "initial_equity", "cost_bps", "sessions"}:
        _fail()
    if payload["schema_version"] != STATEFUL_REPLAY_INPUT_SCHEMA:
        _fail()
    initial_equity = _finite(payload["initial_equity"], positive=True)
    cost_bps = _finite(payload["cost_bps"], nonnegative=True)
    if cost_bps not in {5.0, 10.0, 15.0}:
        _fail()
    sessions = payload["sessions"]
    if not isinstance(sessions, list) or len(sessions) < 2 or len(sessions) > MAX_BATCH_CONTEXTS:
        _fail()
    previous: datetime | None = None
    for raw in sessions:
        session = _mapping(raw)
        if set(session) != {"as_of", "market_data", "prices"}:
            _fail()
        as_of = _timestamp(session["as_of"])
        if previous is not None and as_of <= previous:
            _fail()
        previous = as_of
        _validate_market_data(session["market_data"])
        prices = _mapping(session["prices"])
        if set(prices) != set(_SYMBOLS):
            _fail()
        for price in prices.values():
            _finite(price, positive=True)
    return {
        "initial_equity": initial_equity,
        "cost_bps": cost_bps,
        "sessions": sessions,
    }


def _source_stateful_replay(value: object, candidate: object) -> dict[str, object]:
    """Replay next-session target changes once inside the pinned UES process."""
    replay = _validate_replay_input(value)
    p2 = validate_p2_candidate(candidate)
    try:
        from quant_platform_kit.common.models import PortfolioSnapshot, Position
        from quant_platform_kit.strategy_contracts import StrategyContext
        from us_equity_strategies.entrypoints import (
            build_soxl_soxx_core_only_p2_v2_research_decision,
        )
    except ImportError as exc:  # pragma: no cover - protected by outer identity gate
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc

    cash = float(replay["initial_equity"])
    quantities = {symbol: 0.0 for symbol in _SYMBOLS}
    pending_weights: dict[str, float] | None = None
    one_way_turnover = 0.0
    cost_total = 0.0
    decisions: list[dict[str, object]] = []
    sessions = replay["sessions"]
    assert isinstance(sessions, list)
    for index, raw_session in enumerate(sessions):
        session = _mapping(raw_session)
        as_of = _timestamp(session["as_of"])
        prices = {symbol: _finite(_mapping(session["prices"])[symbol], positive=True) for symbol in _SYMBOLS}
        market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in _SYMBOLS}
        equity_before_trade = cash + sum(market_values.values())
        if equity_before_trade <= 0.0:
            raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL replay equity invalid")
        executed_turnover = 0.0
        executed_cost = 0.0
        if pending_weights is not None:
            current_weights = {symbol: market_values[symbol] / equity_before_trade for symbol in _SYMBOLS}
            executed_turnover = 0.5 * sum(
                abs(pending_weights[symbol] - current_weights[symbol]) for symbol in _SYMBOLS
            )
            executed_cost = equity_before_trade * executed_turnover * float(replay["cost_bps"]) / 10_000.0
            equity_after_trade = equity_before_trade - executed_cost
            if equity_after_trade <= 0.0:
                raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL replay equity invalid")
            for symbol in _SYMBOLS:
                quantities[symbol] = pending_weights[symbol] * equity_after_trade / prices[symbol]
            cash = 0.0
            market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in _SYMBOLS}
            one_way_turnover += executed_turnover
            cost_total += executed_cost
        equity = cash + sum(market_values.values())
        portfolio = PortfolioSnapshot(
            as_of=as_of,
            total_equity=equity,
            buying_power=equity,
            cash_balance=cash,
            positions=tuple(
                Position(
                    symbol=symbol,
                    quantity=quantities[symbol],
                    market_value=market_values[symbol],
                    currency="USD",
                )
                for symbol in _SYMBOLS
                if quantities[symbol] != 0.0
            ),
            metadata={"observed_effective_exposure": 0.0},
        )
        decision = build_soxl_soxx_core_only_p2_v2_research_decision(
            StrategyContext(
                as_of=as_of,
                portfolio=portfolio,
                market_data=_mapping(session["market_data"]),
                runtime_config=_mapping(p2["runtime_config"]),
            )
        )
        summary = _summarize_source_decision(decision, as_of=as_of)
        target_values = _mapping(summary["target_values"])
        total_target_value = sum(_finite(target_values[symbol], nonnegative=True) for symbol in _SYMBOLS)
        if total_target_value <= 0.0:
            _fail()
        pending_weights = {
            symbol: _finite(target_values[symbol], nonnegative=True) / total_target_value for symbol in _SYMBOLS
        }
        if not math.isclose(sum(pending_weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-12):
            _fail()
        decisions.append(
            {
                "signal_as_of": as_of.isoformat(),
                "effective_as_of": (
                    _timestamp(_mapping(sessions[index + 1])["as_of"]).isoformat()
                    if index + 1 < len(sessions)
                    else None
                ),
                "equity_before_signal": equity,
                "executed_one_way_turnover": executed_turnover,
                "executed_cost": executed_cost,
                "decision": summary,
                "pending_target_weights": pending_weights,
            }
        )
    result: dict[str, object] = {
        "schema_version": STATEFUL_REPLAY_RESULT_SCHEMA,
        "entrypoint": ENTRYPOINT,
        "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        "cost_bps": replay["cost_bps"],
        "initial_equity": replay["initial_equity"],
        "final_equity": cash + sum(quantities[symbol] * prices[symbol] for symbol in _SYMBOLS),
        "executed_signal_count": len(sessions) - 1,
        "unexecuted_final_signal": True,
        "one_way_turnover": one_way_turnover,
        "cost_total": cost_total,
        "decisions": decisions,
    }
    result["output_sha256"] = _sha256(result)
    return result


def _file_sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc


def validate_ues_project(path: Path) -> dict[str, str]:
    """Require a clean local checkout of the exact P2 source and lockfile."""
    if not path.is_dir() or not (path / "pyproject.toml").is_file():
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable")
    try:
        revision = subprocess.run(
            ("git", "-C", str(path), "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        status = subprocess.run(
            ("git", "-C", str(path), "status", "--porcelain"),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc
    if revision != P2_UES_REVISION or status or _file_sha256(path / "uv.lock") != P2_UES_UV_LOCK_SHA256:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime identity mismatch")
    return {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": P2_UES_REVISION,
        "quant_platform_kit_revision": P2_QPK_REVISION,
        "uv_lock_sha256": P2_UES_UV_LOCK_SHA256,
    }


def run_isolated_source(
    *,
    ues_project: Path,
    input_path: Path,
    p2_candidate_path: Path,
) -> dict[str, object]:
    """Run one verified local source checkout without using a mutable runtime."""
    identity = validate_ues_project(ues_project)
    if not input_path.is_file() or not p2_candidate_path.is_file() or shutil.which("uv") is None:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable")
    p2 = validate_p2_candidate(_read_json(p2_candidate_path))
    command = (
        "uv",
        "run",
        "--locked",
        "--project",
        str(ues_project),
        "python",
        str(Path(__file__).resolve()),
        "--source-context",
        str(input_path.resolve()),
        "--p2-candidate",
        str(p2_candidate_path.resolve()),
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc
    if completed.returncode != 0:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    try:
        decision = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked") from exc
    decision_mapping = _mapping(decision)
    if decision_mapping.get("schema_version") != DECISION_SCHEMA:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    claimed_digest = decision_mapping.pop("output_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(decision_mapping):
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    result: dict[str, object] = {
        "schema_version": ISOLATED_RESULT_SCHEMA,
        "status": "SUCCESS",
        "execution_identity": identity,
        "p2_identity": {
            "candidate_id": p2["candidate_id"],
            "config_sha256": p2["config_sha256"],
        },
        "decision": decision,
    }
    result["result_sha256"] = _sha256(result)
    return result


def run_isolated_batch(
    *,
    ues_project: Path,
    input_path: Path,
    p2_candidate_path: Path,
) -> dict[str, object]:
    """Run an ordered replay batch once in the verified source environment."""
    identity = validate_ues_project(ues_project)
    if not input_path.is_file() or not p2_candidate_path.is_file() or shutil.which("uv") is None:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable")
    p2 = validate_p2_candidate(_read_json(p2_candidate_path))
    command = (
        "uv",
        "run",
        "--locked",
        "--project",
        str(ues_project),
        "python",
        str(Path(__file__).resolve()),
        "--source-batch",
        str(input_path.resolve()),
        "--p2-candidate",
        str(p2_candidate_path.resolve()),
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc
    if completed.returncode != 0:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    try:
        decision_batch = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked") from exc
    decision_mapping = _mapping(decision_batch)
    if decision_mapping.get("schema_version") != BATCH_DECISION_SCHEMA:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    claimed_digest = decision_mapping.pop("output_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(decision_mapping):
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    result: dict[str, object] = {
        "schema_version": ISOLATED_BATCH_RESULT_SCHEMA,
        "status": "SUCCESS",
        "execution_identity": identity,
        "p2_identity": {
            "candidate_id": p2["candidate_id"],
            "config_sha256": p2["config_sha256"],
        },
        "decision_batch": decision_batch,
    }
    result["result_sha256"] = _sha256(result)
    return result


def run_isolated_replay(
    *,
    ues_project: Path,
    input_path: Path,
    p2_candidate_path: Path,
) -> dict[str, object]:
    """Run one stateful replay in the verified source environment."""
    identity = validate_ues_project(ues_project)
    if not input_path.is_file() or not p2_candidate_path.is_file() or shutil.which("uv") is None:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable")
    p2 = validate_p2_candidate(_read_json(p2_candidate_path))
    command = (
        "uv",
        "run",
        "--locked",
        "--project",
        str(ues_project),
        "python",
        str(Path(__file__).resolve()),
        "--source-replay",
        str(input_path.resolve()),
        "--p2-candidate",
        str(p2_candidate_path.resolve()),
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable") from exc
    if completed.returncode != 0:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    try:
        replay_result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked") from exc
    replay_mapping = _mapping(replay_result)
    if replay_mapping.get("schema_version") != STATEFUL_REPLAY_RESULT_SCHEMA:
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    claimed_digest = replay_mapping.pop("output_sha256", None)
    if not isinstance(claimed_digest, str) or claimed_digest != _sha256(replay_mapping):
        raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL source execution parked")
    result: dict[str, object] = {
        "schema_version": ISOLATED_REPLAY_RESULT_SCHEMA,
        "status": "SUCCESS",
        "execution_identity": identity,
        "p2_identity": {
            "candidate_id": p2["candidate_id"],
            "config_sha256": p2["config_sha256"],
        },
        "replay": replay_result,
    }
    result["result_sha256"] = _sha256(result)
    return result


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SoxlCoreOnlyP3IsolatedRunnerError("invalid SOXL core-only isolated P3 input") from exc


def _parked(failure_class: str) -> dict[str, str]:
    return {
        "schema_version": ISOLATED_RESULT_SCHEMA,
        "status": "PARKED",
        "failure_class": failure_class,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--source-context", help="inner source-only JSON context path")
    group.add_argument("--source-batch", help="inner ordered source-context batch path")
    group.add_argument("--source-replay", help="inner stateful replay input path")
    group.add_argument("--input", help="outer single JSON context path")
    group.add_argument("--batch-input", help="outer ordered JSON context batch path")
    group.add_argument("--replay-input", help="outer stateful replay input path")
    parser.add_argument("--p2-candidate", required=True, help="exact frozen P2 candidate JSON path")
    parser.add_argument("--ues-project", help="clean local checkout at the P2-pinned UES revision")
    args = parser.parse_args(argv)
    try:
        if args.source_context:
            result = _source_decision(
                _read_json(Path(args.source_context)),
                _read_json(Path(args.p2_candidate)),
            )
        elif args.source_batch:
            result = _source_decision_batch(
                _read_json(Path(args.source_batch)),
                _read_json(Path(args.p2_candidate)),
            )
        elif args.source_replay:
            result = _source_stateful_replay(
                _read_json(Path(args.source_replay)),
                _read_json(Path(args.p2_candidate)),
            )
        elif not args.ues_project:
            raise SoxlCoreOnlyP3IsolatedRunnerError("isolated SOXL runtime unavailable")
        elif args.batch_input:
            result = run_isolated_batch(
                ues_project=Path(args.ues_project),
                input_path=Path(args.batch_input),
                p2_candidate_path=Path(args.p2_candidate),
            )
        elif args.replay_input:
            result = run_isolated_replay(
                ues_project=Path(args.ues_project),
                input_path=Path(args.replay_input),
                p2_candidate_path=Path(args.p2_candidate),
            )
        else:
            result = run_isolated_source(
                ues_project=Path(args.ues_project),
                input_path=Path(args.input),
                p2_candidate_path=Path(args.p2_candidate),
            )
    except SoxlCoreOnlyP3IsolatedRunnerError:
        result = _parked("isolated_runtime_or_context_invalid")
    except Exception:  # noqa: BLE001 - defensive boundary for changed source code
        result = _parked("isolated_source_internal_failure")
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
