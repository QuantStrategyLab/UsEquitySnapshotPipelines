from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from typing import Any

import pandas as pd

from .universe_audit_contracts import (
    BENCHMARK_ONLY,
    ELIGIBLE_FOR_RESEARCH_RANKING,
    GateResult,
    MISSING_DATA,
    OBSERVE_UNTIL_SEASONED,
    REJECTED,
    RESEARCH_CANDIDATE,
    SelectionRuleSpec,
    SymbolSpec,
    TRACKER_ONLY,
    UniverseAuditResult,
    WATCHLIST,
    CandidateDecision,
)

DEFAULT_MOMENTUM_WEIGHTS: tuple[tuple[int, float], ...] = ((21, 0.25), (63, 0.35), (126, 0.40))
SUPPORTED_GATE_OPERATORS = frozenset({">=", ">", "<=", "<", "==", "!="})
DEFAULT_ALLOWED_METRICS = frozenset(
    {
        "has_data",
        "eligible_for_trading",
        "trading_days",
        "month_end_closes",
        "data_completeness",
        "ret_21d",
        "ret_63d",
        "ret_126d",
        "ret_252d",
        "ytd_return",
        "return_since_first_trade",
        "momentum_score",
        "vol_63",
        "maxdd_126",
        "adv20_usd",
    }
)


def normalize_symbols(symbols: Sequence[str] | str | None) -> tuple[str, ...]:
    if symbols is None:
        return ()
    raw = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    return tuple(dict.fromkeys(str(symbol or "").strip().upper() for symbol in raw if str(symbol or "").strip()))


def normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"], utc=False).dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    if "volume" in frame.columns:
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    else:
        frame["volume"] = float("nan")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise ValueError("price_history has no usable rows")
    return frame.sort_values(["symbol", "as_of"]).reset_index(drop=True)


def close_matrix(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_price_history(price_history)
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.map(str).str.upper()
    return close.ffill()


def _month_end_close_count(series: pd.Series) -> int:
    clean = series.dropna()
    if clean.empty:
        return 0
    return int(clean.groupby(clean.index.to_period("M")).last().dropna().shape[0])


def _trailing_return(series: pd.Series, days: int) -> float:
    clean = series.dropna().tail(int(days) + 1)
    if clean.shape[0] < 2:
        return float("nan")
    first = float(clean.iloc[0])
    if first == 0.0:
        return float("nan")
    return float(clean.iloc[-1] / first - 1.0)


def _return_since(series: pd.Series, start_date: str) -> float:
    clean = series.dropna().loc[pd.Timestamp(start_date) :]
    if clean.shape[0] < 2:
        return float("nan")
    first = float(clean.iloc[0])
    if first == 0.0:
        return float("nan")
    return float(clean.iloc[-1] / first - 1.0)


def _momentum_score(series: pd.Series, *, weights: Sequence[tuple[int, float]] = DEFAULT_MOMENTUM_WEIGHTS) -> float:
    score = 0.0
    total_weight = 0.0
    for days, weight in weights:
        value = _trailing_return(series, int(days))
        if pd.isna(value):
            continue
        score += float(weight) * float(value)
        total_weight += float(weight)
    return score / total_weight if total_weight > 0 else float("nan")


def _realized_vol(series: pd.Series, days: int) -> float:
    returns = series.dropna().pct_change(fill_method=None).dropna().tail(int(days))
    if returns.shape[0] < max(2, int(days) // 2):
        return float("nan")
    return float(returns.std(ddof=0) * math.sqrt(252.0))


def _max_drawdown(series: pd.Series, days: int) -> float:
    clean = series.dropna().tail(int(days))
    if clean.shape[0] < 2:
        return float("nan")
    equity = clean / float(clean.iloc[0])
    return float((equity / equity.cummax() - 1.0).min())


def _data_completeness(series: pd.Series) -> float:
    clean = series.dropna()
    if clean.empty:
        return 0.0
    expected = max(len(pd.bdate_range(clean.index.min(), clean.index.max())), 1)
    return float(min(1.0, clean.shape[0] / expected))


def _adv20_usd(symbol_history: pd.DataFrame) -> float:
    history = symbol_history.dropna(subset=["close"]).tail(20)
    if history.empty or history["volume"].isna().all():
        return float("nan")
    return float((history["close"] * history["volume"]).mean())


def _coerce_gate_value(value: Any) -> Any:
    if isinstance(value, bool):
        return bool(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _compare(actual: Any, operator: str, threshold: Any) -> bool:
    operator = str(operator).strip()
    if operator not in SUPPORTED_GATE_OPERATORS:
        raise ValueError(f"unsupported gate operator {operator!r}; supported: {sorted(SUPPORTED_GATE_OPERATORS)}")
    actual = _coerce_gate_value(actual)
    threshold = _coerce_gate_value(threshold)
    if actual is None or pd.isna(actual):
        return False
    if operator == ">=":
        return bool(actual >= threshold)
    if operator == ">":
        return bool(actual > threshold)
    if operator == "<=":
        return bool(actual <= threshold)
    if operator == "<":
        return bool(actual < threshold)
    if operator == "==":
        return bool(actual == threshold)
    if operator == "!=":
        return bool(actual != threshold)
    raise AssertionError(operator)


def validate_rule_spec(rule_spec: SelectionRuleSpec, *, allowed_metrics: set[str] | frozenset[str] = DEFAULT_ALLOWED_METRICS) -> SelectionRuleSpec:
    spec = rule_spec.normalized()
    if not spec.rule_id:
        raise ValueError("rule_id is required")
    if not spec.rule_version:
        raise ValueError("rule_version is required")
    if not spec.universe_id:
        raise ValueError("universe_id is required")
    if not spec.hard_gates:
        raise ValueError("at least one hard gate is required")
    for gate in spec.hard_gates:
        if not gate.name:
            raise ValueError("hard gate name is required")
        if gate.metric not in allowed_metrics:
            raise ValueError(f"unknown hard gate metric {gate.metric!r}; allowed metrics: {sorted(allowed_metrics)}")
        if gate.operator not in SUPPORTED_GATE_OPERATORS:
            raise ValueError(f"unsupported gate operator {gate.operator!r}; supported: {sorted(SUPPORTED_GATE_OPERATORS)}")
    for term in spec.score_terms:
        if term.metric not in allowed_metrics:
            raise ValueError(f"unknown score metric {term.metric!r}; allowed metrics: {sorted(allowed_metrics)}")
        if term.weight == 0.0:
            raise ValueError("score term weight must not be zero")
    return spec


def build_candidate_snapshot(
    price_history: pd.DataFrame,
    *,
    specs: Sequence[SymbolSpec],
    as_of_date: str | None = None,
) -> pd.DataFrame:
    prices = normalize_price_history(price_history)
    if as_of_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(as_of_date).normalize()].copy()
    close = prices.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index().ffill()
    close.columns = close.columns.map(str).str.upper()
    latest_as_of = close.index.max() if not close.empty else pd.NaT
    rows: list[dict[str, object]] = []
    for raw_spec in specs:
        spec = raw_spec.normalized()
        row: dict[str, object] = asdict(spec)
        row["as_of"] = pd.Timestamp(latest_as_of).date().isoformat() if pd.notna(latest_as_of) else ""
        if not spec.symbol or spec.symbol not in close.columns:
            rows.append({**row, "has_data": False})
            continue
        series = close[spec.symbol].dropna()
        symbol_history = prices.loc[prices["symbol"] == spec.symbol].copy()
        if series.empty:
            rows.append({**row, "has_data": False})
            continue
        rows.append(
            {
                **row,
                "has_data": True,
                "first_trade_date": series.index.min().date().isoformat(),
                "last_trade_date": series.index.max().date().isoformat(),
                "trading_days": int(series.shape[0]),
                "month_end_closes": _month_end_close_count(series),
                "data_completeness": _data_completeness(series),
                "ytd_return": _return_since(series, "2026-01-02"),
                "return_since_first_trade": float(series.iloc[-1] / series.iloc[0] - 1.0),
                "ret_21d": _trailing_return(series, 21),
                "ret_63d": _trailing_return(series, 63),
                "ret_126d": _trailing_return(series, 126),
                "ret_252d": _trailing_return(series, 252),
                "momentum_score": _momentum_score(series),
                "vol_63": _realized_vol(series, 63),
                "maxdd_126": _max_drawdown(series, 126),
                "adv20_usd": _adv20_usd(symbol_history),
            }
        )
    result = pd.DataFrame(rows)
    if "symbol" in result.columns:
        result["symbol"] = result["symbol"].astype(str).str.upper().str.strip()
    return result


def _research_action_for_row(row: Mapping[str, Any], failed_gates: Sequence[GateResult]) -> str:
    if not bool(row.get("has_data", False)):
        return MISSING_DATA
    role = str(row.get("role") or "").strip().lower()
    if role == "benchmark":
        return BENCHMARK_ONLY
    if not bool(row.get("eligible_for_trading", False)):
        return TRACKER_ONLY
    if failed_gates:
        if any(gate.gate_name in {"min_trading_days", "min_month_end_closes"} for gate in failed_gates):
            return OBSERVE_UNTIL_SEASONED
        return REJECTED
    return ELIGIBLE_FOR_RESEARCH_RANKING


def _decision_for_action(action: str) -> str:
    if action == ELIGIBLE_FOR_RESEARCH_RANKING:
        return RESEARCH_CANDIDATE
    if action in {MISSING_DATA, REJECTED}:
        return REJECTED
    return WATCHLIST


def evaluate_gates(snapshot: pd.DataFrame, rule_spec: SelectionRuleSpec) -> tuple[pd.DataFrame, tuple[CandidateDecision, ...]]:
    spec = validate_rule_spec(rule_spec)
    gate_rows: list[dict[str, object]] = []
    decisions: list[CandidateDecision] = []
    actions: list[str] = []
    seasonings: list[bool] = []
    for row in snapshot.to_dict(orient="records"):
        symbol = str(row.get("symbol") or "").strip().upper()
        failed: list[GateResult] = []
        if not bool(row.get("has_data", False)):
            gate_result = GateResult(
                symbol=symbol,
                gate_name="has_data",
                metric="has_data",
                passed=False,
                actual=False,
                operator="==",
                threshold=True,
                action=REJECTED,
                reason="missing_data",
            )
            gate_rows.append(gate_result.to_row())
            failed.append(gate_result)
        if not bool(row.get("eligible_for_trading", False)):
            gate_result = GateResult(
                symbol=symbol,
                gate_name="eligible_for_trading",
                metric="eligible_for_trading",
                passed=False,
                actual=bool(row.get("eligible_for_trading", False)),
                operator="==",
                threshold=True,
                action=WATCHLIST,
                reason="not_tradeable_candidate",
            )
            gate_rows.append(gate_result.to_row())
            failed.append(gate_result)
        for gate in spec.hard_gates:
            actual = row.get(gate.metric)
            passed = _compare(actual, gate.operator, gate.threshold)
            gate_result = GateResult(
                symbol=symbol,
                gate_name=gate.name,
                metric=gate.metric,
                passed=passed,
                actual=_coerce_gate_value(actual),
                operator=gate.operator,
                threshold=gate.threshold,
                action=gate.failure_action if not passed else "pass",
                reason="pass" if passed else f"{gate.name}_failed",
            )
            gate_rows.append(gate_result.to_row())
            if not passed:
                failed.append(gate_result)
        action = _research_action_for_row(row, failed)
        decisions.append(
            CandidateDecision(
                symbol=symbol,
                decision=_decision_for_action(action),
                research_action=action,
                reason=";".join(gate.reason for gate in failed) if failed else "all_hard_gates_passed",
                failed_gates=tuple(gate.gate_name for gate in failed),
            )
        )
        actions.append(action)
        seasonings.append(not any(gate.gate_name in {"min_trading_days", "min_month_end_closes"} for gate in failed))
    gate_results = pd.DataFrame(gate_rows)
    return gate_results, tuple(decisions)


def score_snapshot(snapshot: pd.DataFrame, rule_spec: SelectionRuleSpec) -> pd.Series:
    spec = validate_rule_spec(rule_spec)
    if snapshot.empty:
        return pd.Series(dtype=float)
    score = pd.Series(0.0, index=snapshot.index)
    total_weight = 0.0
    for term in spec.score_terms:
        values = pd.to_numeric(snapshot.get(term.metric), errors="coerce")
        if values.notna().sum() <= 1:
            normalized = values.fillna(0.0)
        else:
            std = values.std(ddof=0)
            normalized = pd.Series(0.0, index=snapshot.index) if not std or pd.isna(std) else (values - values.mean()) / std
            normalized = normalized.fillna(0.0)
        direction = 1.0 if term.higher_is_better else -1.0
        score += float(term.weight) * direction * normalized
        total_weight += abs(float(term.weight))
    return score / total_weight if total_weight else pd.Series(0.0, index=snapshot.index)


def build_ranking(snapshot: pd.DataFrame, decisions: Sequence[CandidateDecision], rule_spec: SelectionRuleSpec) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot.copy()
    decision_map = {decision.symbol: decision for decision in decisions}
    frame = snapshot.copy()
    frame["research_action"] = frame["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].research_action)
    frame["promotion_decision"] = frame["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].decision)
    frame["decision_reason"] = frame["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].reason)
    eligible = frame["research_action"].eq(ELIGIBLE_FOR_RESEARCH_RANKING)
    ranking = frame.loc[eligible].copy()
    if ranking.empty:
        return ranking
    ranking["score"] = score_snapshot(ranking, rule_spec)
    sort_columns = ["score"]
    ascending = [False]
    if "momentum_score" in ranking.columns:
        sort_columns.append("momentum_score")
        ascending.append(False)
    if "ret_63d" in ranking.columns:
        sort_columns.append("ret_63d")
        ascending.append(False)
    ranking = ranking.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    return ranking


def run_universe_audit(
    price_history: pd.DataFrame,
    *,
    specs: Sequence[SymbolSpec],
    rule_spec: SelectionRuleSpec,
    as_of_date: str | None = None,
) -> UniverseAuditResult:
    spec = validate_rule_spec(rule_spec)
    snapshot = build_candidate_snapshot(price_history, specs=specs, as_of_date=as_of_date)
    gate_results, decisions = evaluate_gates(snapshot, spec)
    decision_map = {decision.symbol: decision for decision in decisions}
    if not snapshot.empty:
        snapshot = snapshot.copy()
        snapshot["seasoning_eligible"] = snapshot["symbol"].map(
            lambda symbol: not any(
                gate in {"min_trading_days", "min_month_end_closes"}
                for gate in decision_map[str(symbol).upper()].failed_gates
            )
        )
        snapshot["research_action"] = snapshot["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].research_action)
        snapshot["promotion_decision"] = snapshot["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].decision)
        snapshot["decision_reason"] = snapshot["symbol"].map(lambda symbol: decision_map[str(symbol).upper()].reason)
    ranking = build_ranking(snapshot, decisions, spec)
    diagnostics = {
        "candidate_count": int(len(snapshot)),
        "ranking_count": int(len(ranking)),
        "rule_id": spec.rule_id,
        "rule_version": spec.rule_version,
        "universe_id": spec.universe_id,
    }
    return UniverseAuditResult(
        candidate_snapshot=snapshot,
        gate_results=gate_results,
        ranking=ranking,
        promotion_decisions=decisions,
        rule_spec=spec,
        diagnostics=diagnostics,
    )
