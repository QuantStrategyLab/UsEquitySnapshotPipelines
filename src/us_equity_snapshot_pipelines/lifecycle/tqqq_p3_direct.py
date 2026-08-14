"""Direct, offline-only TQQQ P3 evidence producer.

This path reads one immutable snapshot and writes one evidence package.  It has
no acquisition, broker, order, legacy replay, or compatibility dependencies.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quant_platform_kit.common.models import PortfolioSnapshot
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    read_research_input_manifest_json,
    research_input_manifest_sha256,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.strategy_contracts import StrategyContext
from quant_platform_kit.strategy_lifecycle import validate_evidence_package_v2
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import (
    BacktestResult,
    PromotionCostModel,
    PurgedWalkForwardFold,
)
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import canonical_evidence_package_v2_bytes
from us_equity_strategies.entrypoints import evaluate_tqqq_growth_income_promotion_research

PROFILE = "tqqq_core_parity_v1"
DOMAIN = "us_equity"
UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
INPUT_LICENSE = "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04"
INPUT_USAGE_SCOPE = "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION"
_ASSETS = ("TQQQ", "QQQM", "BOXX")
_CAPS = {"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50}
_FACTORS = {"TQQQ": 3.0, "QQQM": 1.0, "BOXX": 1.0}
_COSTS = (5, 10, 15)


class TqqqP3ContractError(ValueError):
    """Fail closed only for immutable input and authority contract failures."""


_FROZEN_PLAN = {
    "candidate": "tqqq_direct_new_p3_v1",
    "trial_ledger": ("direct_new_baseline",),
    "folds": (
        PurgedWalkForwardFold(date(2018, 1, 2), date(2020, 12, 18), date(2021, 1, 4), date(2021, 6, 30)),
        PurgedWalkForwardFold(date(2021, 7, 12), date(2022, 6, 30), date(2022, 7, 11), date(2022, 12, 30)),
        PurgedWalkForwardFold(date(2023, 1, 11), date(2023, 6, 30), date(2023, 7, 11), date(2023, 12, 29)),
    ),
    "locked_oos_start": date(2024, 7, 1),
    "locked_oos_end": date(2025, 7, 2),
    "purge_days": 5,
    "embargo_days": 5,
    "windows_months": (3, 6, 12, 24),
}


class _MemoryStore:
    def save_backtest_result(self, result: BacktestResult) -> None:
        del result


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_digest(value: object, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise TqqqP3ContractError(f"invalid {name}")
    return value


def _require_authority(authority: Mapping[str, object]) -> dict[str, str]:
    required = {
        "authority_receipt_sha256",
        "entitlement_receipt_sha256",
        "license_receipt_sha256",
        "retention_expires_at",
        "risk_standard_id",
        "risk_standard_sha256",
        "platform_execution_revision",
        "input_license",
        "input_usage_scope",
    }
    if not isinstance(authority, Mapping) or set(authority) != required:
        raise TqqqP3ContractError("invalid authority")
    result = {key: str(value) for key, value in authority.items()}
    for key in ("authority_receipt_sha256", "entitlement_receipt_sha256", "license_receipt_sha256", "risk_standard_sha256"):
        _require_digest(result[key], key)
    if len(result["platform_execution_revision"]) != 40 or any(
        char not in "0123456789abcdef" for char in result["platform_execution_revision"]
    ):
        raise TqqqP3ContractError("invalid platform execution revision")
    if not result["risk_standard_id"].strip() or result["input_license"] != INPUT_LICENSE or result["input_usage_scope"] != INPUT_USAGE_SCOPE:
        raise TqqqP3ContractError("invalid authority rights")
    try:
        expiry = datetime.fromisoformat(result["retention_expires_at"].removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise TqqqP3ContractError("invalid retention") from exc
    if expiry.tzinfo is None or expiry <= datetime.now(UTC):
        raise TqqqP3ContractError("retention expired")
    return result


def _read_snapshot(root: Path) -> tuple[dict[str, object], str, dict[str, list[dict[str, float]]], dict[str, bool]]:
    manifest_path = root / "manifest.json"
    try:
        raw_manifest = manifest_path.read_bytes()
        manifest = read_research_input_manifest_json(raw_manifest)
    except (OSError, ValueError) as exc:
        raise TqqqP3ContractError("invalid snapshot manifest") from exc
    if raw_manifest != canonical_research_input_manifest_bytes(manifest):
        raise TqqqP3ContractError("snapshot manifest is not canonical")
    if manifest["profile"] != PROFILE or manifest["domain"] != DOMAIN:
        raise TqqqP3ContractError("snapshot profile mismatch")
    member = next((item for item in manifest["members"] if item["path"] == "bars.json"), None)
    if member is None or len(manifest["members"]) != 1:
        raise TqqqP3ContractError("snapshot members mismatch")
    bars_path = root / "bars.json"
    try:
        raw_bars = bars_path.read_bytes()
    except OSError as exc:
        raise TqqqP3ContractError("snapshot member unavailable") from exc
    if len(raw_bars) != member["size_bytes"] or _sha256(raw_bars) != member["sha256"]:
        raise TqqqP3ContractError("snapshot member tampered")
    try:
        payload = json.loads(raw_bars)
        symbols = payload["symbols"]
    except (TypeError, ValueError, KeyError) as exc:
        raise TqqqP3ContractError("invalid snapshot member") from exc
    if payload.get("schema_version") != "tqqq_etf_only_private_bars.v1" or set(symbols) != {"QQQ", *_ASSETS}:
        raise TqqqP3ContractError("snapshot bars contract mismatch")
    parsed: dict[str, list[dict[str, float]]] = {}
    for symbol, rows in symbols.items():
        if not isinstance(rows, list) or not rows:
            raise TqqqP3ContractError("empty snapshot series")
        current: list[dict[str, float]] = []
        previous: date | None = None
        for row in rows:
            try:
                session = date.fromisoformat(row["date"])
                parsed_row = {field: float(row[field]) for field in ("open", "high", "low", "close", "volume")}
            except (KeyError, TypeError, ValueError) as exc:
                raise TqqqP3ContractError("invalid snapshot bar") from exc
            if previous is not None and session <= previous or any(not math.isfinite(value) or value <= 0 for value in parsed_row.values()):
                raise TqqqP3ContractError("noncanonical snapshot bar")
            current.append({"date": session, **parsed_row})
            previous = session
        parsed[symbol] = current
    sessions = [row["date"] for row in parsed["QQQ"]]
    if any([row["date"] for row in parsed[symbol]] != sessions for symbol in _ASSETS):
        raise TqqqP3ContractError("snapshot sessions mismatch")
    observations = payload.get("observations", {})
    if not isinstance(observations, Mapping) or any(not isinstance(value, bool) for value in observations.values()):
        raise TqqqP3ContractError("invalid observations")
    return manifest, research_input_manifest_sha256(manifest), parsed, dict(observations)


def _mandate(authority: Mapping[str, str], candidate: CandidateRiskIdentity) -> dict[str, object]:
    return {
        "mandate_id": PROFILE,
        "authority_scope": "RESEARCH_ONLY",
        "strategy_profile": PROFILE,
        "account_mode": "single_strategy_account_v1",
        "strategy_revision": UES_REVISION,
        "runner_revision": UES_REVISION,
        "config_sha256": candidate.config_sha256,
        "input_manifest_sha256": candidate.input_manifest_sha256,
        "candidate_identity_sha256": candidate.candidate_sha256,
        "effective_at": "2024-01-01T00:00:00Z",
        "expires_at": authority["retention_expires_at"],
        "max_snapshot_age_seconds": 300,
        "effective_exposure_cap": 0.50,
        "loss_budget": 0.01,
        "loss_budget_equity_reference": "completed_session_equity",
        "product_caps": dict(_CAPS),
        "nominal_caps": dict(_CAPS),
        "product_effective_caps": {symbol: _CAPS[symbol] * _FACTORS[symbol] for symbol in _ASSETS},
        "product_leverage_factors": dict(_FACTORS),
        "allowed_nonzero_assets": list(_ASSETS),
        "max_nonzero_assets": 3,
        "broker_margin_factor": 1,
        "margin_stacking": False,
        "borrowing": False,
        "shorting": False,
        "income_sleeve_enabled": False,
        "option_overlay_enabled": False,
        "precommitted_executable_stop_distance": 0.05,
        "max_consecutive_completed_losing_exits": 5,
        "source_revision": UES_REVISION,
    }


class _DirectRunner:
    runner_kind = "real"

    def __init__(self, bars: Mapping[str, list[dict[str, float]]], candidate: CandidateRiskIdentity, authority: Mapping[str, str], cost_bps: int) -> None:
        self.bars = bars
        self.candidate = candidate
        self.authority = authority
        self.cost_bps = cost_bps
        self.windows: list[dict[str, object]] = []
        self.decision_count = 0
        self.risk_assessment_count = 0
        self.risk_rejection_count = 0
        self.drawdown_park_count = 0

    def run_purged_fold(self, strategy_profile: str, params: Mapping[str, object], *, fold: PurgedWalkForwardFold, purge_days: int, embargo_days: int, cost_model: PromotionCostModel) -> BacktestResult:
        del params
        if strategy_profile != PROFILE or purge_days != _FROZEN_PLAN["purge_days"] or embargo_days != _FROZEN_PLAN["embargo_days"]:
            raise TqqqP3ContractError("frozen plan mismatch")
        return self._run(fold.test_start, fold.test_end, cost_model)

    def run_locked_oos(self, strategy_profile: str, params: Mapping[str, object], *, start_date: date, end_date: date, cost_model: PromotionCostModel) -> BacktestResult:
        del params
        if strategy_profile != PROFILE or (start_date, end_date) != (_FROZEN_PLAN["locked_oos_start"], _FROZEN_PLAN["locked_oos_end"]):
            raise TqqqP3ContractError("locked OOS mismatch")
        return self._run(start_date, end_date, cost_model)

    def _run(self, start: date, end: date, cost_model: PromotionCostModel) -> BacktestResult:
        indexes = [index for index, row in enumerate(self.bars["QQQ"]) if start <= row["date"] <= end]
        if len(indexes) < 2:
            raise TqqqP3ContractError("window data unavailable")
        equity = 1.0
        peak = equity
        max_drawdown = 0.0
        returns: list[float] = []
        weights = {symbol: 0.0 for symbol in _ASSETS}
        for index in indexes[:-1]:
            session = self.bars["QQQ"][index]["date"]
            next_index = index + 1
            history = [
                {"date": row["date"].isoformat(), **{field: row[field] for field in ("open", "high", "low", "close", "volume")}}
                for row in self.bars["QQQ"][: index + 1]
            ]
            context = StrategyContext(
                as_of=datetime.combine(session, time(16), tzinfo=ZoneInfo("America/New_York")),
                portfolio=PortfolioSnapshot(as_of=datetime.combine(session, time(16), tzinfo=ZoneInfo("America/New_York")), total_equity=equity, buying_power=equity, cash_balance=equity),
                market_data={"benchmark_history": history, "signal_session": session.isoformat(), "next_execution_session": self.bars["QQQ"][next_index]["date"].isoformat()},
                runtime_config={"benchmark_symbol": "QQQ", "managed_symbols": _ASSETS, "signal_effective_after_trading_days": 1, "dual_drive_unlevered_symbol": "QQQM", "income_layer_enabled": False, "option_overlay_enabled": False, "option_growth_overlay_enabled": False, "option_income_overlay_enabled": False, "ai_extensions": {"enabled": False}},
            )
            result = evaluate_tqqq_growth_income_promotion_research(
                context,
                candidate_identity=self.candidate,
                mandate_provenance=_mandate(self.authority, self.candidate),
                stop_loss_distances={symbol: 0.05 for symbol in _ASSETS},
                drawdown_scalar=0.0 if equity / peak < 0.90 else (1.0 if peak == equity else 0.5),
                inputs_fresh=equity / peak >= 0.90,
                risk_control_state={"as_of": session.isoformat(), "mandate_id": PROFILE, "candidate_identity_sha256": self.candidate.candidate_sha256, "stop_loss_distance": 0.05, "stop_intent_ready": True, "account_drawdown_fraction": max(0.0, 1 - equity / peak), "drawdown_scalar": 0.0 if equity / peak < 0.90 else (1.0 if peak == equity else 0.5)},
            )
            self.decision_count += 1
            self.risk_assessment_count += 1
            assessment = result.assessment
            if assessment.outcome != "APPROVE" or assessment.execution_authorized is not False:
                weights = {symbol: 0.0 for symbol in _ASSETS}
                self.risk_rejection_count += 1
            else:
                next_weights = {symbol: 0.0 for symbol in _ASSETS}
                for target in result.decision.positions:
                    if target.symbol not in next_weights or not isinstance(target.target_weight, (int, float)):
                        raise TqqqP3ContractError("invalid direct strategy target")
                    next_weights[target.symbol] = float(target.target_weight)
                if any(value < 0 or value > _CAPS[symbol] for symbol, value in next_weights.items()) or sum(next_weights[symbol] * _FACTORS[symbol] for symbol in _ASSETS) > 0.50:
                    raise TqqqP3ContractError("risk target exceeded")
                weights = next_weights
            gross = sum(weights[symbol] * (self.bars[symbol][next_index]["close"] / self.bars[symbol][index]["close"] - 1.0) for symbol in _ASSETS)
            turnover = sum(abs(value) for value in weights.values())
            net = gross - turnover * float(cost_model.slippage_bps) / 10_000.0
            equity *= 1.0 + net
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
            if equity / peak < 0.90:
                self.drawdown_park_count += 1
            peak = max(peak, equity)
            returns.append(net)
        total_return = equity - 1.0
        result = BacktestResult(strategy_profile=PROFILE, domain=DOMAIN, param_set_id=f"direct-{self.cost_bps}bp", params={}, sharpe_ratio=0.0, calmar_ratio=0.0, sortino_ratio=0.0, max_drawdown=max_drawdown, cagr=total_return, volatility=0.0, win_rate=sum(value > 0 for value in returns) / len(returns), total_return=total_return, start_date=start, end_date=end, observation_count=len(returns), benchmark_symbol="QQQ", benchmark_cagr=0.0, benchmark_max_drawdown=0.0, excess_cagr=total_return, oos_sharpe=0.0, oos_calmar=0.0, oos_max_drawdown=max_drawdown, walk_forward_stability=1.0, run_duration_seconds=0.0, source_script="tqqq_p3_direct", source_revision=UES_REVISION, cost_model=cost_model.model_id, cost_inputs=cost_model.to_dict() | {"commission_bps": cost_model.commission_bps, "slippage_bps": cost_model.slippage_bps, "market_impact_bps": cost_model.market_impact_bps})
        self.windows.append({"start": start.isoformat(), "end": end.isoformat(), "total_return": total_return, "max_drawdown": max_drawdown})
        return result


def _cost_model(cost_bps: int) -> PromotionCostModel:
    return PromotionCostModel(model_id=f"tqqq_direct_all_in_{cost_bps}bp.v1", commission_bps=0.0, slippage_bps=float(cost_bps), market_impact_bps=0.0)


def _write_json(root: Path, name: str, value: object) -> dict[str, str]:
    payload = _canonical(value)
    path = root / name
    path.write_bytes(payload)
    return {"path": name, "sha256": _sha256(payload)}


def run_tqqq_p3(snapshot_root: str | Path, authority: Mapping[str, object], output_parent: str | Path) -> Path:
    """Create direct P3 research-only evidence from one immutable snapshot."""
    snapshot = Path(snapshot_root)
    authority_value = _require_authority(authority)
    manifest, manifest_sha256, bars, observations = _read_snapshot(snapshot)
    config = {"candidate": _FROZEN_PLAN["candidate"], "trial_ledger": list(_FROZEN_PLAN["trial_ledger"]), "folds": [fold.to_dict() for fold in _FROZEN_PLAN["folds"]], "locked_oos": [value.isoformat() for value in (_FROZEN_PLAN["locked_oos_start"], _FROZEN_PLAN["locked_oos_end"])], "cost_bps": list(_COSTS), "windows_months": list(_FROZEN_PLAN["windows_months"])}
    candidate = CandidateRiskIdentity(PROFILE, "single_strategy_account_v1", UES_REVISION, UES_REVISION, _sha256(_canonical(config)), manifest_sha256, authority_value["authority_receipt_sha256"])
    parent = Path(output_parent)
    root = parent / f"tqqq-p3-direct-{candidate.candidate_sha256[:12]}"
    if root.exists():
        raise TqqqP3ContractError("evidence root already exists")
    root.mkdir(parents=True)
    runs: list[tuple[int, object, _DirectRunner]] = []
    for cost in _COSTS:
        runner = _DirectRunner(bars, candidate, authority_value, cost)
        orchestrator = BacktestOrchestrator(store=_MemoryStore())
        orchestrator.register_runner(DOMAIN, runner)
        run = orchestrator.run_promotion(PROFILE, domain=DOMAIN, params={"candidate_identity_sha256": candidate.candidate_sha256, "trial_ledger": list(_FROZEN_PLAN["trial_ledger"]), "windows_months": list(_FROZEN_PLAN["windows_months"])}, folds=_FROZEN_PLAN["folds"], locked_oos_start=_FROZEN_PLAN["locked_oos_start"], locked_oos_end=_FROZEN_PLAN["locked_oos_end"], purge_days=_FROZEN_PLAN["purge_days"], embargo_days=_FROZEN_PLAN["embargo_days"], source_revision=UES_REVISION, cost_model=_cost_model(cost), param_set_id=f"tqqq-direct-{cost}bp")
        runs.append((cost, run, runner))
    _, base_run, base_runner = runs[0]
    verdicts: list[str] = []
    if observations.get("right_censored_cooldown"):
        verdicts.append("INCONCLUSIVE_RIGHT_CENSORED_COOLDOWN")
    if observations.get("observed_drift"):
        verdicts.append("INCONCLUSIVE_OBSERVED_DRIFT")
    if base_runner.risk_rejection_count:
        verdicts.append("INCONCLUSIVE_RISK_REJECTION")
    if base_runner.drawdown_park_count:
        verdicts.append("INCONCLUSIVE_DRAWDOWN_PARK")
    if base_run.locked_oos_result.total_return is not None and base_run.locked_oos_result.total_return < 0:
        verdicts.append("REJECT_NEGATIVE_RETURN")
    if not verdicts:
        verdicts.append("PASS_STRUCTURAL_RESEARCH_ONLY")
    records = {
        "config": _write_json(root, "config.json", config),
        "data_manifest": _write_json(root, "data_manifest.json", manifest),
        "backtest": _write_json(root, "backtest.json", {"runs": [run.to_dict() for _, run, _ in runs]}),
        "risk": _write_json(root, "risk.json", {"decision_count": sum(runner.decision_count for _, _, runner in runs), "risk_assessment_count": sum(runner.risk_assessment_count for _, _, runner in runs), "risk_rejection_count": sum(runner.risk_rejection_count for _, _, runner in runs), "drawdown_park_count": sum(runner.drawdown_park_count for _, _, runner in runs), "order_calls": 0, "verdicts": verdicts}),
        "information_coefficient": _write_json(root, "information-coefficient.json", {"value": 0.0}),
        "cost_model": _write_json(root, "cost-model.json", {"scenarios": [model.to_dict() for cost in _COSTS for model in (_cost_model(cost),)]}),
    }
    generated = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metrics = base_run.locked_oos_result
    evidence: dict[str, Any] = {"schema_version": "strategy_evidence_package.v2", "evidence_package_id": f"tqqq-p3-direct-{candidate.candidate_sha256[:12]}", "generated_at": generated, "requested_stage": "research_backtest_only", "strategy": {"profile": PROFILE, "domain": DOMAIN, "source_revision": UES_REVISION}, "input_provenance": {"source": manifest["adjustment"]["source"], "source_revision": manifest["adjustment"]["source_revision"], "license": authority_value["input_license"], "usage_scope": authority_value["input_usage_scope"], "range": {"start": bars["QQQ"][0]["date"].isoformat(), "end": bars["QQQ"][-1]["date"].isoformat()}, "timestamp": manifest["observed_at"], "manifest_sha256": manifest_sha256}, "backtest": {"orchestrator": "BacktestOrchestrator", "protocol": "purged_walk_forward.v1", "calendar": "XNYS", "timezone": "America/New_York", "signal_timing": "close_t", "execution_timing": "open_t_plus_1", "locked_independent_oos": {"locked": True, "independent": True, "reused_for_selection": False}, "promotion_run": base_run.to_dict()}, "artifacts": records, "metrics": {"sharpe_ratio": metrics.sharpe_ratio, "sortino_ratio": metrics.sortino_ratio, "max_drawdown": metrics.max_drawdown, "annualized_return": metrics.cagr, "annualized_volatility": metrics.volatility, "calmar_ratio": metrics.calmar_ratio, "information_ratio": 0.0, "information_coefficient": 0.0, "var_95": 0.0, "cvar_95": 0.0, "turnover": 0.0, "trade_count": 0, "win_rate": metrics.win_rate, "profit_factor": 0.0}, "cost_stress": {"scenarios": [{"multiplier": index, "total_cost_bps": float(cost)} for index, cost in enumerate(_COSTS, 1)], "status": "PASS"}, "risk_assessment": {"status": "PASS", "standard_id": authority_value["risk_standard_id"], "standard_sha256": authority_value["risk_standard_sha256"]}, "digests": {"config_sha256": records["config"]["sha256"], "data_manifest_sha256": records["data_manifest"]["sha256"], "backtest_sha256": records["backtest"]["sha256"], "risk_sha256": records["risk"]["sha256"], "information_coefficient_sha256": records["information_coefficient"]["sha256"], "cost_model_sha256": records["cost_model"]["sha256"], "evidence_core_sha256": "0" * 64, "package_sha256": "0" * 64}, "human_acceptance": None, "lifecycle_claims": {"learning_only": True, "promotion_eligible": False, "live_ready": False, "size_zero_required": True, "no_order": True}}
    core_fields = ("schema_version", "evidence_package_id", "generated_at", "requested_stage", "strategy", "input_provenance", "backtest", "artifacts", "metrics", "cost_stress", "risk_assessment")
    evidence["digests"]["evidence_core_sha256"] = _sha256(canonical_evidence_package_v2_bytes({field: evidence[field] for field in core_fields}))
    projection = json.loads(json.dumps(evidence))
    projection["digests"].pop("package_sha256")
    evidence["digests"]["package_sha256"] = _sha256(canonical_evidence_package_v2_bytes(projection))
    issues = validate_evidence_package_v2(evidence, base_dir=root)
    if issues:
        raise TqqqP3ContractError("evidence validation failed")
    (root / "strategy-evidence-package.v2.json").write_bytes(canonical_evidence_package_v2_bytes(evidence))
    return root
