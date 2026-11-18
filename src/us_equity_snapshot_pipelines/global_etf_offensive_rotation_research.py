"""Global ETF offensive rotation research orchestrator and CLI."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

import pandas as pd


from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table
from .yfinance_prices import download_price_history

from ._global_etf_research_models import (
    DEFAULT_PERIODS as DEFAULT_PERIODS,
    DEFAULT_PRICE_START_DATE as DEFAULT_PRICE_START_DATE,
    DEFAULT_TURNOVER_COST_BPS as DEFAULT_TURNOVER_COST_BPS,
    DEFAULT_PRIMARY_BENCHMARK as DEFAULT_PRIMARY_BENCHMARK,
    DEFAULT_SECONDARY_BENCHMARK as DEFAULT_SECONDARY_BENCHMARK,
    DEFAULT_SAFE_HAVEN as DEFAULT_SAFE_HAVEN,
    DEFAULT_CANARY_ASSETS as DEFAULT_CANARY_ASSETS,
    DEFAULT_BASE_POOL as DEFAULT_BASE_POOL,
    DEFAULT_MONTHLY_REBALANCE_MONTHS as DEFAULT_MONTHLY_REBALANCE_MONTHS,
    DEFAULT_OFFENSIVE_POOL as DEFAULT_OFFENSIVE_POOL,
    MIN_TRADING_DAYS_PER_PERIOD as MIN_TRADING_DAYS_PER_PERIOD,
    DRAWNDOWN_ADVANTAGE_VS_QQQ as DRAWNDOWN_ADVANTAGE_VS_QQQ,
    DEFAULT_ROBUSTNESS_CANDIDATES as DEFAULT_ROBUSTNESS_CANDIDATES,
    DEFAULT_ROLLING_ROBUSTNESS_YEARS as DEFAULT_ROLLING_ROBUSTNESS_YEARS,
    DEFAULT_LIVE_BASELINE_CANDIDATE as DEFAULT_LIVE_BASELINE_CANDIDATE,
    DEFAULT_WALK_FORWARD_CANDIDATES as DEFAULT_WALK_FORWARD_CANDIDATES,
    DEFAULT_WALK_FORWARD_TRAIN_YEARS as DEFAULT_WALK_FORWARD_TRAIN_YEARS,
    DEFAULT_WALK_FORWARD_MIN_TRAIN_DAYS_PER_YEAR as DEFAULT_WALK_FORWARD_MIN_TRAIN_DAYS_PER_YEAR,
    DEFAULT_WALK_FORWARD_MIN_TEST_DAYS as DEFAULT_WALK_FORWARD_MIN_TEST_DAYS,
    DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR as DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR,
    DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW as DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW,
    DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME as DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME,
    DEFAULT_DYNAMIC_COST_LOW_LIQUIDITY_PENALTY_BPS as DEFAULT_DYNAMIC_COST_LOW_LIQUIDITY_PENALTY_BPS,
    DEFAULT_DYNAMIC_COST_MAX_LOW_LIQUIDITY_PENALTY_BPS as DEFAULT_DYNAMIC_COST_MAX_LOW_LIQUIDITY_PENALTY_BPS,
    DEFAULT_DYNAMIC_COST_PARTICIPATION_RATE_THRESHOLD as DEFAULT_DYNAMIC_COST_PARTICIPATION_RATE_THRESHOLD,
    DEFAULT_DYNAMIC_COST_PARTICIPATION_PENALTY_BPS as DEFAULT_DYNAMIC_COST_PARTICIPATION_PENALTY_BPS,
    DEFAULT_DYNAMIC_COST_MAX_PARTICIPATION_PENALTY_BPS as DEFAULT_DYNAMIC_COST_MAX_PARTICIPATION_PENALTY_BPS,
    SAFE_LIKE_SYMBOLS as SAFE_LIKE_SYMBOLS,
    LIVE_MIN_LONG_EXCESS_CAGR_VS_BASELINE as LIVE_MIN_LONG_EXCESS_CAGR_VS_BASELINE,
    LIVE_MAX_LONG_DRAWDOWN_DEGRADATION_VS_BASELINE as LIVE_MAX_LONG_DRAWDOWN_DEGRADATION_VS_BASELINE,
    LIVE_MAX_MEDIAN_TURNOVER_INCREASE_VS_BASELINE as LIVE_MAX_MEDIAN_TURNOVER_INCREASE_VS_BASELINE,
    LIVE_MIN_CALENDAR_BASELINE_CAGR_WIN_RATE as LIVE_MIN_CALENDAR_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_ROLLING_3Y_BASELINE_CAGR_WIN_RATE as LIVE_MIN_ROLLING_3Y_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_ROLLING_5Y_BASELINE_CAGR_WIN_RATE as LIVE_MIN_ROLLING_5Y_BASELINE_CAGR_WIN_RATE,
    LIVE_MIN_WORST_ROLLING_EXCESS_CAGR_VS_BASELINE as LIVE_MIN_WORST_ROLLING_EXCESS_CAGR_VS_BASELINE,
    LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE as LIVE_MAX_WORST_WINDOW_DRAWDOWN_DEGRADATION_VS_BASELINE,
    WALK_FORWARD_MIN_WINDOW_COUNT as WALK_FORWARD_MIN_WINDOW_COUNT,
    WALK_FORWARD_MIN_OOS_WIN_RATE as WALK_FORWARD_MIN_OOS_WIN_RATE,
    WALK_FORWARD_MIN_OOS_MEDIAN_EXCESS_CAGR as WALK_FORWARD_MIN_OOS_MEDIAN_EXCESS_CAGR,
    WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR as WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR,
    WALK_FORWARD_MAX_WORST_OOS_DRAWDOWN_DEGRADATION as WALK_FORWARD_MAX_WORST_OOS_DRAWDOWN_DEGRADATION,
    GlobalEtfOffensiveVariantSpec as GlobalEtfOffensiveVariantSpec,
    GlobalEtfLiveableCompositeSpec as GlobalEtfLiveableCompositeSpec,
    DynamicExecutionCostConfig as DynamicExecutionCostConfig,
    GLOBAL_ETF_OFFENSIVE_VARIANTS as GLOBAL_ETF_OFFENSIVE_VARIANTS,
    GLOBAL_ETF_LIVEABLE_COMPOSITES as GLOBAL_ETF_LIVEABLE_COMPOSITES,
    GlobalEtfExperimentProfile as GlobalEtfExperimentProfile,
    GLOBAL_ETF_EXPERIMENT_PROFILES as GLOBAL_ETF_EXPERIMENT_PROFILES,
    resolve_experiment_profile as resolve_experiment_profile,
    _parse_periods as _parse_periods,
    _period_start as _period_start,
    _period_end as _period_end,
    _normalize_symbols as _normalize_symbols,
    _normalize_candidate_ids as _normalize_candidate_ids,
    _filter_variants as _filter_variants,
    _filter_liveable_composites as _filter_liveable_composites,
    _parse_float_list as _parse_float_list,
    collect_required_symbols as collect_required_symbols,
)
from ._global_etf_research_metrics import (
    _normalize_price_history as _normalize_price_history,
    _normalize_weights as _normalize_weights,
    _compute_turnover as _compute_turnover,
    _benchmark_summary as _benchmark_summary,
    summarize_returns as summarize_returns,
)
from ._global_etf_research_backtest import (
    IndicatorContext as IndicatorContext,
    _compute_13612w_momentum_series as _compute_13612w_momentum_series,
    _build_indicator_context as _build_indicator_context,
    _rebalance_dates as _rebalance_dates,
    _indicator_value as _indicator_value,
    _zscore_mapping as _zscore_mapping,
    _pool_correlation_scores as _pool_correlation_scores,
    _candidate_scores as _candidate_scores,
    _resolve_variant_target_weights as _resolve_variant_target_weights,
    run_variant_backtest as run_variant_backtest,
    _period_summary_from_result as _period_summary_from_result,
    _weight_frame_for_index as _weight_frame_for_index,
    _monthly_applied_overlay_weight as _monthly_applied_overlay_weight,
    _gross_returns_from_weights as _gross_returns_from_weights,
    _rolling_total_return as _rolling_total_return,
    _composite_trend_gate as _composite_trend_gate,
    _build_composite_overlay_weight as _build_composite_overlay_weight,
    _combine_composite_weights as _combine_composite_weights,
    _composite_signal_history as _composite_signal_history,
    run_liveable_composite_backtest as run_liveable_composite_backtest,
    build_liveable_composite_results as build_liveable_composite_results,
)
from ._global_etf_research_diagnostics import (
    _gate_reason as _gate_reason,
    _summarize_candidate_window as _summarize_candidate_window,
    build_candidate_robustness_diagnostics as build_candidate_robustness_diagnostics,
    _rolling_dollar_volume as _rolling_dollar_volume,
    _risk_asset_columns as _risk_asset_columns,
    build_candidate_liquidity_diagnostics as build_candidate_liquidity_diagnostics,
    _safe_positive as _safe_positive,
    _dynamic_low_liquidity_penalty_bps as _dynamic_low_liquidity_penalty_bps,
    _dynamic_participation_penalty_bps as _dynamic_participation_penalty_bps,
    _dynamic_cost_for_weights as _dynamic_cost_for_weights,
    build_dynamic_execution_cost_adjusted_returns as build_dynamic_execution_cost_adjusted_returns,
    _metadata_from_period_summary as _metadata_from_period_summary,
    build_period_summary_from_portfolio_returns as build_period_summary_from_portfolio_returns,
    _candidate_period_vs_baseline_summary as _candidate_period_vs_baseline_summary,
    _walk_forward_gate_reason as _walk_forward_gate_reason,
    build_walk_forward_selection_diagnostics as build_walk_forward_selection_diagnostics,
    _numeric_value as _numeric_value,
    _live_readiness_gate_reason as _live_readiness_gate_reason,
    _baseline_relative_window_metrics as _baseline_relative_window_metrics,
    build_live_readiness_summary as build_live_readiness_summary,
    _weights_by_candidate_from_result as _weights_by_candidate_from_result,
    build_ranking as build_ranking,
)

def run_offensive_research(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_PERIODS,
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
    liveable_composites: Sequence[GlobalEtfLiveableCompositeSpec] = GLOBAL_ETF_LIVEABLE_COMPOSITES,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, pd.DataFrame]:
    full_start = _period_start(periods)
    full_end = _period_end(periods)
    close = _normalize_price_history(price_history)
    if full_end is not None:
        close = close.loc[close.index <= pd.Timestamp(full_end).normalize()].copy()
    indicator_context = _build_indicator_context(close, variants=variants)
    rows: list[dict[str, object]] = []
    returns_by_candidate: dict[str, pd.Series] = {}
    weights_by_candidate: dict[str, pd.DataFrame] = {}
    signal_frames: list[pd.DataFrame] = []
    for spec in variants:
        result = run_variant_backtest(
            price_history,
            spec,
            start_date=full_start,
            end_date=full_end,
            turnover_cost_bps=turnover_cost_bps,
            indicator_context=indicator_context,
        )
        returns_by_candidate[spec.candidate_id] = pd.Series(result["portfolio_returns"], name=spec.candidate_id)
        weights_by_candidate[spec.candidate_id] = pd.DataFrame(result["weights_history"])
        signal_history = pd.DataFrame(result.get("signal_history", pd.DataFrame()))
        if not signal_history.empty:
            signal_frames.append(signal_history)
        for period_name, start_date, end_date in periods:
            rows.append(
                _period_summary_from_result(
                    result,
                    period_name=period_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

    composite_result = build_liveable_composite_results(
        context=indicator_context,
        specs=liveable_composites,
        periods=periods,
        weights_by_candidate=weights_by_candidate,
        turnover_cost_bps=turnover_cost_bps,
    )
    rows.extend(composite_result["period_rows"])
    returns_by_candidate.update(composite_result["returns_by_candidate"])
    weights_by_candidate.update(composite_result["weights_by_candidate"])
    composite_signals = pd.DataFrame(composite_result.get("signal_history", pd.DataFrame()))
    if not composite_signals.empty:
        signal_frames.append(composite_signals)

    period_summary = pd.DataFrame(rows)
    ranking = build_ranking(period_summary)
    portfolio_returns = pd.concat(returns_by_candidate.values(), axis=1) if returns_by_candidate else pd.DataFrame()
    signal_history = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame()
    return {
        "period_summary": period_summary,
        "ranking": ranking,
        "portfolio_returns": portfolio_returns,
        "signal_history": signal_history,
        **{f"weights_{candidate_id}": weights for candidate_id, weights in weights_by_candidate.items()},
    }



def build_dynamic_cost_live_readiness_diagnostics(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]],
    config: DynamicExecutionCostConfig,
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
    liveable_composites: Sequence[GlobalEtfLiveableCompositeSpec] = GLOBAL_ETF_LIVEABLE_COMPOSITES,
    robustness_candidates: Sequence[str] = DEFAULT_ROBUSTNESS_CANDIDATES,
) -> dict[str, pd.DataFrame]:
    gross_result = run_offensive_research(
        price_history=price_history,
        periods=periods,
        variants=variants,
        liveable_composites=liveable_composites,
        turnover_cost_bps=0.0,
    )
    weights_by_candidate = _weights_by_candidate_from_result(gross_result)
    adjusted = build_dynamic_execution_cost_adjusted_returns(
        price_history=price_history,
        weights_by_candidate=weights_by_candidate,
        candidate_ids=robustness_candidates,
        config=config,
    )
    portfolio_returns = adjusted["portfolio_returns"]
    if portfolio_returns.empty:
        empty = pd.DataFrame()
        return {
            "dynamic_cost_period_summary": empty,
            "dynamic_cost_ranking": empty,
            "dynamic_cost_portfolio_returns": empty,
            "dynamic_cost_summary": empty,
            "dynamic_cost_robustness_windows": empty,
            "dynamic_cost_robustness_summary": empty,
            "dynamic_cost_live_readiness_summary": empty,
        }
    period_summary = build_period_summary_from_portfolio_returns(
        price_history=price_history,
        periods=periods,
        portfolio_returns=portfolio_returns,
        weights_by_candidate=weights_by_candidate,
        source_period_summary=gross_result["period_summary"],
    )
    ranking = build_ranking(period_summary)
    robustness = build_candidate_robustness_diagnostics(
        price_history=price_history,
        portfolio_returns=portfolio_returns,
        weights_by_candidate=weights_by_candidate,
        candidate_ids=robustness_candidates,
    )
    live_readiness = build_live_readiness_summary(
        period_summary=period_summary,
        ranking=ranking,
        robustness_windows=robustness["robustness_windows"],
    )
    cost_summary = adjusted["dynamic_cost_summary"]
    if not live_readiness.empty and not cost_summary.empty:
        live_readiness = live_readiness.merge(cost_summary, on="Candidate", how="left")
    return {
        "dynamic_cost_period_summary": period_summary,
        "dynamic_cost_ranking": ranking,
        "dynamic_cost_portfolio_returns": portfolio_returns,
        "dynamic_cost_summary": cost_summary,
        "dynamic_cost_robustness_windows": robustness["robustness_windows"],
        "dynamic_cost_robustness_summary": robustness["robustness_summary"],
        "dynamic_cost_live_readiness_summary": live_readiness,
    }


def build_cost_stress_live_readiness_summary(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]],
    cost_bps_values: Sequence[float],
    variants: Sequence[GlobalEtfOffensiveVariantSpec] = GLOBAL_ETF_OFFENSIVE_VARIANTS,
    liveable_composites: Sequence[GlobalEtfLiveableCompositeSpec] = GLOBAL_ETF_LIVEABLE_COMPOSITES,
    robustness_candidates: Sequence[str] = DEFAULT_ROBUSTNESS_CANDIDATES,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for cost_bps in tuple(dict.fromkeys(float(value) for value in cost_bps_values)):
        result = run_offensive_research(
            price_history=price_history,
            periods=periods,
            variants=variants,
            liveable_composites=liveable_composites,
            turnover_cost_bps=float(cost_bps),
        )
        robustness = build_candidate_robustness_diagnostics(
            price_history=price_history,
            portfolio_returns=result["portfolio_returns"],
            weights_by_candidate=_weights_by_candidate_from_result(result),
            candidate_ids=robustness_candidates,
        )
        live_readiness = build_live_readiness_summary(
            period_summary=result["period_summary"],
            ranking=result["ranking"],
            robustness_windows=robustness["robustness_windows"],
        )
        if live_readiness.empty:
            continue
        live_readiness = live_readiness.copy()
        live_readiness.insert(0, "turnover_cost_bps", float(cost_bps))
        frames.append(live_readiness)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_portfolio_returns_with_benchmarks(
    *,
    price_history: pd.DataFrame,
    portfolio_returns: pd.DataFrame,
    benchmarks: Sequence[str] = (DEFAULT_SECONDARY_BENCHMARK, DEFAULT_PRIMARY_BENCHMARK),
) -> pd.DataFrame:
    portfolio = pd.DataFrame(portfolio_returns).copy()
    if portfolio.empty:
        return portfolio
    close = _normalize_price_history(price_history)
    benchmark_columns = tuple(dict.fromkeys(str(symbol).upper() for symbol in benchmarks if str(symbol).strip()))
    missing = [symbol for symbol in benchmark_columns if symbol not in close.columns]
    if missing:
        raise ValueError(f"price history missing benchmark symbols: {missing}")
    benchmark_returns = close.loc[:, list(benchmark_columns)].pct_change().reindex(portfolio.index)
    return pd.concat([portfolio, benchmark_returns], axis=1)


def _first_candidate(frame: pd.DataFrame) -> str:
    if frame.empty or "Candidate" not in frame.columns:
        return DEFAULT_LIVE_BASELINE_CANDIDATE
    return str(frame.iloc[0]["Candidate"])


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        return None if pd.isna(value) else value
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return value


def build_live_decision_summary(
    *,
    ranking: pd.DataFrame,
    period_summary: pd.DataFrame,
    live_readiness_summary: pd.DataFrame | None = None,
    cost_stress_summary: pd.DataFrame | None = None,
    dynamic_cost_summary: pd.DataFrame | None = None,
    walk_forward_summary: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Build a machine-readable live decision summary without changing runtime behavior."""
    live_readiness = pd.DataFrame(live_readiness_summary) if live_readiness_summary is not None else pd.DataFrame()
    cost_stress = pd.DataFrame(cost_stress_summary) if cost_stress_summary is not None else pd.DataFrame()
    dynamic_cost = pd.DataFrame(dynamic_cost_summary) if dynamic_cost_summary is not None else pd.DataFrame()
    walk_forward = pd.DataFrame(walk_forward_summary) if walk_forward_summary is not None else pd.DataFrame()
    live_ready = (
        live_readiness.loc[live_readiness["live_gate_passed"].astype(bool)].copy()
        if "live_gate_passed" in live_readiness.columns
        else pd.DataFrame()
    )
    live_mask = (
        ranking["live_review_candidate"].astype(bool)
        if "live_review_candidate" in ranking.columns
        else pd.Series(False, index=ranking.index)
    )
    live_candidates = ranking.loc[live_mask].copy()
    top_candidates = ranking.loc[ranking["paper_review_candidate"].astype(bool)].copy()
    decision_state = "hold_baseline"
    promotion_blockers: list[str] = []
    preferred_candidates: list[str] = []
    highest_passing_cost_assumptions: dict[str, object] | None = None
    highest_passing_nav_assumptions: dict[str, object] | None = None
    recommendation = "暂不迁移到 live；保留当前 defensive baseline，进攻型候选继续 paper review 或补充样本。"

    if not dynamic_cost.empty and "live_gate_passed" in dynamic_cost.columns:
        dynamic_cost_focus = dynamic_cost.copy()
        dynamic_nav_note = ""
        if "Estimated Portfolio NAV" in dynamic_cost_focus.columns:
            dynamic_cost_focus["Estimated Portfolio NAV"] = pd.to_numeric(
                dynamic_cost_focus["Estimated Portfolio NAV"], errors="coerce"
            )
            nav_values = sorted(float(value) for value in dynamic_cost_focus["Estimated Portfolio NAV"].dropna().unique())
            if len(nav_values) > 1:
                max_nav = max(nav_values)
                max_nav_rows = dynamic_cost_focus.loc[dynamic_cost_focus["Estimated Portfolio NAV"].eq(max_nav)].copy()
                max_nav_passed = max_nav_rows.loc[max_nav_rows["live_gate_passed"].astype(bool)].copy()
                if not max_nav_passed.empty:
                    dynamic_cost_focus = max_nav_rows
                    dynamic_nav_note = f"最高 NAV ${max_nav:,.0f} 压力下"
                else:
                    dynamic_passed_all = dynamic_cost_focus.loc[
                        dynamic_cost_focus["live_gate_passed"].astype(bool)
                    ].copy()
                    if not dynamic_passed_all.empty:
                        highest_pass_nav = float(dynamic_passed_all["Estimated Portfolio NAV"].max())
                        names = dynamic_passed_all.loc[
                            dynamic_passed_all["Estimated Portfolio NAV"].eq(highest_pass_nav)
                        ].head(3)["Candidate"].astype(str).tolist()
                        preferred_candidates = names
                        highest_passing_nav_assumptions = {"estimated_portfolio_nav": highest_pass_nav}
                        decision_state = "dynamic_cost_partial_pass"
                        promotion_blockers.append("dynamic_cost_nav_stress_not_fully_passed")
                        recommendation = (
                            f"动态成本 NAV 压力未全通过，不自动替换 live；最高通过 NAV ${highest_pass_nav:,.0f}，"
                            f"该资金规模下候选：{', '.join(names)}。需要先确认实盘账户规模和执行方式。"
                        )
                    else:
                        decision_state = "dynamic_cost_blocked"
                        promotion_blockers.append("dynamic_cost_live_gate_not_passed")
                        recommendation = (
                            "动态成本模型下无候选通过 live gate；暂不迁移到 live，继续研究成本/流动性假设。"
                        )
                    dynamic_cost_focus = pd.DataFrame()
        dynamic_passed = (
            dynamic_cost_focus.loc[dynamic_cost_focus["live_gate_passed"].astype(bool)].copy()
            if not dynamic_cost_focus.empty
            else pd.DataFrame()
        )
        if not dynamic_passed.empty:
            names = dynamic_passed.head(3)["Candidate"].astype(str).tolist()
            preferred_candidates = names
            decision_state = "live_promotion_review"
            prefix = dynamic_nav_note or "动态成本模型下"
            recommendation = f"{prefix}进入 live promotion review，但不自动替换 live；优先复核候选：{', '.join(names)}。"
        elif dynamic_cost_focus.empty and decision_state == "hold_baseline":
            pass
        elif decision_state == "hold_baseline":
            decision_state = "dynamic_cost_blocked"
            promotion_blockers.append("dynamic_cost_live_gate_not_passed")
            recommendation = "动态成本模型下无候选通过 live gate；暂不迁移到 live，继续研究成本/流动性假设。"
    elif not cost_stress.empty and "turnover_cost_bps" in cost_stress.columns:
        cost_stress["turnover_cost_bps"] = pd.to_numeric(cost_stress["turnover_cost_bps"], errors="coerce")
        costs = sorted(float(value) for value in cost_stress["turnover_cost_bps"].dropna().unique())
        passed = (
            cost_stress.loc[cost_stress["live_gate_passed"].astype(bool)].copy()
            if "live_gate_passed" in cost_stress.columns
            else pd.DataFrame()
        )
        max_cost = max(costs) if costs else float("nan")
        max_cost_passed = passed.loc[passed["turnover_cost_bps"].eq(max_cost)].copy() if not passed.empty else pd.DataFrame()
        if not max_cost_passed.empty:
            names = max_cost_passed.head(3)["Candidate"].astype(str).tolist()
            preferred_candidates = names
            decision_state = "live_promotion_review"
            highest_passing_cost_assumptions = {"turnover_cost_bps": max_cost}
            recommendation = (
                f"进入 live promotion review，但不自动替换 live；"
                f"在最高成本压力 {max_cost:.2f} bps 下优先复核候选：{', '.join(names)}。"
            )
        elif not passed.empty:
            highest_pass_cost = float(passed["turnover_cost_bps"].max())
            names = passed.loc[passed["turnover_cost_bps"].eq(highest_pass_cost)].head(3)["Candidate"].astype(str).tolist()
            preferred_candidates = names
            decision_state = "cost_partial_pass"
            promotion_blockers.append("cost_stress_not_fully_passed")
            highest_passing_cost_assumptions = {"turnover_cost_bps": highest_pass_cost}
            recommendation = (
                f"成本压力未全通过，不自动替换 live；最高通过成本 {highest_pass_cost:.2f} bps，"
                f"该成本下候选：{', '.join(names)}。需要先确认实盘成本假设。"
            )
        else:
            decision_state = "cost_blocked"
            promotion_blockers.append("cost_stress_live_gate_not_passed")
            recommendation = "成本压力下无候选通过 live gate；暂不迁移到 live，继续研究。"
    elif not live_ready.empty:
        names = live_ready.head(3)["Candidate"].astype(str).tolist()
        preferred_candidates = names
        decision_state = "live_promotion_review"
        recommendation = f"进入 live promotion review，但不自动替换 live；优先复核候选：{', '.join(names)}。"
    elif not live_candidates.empty:
        names = live_candidates.head(3)["Candidate"].astype(str).tolist()
        preferred_candidates = names
        decision_state = "live_design_review"
        promotion_blockers.append("baseline_relative_live_gate_not_passed")
        recommendation = (
            f"进入 live design review，但 baseline-relative live gate 未通过，不自动替换 live；优先复核候选：{', '.join(names)}。"
        )
    elif top_candidates.empty:
        decision_state = "hold_baseline"
        promotion_blockers.append("no_paper_review_candidate")
        recommendation = "暂不迁移到 live；保留当前 defensive baseline，进攻型候选继续 paper review 或补充样本。"
    else:
        names = top_candidates.head(3)["Candidate"].astype(str).tolist()
        preferred_candidates = names
        decision_state = "paper_review"
        recommendation = f"仅进入 paper review，不自动 live；优先复核候选：{', '.join(names)}。"

    if not walk_forward.empty and "walk_forward_gate_passed" in walk_forward.columns:
        walk_forward_focus = walk_forward.copy()
        if "Estimated Portfolio NAV" in walk_forward_focus.columns:
            walk_forward_focus["Estimated Portfolio NAV"] = pd.to_numeric(
                walk_forward_focus["Estimated Portfolio NAV"], errors="coerce"
            )
            nav_values = sorted(float(value) for value in walk_forward_focus["Estimated Portfolio NAV"].dropna().unique())
            if nav_values:
                walk_forward_focus = walk_forward_focus.loc[
                    walk_forward_focus["Estimated Portfolio NAV"].eq(max(nav_values))
                ].copy()
        walk_forward_passed = walk_forward_focus["walk_forward_gate_passed"].astype(bool)
        if not bool(walk_forward_passed.any()):
            reason = (
                str(walk_forward_focus["walk_forward_gate_reason"].iloc[0])
                if "walk_forward_gate_reason" in walk_forward_focus.columns and not walk_forward_focus.empty
                else "unknown"
            )
            decision_state = "walk_forward_blocked"
            promotion_blockers.append("walk_forward_gate_not_passed")
            recommendation = f"walk-forward/OOS gate 未通过，不自动替换 live；先保留当前 baseline。失败原因：{reason}。"

    return {
        "manifest_type": "global_etf_offensive_live_decision_summary",
        "decision_state": decision_state,
        "recommendation": recommendation,
        "defensive_baseline_candidate": DEFAULT_LIVE_BASELINE_CANDIDATE,
        "preferred_candidate": _first_candidate(
            pd.DataFrame({"Candidate": preferred_candidates}) if preferred_candidates else pd.DataFrame()
        ),
        "preferred_candidates": preferred_candidates,
        "promotion_blockers": promotion_blockers,
        "highest_passing_cost_assumptions": highest_passing_cost_assumptions,
        "highest_passing_nav_assumptions": highest_passing_nav_assumptions,
    }


def write_live_decision_summary(
    output_dir: Path,
    *,
    ranking: pd.DataFrame,
    period_summary: pd.DataFrame,
    live_readiness_summary: pd.DataFrame | None = None,
    cost_stress_summary: pd.DataFrame | None = None,
    dynamic_cost_summary: pd.DataFrame | None = None,
    walk_forward_summary: pd.DataFrame | None = None,
) -> Path:
    summary = build_live_decision_summary(
        ranking=ranking,
        period_summary=period_summary,
        live_readiness_summary=live_readiness_summary,
        cost_stress_summary=cost_stress_summary,
        dynamic_cost_summary=dynamic_cost_summary,
        walk_forward_summary=walk_forward_summary,
    )
    path = output_dir / "live_decision_summary.json"
    path.write_text(json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_recommendation(
    output_dir: Path,
    *,
    ranking: pd.DataFrame,
    period_summary: pd.DataFrame,
    live_readiness_summary: pd.DataFrame | None = None,
    cost_stress_summary: pd.DataFrame | None = None,
    dynamic_cost_summary: pd.DataFrame | None = None,
    walk_forward_summary: pd.DataFrame | None = None,
    liquidity_summary: pd.DataFrame | None = None,
) -> Path:
    path = output_dir / "recommendation.md"
    live_readiness = pd.DataFrame(live_readiness_summary) if live_readiness_summary is not None else pd.DataFrame()
    cost_stress = pd.DataFrame(cost_stress_summary) if cost_stress_summary is not None else pd.DataFrame()
    dynamic_cost = pd.DataFrame(dynamic_cost_summary) if dynamic_cost_summary is not None else pd.DataFrame()
    walk_forward = pd.DataFrame(walk_forward_summary) if walk_forward_summary is not None else pd.DataFrame()
    liquidity = pd.DataFrame(liquidity_summary) if liquidity_summary is not None else pd.DataFrame()
    decision_summary = build_live_decision_summary(
        ranking=ranking,
        period_summary=period_summary,
        live_readiness_summary=live_readiness_summary,
        cost_stress_summary=cost_stress_summary,
        dynamic_cost_summary=dynamic_cost_summary,
        walk_forward_summary=walk_forward_summary,
    )
    recommendation = str(decision_summary["recommendation"])

    baseline_rows = period_summary.loc[period_summary["Candidate"].eq("live_global_etf_rotation_defensive_baseline")]
    long_baseline = baseline_rows.loc[baseline_rows["Period"].eq("long")]
    baseline_text = ""
    if not long_baseline.empty:
        row = long_baseline.iloc[0]
        baseline_text = (
            f"- Baseline long CAGR: {float(row['CAGR']):.2%}; "
            f"SPY excess: {float(row['Excess CAGR vs Benchmark']):.2%}; "
            f"QQQ excess: {float(row['Excess CAGR vs Secondary Benchmark']):.2%}; "
            f"Max drawdown: {float(row['Max Drawdown']):.2%}\n"
        )

    lines = [
        "# Global ETF Offensive Rotation Research Recommendation",
        "",
        "## Recommendation",
        "",
        recommendation,
        "",
        "## Baseline Long-Window Snapshot",
        "",
        baseline_text or "- Baseline long-window row unavailable.\n",
        "## Ranking Preview",
        "",
        "```csv",
        ranking.head(10).to_csv(index=False).strip(),
        "```",
        "",
        "## Live Readiness Preview",
        "",
        "```csv",
        live_readiness.head(10).to_csv(index=False).strip() if not live_readiness.empty else "",
        "```",
        "",
        "## Cost Stress Preview",
        "",
        "```csv",
        cost_stress.head(20).to_csv(index=False).strip() if not cost_stress.empty else "",
        "```",
        "",
        "## Dynamic Cost Preview",
        "",
        "```csv",
        dynamic_cost.head(20).to_csv(index=False).strip() if not dynamic_cost.empty else "",
        "```",
        "",
        "## Walk-Forward Preview",
        "",
        "```csv",
        walk_forward.head(20).to_csv(index=False).strip() if not walk_forward.empty else "",
        "```",
        "",
        "## Liquidity Preview",
        "",
        "```csv",
        liquidity.head(10).to_csv(index=False).strip() if not liquidity.empty else "",
        "```",
        "",
        "## Boundary",
        "",
        "This is a research-only output. `live_design_review` and `live_promotion_review` only mean the "
        "deterministic rule is worth manual review; they do not change the live `global_etf_rotation` manifest "
        "or runtime behavior.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest research-only offensive Global ETF rotation variants against SPY and QQQ."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--prices", help="Existing long price-history CSV/JSON/Parquet with symbol/as_of/close columns"
    )
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end")
    parser.add_argument(
        "--periods", default=",".join(f"{name}:{start}:{end or ''}" for name, start, end in DEFAULT_PERIODS)
    )
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--symbols", help="Optional comma-separated override for downloaded symbols")
    parser.add_argument(
        "--variants",
        help=(
            "Optional comma-separated base variant IDs to evaluate. "
            "When omitted, all default base variants are evaluated."
        ),
    )
    parser.add_argument(
        "--liveable-composites",
        help=(
            "Optional comma-separated liveable composite IDs to evaluate. "
            "When omitted, all default liveable composites are evaluated."
        ),
    )
    parser.add_argument(
        "--robustness-candidates",
        default=",".join(DEFAULT_ROBUSTNESS_CANDIDATES),
        help="Comma-separated candidate IDs for calendar-year and rolling robustness diagnostics.",
    )
    parser.add_argument(
        "--walk-forward-candidates",
        default=",".join(DEFAULT_WALK_FORWARD_CANDIDATES),
        help="Comma-separated liveable candidate IDs for walk-forward promotion selection diagnostics.",
    )
    parser.add_argument("--walk-forward-train-years", type=int, default=DEFAULT_WALK_FORWARD_TRAIN_YEARS)
    parser.add_argument(
        "--walk-forward-min-train-excess-cagr",
        type=float,
        default=DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR,
        help="Minimum training-window CAGR excess versus baseline required before promoting a candidate OOS.",
    )
    parser.add_argument(
        "--cost-stress-bps",
        help=(
            "Optional comma-separated turnover cost bps values. When provided, writes "
            "cost_stress_live_readiness_summary.csv without emitting full per-cost weight files."
        ),
    )
    parser.add_argument("--liquidity-dollar-volume-window", type=int, default=DEFAULT_LIQUIDITY_DOLLAR_VOLUME_WINDOW)
    parser.add_argument("--low-liquidity-dollar-volume", type=float, default=DEFAULT_LOW_LIQUIDITY_DOLLAR_VOLUME)
    parser.add_argument(
        "--dynamic-cost",
        action="store_true",
        help="Write volume/liquidity-aware dynamic execution-cost diagnostics and live-readiness outputs.",
    )
    parser.add_argument(
        "--dynamic-cost-nav",
        type=float,
        default=0.0,
        help="Estimated portfolio NAV used for participation-rate cost penalties; 0 disables participation penalty.",
    )
    parser.add_argument(
        "--dynamic-cost-navs",
        help=(
            "Optional comma-separated NAV ladder for dynamic execution-cost stress. "
            "When provided, the dynamic live-readiness output contains one row set per NAV."
        ),
    )
    parser.add_argument("--dynamic-cost-base-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument(
        "--dynamic-cost-low-liquidity-penalty-bps",
        type=float,
        default=DEFAULT_DYNAMIC_COST_LOW_LIQUIDITY_PENALTY_BPS,
    )
    parser.add_argument(
        "--dynamic-cost-participation-penalty-bps",
        type=float,
        default=DEFAULT_DYNAMIC_COST_PARTICIPATION_PENALTY_BPS,
    )
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    periods = _parse_periods(args.periods)
    variants = _filter_variants(_normalize_candidate_ids(args.variants))
    liveable_composites = _filter_liveable_composites(_normalize_candidate_ids(args.liveable_composites))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        symbols = _normalize_symbols(args.symbols) or collect_required_symbols(variants=variants)
        prices = download_price_history(list(symbols), start=args.price_start, end=args.price_end)
        prices.to_csv(output_dir / "downloaded_price_history.csv", index=False)
    else:
        prices = read_table(args.prices)

    result = run_offensive_research(
        price_history=prices,
        periods=periods,
        variants=variants,
        liveable_composites=liveable_composites,
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    result["period_summary"].to_csv(output_dir / "period_summary.csv", index=False)
    result["ranking"].to_csv(output_dir / "ranking.csv", index=False)
    result["portfolio_returns"].to_csv(output_dir / "portfolio_returns.csv")
    build_portfolio_returns_with_benchmarks(
        price_history=prices,
        portfolio_returns=result["portfolio_returns"],
    ).to_csv(output_dir / "portfolio_returns_with_benchmarks.csv")
    result["signal_history"].to_csv(output_dir / "rebalance_events.csv", index=False)
    weights_by_candidate = _weights_by_candidate_from_result(result)
    robustness_candidates = _normalize_candidate_ids(args.robustness_candidates) or DEFAULT_ROBUSTNESS_CANDIDATES
    walk_forward_candidates = _normalize_candidate_ids(args.walk_forward_candidates) or DEFAULT_WALK_FORWARD_CANDIDATES
    robustness = build_candidate_robustness_diagnostics(
        price_history=prices,
        portfolio_returns=result["portfolio_returns"],
        weights_by_candidate=weights_by_candidate,
        candidate_ids=robustness_candidates,
    )
    robustness["robustness_windows"].to_csv(output_dir / "candidate_robustness_windows.csv", index=False)
    robustness["robustness_summary"].to_csv(output_dir / "candidate_robustness_summary.csv", index=False)
    live_readiness_summary = build_live_readiness_summary(
        period_summary=result["period_summary"],
        ranking=result["ranking"],
        robustness_windows=robustness["robustness_windows"],
    )
    live_readiness_summary.to_csv(output_dir / "live_readiness_summary.csv", index=False)
    walk_forward = build_walk_forward_selection_diagnostics(
        portfolio_returns=result["portfolio_returns"],
        weights_by_candidate=weights_by_candidate,
        candidate_ids=walk_forward_candidates,
        train_years=int(args.walk_forward_train_years),
        min_train_excess_cagr_vs_baseline=float(args.walk_forward_min_train_excess_cagr),
    )
    walk_forward["walk_forward_windows"].to_csv(output_dir / "walk_forward_selection_windows.csv", index=False)
    walk_forward["walk_forward_summary"].to_csv(output_dir / "walk_forward_selection_summary.csv", index=False)
    liquidity = build_candidate_liquidity_diagnostics(
        price_history=prices,
        weights_by_candidate=weights_by_candidate,
        candidate_ids=robustness_candidates,
        dollar_volume_window=int(args.liquidity_dollar_volume_window),
        low_liquidity_dollar_volume=float(args.low_liquidity_dollar_volume),
    )
    liquidity["liquidity_summary"].to_csv(output_dir / "candidate_liquidity_summary.csv", index=False)
    liquidity["liquidity_symbol_summary"].to_csv(output_dir / "candidate_liquidity_symbol_summary.csv", index=False)
    cost_stress_bps = _parse_float_list(args.cost_stress_bps)
    cost_stress = pd.DataFrame()
    if cost_stress_bps:
        cost_stress = build_cost_stress_live_readiness_summary(
            price_history=prices,
            periods=periods,
            cost_bps_values=cost_stress_bps,
            variants=variants,
            liveable_composites=liveable_composites,
            robustness_candidates=robustness_candidates,
        )
        cost_stress.to_csv(output_dir / "cost_stress_live_readiness_summary.csv", index=False)
    dynamic_cost_live_readiness = pd.DataFrame()
    dynamic_walk_forward_summary = pd.DataFrame()
    dynamic_cost_navs = _parse_float_list(args.dynamic_cost_navs)
    dynamic_enabled = bool(args.dynamic_cost) or bool(dynamic_cost_navs) or float(args.dynamic_cost_nav) > 0.0
    if dynamic_enabled:
        nav_values = dynamic_cost_navs or (float(args.dynamic_cost_nav),)
        live_frames: list[pd.DataFrame] = []
        cost_frames: list[pd.DataFrame] = []
        walk_forward_frames: list[pd.DataFrame] = []
        walk_forward_window_frames: list[pd.DataFrame] = []
        detailed_dynamic_cost: dict[str, pd.DataFrame] | None = None
        for nav in tuple(dict.fromkeys(float(value) for value in nav_values)):
            dynamic_config = DynamicExecutionCostConfig(
                base_cost_bps=float(args.dynamic_cost_base_bps),
                dollar_volume_window=int(args.liquidity_dollar_volume_window),
                low_liquidity_dollar_volume=float(args.low_liquidity_dollar_volume),
                low_liquidity_penalty_bps=float(args.dynamic_cost_low_liquidity_penalty_bps),
                estimated_portfolio_nav=float(nav),
                participation_penalty_bps=float(args.dynamic_cost_participation_penalty_bps),
            )
            dynamic_cost = build_dynamic_cost_live_readiness_diagnostics(
                price_history=prices,
                periods=periods,
                config=dynamic_config,
                variants=variants,
                liveable_composites=liveable_composites,
                robustness_candidates=robustness_candidates,
            )
            if detailed_dynamic_cost is None:
                detailed_dynamic_cost = dynamic_cost
            live_frame = pd.DataFrame(dynamic_cost["dynamic_cost_live_readiness_summary"])
            cost_frame = pd.DataFrame(dynamic_cost["dynamic_cost_summary"])
            if not live_frame.empty:
                live_frames.append(live_frame)
            if not cost_frame.empty:
                cost_frames.append(cost_frame)
            dynamic_walk_forward = build_walk_forward_selection_diagnostics(
                portfolio_returns=dynamic_cost["dynamic_cost_portfolio_returns"],
                weights_by_candidate=weights_by_candidate,
                candidate_ids=walk_forward_candidates,
                train_years=int(args.walk_forward_train_years),
                min_train_excess_cagr_vs_baseline=float(args.walk_forward_min_train_excess_cagr),
            )
            dynamic_wf_summary = pd.DataFrame(dynamic_walk_forward["walk_forward_summary"])
            dynamic_wf_windows = pd.DataFrame(dynamic_walk_forward["walk_forward_windows"])
            if not dynamic_wf_summary.empty:
                dynamic_wf_summary.insert(0, "Estimated Portfolio NAV", float(nav))
                walk_forward_frames.append(dynamic_wf_summary)
            if not dynamic_wf_windows.empty:
                dynamic_wf_windows.insert(0, "Estimated Portfolio NAV", float(nav))
                walk_forward_window_frames.append(dynamic_wf_windows)

        if detailed_dynamic_cost is not None:
            for key, value in detailed_dynamic_cost.items():
                pd.DataFrame(value).to_csv(
                    output_dir / f"{key}.csv",
                    index=str(key).endswith(("portfolio_returns", "daily_costs")),
                )
        dynamic_cost_live_readiness = pd.concat(live_frames, ignore_index=True) if live_frames else pd.DataFrame()
        dynamic_cost_summary = pd.concat(cost_frames, ignore_index=True) if cost_frames else pd.DataFrame()
        dynamic_walk_forward_summary = (
            pd.concat(walk_forward_frames, ignore_index=True) if walk_forward_frames else pd.DataFrame()
        )
        dynamic_walk_forward_windows = (
            pd.concat(walk_forward_window_frames, ignore_index=True) if walk_forward_window_frames else pd.DataFrame()
        )
        dynamic_cost_live_readiness.to_csv(
            output_dir / "dynamic_cost_nav_stress_live_readiness_summary.csv", index=False
        )
        dynamic_cost_summary.to_csv(output_dir / "dynamic_cost_nav_stress_summary.csv", index=False)
        dynamic_walk_forward_summary.to_csv(
            output_dir / "dynamic_cost_nav_stress_walk_forward_summary.csv", index=False
        )
        dynamic_walk_forward_windows.to_csv(
            output_dir / "dynamic_cost_nav_stress_walk_forward_windows.csv", index=False
        )
    for key, value in result.items():
        if key.startswith("weights_"):
            pd.DataFrame(value).to_csv(output_dir / f"{key}.csv")
    write_recommendation(
        output_dir,
        ranking=result["ranking"],
        period_summary=result["period_summary"],
        live_readiness_summary=live_readiness_summary,
        cost_stress_summary=cost_stress,
        dynamic_cost_summary=dynamic_cost_live_readiness,
        walk_forward_summary=dynamic_walk_forward_summary
        if not dynamic_walk_forward_summary.empty
        else walk_forward["walk_forward_summary"],
        liquidity_summary=liquidity["liquidity_summary"],
    )
    write_live_decision_summary(
        output_dir,
        ranking=result["ranking"],
        period_summary=result["period_summary"],
        live_readiness_summary=live_readiness_summary,
        cost_stress_summary=cost_stress,
        dynamic_cost_summary=dynamic_cost_live_readiness,
        walk_forward_summary=dynamic_walk_forward_summary
        if not dynamic_walk_forward_summary.empty
        else walk_forward["walk_forward_summary"],
    )
    manifest = {
        "research": "global_etf_offensive_rotation",
        "periods": [{"name": name, "start": start, "end": end} for name, start, end in periods],
        "turnover_cost_bps": float(args.turnover_cost_bps),
        "dynamic_cost_enabled": dynamic_enabled,
        "dynamic_cost_nav": float(args.dynamic_cost_nav),
        "dynamic_cost_navs": list(dynamic_cost_navs),
        "walk_forward_train_years": int(args.walk_forward_train_years),
        "walk_forward_min_train_excess_cagr": float(args.walk_forward_min_train_excess_cagr),
        "variant_candidates": [variant.candidate_id for variant in variants],
        "liveable_composite_candidates": [spec.candidate_id for spec in liveable_composites],
        "walk_forward_candidates": list(walk_forward_candidates),
        "variants": [asdict(variant) for variant in GLOBAL_ETF_OFFENSIVE_VARIANTS],
        "liveable_composites": [asdict(spec) for spec in GLOBAL_ETF_LIVEABLE_COMPOSITES],
        "outputs": sorted(path.name for path in output_dir.iterdir() if path.is_file()),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(result["ranking"].to_string(index=False))
    print(f"wrote global ETF offensive research outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
