"""Fixed paired-log endpoint for the proposed B1/B2 synthetic replay."""
from math import fsum, isclose, isfinite, log, sqrt
from statistics import stdev
import numpy as np
from quant_platform_kit.research_stats.unified_report import build_unified_net_report

PRIMARY_FEES = (2.0, 5.0)
PRIMARY_PAIRS = (("B1.4", "B1.2"), ("B2.2", "B2.1"))


def _session_window(expected_sessions):
    sessions = tuple(expected_sessions)
    if not sessions or any(b <= a for a, b in zip(sessions, sessions[1:])):
        raise ValueError("EXPECTED_SESSIONS_INVALID")
    return sessions


def validate_net_path(path, sessions):
    if tuple(row["fill"] for row in path) != sessions:
        raise ValueError("PAIRED_SESSIONS_NOT_ALIGNED")
    last_nav = path[0]["start"]
    for row in path:
        start, end = row["start"], row["nav"]
        if any(not isfinite(value) or value <= 0 for value in (start, end)):
            raise ValueError("NET_NAV_INVALID")
        if start != last_nav:
            raise ValueError("NET_NAV_DISCONTINUITY")
        last_nav = end


def paired_log_differences(candidate, control, expected_sessions, *, inherited_opening_capital=False):
    sessions = _session_window(expected_sessions)
    for path in (candidate, control):
        validate_net_path(path, sessions)
    if not inherited_opening_capital and candidate[0]["start"] != control[0]["start"]:
        raise ValueError("PAIRED_INITIAL_NAV_MISMATCH")
    return [log(a["nav"] / a["start"]) - log(b["nav"] / b["start"])
            for a, b in zip(candidate, control, strict=True)]


def net_path_report(path, expected_sessions, *, cash_by_session=None, cash_source=None):
    """Use actual post-cost NAV; keep missing inputs uncomputable in the QPK report."""
    sessions = _session_window(expected_sessions)
    validate_net_path(path, sessions)
    if cash_by_session is not None and set(cash_by_session) != set(sessions):
        raise ValueError("CASH_SESSIONS_NOT_ALIGNED")
    net_returns = [row["nav"] / row["start"] - 1 for row in path]
    execution = _execution_metrics(path)
    # Keep the realized book and remove only this session's execution costs.
    gross_returns = ([row["prefill"] / row["start"] - 1 for row in path]
                     if execution["status"] == "COMPUTED" else None)
    report = build_unified_net_report(
        net_returns,
        gross_returns=gross_returns,
        periods_per_year=252,
        sharpe_basis="net_wealth_excess_sharpe_v2",
        risk_free_returns=[cash_by_session[day] for day in sessions] if cash_by_session is not None else None,
        risk_free_source=cash_source,
        calendar_id="XNYS",
    ).to_dict()
    report["cumulative_net_return"] = path[-1]["nav"] / path[0]["start"] - 1
    report["annualized_net_volatility"] = stdev(net_returns) * sqrt(252) if len(net_returns) > 1 else None
    report["execution"] = execution
    return report


def _execution_metrics(path):
    fields = ("turnover_one_side", "commission_usd", "slippage_usd", "total_cost_usd")
    if any(not {"orders", "prefill", "cost"} <= row.keys() for row in path):
        return {**dict.fromkeys(fields), "status": "UNCOMPUTABLE", "reason_code": "FILL_LEDGER_MISSING"}
    turnover, commissions, slippages = [], [], []
    for row in path:
        prefill = row["prefill"]
        if not isfinite(prefill) or prefill <= 0:
            raise ValueError("PREFILL_NAV_INVALID")
        notional, commission, slippage = [], [], []
        for order in row["orders"]:
            amounts = (order["notional"], order["commission"], order["slippage"])
            if any(not isfinite(value) or value < 0 for value in amounts):
                raise ValueError("FILL_AMOUNTS_INVALID")
            notional.append(amounts[0])
            commission.append(amounts[1])
            slippage.append(amounts[2])
        daily_commission, daily_slippage = fsum(commission), fsum(slippage)
        daily_cost = daily_commission + daily_slippage
        if not (isclose(row["cost"], daily_cost, rel_tol=0, abs_tol=1e-7)
                and isclose(prefill - row["nav"], daily_cost, rel_tol=0, abs_tol=1e-7)):
            raise ValueError("FILL_COST_RECONCILIATION_FAILED")
        turnover.append(fsum(notional) / prefill)
        commissions.append(daily_commission)
        slippages.append(daily_slippage)
    return {
        "status": "COMPUTED", "reason_code": None,
        "turnover_one_side": fsum(turnover), "turnover_basis": "sum_daily_abs_notional_over_prefill_nav",
        "commission_usd": fsum(commissions), "slippage_usd": fsum(slippages),
        "total_cost_usd": fsum(commissions) + fsum(slippages),
    }


def bootstrap_summary(differences):
    values = np.asarray(differences, dtype=float)
    if not len(values) or not np.all(np.isfinite(values)):
        raise ValueError("PAIRED_DIFFERENCES_INVALID")
    summary = {"mean_daily_net_log_delta": float(np.mean(values)), "ci95": None, "ci98_75": None}
    if len(values) < 63:
        return summary
    block = 20
    rng = np.random.default_rng(20261108)
    starts = rng.integers(0, len(values), size=(10000, (len(values) + block - 1) // block))
    positions = (starts[:, :, None] + np.arange(block)) % len(values)
    resampled = values[positions.reshape(10000, -1)[:, :len(values)]].mean(axis=1)
    summary["ci95"] = tuple(map(float, np.quantile(resampled, [0.025, 0.975])))
    summary["ci98_75"] = tuple(map(float, np.quantile(resampled, [0.00625, 0.99375])))
    return summary


def primary_pair_summary(paths, expected_sessions, *, inherited_opening_capital=False, profiles=("TQQQ", "SOXL")):
    """Use registered main-fee paths; inherited-capital tails are descriptive."""
    sessions = _session_window(expected_sessions)
    if inherited_opening_capital and len(sessions) >= 63:
        raise ValueError("DESCRIPTIVE_TAIL_TOO_LONG")
    results = []
    for profile in profiles:
        for candidate, control in PRIMARY_PAIRS:
            row = {
                "profile": profile, "candidate": candidate, "control": control,
                "fee_bps": sum(PRIMARY_FEES), "positive_increment_supported": False,
                "status": "PARKED", "mean_daily_net_log_delta": None,
                "ci95": None, "ci98_75": None,
            }
            results.append(row)
            candidate_path = paths.get((profile, candidate, PRIMARY_FEES))
            control_path = paths.get((profile, control, PRIMARY_FEES))
            if candidate_path is None or control_path is None:
                row["reason_code"] = "PAIRED_PATH_MISSING"
                continue
            try:
                differences = paired_log_differences(
                    candidate_path, control_path, sessions, inherited_opening_capital=inherited_opening_capital,
                )
            except ValueError as exc:
                row["reason_code"] = str(exc)
                continue
            row.update(bootstrap_summary(differences))
            corrected = row["ci98_75"]
            row["status"] = "COMPUTED" if corrected is not None else "DESCRIPTIVE_ONLY"
            row["reason_code"] = None if corrected is not None else "INSUFFICIENT_OBSERVATIONS"
            row["positive_increment_supported"] = corrected is not None and corrected[0] > 0
    return results
