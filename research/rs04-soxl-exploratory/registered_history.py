"""Fixed historical-development entry for the registered B1/B2 archive."""
from datetime import date

from input_qualification import PROFILE_SYMBOLS, select_profiles
from inspect_registered_archive import MANIFEST_SHA256, registered_calendar
from replay import FOLD_SESSIONS, PURGE_SESSIONS, TRAIN_SESSIONS
from risk_observations import DRAWDOWN_LIMIT
from study_matrix import FEE_SCHEDULES, PRIMARY_FEES, PROFILES, evaluate_study_matrix, prepare_replay_inputs
from us_equity_strategies.research.rs04_b3_config import CONFIG


DEVELOPMENT_START = date(2023, 3, 28)
REGISTERED_CAPS = {
    "TQQQ": {"total_nominal": CONFIG.tqqq_total, "drawdown": DRAWDOWN_LIMIT},
    "SOXL": {"total_nominal": CONFIG.soxl_total, "SOXL_nominal": CONFIG.soxl_single, "drawdown": DRAWDOWN_LIMIT},
}
REGISTERED_COST_POLICY = {
    "basis": "one_way_per_security_net_traded_notional_including_boxx",
    "components": ("commission_bps", "slippage_including_spread_bps"),
    "primary": PRIMARY_FEES,
    "report_only": tuple(fees for fees in FEE_SCHEDULES if fees != PRIMARY_FEES),
    "fund_expenses": "embedded_in_etf_returns",
    "financing": "no_borrowing",
}


def registered_windows(sessions):
    development = tuple(day for day in sessions if day >= DEVELOPMENT_START)
    scored = development[TRAIN_SESSIONS + PURGE_SESSIONS:]
    if len(development) != 856 or len(scored) != 9 * FOLD_SESSIONS + 32:
        raise ValueError("REGISTERED_HISTORY_WINDOW_MISMATCH")
    main, tail = scored[:9 * FOLD_SESSIONS], scored[9 * FOLD_SESSIONS:]
    if (development[0], main[0], main[-1], tail[-1]) != (
        DEVELOPMENT_START, date(2024, 4, 5), date(2026, 7, 10), date(2026, 8, 25),
    ):
        raise ValueError("REGISTERED_HISTORY_WINDOW_MISMATCH")
    return main, tail


def evaluate_registered_history(source, *, effective_core_configs, input_bindings, confirmed_research_caps,
                                cash_source, confirmed_cost_policy=None, observe_checkpoint=None, resume=None,
                                profiles=PROFILES):
    """Use bound inputs in the approved environment; no data or run authority is inferred."""
    if source.receipt.get("manifest_sha256") != MANIFEST_SHA256:
        raise ValueError("REGISTERED_HISTORY_MANIFEST_MISMATCH")
    selected = select_profiles(profiles)
    if confirmed_research_caps != {profile: REGISTERED_CAPS[profile] for profile in selected}:
        raise ValueError("REGISTERED_RESEARCH_CAPS_UNCONFIRMED_OR_DIFFERENT")
    if confirmed_cost_policy != REGISTERED_COST_POLICY:
        raise ValueError("REGISTERED_COST_POLICY_UNCONFIRMED_OR_DIFFERENT")
    if set(effective_core_configs) != set(selected) or set(input_bindings) != set(selected):
        raise ValueError("REGISTERED_PROFILES_REQUIRED")
    if selected != PROFILES:
        symbols = set().union(*(PROFILE_SYMBOLS[profile] for profile in selected))
        if (set(source.receipt.get("qualified_symbols", ())) != symbols
                or set(source.core_sessions) != set(selected)):
            raise ValueError("REGISTERED_HISTORY_PROFILE_MEMBERS_MISMATCH")
    expected, calendar_version = registered_calendar()
    if source.sessions != expected["BOXX"] or any(
        source.core_sessions.get(profile) != expected[{"TQQQ": "QQQ", "SOXL": "SOXX"}[profile]]
        for profile in selected
    ):
        raise ValueError("REGISTERED_HISTORY_CALENDAR_MISMATCH")
    main, tail = registered_windows(source.core_sessions[selected[0]])
    inputs = {
        profile: prepare_replay_inputs(
            profile, source, effective_core_config=effective_core_configs[profile],
            trade_start=DEVELOPMENT_START, observation_start=DEVELOPMENT_START,
            cash_source=cash_source, input_binding=input_bindings[profile],
        ) for profile in selected
    }
    result = evaluate_study_matrix(
        inputs, main_sessions=main, tail_sessions=tail, observe_checkpoint=observe_checkpoint, resume=resume,
        selected_profiles=selected,
    )
    result["registered_history"] = {
        "manifest_sha256": MANIFEST_SHA256, "calendar_version": calendar_version,
        "development_start": DEVELOPMENT_START, "main_sessions": len(main), "tail_sessions": len(tail),
        "cost_policy": dict(REGISTERED_COST_POLICY),
        "profiles": selected,
    }
    return result
