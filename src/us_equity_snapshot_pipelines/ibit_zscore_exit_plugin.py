from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from quant_strategy_plugins.artifacts import write_json
from quant_strategy_plugins.crisis_response_shadow_plugin import flatten_for_csv


PLUGIN_IBIT_ZSCORE_EXIT = "ibit_zscore_exit"
IBIT_ZSCORE_EXIT_SCHEMA_VERSION = "ibit_zscore_exit.v1"
THRESHOLD_MODE_ROLLING_PERCENTILE_HYBRID = "rolling_percentile_hybrid"
ROUTE_NORMAL = "normal"
ROUTE_RISK_REDUCED = "risk_reduced"
ROUTE_RISK_OFF = "risk_off"


def _coerce_float(value: object, *, default: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(result):
        return default
    return float(result)


def _coerce_int(value: object, *, default: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    return max(1, int(result))


def _normalize_symbol(value: object, *, default: str) -> str:
    symbol = str(value or "").strip().upper().removesuffix(".US")
    return symbol or default


def _zscore_column(frame: pd.DataFrame) -> str:
    normalized = {str(column).strip().lower(): column for column in frame.columns}
    for name in ("mvrv_zscore", "mvrv_z_score", "zscore", "z_score"):
        if name in normalized:
            return str(normalized[name])
    raise ValueError("ibit_zscore_exit requires a zscore column")


def _date_column(frame: pd.DataFrame) -> str:
    normalized = {str(column).strip().lower(): column for column in frame.columns}
    for name in ("as_of", "date", "timestamp"):
        if name in normalized:
            return str(normalized[name])
    raise ValueError("ibit_zscore_exit requires an as_of/date column")


def _prepare_zscore_history(frame: pd.DataFrame, *, as_of: object | None) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("ibit_zscore_exit requires non-empty zscore history")
    date_column = _date_column(frame)
    zscore_column = _zscore_column(frame)
    prepared = frame[[date_column, zscore_column]].copy()
    prepared.columns = ["as_of", "mvrv_zscore"]
    prepared["as_of"] = pd.to_datetime(prepared["as_of"], errors="coerce")
    prepared["mvrv_zscore"] = pd.to_numeric(prepared["mvrv_zscore"], errors="coerce")
    prepared = prepared.dropna(subset=["as_of", "mvrv_zscore"])
    prepared = prepared.sort_values("as_of")
    if as_of:
        cutoff = pd.Timestamp(as_of)
        prepared = prepared[prepared["as_of"] <= cutoff]
    if prepared.empty:
        raise ValueError("ibit_zscore_exit has no valid zscore rows at or before as_of")
    return prepared


def _threshold_from_history(
    history: pd.Series,
    *,
    percentile: float,
    floor: float,
    cap: float,
    fallback: float,
    min_periods: int,
) -> tuple[float, str]:
    values = pd.to_numeric(history, errors="coerce").dropna()
    if len(values) < min_periods:
        return float(fallback), "fallback_static"
    threshold = float(values.quantile(max(0.0, min(1.0, percentile))))
    threshold = max(float(floor), threshold)
    threshold = min(float(cap), threshold)
    return float(threshold), "rolling_percentile"


def build_ibit_zscore_exit_signal(zscore_history: pd.DataFrame, plugin_config: Mapping[str, Any]) -> dict[str, Any]:
    """Build a dynamic MVRV Z-Score exit/parking signal for IBIT Smart DCA."""

    prepared = _prepare_zscore_history(zscore_history, as_of=plugin_config.get("as_of"))
    latest = prepared.iloc[-1]
    as_of = pd.Timestamp(latest["as_of"]).date().isoformat()
    latest_zscore = float(latest["mvrv_zscore"])

    lookback_days = _coerce_int(plugin_config.get("dynamic_lookback_days"), default=1460)
    min_periods = _coerce_int(plugin_config.get("dynamic_min_periods"), default=365)
    soft_percentile = _coerce_float(plugin_config.get("soft_exit_percentile"), default=0.95)
    hard_percentile = _coerce_float(plugin_config.get("hard_exit_percentile"), default=0.985)
    soft_floor = _coerce_float(plugin_config.get("soft_exit_zscore_floor"), default=5.0)
    hard_floor = _coerce_float(plugin_config.get("hard_exit_zscore_floor"), default=7.0)
    soft_cap = _coerce_float(plugin_config.get("soft_exit_zscore_cap"), default=8.0)
    hard_cap = _coerce_float(plugin_config.get("hard_exit_zscore_cap"), default=10.0)
    soft_fallback = _coerce_float(plugin_config.get("soft_exit_zscore_fallback"), default=7.0)
    hard_fallback = _coerce_float(plugin_config.get("hard_exit_zscore_fallback"), default=9.0)
    risk_reduced_exposure = max(
        0.0,
        min(1.0, _coerce_float(plugin_config.get("risk_reduced_ibit_exposure"), default=0.50)),
    )
    risk_off_exposure = max(
        0.0,
        min(1.0, _coerce_float(plugin_config.get("risk_off_ibit_exposure"), default=0.25)),
    )
    parking_symbol = _normalize_symbol(plugin_config.get("parking_symbol"), default="BOXX")

    history_cutoff = pd.Timestamp(latest["as_of"]) - pd.Timedelta(days=lookback_days)
    threshold_history = prepared[
        (prepared["as_of"] < pd.Timestamp(latest["as_of"])) & (prepared["as_of"] >= history_cutoff)
    ]["mvrv_zscore"]
    soft_threshold, soft_source = _threshold_from_history(
        threshold_history,
        percentile=soft_percentile,
        floor=soft_floor,
        cap=soft_cap,
        fallback=soft_fallback,
        min_periods=min_periods,
    )
    hard_threshold, hard_source = _threshold_from_history(
        threshold_history,
        percentile=hard_percentile,
        floor=hard_floor,
        cap=hard_cap,
        fallback=hard_fallback,
        min_periods=min_periods,
    )
    if hard_threshold < soft_threshold:
        hard_threshold = soft_threshold

    if latest_zscore >= hard_threshold:
        route = ROUTE_RISK_OFF
        suggested_action = "defend"
        target_ibit_exposure = risk_off_exposure
        reason_codes = ("mvrv_zscore_above_dynamic_hard_exit",)
    elif latest_zscore >= soft_threshold:
        route = ROUTE_RISK_REDUCED
        suggested_action = "delever"
        target_ibit_exposure = risk_reduced_exposure
        reason_codes = ("mvrv_zscore_above_dynamic_soft_exit",)
    else:
        route = ROUTE_NORMAL
        suggested_action = "no_action"
        target_ibit_exposure = 1.0
        reason_codes = ()
    target_parking_exposure = 1.0 - target_ibit_exposure

    return {
        "schema_version": IBIT_ZSCORE_EXIT_SCHEMA_VERSION,
        "as_of": as_of,
        "plugin": PLUGIN_IBIT_ZSCORE_EXIT,
        "canonical_route": route,
        "suggested_action": suggested_action,
        "would_trade_if_enabled": route != ROUTE_NORMAL,
        "metrics": {
            "mvrv_zscore": latest_zscore,
            "zscore_history_rows": int(len(prepared)),
            "threshold_history_rows": int(len(threshold_history)),
        },
        "thresholds": {
            "threshold_mode": THRESHOLD_MODE_ROLLING_PERCENTILE_HYBRID,
            "dynamic_lookback_days": int(lookback_days),
            "dynamic_min_periods": int(min_periods),
            "soft_exit_percentile": float(soft_percentile),
            "hard_exit_percentile": float(hard_percentile),
            "soft_exit_zscore": float(soft_threshold),
            "hard_exit_zscore": float(hard_threshold),
            "soft_exit_source": soft_source,
            "hard_exit_source": hard_source,
        },
        "position_control": {
            "final_route": route,
            "route_source": THRESHOLD_MODE_ROLLING_PERCENTILE_HYBRID,
            "suggested_action": suggested_action,
            "parking_symbol": parking_symbol,
            "target_ibit_exposure": float(target_ibit_exposure),
            "target_parking_exposure": float(target_parking_exposure),
            "target_allocations": {
                "IBIT": float(target_ibit_exposure),
                parking_symbol: float(target_parking_exposure),
            },
            "reason_codes": reason_codes,
        },
        "reason_codes": reason_codes,
    }


def write_ibit_zscore_exit_outputs(payload: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
    output_root = Path(output_dir)
    signal_date = str(payload["as_of"])
    signal_dir = output_root / "signals"
    audit_dir = output_root / "audit"
    latest_path = output_root / "latest_signal.json"
    dated_json_path = signal_dir / f"{signal_date}.json"
    dated_csv_path = signal_dir / f"{signal_date}.csv"
    evidence_csv_path = audit_dir / f"{signal_date}_evidence.csv"

    write_json(latest_path, payload)
    write_json(dated_json_path, payload)
    signal_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([flatten_for_csv(payload)]).to_csv(dated_csv_path, index=False)
    pd.DataFrame(
        [
            {
                "as_of": payload.get("as_of"),
                "canonical_route": payload.get("canonical_route"),
                "suggested_action": payload.get("suggested_action"),
                **flatten_for_csv(payload.get("metrics", {})),
                **flatten_for_csv(payload.get("thresholds", {})),
                **flatten_for_csv(payload.get("position_control", {})),
            }
        ]
    ).to_csv(evidence_csv_path, index=False)
    return {
        "latest_signal": latest_path,
        "signal_json": dated_json_path,
        "signal_csv": dated_csv_path,
        "evidence_csv": evidence_csv_path,
    }


__all__ = [
    "IBIT_ZSCORE_EXIT_SCHEMA_VERSION",
    "PLUGIN_IBIT_ZSCORE_EXIT",
    "build_ibit_zscore_exit_signal",
    "write_ibit_zscore_exit_outputs",
]
