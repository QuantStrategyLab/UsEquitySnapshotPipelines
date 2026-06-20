from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .artifacts import sha256_file, write_json
from .mega_cap_leader_rotation_stress_readiness import parse_csv_floats_no_percent
from .russell_1000_multi_factor_defensive_snapshot import read_table

CAPACITY_STRESS_SCHEMA_VERSION = "russell_top50_capacity_stress.v1"
DIAGNOSTIC_SCOPE = "capacity_and_implementation_shortfall_stress_not_live_gate"
DEFAULT_NAV_VALUES = (1_000_000.0, 5_000_000.0, 10_000_000.0, 25_000_000.0)
DEFAULT_SLIPPAGE_BPS_VALUES = (5.0, 10.0, 25.0, 50.0)
DEFAULT_SPLIT_TRADE_DAYS_VALUES = (1, 2, 3)
DEFAULT_MIN_MEDIAN_NET_EXCESS_VS_QQQ = 0.0
DETAIL_COLUMNS = (
    "Date",
    "Run",
    "Variant Type",
    "Portfolio NAV",
    "Slippage Bps",
    "Split Trade Days",
    "Trade Count",
    "Gross Turnover Weight",
    "One Way Turnover Weight",
    "Gross Trade Notional",
    "Daily Split Trade Notional",
    "Estimated Slippage Cost",
    "Slippage Return Drag",
    "Forward Window Trading Days",
    "Forward Strategy Return",
    "Forward QQQ Return",
    "Forward SPY Return",
    "Net Forward Strategy Return",
    "Net Forward Excess Return vs QQQ",
    "Net Forward Excess Return vs SPY",
    "Base Liquidity NAV",
    "Base Max Participation Rate",
    "Allowed Max Participation Rate",
    "Estimated Max Participation Rate",
    "participation_gate_passed",
    "participation_gate_reason",
    "diagnostic_scope",
)
SUMMARY_COLUMNS = (
    "Run",
    "Variant Type",
    "Portfolio NAV",
    "Slippage Bps",
    "Split Trade Days",
    "Rebalance Count",
    "Total Gross Trade Notional",
    "Max Daily Split Trade Notional",
    "Total Estimated Slippage Cost",
    "Median Slippage Return Drag",
    "Median Net Forward Excess Return vs QQQ",
    "Worst Net Forward Excess Return vs QQQ",
    "Median Net Forward Excess Return vs SPY",
    "Worst Net Forward Excess Return vs SPY",
    "Max Estimated Participation Rate",
    "Allowed Max Participation Rate",
    "Participation Gate Passed",
    "capacity_stress_passed",
    "capacity_stress_reason",
    "recommended_action",
    "diagnostic_scope",
)


def _parse_csv_ints(raw_value: str | Iterable[int] | None, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed: list[int] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        parsed.append(int(float(text)))
    return tuple(value for value in parsed if value > 0) or default


def _prepare_shadow_summary(frame: pd.DataFrame) -> pd.DataFrame:
    output = pd.DataFrame(frame).copy()
    required = {
        "Date",
        "Run",
        "Variant Type",
        "Gross Turnover Weight",
        "One Way Turnover Weight",
        "Forward Strategy Return",
        "Forward QQQ Return",
        "Forward SPY Return",
        "Forward Excess Return vs QQQ",
        "Forward Excess Return vs SPY",
    }
    missing = sorted(required.difference(output.columns))
    if missing:
        raise ValueError(f"shadow_live_rebalance_summary must include columns: {', '.join(missing)}")
    output["Date"] = pd.to_datetime(output["Date"], errors="coerce").dt.tz_localize(None)
    output["Run"] = output["Run"].astype(str)
    output["Variant Type"] = output["Variant Type"].astype(str)
    for column in (
        "Portfolio NAV",
        "Trade Count",
        "Gross Turnover Weight",
        "One Way Turnover Weight",
        "Forward Window Trading Days",
        "Forward Strategy Return",
        "Forward QQQ Return",
        "Forward SPY Return",
        "Forward Excess Return vs QQQ",
        "Forward Excess Return vs SPY",
    ):
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
    output = output.dropna(subset=["Date", "Run", "Gross Turnover Weight"])
    if output.empty:
        raise ValueError("shadow_live_rebalance_summary has no usable rows")
    return output.sort_values(["Run", "Date"], kind="stable").reset_index(drop=True)


def _prepare_liquidity_summary(liquidity_summary: pd.DataFrame | None) -> pd.DataFrame:
    if liquidity_summary is None:
        return pd.DataFrame(columns=["Run", "Base Liquidity NAV", "Base Max Participation Rate", "Allowed Max Participation Rate"])
    frame = pd.DataFrame(liquidity_summary).copy()
    if frame.empty:
        return pd.DataFrame(columns=["Run", "Base Liquidity NAV", "Base Max Participation Rate", "Allowed Max Participation Rate"])
    required = {"Run", "Portfolio NAV", "Max Participation Rate"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"liquidity_summary must include columns: {', '.join(missing)}")
    frame["Run"] = frame["Run"].astype(str)
    frame["Base Liquidity NAV"] = pd.to_numeric(frame["Portfolio NAV"], errors="coerce")
    frame["Base Max Participation Rate"] = pd.to_numeric(frame["Max Participation Rate"], errors="coerce")
    if "Allowed Max Participation Rate" in frame.columns:
        frame["Allowed Max Participation Rate"] = pd.to_numeric(frame["Allowed Max Participation Rate"], errors="coerce")
    else:
        frame["Allowed Max Participation Rate"] = np.nan
    frame = frame.dropna(subset=["Run", "Base Liquidity NAV", "Base Max Participation Rate"])
    if frame.empty:
        return pd.DataFrame(columns=["Run", "Base Liquidity NAV", "Base Max Participation Rate", "Allowed Max Participation Rate"])
    # Prefer the smallest base NAV for conservative scaling when multiple liquidity rows exist.
    return (
        frame.sort_values(["Run", "Base Liquidity NAV"], kind="stable")
        .groupby("Run", as_index=False)
        .first()
        .loc[:, ["Run", "Base Liquidity NAV", "Base Max Participation Rate", "Allowed Max Participation Rate"]]
    )


def _participation_context(
    liquidity_by_run: Mapping[str, dict[str, float]],
    *,
    run: str,
    portfolio_nav: float,
    split_trade_days: int,
) -> dict[str, object]:
    context = liquidity_by_run.get(str(run))
    if not context:
        return {
            "Base Liquidity NAV": np.nan,
            "Base Max Participation Rate": np.nan,
            "Allowed Max Participation Rate": np.nan,
            "Estimated Max Participation Rate": np.nan,
            "participation_gate_passed": False,
            "participation_gate_reason": "missing_liquidity_context",
        }
    base_nav = float(context.get("Base Liquidity NAV", np.nan))
    base_rate = float(context.get("Base Max Participation Rate", np.nan))
    allowed = float(context.get("Allowed Max Participation Rate", np.nan))
    if pd.isna(base_nav) or base_nav <= 0.0 or pd.isna(base_rate):
        estimated = np.nan
        passed = False
        reason = "invalid_liquidity_context"
    else:
        estimated = base_rate * float(portfolio_nav) / base_nav / max(int(split_trade_days), 1)
        if pd.isna(allowed):
            passed = False
            reason = "missing_allowed_participation_rate"
        elif estimated <= allowed:
            passed = True
            reason = "pass"
        else:
            passed = False
            reason = "participation_rate_above_limit"
    return {
        "Base Liquidity NAV": base_nav,
        "Base Max Participation Rate": base_rate,
        "Allowed Max Participation Rate": allowed,
        "Estimated Max Participation Rate": estimated,
        "participation_gate_passed": passed,
        "participation_gate_reason": reason,
    }


def _build_detail(
    shadow_summary: pd.DataFrame,
    liquidity_summary: pd.DataFrame,
    *,
    portfolio_nav_values: Iterable[float],
    slippage_bps_values: Iterable[float],
    split_trade_days_values: Iterable[int],
) -> pd.DataFrame:
    liquidity_by_run = {
        str(row["Run"]): {
            "Base Liquidity NAV": float(row["Base Liquidity NAV"]),
            "Base Max Participation Rate": float(row["Base Max Participation Rate"]),
            "Allowed Max Participation Rate": float(row["Allowed Max Participation Rate"]),
        }
        for _, row in liquidity_summary.iterrows()
    }
    rows: list[dict[str, object]] = []
    for _, base in shadow_summary.iterrows():
        for nav in portfolio_nav_values:
            for bps in slippage_bps_values:
                for split_days in split_trade_days_values:
                    gross_turnover = float(base["Gross Turnover Weight"])
                    gross_notional = gross_turnover * float(nav)
                    slippage_cost = gross_notional * float(bps) / 10_000.0
                    slippage_drag = slippage_cost / float(nav) if float(nav) else np.nan
                    participation = _participation_context(
                        liquidity_by_run,
                        run=str(base["Run"]),
                        portfolio_nav=float(nav),
                        split_trade_days=int(split_days),
                    )
                    rows.append(
                        {
                            "Date": pd.Timestamp(base["Date"]).date().isoformat(),
                            "Run": str(base["Run"]),
                            "Variant Type": str(base["Variant Type"]),
                            "Portfolio NAV": float(nav),
                            "Slippage Bps": float(bps),
                            "Split Trade Days": int(split_days),
                            "Trade Count": int(base.get("Trade Count", 0) if pd.notna(base.get("Trade Count", np.nan)) else 0),
                            "Gross Turnover Weight": gross_turnover,
                            "One Way Turnover Weight": float(base["One Way Turnover Weight"]),
                            "Gross Trade Notional": gross_notional,
                            "Daily Split Trade Notional": gross_notional / max(int(split_days), 1),
                            "Estimated Slippage Cost": slippage_cost,
                            "Slippage Return Drag": slippage_drag,
                            "Forward Window Trading Days": int(base.get("Forward Window Trading Days", 0) if pd.notna(base.get("Forward Window Trading Days", np.nan)) else 0),
                            "Forward Strategy Return": float(base["Forward Strategy Return"]),
                            "Forward QQQ Return": float(base["Forward QQQ Return"]),
                            "Forward SPY Return": float(base["Forward SPY Return"]),
                            "Net Forward Strategy Return": float(base["Forward Strategy Return"]) - slippage_drag,
                            "Net Forward Excess Return vs QQQ": float(base["Forward Excess Return vs QQQ"]) - slippage_drag,
                            "Net Forward Excess Return vs SPY": float(base["Forward Excess Return vs SPY"]) - slippage_drag,
                            **participation,
                            "diagnostic_scope": DIAGNOSTIC_SCOPE,
                        }
                    )
    return pd.DataFrame(rows, columns=list(DETAIL_COLUMNS))


def _summarize(detail: pd.DataFrame, *, min_median_net_excess_vs_qqq: float) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame(columns=list(SUMMARY_COLUMNS))
    rows: list[dict[str, object]] = []
    group_cols = ["Run", "Variant Type", "Portfolio NAV", "Slippage Bps", "Split Trade Days"]
    for keys, group in detail.groupby(group_cols, sort=True):
        run, variant_type, nav, bps, split_days = keys
        participation_passed = bool(group["participation_gate_passed"].astype(bool).all())
        median_net_qqq = float(pd.to_numeric(group["Net Forward Excess Return vs QQQ"], errors="coerce").median())
        reasons: list[str] = []
        if not participation_passed:
            reasons.extend(
                str(reason)
                for reason in group.loc[~group["participation_gate_passed"].astype(bool), "participation_gate_reason"]
                if str(reason) and str(reason) != "pass"
            )
        if pd.isna(median_net_qqq) or median_net_qqq < float(min_median_net_excess_vs_qqq):
            reasons.append("median_net_excess_vs_qqq_below_threshold")
        reason = ";".join(dict.fromkeys(reasons)) if reasons else "pass"
        passed = reason == "pass"
        rows.append(
            {
                "Run": run,
                "Variant Type": variant_type,
                "Portfolio NAV": float(nav),
                "Slippage Bps": float(bps),
                "Split Trade Days": int(split_days),
                "Rebalance Count": int(len(group)),
                "Total Gross Trade Notional": float(pd.to_numeric(group["Gross Trade Notional"], errors="coerce").sum()),
                "Max Daily Split Trade Notional": float(pd.to_numeric(group["Daily Split Trade Notional"], errors="coerce").max()),
                "Total Estimated Slippage Cost": float(pd.to_numeric(group["Estimated Slippage Cost"], errors="coerce").sum()),
                "Median Slippage Return Drag": float(pd.to_numeric(group["Slippage Return Drag"], errors="coerce").median()),
                "Median Net Forward Excess Return vs QQQ": median_net_qqq,
                "Worst Net Forward Excess Return vs QQQ": float(pd.to_numeric(group["Net Forward Excess Return vs QQQ"], errors="coerce").min()),
                "Median Net Forward Excess Return vs SPY": float(pd.to_numeric(group["Net Forward Excess Return vs SPY"], errors="coerce").median()),
                "Worst Net Forward Excess Return vs SPY": float(pd.to_numeric(group["Net Forward Excess Return vs SPY"], errors="coerce").min()),
                "Max Estimated Participation Rate": float(pd.to_numeric(group["Estimated Max Participation Rate"], errors="coerce").max()),
                "Allowed Max Participation Rate": float(pd.to_numeric(group["Allowed Max Participation Rate"], errors="coerce").max()),
                "Participation Gate Passed": participation_passed,
                "capacity_stress_passed": passed,
                "capacity_stress_reason": reason,
                "recommended_action": "capacity_stress_live_review" if passed else "reduce_nav_slippage_or_extend_execution_days",
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
            }
        )
    return pd.DataFrame(rows, columns=list(SUMMARY_COLUMNS))


def _manifest_payload(
    *,
    output_dir: Path,
    input_paths: Mapping[str, str | Path] | None,
    row_counts: Mapping[str, int],
    portfolio_nav_values: tuple[float, ...],
    slippage_bps_values: tuple[float, ...],
    split_trade_days_values: tuple[int, ...],
    min_median_net_excess_vs_qqq: float,
) -> dict[str, object]:
    artifacts = {
        "capacity_stress_detail": output_dir / "capacity_stress_detail.csv",
        "capacity_stress_summary": output_dir / "capacity_stress_summary.csv",
    }
    return {
        "manifest_type": "russell_top50_capacity_stress",
        "artifact_schema_version": CAPACITY_STRESS_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostic_scope": DIAGNOSTIC_SCOPE,
        "portfolio_nav_values": list(portfolio_nav_values),
        "slippage_bps_values": list(slippage_bps_values),
        "split_trade_days_values": list(split_trade_days_values),
        "min_median_net_excess_vs_qqq": float(min_median_net_excess_vs_qqq),
        "inputs": {
            name: {
                "path": str(path),
                **({"sha256": sha256_file(path)} if Path(path).exists() else {}),
            }
            for name, path in (input_paths or {}).items()
            if path
        },
        "artifacts": {name: {"path": str(path), "sha256": sha256_file(path)} for name, path in artifacts.items()},
        "row_counts": dict(row_counts),
    }


def build_capacity_stress(
    *,
    shadow_live_rebalance_summary: pd.DataFrame,
    liquidity_summary: pd.DataFrame | None = None,
    output_dir: str | Path | None = None,
    portfolio_nav_values: Iterable[float] = DEFAULT_NAV_VALUES,
    slippage_bps_values: Iterable[float] = DEFAULT_SLIPPAGE_BPS_VALUES,
    split_trade_days_values: Iterable[int] = DEFAULT_SPLIT_TRADE_DAYS_VALUES,
    min_median_net_excess_vs_qqq: float = DEFAULT_MIN_MEDIAN_NET_EXCESS_VS_QQQ,
    input_paths: Mapping[str, str | Path] | None = None,
) -> dict[str, object]:
    nav_values = tuple(float(value) for value in portfolio_nav_values)
    bps_values = tuple(float(value) for value in slippage_bps_values)
    split_values = tuple(int(value) for value in split_trade_days_values)
    shadow = _prepare_shadow_summary(shadow_live_rebalance_summary)
    liquidity = _prepare_liquidity_summary(liquidity_summary)
    detail = _build_detail(
        shadow,
        liquidity,
        portfolio_nav_values=nav_values,
        slippage_bps_values=bps_values,
        split_trade_days_values=split_values,
    )
    summary = _summarize(detail, min_median_net_excess_vs_qqq=float(min_median_net_excess_vs_qqq))
    row_counts = {
        "capacity_stress_detail": int(len(detail)),
        "capacity_stress_summary": int(len(summary)),
    }
    manifest_inputs = {
        "detail_rows": int(len(detail)),
        "summary_rows": int(len(summary)),
        "portfolio_nav_values": list(nav_values),
        "slippage_bps_values": list(bps_values),
        "split_trade_days_values": list(split_values),
    }
    if output_dir is not None:
        root = Path(output_dir)
        root.mkdir(parents=True, exist_ok=True)
        detail.to_csv(root / "capacity_stress_detail.csv", index=False)
        summary.to_csv(root / "capacity_stress_summary.csv", index=False)
        write_json(
            root / "capacity_stress_manifest.json",
            _manifest_payload(
                output_dir=root,
                input_paths=input_paths,
                row_counts=row_counts,
                portfolio_nav_values=nav_values,
                slippage_bps_values=bps_values,
                split_trade_days_values=split_values,
                min_median_net_excess_vs_qqq=float(min_median_net_excess_vs_qqq),
            ),
        )
    return {
        "capacity_stress_detail": detail,
        "capacity_stress_summary": summary,
        "capacity_stress_manifest_inputs": manifest_inputs,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build Russell Top50 capacity and implementation-shortfall stress diagnostics."
    )
    parser.add_argument("--shadow-live-summary", required=True, help="Input shadow_live_rebalance_summary.csv")
    parser.add_argument("--liquidity-summary", help="Optional liquidity_summary.csv for participation scaling")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--portfolio-nav-values",
        default=",".join(str(value) for value in DEFAULT_NAV_VALUES),
        help="Comma-separated assumed portfolio NAV values in USD. Do not pass account identifiers.",
    )
    parser.add_argument(
        "--slippage-bps-values",
        default=",".join(str(value) for value in DEFAULT_SLIPPAGE_BPS_VALUES),
    )
    parser.add_argument(
        "--split-trade-days-values",
        default=",".join(str(value) for value in DEFAULT_SPLIT_TRADE_DAYS_VALUES),
    )
    parser.add_argument("--min-median-net-excess-vs-qqq", type=float, default=DEFAULT_MIN_MEDIAN_NET_EXCESS_VS_QQQ)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_capacity_stress(
        shadow_live_rebalance_summary=read_table(args.shadow_live_summary),
        liquidity_summary=read_table(args.liquidity_summary) if args.liquidity_summary else None,
        output_dir=args.output_dir,
        portfolio_nav_values=parse_csv_floats_no_percent(args.portfolio_nav_values, default=DEFAULT_NAV_VALUES),
        slippage_bps_values=parse_csv_floats_no_percent(args.slippage_bps_values, default=DEFAULT_SLIPPAGE_BPS_VALUES),
        split_trade_days_values=_parse_csv_ints(args.split_trade_days_values, default=DEFAULT_SPLIT_TRADE_DAYS_VALUES),
        min_median_net_excess_vs_qqq=float(args.min_median_net_excess_vs_qqq),
        input_paths={
            "shadow_live_summary": args.shadow_live_summary,
            **({"liquidity_summary": args.liquidity_summary} if args.liquidity_summary else {}),
        },
    )
    print(result["capacity_stress_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote capacity stress -> {Path(args.output_dir)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
