from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TQQQ_PRICES = ROOT / "data" / "output" / "tqqq_volatility_delever_threshold_research" / "normalized_price_history.csv"
DEFAULT_SOXL_PRICES = ROOT / "data" / "output" / "soxl_dynamic_volatility_delever_threshold_research" / "normalized_price_history.csv"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "volatility_delever_retention_policy_research"

WINDOWS = (
    ("full", "2011-03-10", "2026-06-15"),
    ("2020", "2020-02-18", "2020-04-30"),
    ("2022", "2022-01-03", "2022-12-30"),
    ("post2022", "2023-01-03", "2026-06-15"),
    ("ytd2026", "2026-01-02", "2026-06-15"),
    ("3m", "2026-03-16", "2026-06-15"),
)


def _load_prices(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if not {"as_of", "symbol", "close"}.issubset(frame.columns):
        raise ValueError("price file must include as_of, symbol, close columns")
    frame = frame.loc[:, ["as_of", "symbol", "close"]].copy()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["as_of", "symbol", "close"])
    return frame


def _close_matrix(frame: pd.DataFrame, symbols: tuple[str, ...]) -> pd.DataFrame:
    matrix = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index().ffill()
    missing = sorted(symbol for symbol in symbols if symbol not in matrix.columns)
    if missing:
        raise ValueError(f"price file missing required symbols: {', '.join(missing)}")
    return matrix.loc[:, symbols].dropna()


def _realized_vol(close: pd.Series, window: int) -> pd.Series:
    return close.pct_change(fill_method=None).rolling(int(window), min_periods=int(window)).std(ddof=0) * math.sqrt(252)


def _dynamic_threshold(
    vol: pd.Series,
    *,
    percentile: float,
    floor: float,
    cap: float,
    lookback: int = 252,
    min_periods: int = 126,
    fallback: float,
) -> pd.Series:
    raw = vol.rolling(int(lookback), min_periods=int(min_periods)).quantile(float(percentile))
    return raw.clip(lower=float(floor), upper=float(cap)).fillna(float(fallback))


def _rolling_drawdown(close: pd.Series, lookback: int = 252) -> pd.Series:
    peak = close.rolling(int(lookback), min_periods=min(63, int(lookback))).max()
    return close / peak - 1.0


def _relative_return(numerator: pd.Series, denominator: pd.Series, lookback: int) -> pd.Series:
    ratio = numerator / denominator
    return ratio / ratio.shift(int(lookback)) - 1.0


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0).rolling(int(window), min_periods=int(window)).mean()
    losses = (-delta.clip(upper=0.0)).rolling(int(window), min_periods=int(window)).mean()
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _bollinger_upper(close: pd.Series, window: int = 20) -> pd.Series:
    return close.rolling(int(window), min_periods=int(window)).mean() + 2.0 * close.rolling(
        int(window),
        min_periods=int(window),
    ).std(ddof=0)


def _env(
    matrix: pd.DataFrame,
    benchmark_symbol: str,
    as_of: pd.Timestamp,
    *,
    trend_ok: bool,
    slope_ok: bool,
    drawdown_limit: float,
    vix_soft: float,
    vix_hard: float,
    credit_soft: float,
    credit_hard: float,
    financial_hard: float,
    vol_ratio: float,
) -> dict[str, object]:
    benchmark = matrix[benchmark_symbol]
    idx = matrix.index.get_loc(as_of)
    drawdown = float(_rolling_drawdown(benchmark).loc[as_of])
    vix = float(matrix["^VIX"].loc[as_of])
    credit21 = _relative_return(matrix["HYG"], matrix["IEF"], 21).loc[as_of]
    financial63 = _relative_return(matrix["XLF"], matrix["SPY"], 63).loc[as_of]
    rebound1 = idx >= 1 and benchmark.iloc[idx] / benchmark.iloc[idx - 1] - 1.0 > 0.0
    rebound3 = idx >= 3 and benchmark.iloc[idx] / benchmark.iloc[idx - 3] - 1.0 > 0.0
    hard = (
        (not trend_ok)
        or drawdown <= drawdown_limit
        or vix >= vix_hard
        or (pd.notna(credit21) and float(credit21) <= credit_hard)
        or (pd.notna(financial63) and float(financial63) <= financial_hard)
    )
    soft = (
        drawdown <= drawdown_limit / 1.5
        or vix >= vix_soft
        or (pd.notna(credit21) and float(credit21) <= credit_soft)
        or vol_ratio >= 1.65
    )
    return {
        "hard": bool(hard),
        "soft": bool(soft),
        "constructive": bool(trend_ok and slope_ok and not soft),
        "rebound_confirm": bool(trend_ok and (rebound1 or rebound3) and vix < vix_hard and (pd.isna(credit21) or float(credit21) > credit_hard)),
        "drawdown": drawdown,
        "vix": vix,
        "credit21": None if pd.isna(credit21) else float(credit21),
        "financial63": None if pd.isna(financial63) else float(financial63),
        "vol_ratio": float(vol_ratio),
    }


def _retention(policy: str, env: dict[str, object]) -> float:
    hard = bool(env["hard"])
    soft = bool(env["soft"])
    constructive = bool(env["constructive"])
    rebound = bool(env["rebound_confirm"])
    if policy == "current" or hard:
        return 0.0
    if policy == "tqqq_step_softzero_0.25_0.50":
        if soft:
            return 0.0
        return 0.50 if constructive and rebound else 0.25
    if policy == "tqqq_step_softzero_0.35_0.50":
        if soft:
            return 0.0
        return 0.50 if constructive and rebound else 0.35
    if policy == "soxl_step_rebound_0.25_0.50":
        if not rebound:
            return 0.0
        return 0.50 if constructive else 0.25
    if policy == "soxl_rebound_0.50":
        return 0.50 if rebound else 0.0
    raise ValueError(f"unknown policy: {policy}")


def _summary(returns: pd.Series, start: str, end: str) -> dict[str, float]:
    window = returns.loc[(returns.index >= pd.Timestamp(start)) & (returns.index <= pd.Timestamp(end))].fillna(0.0)
    sample = window.iloc[1:]
    if len(sample) < 2:
        return {"total": 0.0, "cagr": 0.0, "mdd": 0.0, "vol": 0.0}
    equity = (1.0 + sample).cumprod()
    days = max(1, int((window.index[-1] - window.index[0]).days))
    peak = equity.cummax()
    return {
        "total": float(equity.iloc[-1] - 1.0),
        "cagr": float(equity.iloc[-1] ** (365.25 / days) - 1.0),
        "mdd": float((equity / peak - 1.0).min()),
        "vol": float(sample.std(ddof=0) * math.sqrt(252)),
    }


def _run_tqqq(matrix: pd.DataFrame, policy: str) -> tuple[pd.Series, dict[str, object], list[dict[str, object]]]:
    qqq = matrix["QQQ"]
    tqqq = matrix["TQQQ"]
    ma20 = qqq.rolling(20, min_periods=20).mean()
    ma200 = qqq.rolling(200, min_periods=200).mean()
    vol5 = _realized_vol(qqq, 5)
    threshold = _dynamic_threshold(vol5, percentile=0.90, floor=0.24, cap=0.36, fallback=0.28)
    vol20 = _realized_vol(qqq, 20)
    returns = pd.Series(0.0, index=matrix.index)
    active = False
    info = {"triggers": 0, "retained_days": 0, "hard_days": 0}
    events: list[dict[str, object]] = []
    for pos, as_of in enumerate(matrix.index[:-1]):
        if pd.isna(ma200.loc[as_of]) or pd.isna(ma20.loc[as_of]) or pos == 0:
            continue
        above = bool(qqq.loc[as_of] > ma200.loc[as_of])
        slope = bool(ma20.loc[as_of] > ma20.shift(1).loc[as_of])
        if active and not above:
            active = False
        elif not active and above and slope:
            active = True
        pull_low = qqq.iloc[max(0, pos - 19) : pos + 1].min()
        pull_threshold = 0.0 if pd.isna(vol20.loc[as_of]) else float(vol20.loc[as_of]) / math.sqrt(252) * 2.0
        pull_rebound = qqq.loc[as_of] / pull_low - 1.0 if pull_low > 0 else 0.0
        pull_on = bool((not above) and qqq.loc[as_of] > ma20.loc[as_of] and slope and pull_rebound > pull_threshold)
        t_weight = 0.45 if active or pull_on else 0.0
        q_weight = 0.45 if active or pull_on else 0.0
        triggered = t_weight > 0.0 and pd.notna(vol5.loc[as_of]) and vol5.loc[as_of] >= threshold.loc[as_of]
        env = _env(
            matrix,
            "QQQ",
            as_of,
            trend_ok=above,
            slope_ok=slope,
            drawdown_limit=-0.12,
            vix_soft=25.0,
            vix_hard=32.0,
            credit_soft=-0.02,
            credit_hard=-0.04,
            financial_hard=-0.08,
            vol_ratio=float(vol5.loc[as_of] / threshold.loc[as_of]) if pd.notna(vol5.loc[as_of]) else 0.0,
        )
        if triggered:
            retain = _retention(policy, env)
            info["triggers"] += 1
            info["retained_days"] += int(retain > 0)
            info["hard_days"] += int(env["hard"])
            q_weight += t_weight * (1.0 - retain)
            t_weight *= retain
            if as_of >= pd.Timestamp("2026-06-01"):
                events.append(_event_row("tqqq", policy, as_of, float(vol5.loc[as_of]), float(threshold.loc[as_of]), retain, env))
        next_as_of = matrix.index[pos + 1]
        returns.loc[next_as_of] = t_weight * (tqqq.loc[next_as_of] / tqqq.loc[as_of] - 1.0) + q_weight * (
            qqq.loc[next_as_of] / qqq.loc[as_of] - 1.0
        )
    return returns, info, events


def _run_soxl(matrix: pd.DataFrame, policy: str) -> tuple[pd.Series, dict[str, object], list[dict[str, object]]]:
    soxx = matrix["SOXX"]
    soxl = matrix["SOXL"]
    ma140 = soxx.rolling(140, min_periods=140).mean()
    ma20 = soxx.rolling(20, min_periods=20).mean()
    rsi14 = _rsi(soxx)
    upper = _bollinger_upper(soxx)
    vol10 = _realized_vol(soxx, 10)
    threshold = _dynamic_threshold(vol10, percentile=0.95, floor=0.50, cap=0.75, fallback=0.55)
    returns = pd.Series(0.0, index=matrix.index)
    active = False
    info = {"triggers": 0, "retained_days": 0, "hard_days": 0}
    events: list[dict[str, object]] = []
    for pos, as_of in enumerate(matrix.index[:-1]):
        if pd.isna(ma140.loc[as_of]) or pos == 0:
            continue
        tier = "defensive"
        if soxx.loc[as_of] > ma140.loc[as_of] * 1.08:
            tier = "full"
        elif soxx.loc[as_of] > ma140.loc[as_of] * 1.06 or (active and soxx.loc[as_of] > ma140.loc[as_of] * 0.98):
            tier = "mid"
        l_weight, x_weight = (0.70, 0.20) if tier == "full" else (0.65, 0.20) if tier == "mid" else (0.0, 0.15)
        overlay = 0
        if tier in {"full", "mid"}:
            overlay += int(pd.notna(rsi14.loc[as_of]) and rsi14.loc[as_of] > 70.0)
            overlay += int(pd.notna(upper.loc[as_of]) and soxx.loc[as_of] > upper.loc[as_of])
        if overlay:
            if tier == "full" and overlay == 1:
                l_weight, x_weight = 0.65, 0.20
            else:
                l_weight, x_weight = 0.0, 0.15
        active = l_weight > 0.0
        slope = bool(pd.notna(ma20.loc[as_of]) and ma20.loc[as_of] > ma20.shift(1).loc[as_of])
        above = bool(soxx.loc[as_of] > ma140.loc[as_of])
        triggered = l_weight > 0.0 and pd.notna(vol10.loc[as_of]) and vol10.loc[as_of] >= threshold.loc[as_of]
        env = _env(
            matrix,
            "SOXX",
            as_of,
            trend_ok=above,
            slope_ok=slope,
            drawdown_limit=-0.18,
            vix_soft=28.0,
            vix_hard=35.0,
            credit_soft=-0.025,
            credit_hard=-0.05,
            financial_hard=-0.10,
            vol_ratio=float(vol10.loc[as_of] / threshold.loc[as_of]) if pd.notna(vol10.loc[as_of]) else 0.0,
        )
        if triggered:
            retain = _retention(policy, env)
            info["triggers"] += 1
            info["retained_days"] += int(retain > 0)
            info["hard_days"] += int(env["hard"])
            x_weight += l_weight * (1.0 - retain)
            l_weight *= retain
            if as_of >= pd.Timestamp("2026-06-01"):
                events.append(_event_row("soxl", policy, as_of, float(vol10.loc[as_of]), float(threshold.loc[as_of]), retain, env))
        next_as_of = matrix.index[pos + 1]
        returns.loc[next_as_of] = l_weight * (soxl.loc[next_as_of] / soxl.loc[as_of] - 1.0) + x_weight * (
            soxx.loc[next_as_of] / soxx.loc[as_of] - 1.0
        )
    return returns, info, events


def _event_row(profile: str, policy: str, as_of: pd.Timestamp, metric: float, threshold: float, retain: float, env: dict[str, object]) -> dict[str, object]:
    return {
        "profile": profile,
        "policy": policy,
        "as_of": as_of.date().isoformat(),
        "metric": metric,
        "threshold": threshold,
        "retention_ratio": retain,
        **env,
    }


def _summary_row(profile: str, policy: str, returns: pd.Series, info: dict[str, object]) -> dict[str, object]:
    rows = {name: _summary(returns, start, end) for name, start, end in WINDOWS}
    return {
        "profile": profile,
        "policy": policy,
        **info,
        "full_cagr": rows["full"]["cagr"],
        "full_mdd": rows["full"]["mdd"],
        "2020_mdd": rows["2020"]["mdd"],
        "2022_return": rows["2022"]["total"],
        "post2022_cagr": rows["post2022"]["cagr"],
        "ytd2026_return": rows["ytd2026"]["total"],
        "3m_return": rows["3m"]["total"],
    }


def run_research(*, tqqq_prices: Path, soxl_prices: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    tqqq_matrix = _close_matrix(_load_prices(tqqq_prices), ("QQQ", "TQQQ", "HYG", "IEF", "XLF", "SPY", "^VIX"))
    soxl_matrix = _close_matrix(_load_prices(soxl_prices), ("SOXX", "SOXL", "HYG", "IEF", "XLF", "SPY", "^VIX"))
    summary_rows = []
    event_rows = []
    for policy in ("current", "tqqq_step_softzero_0.25_0.50", "tqqq_step_softzero_0.35_0.50"):
        returns, info, events = _run_tqqq(tqqq_matrix, policy)
        summary_rows.append(_summary_row("tqqq", policy, returns, info))
        event_rows.extend(events)
    for policy in ("current", "soxl_step_rebound_0.25_0.50", "soxl_rebound_0.50"):
        returns, info, events = _run_soxl(soxl_matrix, policy)
        summary_rows.append(_summary_row("soxl", policy, returns, info))
        event_rows.extend(events)
    pd.DataFrame(summary_rows).to_csv(output_dir / "retention_policy_summary.csv", index=False)
    pd.DataFrame(event_rows).to_csv(output_dir / "recent_retention_events.csv", index=False)
    return output_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research deterministic volatility-delever retention policies.")
    parser.add_argument("--tqqq-prices", default=str(DEFAULT_TQQQ_PRICES))
    parser.add_argument("--soxl-prices", default=str(DEFAULT_SOXL_PRICES))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = run_research(
        tqqq_prices=Path(args.tqqq_prices),
        soxl_prices=Path(args.soxl_prices),
        output_dir=Path(args.output_dir),
    )
    print(f"wrote volatility-delever retention policy research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
