"""Return normalization and performance summary metrics for Global ETF offensive rotation."""

from __future__ import annotations

import math
from collections.abc import Mapping

import pandas as pd


def _normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise ValueError("price_history has no usable rows")
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.map(str).str.upper()
    return close.ffill()


def _normalize_weights(weights: Mapping[str, float], *, safe_haven: str) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): float(weight)
        for symbol, weight in weights.items()
        if str(symbol or "").strip() and pd.notna(weight) and abs(float(weight)) > 1e-12
    }
    total = sum(cleaned.values())
    if total <= 0.0:
        return {safe_haven.upper(): 1.0}
    return {symbol: weight / total for symbol, weight in cleaned.items()}


def _compute_turnover(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    symbols = set(current) | set(target)
    return 0.5 * sum(abs(float(target.get(symbol, 0.0)) - float(current.get(symbol, 0.0))) for symbol in symbols)


def _benchmark_summary(returns: pd.Series, index: pd.DatetimeIndex) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(returns).reindex(index), errors="coerce").dropna()
    if clean.empty:
        return {"total_return": float("nan"), "cagr": float("nan"), "max_drawdown": float("nan")}
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return": float(equity.iloc[-1] - 1.0),
        "cagr": float(equity.iloc[-1] ** (1.0 / years) - 1.0),
        "max_drawdown": float(drawdown.min()),
    }


def summarize_returns(
    returns: pd.Series,
    *,
    weights_history: pd.DataFrame | None = None,
    benchmark_returns: pd.Series | None = None,
    secondary_benchmark_returns: pd.Series | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, float | str | int]:
    clean = pd.to_numeric(pd.Series(returns), errors="coerce").dropna()
    if clean.empty:
        return {
            "Start": start_date or "",
            "End": end_date or "",
            "Trading Days": 0,
            "Total Return": float("nan"),
            "CAGR": float("nan"),
            "Max Drawdown": float("nan"),
            "Volatility": float("nan"),
            "Sharpe": float("nan"),
            "Calmar": float("nan"),
            "Benchmark Total Return": float("nan"),
            "Benchmark CAGR": float("nan"),
            "Benchmark Max Drawdown": float("nan"),
            "Excess CAGR vs Benchmark": float("nan"),
            "Secondary Benchmark Total Return": float("nan"),
            "Secondary Benchmark CAGR": float("nan"),
            "Secondary Benchmark Max Drawdown": float("nan"),
            "Excess CAGR vs Secondary Benchmark": float("nan"),
            "Turnover/Year": float("nan"),
            "Avg Risk Exposure": float("nan"),
        }

    clean.index = pd.to_datetime(clean.index).tz_localize(None).normalize()
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(clean.std(ddof=0) * math.sqrt(252.0))
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252.0)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    benchmark = (
        _benchmark_summary(benchmark_returns, pd.DatetimeIndex(clean.index)) if benchmark_returns is not None else {}
    )
    secondary = (
        _benchmark_summary(secondary_benchmark_returns, pd.DatetimeIndex(clean.index))
        if secondary_benchmark_returns is not None
        else {}
    )
    benchmark_cagr = float(benchmark.get("cagr", float("nan")))
    secondary_cagr = float(secondary.get("cagr", float("nan")))

    turnover_per_year = float("nan")
    avg_risk_exposure = float("nan")
    if weights_history is not None and not weights_history.empty:
        weight_frame = pd.DataFrame(weights_history).copy()
        if "as_of" in weight_frame.columns:
            weight_frame["as_of"] = pd.to_datetime(weight_frame["as_of"]).dt.tz_localize(None).dt.normalize()
            weight_frame = weight_frame.set_index("as_of")
        weight_frame = weight_frame.reindex(clean.index).ffill().fillna(0.0)
        if not weight_frame.empty:
            turnover_per_year = float((0.5 * weight_frame.diff().abs().sum(axis=1).fillna(0.0)).sum() / years)
            safe_like = {"BIL", "BOXX", "SGOV", "CASH"}
            risk_columns = [column for column in weight_frame.columns if str(column).upper() not in safe_like]
            avg_risk_exposure = float(weight_frame[risk_columns].sum(axis=1).mean()) if risk_columns else 0.0

    return {
        "Start": clean.index[0].date().isoformat(),
        "End": clean.index[-1].date().isoformat(),
        "Trading Days": int(len(clean)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Benchmark Total Return": float(benchmark.get("total_return", float("nan"))),
        "Benchmark CAGR": benchmark_cagr,
        "Benchmark Max Drawdown": float(benchmark.get("max_drawdown", float("nan"))),
        "Excess CAGR vs Benchmark": cagr - benchmark_cagr if not pd.isna(benchmark_cagr) else float("nan"),
        "Secondary Benchmark Total Return": float(secondary.get("total_return", float("nan"))),
        "Secondary Benchmark CAGR": secondary_cagr,
        "Secondary Benchmark Max Drawdown": float(secondary.get("max_drawdown", float("nan"))),
        "Excess CAGR vs Secondary Benchmark": cagr - secondary_cagr if not pd.isna(secondary_cagr) else float("nan"),
        "Turnover/Year": turnover_per_year,
        "Avg Risk Exposure": avg_risk_exposure,
    }
