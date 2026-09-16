"""Compare the saved Global ETF replay with fixed VOO and BIL buy-and-hold.

This is a single cloud-only comparison over the already saved snapshot.  It
does not call a runner, fetch data, tune a strategy, or create trading output.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import run_global_etf_learning_replay as replay_module  # noqa: E402
from run_global_etf_learning_replay import (  # noqa: E402
    BUCKET,
    END_DATE,
    INPUT_GENERATION,
    INPUT_OBJECT,
    INPUT_SIZE_BYTES,
    PROFILE,
    RUNNER_REVISION,
    START_DATE,
    load_snapshot,
)

try:
    from us_equity_strategies.backtest.etf_rotation_simulator import compute_backtest_metrics
except Exception:  # pragma: no cover - the cloud environment supplies the locked package
    compute_backtest_metrics = None

REPLAY_OBJECT = "global-etf-learning/2017-2024/20260916/learning-replay.json"
REPLAY_GENERATION = "1789555099559060"
REPLAY_SIZE_BYTES = 1601
OUTPUT_OBJECT = "global-etf-learning/2017-2024/20260916/learning-benchmarks.json"
QPK_REVISION = "5c916917626707c4ee798c6b45a5d43609019816"
VOO_BIL = ("VOO", "BIL")
BENCHMARK_COST_RATE = 0.001
BENCHMARK_COST_BPS = 10.0
EXPECTED_OBSERVATION_COUNT = 2012
WORKFLOW_NAME = "Global ETF Learning Replay"


class BenchmarkError(ValueError):
    """Safe, bounded comparison failure."""


def _require_cloud_environment(environ: Mapping[str, str]) -> None:
    try:
        replay_module._require_cloud_environment(environ)
    except Exception:
        raise BenchmarkError("cloud benchmark comparison unavailable") from None


def load_replay_result(storage_client: object) -> dict[str, object]:
    try:
        bucket = storage_client.bucket(BUCKET)
        blob = bucket.blob(REPLAY_OBJECT, generation=int(REPLAY_GENERATION))
        raw = blob.download_as_bytes(
            retry=None,
            timeout=30,
            if_generation_match=int(REPLAY_GENERATION),
        )
    except Exception:
        raise BenchmarkError("replay result download failed") from None
    if not isinstance(raw, bytes) or len(raw) != REPLAY_SIZE_BYTES:
        raise BenchmarkError("replay result size rejected")
    try:
        result = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        raise BenchmarkError("replay result JSON rejected") from None
    if not isinstance(result, Mapping):
        raise BenchmarkError("replay result shape rejected")
    return validate_replay_result(result)


def _finite_metric(value: object) -> float:
    if isinstance(value, bool):
        raise BenchmarkError("replay metrics rejected")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise BenchmarkError("replay metrics rejected") from None
    if not math.isfinite(number):
        raise BenchmarkError("replay metrics rejected")
    return number


def validate_replay_result(result: Mapping[str, object]) -> dict[str, object]:
    if result.get("status") != "REPLAYED" or result.get("profile") != PROFILE:
        raise BenchmarkError("replay result rejected")
    if result.get("runner_revision") != RUNNER_REVISION:
        raise BenchmarkError("replay identity rejected")
    source = result.get("source")
    expected_source = {
        "bucket": BUCKET,
        "object": INPUT_OBJECT,
        "generation": INPUT_GENERATION,
        "size_bytes": INPUT_SIZE_BYTES,
    }
    if source != expected_source:
        raise BenchmarkError("replay source rejected")
    identity = result.get("runner_identity")
    if not isinstance(identity, Mapping):
        raise BenchmarkError("replay identity rejected")
    ues_identity = identity.get("us_equity_strategies")
    qpk_identity = identity.get("quant_platform_kit")
    if (
        not isinstance(ues_identity, Mapping)
        or not isinstance(qpk_identity, Mapping)
        or ues_identity.get("version") != "0.7.60"
        or ues_identity.get("commit") != RUNNER_REVISION
        or qpk_identity.get("version") != "1.0.0"
        or qpk_identity.get("commit") != QPK_REVISION
    ):
        raise BenchmarkError("replay identity rejected")
    if result.get("window") != {"start": START_DATE.isoformat(), "end": END_DATE.isoformat()}:
        raise BenchmarkError("replay window rejected")
    if result.get("effective_cost_bps") != BENCHMARK_COST_BPS:
        raise BenchmarkError("replay cost rejected")
    params = result.get("params")
    if not isinstance(params, Mapping) or params.get("min_history_days") != 260:
        raise BenchmarkError("replay params rejected")
    metrics = result.get("metrics")
    if not isinstance(metrics, Mapping):
        raise BenchmarkError("replay metrics rejected")
    expected_metrics = {"sharpe_ratio", "max_drawdown", "cagr", "total_return", "volatility", "observation_count"}
    if set(metrics) != expected_metrics or metrics.get("observation_count") != EXPECTED_OBSERVATION_COUNT:
        raise BenchmarkError("replay metrics rejected")
    for key in expected_metrics - {"observation_count"}:
        _finite_metric(metrics.get(key))
    controls = result.get("controls")
    expected_controls = {
        "no_order": True,
        "learning_only": True,
        "pit_verified": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
    }
    if not isinstance(controls, Mapping) or any(controls.get(key) is not value for key, value in expected_controls.items()):
        raise BenchmarkError("replay controls rejected")
    benchmark = result.get("benchmark")
    if not isinstance(benchmark, Mapping) or benchmark.get("compared") is not False:
        raise BenchmarkError("replay comparison state rejected")
    return dict(result)


def _benchmark_daily_returns(frame: pd.DataFrame, *, symbol: str, cost_rate: float) -> pd.Series:
    if symbol not in VOO_BIL or not math.isfinite(cost_rate) or cost_rate < 0.0 or cost_rate >= 1.0:
        raise BenchmarkError("benchmark configuration rejected")
    required = {"date", "symbol", "close"}
    if not isinstance(frame, pd.DataFrame) or not required.issubset(frame.columns):
        raise BenchmarkError("benchmark input rejected")
    prices = frame.loc[frame["symbol"] == symbol, ["date", "close"]].copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    prices = prices.loc[(prices["date"] >= pd.Timestamp(START_DATE)) & (prices["date"] <= pd.Timestamp(END_DATE))]
    prices = prices.sort_values("date").reset_index(drop=True)
    if prices.empty or prices["date"].duplicated().any():
        raise BenchmarkError("benchmark input rejected")
    closes = pd.to_numeric(prices["close"], errors="coerce")
    if closes.isna().any() or not (closes > 0).all() or not closes.map(math.isfinite).all():
        raise BenchmarkError("benchmark price rejected")
    returns = closes.pct_change(fill_method=None).fillna(0.0)
    if len(returns) > 1:
        returns.iloc[1] = (closes.iloc[1] / closes.iloc[0]) / (1.0 + cost_rate) - 1.0
    return returns.reset_index(drop=True)


def _benchmark_metrics(frame: pd.DataFrame, *, symbol: str, cost_rate: float = BENCHMARK_COST_RATE) -> dict[str, float | int]:
    if compute_backtest_metrics is None:
        raise BenchmarkError("metrics helper unavailable")
    returns = _benchmark_daily_returns(frame, symbol=symbol, cost_rate=cost_rate)
    try:
        raw = compute_backtest_metrics(returns)
    except Exception:
        raise BenchmarkError("benchmark metrics failed") from None
    result = {
        "sharpe_ratio": _finite_metric(raw.get("sharpe_ratio")),
        "max_drawdown": _finite_metric(raw.get("max_drawdown")),
        "cagr": _finite_metric(raw.get("annual_return")),
        "total_return": _finite_metric(raw.get("total_return")),
        "volatility": _finite_metric(raw.get("annual_volatility")),
        "observation_count": raw.get("days"),
    }
    if isinstance(result["observation_count"], bool) or not isinstance(result["observation_count"], int):
        raise BenchmarkError("benchmark metrics rejected")
    return result


def _target(storage_client: object) -> object:
    try:
        bucket = storage_client.bucket(BUCKET)
        target = bucket.blob(OUTPUT_OBJECT)
        if target.exists(retry=None, timeout=30):
            raise BenchmarkError("target object already exists")
        return target
    except BenchmarkError:
        raise
    except Exception:
        raise BenchmarkError("benchmark target preflight unavailable") from None


def execute_cloud(*, storage_client: object, environ: Mapping[str, str] | None = None) -> dict[str, object]:
    environment = os.environ if environ is None else environ
    _require_cloud_environment(environment)
    target = _target(storage_client)
    try:
        frame, source = load_snapshot(storage_client)
        replay = load_replay_result(storage_client)
        strategy_metrics = dict(replay["metrics"])
        benchmarks = {symbol: _benchmark_metrics(frame, symbol=symbol) for symbol in VOO_BIL}
        if any(metrics["observation_count"] != EXPECTED_OBSERVATION_COUNT for metrics in benchmarks.values()):
            raise BenchmarkError("benchmark observation count rejected")
    except BenchmarkError:
        raise
    except Exception:
        raise BenchmarkError("benchmark comparison failed") from None
    differences = {
        symbol: {
            key: float(strategy_metrics[key]) - float(metrics[key])
            for key in strategy_metrics
            if key != "observation_count"
        }
        for symbol, metrics in benchmarks.items()
    }
    result = {
        "status": "BENCHMARKED",
        "source": source,
        "replay_source": {
            "bucket": BUCKET,
            "object": REPLAY_OBJECT,
            "generation": REPLAY_GENERATION,
            "size_bytes": REPLAY_SIZE_BYTES,
        },
        "runner_identity": replay["runner_identity"],
        "window": replay["window"],
        "effective_cost_bps": replay["effective_cost_bps"],
        "benchmark_initial_cost_bps": BENCHMARK_COST_BPS,
        "strategy_metrics": strategy_metrics,
        "benchmarks": benchmarks,
        "differences": {"direction": "strategy_minus_benchmark", "values": differences},
        "controls": {
            "no_order": True,
            "learning_only": True,
            "pit_verified": False,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
        },
    }
    payload = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    try:
        target.upload_from_string(
            payload,
            content_type="application/json",
            if_generation_match=0,
            retry=None,
            timeout=30,
        )
    except Exception:
        raise BenchmarkError("benchmark output upload unknown") from None
    return result


def _plan() -> dict[str, object]:
    return {
        "status": "PLAN_ONLY",
        "input": {"bucket": BUCKET, "object": INPUT_OBJECT, "generation": INPUT_GENERATION, "size_bytes": INPUT_SIZE_BYTES},
        "replay": {"object": REPLAY_OBJECT, "generation": REPLAY_GENERATION, "size_bytes": REPLAY_SIZE_BYTES},
        "output_object": OUTPUT_OBJECT,
        "benchmarks": list(VOO_BIL),
        "benchmark_initial_cost_bps": BENCHMARK_COST_BPS,
        "window": {"start": START_DATE.isoformat(), "end": END_DATE.isoformat()},
        "controls": {"no_order": True, "learning_only": True, "pit_verified": False, "promotion_eligible": False},
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Compare the saved Global ETF replay with VOO and BIL.")
    parser.add_argument("--execute-cloud", action="store_true", help="run only in the approved main GitHub workflow")
    args = parser.parse_args(argv)
    if not args.execute_cloud:
        print(json.dumps(_plan(), sort_keys=True, separators=(",", ":")))
        return
    try:
        _require_cloud_environment(os.environ)
        from google.cloud import storage

        result = execute_cloud(storage_client=storage.Client())
    except BenchmarkError as exc:
        raise SystemExit(str(exc)) from None
    except Exception:
        raise SystemExit("cloud benchmark comparison failed") from None
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))


if __name__ == "__main__":
    main()
