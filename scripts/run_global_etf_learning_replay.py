"""Replay the fixed Global ETF learning snapshot through one existing runner.

The default command only prints a plan.  The explicit cloud path reads one
generation-bound GCS object in memory, runs one fixed UESP runner, and creates
one aggregate result object.  It never fetches market data or writes local
bars, prices, or artifacts.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Callable, Mapping
from datetime import date
from importlib import metadata
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from acquire_global_etf_learning_inputs_alpaca import (  # noqa: E402
    SYMBOLS,
    _expected_xnys_sessions,
)

BUCKET = "qsl-runtime-logs-shared"
INPUT_OBJECT = "global-etf-learning/2017-2024/20260916/bars.json"
OUTPUT_OBJECT = "global-etf-learning/2017-2024/20260916/learning-replay.json"
INPUT_GENERATION = "1789552505861428"
INPUT_SIZE_BYTES = 6_643_030
RUNNER_REVISION = "5f11fcfe8c5473de20e1b590e9aa3e87665b6108"
PROFILE = "global_etf_rotation"
START_DATE = date(2017, 1, 1)
END_DATE = date(2024, 12, 31)
MIN_HISTORY_DAYS = 260
FIXED_COST_BPS = 10.0
WORKFLOW_NAME = "Global ETF Learning Replay"


class ReplayError(ValueError):
    """Safe, bounded replay failure with no provider or price details."""


def _require_cloud_environment(environ: Mapping[str, str]) -> None:
    if (
        environ.get("GITHUB_ACTIONS") != "true"
        or environ.get("GITHUB_REF") != "refs/heads/main"
        or environ.get("GITHUB_WORKFLOW") != WORKFLOW_NAME
    ):
        raise ReplayError("cloud replay unavailable")


def _fixed_cost_bps() -> float:
    try:
        from us_equity_strategies.backtest.etf_rotation_simulator import UsRotationBacktestConfig

        cost_bps = float(UsRotationBacktestConfig().cost_bps)
    except Exception:
        raise ReplayError("runner unavailable") from None
    if cost_bps != FIXED_COST_BPS:
        raise ReplayError("runner cost configuration rejected")
    return cost_bps


def _expected_sessions() -> tuple[date, ...]:
    try:
        sessions = tuple(_expected_xnys_sessions())
    except Exception:
        raise ReplayError("calendar unavailable") from None
    if not sessions:
        raise ReplayError("calendar unavailable")
    return sessions


def _validate_controls(payload: Mapping[str, object]) -> None:
    controls = payload.get("controls")
    summary = payload.get("summary")
    if not isinstance(controls, Mapping) or not isinstance(summary, Mapping):
        raise ReplayError("snapshot contract rejected")
    expected_controls = {"no_order": True, "promotion_eligible": False, "live_ready": False}
    if any(controls.get(key) is not value for key, value in expected_controls.items()):
        raise ReplayError("snapshot controls rejected")
    if summary.get("pit_verified") is not False:
        raise ReplayError("snapshot controls rejected")
    if payload.get("learning_only") is not True or payload.get("size_zero_required") is not True:
        raise ReplayError("snapshot controls rejected")
    expected_summary = {
        "provider": "Alpaca",
        "feed": "sip",
        "adjustment": "all",
        "asof": "2024-12-31",
        "currency": "USD",
        "symbol_count": len(SYMBOLS),
    }
    if any(summary.get(key) != value for key, value in expected_summary.items()):
        raise ReplayError("snapshot summary rejected")


def _bars_frame(payload: Mapping[str, object], sessions: tuple[date, ...]) -> pd.DataFrame:
    bars = payload.get("bars")
    summary = payload.get("summary")
    if not isinstance(bars, list) or not isinstance(summary, Mapping):
        raise ReplayError("snapshot shape rejected")
    expected_count = len(SYMBOLS) * len(sessions)
    if summary.get("bar_count") != expected_count or len(bars) != expected_count:
        raise ReplayError("snapshot bar count rejected")
    if summary.get("first_session") != sessions[0].isoformat() or summary.get("last_session") != sessions[-1].isoformat():
        raise ReplayError("snapshot window rejected")

    required = {"date", "symbol", "open", "high", "low", "close", "volume"}
    rows: list[dict[str, object]] = []
    expected_dates = {item.isoformat() for item in sessions}
    seen: set[tuple[str, str]] = set()
    for item in bars:
        if not isinstance(item, Mapping) or set(item) != required:
            raise ReplayError("snapshot row shape rejected")
        symbol = item.get("symbol")
        day = item.get("date")
        if not isinstance(symbol, str) or symbol not in SYMBOLS or not isinstance(day, str) or day not in expected_dates:
            raise ReplayError("snapshot row identity rejected")
        pair = (symbol, day)
        if pair in seen:
            raise ReplayError("snapshot duplicate rejected")
        seen.add(pair)
        values: dict[str, float] = {}
        for key in ("open", "high", "low", "close", "volume"):
            value = item.get(key)
            if isinstance(value, bool):
                raise ReplayError("snapshot numeric value rejected")
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError):
                raise ReplayError("snapshot numeric value rejected") from None
            if not math.isfinite(number) or (key != "volume" and number <= 0) or (key == "volume" and number < 0):
                raise ReplayError("snapshot numeric value rejected")
            values[key] = number
        if values["low"] > min(values["open"], values["high"], values["close"]):
            raise ReplayError("snapshot price range rejected")
        if values["high"] < max(values["open"], values["low"], values["close"]):
            raise ReplayError("snapshot price range rejected")
        rows.append({"date": pd.Timestamp(day), "symbol": symbol, **values})
    if seen != {(symbol, day.isoformat()) for symbol in SYMBOLS for day in sessions}:
        raise ReplayError("snapshot coverage rejected")
    return pd.DataFrame(rows, columns=["date", "symbol", "open", "high", "low", "close", "volume"])


def load_snapshot(storage_client: object) -> tuple[pd.DataFrame, dict[str, object]]:
    """Download and validate the one fixed GCS generation in memory."""
    try:
        bucket = storage_client.bucket(BUCKET)
        blob = bucket.blob(INPUT_OBJECT, generation=int(INPUT_GENERATION))
        raw = blob.download_as_bytes(
            retry=None,
            timeout=60,
            if_generation_match=int(INPUT_GENERATION),
        )
    except Exception:
        raise ReplayError("snapshot download failed") from None
    if not isinstance(raw, bytes) or len(raw) != INPUT_SIZE_BYTES:
        raise ReplayError("snapshot size rejected")
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ReplayError("snapshot JSON rejected") from None
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "global_etf_learning_inputs.v1":
        raise ReplayError("snapshot schema rejected")
    _validate_controls(payload)
    sessions = _expected_sessions()
    frame = _bars_frame(payload, sessions)
    return frame, {
        "bucket": BUCKET,
        "object": INPUT_OBJECT,
        "generation": INPUT_GENERATION,
        "size_bytes": len(raw),
    }


def _result_metrics(result: object) -> dict[str, float | int]:
    fields = {
        "sharpe_ratio": "sharpe_ratio",
        "max_drawdown": "max_drawdown",
        "cagr": "cagr",
        "total_return": "total_return",
        "volatility": "volatility",
        "observation_count": "observation_count",
    }
    metrics: dict[str, float | int] = {}
    for output_key, attr in fields.items():
        value = getattr(result, attr, None)
        if output_key == "observation_count":
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ReplayError("runner metrics rejected")
            metrics[output_key] = value
            continue
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            raise ReplayError("runner metrics rejected") from None
        if not math.isfinite(number):
            raise ReplayError("runner metrics rejected")
        metrics[output_key] = number
    return metrics


def _package_identity(distribution_name: str) -> dict[str, str | None]:
    try:
        distribution = metadata.distribution(distribution_name)
        direct_url = distribution.read_text("direct_url.json") or ""
        raw = json.loads(direct_url) if direct_url else {}
        vcs_info = raw.get("vcs_info", {}) if isinstance(raw, Mapping) else {}
        commit_id = vcs_info.get("commit_id") if isinstance(vcs_info, Mapping) else None
        return {"version": distribution.version, "commit": commit_id if isinstance(commit_id, str) else None}
    except Exception:
        raise ReplayError("runner identity unavailable") from None


def run_replay(
    market_history: pd.DataFrame,
    *,
    source: Mapping[str, object],
    runner: object | None = None,
    runner_factory: Callable[..., object] | None = None,
) -> dict[str, object]:
    """Run exactly one fixed-profile replay through the existing UESP runner."""
    if not isinstance(market_history, pd.DataFrame) or market_history.empty:
        raise ReplayError("snapshot market history unavailable")
    cost_bps = _fixed_cost_bps()
    if runner is None:
        if runner_factory is None:
            try:
                from us_equity_strategies.backtest.orchestrator_runner import UsEtfRotationBacktestRunner
            except Exception:
                raise ReplayError("runner unavailable") from None
            runner_factory = UsEtfRotationBacktestRunner
        try:
            runner = runner_factory(market_history=market_history)
        except Exception:
            raise ReplayError("runner unavailable") from None
    try:
        result = runner.run(
            PROFILE,
            {"min_history_days": MIN_HISTORY_DAYS},
            start_date=START_DATE,
            end_date=END_DATE,
        )
    except Exception:
        raise ReplayError("runner replay failed") from None
    metrics = _result_metrics(result)
    result_params = getattr(result, "params", None)
    if not isinstance(result_params, Mapping):
        raise ReplayError("runner params unavailable")
    return {
        "status": "REPLAYED",
        "profile": PROFILE,
        "runner_revision": RUNNER_REVISION,
        "runner_identity": {
            "us_equity_strategies": _package_identity("us-equity-strategies"),
            "quant_platform_kit": _package_identity("quant-platform-kit"),
        },
        "source": dict(source),
        "window": {"start": START_DATE.isoformat(), "end": END_DATE.isoformat()},
        "params": dict(result_params),
        "requested_params": {"min_history_days": MIN_HISTORY_DAYS},
        "effective_cost_bps": cost_bps,
        "metrics": metrics,
        "benchmark": {"compared": False, "reason": "no comparable benchmark supplied"},
        "controls": {
            "no_order": True,
            "learning_only": True,
            "pit_verified": False,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
        },
    }


def _target(storage_client: object) -> object:
    try:
        bucket = storage_client.bucket(BUCKET)
        target = bucket.blob(OUTPUT_OBJECT)
        if target.exists(retry=None, timeout=30):
            raise ReplayError("target object already exists")
        return target
    except ReplayError:
        raise
    except Exception:
        raise ReplayError("replay target preflight unavailable") from None


def execute_cloud(
    *,
    storage_client: object,
    runner_factory: Callable[..., object] | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    environment = os.environ if environ is None else environ
    _require_cloud_environment(environment)
    target = _target(storage_client)
    market_history, source = load_snapshot(storage_client)
    result = run_replay(market_history, source=source, runner_factory=runner_factory)
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
        raise ReplayError("replay output upload unknown") from None
    return result


def _plan() -> dict[str, object]:
    return {
        "status": "PLAN_ONLY",
        "input": {"bucket": BUCKET, "object": INPUT_OBJECT, "generation": INPUT_GENERATION, "size_bytes": INPUT_SIZE_BYTES},
        "input_generation": INPUT_GENERATION,
        "output_object": OUTPUT_OBJECT,
        "runner_revision": RUNNER_REVISION,
        "profile": PROFILE,
        "window": {"warmup_start": "2016-01-01", "learning_start": "2017-01-01", "end": END_DATE.isoformat()},
        "params": {"min_history_days": MIN_HISTORY_DAYS},
        "effective_cost_bps": FIXED_COST_BPS,
        "controls": {
            "no_order": True,
            "learning_only": True,
            "pit_verified": False,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
        },
        "persistence": "gcs_single_result_only",
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Replay the fixed Global ETF learning snapshot once in cloud.")
    parser.add_argument("--execute-cloud", action="store_true", help="run only in the approved main GitHub workflow")
    args = parser.parse_args(argv)
    if not args.execute_cloud:
        print(json.dumps(_plan(), sort_keys=True, separators=(",", ":")))
        return
    try:
        _require_cloud_environment(os.environ)
        from google.cloud import storage

        result = execute_cloud(storage_client=storage.Client())
    except ReplayError as exc:
        raise SystemExit(str(exc)) from None
    except Exception:
        raise SystemExit("cloud replay failed") from None
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))


if __name__ == "__main__":
    main()
