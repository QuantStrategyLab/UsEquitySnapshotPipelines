from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "compare_global_etf_learning_benchmarks.py"
_SPEC = importlib.util.spec_from_file_location("global_etf_learning_benchmarks", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(module)


def _source() -> dict[str, object]:
    return {
        "bucket": module.BUCKET,
        "object": module.INPUT_OBJECT,
        "generation": module.INPUT_GENERATION,
        "size_bytes": module.INPUT_SIZE_BYTES,
    }


def _replay_result() -> dict[str, object]:
    metrics = {
        "sharpe_ratio": 0.5,
        "max_drawdown": -0.3,
        "cagr": 0.1,
        "total_return": 1.0,
        "volatility": 0.2,
        "observation_count": 2012,
    }
    return {
        "status": "REPLAYED",
        "profile": "global_etf_rotation",
        "runner_revision": module.RUNNER_REVISION,
        "runner_identity": {
            "us_equity_strategies": {"version": "0.7.60", "commit": module.RUNNER_REVISION},
            "quant_platform_kit": {"version": "1.0.0", "commit": module.QPK_REVISION},
        },
        "source": _source(),
        "window": {"start": "2017-01-01", "end": "2024-12-31"},
        "params": {"min_history_days": 260, "sma_period": 250},
        "requested_params": {"min_history_days": 260},
        "effective_cost_bps": 10.0,
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


def _frame() -> pd.DataFrame:
    rows = []
    for day, voo, bil in (
        ("2017-01-03", 100.0, 100.0),
        ("2017-01-04", 110.0, 101.0),
        ("2017-01-05", 100.0, 102.0),
    ):
        rows.extend(
            [
                {"date": pd.Timestamp(day), "symbol": "VOO", "close": voo},
                {"date": pd.Timestamp(day), "symbol": "BIL", "close": bil},
            ]
        )
    return pd.DataFrame(rows)


def test_benchmark_returns_charge_initial_purchase_on_next_day() -> None:
    returns = module._benchmark_daily_returns(_frame(), symbol="VOO", cost_rate=0.001)

    assert returns.tolist() == pytest.approx([0.0, (110.0 / 100.0) / 1.001 - 1.0, 100.0 / 110.0 - 1.0])


def test_benchmark_metrics_use_initial_capital_for_drawdown() -> None:
    frame = pd.DataFrame(
        [
            {"date": pd.Timestamp("2017-01-03"), "symbol": "VOO", "close": 100.0},
            {"date": pd.Timestamp("2017-01-04"), "symbol": "VOO", "close": 90.0},
            {"date": pd.Timestamp("2017-01-05"), "symbol": "VOO", "close": 90.0},
        ]
    )
    metrics = module._benchmark_metrics(frame, symbol="VOO", cost_rate=0.001)

    assert metrics["max_drawdown"] == pytest.approx((90.0 / 100.0) / 1.001 - 1.0)
    assert metrics["observation_count"] == 3


@pytest.mark.parametrize(
    ("field", "mutator", "message"),
    (
        ("source", lambda value: value["source"].update(generation="wrong"), "replay source rejected"),
        ("window", lambda value: value["window"].update(end="2024-12-30"), "replay window rejected"),
        ("controls", lambda value: value["controls"].update(no_order=False), "replay controls rejected"),
    ),
)
def test_replay_contract_mismatch_is_rejected(field, mutator, message) -> None:
    del field
    replay = _replay_result()
    mutator(replay)

    with pytest.raises(module.BenchmarkError, match=message):
        module.validate_replay_result(replay)


def test_replay_identity_mismatch_is_rejected() -> None:
    replay = _replay_result()
    replay["runner_identity"]["us_equity_strategies"]["commit"] = "wrong"

    with pytest.raises(module.BenchmarkError, match="replay identity rejected"):
        module.validate_replay_result(replay)


class _Blob:
    def __init__(self, *, exists: bool = False, error: BaseException | None = None) -> None:
        self._exists = exists
        self.error = error
        self.uploads: list[dict[str, object]] = []

    def exists(self, *, retry, timeout):
        assert retry is None
        assert timeout == 30
        if self.error:
            raise self.error
        return self._exists

    def upload_from_string(self, payload, **kwargs):
        if self.error:
            raise self.error
        self.uploads.append({"payload": payload, **kwargs})


class _UploadErrorBlob(_Blob):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.upload_error = error
        self.upload_attempts = 0

    def upload_from_string(self, payload, **kwargs):
        self.upload_attempts += 1
        raise self.upload_error


class _Bucket:
    def __init__(self, target: _Blob) -> None:
        self.target = target
        self.calls: list[str] = []

    def blob(self, name: str, **kwargs):
        self.calls.append(name)
        assert name == module.OUTPUT_OBJECT
        assert kwargs == {}
        return self.target


class _Storage:
    def __init__(self, target: _Blob) -> None:
        self.bucket_value = _Bucket(target)

    def bucket(self, name: str):
        assert name == module.BUCKET
        return self.bucket_value


def _env() -> dict[str, str]:
    return {
        "GITHUB_ACTIONS": "true",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_WORKFLOW": module.WORKFLOW_NAME,
    }


def _benchmark_metrics_stub(*_args, **_kwargs):
    return {
        "sharpe_ratio": 0.1,
        "max_drawdown": -0.1,
        "cagr": 0.02,
        "total_return": 0.1,
        "volatility": 0.1,
        "observation_count": 2012,
    }


def test_existing_output_stops_before_snapshot_or_replay_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _Storage(_Blob(exists=True))
    monkeypatch.setattr(module, "load_snapshot", lambda *_args: pytest.fail("snapshot read"))
    monkeypatch.setattr(module, "load_replay_result", lambda *_args: pytest.fail("replay read"))

    with pytest.raises(module.BenchmarkError, match="target object already exists"):
        module.execute_cloud(storage_client=storage, environ=_env())


def test_compare_uses_no_runner_and_uploads_create_only(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _Blob()
    storage = _Storage(target)
    monkeypatch.setattr(module, "load_snapshot", lambda *_args: (_frame(), _source()))
    monkeypatch.setattr(module, "load_replay_result", lambda *_args: _replay_result())
    monkeypatch.setattr(module, "_benchmark_metrics", _benchmark_metrics_stub)

    def fail_runner(*_args, **_kwargs):
        raise AssertionError("benchmark comparison must not call a runner")

    monkeypatch.setattr(module.replay_module, "run_replay", fail_runner)
    result = module.execute_cloud(storage_client=storage, environ=_env())

    assert result["status"] == "BENCHMARKED"
    assert set(result["benchmarks"]) == {"VOO", "BIL"}
    assert result["controls"]["promotion_eligible"] is False
    upload = target.uploads[0]
    assert upload["if_generation_match"] == 0
    assert upload["retry"] is None
    assert json.loads(upload["payload"])["replay_source"]["generation"] == module.REPLAY_GENERATION


def test_benchmark_count_mismatch_stops_before_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _Blob()
    storage = _Storage(target)
    monkeypatch.setattr(module, "load_snapshot", lambda *_args: (_frame(), _source()))
    monkeypatch.setattr(module, "load_replay_result", lambda *_args: _replay_result())
    monkeypatch.setattr(
        module,
        "_benchmark_metrics",
        lambda *_args, **_kwargs: {
            "sharpe_ratio": 0.1,
            "max_drawdown": -0.1,
            "cagr": 0.02,
            "total_return": 0.1,
            "volatility": 0.1,
            "observation_count": 2011,
        },
    )

    with pytest.raises(module.BenchmarkError, match="benchmark observation count rejected"):
        module.execute_cloud(storage_client=storage, environ=_env())
    assert target.uploads == []


def test_failed_upload_is_unknown_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    target = _UploadErrorBlob(RuntimeError("secret"))
    storage = _Storage(target)
    monkeypatch.setattr(module, "load_snapshot", lambda *_args: (_frame(), _source()))
    monkeypatch.setattr(module, "load_replay_result", lambda *_args: _replay_result())
    monkeypatch.setattr(module, "_benchmark_metrics", _benchmark_metrics_stub)

    with pytest.raises(module.BenchmarkError, match="benchmark output upload unknown") as exc_info:
        module.execute_cloud(storage_client=storage, environ=_env())
    assert "secret" not in str(exc_info.value)
    assert target.upload_attempts == 1
