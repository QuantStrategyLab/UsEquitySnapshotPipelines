from __future__ import annotations

import importlib.util
import json
import socket
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "run_global_etf_learning_replay.py"
_SPEC = importlib.util.spec_from_file_location("global_etf_learning_replay", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(module)


def _sessions() -> tuple[date, ...]:
    return (date(2016, 1, 4), date(2016, 1, 5))


def _snapshot_bytes(*, controls: dict[str, object] | None = None) -> bytes:
    rows = []
    for symbol in module.SYMBOLS:
        for index, day in enumerate(_sessions()):
            close = 100.0 + index
            rows.append(
                {
                    "date": day.isoformat(),
                    "symbol": symbol,
                    "open": close - 1.0,
                    "high": close + 1.0,
                    "low": close - 2.0,
                    "close": close,
                    "volume": 1000.0,
                }
            )
    payload = {
        "schema_version": "global_etf_learning_inputs.v1",
        "bars": rows,
        "summary": {
            "provider": "Alpaca",
            "feed": "sip",
            "adjustment": "all",
            "asof": "2024-12-31",
            "currency": "USD",
            "symbol_count": 27,
            "bar_count": len(rows),
            "first_session": _sessions()[0].isoformat(),
            "last_session": _sessions()[-1].isoformat(),
            "pit_verified": False,
        },
        "controls": controls or {"no_order": True, "promotion_eligible": False, "live_ready": False},
        "learning_only": True,
        "size_zero_required": True,
    }
    return json.dumps(payload, sort_keys=True).encode()


class _Blob:
    def __init__(self, *, data: bytes = b"", exists: bool = False, error: BaseException | None = None) -> None:
        self.data = data
        self._exists = exists
        self.error = error
        self.download_calls: list[dict[str, object]] = []
        self.uploads: list[dict[str, object]] = []

    def exists(self, *, retry, timeout):
        assert retry is None
        assert timeout == 30
        if self.error is not None:
            raise self.error
        return self._exists

    def download_as_bytes(self, **kwargs):
        self.download_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.data

    def upload_from_string(self, payload, **kwargs):
        if self.error is not None:
            raise self.error
        self.uploads.append({"payload": payload, **kwargs})


class _Bucket:
    def __init__(self, input_blob: _Blob, output_blob: _Blob) -> None:
        self.input_blob = input_blob
        self.output_blob = output_blob
        self.names: list[str] = []

    def blob(self, name: str, **kwargs):
        self.names.append(name)
        if name == module.INPUT_OBJECT:
            assert kwargs == {"generation": int(module.INPUT_GENERATION)}
            return self.input_blob
        assert name == module.OUTPUT_OBJECT
        assert kwargs == {}
        return self.output_blob


class _UploadErrorBlob(_Blob):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.upload_error = error

    def upload_from_string(self, payload, **kwargs):
        raise self.upload_error


class _Storage:
    def __init__(self, bucket: _Bucket) -> None:
        self.bucket_value = bucket
        self.names: list[str] = []

    def bucket(self, name: str):
        self.names.append(name)
        assert name == module.BUCKET
        return self.bucket_value


def _env() -> dict[str, str]:
    return {
        "GITHUB_ACTIONS": "true",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_WORKFLOW": "Global ETF Learning Replay",
    }


def _patch_small_contract(monkeypatch: pytest.MonkeyPatch, raw: bytes) -> None:
    monkeypatch.setattr(module, "_expected_xnys_sessions", lambda: _sessions())
    monkeypatch.setattr(module, "INPUT_SIZE_BYTES", len(raw))
    monkeypatch.setattr(module, "_fixed_cost_bps", lambda: 10.0)


def test_plan_only_does_not_open_network_or_print_data(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALPACA_API_KEY_ID", "secret")
    monkeypatch.setattr(socket, "socket", lambda *_args, **_kwargs: pytest.fail("network opened"))

    module.main([])

    plan = json.loads(capsys.readouterr().out)
    assert plan["status"] == "PLAN_ONLY"
    assert plan["input_generation"] == module.INPUT_GENERATION
    assert plan["output_object"] == module.OUTPUT_OBJECT
    assert plan["controls"] == {
        "no_order": True,
        "learning_only": True,
        "pit_verified": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
    }
    assert "bars" not in plan
    assert list(tmp_path.iterdir()) == []


def test_snapshot_download_binds_generation_and_validates_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot_bytes()
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    storage = _Storage(_Bucket(input_blob, _Blob()))

    frame, source = module.load_snapshot(storage)

    assert len(frame) == 54
    assert set(frame["symbol"]) == set(module.SYMBOLS)
    assert source == {
        "bucket": module.BUCKET,
        "object": module.INPUT_OBJECT,
        "generation": module.INPUT_GENERATION,
        "size_bytes": len(raw),
    }
    assert input_blob.download_calls == [{"retry": None, "timeout": 60, "if_generation_match": int(module.INPUT_GENERATION)}]


def test_invalid_controls_are_rejected_before_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot_bytes(controls={"no_order": False, "promotion_eligible": False, "live_ready": False})
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    storage = _Storage(_Bucket(input_blob, _Blob()))

    with pytest.raises(module.ReplayError, match="snapshot controls rejected"):
        module.load_snapshot(storage)
    assert len(input_blob.download_calls) == 1


def test_empty_snapshot_is_rejected_without_a_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = json.loads(_snapshot_bytes())
    payload["bars"] = []
    payload["summary"]["bar_count"] = 0
    raw = json.dumps(payload, sort_keys=True).encode()
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    storage = _Storage(_Bucket(input_blob, _Blob()))

    with pytest.raises(module.ReplayError, match="snapshot bar count rejected"):
        module.load_snapshot(storage)


def test_existing_output_stops_before_download_or_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot_bytes()
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    output_blob = _Blob(exists=True)
    storage = _Storage(_Bucket(input_blob, output_blob))
    calls: list[object] = []

    with pytest.raises(module.ReplayError, match="target object already exists"):
        module.execute_cloud(storage_client=storage, runner_factory=lambda **_: calls.append(True), environ=_env())

    assert calls == []
    assert input_blob.download_calls == []


class _Result:
    sharpe_ratio = 0.1
    max_drawdown = -0.2
    cagr = 0.03
    total_return = 0.2
    volatility = 0.15
    observation_count = 100
    start_date = date(2017, 1, 1)
    end_date = date(2024, 12, 31)
    params = {"min_history_days": 260, "sma_period": 250, "confidence_weighting_enabled": True}


class _Runner:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.calls: list[tuple[object, object, object, object]] = []

    def run(self, profile, params, *, start_date, end_date):
        self.calls.append((profile, params, start_date, end_date))
        return _Result()


def test_one_replay_uses_fixed_runner_call_and_create_only_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot_bytes()
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    output_blob = _Blob()
    storage = _Storage(_Bucket(input_blob, output_blob))
    runners: list[_Runner] = []

    def factory(**kwargs):
        runner = _Runner(**kwargs)
        runners.append(runner)
        return runner

    result = module.execute_cloud(storage_client=storage, runner_factory=factory, environ=_env())

    assert len(runners) == 1
    runner = runners[0]
    assert isinstance(runner.kwargs["market_history"], pd.DataFrame)
    assert runner.calls == [("global_etf_rotation", {"min_history_days": 260}, date(2017, 1, 1), date(2024, 12, 31))]
    assert result["metrics"] == {
        "sharpe_ratio": 0.1,
        "max_drawdown": -0.2,
        "cagr": 0.03,
        "total_return": 0.2,
        "volatility": 0.15,
        "observation_count": 100,
    }
    assert result["window"] == {"start": "2017-01-01", "end": "2024-12-31"}
    assert result["params"] == _Result.params
    assert result["controls"]["no_order"] is True
    assert result["controls"]["learning_only"] is True
    assert output_blob.uploads[0]["if_generation_match"] == 0
    assert output_blob.uploads[0]["retry"] is None
    assert output_blob.uploads[0]["content_type"] == "application/json"
    assert json.loads(output_blob.uploads[0]["payload"])["source"]["generation"] == module.INPUT_GENERATION


def test_upload_unknown_is_sanitized_and_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot_bytes()
    _patch_small_contract(monkeypatch, raw)
    input_blob = _Blob(data=raw)
    output_blob = _UploadErrorBlob(RuntimeError("secret response"))
    storage = _Storage(_Bucket(input_blob, output_blob))

    with pytest.raises(module.ReplayError, match="replay output upload unknown") as exc_info:
        module.execute_cloud(storage_client=storage, runner_factory=lambda **_: _Runner(), environ=_env())
    assert "secret" not in str(exc_info.value)
    assert len(output_blob.uploads) == 0


def test_actual_runner_synthetic_integration(monkeypatch: pytest.MonkeyPatch) -> None:
    strategies = pytest.importorskip("us_equity_strategies.strategies.global_etf_rotation")
    runner_module = pytest.importorskip("us_equity_strategies.backtest.orchestrator_runner")
    dates = tuple(pd.bdate_range("2016-01-04", periods=300).date)
    symbols = tuple(strategies.RANKING_POOL + strategies.CANARY_ASSETS + [strategies.SAFE_HAVEN])
    rows = [
        {"date": day, "symbol": symbol, "close": 100.0 + index * 0.01}
        for index, day in enumerate(dates)
        for symbol in dict.fromkeys(symbols)
    ]
    frame = pd.DataFrame(rows)

    runner = runner_module.UsEtfRotationBacktestRunner(market_history=frame)
    result = module.run_replay(frame, source={"generation": module.INPUT_GENERATION}, runner=runner)

    assert result["runner_revision"] == module.RUNNER_REVISION
    assert result["runner_identity"]["quant_platform_kit"]["commit"] == "5c916917626707c4ee798c6b45a5d43609019816"
    assert result["window"] == {"start": "2017-01-01", "end": "2024-12-31"}
    assert result["controls"]["no_order"] is True
