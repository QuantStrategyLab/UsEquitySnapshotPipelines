from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
import json
from pathlib import Path

from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner
from us_equity_snapshot_pipelines.tqqq_local_no_order_runner import TqqqForwardInputEnvelope, run_tqqq_local_no_order


def test_tqqq_local_no_order_runner_exposes_only_the_frozen_inputs() -> None:
    assert tuple(inspect.signature(run_tqqq_local_no_order).parameters) == (
        "benchmark_history_csv",
        "as_of",
        "session_id",
        "output_parent",
    )


def test_tqqq_forward_envelope_is_immutable_and_absent_only() -> None:
    envelope = TqqqForwardInputEnvelope(
        schema="qsl.tqqq_forward_input_envelope.v1",
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        uesp_commit_sha="a" * 40,
        market_csv_bytes=b"session_date,close\n",
        merged_config_json=b"{}",
        portfolio_json=b"{}",
        plugin_control_status="ABSENT",
    )

    assert envelope.plugin_control_status == "ABSENT"
    try:
        envelope.as_of = "2026-07-18"  # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:  # pragma: no cover - defensive assertion for dataclass semantics
        raise AssertionError("envelope must be frozen")


def _write_history(path: Path, *, as_of: str = "2026-07-17") -> None:
    rows = ["session_date,close"]
    start = date.fromisoformat(as_of) - timedelta(days=251)
    for offset in range(252):
        rows.append(f"{start + timedelta(days=offset)},{100.0 + offset}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_runner_publishes_an_absent_only_two_file_package(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    _write_history(history)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    _, package = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        output_parent=tmp_path,
    )

    assert {path.name for path in package.iterdir()} == {"input_envelope.json", "decision.json"}
    envelope = json.loads((package / "input_envelope.json").read_text(encoding="utf-8"))
    decision = json.loads((package / "decision.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"] == {"status": "ABSENT"}
    assert envelope["portfolio"]["value"]["metadata"] == {}
    assert decision["input_envelope_sha256"] == package.name.rsplit("-", 1)[-1]


def test_cli_rejects_removed_plugin_control_option(capsys) -> None:
    assert runner.main(["--plugin-control-json", "control.json"]) == 2
    assert capsys.readouterr().err == "ERROR T2B1_INPUT_INVALID\n"
