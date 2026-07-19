from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner


AS_OF = "2026-07-17"
SESSION_ID = f"XNAS:{AS_OF}"
QSP_SHA = "1966235aaed08df4c4b2004b0ae7015f7574a192"


@dataclass(frozen=True)
class _Decision:
    positions: tuple[object, ...] = ()
    budgets: tuple[object, ...] = ()
    risk_flags: tuple[str, ...] = ()
    diagnostics: dict[str, object] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.diagnostics is None:
            object.__setattr__(self, "diagnostics", {"risk_gated": True})


@pytest.fixture
def market_csv(tmp_path: Path) -> Path:
    path = tmp_path / "market.csv"
    rows = ["session_date,close"]
    first_day = date.fromisoformat(AS_OF) - timedelta(days=251)
    for offset in range(252):
        day = (first_day + timedelta(days=offset)).isoformat()
        rows.append(f"{day},{100 + offset}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


@pytest.fixture
def output_parent(tmp_path: Path) -> Path:
    parent = tmp_path / "output"
    parent.mkdir()
    return parent


@pytest.fixture
def stable_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "_source_commit", lambda: "a" * 40)
    monkeypatch.setattr(runner, "_validate_runtime_identities", lambda: None)
    monkeypatch.setattr(runner, "_default_config", lambda: {"alpha": 1})


def _run(market_csv: Path, output_parent: Path, **kwargs: object):
    return runner.run_tqqq_local_no_order(
        benchmark_history_csv=market_csv,
        as_of=AS_OF,
        session_id=SESSION_ID,
        output_parent=output_parent,
        **kwargs,
    )


def test_absent_happy_path_is_single_compute_canonical_two_file_package(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, output_parent: Path, stable_identity: None
) -> None:
    decision = _Decision()
    calls = []
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: calls.append(ctx) or decision)

    actual, package = _run(market_csv, output_parent)

    assert actual is decision
    assert len(calls) == 1
    assert calls[0].runtime_config == {}
    assert sorted(path.name for path in package.iterdir()) == ["decision.json", "input_envelope.json"]
    envelope = json.loads((package / "input_envelope.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"] == {"status": "ABSENT"}
    assert envelope["portfolio"]["value"]["cash_balance"] == 100000.0


def test_present_control_binds_exact_local_bytes_without_provenance_claim(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, output_parent: Path, stable_identity: None, tmp_path: Path
) -> None:
    control = {"plugin": "market_regime_control", "as_of": AS_OF, "signal": "risk_on"}
    control_path = tmp_path / "control.json"
    control_bytes = json.dumps(control, separators=(",", ":")).encode()
    control_path.write_bytes(control_bytes)
    captured = []
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: captured.append(ctx) or _Decision())

    _, package = _run(market_csv, output_parent, plugin_control_json=control_path, qsp_commit_sha=QSP_SHA)

    envelope = json.loads((package / "input_envelope.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"]["status"] == "PRESENT"
    assert "producer" not in envelope["plugin_control"]
    assert captured[0].portfolio.metadata == {"market_regime_control": control}


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda text: text.replace("session_date,close", "date,close"), "T2B1_INPUT_INVALID"),
        (lambda text: "\n".join([*text.splitlines()[:2], text.splitlines()[1], *text.splitlines()[3:]]) + "\n", "T2B1_INPUT_INVALID"),
        (lambda text: text.replace(",102", ",-1", 1), "T2B1_INPUT_INVALID"),
        (lambda text: "\n".join(text.splitlines()[:252]) + "\n", "T2B1_INPUT_INVALID"),
    ],
)
def test_invalid_forward_market_input_fails_before_compute(
    monkeypatch: pytest.MonkeyPatch,
    market_csv: Path,
    output_parent: Path,
    stable_identity: None,
    mutation,
    code: str,
) -> None:
    market_csv.write_text(mutation(market_csv.read_text(encoding="utf-8")), encoding="utf-8")
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: pytest.fail("must not compute"))

    with pytest.raises(runner._T2B1Error, match=code):
        _run(market_csv, output_parent)


def test_envelope_and_decision_bytes_are_deterministic(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, tmp_path: Path, stable_identity: None
) -> None:
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: _Decision())
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    _, first_package = _run(market_csv, first)
    _, second_package = _run(market_csv, second)

    for name in ("input_envelope.json", "decision.json"):
        assert (first_package / name).read_bytes() == (second_package / name).read_bytes()


def test_code_identity_failure_precedes_compute(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, output_parent: Path
) -> None:
    monkeypatch.setattr(runner, "_source_commit", lambda: None)
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: pytest.fail("must not compute"))

    with pytest.raises(runner._T2B1Error, match="T2B1_CODE_IDENTITY_INVALID"):
        _run(market_csv, output_parent)


@pytest.mark.parametrize("fault", ["serialize", "write", "readback", "rename"])
def test_post_compute_evidence_fault_preserves_same_decision(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, output_parent: Path, stable_identity: None, fault: str
) -> None:
    decision = _Decision()
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: decision)
    target = {"serialize": "_decision_bytes", "write": "_write_stage", "readback": "_strict_readback", "rename": "_publish_stage"}[fault]
    monkeypatch.setattr(runner, target, lambda *args, **kwargs: (_ for _ in ()).throw(OSError(fault)))

    with pytest.raises(runner._T2B1Error) as raised:
        _run(market_csv, output_parent)

    assert raised.value.code in {"T2B1_STAGE_FAILED", "T2B1_READBACK_FAILED", "T2B1_PUBLISH_FAILED"}
    assert raised.value.decision is decision
    assert not list(output_parent.glob("tqqq-local-no-order-*"))


def test_existing_final_destination_fails_closed(
    monkeypatch: pytest.MonkeyPatch, market_csv: Path, output_parent: Path, stable_identity: None
) -> None:
    monkeypatch.setattr(runner, "compute_tqqq_growth_income_decision", lambda ctx: _Decision())
    _, package = _run(market_csv, output_parent)
    package.rename(output_parent / package.name)

    with pytest.raises(runner._T2B1Error, match="T2B1_INPUT_INVALID"):
        _run(market_csv, output_parent)


@pytest.mark.parametrize("kind", ["extra", "symlink", "duplicate", "noncanonical"])
def test_strict_readback_rejects_invalid_stage_packages(tmp_path: Path, kind: str) -> None:
    stage = tmp_path / "stage"
    stage.mkdir()
    envelope = b'{"schema":"x","schema":"y"}' if kind == "duplicate" else b'{}'
    decision = b'{ }' if kind == "noncanonical" else b'{}'
    (stage / "input_envelope.json").write_bytes(envelope)
    (stage / "decision.json").write_bytes(decision)
    if kind == "extra":
        (stage / "unexpected.json").write_text("{}", encoding="utf-8")
    if kind == "symlink":
        (stage / "decision.json").unlink()
        (stage / "decision.json").symlink_to(stage / "input_envelope.json")

    with pytest.raises((ValueError, OSError, json.JSONDecodeError)):
        runner._strict_readback(stage, envelope, decision)
