"""Exact-clean source-checkout runner for one local, no-order TQQQ decision."""

from __future__ import annotations

import base64
import csv
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import mkdtemp
from typing import Any, Literal, Mapping


QPK_PIN = "ff09c889ed21e2eb6fcb37f6cdaa159190ec82da"
UES_PIN = "b0b590ce5ac0233a40b8fb957c249cf375a6ff21"
PROFILE = "tqqq_growth_income"
MODE = "local_private_no_order"
ENVELOPE_SCHEMA = "qsl.tqqq_forward_input_envelope.v1"
DECISION_SCHEMA = "qsl.tqqq_local_no_order_decision.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class TqqqForwardInputEnvelope:
    schema: str
    as_of: str
    session_id: str
    uesp_commit_sha: str
    market_csv_bytes: bytes
    merged_config_json: bytes
    portfolio_json: bytes
    plugin_control_status: Literal["ABSENT"]


class _RunnerError(RuntimeError):
    def __init__(self, code: str, decision: Any | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.decision = decision


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _git(checkout: Path, *args: str) -> str:
    try:
        return subprocess.run(
            ["git", *args], cwd=checkout, check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID") from exc


def _source_identity() -> tuple[Path, str]:
    checkout = Path(_git(Path.cwd(), "rev-parse", "--show-toplevel")).resolve()
    if Path.cwd().resolve() != checkout:
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID")
    origin = Path(__file__).resolve()
    expected = checkout / "src" / "us_equity_snapshot_pipelines" / "tqqq_local_no_order_runner.py"
    if origin != expected or _git(checkout, "ls-files", "--error-unmatch", "--", str(origin.relative_to(checkout))) != str(
        origin.relative_to(checkout)
    ):
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID")
    if _git(checkout, "status", "--porcelain"):
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID")
    head = _git(checkout, "rev-parse", "HEAD")
    if not _SHA256_RE.fullmatch(head):
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID")
    return checkout, head


def _runtime_pin(project: str, expected: str) -> None:
    try:
        direct_url = json.loads(importlib.metadata.distribution(project).read_text("direct_url.json") or "{}")
    except (importlib.metadata.PackageNotFoundError, json.JSONDecodeError) as exc:
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID") from exc
    commit_id = direct_url.get("vcs_info", {}).get("commit_id")
    if commit_id != expected:
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID")


def _parse_inputs(csv_path: str | Path, as_of: str, session_id: str) -> tuple[bytes, list[tuple[str, float]]]:
    try:
        parsed_as_of = date.fromisoformat(as_of)
        raw = Path(csv_path).read_bytes()
        text = raw.decode("utf-8")
        rows = list(csv.reader(text.splitlines()))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise _RunnerError("T2B1_INPUT_INVALID") from exc
    if session_id != f"XNAS:{parsed_as_of.isoformat()}" or not rows or rows[0] != ["session_date", "close"]:
        raise _RunnerError("T2B1_INPUT_INVALID")
    values: list[tuple[str, float]] = []
    previous: date | None = None
    for row in rows[1:]:
        if len(row) != 2:
            raise _RunnerError("T2B1_INPUT_INVALID")
        try:
            session_date = date.fromisoformat(row[0])
            close = float(row[1])
        except ValueError as exc:
            raise _RunnerError("T2B1_INPUT_INVALID") from exc
        if previous is not None and session_date <= previous or not math.isfinite(close) or close <= 0.0:
            raise _RunnerError("T2B1_INPUT_INVALID")
        previous = session_date
        values.append((session_date.isoformat(), close))
    if len(values) < 252 or not values or values[-1][0] != parsed_as_of.isoformat():
        raise _RunnerError("T2B1_INPUT_INVALID")
    return raw, values


def _normalize(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: _normalize(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _normalize(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(child) for child in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite value")
    return value


def _read_canonical(path: Path) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    return json.loads(path.read_bytes(), object_pairs_hook=no_duplicates, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))


def _strict_readback(
    stage: Path, envelope_bytes: bytes, decision_bytes: bytes, plugin_control: Mapping[str, Any]
) -> None:
    files = tuple(stage.iterdir())
    expected = {"input_envelope.json", "decision.json"}
    if {item.name for item in files} != expected or any(not item.is_file() or item.is_symlink() for item in files):
        raise _RunnerError("T2B1_READBACK_FAILED")
    envelope_path = stage / "input_envelope.json"
    decision_path = stage / "decision.json"
    if envelope_path.read_bytes() != envelope_bytes or decision_path.read_bytes() != decision_bytes:
        raise _RunnerError("T2B1_READBACK_FAILED")
    try:
        envelope = _read_canonical(envelope_path)
        decision = _read_canonical(decision_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise _RunnerError("T2B1_READBACK_FAILED") from exc
    if set(envelope) != {
        "as_of", "code", "market", "merged_config", "mode", "plugin_control", "portfolio", "profile", "schema", "session_id"
    } or envelope.get("schema") != ENVELOPE_SCHEMA or envelope.get("plugin_control") != plugin_control:
        raise _RunnerError("T2B1_READBACK_FAILED")
    if set(decision) != {"budgets", "diagnostics", "input_envelope_sha256", "positions", "risk_flags", "schema"}:
        raise _RunnerError("T2B1_READBACK_FAILED")
    if decision.get("schema") != DECISION_SCHEMA or decision.get("input_envelope_sha256") != _sha256(envelope_bytes):
        raise _RunnerError("T2B1_READBACK_FAILED")


def _build_envelope_payload(
    envelope: TqqqForwardInputEnvelope,
    market_rows: list[tuple[str, float]],
    config: Mapping[str, Any],
    portfolio: Mapping[str, Any],
    plugin_control: Mapping[str, Any],
) -> tuple[bytes, str]:
    market = {
        "bytes_b64": base64.b64encode(envelope.market_csv_bytes).decode("ascii"),
        "first_date": market_rows[0][0],
        "last_date": market_rows[-1][0],
        "row_count": len(market_rows),
        "sha256": _sha256(envelope.market_csv_bytes),
    }
    payload = {
        "as_of": envelope.as_of,
        "code": {
            "qpk": f"QuantStrategyLab/QuantPlatformKit@{QPK_PIN}",
            "ues": f"QuantStrategyLab/UsEquityStrategies@{UES_PIN}",
            "uesp": f"QuantStrategyLab/UsEquitySnapshotPipelines@{envelope.uesp_commit_sha}",
        },
        "market": market,
        "merged_config": {"sha256": _sha256(envelope.merged_config_json), "value": config},
        "mode": MODE,
        "plugin_control": dict(plugin_control),
        "portfolio": {"sha256": _sha256(envelope.portfolio_json), "value": portfolio},
        "profile": PROFILE,
        "schema": envelope.schema,
        "session_id": envelope.session_id,
    }
    encoded = _canonical_json(payload)
    return encoded, _sha256(encoded)


def _run_tqqq_local_no_order(
    *,
    benchmark_history_csv: str | Path,
    as_of: str,
    session_id: str,
    output_parent: str | Path,
    plugin_control: Mapping[str, Any],
) -> tuple[Any, Path]:
    """Compute and atomically publish evidence while preserving decision-bearing inputs.

    ``plugin_control`` is evidence-only: callers must verify it before reaching this
    core, and it is serialized only as ``input_envelope.plugin_control``.
    """
    checkout, uesp_head = _source_identity()
    _runtime_pin("us-equity-strategies", UES_PIN)
    _runtime_pin("quant-platform-kit", QPK_PIN)
    parent = Path(output_parent).resolve()
    if not parent.is_dir() or checkout == parent or checkout in parent.parents:
        raise _RunnerError("T2B1_INPUT_INVALID")
    market_bytes, market_rows = _parse_inputs(benchmark_history_csv, as_of, session_id)
    try:
        import pandas as pd
        from quant_platform_kit.common.models import PortfolioSnapshot
        from quant_platform_kit.common.strategy_contracts import StrategyContext
        from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision
        from us_equity_strategies.manifests import tqqq_growth_income_manifest
    except ImportError as exc:
        raise _RunnerError("T2B1_CODE_IDENTITY_INVALID") from exc
    config = _normalize(dict(tqqq_growth_income_manifest.default_config))
    portfolio_value = {
        "as_of": f"{as_of}T00:00:00+00:00",
        "buying_power": 100000.0,
        "cash_balance": 100000.0,
        "metadata": {},
        "positions": [],
        "total_equity": 100000.0,
    }
    envelope = TqqqForwardInputEnvelope(
        schema=ENVELOPE_SCHEMA,
        as_of=as_of,
        session_id=session_id,
        uesp_commit_sha=uesp_head,
        market_csv_bytes=market_bytes,
        merged_config_json=_canonical_json(config),
        portfolio_json=_canonical_json(portfolio_value),
        plugin_control_status="ABSENT",
    )
    envelope_bytes, envelope_sha = _build_envelope_payload(
        envelope, market_rows, config, portfolio_value, plugin_control
    )
    destination = parent / f"tqqq-local-no-order-{as_of}-{envelope_sha}"
    if destination.exists():
        raise _RunnerError("T2B1_INPUT_INVALID")
    portfolio = PortfolioSnapshot(
        as_of=datetime.fromisoformat(portfolio_value["as_of"]),
        total_equity=100000.0,
        buying_power=100000.0,
        cash_balance=100000.0,
        positions=(),
        metadata={},
    )
    context = StrategyContext(
        as_of=as_of,
        market_data={"benchmark_history": pd.DataFrame(market_rows, columns=["session_date", "close"])},
        portfolio=portfolio,
        state={},
        runtime_config={},
        capabilities={},
        artifacts={},
    )
    try:
        decision = compute_tqqq_growth_income_decision(context)
    except Exception as exc:
        raise _RunnerError("T2B1_COMPUTE_FAILED") from exc
    stage = Path(mkdtemp(prefix=".tqqq-local-no-order-", dir=parent))
    try:
        decision_payload = {
            "budgets": _normalize(decision.budgets),
            "diagnostics": _normalize(decision.diagnostics),
            "input_envelope_sha256": envelope_sha,
            "positions": _normalize(decision.positions),
            "risk_flags": _normalize(decision.risk_flags),
            "schema": DECISION_SCHEMA,
        }
        decision_bytes = _canonical_json(decision_payload)
        (stage / "input_envelope.json").write_bytes(envelope_bytes)
        (stage / "decision.json").write_bytes(decision_bytes)
    except Exception as exc:
        shutil.rmtree(stage, ignore_errors=True)
        raise _RunnerError("T2B1_STAGE_FAILED", decision) from exc
    try:
        _strict_readback(stage, envelope_bytes, decision_bytes, plugin_control)
    except _RunnerError as exc:
        shutil.rmtree(stage, ignore_errors=True)
        exc.decision = decision
        raise
    try:
        stage.rename(destination)
    except OSError as exc:
        shutil.rmtree(stage, ignore_errors=True)
        raise _RunnerError("T2B1_PUBLISH_FAILED", decision) from exc
    return decision, destination


def run_tqqq_local_no_order(
    *, benchmark_history_csv: str | Path, as_of: str, session_id: str, output_parent: str | Path
) -> tuple[Any, Path]:
    """Run the frozen ABSENT-only local no-order path without plugin-control inputs."""
    return _run_tqqq_local_no_order(
        benchmark_history_csv=benchmark_history_csv,
        as_of=as_of,
        session_id=session_id,
        output_parent=output_parent,
        plugin_control={"status": "ABSENT"},
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 8 or args[::2] != ["--benchmark-history-csv", "--as-of", "--session-id", "--output-parent"]:
        print("ERROR T2B1_INPUT_INVALID", file=sys.stderr)
        return 2
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec is None or main_spec.name != __spec__.name:
        print("ERROR T2B1_CODE_IDENTITY_INVALID", file=sys.stderr)
        return 2
    try:
        _, destination = run_tqqq_local_no_order(
            benchmark_history_csv=args[1], as_of=args[3], session_id=args[5], output_parent=args[7]
        )
    except _RunnerError as exc:
        print(f"ERROR {exc.code}", file=sys.stderr)
        return {"T2B1_CODE_IDENTITY_INVALID": 2, "T2B1_INPUT_INVALID": 2, "T2B1_COMPUTE_FAILED": 3}.get(exc.code, 4)
    except Exception:
        print("ERROR T2B1_INTERNAL", file=sys.stderr)
        return 70
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
