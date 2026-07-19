"""Local-private, forward-only TQQQ decision evidence runner."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import pandas as pd
from quant_platform_kit import PortfolioSnapshot
from quant_platform_kit.strategy_contracts import StrategyContext, StrategyDecision
from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision
from us_equity_strategies.manifests import tqqq_growth_income_manifest

_QPK_SHA = "ff09c889ed21e2eb6fcb37f6cdaa159190ec82da"
_UES_SHA = "b0b590ce5ac0233a40b8fb957c249cf375a6ff21"
_QSP_SHA = "1966235aaed08df4c4b2004b0ae7015f7574a192"
_SHA40 = set("0123456789abcdef")
_ENVELOPE_SCHEMA = "qsl.tqqq_forward_input_envelope.v1"
_DECISION_SCHEMA = "qsl.tqqq_local_no_order_decision.v1"


@dataclass(frozen=True, slots=True)
class TqqqForwardInputEnvelope:
    schema: str
    as_of: str
    session_id: str
    uesp_commit_sha: str
    market_csv_bytes: bytes
    merged_config_json: bytes
    portfolio_json: bytes
    plugin_control_status: Literal["ABSENT", "PRESENT"]
    plugin_control_json_bytes: bytes | None
    qsp_commit_sha: str | None


class _T2B1Error(RuntimeError):
    def __init__(self, code: str, *, decision: StrategyDecision | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.decision = decision


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_object_bytes(value: bytes) -> Mapping[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = item
        return result

    def reject_constant(_: str) -> None:
        raise ValueError("non-finite JSON constant")

    decoded = json.loads(value.decode("utf-8"), object_pairs_hook=reject_duplicates, parse_constant=reject_constant)
    if not isinstance(decoded, Mapping):
        raise ValueError("JSON root must be an object")
    return decoded


def _checkout_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _source_commit() -> str | None:
    root = _checkout_root()
    try:
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain"], check=True, capture_output=True, text=True
        )
        if status.stdout:
            return None
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return head if len(head) == 40 and set(head) <= _SHA40 else None


def _distribution_commit(distribution: str) -> str | None:
    try:
        direct_url = importlib.metadata.distribution(distribution).read_text("direct_url.json")
        payload = json.loads(direct_url or "{}")
        commit = payload.get("vcs_info", {}).get("commit_id")
    except (importlib.metadata.PackageNotFoundError, ValueError, TypeError):
        return None
    return commit if isinstance(commit, str) else None


def _validate_runtime_identities() -> None:
    if _distribution_commit("us-equity-strategies") != _UES_SHA or _distribution_commit("quant-platform-kit") != _QPK_SHA:
        raise _T2B1Error("T2B1_CODE_IDENTITY_INVALID")


def _input_error() -> _T2B1Error:
    return _T2B1Error("T2B1_INPUT_INVALID")


def _parse_market(market_bytes: bytes, as_of: str, session_id: str) -> pd.DataFrame:
    if session_id != f"XNAS:{as_of}":
        raise _input_error()
    try:
        parsed_as_of = date.fromisoformat(as_of)
        if parsed_as_of.isoformat() != as_of:
            raise ValueError
        rows = list(csv.reader(market_bytes.decode("utf-8").splitlines()))
    except (UnicodeDecodeError, ValueError, csv.Error):
        raise _input_error() from None
    if not rows or rows[0] != ["session_date", "close"] or len(rows) < 253:
        raise _input_error()
    dates: list[str] = []
    closes: list[float] = []
    previous: str | None = None
    for row in rows[1:]:
        if len(row) != 2:
            raise _input_error()
        try:
            parsed_date = date.fromisoformat(row[0])
            if parsed_date.isoformat() != row[0]:
                raise ValueError
            close = float(row[1])
        except ValueError:
            raise _input_error() from None
        if not math.isfinite(close) or close <= 0 or (previous is not None and row[0] <= previous):
            raise _input_error()
        previous = row[0]
        dates.append(row[0])
        closes.append(close)
    if dates[-1] != as_of:
        raise _input_error()
    return pd.DataFrame({"session_date": dates, "close": closes})


def _default_config() -> Mapping[str, Any]:
    return dict(tqqq_growth_income_manifest.default_config)


def _plugin_control(
    plugin_control_json: str | Path | None, qsp_commit_sha: str | None, as_of: str
) -> tuple[Literal["ABSENT", "PRESENT"], bytes | None, str | None, Mapping[str, Any] | None]:
    if plugin_control_json is None and qsp_commit_sha is None:
        return "ABSENT", None, None, None
    if plugin_control_json is None or qsp_commit_sha != _QSP_SHA:
        raise _T2B1Error("T2B1_PLUGIN_CONTROL_INVALID")
    try:
        control_bytes = Path(plugin_control_json).read_bytes()
        control = _json_object_bytes(control_bytes)
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise _T2B1Error("T2B1_PLUGIN_CONTROL_INVALID") from None
    if control.get("plugin", control.get("profile")) != "market_regime_control" or control.get("as_of") != as_of:
        raise _T2B1Error("T2B1_PLUGIN_CONTROL_INVALID")
    return "PRESENT", control_bytes, qsp_commit_sha, control


def _build_envelope(
    *,
    as_of: str,
    session_id: str,
    uesp_commit_sha: str,
    market_bytes: bytes,
    config_bytes: bytes,
    portfolio_bytes: bytes,
    plugin_status: Literal["ABSENT", "PRESENT"],
    plugin_bytes: bytes | None,
    qsp_commit_sha: str | None,
    market_frame: pd.DataFrame,
) -> tuple[TqqqForwardInputEnvelope, bytes]:
    envelope = TqqqForwardInputEnvelope(
        schema=_ENVELOPE_SCHEMA,
        as_of=as_of,
        session_id=session_id,
        uesp_commit_sha=uesp_commit_sha,
        market_csv_bytes=market_bytes,
        merged_config_json=config_bytes,
        portfolio_json=portfolio_bytes,
        plugin_control_status=plugin_status,
        plugin_control_json_bytes=plugin_bytes,
        qsp_commit_sha=qsp_commit_sha,
    )
    plugin_wire: dict[str, Any] = {"status": plugin_status}
    if plugin_status == "PRESENT":
        assert plugin_bytes is not None and qsp_commit_sha is not None
        plugin_wire = {
            "artifact_sha256": _sha256(plugin_bytes),
            "content_base64": base64.b64encode(plugin_bytes).decode("ascii"),
            "format": "json:utf-8",
            "qsp_commit_sha": qsp_commit_sha,
            "status": "PRESENT",
        }
    wire = {
        "as_of": envelope.as_of,
        "code": {
            "qpk": {"commit_sha": _QPK_SHA, "repository": "QuantStrategyLab/QuantPlatformKit"},
            "ues": {"commit_sha": _UES_SHA, "repository": "QuantStrategyLab/UsEquityStrategies"},
            "uesp": {"commit_sha": envelope.uesp_commit_sha, "repository": "QuantStrategyLab/UsEquitySnapshotPipelines"},
        },
        "market": {
            "content_base64": base64.b64encode(envelope.market_csv_bytes).decode("ascii"),
            "first_session_date": str(market_frame.iloc[0]["session_date"]),
            "format": "csv:utf-8",
            "last_session_date": str(market_frame.iloc[-1]["session_date"]),
            "row_count": len(market_frame),
            "sha256": _sha256(envelope.market_csv_bytes),
        },
        "merged_config": {"sha256": _sha256(envelope.merged_config_json), "value": _json_object_bytes(envelope.merged_config_json)},
        "mode": "local_private_no_order",
        "plugin_control": plugin_wire,
        "portfolio": {"sha256": _sha256(envelope.portfolio_json), "value": _json_object_bytes(envelope.portfolio_json)},
        "profile": "tqqq_growth_income",
        "schema": envelope.schema,
        "session_id": envelope.session_id,
    }
    return envelope, _canonical_json(wire)


def _normalize_json(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite value")
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _normalize_json(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or key in normalized:
                raise ValueError("invalid mapping key")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, (tuple, list)):
        return [_normalize_json(item) for item in value]
    if hasattr(value, "item"):
        return _normalize_json(value.item())
    raise ValueError("unsupported evidence value")


def _decision_bytes(decision: StrategyDecision, envelope_sha256: str) -> bytes:
    return _canonical_json(
        {
            "budgets": _normalize_json(getattr(decision, "budgets", ())),
            "diagnostics": _normalize_json(getattr(decision, "diagnostics", {})),
            "input_envelope_sha256": envelope_sha256,
            "positions": _normalize_json(getattr(decision, "positions", ())),
            "risk_flags": _normalize_json(getattr(decision, "risk_flags", ())),
            "schema": _DECISION_SCHEMA,
        }
    )


def _write_stage(stage: Path, envelope_bytes: bytes, decision_bytes: bytes) -> None:
    (stage / "input_envelope.json").write_bytes(envelope_bytes)
    (stage / "decision.json").write_bytes(decision_bytes)


def _strict_readback(stage: Path, envelope_bytes: bytes, decision_bytes: bytes) -> None:
    expected_names = {"input_envelope.json", "decision.json"}
    entries = list(stage.iterdir())
    if {entry.name for entry in entries} != expected_names or any(entry.is_symlink() or not entry.is_file() for entry in entries):
        raise ValueError("package members")
    actual_envelope = (stage / "input_envelope.json").read_bytes()
    actual_decision = (stage / "decision.json").read_bytes()
    if actual_envelope != envelope_bytes or actual_decision != decision_bytes:
        raise ValueError("noncanonical bytes")
    envelope = _json_object_bytes(actual_envelope)
    decision = _json_object_bytes(actual_decision)
    if set(envelope) != {"as_of", "code", "market", "merged_config", "mode", "plugin_control", "portfolio", "profile", "schema", "session_id"}:
        raise ValueError("envelope shape")
    if set(decision) != {"schema", "input_envelope_sha256", "positions", "budgets", "risk_flags", "diagnostics"}:
        raise ValueError("decision shape")
    if _canonical_json(envelope) != actual_envelope or _canonical_json(decision) != actual_decision:
        raise ValueError("canonical form")
    if envelope["schema"] != _ENVELOPE_SCHEMA or envelope["mode"] != "local_private_no_order" or envelope["profile"] != "tqqq_growth_income":
        raise ValueError("fixed fields")
    if envelope["code"] != {
        "qpk": {"commit_sha": _QPK_SHA, "repository": "QuantStrategyLab/QuantPlatformKit"},
        "ues": {"commit_sha": _UES_SHA, "repository": "QuantStrategyLab/UsEquityStrategies"},
        "uesp": {"commit_sha": envelope["code"]["uesp"].get("commit_sha"), "repository": "QuantStrategyLab/UsEquitySnapshotPipelines"},
    }:
        raise ValueError("code identity")
    market = envelope["market"]
    market_bytes = base64.b64decode(market["content_base64"], validate=True)
    market_frame = _parse_market(market_bytes, envelope["as_of"], envelope["session_id"])
    if market["sha256"] != _sha256(market_bytes) or market["row_count"] != len(market_frame):
        raise ValueError("market digest")
    for key in ("merged_config", "portfolio"):
        if envelope[key]["sha256"] != _sha256(_canonical_json(envelope[key]["value"])):
            raise ValueError(f"{key} digest")
    plugin = envelope["plugin_control"]
    if plugin.get("status") == "ABSENT":
        if plugin != {"status": "ABSENT"}:
            raise ValueError("absent plugin")
    elif plugin.get("status") == "PRESENT":
        plugin_bytes = base64.b64decode(plugin["content_base64"], validate=True)
        if plugin.get("qsp_commit_sha") != _QSP_SHA or plugin.get("artifact_sha256") != _sha256(plugin_bytes):
            raise ValueError("plugin digest")
        _plugin_control_bytes = _json_object_bytes(plugin_bytes)
        if (
            _plugin_control_bytes.get("plugin", _plugin_control_bytes.get("profile")) != "market_regime_control"
            or _plugin_control_bytes.get("as_of") != envelope["as_of"]
        ):
            raise ValueError("plugin profile")
    else:
        raise ValueError("plugin status")
    if decision["schema"] != _DECISION_SCHEMA or decision["input_envelope_sha256"] != _sha256(actual_envelope):
        raise ValueError("decision link")


def _publish_stage(stage: Path, final_path: Path) -> None:
    os.replace(stage, final_path)


def run_tqqq_local_no_order(
    *,
    benchmark_history_csv: str | Path,
    as_of: str,
    session_id: str,
    output_parent: str | Path,
    plugin_control_json: str | Path | None = None,
    qsp_commit_sha: str | None = None,
) -> tuple[StrategyDecision, Path]:
    source_commit = _source_commit()
    if source_commit is None:
        raise _T2B1Error("T2B1_CODE_IDENTITY_INVALID")
    parent = Path(output_parent).resolve()
    if not parent.is_dir() or parent == _checkout_root() or _checkout_root() in parent.parents:
        raise _input_error()
    try:
        market_bytes = Path(benchmark_history_csv).read_bytes()
    except OSError:
        raise _input_error() from None
    market_frame = _parse_market(market_bytes, as_of, session_id)
    plugin_status, plugin_bytes, plugin_sha, plugin_value = _plugin_control(plugin_control_json, qsp_commit_sha, as_of)
    _validate_runtime_identities()
    config_bytes = _canonical_json(_default_config())
    portfolio_value = {
        "as_of": f"{as_of}T00:00:00+00:00",
        "buying_power": 100000.0,
        "cash_balance": 100000.0,
        "metadata": {} if plugin_value is None else {"market_regime_control": plugin_value},
        "positions": [],
        "total_equity": 100000.0,
    }
    portfolio_bytes = _canonical_json(portfolio_value)
    envelope, envelope_bytes = _build_envelope(
        as_of=as_of,
        session_id=session_id,
        uesp_commit_sha=source_commit,
        market_bytes=market_bytes,
        config_bytes=config_bytes,
        portfolio_bytes=portfolio_bytes,
        plugin_status=plugin_status,
        plugin_bytes=plugin_bytes,
        qsp_commit_sha=plugin_sha,
        market_frame=market_frame,
    )
    portfolio = PortfolioSnapshot(
        as_of=datetime.fromisoformat(portfolio_value["as_of"]).replace(tzinfo=timezone.utc),
        total_equity=100000.0,
        buying_power=100000.0,
        cash_balance=100000.0,
        positions=(),
        metadata=dict(portfolio_value["metadata"]),
    )
    context = StrategyContext(
        as_of=envelope.as_of,
        market_data={"benchmark_history": market_frame},
        portfolio=portfolio,
        state={},
        runtime_config={},
        capabilities={},
        artifacts={},
    )
    try:
        decision = compute_tqqq_growth_income_decision(context)
    except Exception as exc:
        raise _T2B1Error("T2B1_COMPUTE_FAILED") from exc
    final_path = parent / f"tqqq-local-no-order-{as_of}-{_sha256(envelope_bytes)}"
    if final_path.exists():
        raise _input_error()
    stage: Path | None = None
    try:
        stage = Path(tempfile.mkdtemp(prefix=".tqqq-local-no-order-", dir=parent))
        decision_bytes = _decision_bytes(decision, _sha256(envelope_bytes))
        _write_stage(stage, envelope_bytes, decision_bytes)
    except Exception as exc:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)
        raise _T2B1Error("T2B1_STAGE_FAILED", decision=decision) from exc
    try:
        _strict_readback(stage, envelope_bytes, decision_bytes)
    except Exception as exc:
        shutil.rmtree(stage, ignore_errors=True)
        raise _T2B1Error("T2B1_READBACK_FAILED", decision=decision) from exc
    try:
        _publish_stage(stage, final_path)
    except Exception as exc:
        shutil.rmtree(stage, ignore_errors=True)
        raise _T2B1Error("T2B1_PUBLISH_FAILED", decision=decision) from exc
    return decision, final_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-history-csv", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--output-parent", required=True)
    parser.add_argument("--plugin-control-json")
    parser.add_argument("--qsp-commit-sha")
    args = parser.parse_args(argv)
    try:
        _, package = run_tqqq_local_no_order(
            benchmark_history_csv=args.benchmark_history_csv,
            as_of=args.as_of,
            session_id=args.session_id,
            output_parent=args.output_parent,
            plugin_control_json=args.plugin_control_json,
            qsp_commit_sha=args.qsp_commit_sha,
        )
    except _T2B1Error as exc:
        print(f"ERROR {exc.code}", file=sys.stderr)
        return {"T2B1_INPUT_INVALID": 2, "T2B1_PLUGIN_CONTROL_INVALID": 2, "T2B1_CODE_IDENTITY_INVALID": 2, "T2B1_COMPUTE_FAILED": 3}.get(exc.code, 4)
    except Exception:
        return 70
    print(package)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
