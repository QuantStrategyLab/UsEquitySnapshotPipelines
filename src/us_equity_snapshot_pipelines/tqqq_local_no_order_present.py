"""Verify one canonical PRESENT package as evidence without changing a TQQQ decision.

This bounded consumer never imports QuantStrategyPlugins and never mounts payload
fields into portfolio or strategy context. It first binds package ``inputs.prices``
to the exact benchmark bytes, then embeds the exact verified package only at
``input_envelope.plugin_control``.
"""

from __future__ import annotations

import base64
import binascii
from datetime import date
import json
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping

from . import tqqq_local_no_order_runner as runner


PRESENT_SCHEMA = "qsl.tqqq_market_regime_control_present.v1"
QSP_REPOSITORY = "QuantStrategyLab/QuantStrategyPlugins"
QSP_ENTRYPOINT = "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SENSITIVE_KEY_PARTS = ("token", "secret", "password", "cookie", "jwt", "api_key")
_PACKAGE_KEYS = {
    "as_of",
    "config",
    "inputs",
    "payload",
    "producer",
    "schema",
    "session_id",
    "status",
    "subject",
}
_PAYLOAD_KEYS = {
    "as_of",
    "audit_summary",
    "arbiter",
    "canonical_route",
    "component_signals",
    "configured_mode",
    "consumption_policy",
    "effective_mode",
    "execution_controls",
    "generated_at",
    "localized_messages",
    "log_record",
    "mode",
    "notification",
    "plugin",
    "position_control",
    "profile",
    "schema_version",
    "strategy",
    "strategy_policy",
    "suggested_action",
    "target_type",
    "would_trade_if_enabled",
}


def _invalid() -> None:
    raise runner._RunnerError("T2B2_PRESENT_INVALID")


def _strict_json(raw: bytes) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=no_duplicates,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
    )


def _is_hash(value: Any) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


def _is_commit(value: Any) -> bool:
    return type(value) is str and _COMMIT_RE.fullmatch(value) is not None


def _is_size(value: Any) -> bool:
    return type(value) is int and value > 0


def _validate_config_keys(value: Any) -> None:
    if type(value) is dict:
        for key, child in value.items():
            lowered = key.lower()
            if lowered.startswith("ai_audit_") or any(part in lowered for part in _SENSITIVE_KEY_PARTS):
                _invalid()
            _validate_config_keys(child)
    elif type(value) is list:
        for child in value:
            _validate_config_keys(child)


def _verify_present_package(
    package_path: str | Path,
    *,
    benchmark_history_csv: str | Path,
    as_of: str,
    session_id: str,
    expected_digest: str,
    expected_qsp_commit: str,
) -> tuple[Mapping[str, Any], bytes]:
    """Fail closed unless canonical bytes and every bounded identity match."""
    if not _is_hash(expected_digest) or not _is_commit(expected_qsp_commit):
        _invalid()
    try:
        parsed_as_of = date.fromisoformat(as_of)
        if as_of != parsed_as_of.isoformat():
            _invalid()
        path = Path(package_path)
        file_stat = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(file_stat.st_mode):
            _invalid()
        raw = path.read_bytes()
        package = _strict_json(raw)
    except (OSError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        _invalid()
    try:
        canonical_package = runner._canonical_json(package)
    except (TypeError, ValueError):
        _invalid()
    if (
        type(package) is not dict
        or canonical_package != raw
        or runner._sha256(raw) != expected_digest
        or path.name != f"tqqq-market-regime-control-present-{as_of}-{expected_digest}.json"
        or set(package) != _PACKAGE_KEYS
        or package.get("schema") != PRESENT_SCHEMA
        or package.get("status") != "PRESENT"
        or package.get("as_of") != parsed_as_of.isoformat()
        or package.get("session_id") != session_id
        or session_id != f"XNAS:{as_of}"
    ):
        _invalid()

    subject = package.get("subject")
    producer = package.get("producer")
    config = package.get("config")
    inputs = package.get("inputs")
    payload = package.get("payload")
    if (
        type(subject) is not dict
        or subject != {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"}
        or type(producer) is not dict
        or producer != {"commit_sha": expected_qsp_commit, "entrypoint": QSP_ENTRYPOINT, "repository": QSP_REPOSITORY}
        or type(config) is not dict
        or set(config) != {"sha256", "value"}
        or not _is_hash(config.get("sha256"))
        or type(config.get("value")) is not dict
    ):
        _invalid()
    config_value = config["value"]
    _validate_config_keys(config_value)
    try:
        config_digest = runner._sha256(runner._canonical_json(config_value))
    except (TypeError, ValueError):
        _invalid()
    if (
        config["sha256"] != config_digest
        or config_value.get("as_of") != as_of
        or config_value.get("enabled") is not True
        or config_value.get("mode") != "shadow"
        or config_value.get("plugin") != "market_regime_control"
        or config_value.get("prices") != "@input:prices"
        or config_value.get("strategy") != "tqqq_growth_income"
    ):
        _invalid()

    if type(inputs) is not dict or set(inputs) != {"external_context", "prices"}:
        _invalid()
    prices = inputs["prices"]
    external_context = inputs["external_context"]
    if (
        type(prices) is not dict
        or set(prices) != {"format", "sha256", "size_bytes"}
        or prices.get("format") != "csv"
        or not _is_hash(prices.get("sha256"))
        or not _is_size(prices.get("size_bytes"))
        or type(external_context) is not dict
    ):
        _invalid()
    try:
        benchmark_bytes = Path(benchmark_history_csv).read_bytes()
    except (OSError, TypeError):
        _invalid()
    if prices["sha256"] != runner._sha256(benchmark_bytes) or prices["size_bytes"] != len(benchmark_bytes):
        _invalid()
    if external_context.get("status") == "ABSENT":
        if external_context != {"status": "ABSENT"}:
            _invalid()
    elif (
        set(external_context) != {"status", "format", "sha256", "size_bytes"}
        or external_context.get("status") != "PRESENT"
        or external_context.get("format") != "csv"
        or not _is_hash(external_context.get("sha256"))
        or not _is_size(external_context.get("size_bytes"))
    ):
        _invalid()

    if type(payload) is not dict or set(payload) != {"bytes_b64", "schema_version", "sha256", "size_bytes"}:
        _invalid()
    if (
        type(payload.get("bytes_b64")) is not str
        or payload.get("schema_version") != "market_regime_control.v1"
        or not _is_hash(payload.get("sha256"))
        or not _is_size(payload.get("size_bytes"))
    ):
        _invalid()
    try:
        payload_bytes = base64.b64decode(payload["bytes_b64"], validate=True)
        payload_value = _strict_json(payload_bytes)
    except (binascii.Error, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        _invalid()
    if (
        base64.b64encode(payload_bytes).decode("ascii") != payload["bytes_b64"]
        or
        payload["sha256"] != runner._sha256(payload_bytes)
        or payload["size_bytes"] != len(payload_bytes)
        or type(payload_value) is not dict
        or set(payload_value) != _PAYLOAD_KEYS
        or payload_value.get("schema_version") != "market_regime_control.v1"
        or payload_value.get("profile") != "market_regime_control"
        or payload_value.get("strategy") != "tqqq_growth_income"
        or payload_value.get("plugin") != "market_regime_control"
        or payload_value.get("target_type") != "strategy"
        or any(payload_value.get(key) != "shadow" for key in ("mode", "configured_mode", "effective_mode"))
        or payload_value.get("as_of") != as_of
    ):
        _invalid()
    policy = payload_value.get("consumption_policy")
    if (
        type(policy) is not dict
        or policy.get("plugin") != "market_regime_control"
        or policy.get("strategy") != "tqqq_growth_income"
        or policy.get("position_control_allowed") is not True
        or policy.get("evidence_status") != "automation_approved"
    ):
        _invalid()
    return package, benchmark_bytes


def run_tqqq_local_no_order_present(
    *,
    benchmark_history_csv: str | Path,
    as_of: str,
    session_id: str,
    output_parent: str | Path,
    plugin_control_package: str | Path,
    plugin_control_package_sha256: str,
    qsp_commit_sha: str,
) -> tuple[Any, Path]:
    """Verify PRESENT provenance before the shared no-order compute/publication core."""
    package, benchmark_bytes = _verify_present_package(
        plugin_control_package,
        benchmark_history_csv=benchmark_history_csv,
        as_of=as_of,
        session_id=session_id,
        expected_digest=plugin_control_package_sha256,
        expected_qsp_commit=qsp_commit_sha,
    )
    return runner._run_tqqq_local_no_order(
        benchmark_history_csv=benchmark_history_csv,
        as_of=as_of,
        session_id=session_id,
        output_parent=output_parent,
        plugin_control=package,
        market_csv_bytes=benchmark_bytes,
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    names = [
        "--benchmark-history-csv",
        "--as-of",
        "--session-id",
        "--output-parent",
        "--plugin-control-package",
        "--plugin-control-package-sha256",
        "--qsp-commit-sha",
    ]
    if len(args) != 14 or args[::2] != names:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec is None or main_spec.name != __spec__.name:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    try:
        _, destination = run_tqqq_local_no_order_present(
            benchmark_history_csv=args[1],
            as_of=args[3],
            session_id=args[5],
            output_parent=args[7],
            plugin_control_package=args[9],
            plugin_control_package_sha256=args[11],
            qsp_commit_sha=args[13],
        )
    except runner._RunnerError as exc:
        print(f"ERROR {exc.code}", file=sys.stderr)
        return {"T2B1_CODE_IDENTITY_INVALID": 2, "T2B1_INPUT_INVALID": 2, "T2B2_PRESENT_INVALID": 2, "T2B1_COMPUTE_FAILED": 3}.get(
            exc.code, 4
        )
    except Exception:
        print("ERROR T2B2_INTERNAL", file=sys.stderr)
        return 70
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
