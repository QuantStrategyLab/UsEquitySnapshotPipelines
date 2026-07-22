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
import math
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping

from . import tqqq_local_no_order_runner as runner


PRESENT_SCHEMA = "qsl.tqqq_market_regime_control_present.v1"
MIN_AS_OF = date(2026, 7, 21)
QSP_REPOSITORY = "QuantStrategyLab/QuantStrategyPlugins"
QSP_ENTRYPOINT = "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin"
QSP_BUNDLE_ENTRYPOINT = "quant_strategy_plugins.tqqq_research_input_bundle"
QSP_COMMIT = "c798397d9ca9230e404673d7774bac3d478217dc"
BUNDLE_SCHEMA = "qsl.t2b3.qqq_price_projection_bundle.v1"
RAW_FORMAT = "qsp.t2b3.long_price_csv.v1"
RAW_HEADER = b"symbol,as_of,open,high,low,close,volume\n"
REQUESTED_SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
_BUNDLE_MEMBERS = {"config.toml", "prices.csv", "manifest.json"}
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


def _is_date(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        return value == date.fromisoformat(value).isoformat()
    except ValueError:
        return False


def _read_member(path: Path) -> bytes:
    try:
        member_stat = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(member_stat.st_mode):
            _invalid()
        return path.read_bytes()
    except OSError:
        _invalid()


def _validate_qsp_number(token: str, *, positive: bool, optional: bool) -> None:
    if token == "" and optional:
        return
    try:
        numeric = float(token)
    except ValueError:
        _invalid()
    if (
        not token
        or any(character.isspace() for character in token)
        or not math.isfinite(numeric)
        or (numeric == 0 and math.copysign(1.0, numeric) < 0)
        or (positive and numeric <= 0)
        or (not positive and numeric < 0)
        or token != ("0" if numeric == 0 else format(numeric, ".17g"))
    ):
        _invalid()


def _project_qsp_benchmark(raw: bytes) -> tuple[bytes, int, str, str, int, str]:
    """Validate QSP canonical R and independently derive the QQQ-only B."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        _invalid()
    if not text.startswith(RAW_HEADER.decode()) or "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
        _invalid()
    qqq_rows: list[tuple[str, str]] = []
    dates: list[str] = []
    counts = {symbol: 0 for symbol in REQUESTED_SYMBOLS}
    previous: tuple[str, str] | None = None
    for line in text.splitlines()[1:]:
        fields = line.split(",")
        if len(fields) != 7 or any('"' in field for field in fields):
            _invalid()
        symbol, observed, open_, high, low, close, volume = fields
        current = (observed, symbol)
        if symbol not in counts or not _is_date(observed) or (previous is not None and current <= previous):
            _invalid()
        previous = current
        _validate_qsp_number(open_, positive=True, optional=True)
        _validate_qsp_number(high, positive=True, optional=True)
        _validate_qsp_number(low, positive=True, optional=True)
        _validate_qsp_number(close, positive=True, optional=False)
        _validate_qsp_number(volume, positive=False, optional=True)
        counts[symbol] += 1
        dates.append(observed)
        if symbol == "QQQ":
            qqq_rows.append((observed, close))
    if any(count < 252 for count in counts.values()) or len(qqq_rows) < 252:
        _invalid()
    first, last = qqq_rows[0][0], qqq_rows[-1][0]
    if last < MIN_AS_OF.isoformat() or any(observed > last for observed in dates):
        _invalid()
    benchmark = b"session_date,close\n" + b"".join(f"{observed},{close}\n".encode("ascii") for observed, close in qqq_rows)
    return benchmark, len(qqq_rows), first, last, sum(counts.values()), dates[0]


def _verify_qsp_bundle(
    input_bundle: str | Path, *, expected_manifest_digest: str, expected_qsp_commit: str
) -> tuple[Mapping[str, Any], bytes, bytes, str, str]:
    if not _is_hash(expected_manifest_digest) or expected_qsp_commit != QSP_COMMIT:
        _invalid()
    path = Path(input_bundle)
    try:
        if not path.is_absolute() or path.is_symlink() or not path.is_dir() or {entry.name for entry in path.iterdir()} != _BUNDLE_MEMBERS:
            _invalid()
    except OSError:
        _invalid()
    config_bytes = _read_member(path / "config.toml")
    raw = _read_member(path / "prices.csv")
    manifest_raw = _read_member(path / "manifest.json")
    manifest = _strict_json(manifest_raw)
    try:
        if type(manifest) is not dict or runner._canonical_json(manifest) != manifest_raw:
            _invalid()
    except (TypeError, ValueError):
        _invalid()
    benchmark, count, first, last, raw_count, raw_first = _project_qsp_benchmark(raw)
    expected_keys = {"config", "external_context", "prices", "producer", "projection", "provider", "schema", "session", "status"}
    if (
        runner._sha256(manifest_raw) != expected_manifest_digest
        or set(manifest) != expected_keys
        or manifest.get("schema") != BUNDLE_SCHEMA
        or manifest.get("status") != "READY"
        or manifest.get("external_context") != {"status": "ABSENT"}
    ):
        _invalid()
    config, prices, producer, projection, provider, session = (
        manifest[key] for key in ("config", "prices", "producer", "projection", "provider", "session")
    )
    if not all(type(value) is dict for value in (config, prices, producer, projection, provider, session)):
        _invalid()
    if (
        config != {"filename": "config.toml", "sha256": runner._sha256(config_bytes), "size_bytes": len(config_bytes)}
        or prices
        != {
            "filename": "prices.csv",
            "first_date": raw_first,
            "format": RAW_FORMAT,
            "last_date": last,
            "row_count": raw_count,
            "sha256": runner._sha256(raw),
            "size_bytes": len(raw),
            "symbols": sorted(REQUESTED_SYMBOLS),
        }
        or producer != {"commit_sha": QSP_COMMIT, "entrypoint": QSP_BUNDLE_ENTRYPOINT, "repository": QSP_REPOSITORY}
        or projection
        != {
            "benchmark_sha256": runner._sha256(benchmark),
            "benchmark_size_bytes": len(benchmark),
            "first_date": first,
            "last_date": last,
            "raw_sha256": runner._sha256(raw),
            "row_count": count,
            "symbol": "QQQ",
            "transform_id": "qsp.t2b3.qqq_session_date_close_csv",
            "transform_version": "1",
        }
        or provider
        != {
            "auto_adjust": True,
            "credentials": "ABSENT",
            "end_exclusive": provider.get("end_exclusive"),
            "path": "quant_strategy_plugins.yfinance_prices:download_price_history",
            "provider_id": "yahoo_yfinance_public",
            "requested_symbols": list(REQUESTED_SYMBOLS),
            "start": "2010-01-01",
        }
        or not _is_date(provider["end_exclusive"])
        or last >= provider["end_exclusive"]
        or session
        != {
            "as_of": last,
            "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF",
            "session_id": f"XNAS:{last}",
            "source": "LAST_COMPLETE_QQQ_ROW",
        }
    ):
        _invalid()
    return manifest, raw, benchmark, last, f"XNAS:{last}"


def _verify_present_package(
    package_path: str | Path,
    *,
    as_of: str,
    session_id: str,
    expected_digest: str,
    expected_qsp_commit: str,
    expected_prices: bytes,
) -> Mapping[str, Any]:
    """Fail closed unless canonical bytes and every bounded identity match."""
    if not _is_hash(expected_digest) or expected_qsp_commit != QSP_COMMIT:
        _invalid()
    try:
        parsed_as_of = date.fromisoformat(as_of)
        if as_of != parsed_as_of.isoformat() or parsed_as_of < MIN_AS_OF:
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

    if type(inputs) is not dict:
        _invalid()
    if set(inputs) != {"external_context", "prices"}:
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
    if prices["sha256"] != runner._sha256(expected_prices) or prices["size_bytes"] != len(expected_prices):
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
    return package


def _provider_observed_control(package: Mapping[str, Any], expected_digest: str) -> dict[str, Any]:
    """Wrap verified provider observations with forward-only deny markers."""
    manifest = package["inputs"]
    return {
        "calendar_authority": "provider_observed_unverified",
        "historical_backfill": False,
        "input_bundle": {
            "manifest": manifest,
            "manifest_sha256": runner._sha256(runner._canonical_json(manifest)),
        },
        "optimization_eligible": False,
        "package": {"sha256": expected_digest, "value": package},
        "status": "PRESENT",
    }


def run_tqqq_local_no_order_present(
    *,
    output_parent: str | Path,
    input_bundle: str | Path,
    input_bundle_manifest_sha256: str,
    plugin_control_package: str | Path,
    plugin_control_package_sha256: str,
    qsp_commit_sha: str,
) -> tuple[Any, Path]:
    """Verify QSP R, derive B, and publish only evidence outside no-order compute."""
    manifest, raw, benchmark_bytes, as_of, session_id = _verify_qsp_bundle(
        input_bundle,
        expected_manifest_digest=input_bundle_manifest_sha256,
        expected_qsp_commit=qsp_commit_sha,
    )
    package = _verify_present_package(
        plugin_control_package,
        as_of=as_of,
        session_id=session_id,
        expected_digest=plugin_control_package_sha256,
        expected_qsp_commit=qsp_commit_sha,
        expected_prices=raw,
    )
    plugin_control = _provider_observed_control(package, plugin_control_package_sha256)
    plugin_control["input_bundle"] = {
        "manifest": manifest,
        "manifest_sha256": input_bundle_manifest_sha256,
    }
    return runner._run_tqqq_local_no_order(
        benchmark_history_csv="<verified-qsp-projection>",
        as_of=as_of,
        session_id=session_id,
        output_parent=output_parent,
        plugin_control=plugin_control,
        market_csv_bytes=benchmark_bytes,
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    names = [
        "--output-parent",
        "--input-bundle",
        "--input-bundle-manifest-sha256",
        "--plugin-control-package",
        "--plugin-control-package-sha256",
        "--qsp-commit-sha",
    ]
    if len(args) != 12 or args[::2] != names:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec is None or main_spec.name != __spec__.name:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    try:
        _, destination = run_tqqq_local_no_order_present(
            output_parent=args[1],
            input_bundle=args[3],
            input_bundle_manifest_sha256=args[5],
            plugin_control_package=args[7],
            plugin_control_package_sha256=args[9],
            qsp_commit_sha=args[11],
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
