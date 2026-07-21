"""Consume a frozen QSP bundle as opaque, no-order PRESENT evidence.

This research-only interface intentionally replaces the former caller-supplied
benchmark/as-of/session CLI.  Untrusted bundle and package paths/bytes are
validated against independently dispatched ``DJ``, ``DP`` and ``Cqsp`` pins;
only the locally rederived QQQ benchmark reaches the shared no-order core.
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
BUNDLE_SCHEMA = "qsl.t2b3.qqq_price_projection_bundle.v1"
QSP_REPOSITORY = "QuantStrategyLab/QuantStrategyPlugins"
QSP_ENTRYPOINT = "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin"
QSP_BUNDLE_ENTRYPOINT = "quant_strategy_plugins.tqqq_research_input_bundle"
QSP_COMMIT = "c798397d9ca9230e404673d7774bac3d478217dc"
PROJECTION_CONTRACT_SHA256 = "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"
QSP_RECOVERY_CONTRACT_SHA256 = "dfeffa2ab9d6d4fa25f8b5ac5525912174910f85bd9ee61caf62b7a87b9172ce"
STAGE2_RECOVERY_CONTRACT_SHA256 = "2b836bb1da2d2762e6851dc3097654998068806d9ff70e7e4924b5fdfbe13933"
TRANSFORM_ID = "qsp.t2b3.qqq_session_date_close_csv"
TRANSFORM_VERSION = "1"
MIN_AS_OF = "2026-07-21"
_RAW_FORMAT = "qsp.t2b3.long_price_csv.v1"
_SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
_RAW_HEADER = b"symbol,as_of,open,high,low,close,volume\n"
_MEMBERS = {"config.toml", "prices.csv", "manifest.json"}
_HASH = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_PAYLOAD_KEYS = {
    "as_of", "audit_summary", "arbiter", "canonical_route", "component_signals", "configured_mode", "consumption_policy",
    "effective_mode", "execution_controls", "generated_at", "localized_messages", "log_record", "mode", "notification",
    "plugin", "position_control", "profile", "schema_version", "strategy", "strategy_policy", "suggested_action",
    "target_type", "would_trade_if_enabled",
}
_CONFIG_BYTES = b'''default_mode = "shadow"

[[strategy_plugins]]
strategy = "tqqq_growth_income"
plugin = "market_regime_control"
enabled = true

[strategy_plugins.inputs]
prices = "prices.csv"
event_set = "geopolitical-deescalation"
benchmark_symbol = "QQQ"
attack_symbol = "TQQQ"
vix_symbols = ["VIX", "^VIX", "VIXCLS"]
vix3m_symbols = ["VIX3M", "^VIX3M", "VXV", "^VXV"]
credit_pairs = ["HYG:IEF", "LQD:IEF"]
financial_symbols = ["XLF", "KRE"]
rate_symbols = ["IEF", "TLT"]
strategy_policy = "levered_growth_income_v1"
realized_vol_threshold = 0.30
realized_vol_requires_confirmation = true
external_stress_actionable = false
delever_risk_asset_scalar = 0.0
crisis_enabled = true
macro_enabled = true
taco_enabled = true
panic_reversal_enabled = false

[strategy_plugins.outputs]
output_dir = "data/output/tqqq_growth_income/plugins/market_regime_control"
'''


def _bundle_invalid() -> None:
    raise runner._RunnerError("T2B3_BUNDLE_INVALID")


def _present_invalid() -> None:
    raise runner._RunnerError("T2B2_PRESENT_INVALID")


def _strict_json(raw: bytes, error) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=no_duplicates, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        error()


def _canonical_date(value: object) -> str:
    if type(value) is not str:
        _bundle_invalid()
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _bundle_invalid()
    if value != parsed.isoformat():
        _bundle_invalid()
    return value


def _wire_number(token: str, *, positive: bool, optional: bool) -> None:
    """Accept the QSP-only ``0``/binary64 ``.17g`` wire, never short decimals."""
    if token == "" and optional:
        return
    if not token or any(character.isspace() for character in token):
        _bundle_invalid()
    try:
        value = float(token)
    except ValueError:
        _bundle_invalid()
    if not math.isfinite(value) or (value == 0 and math.copysign(1.0, value) < 0) or (positive and value <= 0) or (not positive and value < 0):
        _bundle_invalid()
    if token != ("0" if value == 0 else format(value, ".17g")):
        _bundle_invalid()


def _parse_raw(raw: bytes) -> tuple[list[dict[str, str]], str]:
    """Independently parse canonical QSP raw R; SPY is required in the fixed universe."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        _bundle_invalid()
    if not text.startswith(_RAW_HEADER.decode("ascii")) or "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
        _bundle_invalid()
    lines = text.splitlines()
    if not lines or lines[0] != _RAW_HEADER.decode("ascii").rstrip("\n"):
        _bundle_invalid()
    rows: list[dict[str, str]] = []
    counts = {symbol: 0 for symbol in _SYMBOLS}
    previous: tuple[str, str] | None = None
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) != 7 or any('"' in field for field in fields):
            _bundle_invalid()
        symbol, as_of, opening, high, low, close, volume = fields
        if symbol not in counts or _canonical_date(as_of) != as_of:
            _bundle_invalid()
        current = (as_of, symbol)
        if previous is not None and current <= previous:
            _bundle_invalid()
        previous = current
        _wire_number(opening, positive=True, optional=True)
        _wire_number(high, positive=True, optional=True)
        _wire_number(low, positive=True, optional=True)
        _wire_number(close, positive=True, optional=False)
        _wire_number(volume, positive=False, optional=True)
        counts[symbol] += 1
        rows.append(dict(zip(("symbol", "as_of", "open", "high", "low", "close", "volume"), fields, strict=True)))
    if not rows or any(count < 252 for count in counts.values()):
        _bundle_invalid()
    qqq = [row for row in rows if row["symbol"] == "QQQ"]
    as_of = qqq[-1]["as_of"]
    if as_of < MIN_AS_OF or any(row["as_of"] > as_of for row in rows) or any(qqq[-1][key] == "" for key in ("open", "high", "low", "close", "volume")):
        _bundle_invalid()
    return rows, as_of


def _project(raw: bytes) -> tuple[bytes, int, str, str, list[dict[str, str]]]:
    rows, as_of = _parse_raw(raw)
    qqq = [row for row in rows if row["symbol"] == "QQQ"]
    benchmark = b"session_date,close\n" + b"".join(f"{row['as_of']},{row['close']}\n".encode("ascii") for row in qqq)
    if any(later["as_of"] <= earlier["as_of"] for earlier, later in zip(qqq, qqq[1:])):
        _bundle_invalid()
    return benchmark, len(qqq), qqq[0]["as_of"], as_of, rows


def _expected_manifest(raw: bytes, benchmark: bytes, count: int, first_date: str, as_of: str, rows: list[dict[str, str]], end: str) -> dict[str, Any]:
    return {
        "config": {"filename": "config.toml", "sha256": runner._sha256(_CONFIG_BYTES), "size_bytes": len(_CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {"filename": "prices.csv", "first_date": rows[0]["as_of"], "format": _RAW_FORMAT, "last_date": as_of, "row_count": len(rows), "sha256": runner._sha256(raw), "size_bytes": len(raw), "symbols": sorted(_SYMBOLS)},
        "producer": {"commit_sha": QSP_COMMIT, "entrypoint": QSP_BUNDLE_ENTRYPOINT, "repository": QSP_REPOSITORY},
        "projection": {"benchmark_sha256": runner._sha256(benchmark), "benchmark_size_bytes": len(benchmark), "first_date": first_date, "last_date": as_of, "raw_sha256": runner._sha256(raw), "row_count": count, "symbol": "QQQ", "transform_id": TRANSFORM_ID, "transform_version": TRANSFORM_VERSION},
        "provider": {"auto_adjust": True, "credentials": "ABSENT", "end_exclusive": end, "path": "quant_strategy_plugins.yfinance_prices:download_price_history", "provider_id": "yahoo_yfinance_public", "requested_symbols": list(_SYMBOLS), "start": "2010-01-01"},
        "schema": BUNDLE_SCHEMA,
        "session": {"as_of": as_of, "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF", "session_id": f"XNAS:{as_of}", "source": "LAST_COMPLETE_QQQ_ROW"},
        "status": "READY",
    }


def _read_regular(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            _bundle_invalid()
        return path.read_bytes()
    except OSError:
        _bundle_invalid()


def _read_bundle(path: str | Path, expected_digest: str, expected_commit: str) -> tuple[Mapping[str, Any], bytes, bytes, str, str]:
    """Snapshot config/R/manifest once; DJ and Cqsp authenticate untrusted member bytes.

    ``end_exclusive`` is authenticated canonical manifest content, not a CLI trust
    argument. Relative paths and symlinks are deliberately rejected at this boundary.
    """
    if type(expected_digest) is not str or _HASH.fullmatch(expected_digest) is None or expected_commit != QSP_COMMIT:
        _bundle_invalid()
    bundle = Path(path)
    try:
        metadata = bundle.lstat()
        if not bundle.is_absolute() or bundle.is_symlink() or not stat.S_ISDIR(metadata.st_mode) or {item.name for item in bundle.iterdir()} != _MEMBERS:
            _bundle_invalid()
    except OSError:
        _bundle_invalid()
    config, raw, manifest_bytes = (_read_regular(bundle / name) for name in ("config.toml", "prices.csv", "manifest.json"))
    if config != _CONFIG_BYTES or runner._sha256(manifest_bytes) != expected_digest:
        _bundle_invalid()
    manifest = _strict_json(manifest_bytes, _bundle_invalid)
    if type(manifest) is not dict or runner._canonical_json(manifest) != manifest_bytes:
        _bundle_invalid()
    benchmark, count, first_date, as_of, rows = _project(raw)
    provider = manifest.get("provider")
    if type(provider) is not dict:
        _bundle_invalid()
    end = provider.get("end_exclusive")
    if _canonical_date(end) != end or as_of >= end or manifest != _expected_manifest(raw, benchmark, count, first_date, as_of, rows, end):
        _bundle_invalid()
    if bundle.name != f"qsp-t2b3-qqq-input-v1-{as_of}-{expected_digest}":
        _bundle_invalid()
    return manifest, raw, benchmark, as_of, f"XNAS:{as_of}"


def _expected_package_config(as_of: str) -> dict[str, Any]:
    return {"as_of": as_of, "attack_symbol": "TQQQ", "benchmark_symbol": "QQQ", "credit_pairs": ["HYG:IEF", "LQD:IEF"], "crisis_enabled": True, "delever_risk_asset_scalar": 0.0, "enabled": True, "event_set": "geopolitical-deescalation", "external_stress_actionable": False, "financial_symbols": ["XLF", "KRE"], "macro_enabled": True, "mode": "shadow", "panic_reversal_enabled": False, "plugin": "market_regime_control", "prices": "@input:prices", "rate_symbols": ["IEF", "TLT"], "realized_vol_requires_confirmation": True, "realized_vol_threshold": 0.30, "strategy": "tqqq_growth_income", "strategy_policy": "levered_growth_income_v1", "taco_enabled": True, "taco_opportunity_size_scalar": 0.0, "vix3m_symbols": ["VIX3M", "^VIX3M", "VXV", "^VXV"], "vix_symbols": ["VIX", "^VIX", "VIXCLS"]}


def _verify_package(path_value: str | Path, *, as_of: str, session_id: str, digest: str, raw: bytes) -> Mapping[str, Any]:
    """Validate a PRESENT package bound to raw R, not projected B or self-attestation."""
    if type(digest) is not str or _HASH.fullmatch(digest) is None:
        _present_invalid()
    path = Path(path_value)
    try:
        metadata = path.lstat()
        if not path.is_absolute() or path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            _present_invalid()
        package_bytes = path.read_bytes()
    except OSError:
        _present_invalid()
    package = _strict_json(package_bytes, _present_invalid)
    if type(package) is not dict or runner._canonical_json(package) != package_bytes or runner._sha256(package_bytes) != digest or path.name != f"tqqq-market-regime-control-present-{as_of}-{digest}.json":
        _present_invalid()
    required = {"as_of", "config", "inputs", "payload", "producer", "schema", "session_id", "status", "subject"}
    if set(package) != required or package.get("schema") != PRESENT_SCHEMA or package.get("status") != "PRESENT" or package.get("as_of") != as_of or package.get("session_id") != session_id or package.get("subject") != {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"} or package.get("producer") != {"commit_sha": QSP_COMMIT, "entrypoint": QSP_ENTRYPOINT, "repository": QSP_REPOSITORY}:
        _present_invalid()
    config = package.get("config")
    expected_config = _expected_package_config(as_of)
    if type(config) is not dict or config != {"sha256": runner._sha256(runner._canonical_json(expected_config)), "value": expected_config}:
        _present_invalid()
    inputs = package.get("inputs")
    expected_prices = {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)}
    if type(inputs) is not dict or inputs != {"external_context": {"status": "ABSENT"}, "prices": expected_prices}:
        _present_invalid()
    payload = package.get("payload")
    if type(payload) is not dict or set(payload) != {"bytes_b64", "schema_version", "sha256", "size_bytes"} or payload.get("schema_version") != "market_regime_control.v1" or type(payload.get("bytes_b64")) is not str or type(payload.get("size_bytes")) is not int or payload["size_bytes"] <= 0 or type(payload.get("sha256")) is not str or _HASH.fullmatch(payload["sha256"]) is None:
        _present_invalid()
    try:
        payload_bytes = base64.b64decode(payload["bytes_b64"], validate=True)
        payload_value = _strict_json(payload_bytes, _present_invalid)
    except (binascii.Error, ValueError):
        _present_invalid()
    if base64.b64encode(payload_bytes).decode("ascii") != payload["bytes_b64"] or payload["sha256"] != runner._sha256(payload_bytes) or payload["size_bytes"] != len(payload_bytes) or type(payload_value) is not dict or set(payload_value) != _PAYLOAD_KEYS:
        _present_invalid()
    expected_payload = {"as_of": as_of, "profile": "market_regime_control", "strategy": "tqqq_growth_income", "plugin": "market_regime_control", "target_type": "strategy", "schema_version": "market_regime_control.v1", "mode": "shadow", "configured_mode": "shadow", "effective_mode": "shadow"}
    if any(payload_value.get(key) != value for key, value in expected_payload.items()):
        _present_invalid()
    policy = payload_value.get("consumption_policy")
    if type(policy) is not dict or policy.get("plugin") != "market_regime_control" or policy.get("strategy") != "tqqq_growth_income" or policy.get("position_control_allowed") is not True or policy.get("evidence_status") != "automation_approved":
        _present_invalid()
    return package


def run_tqqq_local_no_order_present(*, input_bundle: str | Path, input_bundle_manifest_sha256: str, plugin_control_package: str | Path, plugin_control_package_sha256: str, qsp_commit_sha: str, output_parent: str | Path) -> tuple[Any, Path]:
    """Publish the frozen opaque wrapper; no downstream decision consumer currently exists.

    The logical quarantine boundary rejects invalid evidence before compute. The runner
    receives only B and serializes this wrapper as opaque envelope evidence.
    """
    manifest, raw, benchmark, as_of, session_id = _read_bundle(input_bundle, input_bundle_manifest_sha256, qsp_commit_sha)
    package = _verify_package(plugin_control_package, as_of=as_of, session_id=session_id, digest=plugin_control_package_sha256, raw=raw)
    plugin_control = {"input_bundle": {"manifest": manifest, "manifest_sha256": input_bundle_manifest_sha256}, "package": {"sha256": plugin_control_package_sha256, "value": package}, "status": "PRESENT"}
    return runner._run_tqqq_local_no_order(benchmark_history_csv="<verified-qsp-projection>", as_of=as_of, session_id=session_id, output_parent=output_parent, plugin_control=plugin_control, market_csv_bytes=benchmark)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    names = ["--input-bundle", "--input-bundle-manifest-sha256", "--plugin-control-package", "--plugin-control-package-sha256", "--qsp-commit-sha", "--output-parent"]
    if len(args) != 12 or args[::2] != names:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec is None or main_spec.name != __spec__.name:
        print("ERROR T2B2_PRESENT_INVALID", file=sys.stderr)
        return 2
    try:
        _, destination = run_tqqq_local_no_order_present(input_bundle=args[1], input_bundle_manifest_sha256=args[3], plugin_control_package=args[5], plugin_control_package_sha256=args[7], qsp_commit_sha=args[9], output_parent=args[11])
    except runner._RunnerError as exc:
        print(f"ERROR {exc.code}", file=sys.stderr)
        return {"T2B3_BUNDLE_INVALID": 2, "T2B2_PRESENT_INVALID": 2, "T2B1_CODE_IDENTITY_INVALID": 2, "T2B1_INPUT_INVALID": 2, "T2B1_COMPUTE_FAILED": 3}.get(exc.code, 4)
    except Exception:
        print("ERROR T2B3_INTERNAL", file=sys.stderr)
        return 70
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
