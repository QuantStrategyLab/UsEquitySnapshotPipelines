"""Consume a QSP T2B3 bundle without changing a TQQQ decision.

The consumer snapshots the three immutable bundle members once, independently
rederives the frozen QQQ ``session_date,close`` bytes, and passes only that
snapshot to the shared no-order core.  QSP is deliberately not imported.  The
PRESENT package and bundle manifest are evidence-envelope-only: they never
enter strategy, portfolio, state, runtime configuration, or broker paths.
Logical quarantine means reject/no output; it is not a physical subsystem.
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
CONTRACT_SHA256 = "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"
TRANSFORM_ID = "qsp.t2b3.qqq_session_date_close_csv"
TRANSFORM_VERSION = "1"
_RAW_FORMAT = "qsp.t2b3.long_price_csv.v1"
_REQUESTED_SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
_RAW_HEADER = b"symbol,as_of,open,high,low,close,volume\n"
_MEMBERS = {"config.toml", "prices.csv", "manifest.json"}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
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
taco_opportunity_size_scalar = 0.0
crisis_enabled = true
macro_enabled = true
taco_enabled = true
panic_reversal_enabled = false

[strategy_plugins.outputs]
output_dir = "data/output/tqqq_growth_income/plugins/market_regime_control"
'''


def _fail_bundle() -> None:
    raise runner._RunnerError("T2B3_BUNDLE_INVALID")


def _fail_present() -> None:
    raise runner._RunnerError("T2B2_PRESENT_INVALID")


def _strict_json(raw: bytes, *, bundle: bool) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        return json.loads(
            raw.decode("utf-8"), object_pairs_hook=no_duplicates, parse_constant=lambda _: (_ for _ in ()).throw(ValueError())
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        (_fail_bundle if bundle else _fail_present)()


def _is_hash(value: Any) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


def _is_commit(value: Any) -> bool:
    return type(value) is str and _COMMIT_RE.fullmatch(value) is not None


def _is_size(value: Any) -> bool:
    return type(value) is int and value > 0


def _canonical_date(value: object) -> str:
    if type(value) is not str:
        _fail_bundle()
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail_bundle()
    if value != parsed.isoformat():
        _fail_bundle()
    return value


def _number(token: str, *, positive: bool, optional: bool) -> None:
    if token == "" and optional:
        return
    if not token or any(character.isspace() for character in token):
        _fail_bundle()
    try:
        value = float(token)
    except ValueError:
        _fail_bundle()
    if not math.isfinite(value) or (value == 0 and math.copysign(1.0, value) < 0) or (positive and value <= 0) or (not positive and value < 0):
        _fail_bundle()
    if token != ("0" if value == 0 else format(value, ".17g")):
        _fail_bundle()


def _parse_raw(raw: bytes) -> tuple[list[dict[str, str]], str]:
    """Strictly rederive QSP raw-row lineage without importing QSP code."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        _fail_bundle()
    if not text.startswith(_RAW_HEADER.decode("ascii")) or "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
        _fail_bundle()
    lines = text.splitlines()
    if not lines or lines[0] != _RAW_HEADER.decode("ascii").rstrip("\n"):
        _fail_bundle()
    rows: list[dict[str, str]] = []
    previous: tuple[str, str] | None = None
    counts = {symbol: 0 for symbol in _REQUESTED_SYMBOLS}
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) != 7 or any('"' in field for field in fields):
            _fail_bundle()
        symbol, as_of, opening, high, low, close, volume = fields
        if symbol not in counts or _canonical_date(as_of) != as_of:
            _fail_bundle()
        current = (as_of, symbol)
        if previous is not None and current <= previous:
            _fail_bundle()
        previous = current
        _number(opening, positive=True, optional=True)
        _number(high, positive=True, optional=True)
        _number(low, positive=True, optional=True)
        _number(close, positive=True, optional=False)
        _number(volume, positive=False, optional=True)
        counts[symbol] += 1
        rows.append(dict(zip(("symbol", "as_of", "open", "high", "low", "close", "volume"), fields, strict=True)))
    if not rows or any(count < 252 for count in counts.values()):
        _fail_bundle()
    qqq = [row for row in rows if row["symbol"] == "QQQ"]
    as_of = qqq[-1]["as_of"]
    if as_of < "2026-07-21" or any(row["as_of"] > as_of for row in rows) or any(qqq[-1][key] == "" for key in ("open", "high", "low", "close", "volume")):
        _fail_bundle()
    return rows, as_of


def _project(raw: bytes) -> tuple[bytes, int, str, str, list[dict[str, str]]]:
    rows, as_of = _parse_raw(raw)
    qqq = [row for row in rows if row["symbol"] == "QQQ"]
    benchmark = b"session_date,close\n" + b"".join(f"{row['as_of']},{row['close']}\n".encode("ascii") for row in qqq)
    if len(qqq) < 252 or any(later["as_of"] <= earlier["as_of"] for earlier, later in zip(qqq, qqq[1:])):
        _fail_bundle()
    return benchmark, len(qqq), qqq[0]["as_of"], as_of, rows


def _manifest(raw: bytes, benchmark: bytes, count: int, first_date: str, as_of: str, rows: list[dict[str, str]], end: str) -> dict[str, Any]:
    return {
        "config": {"filename": "config.toml", "sha256": runner._sha256(_CONFIG_BYTES), "size_bytes": len(_CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {
            "filename": "prices.csv", "first_date": rows[0]["as_of"], "format": _RAW_FORMAT, "last_date": as_of,
            "row_count": len(rows), "sha256": runner._sha256(raw), "size_bytes": len(raw), "symbols": sorted(_REQUESTED_SYMBOLS),
        },
        "producer": {"commit_sha": QSP_COMMIT, "entrypoint": QSP_BUNDLE_ENTRYPOINT, "repository": QSP_REPOSITORY},
        "projection": {
            "benchmark_sha256": runner._sha256(benchmark), "benchmark_size_bytes": len(benchmark), "first_date": first_date,
            "last_date": as_of, "raw_sha256": runner._sha256(raw), "row_count": count, "symbol": "QQQ",
            "transform_id": TRANSFORM_ID, "transform_version": TRANSFORM_VERSION,
        },
        "provider": {
            "auto_adjust": True, "credentials": "ABSENT", "end_exclusive": end,
            "path": "quant_strategy_plugins.yfinance_prices:download_price_history", "provider_id": "yahoo_yfinance_public",
            "requested_symbols": list(_REQUESTED_SYMBOLS), "start": "2010-01-01",
        },
        "schema": BUNDLE_SCHEMA,
        "session": {"as_of": as_of, "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF", "session_id": f"XNAS:{as_of}", "source": "LAST_COMPLETE_QQQ_ROW"},
        "status": "READY",
    }


def _read_member(path: Path) -> bytes:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            _fail_bundle()
        return path.read_bytes()
    except OSError:
        _fail_bundle()


def _read_bundle(path: str | Path, expected_digest: str, expected_qsp_commit: str) -> tuple[Mapping[str, Any], bytes, bytes, str, str]:
    """Snapshot every member once and verify raw/config/manifest/projection lineage."""
    if not _is_hash(expected_digest) or expected_qsp_commit != QSP_COMMIT:
        _fail_bundle()
    bundle = Path(path)
    try:
        metadata = bundle.lstat()
        if not bundle.is_absolute() or bundle.is_symlink() or not stat.S_ISDIR(metadata.st_mode) or {item.name for item in bundle.iterdir()} != _MEMBERS:
            _fail_bundle()
    except OSError:
        _fail_bundle()
    config = _read_member(bundle / "config.toml")
    raw = _read_member(bundle / "prices.csv")
    manifest_bytes = _read_member(bundle / "manifest.json")
    if config != _CONFIG_BYTES or runner._sha256(manifest_bytes) != expected_digest:
        _fail_bundle()
    manifest = _strict_json(manifest_bytes, bundle=True)
    if type(manifest) is not dict or runner._canonical_json(manifest) != manifest_bytes:
        _fail_bundle()
    benchmark, count, first_date, as_of, rows = _project(raw)
    provider = manifest.get("provider")
    if type(provider) is not dict:
        _fail_bundle()
    end = provider.get("end_exclusive")
    if _canonical_date(end) != end or as_of >= end:
        _fail_bundle()
    expected = _manifest(raw, benchmark, count, first_date, as_of, rows, end)
    if manifest != expected:
        _fail_bundle()
    if bundle.name != f"qsp-t2b3-qqq-input-v1-{as_of}-{expected_digest}":
        _fail_bundle()
    return manifest, raw, benchmark, as_of, f"XNAS:{as_of}"


def _package_config(as_of: str) -> dict[str, Any]:
    return {
        "as_of": as_of, "attack_symbol": "TQQQ", "benchmark_symbol": "QQQ", "credit_pairs": ["HYG:IEF", "LQD:IEF"],
        "crisis_enabled": True, "delever_risk_asset_scalar": 0.0, "enabled": True, "event_set": "geopolitical-deescalation",
        "external_stress_actionable": False, "financial_symbols": ["XLF", "KRE"], "macro_enabled": True, "mode": "shadow",
        "panic_reversal_enabled": False, "plugin": "market_regime_control", "prices": "@input:prices", "rate_symbols": ["IEF", "TLT"],
        "realized_vol_requires_confirmation": True, "realized_vol_threshold": 0.30, "strategy": "tqqq_growth_income",
        "strategy_policy": "levered_growth_income_v1", "taco_enabled": True, "taco_opportunity_size_scalar": 0.0,
        "vix3m_symbols": ["VIX3M", "^VIX3M", "VXV", "^VXV"], "vix_symbols": ["VIX", "^VIX", "VIXCLS"],
    }


def _verify_present_package(package_path: str | Path, *, as_of: str, session_id: str, expected_digest: str, raw: bytes) -> Mapping[str, Any]:
    """Verify independently supplied package trust values and bind it to raw ``R``."""
    if not _is_hash(expected_digest):
        _fail_present()
    try:
        path = Path(package_path)
        metadata = path.lstat()
        if not path.is_absolute() or path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            _fail_present()
        package_bytes = path.read_bytes()
    except OSError:
        _fail_present()
    package = _strict_json(package_bytes, bundle=False)
    if type(package) is not dict or runner._canonical_json(package) != package_bytes or runner._sha256(package_bytes) != expected_digest:
        _fail_present()
    if path.name != f"tqqq-market-regime-control-present-{as_of}-{expected_digest}.json":
        _fail_present()
    required = {"as_of", "config", "inputs", "payload", "producer", "schema", "session_id", "status", "subject"}
    if set(package) != required or package.get("schema") != PRESENT_SCHEMA or package.get("status") != "PRESENT" or package.get("as_of") != as_of or package.get("session_id") != session_id:
        _fail_present()
    if package.get("subject") != {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"}:
        _fail_present()
    if package.get("producer") != {"commit_sha": QSP_COMMIT, "entrypoint": QSP_ENTRYPOINT, "repository": QSP_REPOSITORY}:
        _fail_present()
    config = package.get("config")
    expected_config = _package_config(as_of)
    if type(config) is not dict or set(config) != {"sha256", "value"} or config.get("value") != expected_config or config.get("sha256") != runner._sha256(runner._canonical_json(expected_config)):
        _fail_present()
    inputs = package.get("inputs")
    expected_prices = {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)}
    if type(inputs) is not dict or set(inputs) != {"external_context", "prices"} or inputs.get("prices") != expected_prices or inputs.get("external_context") != {"status": "ABSENT"}:
        _fail_present()
    payload = package.get("payload")
    if type(payload) is not dict or set(payload) != {"bytes_b64", "schema_version", "sha256", "size_bytes"} or payload.get("schema_version") != "market_regime_control.v1" or not _is_hash(payload.get("sha256")) or not _is_size(payload.get("size_bytes")) or type(payload.get("bytes_b64")) is not str:
        _fail_present()
    try:
        payload_bytes = base64.b64decode(payload["bytes_b64"], validate=True)
        payload_value = _strict_json(payload_bytes, bundle=False)
    except (binascii.Error, ValueError):
        _fail_present()
    if base64.b64encode(payload_bytes).decode("ascii") != payload["bytes_b64"] or payload["sha256"] != runner._sha256(payload_bytes) or payload["size_bytes"] != len(payload_bytes) or type(payload_value) is not dict or set(payload_value) != _PAYLOAD_KEYS:
        _fail_present()
    if any(payload_value.get(key) != value for key, value in {"as_of": as_of, "profile": "market_regime_control", "strategy": "tqqq_growth_income", "plugin": "market_regime_control", "target_type": "strategy", "schema_version": "market_regime_control.v1", "mode": "shadow", "configured_mode": "shadow", "effective_mode": "shadow"}.items()):
        _fail_present()
    policy = payload_value.get("consumption_policy")
    if type(policy) is not dict or policy.get("plugin") != "market_regime_control" or policy.get("strategy") != "tqqq_growth_income" or policy.get("position_control_allowed") is not True or policy.get("evidence_status") != "automation_approved":
        _fail_present()
    return package


def run_tqqq_local_no_order_present(
    *, input_bundle: str | Path, input_bundle_manifest_sha256: str, plugin_control_package: str | Path,
    plugin_control_package_sha256: str, qsp_commit_sha: str, output_parent: str | Path,
) -> tuple[Any, Path]:
    """Accept only trusted QSP lineage and publish PRESENT control as envelope evidence.

    ``DJ``, ``DP``, and ``Cqsp`` are independent caller inputs rather than values
    learned from presented bytes/names.  The shared core receives no bundle path
    and no raw bytes: it receives the one immutable independently-derived ``B``.
    """
    manifest, raw, benchmark, as_of, session_id = _read_bundle(
        input_bundle, input_bundle_manifest_sha256, qsp_commit_sha
    )
    package = _verify_present_package(
        plugin_control_package, as_of=as_of, session_id=session_id, expected_digest=plugin_control_package_sha256,
        raw=raw,
    )
    plugin_control = {
        "input_bundle": {"manifest": manifest, "manifest_sha256": input_bundle_manifest_sha256},
        "package": {"sha256": plugin_control_package_sha256, "value": package},
        "status": "PRESENT",
    }
    return runner._run_tqqq_local_no_order(
        benchmark_history_csv="<verified-qsp-projection>", as_of=as_of, session_id=session_id, output_parent=output_parent,
        plugin_control=plugin_control, market_csv_bytes=benchmark,
    )


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
        _, destination = run_tqqq_local_no_order_present(
            input_bundle=args[1], input_bundle_manifest_sha256=args[3], plugin_control_package=args[5],
            plugin_control_package_sha256=args[7], qsp_commit_sha=args[9], output_parent=args[11],
        )
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
