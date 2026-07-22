"""Consume a verified QSP bundle as opaque, no-order PRESENT evidence.

This research-only interface intentionally replaces the legacy same-byte CLI.
Bundle/package paths and bytes are untrusted; independently dispatched DJ, DP and
Cqsp pins authenticate them.  UESP rederives B from R and sends only B to the
shared runner.  The wrapper is opaque envelope evidence and invalid evidence is
logically quarantined before compute; no decision, state, portfolio, or order
path reads it.
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
from typing import Any, Callable, Mapping

from . import tqqq_local_no_order_runner as runner

PRESENT_SCHEMA = "qsl.tqqq_market_regime_control_present.v1"
BUNDLE_SCHEMA = "qsl.t2b3.qqq_price_projection_bundle.v1"
QSP_REPOSITORY = "QuantStrategyLab/QuantStrategyPlugins"
QSP_ENTRYPOINT = "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin"
QSP_BUNDLE_ENTRYPOINT = "quant_strategy_plugins.tqqq_research_input_bundle"
QSP_COMMIT = "c798397d9ca9230e404673d7774bac3d478217dc"
PROJECTION_CONTRACT_SHA256 = "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"
QSP_RECOVERY_CONTRACT_SHA256 = "dfeffa2ab9d6d4fa25f8b5ac5525912174910f85bd9ee61caf62b7a87b9172ce"
TRANSFORM_ID = "qsp.t2b3.qqq_session_date_close_csv"
TRANSFORM_VERSION = "1"
MIN_AS_OF = "2026-07-21"
REQUESTED_SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
RAW_HEADER = b"symbol,as_of,open,high,low,close,volume\n"
MEMBERS = {"config.toml", "prices.csv", "manifest.json"}
PAYLOAD_KEYS = {
    "as_of", "audit_summary", "arbiter", "canonical_route", "component_signals", "configured_mode",
    "consumption_policy", "effective_mode", "execution_controls", "generated_at", "localized_messages",
    "log_record", "mode", "notification", "plugin", "position_control", "profile", "schema_version",
    "strategy", "strategy_policy", "suggested_action", "target_type", "would_trade_if_enabled",
}
_HASH = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
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


def _bundle_invalid() -> None:
    raise runner._RunnerError("T2B3_BUNDLE_INVALID")


def _present_invalid() -> None:
    raise runner._RunnerError("T2B2_PRESENT_INVALID")


def _strict_json(raw: bytes, fail: Callable[[], None]) -> Any:
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
        fail()


def _is_date(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        return value == date.fromisoformat(value).isoformat()
    except ValueError:
        return False


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _member(path: Path, fail: Callable[[], None]) -> bytes:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            fail()
        return path.read_bytes()
    except OSError:
        fail()


def _canonical_number(token: str, *, positive: bool, optional: bool) -> None:
    if token == "" and optional:
        return
    try:
        numeric = float(token)
    except ValueError:
        _bundle_invalid()
    if not token or any(char.isspace() for char in token) or not math.isfinite(numeric) or (numeric == 0 and math.copysign(1, numeric) < 0):
        _bundle_invalid()
    if (positive and numeric <= 0) or (not positive and numeric < 0) or token != ("0" if numeric == 0 else format(numeric, ".17g")):
        _bundle_invalid()


def _project(raw: bytes) -> tuple[bytes, int, str, str, int, str]:
    """Independently rederive B from canonical QSP R using only provider-observed rows."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        _bundle_invalid()
    if not text.startswith(RAW_HEADER.decode()) or "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
        _bundle_invalid()
    lines = text.splitlines()
    rows: list[tuple[str, str, str]] = []
    observed_dates: list[str] = []
    counts = {symbol: 0 for symbol in REQUESTED_SYMBOLS}
    prior: tuple[str, str] | None = None
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != 7 or any('"' in part for part in parts):
            _bundle_invalid()
        symbol, observed, open_, high, low, close, volume = parts
        if symbol not in counts or not _is_date(observed):
            _bundle_invalid()
        current = (observed, symbol)
        if prior is not None and current <= prior:
            _bundle_invalid()
        prior = current
        _canonical_number(open_, positive=True, optional=True)
        _canonical_number(high, positive=True, optional=True)
        _canonical_number(low, positive=True, optional=True)
        _canonical_number(close, positive=True, optional=False)
        _canonical_number(volume, positive=False, optional=True)
        counts[symbol] += 1
        observed_dates.append(observed)
        if symbol == "QQQ":
            rows.append((observed, close, ",".join(parts)))
    if any(count < 252 for count in counts.values()) or len(rows) < 252:
        _bundle_invalid()
    first, last = rows[0][0], rows[-1][0]
    if last < MIN_AS_OF or any(observed > last for observed in observed_dates):
        _bundle_invalid()
    projected = b"session_date,close\n" + b"".join(f"{observed},{close}\n".encode("ascii") for observed, close, _ in rows)
    return projected, len(rows), first, last, sum(counts.values()), observed_dates[0]


def _read_bundle(value: str | Path, digest: str, commit: str) -> tuple[Mapping[str, Any], bytes, bytes, str, str]:
    """Snapshot the exact three bundle members once; DJ/Cqsp are independent trust anchors."""
    if not _HASH.fullmatch(digest) or commit != QSP_COMMIT or not _COMMIT.fullmatch(commit):
        _bundle_invalid()
    path = Path(value)
    try:
        if not path.is_absolute() or path.is_symlink() or not path.is_dir() or {entry.name for entry in path.iterdir()} != MEMBERS:
            _bundle_invalid()
    except OSError:
        _bundle_invalid()
    config, raw, manifest_bytes = (_member(path / name, _bundle_invalid) for name in ("config.toml", "prices.csv", "manifest.json"))
    if config != _CONFIG_BYTES or runner._sha256(manifest_bytes) != digest:
        _bundle_invalid()
    manifest = _strict_json(manifest_bytes, _bundle_invalid)
    if type(manifest) is not dict or runner._canonical_json(manifest) != manifest_bytes:
        _bundle_invalid()
    expected_keys = {"config", "external_context", "prices", "producer", "projection", "provider", "schema", "session", "status"}
    if set(manifest) != expected_keys or manifest.get("schema") != BUNDLE_SCHEMA or manifest.get("status") != "READY" or manifest.get("external_context") != {"status": "ABSENT"}:
        _bundle_invalid()
    benchmark, count, first, last, raw_count, raw_first = _project(raw)
    config_identity, prices, producer, projection, provider, session = (manifest[key] for key in ("config", "prices", "producer", "projection", "provider", "session"))
    if not all(type(item) is dict for item in (config_identity, prices, producer, projection, provider, session)):
        _bundle_invalid()
    if config_identity != {"filename": "config.toml", "sha256": runner._sha256(config), "size_bytes": len(config)}:
        _bundle_invalid()
    if prices != {"filename": "prices.csv", "first_date": raw_first, "format": "qsp.t2b3.long_price_csv.v1", "last_date": last, "row_count": raw_count, "sha256": runner._sha256(raw), "size_bytes": len(raw), "symbols": sorted(REQUESTED_SYMBOLS)}:
        _bundle_invalid()
    if producer != {"commit_sha": commit, "entrypoint": QSP_BUNDLE_ENTRYPOINT, "repository": QSP_REPOSITORY}:
        _bundle_invalid()
    if projection != {"benchmark_sha256": runner._sha256(benchmark), "benchmark_size_bytes": len(benchmark), "first_date": first, "last_date": last, "raw_sha256": runner._sha256(raw), "row_count": count, "symbol": "QQQ", "transform_id": TRANSFORM_ID, "transform_version": TRANSFORM_VERSION}:
        _bundle_invalid()
    expected_provider = {"auto_adjust": True, "credentials": "ABSENT", "end_exclusive": provider.get("end_exclusive"), "path": "quant_strategy_plugins.yfinance_prices:download_price_history", "provider_id": "yahoo_yfinance_public", "requested_symbols": list(REQUESTED_SYMBOLS), "start": "2010-01-01"}
    if provider != expected_provider or not _is_date(provider["end_exclusive"]) or last >= provider["end_exclusive"]:
        _bundle_invalid()
    if session != {"as_of": last, "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF", "session_id": f"XNAS:{last}", "source": "LAST_COMPLETE_QQQ_ROW"}:
        _bundle_invalid()
    return manifest, raw, benchmark, last, f"XNAS:{last}"


def _verify_package(path_value: str | Path, *, digest: str, commit: str, raw: bytes, as_of: str, session_id: str) -> Mapping[str, Any]:
    """Verify DP/Cqsp and require the PRESENT package to bind R, never B."""
    if not _HASH.fullmatch(digest) or commit != QSP_COMMIT:
        _present_invalid()
    path = Path(path_value)
    if not path.is_absolute():
        _present_invalid()
    raw_package = _member(path, _present_invalid)
    package = _strict_json(raw_package, _present_invalid)
    if type(package) is not dict or runner._canonical_json(package) != raw_package or runner._sha256(raw_package) != digest:
        _present_invalid()
    if path.name != f"tqqq-market-regime-control-present-{as_of}-{digest}.json" or set(package) != {"as_of", "config", "inputs", "payload", "producer", "schema", "session_id", "status", "subject"}:
        _present_invalid()
    if package.get("schema") != PRESENT_SCHEMA or package.get("status") != "PRESENT" or package.get("as_of") != as_of or package.get("session_id") != session_id:
        _present_invalid()
    if package.get("subject") != {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"} or package.get("producer") != {"commit_sha": commit, "entrypoint": QSP_ENTRYPOINT, "repository": QSP_REPOSITORY}:
        _present_invalid()
    inputs = package.get("inputs")
    if set(inputs) != {"external_context", "prices"} or inputs.get("external_context") != {"status": "ABSENT"} or inputs.get("prices") != {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)}:
        _present_invalid()
    config, payload = package.get("config"), package.get("payload")
    if type(config) is not dict or set(config) != {"sha256", "value"} or type(config.get("value")) is not dict or config["sha256"] != runner._sha256(runner._canonical_json(config["value"])):
        _present_invalid()
    if config["value"] != {"as_of": as_of, "enabled": True, "mode": "shadow", "plugin": "market_regime_control", "prices": "@input:prices", "strategy": "tqqq_growth_income"}:
        _present_invalid()
    if type(payload) is not dict or set(payload) != {"bytes_b64", "schema_version", "sha256", "size_bytes"} or payload.get("schema_version") != "market_regime_control.v1" or type(payload.get("bytes_b64")) is not str:
        _present_invalid()
    try:
        payload_bytes = base64.b64decode(payload["bytes_b64"], validate=True)
    except (binascii.Error, ValueError):
        _present_invalid()
    payload_value = _strict_json(payload_bytes, _present_invalid)
    if (
        payload.get("sha256") != runner._sha256(payload_bytes)
        or payload.get("size_bytes") != len(payload_bytes)
        or type(payload_value) is not dict
        or set(payload_value) != PAYLOAD_KEYS
        or runner._canonical_json(payload_value) != payload_bytes
        or any(
            payload_value.get(key) != value
            for key, value in {
                "as_of": as_of,
                "configured_mode": "shadow",
                "effective_mode": "shadow",
                "mode": "shadow",
                "plugin": "market_regime_control",
                "profile": "market_regime_control",
                "schema_version": "market_regime_control.v1",
                "strategy": "tqqq_growth_income",
                "target_type": "strategy",
            }.items()
        )
    ):
        _present_invalid()
    policy = payload_value.get("consumption_policy")
    if (
        type(policy) is not dict
        or policy.get("evidence_status") != "automation_approved"
        or policy.get("plugin") != "market_regime_control"
        or policy.get("position_control_allowed") is not True
        or policy.get("strategy") != "tqqq_growth_income"
    ):
        _present_invalid()
    return package


def run_tqqq_local_no_order_present(*, input_bundle: str | Path, input_bundle_manifest_sha256: str, plugin_control_package: str | Path, plugin_control_package_sha256: str, qsp_commit_sha: str, output_parent: str | Path) -> tuple[Any, Path]:
    """Publish opaque PRESENT lineage while only B enters no-order computation."""
    manifest, raw, benchmark, as_of, session_id = _read_bundle(input_bundle, input_bundle_manifest_sha256, qsp_commit_sha)
    package = _verify_package(plugin_control_package, digest=plugin_control_package_sha256, commit=qsp_commit_sha, raw=raw, as_of=as_of, session_id=session_id)
    control = {"calendar_authority": "provider_observed_unverified", "historical_backfill": False, "input_bundle": {"manifest": manifest, "manifest_sha256": input_bundle_manifest_sha256}, "optimization_eligible": False, "package": {"sha256": plugin_control_package_sha256, "value": package}, "status": "PRESENT"}
    return runner._run_tqqq_local_no_order(benchmark_history_csv="<verified-qsp-projection>", as_of=as_of, session_id=session_id, output_parent=output_parent, plugin_control=control, market_csv_bytes=benchmark)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    names = ["--input-bundle", "--input-bundle-manifest-sha256", "--plugin-control-package", "--plugin-control-package-sha256", "--qsp-commit-sha", "--output-parent"]
    if len(args) != 12 or args[::2] != names or getattr(sys.modules.get("__main__"), "__spec__", None) is None:
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
