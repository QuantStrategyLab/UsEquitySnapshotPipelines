"""No-order, provenance-bound TQQQ R1 QQQ/TQQQ snapshot materializer."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
from io import BytesIO
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import pandas as pd

from .yfinance_prices import download_price_history


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
RECEIPT_SCHEMA_VERSION = "tqqq_r1_retrieval_receipt.v1"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
ROUTE = "Yahoo/yfinance public USD0"
CLASSIFICATION = "RESEARCH_SNAPSHOT_CORE_ONLY_NON_PRODUCTION_EQUIVALENT"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")
SOURCE_IDENTITY_PATHS = (
    Path("src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py"),
    Path("src/us_equity_snapshot_pipelines/yfinance_prices.py"),
)


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable R1 contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str
    receipt_path: Path
    receipt_sha256: str


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Mapping[str, object]) -> bytes:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    path.write_text(encoded, encoding="utf-8")
    return encoded.encode("utf-8")


def _parse_json_object(raw: bytes, name: str) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if type(value) is not dict:
        _invalid(f"invalid {name}")
    return value


def _has_exact_type(value: object, expected: object) -> bool:
    if type(value) is not type(expected):
        return False
    if type(expected) is dict:
        return set(value) == set(expected) and all(_has_exact_type(value[key], expected[key]) for key in expected)
    if type(expected) is list:
        return len(value) == len(expected) and all(_has_exact_type(actual, wanted) for actual, wanted in zip(value, expected))
    return value == expected


def _normalize_session(value: object) -> pd.Timestamp:
    if pd.api.types.is_bool(value):
        _invalid("invalid session")
    try:
        session = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid session") from exc
    if pd.isna(session):
        _invalid("invalid session")
    if session.tz is not None:
        _invalid("timezone-aware session is not allowed")
    return session.normalize()


def _is_canonical_session(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:].isdigit()
    )


def _require_once(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    if any(list(frame.columns).count(column) != 1 for column in columns):
        _invalid("required columns must appear exactly once")
    missing = set(columns).difference(frame.columns)
    if missing:
        _invalid(f"missing required columns: {', '.join(sorted(missing))}")


def _normalize_prices(
    prices: pd.DataFrame,
    *,
    require_exact_columns: bool = False,
    require_canonical_symbols: bool = False,
    require_canonical_sessions: bool = False,
    require_complete_sessions: bool = True,
) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("prices must be a DataFrame")
    required = ("session", "symbol", PRICE_FIELD)
    _require_once(prices, required)
    if require_exact_columns and list(prices.columns) != list(required):
        _invalid("prices.csv must contain exact columns")
    normalized = prices.loc[:, list(required)].copy()
    if require_canonical_symbols:
        if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
            _invalid("prices.csv contains noncanonical symbol")
    else:
        normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    if set(normalized["symbol"]) != set(SYMBOLS):
        _invalid("missing required symbol or unexpected symbol")
    raw_sessions = normalized["session"]
    normalized["session"] = raw_sessions.map(_normalize_session)
    if require_canonical_sessions and not raw_sessions.map(_is_canonical_session).all():
        _invalid("prices.csv contains noncanonical session")
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        _invalid("session precedes requested lower bound")
    if (normalized["session"].dt.dayofweek >= 5).any():
        _invalid("observed session must be a weekday")
    if normalized.duplicated(["session", "symbol"]).any():
        _invalid("duplicate session for symbol")
    if require_complete_sessions and not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")
    adjusted_close = normalized[PRICE_FIELD]
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    if pd.api.types.is_datetime64_any_dtype(adjusted_close) or pd.api.types.is_timedelta64_dtype(adjusted_close):
        _invalid("datetime-like adjusted_close is not allowed")
    normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if (
        normalized[PRICE_FIELD].isna().any()
        or not normalized[PRICE_FIELD].map(math.isfinite).all()
        or (normalized[PRICE_FIELD] <= 0).any()
    ):
        _invalid("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _coverage(prices: pd.DataFrame) -> dict[str, dict[str, object]]:
    return {
        symbol: {
            "first_session": prices.loc[prices["symbol"].eq(symbol), "session"].min().strftime("%Y-%m-%d"),
            "last_session": prices.loc[prices["symbol"].eq(symbol), "session"].max().strftime("%Y-%m-%d"),
            "row_count": int(prices["symbol"].eq(symbol).sum()),
        }
        for symbol in SYMBOLS
    }


def _common_session_coverage(prices: pd.DataFrame) -> dict[str, object]:
    sessions = prices["session"].drop_duplicates().sort_values()
    return {
        "first_session": sessions.iloc[0].strftime("%Y-%m-%d"),
        "last_session": sessions.iloc[-1].strftime("%Y-%m-%d"),
        "row_count": int(len(sessions)),
    }


def _normalize_downloaded_prices(downloaded: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, object]], dict[str, object]]:
    if not isinstance(downloaded, pd.DataFrame):
        _invalid("downloaded prices must be a DataFrame")
    _require_once(downloaded, ("symbol", "as_of", "close"))
    raw = downloaded.loc[:, ["symbol", "as_of", "close"]].rename(columns={"as_of": "session", "close": PRICE_FIELD})
    raw = _normalize_prices(raw, require_complete_sessions=False)
    raw_coverage = _coverage(raw)
    common_sessions = set(raw.loc[raw["symbol"].eq(SYMBOLS[0]), "session"])
    common_sessions.intersection_update(raw.loc[raw["symbol"].eq(SYMBOLS[1]), "session"])
    common = raw.loc[raw["session"].isin(common_sessions)].copy()
    if common.empty:
        _invalid("no common observed sessions")
    common = _normalize_prices(common)
    return common, raw_coverage, _common_session_coverage(common)


def _write_prices(path: Path, prices: pd.DataFrame) -> None:
    output = prices.copy()
    output["session"] = output["session"].dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.17g")


def _receipt_path(output_dir: Path) -> Path:
    return output_dir.parent / f"{output_dir.name}.tqqq_r1_retrieval_receipt.v1.json"


def _candidate_source_roots(module_path: Path) -> list[Path]:
    candidates = [module_path.parents[2]]
    try:
        direct_url = importlib.metadata.distribution("us-equity-snapshot-pipelines").read_text("direct_url.json")
        source_url = json.loads(direct_url or "{}").get("url")
        parsed = urlsplit(source_url) if type(source_url) is str else None
        if parsed is not None and parsed.scheme == "file":
            candidates.append(Path(unquote(parsed.path)))
    except (importlib.metadata.PackageNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    return list(dict.fromkeys(candidates))


def _source_identity() -> dict[str, str]:
    module_path = Path(__file__).resolve()
    helper_module = sys.modules.get(download_price_history.__module__)
    helper_file = getattr(helper_module, "__file__", None)
    if not isinstance(helper_file, str):
        _invalid("unable to resolve verified source identity")
    executed_paths = (module_path, Path(helper_file).resolve())
    for repository_root in _candidate_source_roots(module_path):
        if not (repository_root / SOURCE_IDENTITY_PATHS[0]).is_file() or (
            repository_root / SOURCE_IDENTITY_PATHS[0]
        ).read_bytes() != executed_paths[0].read_bytes():
            continue
        try:
            dirty_paths = subprocess.run(
                ["git", "status", "--porcelain=v1", "--untracked-files=all", "--", *map(str, SOURCE_IDENTITY_PATHS)],
                cwd=repository_root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            if dirty_paths:
                _invalid("dirty source identity paths are not allowed")
            if any(
                not (repository_root / relative_path).is_file()
                or (repository_root / relative_path).read_bytes() != executed_path.read_bytes()
                for relative_path, executed_path in zip(SOURCE_IDENTITY_PATHS, executed_paths)
            ):
                continue
            repository = subprocess.run(
                ["git", "config", "--get", "remote.origin.url"],
                cwd=repository_root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=repository_root, check=True, capture_output=True, text=True
            ).stdout.strip()
            tree = subprocess.run(
                ["git", "rev-parse", "HEAD^{tree}"], cwd=repository_root, check=True, capture_output=True, text=True
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise SnapshotValidationError("unable to resolve verified source identity") from exc
        if not repository or not commit or not tree:
            _invalid("unable to resolve verified source identity")
        parsed_repository = urlsplit(repository)
        if parsed_repository.scheme and parsed_repository.hostname:
            host = parsed_repository.hostname
            if parsed_repository.port:
                host = f"{host}:{parsed_repository.port}"
            repository = urlunsplit((parsed_repository.scheme, host, parsed_repository.path, "", ""))
        return {"repository": repository, "commit": commit, "tree": tree}
    _invalid("unable to resolve verified source identity")


def _yfinance_runtime_version() -> str:
    try:
        return importlib.metadata.version("yfinance")
    except importlib.metadata.PackageNotFoundError as exc:
        raise SnapshotValidationError("yfinance runtime version is unavailable") from exc


def _retrieval_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _reject_proxy_configuration() -> None:
    proxy_variables = (
        "YFINANCE_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    )
    if any(os.environ.get(name) for name in proxy_variables):
        _invalid("proxy is not allowed")


def _clear_yfinance_proxy_configuration() -> None:
    import yfinance as yf

    try:
        yf.config.network.proxy = None
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnapshotValidationError("unable to clear yfinance proxy") from exc
    if yf.config.network.proxy is not None:
        _invalid("unable to clear yfinance proxy")


def _new_york_now() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))


def _reject_unclosed_trading_session(prices: pd.DataFrame) -> None:
    now = _new_york_now()
    if now.weekday() < 5 and now.hour < 16 and prices["session"].eq(pd.Timestamp(now.date())).any():
        _invalid("unclosed trading session is not allowed")


def _publish_receipt_no_clobber(receipt_path: Path, receipt_bytes: bytes) -> None:
    with receipt_path.open("xb") as handle:
        handle.write(receipt_bytes)


def _build_receipt(
    *,
    coverage: dict[str, dict[str, object]],
    common_sessions: dict[str, object],
    source: dict[str, str],
    yfinance_version: str,
    retrieval_utc: str,
) -> dict[str, object]:
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "route": ROUTE,
        "requested_symbols": list(SYMBOLS),
        "observed_symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "observed_coverage": coverage,
        "common_session_coverage": common_sessions,
        "price_field": PRICE_FIELD,
        "adjusted_price": True,
        "yfinance_runtime_version": yfinance_version,
        "retrieval_utc": retrieval_utc,
        "source": source,
        "classification": CLASSIFICATION,
        "plugin": PLUGIN,
        "size": 0,
        "provider_observed_weekdays": True,
        "cross_symbol_alignment": True,
    }


def _validate_coverage(value: object, *, common: bool = False) -> None:
    expected_keys = {"first_session", "last_session", "row_count"}
    if common:
        if type(value) is not dict or set(value) != expected_keys:
            _invalid("invalid receipt")
        values = [value]
    else:
        if type(value) is not dict or set(value) != set(SYMBOLS):
            _invalid("invalid receipt")
        values = list(value.values())
    for item in values:
        if type(item) is not dict or set(item) != expected_keys:
            _invalid("invalid receipt")
        if (
            not _is_canonical_session(item["first_session"])
            or not _is_canonical_session(item["last_session"])
            or type(item["row_count"]) is not int
            or item["row_count"] <= 0
        ):
            _invalid("invalid receipt")


def _coverage_session(value: object) -> pd.Timestamp:
    if not _is_canonical_session(value):
        _invalid("receipt coverage mismatch")
    try:
        session = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("receipt coverage mismatch") from exc
    if pd.isna(session) or session.dayofweek >= 5:
        _invalid("receipt coverage mismatch")
    return session


def _validate_receipt(receipt: object) -> dict[str, Any]:
    if type(receipt) is not dict:
        _invalid("invalid receipt")
    required = {
        "schema_version",
        "route",
        "requested_symbols",
        "observed_symbols",
        "requested_lower_bound",
        "observed_coverage",
        "common_session_coverage",
        "price_field",
        "adjusted_price",
        "yfinance_runtime_version",
        "retrieval_utc",
        "source",
        "classification",
        "plugin",
        "size",
        "provider_observed_weekdays",
        "cross_symbol_alignment",
    }
    if set(receipt) != required:
        _invalid("invalid receipt")
    if (
        receipt["schema_version"] != RECEIPT_SCHEMA_VERSION
        or receipt["route"] != ROUTE
        or receipt["requested_symbols"] != list(SYMBOLS)
        or receipt["observed_symbols"] != list(SYMBOLS)
        or receipt["requested_lower_bound"] != REQUESTED_LOWER_BOUND
        or receipt["price_field"] != PRICE_FIELD
        or receipt["adjusted_price"] is not True
        or receipt["classification"] != CLASSIFICATION
        or receipt["plugin"] != PLUGIN
        or type(receipt["size"]) is not int
        or receipt["size"] != 0
        or receipt["provider_observed_weekdays"] is not True
        or receipt["cross_symbol_alignment"] is not True
        or type(receipt["yfinance_runtime_version"]) is not str
        or not receipt["yfinance_runtime_version"]
        or type(receipt["retrieval_utc"]) is not str
        or not receipt["retrieval_utc"].endswith("Z")
    ):
        _invalid("invalid receipt")
    try:
        retrieval_time = datetime.fromisoformat(receipt["retrieval_utc"].replace("Z", "+00:00"))
    except ValueError:
        _invalid("invalid receipt")
    if retrieval_time.tzinfo != timezone.utc:
        _invalid("invalid receipt")
    source = receipt["source"]
    if type(source) is not dict or set(source) != {"repository", "commit", "tree"}:
        _invalid("invalid receipt")
    if any(type(value) is not str or not value for value in source.values()):
        _invalid("invalid receipt")
    _validate_coverage(receipt["observed_coverage"])
    _validate_coverage(receipt["common_session_coverage"], common=True)
    return receipt


def _expected_manifest(prices: pd.DataFrame, receipt: dict[str, Any], receipt_sha256: str, prices_sha256: str) -> dict[str, object]:
    return {
        "contract_version": CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "classification": CLASSIFICATION,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": prices_sha256,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "receipt_sha256": receipt_sha256,
        "route": receipt["route"],
        "requested_symbols": receipt["requested_symbols"],
        "observed_symbols": receipt["observed_symbols"],
        "requested_lower_bound_retrieval": receipt["requested_lower_bound"],
        "observed_coverage": receipt["observed_coverage"],
        "common_session_coverage": receipt["common_session_coverage"],
        "adjusted_price": receipt["adjusted_price"],
        "yfinance_runtime_version": receipt["yfinance_runtime_version"],
        "retrieval_utc": receipt["retrieval_utc"],
        "source": receipt["source"],
    }


def _expected_validation(prices: pd.DataFrame, receipt: dict[str, Any]) -> dict[str, object]:
    return {
        "valid": True,
        "coverage": receipt["observed_coverage"],
        "duplicate_sessions": False,
        "finite_positive_adjusted_close": True,
        "provider_observed_weekdays": True,
        "exact_symbols": list(SYMBOLS),
        "common_session_alignment": receipt["common_session_coverage"],
        "strict_readback": True,
        "row_count": len(prices),
    }


def _assert_receipt_matches_prices(receipt: dict[str, Any], prices: pd.DataFrame) -> None:
    if receipt["common_session_coverage"] != _common_session_coverage(prices):
        _invalid("receipt common-session coverage mismatch")
    common_coverage = receipt["common_session_coverage"]
    common_first = _coverage_session(common_coverage["first_session"])
    common_last = _coverage_session(common_coverage["last_session"])
    if common_first > common_last:
        _invalid("receipt coverage mismatch")
    coverage = receipt["observed_coverage"]
    for symbol in SYMBOLS:
        observed_first = _coverage_session(coverage[symbol]["first_session"])
        observed_last = _coverage_session(coverage[symbol]["last_session"])
        if observed_first > observed_last or observed_first > common_first or observed_last < common_last:
            _invalid("receipt coverage mismatch")
        if coverage[symbol]["row_count"] < int(prices["symbol"].eq(symbol).sum()):
            _invalid("receipt coverage mismatch")


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_receipt_path: str | Path,
    expected_receipt_bytes: bytes,
) -> SnapshotResult:
    output = Path(output_dir)
    receipt_path = Path(expected_receipt_path)
    if output.is_symlink() or receipt_path.is_symlink() or receipt_path.parent == output:
        _invalid("symlinked snapshot path is not allowed")
    try:
        if receipt_path.resolve().is_relative_to(output.resolve()):
            _invalid("receipt must be outside snapshot directory")
        names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
        receipt_bytes = receipt_path.read_bytes()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if type(expected_receipt_bytes) is not bytes or receipt_bytes != expected_receipt_bytes:
        _invalid("trusted receipt bytes mismatch")
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        _invalid(f"unexpected output files: {names}")
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in OUTPUT_FILENAMES):
        _invalid("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in OUTPUT_FILENAMES}
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if type(expected_manifest_sha256) is not str or member_hashes["manifest.json"] != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")
    receipt = _validate_receipt(_parse_json_object(receipt_bytes, "receipt"))
    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    manifest = _parse_json_object(members["manifest.json"], "manifest")
    validation = _parse_json_object(members["validation.json"], "validation")
    if set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        _invalid("invalid sha256sums")
    for name, expected in sums.items():
        if type(expected) is not str or member_hashes[name] != expected:
            _invalid(f"hash mismatch: {name}")
    try:
        prices = _normalize_prices(
            pd.read_csv(BytesIO(members["prices.csv"])),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
        )
    except (UnicodeDecodeError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("invalid prices.csv") from exc
    _assert_receipt_matches_prices(receipt, prices)
    receipt_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
    if not _has_exact_type(manifest, _expected_manifest(prices, receipt, receipt_sha256, member_hashes["prices.csv"])):
        _invalid("invalid manifest")
    if not _has_exact_type(validation, _expected_validation(prices, receipt)):
        _invalid("invalid validation")
    return SnapshotResult(output, member_hashes["manifest.json"], receipt_path, receipt_sha256)


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    retrieval_receipt: Mapping[str, object],
    receipt_path: str | Path | None = None,
    mode: str = MODE,
    plugin: str = PLUGIN,
    size: int = 0,
) -> SnapshotResult:
    """Atomically write a strict-readback snapshot from no-order runner input."""
    if mode != MODE or plugin != PLUGIN or size != 0:
        _invalid("snapshot must remain core_only, ABSENT_DISABLED, and size zero")
    normalized = _normalize_prices(prices)
    receipt = _validate_receipt(dict(retrieval_receipt))
    _assert_receipt_matches_prices(receipt, normalized)
    destination = Path(output_dir)
    actual_receipt_path = Path(receipt_path) if receipt_path is not None else _receipt_path(destination)
    if destination.exists() or destination.is_symlink() or actual_receipt_path.exists() or actual_receipt_path.is_symlink():
        _invalid("immutable output or receipt already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    staged_receipt = temporary.parent / f".{temporary.name}.receipt.json"
    receipt_published = False
    try:
        receipt_bytes = _write_json(staged_receipt, receipt)
        prices_path = temporary / "prices.csv"
        _write_prices(prices_path, normalized)
        _write_json(temporary / "validation.json", _expected_validation(normalized, receipt))
        receipt_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
        _write_json(
            temporary / "manifest.json",
            _expected_manifest(normalized, receipt, receipt_sha256, _sha256(prices_path)),
        )
        _write_json(
            temporary / "sha256sums.json",
            {name: _sha256(temporary / name) for name in ("prices.csv", "manifest.json", "validation.json")},
        )
        manifest_sha256 = _sha256(temporary / "manifest.json")
        verify_tqqq_r1_snapshot(
            temporary,
            expected_manifest_sha256=manifest_sha256,
            expected_receipt_path=staged_receipt,
            expected_receipt_bytes=receipt_bytes,
        )
        _publish_receipt_no_clobber(actual_receipt_path, receipt_bytes)
        staged_receipt.unlink()
        receipt_published = True
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        staged_receipt.unlink(missing_ok=True)
        if receipt_published and actual_receipt_path.is_file() and _sha256(actual_receipt_path) == receipt_sha256:
            actual_receipt_path.unlink(missing_ok=True)
        raise
    return SnapshotResult(destination, manifest_sha256, actual_receipt_path, receipt_sha256)


def run_tqqq_r1_snapshot(output_dir: str | Path) -> SnapshotResult:
    """Download exactly QQQ/TQQQ once and materialize only common observed sessions."""
    _reject_proxy_configuration()
    _clear_yfinance_proxy_configuration()
    downloaded = download_price_history(list(SYMBOLS), start=REQUESTED_LOWER_BOUND, price_field=PRICE_FIELD)
    prices, coverage, common_sessions = _normalize_downloaded_prices(downloaded)
    _reject_unclosed_trading_session(prices)
    receipt = _build_receipt(
        coverage=coverage,
        common_sessions=common_sessions,
        source=_source_identity(),
        yfinance_version=_yfinance_runtime_version(),
        retrieval_utc=_retrieval_utc(),
    )
    return materialize_tqqq_r1_snapshot(prices, output_dir, retrieval_receipt=receipt)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Materialize the no-order TQQQ R1 research snapshot.")
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args(argv)
    result = run_tqqq_r1_snapshot(args.output_dir)
    print(json.dumps({"output_dir": str(result.output_dir), "manifest_sha256": result.manifest_sha256}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
