"""One-shot TQQQ three-input acquisition-to-evidence orchestration."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_platform_kit.data import research_mandate as research_mandate_store
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)
from quant_platform_kit.data.research_mandate import ResearchMandateAuthorityGuard
from quant_platform_kit.ibkr import StrictAdjustedHistoryResult
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    validate_evidence_package_v2,
)
from us_equity_snapshot_pipelines.tqqq_r1_snapshot import _publish_noreplace

from . import tqqq_promotion_runner as promotion_runner
from .soxl_acquisition_orchestration import OFFICIAL_IBAPI_PROVENANCE_SHA256
from .soxl_pit_input_packager import _xnys_holidays
from .tqqq_promotion_evidence import (
    run_tqqq_promotion_diagnostic,
    run_tqqq_promotion_evidence,
)

TQQQ_PROMOTION_ASSETS = ("QQQ", "TQQQ", "BOXX")
EXACT_DURATIONS = {"QQQ": "9 Y", "TQQQ": "9 Y", "BOXX": "4 Y"}
FIRST_ELIGIBLE_SESSION = {"BOXX": "2022-12-28"}
FIXED_CUTOFF = "2025-07-02T03:59:59Z"
INPUT_LICENSE = "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04"
INPUT_USAGE_SCOPE = "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION"
QPK_REVISION = promotion_runner._QPK_REVISION
UES_REVISION = promotion_runner._UES_REVISION

_INPUT_CONTRACT_ID = "tqqq_etf_only_ibkr_adjusted_last.v1"
_INPUT_SCHEMA = "tqqq_etf_only_private_bars.v1"
_PROFILE = "tqqq_etf_only_single_strategy_research_v1"
_MANDATE_ID = "tqqq_etf_only_research_v1"
_FROZEN_CALENDAR_SHA256 = "cb72b5dde5293bdb029c53e20ed3d06198e6d3bf096a4993139e59e5377ef51c"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_DEPENDENCY_REPOSITORIES = {
    "quant-platform-kit": "https://github.com/QuantStrategyLab/QuantPlatformKit.git",
    "us-equity-strategies": "https://github.com/QuantStrategyLab/UsEquityStrategies.git",
}
_SESSION_PROVIDER = {
    "paper": "IBKR Paper Gateway TWS API",
    "live-data-only": "IBKR Live Gateway Data Only TWS API",
}
_SESSION_TOOL = {
    "paper": "tqqq_ibkr_paper_single_acquisition",
    "live-data-only": "tqqq_ibkr_live_data_only_single_acquisition",
}
_DIAGNOSTIC_CHANGED_PATHS = {
    "scripts/run_existing_tqqq_snapshot_diagnostic.py",
    "src/us_equity_snapshot_pipelines/lifecycle/tqqq_acquisition_orchestration.py",
    "src/us_equity_snapshot_pipelines/lifecycle/tqqq_promotion_evidence.py",
    "tests/test_tqqq_promotion_input_acquisition.py",
}
_DIAGNOSTIC_FUNCTION_IDENTIFIERS = {
    "quant_platform_kit.risk.engine:RiskEngine.assess",
    "quant_platform_kit.risk.engine:assess",
    "quant_platform_kit.risk.engine:evaluate",
    "quant_platform_kit.risk.engine:resolve",
    "quant_platform_kit.risk.gate:_assess_with_evidence_static",
    "quant_platform_kit.risk.gate:_candidate_binding_errors",
    "quant_platform_kit.risk.gate:_exact_tqqq_mandate_errors",
    "quant_platform_kit.risk.gate:assess_with_evidence",
    "quant_platform_kit.strategy_lifecycle.backtest_orchestrator:_validate_promotion_result",
    "quant_platform_kit.strategy_lifecycle.backtest_orchestrator:run_promotion",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration:orchestrate_existing_tqqq_snapshot_diagnostic",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_assessment",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_parse_bar",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_run_tqqq_promotion_replay",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_trade_to_target",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_validate_config",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:_validate_input",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:__call__",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence:run_tqqq_promotion_diagnostic",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:_relative_metrics",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:_run_window",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:_validate_replay",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:run_locked_oos",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:run_purged_fold",
    "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner:run_tqqq_promotion_research",
    "us_equity_strategies.production_parity.tqqq_contract:_decision_errors",
    "us_equity_strategies.production_parity.tqqq_contract:_evidence_errors",
    "us_equity_strategies.production_parity.tqqq_contract:evaluate_tqqq_research_contract",
}


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _frozen_xnys_sessions() -> tuple[str, ...]:
    start = date(2018, 1, 2)
    end = date(2025, 7, 1)
    holidays = set().union(
        *(_xnys_holidays(year) for year in range(start.year, end.year + 1))
    )
    sessions: list[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            sessions.append(current.isoformat())
        current += timedelta(days=1)
    result = tuple(sessions)
    if hashlib.sha256(_canonical(list(result))).hexdigest() != _FROZEN_CALENDAR_SHA256:
        raise RuntimeError("frozen TQQQ XNYS calendar contract is inconsistent")
    return result


FROZEN_XNYS_SESSIONS = _frozen_xnys_sessions()


class TqqqOrchestrationError(ValueError):
    """Sanitized fail-closed error for the concrete one-shot orchestration."""

    def __init__(
        self,
        message: str,
        *,
        snapshot_digest: str | None = None,
        mandate_receipt_digest: str | None = None,
        evidence_artifact_count: int | None = None,
    ) -> None:
        super().__init__(message)
        if snapshot_digest is None:
            self.sanitized_failure = None
            return
        if (
            not isinstance(snapshot_digest, str)
            or not _DIGEST.fullmatch(snapshot_digest)
            or not isinstance(mandate_receipt_digest, str)
            or not _DIGEST.fullmatch(mandate_receipt_digest)
            or isinstance(evidence_artifact_count, bool)
            or not isinstance(evidence_artifact_count, int)
            or evidence_artifact_count < 0
        ):
            raise ValueError("invalid sanitized TQQQ orchestration failure")
        self.sanitized_failure = {
            "classification": "promotion_evidence_failed",
            "evidence_artifact_count": evidence_artifact_count,
            "mandate_receipt_digest": mandate_receipt_digest,
            "runner_completion_count": 0,
            "runner_invocation_count": 1,
            "snapshot_digest": snapshot_digest,
            "stage": (
                "promotion_evidence_pre_artifact"
                if evidence_artifact_count == 0
                else "promotion_evidence"
            ),
        }


def _require_digest(value: str, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqOrchestrationError(f"invalid {label}")
    return value


def _require_revision(value: str, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise TqqqOrchestrationError(f"invalid {label}")
    return value


def _require_timestamp(value: str, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise TqqqOrchestrationError(f"invalid {label}")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise TqqqOrchestrationError(f"invalid {label}") from exc
    if parsed.tzinfo != UTC:
        raise TqqqOrchestrationError(f"invalid {label}")
    return parsed


def _require_text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise TqqqOrchestrationError(f"invalid {label}")
    return value


@dataclass(frozen=True)
class TqqqOrchestrationAuthority:
    """External authority identities required by the frozen TQQQ contracts."""

    authority_receipt_sha256: str
    entitlement_receipt_sha256: str
    license_receipt_sha256: str
    retention_expires_at: str
    risk_standard_id: str
    risk_standard_sha256: str
    platform_execution_revision: str
    input_license: str
    input_usage_scope: str

    def __post_init__(self) -> None:
        _require_digest(self.authority_receipt_sha256, "authority receipt digest")
        _require_digest(self.entitlement_receipt_sha256, "entitlement receipt digest")
        _require_digest(self.license_receipt_sha256, "license receipt digest")
        _require_timestamp(self.retention_expires_at, "retention expiry")
        _require_text(self.risk_standard_id, "risk standard identity")
        _require_digest(self.risk_standard_sha256, "risk standard digest")
        _require_revision(self.platform_execution_revision, "platform revision")
        if self.input_license != INPUT_LICENSE or self.input_usage_scope != INPUT_USAGE_SCOPE:
            raise TqqqOrchestrationError("invalid input usage authority")


def _installed_vcs_revision(distribution_name: str) -> str:
    repository = _DEPENDENCY_REPOSITORIES.get(distribution_name)
    if repository is None:
        raise TqqqOrchestrationError("installed dependency identity is unavailable")
    try:
        distribution = importlib.metadata.distribution(distribution_name)
        raw = distribution.read_text("direct_url.json")
        direct_url = json.loads(raw) if raw else {}
        revision = direct_url.get("vcs_info", {}).get("commit_id")
    except (
        importlib.metadata.PackageNotFoundError,
        AttributeError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise TqqqOrchestrationError(
            "installed dependency identity is unavailable"
        ) from exc
    if direct_url.get("url") != repository or not isinstance(revision, str):
        raise TqqqOrchestrationError("installed dependency identity mismatch")
    return _require_revision(revision, "installed dependency revision")


def resolve_tqqq_runtime_identity() -> tuple[str, str]:
    """Resolve one clean current checkout before any provider contact."""
    revision = promotion_runner._resolve_runner_revision()
    repository = Path(__file__).resolve().parents[3]
    try:
        status = subprocess.run(
            ["git", "-C", str(repository), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        )
        head = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        tree = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise TqqqOrchestrationError("runner implementation identity is unavailable") from exc
    if status.stdout or head != revision:
        raise TqqqOrchestrationError("runner implementation checkout is not immutable")
    return _require_revision(revision, "runner revision"), _require_revision(
        tree, "runner tree"
    )


def _strict_bars(
    results: Mapping[str, StrictAdjustedHistoryResult],
) -> dict[str, Any]:
    if not isinstance(results, Mapping) or tuple(results) != TQQQ_PROMOTION_ASSETS:
        raise TqqqOrchestrationError("exact three-input result is required")
    symbols: dict[str, list[dict[str, Any]]] = {}
    for symbol in TQQQ_PROMOTION_ASSETS:
        result = results[symbol]
        if not isinstance(result, StrictAdjustedHistoryResult):
            raise TqqqOrchestrationError("exact three-input result is required")
        first = FIRST_ELIGIBLE_SESSION.get(symbol, FROZEN_XNYS_SESSIONS[0])
        expected_sessions = tuple(
            value for value in FROZEN_XNYS_SESSIONS if value >= first
        )
        observed_sessions = tuple(candle.session.isoformat() for candle in result.candles)
        provenance = result.provenance
        diagnostic = result.diagnostic
        if (
            observed_sessions != expected_sessions
            or provenance.symbol != symbol
            or provenance.exchange != "SMART"
            or provenance.currency != "USD"
            or provenance.end_datetime != FIXED_CUTOFF
            or provenance.duration != EXACT_DURATIONS[symbol]
            or provenance.bar_size != "1 day"
            or provenance.what_to_show != "ADJUSTED_LAST"
            or provenance.use_rth is not True
            or provenance.format_date != 1
            or provenance.keep_up_to_date is not False
            or provenance.returned_row_count != len(expected_sessions)
            or diagnostic.classification != "exact_match"
            or diagnostic.completion_observed is not True
            or diagnostic.expected_count != len(expected_sessions)
            or diagnostic.observed_in_window_count != len(expected_sessions)
            or diagnostic.missing_count != 0
            or diagnostic.extra_count != 0
            or diagnostic.duplicate_count != 0
            or diagnostic.provider_error_code_counts
        ):
            raise TqqqOrchestrationError("strict history result identity mismatch")
        symbols[symbol] = [
            {
                "date": candle.session.isoformat(),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
            for candle in result.candles
        ]
    return {"schema_version": _INPUT_SCHEMA, "symbols": symbols}


def _source_identity(
    results: Mapping[str, StrictAdjustedHistoryResult],
    authority: TqqqOrchestrationAuthority,
    *,
    observed_at: str,
    session_class: str,
) -> str:
    requests = [
        {
            "symbol": symbol,
            "security": "STK/SMART/USD",
            "end_datetime": "",
            "duration": results[symbol].provenance.duration,
            "bar_size": "1 day",
            "what_to_show": "ADJUSTED_LAST",
            "use_rth": True,
            "format_date": 1,
            "keep_up_to_date": False,
            "session_class": session_class,
        }
        for symbol in TQQQ_PROMOTION_ASSETS
    ]
    return hashlib.sha256(
        _canonical(
            {
                "provider": _SESSION_PROVIDER[session_class],
                "session_class": session_class,
                "official_ibapi_provenance_sha256": OFFICIAL_IBAPI_PROVENANCE_SHA256,
                "entitlement_receipt_sha256": authority.entitlement_receipt_sha256,
                "license_receipt_sha256": authority.license_receipt_sha256,
                "retention_expires_at": authority.retention_expires_at,
                "observed_at": observed_at,
                "fixed_cutoff": FIXED_CUTOFF,
                "calendar_sha256": _FROZEN_CALENDAR_SHA256,
                "requests": requests,
                "fallback": False,
                "substitution": False,
                "retry_count": 0,
            }
        )
    ).hexdigest()


def _input_payload(
    bars: dict[str, Any],
    results: Mapping[str, StrictAdjustedHistoryResult],
    authority: TqqqOrchestrationAuthority,
    *,
    runner_revision: str,
    runner_tree_sha: str,
    observed_at: str,
    session_class: str,
) -> tuple[dict[str, Any], bytes, bytes, str]:
    bars_bytes = _canonical(bars)
    source_revision = _source_identity(
        results,
        authority,
        observed_at=observed_at,
        session_class=session_class,
    )
    sources = [
        {
            "source_id": f"ibkr:{symbol}",
            "revision": source_revision,
            "observed_at": observed_at,
            "content_sha256": hashlib.sha256(
                _canonical(bars["symbols"][symbol])
            ).hexdigest(),
        }
        for symbol in sorted(TQQQ_PROMOTION_ASSETS)
    ]
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": (
                f"tqqq-ibkr-{session_class}-single-acquisition-"
                f"{hashlib.sha256(bars_bytes).hexdigest()[:24]}"
            ),
            "research_input_contract_id": _INPUT_CONTRACT_ID,
            "domain": "us_equity",
            "profile": _PROFILE,
            "artifact_type": "immutable_adjusted_ohlcv_etf_only",
            "observed_at": observed_at,
            "effective_at": observed_at,
            "as_of": observed_at,
            "producer": {
                "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
                "commit_sha": runner_revision,
                "tree_sha": runner_tree_sha,
                "tool": _SESSION_TOOL[session_class],
                "tool_version": "v1",
            },
            "calendar": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "session_date": FROZEN_XNYS_SESSIONS[-1],
                "source": "uesp_repo_local_xnys_holiday_rules",
                "source_revision": _FROZEN_CALENDAR_SHA256,
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "IBKR_ADJUSTED_LAST",
                "source_revision": OFFICIAL_IBAPI_PROVENANCE_SHA256,
            },
            "sources": sources,
            "members": [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(bars_bytes),
                    "sha256": hashlib.sha256(bars_bytes).hexdigest(),
                }
            ],
        }
    )
    manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    manifest_sha256 = research_input_manifest_sha256(manifest)
    payload = {
        "provenance": {
            "evidence_class": "provider_observed",
            "real_producer": True,
            "provider": _SESSION_PROVIDER[session_class],
            "provider_revision": source_revision,
            "session_class": session_class,
            "license": INPUT_LICENSE,
            "usage_scope": INPUT_USAGE_SCOPE,
        },
        "input_manifest": manifest,
        "bars": bars,
    }
    return payload, bars_bytes, manifest_bytes, manifest_sha256


def _config(
    authority: TqqqOrchestrationAuthority, *, session_class: str
) -> dict[str, Any]:
    return {
        "schema_version": "tqqq_etf_only_replay_config.v1",
        "strategy_profile": _PROFILE,
        "signal_model": "qqq_sma_200_close_t_open_t_plus_1",
        "signal_window_sessions": 200,
        "tqqq_nominal_cap": 0.15,
        "boxx_nominal_cap": 0.50,
        "risk_mandate_id": _MANDATE_ID,
        "risk_standard_id": authority.risk_standard_id,
        "risk_standard_sha256": authority.risk_standard_sha256,
        "authority_receipt_sha256": authority.authority_receipt_sha256,
        "platform_execution_revision": authority.platform_execution_revision,
        "input_license": INPUT_LICENSE,
        "input_usage_scope": INPUT_USAGE_SCOPE,
        "session_class": session_class,
    }


def _private_directory(path: Path) -> None:
    if path.is_symlink():
        raise TqqqOrchestrationError("private output root is unavailable")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise TqqqOrchestrationError("private output root is unavailable")
    os.chmod(path, 0o700)


def _write_private(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _publish_input(
    output_root: str | Path,
    *,
    input_manifest_sha256: str,
    bars_bytes: bytes,
    manifest_bytes: bytes,
) -> Path:
    root = Path(output_root)
    _private_directory(root)
    run_root = root / input_manifest_sha256
    if run_root.exists() or run_root.is_symlink():
        raise TqqqOrchestrationError("content-addressed TQQQ input already exists")
    temporary: Path | None = None
    try:
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{input_manifest_sha256}.", dir=root)
        )
        if temporary.is_symlink():
            raise TqqqOrchestrationError("private output symlink is forbidden")
        os.chmod(temporary, 0o700)
        snapshot = temporary / "snapshot"
        snapshot.mkdir(mode=0o700)
        _write_private(snapshot / "bars.json", bars_bytes)
        _write_private(snapshot / "input-manifest.json", manifest_bytes)
        _readback_input(
            snapshot,
            expected_bars_sha256=hashlib.sha256(bars_bytes).hexdigest(),
            expected_bars_size=len(bars_bytes),
            expected_manifest_sha256=input_manifest_sha256,
            expected_manifest_size=len(manifest_bytes),
        )
        _publish_noreplace(temporary, run_root)
        temporary = None
    except TqqqOrchestrationError:
        raise
    except Exception as exc:  # noqa: BLE001 - the diagnostic must capture any root exception
        raise TqqqOrchestrationError(
            "content-addressed TQQQ input publication failed"
        ) from exc
    finally:
        if temporary is not None:
            try:
                if temporary.is_symlink():
                    temporary.unlink()
                elif temporary.exists():
                    shutil.rmtree(temporary)
            except OSError:
                pass
    return run_root / "snapshot"


def _readback_input(
    snapshot: Path,
    *,
    expected_bars_sha256: str,
    expected_bars_size: int,
    expected_manifest_sha256: str,
    expected_manifest_size: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        if (
            snapshot.is_symlink()
            or not snapshot.is_dir()
            or snapshot.stat().st_mode & 0o777 != 0o700
        ):
            raise TqqqOrchestrationError("immutable snapshot readback failed")
        entries = {path.name: path for path in snapshot.iterdir()}
        if set(entries) != {"bars.json", "input-manifest.json"}:
            raise TqqqOrchestrationError("immutable snapshot readback failed")
        for path in entries.values():
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o777 != 0o600:
                raise TqqqOrchestrationError("immutable snapshot readback failed")
        bars_bytes = entries["bars.json"].read_bytes()
        manifest_bytes = entries["input-manifest.json"].read_bytes()
        if (
            len(bars_bytes) != expected_bars_size
            or len(manifest_bytes) != expected_manifest_size
            or hashlib.sha256(bars_bytes).hexdigest() != expected_bars_sha256
            or hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_sha256
        ):
            raise TqqqOrchestrationError("immutable snapshot readback failed")
        bars = json.loads(bars_bytes)
        manifest = validate_research_input_manifest(json.loads(manifest_bytes))
        if (
            _canonical(bars) != bars_bytes
            or canonical_research_input_manifest_bytes(manifest) != manifest_bytes
            or research_input_manifest_sha256(manifest) != expected_manifest_sha256
        ):
            raise TqqqOrchestrationError("immutable snapshot readback failed")
        bars["symbols"] = {
            symbol: bars["symbols"][symbol] for symbol in TQQQ_PROMOTION_ASSETS
        }
        return bars, manifest
    except TqqqOrchestrationError:
        raise
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise TqqqOrchestrationError("immutable snapshot readback failed") from exc


def _seal_private_tree(root: Path) -> None:
    if root.is_symlink() or not root.is_dir():
        raise TqqqOrchestrationError("private output symlink is forbidden")
    paths = list(root.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise TqqqOrchestrationError("private output symlink is forbidden")
    for path in paths:
        os.chmod(path, 0o700 if path.is_dir() else 0o600)
    os.chmod(root, 0o700)


def _require_diagnostic_execution_compatibility(
    execution_revision: str,
    execution_tree_sha: str,
    runner_revision: str,
    runner_tree_sha: str,
) -> None:
    repository = Path(__file__).resolve().parents[3]
    try:
        observed_execution_tree = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", f"{execution_revision}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise TqqqOrchestrationError("snapshot execution identity is unavailable") from exc
    if observed_execution_tree != execution_tree_sha:
        raise TqqqOrchestrationError("snapshot execution tree mismatch")
    if execution_revision == runner_revision:
        if execution_tree_sha != runner_tree_sha:
            raise TqqqOrchestrationError("current runner tree mismatch")
        return
    try:
        ancestry = subprocess.run(
            ["git", "-C", str(repository), "merge-base", "--is-ancestor", execution_revision, runner_revision],
            check=False,
            capture_output=True,
            text=True,
        )
        changed = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "diff",
                "--name-only",
                execution_revision,
                runner_revision,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise TqqqOrchestrationError("diagnostic revision compatibility is unavailable") from exc
    changed_paths = set(changed.stdout.splitlines())
    if (
        ancestry.returncode != 0
        or not changed_paths
        or not changed_paths <= _DIAGNOSTIC_CHANGED_PATHS
    ):
        raise TqqqOrchestrationError("diagnostic revision compatibility mismatch")


def _load_existing_tqqq_snapshot(
    run_root: Path,
    *,
    expected_snapshot_digest: str,
    execution_revision: str,
    execution_tree_sha: str,
    runner_revision: str,
    runner_tree_sha: str,
    session_class: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        if (
            run_root.is_symlink()
            or not run_root.is_dir()
            or stat.S_IMODE(run_root.stat().st_mode) != 0o700
        ):
            raise TqqqOrchestrationError("invalid private diagnostic run root")
        entries = {path.name: path for path in run_root.iterdir()}
    except OSError as exc:
        raise TqqqOrchestrationError("private diagnostic run root is unavailable") from exc
    if set(entries) != {"mandate-authority.sqlite3", "snapshot"}:
        raise TqqqOrchestrationError("invalid private diagnostic run members")
    snapshot = entries["snapshot"]
    try:
        if (
            snapshot.is_symlink()
            or not snapshot.is_dir()
            or stat.S_IMODE(snapshot.stat().st_mode) != 0o700
        ):
            raise TqqqOrchestrationError("invalid private snapshot directory")
        snapshot_entries = {path.name: path for path in snapshot.iterdir()}
    except OSError as exc:
        raise TqqqOrchestrationError("private snapshot is unavailable") from exc
    if set(snapshot_entries) != {"bars.json", "input-manifest.json"}:
        raise TqqqOrchestrationError("invalid private snapshot members")
    for path in snapshot_entries.values():
        try:
            if (
                path.is_symlink()
                or not path.is_file()
                or stat.S_IMODE(path.stat().st_mode) != 0o600
            ):
                raise TqqqOrchestrationError("invalid private snapshot member")
        except OSError as exc:
            raise TqqqOrchestrationError("private snapshot is unavailable") from exc
    try:
        bars_bytes = snapshot_entries["bars.json"].read_bytes()
        manifest_bytes = snapshot_entries["input-manifest.json"].read_bytes()
        manifest = validate_research_input_manifest(json.loads(manifest_bytes))
    except Exception as exc:
        raise TqqqOrchestrationError("immutable snapshot readback failed") from exc
    if (
        hashlib.sha256(manifest_bytes).hexdigest() != expected_snapshot_digest
        or canonical_research_input_manifest_bytes(manifest) != manifest_bytes
        or research_input_manifest_sha256(manifest) != expected_snapshot_digest
    ):
        raise TqqqOrchestrationError("snapshot digest mismatch")
    members = manifest["members"]
    if (
        len(members) != 1
        or members[0]["path"] != "bars.json"
        or members[0]["media_type"] != "application/json"
        or members[0]["size_bytes"] != len(bars_bytes)
        or members[0]["sha256"] != hashlib.sha256(bars_bytes).hexdigest()
    ):
        raise TqqqOrchestrationError("snapshot member integrity mismatch")
    bars, readback_manifest = _readback_input(
        snapshot,
        expected_bars_sha256=members[0]["sha256"],
        expected_bars_size=members[0]["size_bytes"],
        expected_manifest_sha256=expected_snapshot_digest,
        expected_manifest_size=len(manifest_bytes),
    )
    producer = readback_manifest["producer"]
    if producer != {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": execution_revision,
        "tree_sha": execution_tree_sha,
        "tool": _SESSION_TOOL[session_class],
        "tool_version": "v1",
    }:
        raise TqqqOrchestrationError("snapshot producer identity mismatch")
    _require_diagnostic_execution_compatibility(
        execution_revision,
        execution_tree_sha,
        runner_revision,
        runner_tree_sha,
    )
    return bars, readback_manifest


def _validate_consumed_diagnostic_binding(
    database: Path,
    *,
    expected_candidate_id: str,
    expected_config_digest: str,
    expected_snapshot_digest: str,
    expected_authority_id: str,
    expected_receipt_digest: str,
) -> None:
    try:
        if (
            database.is_symlink()
            or not database.is_file()
            or stat.S_IMODE(database.stat().st_mode) != 0o600
        ):
            raise TqqqOrchestrationError("invalid consumed mandate store")
        before = hashlib.sha256(database.read_bytes()).hexdigest()
        connection = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
        try:
            connection.execute("PRAGMA query_only = ON")
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            metadata = connection.execute(
                "SELECT key, value FROM metadata ORDER BY key"
            ).fetchall()
            columns = tuple(
                row[1] for row in connection.execute("PRAGMA table_info(mandates)")
            )
            selected = connection.execute(
                f"SELECT {','.join(research_mandate_store._ROW_FIELDS)} FROM mandates"
            ).fetchall()
        finally:
            connection.close()
        if (
            tables != {"metadata", "mandates"}
            or metadata
            != [
                ("schema_digest", research_mandate_store._SCHEMA_DIGEST),
                ("schema_version", "1"),
            ]
            or columns != research_mandate_store._ROW_FIELDS
            or len(selected) != 1
        ):
            raise TqqqOrchestrationError("invalid consumed mandate store")
        row = dict(zip(research_mandate_store._ROW_FIELDS, selected[0], strict=True))
        research_mandate_store._validate_row(row)
        if (
            row["status"] != "CONSUMED"
            or row["candidate_id"] != expected_candidate_id
            or row["mandate_id"] != _MANDATE_ID
            or row["config_digest"] != expected_config_digest
            or row["input_digest"] != expected_snapshot_digest
            or row["authority_id"] != expected_authority_id
            or row["receipt_digest"] != expected_receipt_digest
            or hashlib.sha256(database.read_bytes()).hexdigest() != before
        ):
            raise TqqqOrchestrationError("consumed mandate identity mismatch")
    except TqqqOrchestrationError:
        raise
    except Exception as exc:
        raise TqqqOrchestrationError("consumed mandate validation failed") from exc


def _root_exception(error: BaseException) -> BaseException:
    observed: set[int] = set()
    current = error
    while id(current) not in observed:
        observed.add(id(current))
        next_error = current.__cause__
        if next_error is None and not current.__suppress_context__:
            next_error = current.__context__
        if next_error is None:
            break
        current = next_error
    return current


def _diagnostic_function_identifiers(error: BaseException) -> list[str]:
    identifiers: list[str] = []
    current: BaseException | None = error
    observed: set[int] = set()
    while current is not None and id(current) not in observed:
        observed.add(id(current))
        traceback = current.__traceback__
        while traceback is not None:
            module = traceback.tb_frame.f_globals.get("__name__")
            function = traceback.tb_frame.f_code.co_name
            identifier = f"{module}:{function}"
            if identifier in _DIAGNOSTIC_FUNCTION_IDENTIFIERS and identifier not in identifiers:
                identifiers.append(identifier)
            traceback = traceback.tb_next
        current = current.__cause__ or (
            None if current.__suppress_context__ else current.__context__
        )
    return identifiers[-3:]


def orchestrate_existing_tqqq_snapshot_diagnostic(
    run_root: str | Path,
    *,
    expected_snapshot_digest: str,
    expected_mandate_receipt_digest: str,
    authority: TqqqOrchestrationAuthority,
    execution_revision: str,
    execution_tree_sha: str,
    runner_revision: str,
    runner_tree_sha: str,
    session_class: str,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Validate a consumed immutable snapshot and replay it once without persistence."""
    if not isinstance(authority, TqqqOrchestrationAuthority):
        raise TqqqOrchestrationError("invalid orchestration authority")
    snapshot_digest = _require_digest(expected_snapshot_digest, "snapshot digest")
    mandate_receipt_digest = _require_digest(
        expected_mandate_receipt_digest, "mandate receipt digest"
    )
    execution_revision = _require_revision(execution_revision, "execution revision")
    execution_tree_sha = _require_revision(execution_tree_sha, "execution tree")
    runner_revision = _require_revision(runner_revision, "runner revision")
    runner_tree_sha = _require_revision(runner_tree_sha, "runner tree")
    if session_class not in _SESSION_PROVIDER:
        raise TqqqOrchestrationError("invalid provider session identity")
    now = (clock or (lambda: datetime.now(UTC)))()
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise TqqqOrchestrationError("invalid diagnostic timestamp")
    now = now.astimezone(UTC).replace(microsecond=0)
    if _require_timestamp(authority.retention_expires_at, "retention expiry") <= now:
        raise TqqqOrchestrationError("retention authority is expired")
    if (
        _installed_vcs_revision("quant-platform-kit") != QPK_REVISION
        or _installed_vcs_revision("us-equity-strategies") != UES_REVISION
    ):
        raise TqqqOrchestrationError("installed dependency identity mismatch")
    root = Path(run_root)
    bars, manifest = _load_existing_tqqq_snapshot(
        root,
        expected_snapshot_digest=snapshot_digest,
        execution_revision=execution_revision,
        execution_tree_sha=execution_tree_sha,
        runner_revision=runner_revision,
        runner_tree_sha=runner_tree_sha,
        session_class=session_class,
    )
    config = _config(authority, session_class=session_class)
    config_digest = hashlib.sha256(_canonical(config)).hexdigest()
    candidate = CandidateRiskIdentity(
        strategy_profile=_PROFILE,
        account_mode="single_strategy_account_v1",
        strategy_revision=UES_REVISION,
        runner_revision=execution_revision,
        config_sha256=config_digest,
        input_manifest_sha256=snapshot_digest,
        authority_receipt_sha256=authority.authority_receipt_sha256,
    )
    _validate_consumed_diagnostic_binding(
        root / "mandate-authority.sqlite3",
        expected_candidate_id=candidate.candidate_sha256,
        expected_config_digest=config_digest,
        expected_snapshot_digest=snapshot_digest,
        expected_authority_id=authority.authority_receipt_sha256,
        expected_receipt_digest=mandate_receipt_digest,
    )
    source_revisions = {source["revision"] for source in manifest["sources"]}
    if len(source_revisions) != 1:
        raise TqqqOrchestrationError("snapshot source identity mismatch")
    input_payload = {
        "provenance": {
            "evidence_class": "provider_observed",
            "real_producer": True,
            "provider": _SESSION_PROVIDER[session_class],
            "provider_revision": source_revisions.pop(),
            "session_class": session_class,
            "license": authority.input_license,
            "usage_scope": authority.input_usage_scope,
        },
        "input_manifest": manifest,
        "bars": bars,
    }
    try:
        run_tqqq_promotion_diagnostic(
            input_payload=input_payload,
            config_payload=config,
            mandate_receipt_sha256=mandate_receipt_digest,
        )
    except Exception as exc:
        root_error = _root_exception(exc)
        return {
            "config_digest": config_digest,
            "exception_class": type(root_error).__name__,
            "function_identifiers": _diagnostic_function_identifiers(exc),
            "mandate_receipt_digest": mandate_receipt_digest,
            "runner_completion_count": 0,
            "runner_invocation_count": 1,
            "snapshot_digest": snapshot_digest,
            "stage": "promotion_replay_exception",
        }
    return {
        "config_digest": config_digest,
        "exception_class": None,
        "function_identifiers": [],
        "mandate_receipt_digest": mandate_receipt_digest,
        "runner_completion_count": 1,
        "runner_invocation_count": 1,
        "snapshot_digest": snapshot_digest,
        "stage": "promotion_replay_completed",
    }


def orchestrate_tqqq_promotion(
    results: Mapping[str, StrictAdjustedHistoryResult],
    *,
    authority: TqqqOrchestrationAuthority,
    output_root: str | Path,
    runner_revision: str,
    runner_tree_sha: str,
    session_class: str = "paper",
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Publish exact input, consume one mandate, then invoke existing evidence once."""
    if not isinstance(authority, TqqqOrchestrationAuthority):
        raise TqqqOrchestrationError("invalid orchestration authority")
    if not isinstance(session_class, str) or session_class not in _SESSION_PROVIDER:
        raise TqqqOrchestrationError("invalid provider session identity")
    runner_revision = _require_revision(runner_revision, "runner revision")
    runner_tree_sha = _require_revision(runner_tree_sha, "runner tree")
    if (
        _installed_vcs_revision("quant-platform-kit") != QPK_REVISION
        or _installed_vcs_revision("us-equity-strategies") != UES_REVISION
    ):
        raise TqqqOrchestrationError("installed dependency identity mismatch")
    now = (clock or (lambda: datetime.now(UTC)))()
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise TqqqOrchestrationError("invalid orchestration timestamp")
    now = now.astimezone(UTC).replace(microsecond=0)
    if _require_timestamp(authority.retention_expires_at, "retention expiry") <= now:
        raise TqqqOrchestrationError("retention authority is expired")
    observed_at = now.isoformat().replace("+00:00", "Z")
    bars = _strict_bars(results)
    input_payload, bars_bytes, manifest_bytes, manifest_sha256 = _input_payload(
        bars,
        results,
        authority,
        runner_revision=runner_revision,
        runner_tree_sha=runner_tree_sha,
        observed_at=observed_at,
        session_class=session_class,
    )
    config = _config(authority, session_class=session_class)
    config_sha256 = hashlib.sha256(_canonical(config)).hexdigest()
    candidate = CandidateRiskIdentity(
        strategy_profile=_PROFILE,
        account_mode="single_strategy_account_v1",
        strategy_revision=UES_REVISION,
        runner_revision=runner_revision,
        config_sha256=config_sha256,
        input_manifest_sha256=manifest_sha256,
        authority_receipt_sha256=authority.authority_receipt_sha256,
    )
    old_umask = os.umask(0o077)
    run_root = Path(output_root) / manifest_sha256
    published = False
    try:
        snapshot = _publish_input(
            output_root,
            input_manifest_sha256=manifest_sha256,
            bars_bytes=bars_bytes,
            manifest_bytes=manifest_bytes,
        )
        published = True
        readback_bars, readback_manifest = _readback_input(
            snapshot,
            expected_bars_sha256=hashlib.sha256(bars_bytes).hexdigest(),
            expected_bars_size=len(bars_bytes),
            expected_manifest_sha256=manifest_sha256,
            expected_manifest_size=len(manifest_bytes),
        )
        input_payload = {
            **input_payload,
            "bars": readback_bars,
            "input_manifest": readback_manifest,
        }
        guard = ResearchMandateAuthorityGuard(
            run_root / "mandate-authority.sqlite3",
            clock=lambda: now,
        )
        mandate = guard.issue(
            candidate_id=candidate.candidate_sha256,
            mandate_id=_MANDATE_ID,
            config_digest=config_sha256,
            input_digest=manifest_sha256,
            authority_id=authority.authority_receipt_sha256,
        )
        consumption = guard.consume(
            mandate,
            candidate_id=candidate.candidate_sha256,
            mandate_id=_MANDATE_ID,
            config_digest=config_sha256,
            input_digest=manifest_sha256,
            authority_id=authority.authority_receipt_sha256,
        )
        evidence_root = run_root / "evidence"
        try:
            evidence = run_tqqq_promotion_evidence(
                input_payload=input_payload,
                config_payload=config,
                output_dir=evidence_root,
                generated_at=observed_at,
                mandate_receipt_sha256=consumption.receipt_digest,
            )
            if (
                not isinstance(evidence, Mapping)
                or set(evidence)
                != {
                    "evidence_sha256",
                    "promotion_result_sha256",
                    "candidate_identity_sha256",
                    "input_manifest_sha256",
                }
                or evidence["input_manifest_sha256"] != manifest_sha256
                or any(
                    not isinstance(evidence[field], str)
                    or not _DIGEST.fullmatch(evidence[field])
                    for field in evidence
                )
            ):
                raise ValueError("invalid evidence identity")
            evidence_bytes = (evidence_root / "strategy-evidence-package.v2.json").read_bytes()
            terminal_bytes = (evidence_root / "promotion-research-result.v1.json").read_bytes()
            evidence_payload = json.loads(evidence_bytes)
            terminal_payload = json.loads(terminal_bytes)
            promotion_run = evidence_payload.get("backtest", {}).get("promotion_run", {})
            fold_results = promotion_run.get("fold_results", [])
            locked_oos_result = promotion_run.get("locked_oos_result")
            if (
                hashlib.sha256(evidence_bytes).hexdigest() != evidence["evidence_sha256"]
                or hashlib.sha256(terminal_bytes).hexdigest()
                != evidence["promotion_result_sha256"]
                or terminal_payload.get("status") != "EVIDENCE_V2_COMPLETE"
                or terminal_payload.get("candidate_identity_sha256")
                != evidence["candidate_identity_sha256"]
                or terminal_payload.get("input_manifest_sha256") != manifest_sha256
                or not isinstance(fold_results, list)
                or not fold_results
                or not isinstance(locked_oos_result, Mapping)
                or any(
                    not isinstance(result, Mapping)
                    or result.get("params", {}).get("mandate_receipt_sha256")
                    != consumption.receipt_digest
                    for result in [*fold_results, locked_oos_result]
                )
                or evidence_payload.get("lifecycle_claims")
                != {
                    "learning_only": False,
                    "promotion_eligible": False,
                    "live_ready": False,
                    "size_zero_required": True,
                    "no_order": True,
                }
                or any(
                    terminal_payload.get(field) is not expected
                    for field, expected in {
                        "promotion_eligible": False,
                        "live_ready": False,
                        "size_zero_required": True,
                        "no_order": True,
                    }.items()
                )
            ):
                raise ValueError("invalid evidence readback")
            if validate_evidence_package_v2(
                evidence_payload, base_dir=evidence_root
            ):
                raise ValueError("invalid referenced evidence artifacts")
        except Exception as exc:
            try:
                artifact_count = sum(path.is_file() for path in evidence_root.rglob("*"))
            except OSError:
                artifact_count = 0
            raise TqqqOrchestrationError(
                "promotion evidence failed",
                snapshot_digest=manifest_sha256,
                mandate_receipt_digest=consumption.receipt_digest,
                evidence_artifact_count=artifact_count,
            ) from exc
        _seal_private_tree(run_root)
        return {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": len(TQQQ_PROMOTION_ASSETS),
            "snapshot_digest": manifest_sha256,
            "evidence_digest": evidence["evidence_sha256"],
            "mandate_receipt_digest": consumption.receipt_digest,
            "rerun_count": 1,
        }
    except TqqqOrchestrationError:
        if published and run_root.exists() and not run_root.is_symlink():
            _seal_private_tree(run_root)
        raise
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        if published and run_root.exists() and not run_root.is_symlink():
            _seal_private_tree(run_root)
        raise TqqqOrchestrationError("TQQQ promotion orchestration failed") from exc
    finally:
        os.umask(old_umask)


__all__ = [
    "EXACT_DURATIONS",
    "FIRST_ELIGIBLE_SESSION",
    "FIXED_CUTOFF",
    "FROZEN_XNYS_SESSIONS",
    "INPUT_LICENSE",
    "INPUT_USAGE_SCOPE",
    "OFFICIAL_IBAPI_PROVENANCE_SHA256",
    "QPK_REVISION",
    "TQQQ_PROMOTION_ASSETS",
    "UES_REVISION",
    "TqqqOrchestrationAuthority",
    "TqqqOrchestrationError",
    "orchestrate_existing_tqqq_snapshot_diagnostic",
    "orchestrate_tqqq_promotion",
    "resolve_tqqq_runtime_identity",
]
