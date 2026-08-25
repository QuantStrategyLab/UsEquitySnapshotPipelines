"""Materialize verified v4 split-adjusted closes into offline P3 contexts.

The v4 materializer has a distinct P1 member and P2 identity from the legacy
v3 OHLCV path.  It accepts only the canonical Twelve close series after its
Yahoo verifier receipt has been bound by P1.  It neither acquires data nor
invokes a strategy, writes storage, accesses credentials, or creates orders.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence

import pandas as pd

from .soxl_core_only_free_split_close_p1 import (
    SoxlCoreOnlyFreeSplitCloseP1Error,
    canonical_soxl_core_only_free_split_close_series_bytes,
    soxl_core_only_free_split_close_p1_binding_sha256,
    validate_soxl_core_only_free_split_close_assurance_member,
    validate_soxl_core_only_free_split_close_input_manifest,
    validate_soxl_core_only_free_split_close_p1_binding,
)
from .soxl_core_only_p1_binding import expected_soxl_core_only_sessions

MATERIALIZED_INPUT_SCHEMA = "qsl.soxl-soxx-core-only-p3-free-split-close-materialized-input.v1"
INDICATOR_SPEC_ID = "soxl-soxx-core-only-split-adjusted-close-indicators.v1"
_CLOSES_SCHEMA = "qsl.soxl-soxx-core-only-split-adjusted-close-series.v1"
_SYMBOLS = ("SOXL", "SOXX", "BOXX")
_MIN_RAW_SESSIONS = 252
_MAX_MATERIALIZED_SESSIONS = 1024
_TREND_WINDOW = 140
_MA20_WINDOW = 20
_RSI_WINDOW = 14
_VOL_WINDOW = 10
_VOL_LOOKBACK = 252
_VOL_MIN_PERIODS = 126
_VOL_PERCENTILE = 0.95
_VOL_FLOOR = 0.50
_VOL_CAP = 0.75


class SoxlCoreOnlyFreeSplitCloseP3MaterializerError(ValueError):
    """Fail-closed error that never includes source rows or close values."""


def _fail() -> None:
    raise SoxlCoreOnlyFreeSplitCloseP3MaterializerError("invalid SOXL free-source P3 materialization input")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3MaterializerError(
            "invalid SOXL free-source P3 materialization input"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _normalized_close_member(member_bytes: bytes, *, date_cutoff: object) -> dict[str, list[dict[str, object]]]:
    if not isinstance(member_bytes, bytes):
        _fail()
    try:
        payload = json.loads(member_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError):
        _fail()
    payload = _mapping(payload)
    if set(payload) != {"schema_version", "series"} or payload["schema_version"] != _CLOSES_SCHEMA:
        _fail()
    raw_series = _mapping(payload["series"])
    if set(raw_series) != set(_SYMBOLS):
        _fail()
    expected = expected_soxl_core_only_sessions(date_cutoff)
    result: dict[str, list[dict[str, object]]] = {}
    for symbol in _SYMBOLS:
        try:
            canonical = canonical_soxl_core_only_free_split_close_series_bytes(
                symbol=symbol,
                series=raw_series[symbol],
            )
            parsed = json.loads(canonical)
            rows = parsed["sessions"]
            dates = tuple(row["session_date"] for row in rows)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, SoxlCoreOnlyFreeSplitCloseP1Error):
            _fail()
        if dates != tuple(session.isoformat() for session in expected[symbol]):
            _fail()
        result[symbol] = rows
    if _canonical({"schema_version": _CLOSES_SCHEMA, "series": result}) != member_bytes:
        _fail()
    return result


def _validate_members(
    *,
    binding: Mapping[str, object],
    manifest: Mapping[str, object],
    closes_bytes: bytes,
    assurance_bytes: bytes,
    p2_contract: object | None = None,
) -> tuple[dict[str, object], dict[str, list[dict[str, object]]], str]:
    try:
        frozen = validate_soxl_core_only_free_split_close_p1_binding(binding, p2_contract=p2_contract)
        manifest_sha256 = validate_soxl_core_only_free_split_close_input_manifest(
            manifest,
            frozen,
            p2_contract=p2_contract,
        )
    except SoxlCoreOnlyFreeSplitCloseP1Error as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3MaterializerError("SOXL free-source P1 provenance mismatch") from exc
    identity = _mapping(frozen["data_identity"])
    series = _normalized_close_member(closes_bytes, date_cutoff=identity["date_cutoff"])
    close_digests = {
        symbol: hashlib.sha256(
            canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=series[symbol])
        ).hexdigest()
        for symbol in _SYMBOLS
    }
    try:
        assurance = json.loads(assurance_bytes)
        source_snapshots = validate_soxl_core_only_free_split_close_assurance_member(
            assurance,
            date_cutoff=str(identity["date_cutoff"]),
            canonical_close_sha256=close_digests,
            p2_contract=p2_contract,
        )
    except (TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError, SoxlCoreOnlyFreeSplitCloseP1Error):
        _fail()
    if assurance_bytes != _canonical(assurance):
        _fail()
    members = manifest.get("members")
    expected_members = {
        "assurance.json": assurance_bytes,
        "closes.json": closes_bytes,
    }
    if (
        not isinstance(members, list)
        or {member.get("path"): member for member in members if isinstance(member, Mapping)}
        != {
            path: {
                "path": path,
                "media_type": "application/json",
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for path, content in expected_members.items()
        }
    ):
        _fail()
    expected_sources = {
        f"{source_id}:{symbol}": source_snapshots[symbol][source_id]
        for symbol in _SYMBOLS
        for source_id in ("twelve_data_1day_split_adjusted", "yahoo_finance_chart_1day_split_adjusted")
    }
    sources = manifest.get("sources")
    if not isinstance(sources, list) or {
        source.get("source_id"): source.get("content_sha256") for source in sources if isinstance(source, Mapping)
    } != expected_sources:
        _fail()
    return frozen, series, manifest_sha256


def _close_series(series: Sequence[Mapping[str, object]]) -> pd.Series:
    try:
        return pd.Series(
            [float(row["close"]) for row in series],
            index=[str(row["session_date"]) for row in series],
            dtype=float,
        )
    except (KeyError, TypeError, ValueError):
        _fail()


def _indicator_history(series: Mapping[str, Sequence[Mapping[str, object]]]) -> pd.DataFrame:
    soxl = _close_series(series["SOXL"])
    soxx = _close_series(series["SOXX"])
    returns = soxx.pct_change(fill_method=None)
    vol10 = returns.rolling(_VOL_WINDOW).std() * math.sqrt(252.0)
    dynamic_threshold = (
        vol10.rolling(_VOL_LOOKBACK, min_periods=_VOL_MIN_PERIODS)
        .quantile(_VOL_PERCENTILE)
        .clip(lower=_VOL_FLOOR, upper=_VOL_CAP)
    )
    dynamic_samples = vol10.rolling(_VOL_LOOKBACK, min_periods=1).count()
    delta = soxx.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    average_gain = gains.ewm(alpha=1 / _RSI_WINDOW, min_periods=_RSI_WINDOW, adjust=False).mean()
    average_loss = losses.ewm(alpha=1 / _RSI_WINDOW, min_periods=_RSI_WINDOW, adjust=False).mean()
    relative_strength = average_gain / average_loss.mask(average_loss == 0.0)
    rsi14 = 100.0 - (100.0 / (1.0 + relative_strength))
    rsi14 = rsi14.mask(average_loss == 0.0, 100.0).mask(average_gain == 0.0, 0.0)
    ma20 = soxx.rolling(_MA20_WINDOW).mean()
    bb_upper = ma20 + (2.0 * soxx.rolling(_MA20_WINDOW).std(ddof=0))
    return pd.DataFrame(
        {
            "indicator_session_count": range(1, len(soxx.index) + 1),
            "soxl_price": soxl,
            "soxl_ma_trend": soxl.rolling(_TREND_WINDOW).mean(),
            "soxx_price": soxx,
            "soxx_ma_trend": soxx.rolling(_TREND_WINDOW).mean(),
            "soxx_ma20": ma20,
            "soxx_ma20_slope": ma20.diff(),
            "soxx_rsi14": rsi14,
            "soxx_bb_upper": bb_upper,
            "soxx_realized_volatility_10": vol10,
            "soxx_dynamic_threshold": dynamic_threshold,
            "soxx_dynamic_sample_count": dynamic_samples,
        },
        index=soxx.index,
    )


def materialize_soxl_core_only_free_split_close_p3_input(
    *,
    binding: Mapping[str, object],
    manifest: Mapping[str, object],
    closes_bytes: bytes,
    assurance_bytes: bytes,
    p2_contract: object | None = None,
) -> dict[str, object]:
    """Build bounded P3 contexts from a verified candidate input without execution."""
    frozen, raw_series, manifest_sha256 = _validate_members(
        binding=binding,
        manifest=manifest,
        closes_bytes=closes_bytes,
        assurance_bytes=assurance_bytes,
        p2_contract=p2_contract,
    )
    if len(raw_series["SOXL"]) < _MIN_RAW_SESSIONS or len(raw_series["SOXX"]) < _MIN_RAW_SESSIONS:
        _fail()
    history = _indicator_history(raw_series)
    boxx_by_session = {row["session_date"]: float(row["close"]) for row in raw_series["BOXX"]}
    sessions: list[dict[str, object]] = []
    for session_date, boxx_close in boxx_by_session.items():
        if session_date not in history.index:
            continue
        row = history.loc[session_date]
        if int(row["indicator_session_count"]) < _MIN_RAW_SESSIONS or row.isna().any():
            continue
        sessions.append(
            {
                "as_of": f"{session_date}T00:00:00+00:00",
                "market_data": {
                    "derived_indicators": {
                        "SOXL": {"price": float(row["soxl_price"]), "ma_trend": float(row["soxl_ma_trend"])},
                        "SOXX": {
                            "price": float(row["soxx_price"]),
                            "ma_trend": float(row["soxx_ma_trend"]),
                            "ma20": float(row["soxx_ma20"]),
                            "ma20_slope": float(row["soxx_ma20_slope"]),
                            "rsi14": float(row["soxx_rsi14"]),
                            "bb_upper": float(row["soxx_bb_upper"]),
                            "realized_volatility_10": float(row["soxx_realized_volatility_10"]),
                            "realized_volatility_10_dynamic_threshold": float(row["soxx_dynamic_threshold"]),
                            "realized_volatility_10_dynamic_sample_count": float(row["soxx_dynamic_sample_count"]),
                        },
                    }
                },
                "prices": {
                    "SOXL": float(row["soxl_price"]),
                    "SOXX": float(row["soxx_price"]),
                    "BOXX": boxx_close,
                },
            }
        )
    if len(sessions) < 2 or len(sessions) > _MAX_MATERIALIZED_SESSIONS:
        _fail()
    result: dict[str, object] = {
        "schema_version": MATERIALIZED_INPUT_SCHEMA,
        "p1_identity": {
            "input_manifest_sha256": manifest_sha256,
            "binding_sha256": soxl_core_only_free_split_close_p1_binding_sha256(
                frozen,
                p2_contract=p2_contract,
            ),
            "closes_member_sha256": hashlib.sha256(closes_bytes).hexdigest(),
            "assurance_member_sha256": hashlib.sha256(assurance_bytes).hexdigest(),
            "date_cutoff": frozen["data_identity"]["date_cutoff"],
        },
        "p2_identity": {
            "candidate_id": frozen["candidate"]["candidate_id"],
            "config_sha256": frozen["candidate"]["config_sha256"],
        },
        "indicator_spec": {
            "id": INDICATOR_SPEC_ID,
            "price_field": "split_adjusted_close",
            "trend_window_sessions": _TREND_WINDOW,
            "rsi_window_sessions": _RSI_WINDOW,
            "bollinger_window_sessions": _MA20_WINDOW,
            "bollinger_stddev_ddof": 0,
            "volatility_window_sessions": _VOL_WINDOW,
            "volatility_annualization_sessions": 252,
            "dynamic_threshold": {
                "lookback_sessions": _VOL_LOOKBACK,
                "minimum_sessions": _VOL_MIN_PERIODS,
                "percentile": _VOL_PERCENTILE,
                "floor": _VOL_FLOOR,
                "cap": _VOL_CAP,
            },
        },
        "sessions": sessions,
    }
    result["materialized_input_sha256"] = _sha256(result)
    return result


__all__ = [
    "INDICATOR_SPEC_ID",
    "MATERIALIZED_INPUT_SCHEMA",
    "SoxlCoreOnlyFreeSplitCloseP3MaterializerError",
    "materialize_soxl_core_only_free_split_close_p3_input",
]
