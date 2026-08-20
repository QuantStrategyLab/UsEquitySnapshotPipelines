"""Materialize verified SOXL P1 bars into source-independent P3 contexts.

This is a pure, offline P3 preparation step for the frozen
``soxl_soxx_core_only_p2_v2`` candidate.  It verifies a canonical private
``bars.json`` member against the exact P1 binding and manifest, recomputes
only the candidate's required daily indicators, and returns bounded contexts
for the isolated source runner.  It neither acquires data nor invokes that
runner, writes storage, schedules work, accesses credentials, or creates an
order.

The input member is deliberately a new canonical three-asset shape.  It is
not compatible with the retired nine-asset SOXL promotion package.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import pandas as pd

from .soxl_core_only_p1_binding import (
    SoxlCoreOnlyP1BindingError,
    soxl_core_only_p1_binding_sha256,
    validate_soxl_core_only_input_manifest,
    validate_soxl_core_only_p1_binding,
)
from .soxl_core_only_p2_v2_contract import P2_V2_CONTRACT


BARS_SCHEMA = "qsl.soxl-soxx-core-only-adjusted-ohlcv.v1"
SOURCE_SERIES_SCHEMA = "qsl.soxl-soxx-core-only-adjusted-ohlcv-source.v1"
MATERIALIZED_INPUT_SCHEMA = "qsl.soxl-soxx-core-only-p3-materialized-input.v1"
INDICATOR_SPEC_ID = "soxl-soxx-core-only-close-indicators.v1"
_SYMBOLS = ("SOXL", "SOXX", "BOXX")
_MAX_MATERIALIZED_SESSIONS = 1024
_MIN_RAW_SESSIONS = 252
_TREND_WINDOW = 140
_MA20_WINDOW = 20
_RSI_WINDOW = 14
_VOL_WINDOW = 10
_VOL_LOOKBACK = 252
_VOL_MIN_PERIODS = 126
_VOL_PERCENTILE = 0.95
_VOL_FLOOR = 0.50
_VOL_CAP = 0.75


class SoxlCoreOnlyP3MaterializerError(ValueError):
    """Fail-closed error that never includes raw bar data."""


def _fail() -> None:
    raise SoxlCoreOnlyP3MaterializerError("invalid SOXL core-only P3 materialization input")


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
        raise SoxlCoreOnlyP3MaterializerError("invalid SOXL core-only P3 materialization input") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail()
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0) or (nonnegative and result < 0.0):
        _fail()
    return 0.0 if result == 0.0 else result


def _session_date(value: object) -> str:
    if not isinstance(value, str):
        _fail()
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail()
    if parsed.isoformat() != value or parsed.weekday() >= 5:
        _fail()
    return value


def _bar(value: object) -> dict[str, float]:
    bar = _mapping(value)
    if set(bar) != {"open", "high", "low", "close", "volume"}:
        _fail()
    open_price = _finite(bar["open"], positive=True)
    high = _finite(bar["high"], positive=True)
    low = _finite(bar["low"], positive=True)
    close = _finite(bar["close"], positive=True)
    volume = _finite(bar["volume"], nonnegative=True)
    if low > min(open_price, close) or high < max(open_price, close) or high < low:
        _fail()
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _normalized_bars_member(member_bytes: bytes) -> list[dict[str, object]]:
    if not isinstance(member_bytes, bytes):
        _fail()
    try:
        payload = json.loads(member_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        _fail()
    canonical_payload = _mapping(payload)
    if set(canonical_payload) != {"schema_version", "sessions"}:
        _fail()
    if canonical_payload["schema_version"] != BARS_SCHEMA:
        _fail()
    sessions_value = canonical_payload["sessions"]
    if not isinstance(sessions_value, list) or len(sessions_value) < _MIN_RAW_SESSIONS:
        _fail()
    sessions: list[dict[str, object]] = []
    previous: str | None = None
    for raw_session in sessions_value:
        session = _mapping(raw_session)
        if set(session) != {"session_date", "bars"}:
            _fail()
        session_date = _session_date(session["session_date"])
        if previous is not None and session_date <= previous:
            _fail()
        previous = session_date
        raw_bars = _mapping(session["bars"])
        if set(raw_bars) != set(_SYMBOLS):
            _fail()
        sessions.append(
            {
                "session_date": session_date,
                "bars": {symbol: _bar(raw_bars[symbol]) for symbol in _SYMBOLS},
            }
        )
    normalized = {"schema_version": BARS_SCHEMA, "sessions": sessions}
    if _canonical(normalized) != member_bytes:
        _fail()
    return sessions


def canonical_soxl_core_only_source_series_bytes(
    *, symbol: str, sessions: Sequence[Mapping[str, object]]
) -> bytes:
    """Return the per-symbol canonical content whose SHA appears in P1."""
    if symbol not in _SYMBOLS:
        _fail()
    series: list[dict[str, object]] = []
    for raw_session in sessions:
        session = _mapping(raw_session)
        if set(session) != {"session_date", "bars"}:
            _fail()
        bars = _mapping(session["bars"])
        if set(bars) != set(_SYMBOLS):
            _fail()
        series.append(
            {
                "session_date": _session_date(session["session_date"]),
                "bar": _bar(bars[symbol]),
            }
        )
    return _canonical(
        {
            "schema_version": SOURCE_SERIES_SCHEMA,
            "symbol": symbol,
            "sessions": series,
        }
    )


def _validate_manifest_and_member(
    *, binding: Mapping[str, object], manifest: Mapping[str, object], member_bytes: bytes
) -> tuple[dict[str, object], list[dict[str, object]], str]:
    try:
        frozen_binding = validate_soxl_core_only_p1_binding(binding)
        manifest_sha256 = validate_soxl_core_only_input_manifest(manifest, frozen_binding)
    except SoxlCoreOnlyP1BindingError as exc:
        raise SoxlCoreOnlyP3MaterializerError("SOXL core-only P1 provenance mismatch") from exc
    sessions = _normalized_bars_member(member_bytes)
    members = manifest.get("members")
    if not isinstance(members, list) or len(members) != 1:
        _fail()
    member = _mapping(members[0])
    if member != {
        "path": "bars.json",
        "media_type": "application/json",
        "size_bytes": len(member_bytes),
        "sha256": hashlib.sha256(member_bytes).hexdigest(),
    }:
        _fail()
    declared_sources: dict[str, str] = {}
    sources = manifest.get("sources")
    if not isinstance(sources, list):
        _fail()
    for raw_source in sources:
        source = _mapping(raw_source)
        source_id = source.get("source_id")
        digest = source.get("content_sha256")
        if not isinstance(source_id, str) or not isinstance(digest, str):
            _fail()
        declared_sources[source_id] = digest
    for symbol in _SYMBOLS:
        source_id = f"alpaca_sip_1day_adjustment_all:{symbol}"
        expected_digest = hashlib.sha256(
            canonical_soxl_core_only_source_series_bytes(symbol=symbol, sessions=sessions)
        ).hexdigest()
        if declared_sources.get(source_id) != expected_digest:
            _fail()
    identity = frozen_binding["data_identity"]
    assert isinstance(identity, Mapping)
    if sessions[-1]["session_date"] != identity["date_cutoff"]:
        _fail()
    return frozen_binding, sessions, manifest_sha256


def _indicator_history(sessions: Sequence[Mapping[str, object]]) -> pd.DataFrame:
    index = [str(_mapping(session)["session_date"]) for session in sessions]
    close = pd.DataFrame(
        {
            symbol: [float(_mapping(_mapping(session)["bars"])[symbol]["close"]) for session in sessions]
            for symbol in _SYMBOLS
        },
        index=index,
        dtype=float,
    )
    soxl = close["SOXL"]
    soxx = close["SOXX"]
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
        index=index,
    )


def materialize_soxl_core_only_p3_input(
    *, binding: Mapping[str, object], manifest: Mapping[str, object], member_bytes: bytes
) -> dict[str, object]:
    """Build bounded source-runner sessions after P1 provenance verification.

    Output timestamps are UTC date markers for daily replay identity, not
    claimed provider timestamps or execution fills.  The next-session timing
    remains enforced only by the isolated replay runner.
    """
    frozen_binding, raw_sessions, manifest_sha256 = _validate_manifest_and_member(
        binding=binding,
        manifest=manifest,
        member_bytes=member_bytes,
    )
    history = _indicator_history(raw_sessions)
    sessions: list[dict[str, object]] = []
    for raw_session in raw_sessions:
        session = _mapping(raw_session)
        session_date = str(session["session_date"])
        row = history.loc[session_date]
        if row.isna().any():
            continue
        bars = _mapping(session["bars"])
        sessions.append(
            {
                "as_of": f"{session_date}T00:00:00+00:00",
                "market_data": {
                    "derived_indicators": {
                        "SOXL": {
                            "price": float(row["soxl_price"]),
                            "ma_trend": float(row["soxl_ma_trend"]),
                        },
                        "SOXX": {
                            "price": float(row["soxx_price"]),
                            "ma_trend": float(row["soxx_ma_trend"]),
                            "ma20": float(row["soxx_ma20"]),
                            "ma20_slope": float(row["soxx_ma20_slope"]),
                            "rsi14": float(row["soxx_rsi14"]),
                            "bb_upper": float(row["soxx_bb_upper"]),
                            "realized_volatility_10": float(row["soxx_realized_volatility_10"]),
                            "realized_volatility_10_dynamic_threshold": float(
                                row["soxx_dynamic_threshold"]
                            ),
                            "realized_volatility_10_dynamic_sample_count": float(
                                row["soxx_dynamic_sample_count"]
                            ),
                        },
                    }
                },
                "prices": {symbol: float(bars[symbol]["close"]) for symbol in _SYMBOLS},
            }
        )
    if len(sessions) < 2 or len(sessions) > _MAX_MATERIALIZED_SESSIONS:
        _fail()
    result: dict[str, object] = {
        "schema_version": MATERIALIZED_INPUT_SCHEMA,
        "p1_identity": {
            "input_manifest_sha256": manifest_sha256,
            "binding_sha256": soxl_core_only_p1_binding_sha256(frozen_binding),
            "bars_member_sha256": hashlib.sha256(member_bytes).hexdigest(),
            "date_cutoff": frozen_binding["data_identity"]["date_cutoff"],
        },
        "p2_identity": {
            "candidate_id": P2_V2_CONTRACT.candidate_id,
            "config_sha256": P2_V2_CONTRACT.config_sha256,
        },
        "indicator_spec": {
            "id": INDICATOR_SPEC_ID,
            "price_field": "adjusted_close",
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
    "BARS_SCHEMA",
    "INDICATOR_SPEC_ID",
    "MATERIALIZED_INPUT_SCHEMA",
    "SOURCE_SERIES_SCHEMA",
    "SoxlCoreOnlyP3MaterializerError",
    "canonical_soxl_core_only_source_series_bytes",
    "materialize_soxl_core_only_p3_input",
]
