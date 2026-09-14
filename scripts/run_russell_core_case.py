"""One-shot non-live Russell core-signal research case.

The provider response and bars remain in memory.  This script prints only
aggregate research metrics and is intentionally not a workflow trigger.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, date, datetime
from io import BytesIO
from urllib.parse import urlencode

import exchange_calendars as xcals
import numpy as np
import pandas as pd
import requests

from us_equity_snapshot_pipelines.lifecycle.russell_research_runner import (
    PROFILE,
    RESEARCH_FLAGS,
    RussellResearchRunner,
)

BUCKET = "qsl-runtime-logs-shared"
OBJECT = "strategy-artifacts/us_equity/russell_top50_leader_rotation_staging/candidates/33473013774-1/russell_top50_leader_rotation_feature_snapshot_latest.csv"
GENERATION = "1788239901963544"
FEATURE_SHA256 = "493f5ff986e1d421a36e20efefc5ecef0142b01c2cca8a139942e73ee7480967"
FEATURE_AVAILABLE_AT = "2026-09-01T05:18:23Z"
SYMBOLS = ("AMD", "INTC", "MU", "PANW")
START = "2026-09-01T00:00:00-04:00"
END = "2026-09-12T00:00:00-04:00"
VARIANT = "blend_top2_50_top4_50"


def load_feature_snapshot(storage_client, *, bucket=BUCKET, object_name=OBJECT, generation=GENERATION, expected_sha256=FEATURE_SHA256):
    blob = storage_client.bucket(bucket).blob(object_name, generation=generation)
    raw = blob.download_as_bytes(retry=None)
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("feature snapshot hash mismatch")
    frame = pd.read_csv(BytesIO(raw))
    if frame.empty:
        raise ValueError("feature snapshot is empty")
    if "available_at" not in frame.columns:
        frame["available_at"] = FEATURE_AVAILABLE_AT
    frame["available_at"] = pd.to_datetime(frame["available_at"], utc=True)
    return frame


def _expected_sessions():
    calendar = xcals.get_calendar("XNYS")
    return tuple(pd.Timestamp(x).date() for x in calendar.sessions_in_range("2026-09-01", "2026-09-12"))


def validate_bars_payload(payload, *, symbols=SYMBOLS):
    if not isinstance(payload, dict) or not isinstance(payload.get("bars"), dict):
        raise TypeError("invalid bars response")
    bars = payload["bars"]
    if payload.get("next_page_token"):
        raise ValueError("pagination is not allowed")
    if set(bars) != set(symbols):
        raise ValueError("bars symbols do not exactly match requested symbols")
    rows = []
    for symbol in symbols:
        if not isinstance(bars[symbol], list):
            raise TypeError("invalid bars rows")
        for item in bars[symbol]:
            try:
                timestamp = pd.Timestamp(item["t"])
                if timestamp.tzinfo is None:
                    raise ValueError
                local_timestamp = timestamp.tz_convert("America/New_York")
                if local_timestamp.time().isoformat() != "00:00:00":
                    raise ValueError
                values = [float(item["o"]), float(item["c"])]
            except (KeyError, TypeError, ValueError):
                raise ValueError("invalid bar") from None
            if not all(np.isfinite(values)) or any(value <= 0 for value in values):
                raise ValueError("invalid bar price")
            rows.append({"session": local_timestamp.date().isoformat(), "symbol": symbol, "open": values[0], "close": values[1]})
    frame = pd.DataFrame(rows)
    expected = _expected_sessions()
    if len(frame) != len(expected) * len(symbols):
        raise ValueError("bars are not a complete fixed window")
    if frame.duplicated(["session", "symbol"]).any():
        raise ValueError("duplicate bars")
    if set(pd.to_datetime(frame["session"]).dt.date) != set(expected):
        raise ValueError("bars sessions do not match fixed XNYS window")
    if len(frame) != len(set(frame["session"])) * len(symbols):
        raise ValueError("bars are incomplete")
    return frame


def fetch_bars(http_get, key_id: str, secret: str, *, url="https://data.alpaca.markets/v2/stocks/bars"):
    query = urlencode({"symbols": ",".join(SYMBOLS), "timeframe": "1Day", "start": START, "end": END, "feed": "sip", "adjustment": "all", "sort": "asc", "limit": "1000"})
    response = http_get(
        f"{url}?{query}",
        headers={"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret},
        timeout=30,
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Alpaca request rejected (HTTP {response.status_code})")
    return validate_bars_payload(response.json())


def run_case(features: pd.DataFrame, prices: pd.DataFrame, *, data_kind: str):
    runner = RussellResearchRunner(feature_snapshots=features, prices=prices, data_kind=data_kind, initial_equity=100_000.0, cost_bps=25.0)
    result = runner.run(PROFILE, {"variant": VARIANT})
    trades = runner.last_artifacts["trades"]
    first_session = trades["session"].min() if not trades.empty else None
    first_symbols = set(trades.loc[trades["session"] == first_session, "symbol"]) if first_session else set()
    if first_symbols != set(SYMBOLS):
        raise ValueError("core preflight did not build all four requested target positions")
    return {
        "strategy_profile": PROFILE,
        "variant": VARIANT,
        "data_kind": data_kind,
        "source": {"feature_object": OBJECT, "feature_generation": GENERATION, "feature_sha256": FEATURE_SHA256, "price_source": "Alpaca SIP bars", "window_start": START, "window_end": END},
        "sample_count": result.observation_count,
        "total_return": result.total_return,
        "max_drawdown": result.max_drawdown,
        "fees": float(trades["cost"].sum()) if not trades.empty else 0.0,
        "research_flags": {key: result.params[key] for key in RESEARCH_FLAGS},
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="perform the explicitly requested non-live provider read")
    args = parser.parse_args(argv)
    if not args.run:
        raise SystemExit("pass --run to execute this manual non-live case")
    if datetime.now(UTC).date() <= date.fromisoformat("2026-09-12"):
        raise SystemExit("fixed market-data window has not ended")
    key_id, secret = os.environ.get("ALPACA_API_KEY_ID"), os.environ.get("ALPACA_API_SECRET_KEY")
    if not key_id or not secret:
        raise SystemExit("Alpaca credentials are not configured")
    from google.cloud import storage
    try:
        features = load_feature_snapshot(storage.Client())
        stub_prices = pd.DataFrame(
            [
                {"session": session.isoformat(), "symbol": symbol, "open": 100.0, "close": 100.0}
                for session in _expected_sessions()
                for symbol in SYMBOLS
            ]
        )
        run_case(features, stub_prices, data_kind="synthetic")
        prices = fetch_bars(requests.get, key_id, secret)
        summary = run_case(features, prices, data_kind="research")
    except Exception:  # noqa: BLE001 - the CLI must never expose provider details
        raise SystemExit("research case failed: bounded_validation_or_provider_error") from None
    print(json.dumps(summary, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
