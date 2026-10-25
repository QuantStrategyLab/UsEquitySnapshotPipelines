"""Read an exact research archive and describe one predeclared benchmark window.

Pure preparation seam: the caller supplies its approved exact-object reader.
Raw pages and daily returns stay in that caller's protected environment. This
does not create an account policy, a net account path or a strategy backtest.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta
from importlib.metadata import version

import exchange_calendars as xcals
import pandas as pd

from quant_platform_kit.strategy_lifecycle.performance_metrics import compute_window_metrics
from scripts.research_input_pilot import Stop, bar_dates, gcs_uri, sha


def require(condition, reason):
    if not condition:
        raise Stop(reason)


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(result.tzinfo is not None and result.utcoffset() is not None, "REFERENCE_TIME_INVALID")
        return result
    except (ValueError, TypeError, AttributeError):
        raise Stop("REFERENCE_TIME_INVALID") from None


def prepare_reference(inventory_ref, read_object, *, symbol, start, end, frozen_at, observed_at):
    """Return a private daily payload and a separate allowlisted diagnostic.

    The ratio of provider-adjusted closes is explicitly a security proxy,
    never silently upgraded to an account's fee-net total-return series.
    """
    def read(ref):
        require(isinstance(ref, dict) and all(k in ref for k in ("uri", "generation", "bytes", "sha256")),
                "REFERENCE_OBJECT_IDENTITY_REQUIRED")
        gcs_uri(ref["uri"])
        require(isinstance(ref["generation"], str) and ref["generation"].isdigit()
                and int(ref["generation"]) > 0 and type(ref["bytes"]) is int
                and 0 < ref["bytes"] <= 12 * 1024**2 and isinstance(ref["sha256"], str)
                and re.fullmatch(r"[a-f0-9]{64}", ref["sha256"]), "REFERENCE_OBJECT_IDENTITY_INVALID")
        body = read_object(ref)
        require(isinstance(body, bytes) and len(body) == ref["bytes"] and sha(body) == ref["sha256"],
                "REFERENCE_OBJECT_BYTES_MISMATCH")
        try:
            return json.loads(body)
        except (ValueError, UnicodeError):
            raise Stop("REFERENCE_OBJECT_JSON_INVALID") from None

    require(isinstance(symbol, str) and re.fullmatch(r"[A-Z][A-Z0-9.]{0,9}", symbol),
            "REFERENCE_SYMBOL_REQUIRED")
    try:
        first, last = pd.Timestamp(start), pd.Timestamp(end)
        require(all(isinstance(x, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", x) for x in (start, end))
                and first <= last, "REFERENCE_WINDOW_INVALID")
    except (ValueError, TypeError):
        raise Stop("REFERENCE_WINDOW_INVALID") from None
    manifest = read(inventory_ref)
    require(isinstance(manifest, dict) and manifest.get("schema_version") == "research_input_inventory.v1"
            and manifest.get("research_only") is True, "REFERENCE_INVENTORY_REQUIRED")
    # The producer stamps created_at at run START, before acquiring pages.
    # Availability comes from the exact final object's native metadata instead.
    available = timestamp(inventory_ref.get("available_at_utc"))
    require(timestamp(manifest.get("created_at")) <= available <= timestamp(frozen_at) <= timestamp(observed_at),
            "REFERENCE_NOT_AVAILABLE_AT_FREEZE")
    entries = [row for row in manifest.get("alpaca", []) if isinstance(row, dict)
               and row.get("symbol") == symbol and row.get("vendor_adjustment") == "all"]
    require(len(entries) == 1, "REFERENCE_ADJUSTED_SOURCE_NOT_UNIQUE")
    entry = entries[0]
    require(entry.get("source_type") == "real" and entry.get("provider") == "alpaca"
            and entry.get("feed") == "sip" and entry.get("currency") == "USD"
            and entry.get("price_basis") == "split_dividend_adjusted"
            and entry.get("market_timezone") == "America/New_York"
            and entry.get("calendar_id") == "XNYS", "REFERENCE_SOURCE_BASIS_INVALID")
    require(entry.get("calendar_version") == version("exchange-calendars"), "REFERENCE_CALENDAR_VERSION_MISMATCH")
    declared = entry.get("coverage", {})
    try:
        calendar_end = (datetime.fromisoformat(declared["requested_end"]) + timedelta(days=7)).date().isoformat()
        calendar = xcals.get_calendar("XNYS", start=declared["requested_start"], end=calendar_end)
        sessions = calendar.sessions_in_range(start, end)
        require(len(sessions) >= 2, "REFERENCE_SESSION_WINDOW_REQUIRED")
        baseline = calendar.previous_session(sessions[0])
    except (ValueError, TypeError, KeyError):
        raise Stop("REFERENCE_CALENDAR_COVERAGE_INVALID") from None
    require(last.date() < available.date(), "REFERENCE_UNFINISHED_DAILY_WINDOW")
    pages = entry.get("pages")
    require(isinstance(pages, list) and 1 <= len(pages) <= 10, "REFERENCE_PAGES_REQUIRED")
    bucket, path = gcs_uri(inventory_ref["uri"])
    prefix = path.rsplit("/", 1)[0] + f"/alpaca/{symbol}/all/"
    dates, closes, tokens = [], [], set()
    for n, ref in enumerate(pages, 1):
        require(isinstance(ref, dict), "REFERENCE_PAGE_IDENTITY_REQUIRED")
        require(gcs_uri(ref.get("uri", "")) == (bucket, prefix + f"page-{n:03}.json"),
                "REFERENCE_PAGE_SCOPE_MISMATCH")
        require(timestamp(ref.get("fetched_at")) <= available, "REFERENCE_PAGE_TIME_INVALID")
        data = read(ref)
        require(isinstance(data, dict) and data.get("symbol") == symbol, "REFERENCE_PAGE_SYMBOL_MISMATCH")
        dates.extend(bar_dates(data, declared["requested_start"], declared["requested_end"]))
        closes.extend(float(row["c"]) for row in data["bars"])
        token = data.get("next_page_token")
        if n < len(pages):
            require(isinstance(token, str) and bool(token) and token not in tokens, "REFERENCE_PAGINATION_INVALID")
            tokens.add(token)
        else:
            require(token is None, "REFERENCE_PAGINATION_INCOMPLETE")
    require(dates == sorted(set(dates)) and all(math.isfinite(c) and c > 0 for c in closes),
            "REFERENCE_DAILY_PATH_INVALID")
    wanted = [baseline.date().isoformat(), *[s.date().isoformat() for s in sessions]]
    selected = [(d, c) for d, c in zip(dates, closes) if wanted[0] <= d <= wanted[-1]]
    require([d for d, _ in selected] == wanted, "REFERENCE_SESSION_GAP_OR_EXTRA")
    prices = pd.Series([c for _, c in selected], index=pd.to_datetime(wanted))
    returns = prices.pct_change(fill_method=None).iloc[1:]
    require(all(math.isfinite(r) and r > -1 for r in returns), "REFERENCE_RETURN_INVALID")
    try:
        loss = -compute_window_metrics(returns).max_drawdown
    except (ArithmeticError, ValueError):
        raise Stop("REFERENCE_METRIC_UNCOMPUTABLE") from None
    require(math.isfinite(loss) and 0 <= loss < 1, "REFERENCE_METRIC_UNCOMPUTABLE")
    identity = {"inventory": inventory_ref, "pages": pages, "symbol": symbol, "start": start, "end": end,
                "frozen_at": frozen_at, "calendar_id": "XNYS", "calendar_version": entry["calendar_version"]}
    input_hash = sha(json.dumps(identity, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
    summary = {"schema_version": "benchmark_reference_diagnostic.v1", "status": "REFERENCE_PROXY_COMPUTED",
               "symbol": symbol, "requested_start": start, "requested_end": end,
               "baseline_session": wanted[0], "first_session": wanted[1], "last_session": wanted[-1],
               "observations": len(returns), "source_objects_verified": 1 + len(pages),
               "source_inventory_sha256": inventory_ref["sha256"], "input_sha256": input_hash,
               "max_drawdown_loss_fraction": loss, "source_available_at": inventory_ref["available_at_utc"],
               "source_time_basis": "CALLER_SUPPLIED_EXACT_OBJECT_CREATION_METADATA",
               "frozen_at": frozen_at, "observed_at": observed_at, "return_basis": "PROVIDER_ADJUSTED_CLOSE_RATIO",
               "account_costs_included": False, "net_account_path_verified": False,
               "historical_pit_established": False, "risk_policy_qualified": False,
               "policy_selected": False, "settings_applied": False, "execution_authorized": False}
    private = {"summary": summary, "source": identity,
               "dates": [d.date().isoformat() for d in returns.index], "returns": returns.tolist()}
    return private, summary
