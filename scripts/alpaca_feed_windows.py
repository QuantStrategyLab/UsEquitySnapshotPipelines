"""Explicit free-plan request segments; no credentials or network access here."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import math
from typing import Mapping


def feed_windows(start: datetime, end: datetime, *, now: datetime,
                 env: Mapping[str, str]) -> list[dict[str, str]]:
    """Return disjoint inclusive windows, keeping each feed separately labelled."""
    if any(value.tzinfo is None for value in (start, end, now)):
        raise ValueError("ALPACA_WINDOW_TIMEZONE_REQUIRED")
    start, end, now = (value.astimezone(UTC) for value in (start, end, now))
    delay = float(env.get("ALPACA_SIP_MIN_DELAY_MINUTES", "16"))
    if not math.isfinite(delay) or delay < 15:
        raise ValueError("ALPACA_SIP_MIN_DELAY_MINUTES must be at least 15")
    historical = env.get("ALPACA_FEED_HISTORICAL", "sip").lower()
    recent = env.get("ALPACA_FEED_RECENT", "iex").lower()
    if historical not in {"sip", "iex"} or recent not in {"iex", "none"}:
        raise ValueError("ALPACA_FEED_CONFIGURATION_INVALID")
    if start > end or end > now:
        raise ValueError("ALPACA_WINDOW_INVALID_OR_FUTURE")
    boundary = now - timedelta(minutes=delay)
    segments = []

    def add(left: datetime, right: datetime, feed: str, kind: str) -> None:
        if left <= right:
            segments.append({"start": left.isoformat(), "end": right.isoformat(),
                             "feed": feed, "segment": kind})

    add(start, min(end, boundary), historical, "historical")
    if recent != "none":
        add(max(start, boundary + timedelta(microseconds=1)), end, recent, "recent_tail")
    return segments
