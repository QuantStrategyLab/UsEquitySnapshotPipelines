from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .yfinance_prices import download_price_history

EVENT_KIND_SHOCK = "shock"
EVENT_KIND_SOFTENING = "softening"
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = None
DEFAULT_TROUGH_WINDOW_DAYS = 10
DEFAULT_HORIZONS = (5, 10, 21, 42, 63)
DEFAULT_SYMBOLS = (
    "SPY",
    "QQQ",
    "IWM",
    "XLK",
    "SMH",
    "SOXX",
    "SSO",
    "QLD",
    "TQQQ",
    "ROM",
    "USD",
    "TECL",
    "SOXL",
    "FXI",
    "MCHI",
    "KWEB",
    "CQQQ",
    "YINN",
    "AAPL",
    "NVDA",
    "AMD",
    "TSLA",
    "BA",
    "CAT",
)


@dataclass(frozen=True)
class TradeWarEvent:
    event_id: str
    event_date: str
    kind: str
    region: str
    title: str
    source: str
    source_url: str


FIRST_TERM_TRADE_WAR_EVENTS: tuple[TradeWarEvent, ...] = (
    TradeWarEvent(
        event_id="2018-03-22-section-301-china-actions",
        event_date="2018-03-22",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump announces Section 301 China actions and directs USTR to propose tariffs",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2018/march/"
            "president-trump-announces-strong"
        ),
    ),
    TradeWarEvent(
        event_id="2018-04-05-additional-section-301-measures",
        event_date="2018-04-05",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump directs USTR to consider additional Section 301 measures after China retaliation threat",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2018/april/"
            "ustr-robert-lighthizer-statement"
        ),
    ),
    TradeWarEvent(
        event_id="2018-07-10-200b-section-301-process",
        event_date="2018-07-10",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="USTR begins process for tariffs on an additional $200 billion of Chinese imports",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2018/july/"
            "statement-us-trade-representative"
        ),
    ),
    TradeWarEvent(
        event_id="2019-02-24-delay-march-tariff-increase",
        event_date="2019-02-24",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="Trump delays planned China tariff increase after citing progress in negotiations",
        source="Time/AP",
        source_url="https://time.com/5536807/donald-trump-delays-tariffs-china/",
    ),
    TradeWarEvent(
        event_id="2019-05-05-threaten-25pct-200b",
        event_date="2019-05-05",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump says tariffs on $200 billion of Chinese goods will rise to 25%",
        source="Axios/Reuters",
        source_url="https://www.axios.com/2019/05/06/trumps-tariffs-threat-to-china-dow-futures-fall",
    ),
    TradeWarEvent(
        event_id="2019-05-30-mexico-tariff-threat",
        event_date="2019-05-30",
        kind=EVENT_KIND_SHOCK,
        region="mexico",
        title="Trump threatens 5% tariff on all Mexican imports starting June 10",
        source="CNBC",
        source_url=(
            "https://www.cnbc.com/2019/05/31/"
            "trump-says-us-will-impose-5percent-tariff-on-all-mexican-imports-from-june-10.html"
        ),
    ),
    TradeWarEvent(
        event_id="2019-06-07-mexico-tariff-suspended",
        event_date="2019-06-07",
        kind=EVENT_KIND_SOFTENING,
        region="mexico",
        title="Trump says the U.S. and Mexico reached a deal to avoid tariffs",
        source="CNBC",
        source_url=(
            "https://www.cnbc.com/2019/06/07/"
            "trump-says-the-us-and-mexico-have-reached-a-deal-to-avoid-tariffs.html"
        ),
    ),
    TradeWarEvent(
        event_id="2019-08-01-300b-10pct-china-tariff",
        event_date="2019-08-01",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump announces 10% tariff on roughly $300 billion of Chinese imports",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2019/august/"
            "ustr-announces-next-steps-proposed"
        ),
    ),
    TradeWarEvent(
        event_id="2019-08-13-delay-list-4b-tariffs",
        event_date="2019-08-13",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="USTR delays tariffs on certain China list 4 products to December 15",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2019/august/"
            "ustr-announces-next-steps-proposed"
        ),
    ),
    TradeWarEvent(
        event_id="2019-08-23-550b-tariff-escalation",
        event_date="2019-08-23",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump instructs USTR to increase tariffs on about $550 billion of Chinese imports",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2019/august/"
            "ustr-statement-section-301-tariff"
        ),
    ),
    TradeWarEvent(
        event_id="2019-12-13-phase-one-agreement",
        event_date="2019-12-13",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="United States and China reach Phase One trade agreement",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2019/december/"
            "united-states-and-china-reach"
        ),
    ),
)



BIDEN_TERM_TRADE_WAR_EVENTS: tuple[TradeWarEvent, ...] = (
    TradeWarEvent(
        event_id="2024-05-14-biden-section-301-strategic-sector-tariffs",
        event_date="2024-05-14",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Biden directs USTR to increase Section 301 tariffs on strategic China sectors",
        source="USTR/Commerce",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2024/may/"
            "us-trade-representative-katherine-tai-take-further-action-china-tariffs-after-releasing-statutory"
        ),
    ),
    TradeWarEvent(
        event_id="2024-12-11-biden-section-301-wafers-polysilicon-tungsten",
        event_date="2024-12-11",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="USTR increases Section 301 tariffs on wafers, polysilicon, and tungsten products",
        source="USTR",
        source_url=(
            "https://ustr.gov/about-us/policy-offices/press-office/press-releases/2024/december/"
            "ustr-increases-tariffs-under-section-301-tungsten-products-wafers-and-polysilicon-concluding"
        ),
    ),
)

SECOND_TERM_TRADE_WAR_EVENTS: tuple[TradeWarEvent, ...] = (
    TradeWarEvent(
        event_id="2025-02-01-canada-mexico-china-tariffs",
        event_date="2025-02-01",
        kind=EVENT_KIND_SHOCK,
        region="global",
        title="Trump imposes tariffs on imports from Canada, Mexico, and China",
        source="White House",
        source_url=(
            "https://www.whitehouse.gov/fact-sheets/2025/02/"
            "fact-sheet-president-donald-j-trump-imposes-tariffs-on-imports-from-canada-mexico-and-china/"
        ),
    ),
    TradeWarEvent(
        event_id="2025-02-03-canada-mexico-tariff-pause",
        event_date="2025-02-03",
        kind=EVENT_KIND_SOFTENING,
        region="north_america",
        title="Trump agrees to pause tariffs on Canada and Mexico for 30 days",
        source="AP",
        source_url="https://apnews.com/article/017efa8c3343b8d2a9444f7e65356ae9",
    ),
    TradeWarEvent(
        event_id="2025-03-06-usmca-canada-mexico-pause",
        event_date="2025-03-06",
        kind=EVENT_KIND_SOFTENING,
        region="north_america",
        title="Trump pauses tariffs on USMCA-compliant Canada and Mexico imports until April 2",
        source="CNBC",
        source_url=(
            "https://www.cnbc.com/amp/2025/03/06/"
            "trump-tariffs-live-updates.html"
        ),
    ),
    TradeWarEvent(
        event_id="2025-04-02-liberation-day-reciprocal-tariffs",
        event_date="2025-04-02",
        kind=EVENT_KIND_SHOCK,
        region="global",
        title="Trump announces Liberation Day reciprocal tariffs",
        source="White House/Congress CRS",
        source_url="https://www.congress.gov/crs-product/R48549",
    ),
    TradeWarEvent(
        event_id="2025-04-09-reciprocal-tariff-90-day-pause",
        event_date="2025-04-09",
        kind=EVENT_KIND_SOFTENING,
        region="global_except_china",
        title="Trump announces 90-day pause on most reciprocal tariffs while raising China tariffs",
        source="White House/CNBC",
        source_url="https://www.cnbc.com/amp/2025/04/09/trump-tariffs-live-updates.html",
    ),
    TradeWarEvent(
        event_id="2025-05-12-us-china-90-day-tariff-reduction",
        event_date="2025-05-12",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="United States and China agree to a 90-day tariff reduction after Geneva talks",
        source="White House/AP",
        source_url=(
            "https://www.whitehouse.gov/articles/2025/05/"
            "joint-statement-on-u-s-china-economic-and-trade-meeting-in-geneva/"
        ),
    ),
    TradeWarEvent(
        event_id="2025-08-11-china-tariff-pause-extension",
        event_date="2025-08-11",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="Trump extends the China tariff truce for another 90 days",
        source="White House/AP",
        source_url=(
            "https://www.whitehouse.gov/fact-sheets/2025/08/"
            "fact-sheet-president-donald-j-trump-continues-the-suspension-of-the-heightened-tariffs-on-china/"
        ),
    ),
    TradeWarEvent(
        event_id="2025-10-10-china-100pct-tariff-threat",
        event_date="2025-10-10",
        kind=EVENT_KIND_SHOCK,
        region="china",
        title="Trump threatens an additional 100% tariff on China after rare-earth export controls",
        source="AP",
        source_url="https://apnews.com/article/f2c8bcc1f46043ab504cf4b0281e3401",
    ),
    TradeWarEvent(
        event_id="2025-10-26-us-china-framework-avoids-100pct-tariff",
        event_date="2025-10-26",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="Bessent says a U.S.-China framework will avoid the threatened 100% tariff boost",
        source="Axios",
        source_url="https://www.axios.com/2025/10/26/us-china-tariffs",
    ),
    TradeWarEvent(
        event_id="2025-11-01-us-china-economic-trade-deal",
        event_date="2025-11-01",
        kind=EVENT_KIND_SOFTENING,
        region="china",
        title="White House outlines U.S.-China economic and trade deal after Trump-Xi meeting",
        source="White House",
        source_url=(
            "https://www.whitehouse.gov/fact-sheets/2025/11/"
            "fact-sheet-president-donald-j-trump-strikes-deal-on-economic-and-trade-relations-with-china/"
        ),
    ),
)

GEOPOLITICAL_CONFLICT_EVENTS_2026: tuple[TradeWarEvent, ...] = (
    TradeWarEvent(
        event_id="2026-02-28-us-israel-strikes-iran-war-start",
        event_date="2026-02-28",
        kind=EVENT_KIND_SHOCK,
        region="iran_middle_east",
        title="U.S. and Israel strike Iran, starting the 2026 Iran war escalation",
        source="AP",
        source_url="https://apnews.com/article/e85410b6f404ddd45a9da0a09f1c285f",
    ),
    TradeWarEvent(
        event_id="2026-03-26-iran-war-ceasefire-doubts-hormuz-deadline",
        event_date="2026-03-26",
        kind=EVENT_KIND_SHOCK,
        region="iran_middle_east",
        title="Stocks fall on Iran ceasefire doubts and Strait of Hormuz deadline risk",
        source="Spokesman/Bloomberg",
        source_url=(
            "https://www.spokesman.com/stories/2026/mar/26/"
            "wall-street-slides-as-mideast-de-escalation-uncert/"
        ),
    ),
)

GEOPOLITICAL_DEESCALATION_EVENTS_2026: tuple[TradeWarEvent, ...] = (
    TradeWarEvent(
        event_id="2026-03-31-iran-war-peace-hopes-rally",
        event_date="2026-03-31",
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="U.S. stocks surge as hope returns for a possible end to the Iran war",
        source="AP",
        source_url="https://apnews.com/article/wall-street-stocks-dow-nasdaq-57d35b474bf9d44af81724b3b0ee8936",
    ),
    TradeWarEvent(
        event_id="2026-04-08-us-iran-two-week-ceasefire",
        event_date="2026-04-08",
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="U.S. and Iran agree to a two-week ceasefire and Strait of Hormuz reopening",
        source="AP",
        source_url="https://apnews.com/article/financial-markets-iran-oil-bcd3342cd0b4e60ebedc1e81db08f465",
    ),
    TradeWarEvent(
        event_id="2026-04-10-planned-us-iran-talks",
        event_date="2026-04-10",
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="Planned U.S.-Iran talks follow the shaky ceasefire agreement",
        source="AP",
        source_url="https://apnews.com/article/stock-markets-trump-iran-ceasefire-oil-7ef6ebab1aaa731d2da6406b3cbde6dd",
    ),
    TradeWarEvent(
        event_id="2026-04-15-in-principle-ceasefire-extension",
        event_date="2026-04-15",
        kind=EVENT_KIND_SOFTENING,
        region="iran_middle_east",
        title="Officials cite an in-principle agreement to extend the ceasefire for diplomacy",
        source="AP",
        source_url="https://apnews.com/article/stock-markets-trump-oil-iran-war-7659569791b1f5e108489360d18e50f1",
    ),
)

GEOPOLITICAL_CONFLICT_AND_DEESCALATION_EVENTS_2026: tuple[TradeWarEvent, ...] = (
    *GEOPOLITICAL_CONFLICT_EVENTS_2026,
    *GEOPOLITICAL_DEESCALATION_EVENTS_2026,
)

TRADE_WAR_EVENTS_2018_TO_PRESENT: tuple[TradeWarEvent, ...] = (
    *FIRST_TERM_TRADE_WAR_EVENTS,
    *BIDEN_TERM_TRADE_WAR_EVENTS,
    *SECOND_TERM_TRADE_WAR_EVENTS,
)

TACO_EVENTS_WITH_GEOPOLITICAL_DEESCALATION: tuple[TradeWarEvent, ...] = (
    *TRADE_WAR_EVENTS_2018_TO_PRESENT,
    *GEOPOLITICAL_DEESCALATION_EVENTS_2026,
)

TACO_EVENTS_WITH_GEOPOLITICAL_CONFLICT_AND_DEESCALATION: tuple[TradeWarEvent, ...] = (
    *TRADE_WAR_EVENTS_2018_TO_PRESENT,
    *GEOPOLITICAL_CONFLICT_AND_DEESCALATION_EVENTS_2026,
)

DEFAULT_EVENT_SET = "full"
TRADE_WAR_EVENT_SETS: dict[str, tuple[TradeWarEvent, ...]] = {
    "first-term": FIRST_TERM_TRADE_WAR_EVENTS,
    "biden": BIDEN_TERM_TRADE_WAR_EVENTS,
    "second-term": SECOND_TERM_TRADE_WAR_EVENTS,
    "geopolitical-conflict": GEOPOLITICAL_CONFLICT_EVENTS_2026,
    "geopolitical-deescalation": GEOPOLITICAL_DEESCALATION_EVENTS_2026,
    "geopolitical-conflict-and-deescalation": GEOPOLITICAL_CONFLICT_AND_DEESCALATION_EVENTS_2026,
    "full-plus-geopolitical-deescalation": TACO_EVENTS_WITH_GEOPOLITICAL_DEESCALATION,
    "full-plus-geopolitical-conflict-and-deescalation": TACO_EVENTS_WITH_GEOPOLITICAL_CONFLICT_AND_DEESCALATION,
    "full": TRADE_WAR_EVENTS_2018_TO_PRESENT,
}


def resolve_trade_war_event_set(name: str | None = DEFAULT_EVENT_SET) -> tuple[TradeWarEvent, ...]:
    event_set = str(name or DEFAULT_EVENT_SET).strip().lower()
    if event_set not in TRADE_WAR_EVENT_SETS:
        raise ValueError(f"Unsupported event set: {name!r}")
    return TRADE_WAR_EVENT_SETS[event_set]


def split_symbols(raw: str | Iterable[str] | None) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_SYMBOLS
    if isinstance(raw, str):
        values = raw.split(",")
    else:
        values = list(raw)
    symbols = []
    for value in values:
        symbol = str(value or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


def events_to_frame(events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT) -> pd.DataFrame:
    return pd.DataFrame([asdict(event) for event in events])


def price_history_to_close_matrix(price_history) -> pd.DataFrame:
    frame = read_table(price_history) if isinstance(price_history, (str, Path)) else pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price history missing required columns: {', '.join(sorted(missing))}")
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise RuntimeError("No usable price history rows")
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.astype(str).str.upper()
    return close


def _event_signal_date(index: pd.DatetimeIndex, event_date: str | pd.Timestamp) -> pd.Timestamp | None:
    date = pd.Timestamp(event_date).tz_localize(None).normalize()
    candidates = index[index >= date]
    if candidates.empty:
        return None
    return pd.Timestamp(candidates[0]).normalize()


def _value_at_offset(series: pd.Series, start_pos: int, offset: int) -> tuple[pd.Timestamp | None, float | None]:
    target_pos = start_pos + int(offset)
    if target_pos >= len(series):
        return None, None
    value = series.iloc[target_pos]
    if pd.isna(value):
        return None, None
    return pd.Timestamp(series.index[target_pos]).normalize(), float(value)


def analyze_event_windows(
    close: pd.DataFrame,
    *,
    events: Sequence[TradeWarEvent] = TRADE_WAR_EVENTS_2018_TO_PRESENT,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    trough_window_days: int = DEFAULT_TROUGH_WINDOW_DAYS,
) -> pd.DataFrame:
    if close.empty:
        raise RuntimeError("close matrix is empty")
    close = close.copy().sort_index()
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    close.columns = close.columns.astype(str).str.upper().str.strip()

    rows: list[dict[str, object]] = []
    for event in events:
        signal_date = _event_signal_date(close.index, event.event_date)
        if signal_date is None:
            continue
        for symbol in close.columns:
            series = pd.to_numeric(close[symbol], errors="coerce").dropna()
            if series.empty or signal_date not in series.index:
                continue
            signal_pos = series.index.get_loc(signal_date)
            if not isinstance(signal_pos, int):
                signal_pos = int(pd.Series(range(len(series)), index=series.index).loc[signal_date].max())
            signal_close = float(series.iloc[signal_pos])
            if signal_close <= 0:
                continue

            row: dict[str, object] = {
                "event_id": event.event_id,
                "event_date": pd.Timestamp(event.event_date).date().isoformat(),
                "signal_date": signal_date.date().isoformat(),
                "kind": event.kind,
                "region": event.region,
                "title": event.title,
                "symbol": symbol,
                "signal_close": signal_close,
            }

            if event.kind == EVENT_KIND_SHOCK:
                trough_end = min(len(series) - 1, signal_pos + int(trough_window_days))
                trough_slice = series.iloc[signal_pos : trough_end + 1].dropna()
                if trough_slice.empty:
                    continue
                trough_date = pd.Timestamp(trough_slice.idxmin()).normalize()
                trough_pos = series.index.get_loc(trough_date)
                if not isinstance(trough_pos, int):
                    trough_pos = int(pd.Series(range(len(series)), index=series.index).loc[trough_date].max())
                trough_close = float(trough_slice.min())
                row.update(
                    {
                        "trough_date": trough_date.date().isoformat(),
                        "trough_close": trough_close,
                        "trough_days_from_signal": int(trough_pos - signal_pos),
                        "trough_return_from_signal": trough_close / signal_close - 1.0,
                    }
                )
                base_pos = trough_pos
                base_close = trough_close
                prefix = "from_trough"
            else:
                row.update(
                    {
                        "trough_date": "",
                        "trough_close": float("nan"),
                        "trough_days_from_signal": 0,
                        "trough_return_from_signal": 0.0,
                    }
                )
                base_pos = signal_pos
                base_close = signal_close
                prefix = "from_signal"

            for horizon in horizons:
                horizon_date, horizon_close = _value_at_offset(series, base_pos, int(horizon))
                if horizon_close is None or horizon_date is None:
                    row[f"return_{prefix}_{horizon}d"] = float("nan")
                    row[f"max_return_{prefix}_{horizon}d"] = float("nan")
                    row[f"horizon_date_{horizon}d"] = ""
                    continue
                window = series.iloc[base_pos : min(len(series), base_pos + int(horizon) + 1)].dropna()
                row[f"return_{prefix}_{horizon}d"] = horizon_close / base_close - 1.0
                row[f"max_return_{prefix}_{horizon}d"] = (
                    float(window.max() / base_close - 1.0) if not window.empty else float("nan")
                )
                row[f"horizon_date_{horizon}d"] = horizon_date.date().isoformat()

                signal_horizon_date, signal_horizon_close = _value_at_offset(series, signal_pos, int(horizon))
                row[f"return_from_signal_{horizon}d"] = (
                    signal_horizon_close / signal_close - 1.0 if signal_horizon_close is not None else float("nan")
                )
                row[f"horizon_from_signal_date_{horizon}d"] = (
                    signal_horizon_date.date().isoformat() if signal_horizon_date is not None else ""
                )

            rows.append(row)
    return pd.DataFrame(rows)


def summarize_symbol_windows(
    event_windows: pd.DataFrame,
    *,
    kind: str = EVENT_KIND_SHOCK,
    ranking_horizon: int = 42,
) -> pd.DataFrame:
    frame = event_windows.loc[event_windows["kind"].eq(kind)].copy()
    if frame.empty:
        return pd.DataFrame()

    if kind == EVENT_KIND_SHOCK:
        rebound_col = f"max_return_from_trough_{ranking_horizon}d"
        direct_col = f"return_from_signal_{ranking_horizon}d"
    else:
        rebound_col = f"max_return_from_signal_{ranking_horizon}d"
        direct_col = f"return_from_signal_{ranking_horizon}d"

    required = ["symbol", rebound_col, direct_col, "trough_return_from_signal"]
    for column in required:
        if column not in frame.columns:
            frame[column] = float("nan")

    rows = []
    for symbol, group in frame.groupby("symbol", sort=False):
        rebound = pd.to_numeric(group[rebound_col], errors="coerce")
        direct = pd.to_numeric(group[direct_col], errors="coerce")
        trough = pd.to_numeric(group["trough_return_from_signal"], errors="coerce")
        rows.append(
            {
                "symbol": symbol,
                "event_count": int(len(group)),
                "usable_rebound_count": int(rebound.notna().sum()),
                "median_trough_return_from_signal": float(trough.median()) if trough.notna().any() else float("nan"),
                "worst_trough_return_from_signal": float(trough.min()) if trough.notna().any() else float("nan"),
                f"median_max_rebound_{ranking_horizon}d": (
                    float(rebound.median()) if rebound.notna().any() else float("nan")
                ),
                f"best_max_rebound_{ranking_horizon}d": float(rebound.max()) if rebound.notna().any() else float("nan"),
                f"median_direct_return_{ranking_horizon}d": (
                    float(direct.median()) if direct.notna().any() else float("nan")
                ),
                f"hit_rate_max_rebound_gt_20pct_{ranking_horizon}d": float((rebound > 0.20).mean())
                if rebound.notna().any()
                else float("nan"),
                f"hit_rate_direct_return_gt_10pct_{ranking_horizon}d": float((direct > 0.10).mean())
                if direct.notna().any()
                else float("nan"),
            }
        )
    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    summary["research_score"] = (
        pd.to_numeric(summary[f"median_max_rebound_{ranking_horizon}d"], errors="coerce").fillna(0.0)
        + pd.to_numeric(summary[f"median_direct_return_{ranking_horizon}d"], errors="coerce").fillna(0.0) * 0.5
        + pd.to_numeric(summary["median_trough_return_from_signal"], errors="coerce").fillna(0.0) * 0.25
    )
    return summary.sort_values(
        [
            "research_score",
            f"median_max_rebound_{ranking_horizon}d",
            f"hit_rate_max_rebound_gt_20pct_{ranking_horizon}d",
        ],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _format_percent_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in output.columns:
        is_rate_column = (
            column.startswith(("median_", "worst_", "best_", "hit_rate_"))
            or column.startswith("research_score")
        )
        if is_rate_column:
            output[column] = output[column].map(lambda value: f"{float(value):.2%}" if pd.notna(value) else "")
    return output


def _split_ints(raw: str | Sequence[int]) -> tuple[int, ...]:
    if isinstance(raw, str):
        values = raw.split(",")
    else:
        values = list(raw)
    output = []
    for value in values:
        value_text = str(value).strip()
        if value_text:
            output.append(int(value_text))
    return tuple(output)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research trade-war/TACO-like panic rebound event windows.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated symbol list")
    parser.add_argument("--price-start", default=DEFAULT_START_DATE)
    parser.add_argument("--price-end", default=DEFAULT_END_DATE)
    parser.add_argument("--horizons", default=",".join(str(value) for value in DEFAULT_HORIZONS))
    parser.add_argument("--trough-window-days", type=int, default=DEFAULT_TROUGH_WINDOW_DAYS)
    parser.add_argument("--ranking-horizon", type=int, default=42)
    parser.add_argument("--event-set", choices=tuple(sorted(TRADE_WAR_EVENT_SETS)), default=DEFAULT_EVENT_SET)
    parser.add_argument("--output-dir", required=True, help="Directory for event-window research outputs")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols = split_symbols(args.symbols)
    if args.download:
        price_history = download_price_history(
            list(symbols),
            start=args.price_start,
            end=args.price_end,
        )
        input_dir = output_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        prices_path = input_dir / "taco_panic_rebound_price_history.csv"
        price_history.to_csv(prices_path, index=False)
        print(f"downloaded {len(price_history)} price rows -> {prices_path}")
    else:
        price_history = read_table(args.prices)

    close = price_history_to_close_matrix(price_history)
    horizons = _split_ints(args.horizons)
    events = resolve_trade_war_event_set(args.event_set)
    windows = analyze_event_windows(
        close,
        events=events,
        horizons=horizons,
        trough_window_days=int(args.trough_window_days),
    )
    shock_summary = summarize_symbol_windows(windows, kind=EVENT_KIND_SHOCK, ranking_horizon=int(args.ranking_horizon))
    softening_summary = summarize_symbol_windows(
        windows,
        kind=EVENT_KIND_SOFTENING,
        ranking_horizon=int(args.ranking_horizon),
    )
    event_calendar = events_to_frame(events)

    windows_path = output_dir / "event_windows.csv"
    shock_summary_path = output_dir / "shock_symbol_summary.csv"
    softening_summary_path = output_dir / "softening_symbol_summary.csv"
    events_path = output_dir / "event_calendar.csv"
    windows.to_csv(windows_path, index=False)
    shock_summary.to_csv(shock_summary_path, index=False)
    softening_summary.to_csv(softening_summary_path, index=False)
    event_calendar.to_csv(events_path, index=False)

    print("\nShock-event ranking:")
    print(_format_percent_columns(shock_summary.head(15)).to_string(index=False))
    print("\nSoftening-event ranking:")
    print(_format_percent_columns(softening_summary.head(15)).to_string(index=False))
    print(f"\nwrote event windows -> {windows_path}")
    print(f"wrote shock summary -> {shock_summary_path}")
    print(f"wrote softening summary -> {softening_summary_path}")
    print(f"wrote event calendar -> {events_path}")
    return 0


__all__ = [
    "DEFAULT_HORIZONS",
    "DEFAULT_SYMBOLS",
    "EVENT_KIND_SHOCK",
    "EVENT_KIND_SOFTENING",
    "FIRST_TERM_TRADE_WAR_EVENTS",
    "BIDEN_TERM_TRADE_WAR_EVENTS",
    "SECOND_TERM_TRADE_WAR_EVENTS",
    "GEOPOLITICAL_CONFLICT_EVENTS_2026",
    "GEOPOLITICAL_DEESCALATION_EVENTS_2026",
    "GEOPOLITICAL_CONFLICT_AND_DEESCALATION_EVENTS_2026",
    "TRADE_WAR_EVENTS_2018_TO_PRESENT",
    "TACO_EVENTS_WITH_GEOPOLITICAL_DEESCALATION",
    "TACO_EVENTS_WITH_GEOPOLITICAL_CONFLICT_AND_DEESCALATION",
    "TRADE_WAR_EVENT_SETS",
    "TradeWarEvent",
    "analyze_event_windows",
    "events_to_frame",
    "main",
    "price_history_to_close_matrix",
    "resolve_trade_war_event_set",
    "split_symbols",
    "summarize_symbol_windows",
]
