"""Inject Twelve Data and Yahoo Finance observations into the TQQQ V8 P1 boundary."""

from __future__ import annotations

from quant_platform_kit.data.multisource_assurance import DailyBarSourceObservation

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_free_ohlcv_p1 import (
    TqqqCoreOnlyFreeOhlcvObserver,
)
from us_equity_snapshot_pipelines.twelve_data_daily import (
    TWELVE_DATA_DAILY_SOURCE_ID,
    observe_twelve_data_adjusted_daily_bars,
)
from us_equity_snapshot_pipelines.yahoo_finance_daily import (
    YAHOO_FINANCE_DAILY_SOURCE_ID,
    observe_yahoo_finance_adjusted_daily_bars,
)


class TwelveYahooOhlcvObserver(TqqqCoreOnlyFreeOhlcvObserver):
    """Fixed canonical/verifier pairing; no fallback or persistence side effect."""

    def __init__(self, twelve_data_api_key: str) -> None:
        self._twelve_data_api_key = twelve_data_api_key

    def observe_daily_bars(
        self,
        *,
        source_id: str,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> DailyBarSourceObservation:
        if source_id == TWELVE_DATA_DAILY_SOURCE_ID:
            return observe_twelve_data_adjusted_daily_bars(
                api_key=self._twelve_data_api_key,
                symbol=symbol,
                start_date=start_date,
                date_cutoff=date_cutoff,
            )
        if source_id == YAHOO_FINANCE_DAILY_SOURCE_ID:
            return observe_yahoo_finance_adjusted_daily_bars(
                symbol=symbol,
                start_date=start_date,
                date_cutoff=date_cutoff,
            )
        raise ValueError("unknown free TQQQ P1 source")


__all__ = ["TwelveYahooOhlcvObserver"]
