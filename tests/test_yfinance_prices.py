from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines import yfinance_prices


def test_load_proxy_candidates_normalizes_http_proxy_file(tmp_path) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(
        "\n".join(
            [
                "# comment",
                "1.2.3.4:8080",
                "http://5.6.7.8:3128",
                "socks5://9.9.9.9:1080",
                "bad proxy",
                "1.2.3.4:8080",
            ]
        ),
        encoding="utf-8",
    )

    assert yfinance_prices.load_proxy_candidates(proxy_file) == [
        "http://1.2.3.4:8080",
        "http://5.6.7.8:3128",
        "socks5://9.9.9.9:1080",
    ]


def test_proxy_candidate_download_falls_through_to_second_candidate(monkeypatch) -> None:
    calls: list[str | None] = []

    def fake_download(
        symbols,
        *,
        start,
        end=None,
        chunk_size=100,
        download_fn=None,
        symbol_aliases=None,
        proxy=None,
        price_field="adjusted_close",
    ):
        calls.append(proxy)
        if proxy == "http://1.2.3.4:8080":
            raise RuntimeError("proxy failed")
        return pd.DataFrame(
            [
                {
                    "symbol": "QQQ",
                    "as_of": pd.Timestamp("2026-01-02"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1000,
                }
            ]
        )

    monkeypatch.setattr(yfinance_prices, "download_price_history", fake_download)

    result = yfinance_prices.download_price_history_with_proxy_candidates(
        ["QQQ"],
        start="2026-01-01",
        download_fn=lambda *args, **kwargs: None,
        proxy_candidates=["1.2.3.4:8080", "5.6.7.8:3128"],
    )

    assert calls == ["http://1.2.3.4:8080", "http://5.6.7.8:3128"]
    assert result["symbol"].tolist() == ["QQQ"]


def test_fetch_yahoo_chart_payload_uses_requests_for_socks_proxy(monkeypatch) -> None:
    import requests

    proxy = "socks5h://user:password@proxy.example:1080"
    calls: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            calls["raised"] = True

        def json(self) -> dict[str, object]:
            return {"chart": {"result": [], "error": None}}

    def fake_get(url, *, headers, proxies, timeout):
        calls["url"] = url
        calls["headers"] = headers
        calls["proxies"] = proxies
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(requests, "get", fake_get)

    result = yfinance_prices._fetch_yahoo_chart_payload(
        "QQQ",
        start="2026-01-01",
        end="2026-01-02",
        proxy=proxy,
    )

    assert result == {"chart": {"result": [], "error": None}}
    assert calls["proxies"] == {"http": proxy, "https": proxy}
    assert calls["timeout"] == 10
    assert calls["raised"] is True


def test_normalize_yahoo_chart_payload_uses_adjusted_close_ratio() -> None:
    payload = {
        "chart": {
            "result": [
                {
                    "timestamp": [1767312000],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100.0],
                                "high": [110.0],
                                "low": [90.0],
                                "close": [100.0],
                                "volume": [1000],
                            }
                        ],
                        "adjclose": [{"adjclose": [50.0]}],
                    },
                }
            ],
            "error": None,
        }
    }

    result = yfinance_prices._normalize_yahoo_chart_payload(
        payload,
        original_symbol="QQQ",
        price_field="adjusted_close",
    )

    row = result.iloc[0]
    assert row["symbol"] == "QQQ"
    assert row["open"] == 50.0
    assert row["high"] == 55.0
    assert row["low"] == 45.0
    assert row["close"] == 50.0


def test_normalize_yahoo_chart_payload_can_use_raw_close() -> None:
    payload = {
        "chart": {
            "result": [
                {
                    "timestamp": [1767312000],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100.0],
                                "high": [110.0],
                                "low": [90.0],
                                "close": [100.0],
                                "volume": [1000],
                            }
                        ],
                        "adjclose": [{"adjclose": [50.0]}],
                    },
                }
            ],
            "error": None,
        }
    }

    result = yfinance_prices._normalize_yahoo_chart_payload(payload, original_symbol="QQQ", price_field="close")

    row = result.iloc[0]
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 90.0
    assert row["close"] == 100.0


def test_download_price_history_uses_auto_adjust_for_adjusted_close(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_download(symbols, *, start, end, auto_adjust, progress, threads):
        calls.append({"symbols": tuple(symbols), "auto_adjust": auto_adjust})
        dates = pd.DatetimeIndex([pd.Timestamp("2026-01-02")])
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.0],
                "Volume": [1000],
            },
            index=dates,
        )

    yfinance_prices.download_price_history(
        ["QQQ"],
        start="2026-01-01",
        end="2026-01-03",
        download_fn=fake_download,
        price_field="adjusted_close",
    )
    yfinance_prices.download_price_history(
        ["QQQ"],
        start="2026-01-01",
        end="2026-01-03",
        download_fn=fake_download,
        price_field="close",
    )

    assert calls == [
        {"symbols": ("QQQ",), "auto_adjust": True},
        {"symbols": ("QQQ",), "auto_adjust": False},
    ]
