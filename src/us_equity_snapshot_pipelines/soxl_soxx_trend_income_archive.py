from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

import pandas as pd

from .artifacts import sha256_file, write_json
from .backtest_windows import build_benchmark_returns, build_window_summary
from .contracts import SOURCE_PROJECT
from .pipelines.soxl_soxx_trend_income_backtest import (
    DEFAULT_BACKTEST_START,
    DEFAULT_INITIAL_EQUITY_USD,
    DEFAULT_OUTPUT_COLUMNS,
    DEFAULT_PRICE_START,
    DEFAULT_TURNOVER_COST_BPS,
    MANAGED_SYMBOLS,
    PROFILE,
    _build_price_frame,
    _format_summary,
    run_backtest,
)
from .yfinance_prices import (
    _resolve_yfinance_proxy,
    download_price_history_with_proxy_candidates,
    load_proxy_candidates,
)

DEFAULT_CORE_LONG_PRICE_START = "2010-01-01"
DEFAULT_DYNAMIC_RSI_QUANTILE_WINDOW = 252
DEFAULT_DYNAMIC_RSI_QUANTILE = 0.90
DEFAULT_DYNAMIC_RSI_FLOOR = 70.0


@dataclass(frozen=True)
class ArchiveModeSpec:
    mode: str
    description: str
    symbols: tuple[str, ...]
    price_start: str
    backtest_start: str
    disable_income_layer: bool
    symbol_aliases: Mapping[str, tuple[str, ...]]


ARCHIVE_MODE_SPECS = {
    "live-full": ArchiveModeSpec(
        mode="live-full",
        description="Production-style SOXL/SOXX trend-income archive with BOXX and the diversified income basket.",
        symbols=(*MANAGED_SYMBOLS, "QQQ", "SPY"),
        price_start=DEFAULT_PRICE_START,
        backtest_start=DEFAULT_BACKTEST_START,
        disable_income_layer=False,
        symbol_aliases={},
    ),
    "core-long": ArchiveModeSpec(
        mode="core-long",
        description="Long-history SOXL/SOXX core archive with BIL downloaded and stored as BOXX.",
        symbols=("SOXL", "SOXX", "BOXX", "QQQ", "SPY"),
        price_start=DEFAULT_CORE_LONG_PRICE_START,
        backtest_start=DEFAULT_CORE_LONG_PRICE_START,
        disable_income_layer=True,
        symbol_aliases={"BOXX": ("BIL",)},
    ),
}


def _resolve_output_dir(
    *,
    mode: str,
    output_dir: str | Path | None,
    output_root: str | Path,
    archive_date: str,
) -> Path:
    if output_dir:
        return Path(output_dir)
    normalized_mode = str(mode).replace("-", "_")
    return Path(output_root) / f"{PROFILE}_{normalized_mode}_archive_{archive_date}"


def _parse_symbol_aliases(values: Sequence[str] | None) -> dict[str, tuple[str, ...]]:
    aliases: dict[str, tuple[str, ...]] = {}
    for raw_value in values or ():
        key, separator, raw_candidates = str(raw_value or "").partition("=")
        symbol = key.strip().upper()
        if not separator or not symbol:
            raise ValueError(f"invalid symbol alias {raw_value!r}; expected SYMBOL=CANDIDATE[,CANDIDATE]")
        candidates = tuple(
            candidate.strip().upper().replace(".", "-")
            for candidate in raw_candidates.split(",")
            if candidate.strip()
        )
        if not candidates:
            raise ValueError(f"invalid symbol alias {raw_value!r}; expected at least one candidate")
        aliases[symbol] = candidates
    return aliases


def _merge_symbol_aliases(
    default_aliases: Mapping[str, Sequence[str]],
    override_aliases: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    merged = {
        str(symbol).strip().upper(): tuple(str(candidate).strip().upper().replace(".", "-") for candidate in candidates)
        for symbol, candidates in dict(default_aliases or {}).items()
    }
    for symbol, candidates in dict(override_aliases or {}).items():
        symbol_text = str(symbol).strip().upper()
        normalized_candidates = tuple(
            str(candidate).strip().upper().replace(".", "-") for candidate in candidates if str(candidate).strip()
        )
        if symbol_text and normalized_candidates:
            merged[symbol_text] = normalized_candidates
    return merged


def _download_symbol_for(symbol: str, aliases: Mapping[str, Sequence[str]]) -> str:
    normalized_symbol = str(symbol or "").strip().upper()
    candidates = tuple(aliases.get(normalized_symbol, ()))
    return str(candidates[0]).strip().upper() if candidates else normalized_symbol


def _build_data_quality_report(
    prices: pd.DataFrame,
    *,
    requested_symbols: Sequence[str],
    symbol_aliases: Mapping[str, Sequence[str]],
) -> pd.DataFrame:
    price_frame = _build_price_frame(prices)
    rows = []
    for symbol in requested_symbols:
        symbol_text = str(symbol).strip().upper()
        group = price_frame.loc[price_frame["symbol"].eq(symbol_text)]
        start = group["as_of"].min() if not group.empty else pd.NaT
        end = group["as_of"].max() if not group.empty else pd.NaT
        rows.append(
            {
                "symbol": symbol_text,
                "download_symbol": _download_symbol_for(symbol_text, symbol_aliases),
                "row_count": int(len(group)),
                "start": start.date().isoformat() if pd.notna(start) else "",
                "end": end.date().isoformat() if pd.notna(end) else "",
                "min_close": float(group["close"].min()) if not group.empty else float("nan"),
                "max_close": float(group["close"].max()) if not group.empty else float("nan"),
                "first_close": float(group.sort_values("as_of")["close"].iloc[0]) if not group.empty else float("nan"),
                "last_close": float(group.sort_values("as_of")["close"].iloc[-1]) if not group.empty else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _source_ranges(data_quality_report: pd.DataFrame) -> dict[str, object]:
    usable = data_quality_report.loc[data_quality_report["row_count"].gt(0)].copy()
    if usable.empty:
        return {"common_start": None, "common_end": None, "symbols": []}
    return {
        "common_start": str(usable["start"].max()),
        "common_end": str(usable["end"].min()),
        "symbols": json.loads(usable.to_json(orient="records")),
    }


def _git_info(path: str | Path) -> dict[str, object]:
    repo = Path(path)
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
        status = subprocess.check_output(["git", "status", "--short"], cwd=repo, text=True).strip()
    except Exception:
        return {"commit": None, "dirty": None}
    return {"commit": commit, "dirty": bool(status)}


def _sanitize_argv(argv: Sequence[str]) -> list[str]:
    sanitized: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            sanitized.append("<redacted>")
            skip_next = False
            continue
        if arg == "--proxy":
            sanitized.append(arg)
            skip_next = True
            continue
        if arg.startswith("--proxy="):
            sanitized.append("--proxy=<redacted>")
            continue
        sanitized.append(str(arg))
    return sanitized


def _json_safe(value):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _archive_proxy_used(*, source_kind: str, proxy: str | None, proxy_list: str | None = None) -> bool:
    if source_kind != "yfinance":
        return False
    return bool(_resolve_yfinance_proxy(proxy) or str(proxy_list or "").strip())


def _write_backtest_outputs(
    *,
    output_dir: Path,
    prices: pd.DataFrame,
    result: Mapping[str, object],
    config: Mapping[str, object],
) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    price_frame = _build_price_frame(prices)
    price_frame.to_csv(output_dir / "price_history.csv", index=False)
    summary_frame = _format_summary(result["summary"])
    summary_frame.to_csv(output_dir / "summary.csv", index=False)
    window_summary = build_window_summary(
        result["portfolio_returns"],
        benchmark_returns=build_benchmark_returns(price_frame),
    )
    window_summary.to_csv(output_dir / "window_summary.csv", index=False)
    result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
    result["weights_history"].to_csv(output_dir / "weights_history.csv")
    result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
    result["trades"].to_csv(output_dir / "trades.csv", index=False)
    result["signal_history"].to_csv(output_dir / "signal_history.csv", index=False)
    write_json(output_dir / "backtest_config.json", config)
    return summary_frame


def archive_backtest(
    *,
    mode: str,
    output_dir: str | Path,
    prices: pd.DataFrame | None = None,
    prices_path: str | Path | None = None,
    price_start: str | None = None,
    price_end: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    initial_equity: float = DEFAULT_INITIAL_EQUITY_USD,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    dynamic_rsi: bool = True,
    dynamic_rsi_quantile_window: int = DEFAULT_DYNAMIC_RSI_QUANTILE_WINDOW,
    dynamic_rsi_quantile: float = DEFAULT_DYNAMIC_RSI_QUANTILE,
    dynamic_rsi_floor: float = DEFAULT_DYNAMIC_RSI_FLOOR,
    symbol_aliases: Mapping[str, Sequence[str]] | None = None,
    proxy: str | None = None,
    proxy_list: str | None = None,
    proxy_list_max: int = 12,
    sanitized_argv: Sequence[str] | None = None,
    download_fn: Callable | None = None,
) -> Path:
    if mode not in ARCHIVE_MODE_SPECS:
        expected = ", ".join(sorted(ARCHIVE_MODE_SPECS))
        raise ValueError(f"unsupported archive mode {mode!r}; expected one of: {expected}")
    if prices is not None and prices_path is not None:
        raise ValueError("provide either prices or prices_path, not both")

    spec = ARCHIVE_MODE_SPECS[mode]
    merged_aliases = _merge_symbol_aliases(spec.symbol_aliases, symbol_aliases)
    effective_price_start = price_start or spec.price_start
    effective_start = start_date or spec.backtest_start
    output_path = Path(output_dir)

    source_kind = "local_prices"
    source_path = str(prices_path) if prices_path is not None else None
    if prices is None:
        if prices_path is not None:
            prices = pd.read_csv(prices_path)
        else:
            source_kind = "yfinance"
            prices = download_price_history_with_proxy_candidates(
                list(spec.symbols),
                start=effective_price_start,
                end=price_end,
                chunk_size=25,
                download_fn=download_fn,
                symbol_aliases=merged_aliases,
                proxy=proxy,
                proxy_candidates=load_proxy_candidates(proxy_list, max_candidates=proxy_list_max) if proxy_list else None,
            )

    data_quality_report = _build_data_quality_report(
        prices,
        requested_symbols=spec.symbols,
        symbol_aliases=merged_aliases,
    )
    missing_symbols = sorted(data_quality_report.loc[data_quality_report["row_count"].eq(0), "symbol"].tolist())
    if missing_symbols:
        missing_text = ", ".join(missing_symbols)
        raise RuntimeError(f"archive price history missing required symbols: {missing_text}")

    result = run_backtest(
        prices,
        initial_equity=float(initial_equity),
        start_date=effective_start,
        end_date=end_date,
        turnover_cost_bps=float(turnover_cost_bps),
        dynamic_rsi_quantile_window=int(dynamic_rsi_quantile_window) if dynamic_rsi else None,
        dynamic_rsi_quantile=float(dynamic_rsi_quantile) if dynamic_rsi else None,
        dynamic_rsi_floor=float(dynamic_rsi_floor) if dynamic_rsi else None,
        disable_income_layer=bool(spec.disable_income_layer),
    )
    config = {
        "profile": PROFILE,
        "archive_mode": spec.mode,
        "archive_mode_description": spec.description,
        "initial_equity": float(initial_equity),
        "price_start": effective_price_start,
        "price_end": price_end,
        "start_date": effective_start,
        "end_date": end_date,
        "turnover_cost_bps": float(turnover_cost_bps),
        "dynamic_rsi": bool(dynamic_rsi),
        "dynamic_rsi_quantile_window": int(dynamic_rsi_quantile_window) if dynamic_rsi else None,
        "dynamic_rsi_quantile": float(dynamic_rsi_quantile) if dynamic_rsi else None,
        "dynamic_rsi_floor": float(dynamic_rsi_floor) if dynamic_rsi else None,
        "disable_income_layer": bool(spec.disable_income_layer),
        "symbols": list(spec.symbols),
        "symbol_aliases": {key: list(value) for key, value in sorted(merged_aliases.items())},
    }
    summary_frame = _write_backtest_outputs(
        output_dir=output_path,
        prices=prices,
        result=result,
        config=config,
    )
    data_quality_report.to_csv(output_path / "data_quality_report.csv", index=False)

    manifest = {
        "manifest_type": "research_backtest_archive",
        "source_project": SOURCE_PROJECT,
        "strategy_profile": PROFILE,
        "archive_mode": spec.mode,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_kind": source_kind,
        "price_source": {
            "provider": "yfinance" if source_kind == "yfinance" else "local_prices",
            "auto_adjust": True if source_kind == "yfinance" else None,
            "prices_path": source_path,
            "price_start": effective_price_start,
            "price_end": price_end,
            "requested_symbols": list(spec.symbols),
            "symbol_aliases": {key: list(value) for key, value in sorted(merged_aliases.items())},
            "proxy_used": _archive_proxy_used(source_kind=source_kind, proxy=proxy, proxy_list=proxy_list),
        },
        "data_quality": _source_ranges(data_quality_report),
        "backtest": config,
        "artifacts": {
            "price_history": {
                "path": str(output_path / "price_history.csv"),
                "sha256": sha256_file(output_path / "price_history.csv"),
            },
            "summary": {
                "path": str(output_path / "summary.csv"),
                "sha256": sha256_file(output_path / "summary.csv"),
            },
            "window_summary": {
                "path": str(output_path / "window_summary.csv"),
                "sha256": sha256_file(output_path / "window_summary.csv"),
            },
            "backtest_config": {
                "path": str(output_path / "backtest_config.json"),
                "sha256": sha256_file(output_path / "backtest_config.json"),
            },
            "data_quality_report": {
                "path": str(output_path / "data_quality_report.csv"),
                "sha256": sha256_file(output_path / "data_quality_report.csv"),
            },
        },
        "summary": {
            column: _json_safe(summary_frame.iloc[0][column])
            for column in DEFAULT_OUTPUT_COLUMNS
            if column in summary_frame.columns
        },
        "git": _git_info(Path(__file__).resolve().parents[2]),
        "command_argv": list(sanitized_argv or ()),
    }
    write_json(output_path / "source_manifest.json", manifest)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive SOXL/SOXX trend-income backtest outputs with source metadata.")
    parser.add_argument("--mode", choices=sorted(ARCHIVE_MODE_SPECS), required=True)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--download", action="store_true", help="Download adjusted prices with yfinance")
    input_group.add_argument("--prices", help="Use an existing archive-compatible price history CSV")
    parser.add_argument("--output-root", default="data/output")
    parser.add_argument("--output-dir", help="Explicit output directory; overrides --output-root and --archive-date")
    parser.add_argument(
        "--archive-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Date suffix used when --output-dir is omitted",
    )
    parser.add_argument("--price-start", help="Download start date")
    parser.add_argument("--price-end", help="Download end date")
    parser.add_argument("--start", dest="start_date", help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--disable-dynamic-rsi", action="store_true")
    parser.add_argument("--dynamic-rsi-quantile-window", type=int, default=DEFAULT_DYNAMIC_RSI_QUANTILE_WINDOW)
    parser.add_argument("--dynamic-rsi-quantile", type=float, default=DEFAULT_DYNAMIC_RSI_QUANTILE)
    parser.add_argument("--dynamic-rsi-floor", type=float, default=DEFAULT_DYNAMIC_RSI_FLOOR)
    parser.add_argument("--symbol-alias", action="append", help="Override download alias, for example BOXX=BIL")
    parser.add_argument("--proxy", help="Proxy URL for yfinance; redacted from source_manifest.json")
    parser.add_argument("--proxy-list", help="Path or URL with one HTTP(S) proxy per line")
    parser.add_argument("--proxy-list-max", type=int, default=12, help="Maximum proxy candidates to try")
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(raw_argv)
    output_dir = _resolve_output_dir(
        mode=args.mode,
        output_dir=args.output_dir,
        output_root=args.output_root,
        archive_date=args.archive_date,
    )
    archive_dir = archive_backtest(
        mode=args.mode,
        output_dir=output_dir,
        prices_path=args.prices,
        price_start=args.price_start,
        price_end=args.price_end,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_equity=float(args.initial_equity),
        turnover_cost_bps=float(args.turnover_cost_bps),
        dynamic_rsi=not bool(args.disable_dynamic_rsi),
        dynamic_rsi_quantile_window=int(args.dynamic_rsi_quantile_window),
        dynamic_rsi_quantile=float(args.dynamic_rsi_quantile),
        dynamic_rsi_floor=float(args.dynamic_rsi_floor),
        symbol_aliases=_parse_symbol_aliases(args.symbol_alias),
        proxy=args.proxy,
        proxy_list=args.proxy_list,
        proxy_list_max=int(args.proxy_list_max),
        sanitized_argv=_sanitize_argv(raw_argv),
    )
    print(f"wrote SOXL/SOXX archive -> {archive_dir}")
    print(pd.read_csv(archive_dir / "summary.csv").to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
