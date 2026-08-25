from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_UNAVAILABLE,
    DailyBarSourceObservation,
)

SCRIPT = Path(__file__).parents[1] / "scripts" / "acquire_soxl_core_only_free_split_close_p1.py"
SPEC = importlib.util.spec_from_file_location("acquire_soxl_core_only_free_split_close_p1", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
acquisition_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acquisition_cli)


def test_adapter_routes_only_the_fixed_canonical_and_verifier_source_ids(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def twelve(**kwargs):
        calls.append({"source": "twelve", **kwargs})
        return DailyBarSourceObservation("twelve_data_1day_split_adjusted", SOURCE_OBSERVATION_UNAVAILABLE)

    def yahoo(**kwargs):
        calls.append({"source": "yahoo", **kwargs})
        return DailyBarSourceObservation("yahoo_finance_chart_1day_split_adjusted", SOURCE_OBSERVATION_UNAVAILABLE)

    monkeypatch.setattr(acquisition_cli, "observe_twelve_data_adjusted_daily_bars", twelve)
    monkeypatch.setattr(acquisition_cli, "observe_yahoo_finance_adjusted_daily_bars", yahoo)
    observer = acquisition_cli.TwelveYahooSplitAdjustedCloseObserver("secret-not-emitted")

    assert observer.observe_daily_bars(
        source_id="twelve_data_1day_split_adjusted",
        symbol="SOXL",
        start_date="2022-01-03",
        date_cutoff="2026-08-18",
    ).status == SOURCE_OBSERVATION_UNAVAILABLE
    assert observer.observe_daily_bars(
        source_id="yahoo_finance_chart_1day_split_adjusted",
        symbol="SOXX",
        start_date="2022-01-03",
        date_cutoff="2026-08-18",
    ).status == SOURCE_OBSERVATION_UNAVAILABLE
    assert calls == [
        {
            "source": "twelve",
            "api_key": "secret-not-emitted",
            "symbol": "SOXL",
            "start_date": "2022-01-03",
            "date_cutoff": "2026-08-18",
        },
        {
            "source": "yahoo",
            "symbol": "SOXX",
            "start_date": "2022-01-03",
            "date_cutoff": "2026-08-18",
        },
    ]


def test_cli_without_explicit_injection_parks_without_writing_or_echoing_paths(tmp_path: Path, capsys) -> None:
    output_root = tmp_path / "private-p1"

    assert acquisition_cli.main(
        [
            "--output-root",
            str(output_root),
            "--observed-at",
            "2026-08-19T00:00:00Z",
            "--date-cutoff",
            "2026-08-18",
        ]
    ) == 2

    output = capsys.readouterr().out.strip()
    assert json.loads(output) == {"status": "PARKED"}
    assert str(output_root) not in output
    assert not output_root.exists()
