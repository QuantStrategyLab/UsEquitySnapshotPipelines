from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

SCRIPT = Path("scripts/diagnose_twelve_data_daily.py")
SPEC = importlib.util.spec_from_file_location("diagnose_twelve_data_daily", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
diagnostic_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic_cli)


def test_diagnostic_prints_hashes_and_statuses_but_not_raw_bars(monkeypatch, capsys) -> None:
    def ready(*, symbol: str, **kwargs):
        snapshot = DailyBarSourceSnapshot(
            source_id="twelve_data_1day_adjustment_all",
            symbol=symbol,
            date_cutoff="2026-08-21",
            adjustment_basis="total_return_adjusted",
            source_artifact_sha256="a" * 64,
            bars=(DailyBar("2026-08-21", 100, 102, 99, 101, 1_000_000),),
        )
        return DailyBarSourceObservation(snapshot.source_id, SOURCE_OBSERVATION_READY, snapshot)

    monkeypatch.setattr(diagnostic_cli, "observe_twelve_data_adjusted_daily_bars", ready)

    assert diagnostic_cli.main(["--date-cutoff", "2026-08-21"]) == 0

    output = capsys.readouterr().out.strip()
    assert output.startswith("TWELVE_DATA_DAILY_DIAGNOSTIC=")
    payload = json.loads(output.removeprefix("TWELVE_DATA_DAILY_DIAGNOSTIC="))
    assert payload["status"] == "TWELVE_DATA_DAILY_ACCESS_OK"
    assert payload["symbol_statuses"] == {"BOXX": "READY", "SOXL": "READY", "SOXX": "READY"}
    assert '"open"' not in output
    assert '"volume"' not in output
