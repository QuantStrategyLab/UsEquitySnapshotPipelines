from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd


SCRIPT = Path(__file__).parents[1] / "scripts" / "research_volatility_delever_retention_policies.py"


def _module():
    spec = importlib.util.spec_from_file_location("research_volatility_delever_retention_policies", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _price_frame(symbols: tuple[str, ...]) -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=280)
    rows: list[dict[str, object]] = []
    for offset, symbol in enumerate(symbols):
        start = 40.0 + offset * 7.0
        for index, as_of in enumerate(dates):
            close = start * (1.0 + 0.0007 * index + ((index % 11) - 5) * 0.0008)
            rows.append({"as_of": as_of.date().isoformat(), "symbol": symbol, "close": close})
    return pd.DataFrame(rows)


def _write_inputs(tmp_path: Path, *, include_leveraged_targets: bool = True) -> tuple[Path, Path]:
    tqqq = tmp_path / "tqqq.csv"
    soxl = tmp_path / "soxl.csv"
    tqqq_symbols = ("QQQ", "HYG", "IEF", "XLF", "SPY", "^VIX")
    soxl_symbols = ("SOXX", "HYG", "IEF", "XLF", "SPY", "^VIX")
    if include_leveraged_targets:
        tqqq_symbols = ("QQQ", "TQQQ", "HYG", "IEF", "XLF", "SPY", "^VIX")
        soxl_symbols = ("SOXX", "SOXL", "HYG", "IEF", "XLF", "SPY", "^VIX")
    _price_frame(tqqq_symbols).to_csv(tqqq, index=False)
    _price_frame(soxl_symbols).to_csv(soxl, index=False)
    return tqqq, soxl


def test_synthetic_replay_writes_hashed_stress_only_evidence(tmp_path: Path) -> None:
    module = _module()
    tqqq, soxl = _write_inputs(tmp_path, include_leveraged_targets=False)
    output_dir = tmp_path / "synthetic-output"

    module.run_research(
        tqqq_prices=tqqq,
        soxl_prices=soxl,
        output_dir=output_dir,
        windows=(("fixture", "2020-01-02", "2021-02-01"),),
        synthesize_tqqq_from="qqq",
        synthesize_soxl_from="soxx",
        synthetic_leverage=3.0,
        synthetic_annual_expense_ratio=0.01,
    )

    evidence = json.loads((output_dir / module.SYNTHETIC_STRESS_EVIDENCE_FILENAME).read_text(encoding="utf-8"))
    assert evidence["schema_version"] == module.SYNTHETIC_STRESS_EVIDENCE_SCHEMA
    assert evidence["lane_id"] == "SyntheticLongHistoryStress"
    assert evidence["authority"] == {
        "research_only": True,
        "order_submission_authorized": False,
        "p1_historical_stress_allowed": True,
        "p2_research_comparison_allowed": True,
        "observed_p3_evidence_eligible": False,
        "p4_paper_authorized": False,
        "p5_shadow_authorized": False,
        "p6_live_authorized": False,
    }
    assert evidence["synthetic_legs"] == [
        {
            "profile": "tqqq",
            "source_symbol": "QQQ",
            "target_symbol": "TQQQ",
            "daily_reset_leverage": 3.0,
            "annual_expense_ratio": 0.01,
        },
        {
            "profile": "soxl",
            "source_symbol": "SOXX",
            "target_symbol": "SOXL",
            "daily_reset_leverage": 3.0,
            "annual_expense_ratio": 0.01,
        },
    ]
    assert evidence["validated_matrix_coverage"][0]["session_count"] == 280
    assert "TQQQ" not in evidence["input_artifacts"][0]["symbols"]
    assert "SOXL" not in evidence["input_artifacts"][1]["symbols"]
    assert evidence["evidence_sha256"] == module._canonical_sha256(
        {key: value for key, value in evidence.items() if key != "evidence_sha256"}
    )


def test_observed_only_replay_does_not_claim_synthetic_stress_authority(tmp_path: Path) -> None:
    module = _module()
    tqqq, soxl = _write_inputs(tmp_path)
    output_dir = tmp_path / "observed-output"

    module.run_research(
        tqqq_prices=tqqq,
        soxl_prices=soxl,
        output_dir=output_dir,
        windows=(("fixture", "2020-01-02", "2021-02-01"),),
    )

    assert not (output_dir / module.SYNTHETIC_STRESS_EVIDENCE_FILENAME).exists()
