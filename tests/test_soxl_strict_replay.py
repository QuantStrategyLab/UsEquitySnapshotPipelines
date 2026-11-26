import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines.research import soxl_strict_replay as study

ROOT = Path(__file__).resolve().parents[1]


def _prices(seed=5):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2010-01-04", periods=3400)
    soxx = rng.normal(0.0006, 0.015, len(dates))
    paths = {"SOXX": soxx, "SOXL": 3 * soxx - 0.0001, "BIL": rng.normal(0.0001, 0.0002, len(dates)),
             "SPY": rng.normal(0.0004, 0.011, len(dates))}
    rows = []
    for symbol, r in paths.items():
        close = 100 * np.cumprod(1 + r)
        rows += [{"symbol": symbol, "as_of": d, "close": c} for d, c in zip(dates, close)]
    return pd.DataFrame(rows)


def _trend_targets(close):
    """A simple stand-in strategy, already lagged one session like the real simulator."""
    on = close["SOXX"] > close["SOXX"].rolling(140).mean()
    targets = pd.DataFrame(0.0, index=close.index, columns=["SOXL", "SOXX", "BIL"])
    targets.loc[on, "SOXL"] = 0.7
    targets.loc[on, "BIL"] = 0.3
    targets.loc[~on, "BIL"] = 1.0
    return targets.shift(1)


def test_strict_is_never_better_than_original_and_stress_only_hurts():
    rows = study.run(_prices(), _trend_targets)["rows"]
    full = rows.loc[rows["period"].eq("full")]
    original = full.loc[full["method"].eq("original"), "cagr"].iloc[0]
    strict = full.loc[full["method"].eq("strict")].sort_values("stress")["cagr"].tolist()
    assert strict[0] <= original + 0.02  # next-close execution plus costs cannot add much
    assert all(a >= b - 1e-12 for a, b in zip(strict, strict[1:]))
    assert set(rows["period"]) == {"full", "oos"}


def test_original_replay_matches_hand_calculation():
    idx = pd.bdate_range("2021-01-04", periods=3)
    close = pd.DataFrame({"SOXL": [100.0, 110.0, 99.0], "BIL": [1.0, 1.0, 1.0]}, index=idx)
    targets = pd.DataFrame({"SOXL": [1.0, 1.0, 0.0], "BIL": [0.0, 0.0, 1.0]}, index=idx)
    out = study.original_replay(targets, close, cost_bps=5.0)
    assert out.iloc[1] == pytest.approx(0.10)
    assert out.iloc[2] == pytest.approx(0.0 - 1.0 * 5e-4)  # switched to BIL; full turnover cost


def test_summary_flags_balanced_tier_exclusion():
    output = study.run(_prices(), _trend_targets)
    assert output["summary"]["excluded_on_balanced_tier"] == (abs(output["summary"]["strict_max_drawdown"]) > 0.30)
    assert "live_ready=False" in study.render_markdown(output)


def test_mandate_gates_data_access(tmp_path):
    mandate = ROOT / study.MANDATE_PATH
    assert study.verify_mandate(mandate, "2026-11-25", now=pd.Timestamp("2026-11-27", tz="UTC"))["scope"]["no_live"]
    for end, now in (("2026-12-15", "2026-11-27"), ("2026-11-25", "2027-01-01")):
        with pytest.raises(SystemExit):
            study.verify_mandate(mandate, end, now=pd.Timestamp(now, tz="UTC"))
    tampered = json.loads(mandate.read_text())
    tampered["scope"]["latest_end_date"] = "2030-01-01"
    path = tmp_path / "m.json"
    path.write_text(json.dumps(tampered))
    with pytest.raises(SystemExit):
        study.verify_mandate(path, "2026-11-25", now=pd.Timestamp("2026-11-27", tz="UTC"))
    entry = subprocess.run([sys.executable, "-m", study.STUDY_ID, "--end-date", "2027-06-30", "--output-dir",
                            str(tmp_path / "o")], capture_output=True, text=True, cwd=ROOT)
    assert "rejected: end_date" in entry.stderr + entry.stdout
