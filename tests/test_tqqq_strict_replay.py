import itertools
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines.research import tqqq_strict_replay as study

ROOT = Path(__file__).resolve().parents[1]


def _prices(seed=7, *, include_qqqm=True):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2010-01-04", periods=3600)
    qqq = rng.normal(0.0005, 0.012, len(dates))
    paths = {
        "QQQ": qqq,
        "TQQQ": 3 * qqq - 0.0001,
        "BIL": rng.normal(0.0001, 0.0002, len(dates)),
        "SPY": rng.normal(0.0004, 0.011, len(dates)),
    }
    rows = []
    for symbol, r in paths.items():
        close = 100 * np.cumprod(1 + r)
        rows += [{"symbol": symbol, "as_of": d, "close": c} for d, c in zip(dates, close)]
    if include_qqqm:
        qqqm_dates = dates[dates >= pd.Timestamp("2020-10-13")]
        qqqm_r = qqq[dates >= pd.Timestamp("2020-10-13")]
        qqqm_close = 100 * np.cumprod(1 + qqqm_r)
        rows += [{"symbol": "QQQM", "as_of": d, "close": c} for d, c in zip(qqqm_dates, qqqm_close)]
    return pd.DataFrame(rows)


def _trend_targets(close):
    """Simple lagged target builder using QQQ pre-listing and QQQM after listing."""
    on = close["QQQ"] > close["QQQ"].rolling(140).mean()
    has_qqqm = "QQQM" in close.columns
    first_qqqm = close["QQQM"].first_valid_index() if has_qqqm else None
    use_qqqm = has_qqqm and first_qqqm is not None
    targets = pd.DataFrame(0.0, index=close.index, columns=["TQQQ", "QQQ", "QQQM", "BIL"])
    targets.loc[on, "TQQQ"] = 0.45
    if use_qqqm:
        pre = on & (close.index < first_qqqm)
        post = on & (close.index >= first_qqqm)
        targets.loc[pre, "QQQ"] = 0.45
        targets.loc[post, "QQQM"] = 0.45
    else:
        targets.loc[on, "QQQ"] = 0.45
    targets.loc[on, "BIL"] = 0.10
    targets.loc[~on, "BIL"] = 1.0
    return targets.shift(1)


def test_strict_and_stress_periods_run_cleanly():
    rows = study.run(_prices(), _trend_targets)["rows"]
    full = rows.loc[rows["period"].eq("full")]
    original = full.loc[full["method"].eq("original"), "cagr"].iloc[0]
    strict = full.loc[full["method"].eq("strict")].sort_values("stress")["cagr"].tolist()
    assert strict[0] <= original + 0.02
    assert all(a >= b - 1e-12 for a, b in itertools.pairwise(strict))
    assert set(rows["period"]) == {"full", "oos"}


def test_summary_flags_balanced_tier_exclusion_and_header():
    output = study.run(_prices(), _trend_targets)
    assert output["summary"]["excluded_on_balanced_tier"] == (abs(output["summary"]["strict_max_drawdown"]) > 0.30)
    report = study.render_markdown(output)
    assert "live_ready=False" in report
    assert "tqqq_growth_income" in report


def test_real_strategy_targets_and_reference_on_synthetic_data():
    prices = _prices()
    output = study.run(prices, study.tqqq_growth_income_targets, reference=study.tqqq_growth_income_reference)
    methods = set(output["rows"]["method"])
    assert methods == {"original", "strict", "original_simulator"}
    assert output["summary"]["baseline"] == "original_simulator"


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
    entry = subprocess.run(
        [sys.executable, "-m", study.STUDY_ID, "--end-date", "2027-06-30", "--output-dir", str(tmp_path / "o")],
        check=False, capture_output=True, text=True, cwd=ROOT,
    )
    assert "rejected: end_date" in entry.stderr + entry.stdout
    env = {k: v for k, v in __import__("os").environ.items() if k not in ("GITHUB_ACTIONS", "GITHUB_REF")}
    local = subprocess.run(
        [sys.executable, "-m", study.STUDY_ID, "--end-date", "2026-11-25", "--output-dir", str(tmp_path / "o")],
        check=False, capture_output=True, text=True, cwd=ROOT, env=env,
    )
    assert "only in the reviewed workflow on main" in local.stderr + local.stdout


def test_missing_session_stops_study_for_tqqq_and_qqqm():
    prices = _prices()
    tqqq_gap = prices["symbol"].eq("TQQQ") & prices["as_of"].eq(pd.Timestamp("2015-03-10"))
    with pytest.raises(ValueError, match="TQQQ"):
        study.run(prices.loc[~tqqq_gap], _trend_targets)
    qqqm_gap = prices["symbol"].eq("QQQM") & prices["as_of"].eq(pd.Timestamp("2022-03-10"))
    with pytest.raises(ValueError, match="QQQM"):
        study.run(prices.loc[~qqqm_gap], _trend_targets)
