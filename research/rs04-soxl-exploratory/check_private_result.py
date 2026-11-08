import json
import unittest
from private_result import project_result


def fixture():
    period = {
        "from": "2024-04-05",
        "to": "2026-07-10",
        "sessions": 567,
        "report": {
            "cumulative_net_return": 0.2,
            "annualized_net_volatility": 0.3,
            "metrics": [
                {
                    "name": "net_cagr",
                    "status": "COMPUTED",
                    "value": 0.1,
                    "reason_code": None,
                    "details": {"raw_quote": "SECRET"},
                }
            ],
            "execution": {"turnover_one_side": 2.0, "commission_usd": 4.0, "slippage_usd": 5.0, "total_cost_usd": 9.0},
        },
        "drawdown_observations": {"drawdown_limit": 0.2, "peak_recovery_episodes": [{"first_underwater": "SECRET"}]},
        "max_drawdown_with_path_peak": -0.2,
        "points": ["SECRET"],
    }
    scenario = {
        "scenario_id": "SOXL/B1.4/registered/7bps",
        "profile": "SOXL",
        "arm": "B1.4",
        "variant": "registered",
        "fees": (2.0, 5.0),
        "ewma_decay": 0.94,
        "target_annual_vol": 0.2,
        "status": "FAILED",
        "reason_code": "private exception SECRET",
        "completed_sessions": 567,
        "failed_session": "SECRET",
        "folds": [period],
        "main": period,
        "tail": None,
        "orders": ["SECRET"],
    }
    pair = {
        "profile": "SOXL",
        "candidate": "B1.4",
        "control": "B1.2",
        "fee_bps": 7,
        "status": "PARKED",
        "positive_increment_supported": False,
        "reason_code": "PAIRED_PATH_MISSING",
        "mean_daily_net_log_delta": None,
        "ci95": None,
        "ci98_75": None,
    }
    return {
        "scenarios": [scenario],
        "primary": [pair],
        "descriptive_tail": [],
        "d10": {
            "basis": "one_way_per_security_net_traded_notional_including_boxx",
            "components": ("commission_bps", "slippage_including_spread_bps"),
            "primary": (2.0, 5.0),
            "report_only": ((0.0, 0.0), (5.0, 10.0)),
            "fund_expenses": "embedded_in_etf_returns",
            "financing": "no_borrowing",
            "extra": "SECRET",
        },
        "provenance": {
            "source_sha": "a" * 40,
            "manifest_sha256": "b" * 64,
            "issuer_csv_sha256": "c" * 64,
            "plan_sha256": "d" * 64,
            "archive_source_binding": "e" * 64,
            "calendar_version": "4.11.1",
            "path": "SECRET",
        },
    }


class ProjectionTest(unittest.TestCase):
    def test_aggregate_and_failed_status_only(self):
        result = project_result(fixture())
        self.assertNotIn("SECRET", json.dumps(result, allow_nan=False))
        self.assertEqual(result["scenarios"][0]["status"], "FAILED")
        self.assertEqual(result["scenarios"][0]["reason_code"], "OTHER_FAILURE")
        self.assertEqual(result["scenarios"][0]["main"]["metrics"][0]["value"], 0.1)
        self.assertEqual(result["primary"][0]["reason_code"], "PAIRED_PATH_MISSING")
        self.assertFalse(result["scope"]["strict_pit_verified"])

    def test_invalid_inputs_are_rejected(self):
        for change in ("identity", "number", "provenance", "d10"):
            sample = fixture()
            if change == "identity":
                sample["scenarios"][0]["scenario_id"] = "SECRET"
            elif change == "number":
                sample["scenarios"][0]["main"]["report"]["cumulative_net_return"] = float("nan")
            elif change == "provenance":
                sample["provenance"]["calendar_version"] = "secret/path"
            else:
                sample["d10"]["primary"] = (1, 5)
            with self.subTest(change=change), self.assertRaises(ValueError):
                project_result(sample)


if __name__ == "__main__":
    unittest.main()
