import unittest
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

from qqq_compare import Event
from qqq_residual import locate_residuals


class ResidualTests(unittest.TestCase):
    def setUp(self):
        first = date(2026, 2, 1)
        self.issuer = [Event(first + timedelta(days=i), first + timedelta(days=i + 3), Decimal("0.125"))
                       for i in range(20)]
        self.archive = list(self.issuer)
        for i, delta in ((2, 1), (6, -1)):
            self.archive[i] = replace(self.issuer[i], payable_date=self.issuer[i].payable_date + timedelta(days=delta))
        self.archive.append(Event(first + timedelta(days=24), first + timedelta(days=27), Decimal("0.3330")))

    def test_only_three_locators_are_returned_in_deterministic_order(self):
        result = locate_residuals(self.issuer[::-1], self.archive[::-1])
        self.assertEqual(result, {
            "status": "QQQ_RESIDUALS_LOCATED",
            "payable_date_differences": [
                {"ex_date": "2026-02-03", "issuer_payable_date": "2026-02-06",
                 "archive_payable_date": "2026-02-07", "archive_minus_issuer_days": 1},
                {"ex_date": "2026-02-07", "issuer_payable_date": "2026-02-10",
                 "archive_payable_date": "2026-02-09", "archive_minus_issuer_days": -1},
            ],
            "archive_unmatched": {
                "ex_date": "2026-02-25", "payable_date": "2026-02-28", "amount_usd_per_share": "0.3330",
            },
        })

    def test_changed_counts_amount_or_pairing_never_returns_locators(self):
        amount_changed = [replace(self.archive[2], amount=Decimal("0.126")), *self.archive[:2], *self.archive[3:]]
        ambiguous = [*self.archive, replace(self.archive[2], amount=Decimal("0.126"))]
        for archive in (self.archive[:-1], amount_changed, ambiguous):
            with self.subTest(size=len(archive)):
                self.assertEqual(locate_residuals(self.issuer, archive),
                                 {"status": "PARKED", "reason_code": "QQQ_PRIOR_RESULT_NOT_REPRODUCED"})
