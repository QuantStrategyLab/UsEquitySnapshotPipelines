import unittest
from datetime import date
from decimal import Decimal

from qqq_compare import Event, compare


class CompareTests(unittest.TestCase):
    def event(self, day=1, payable=2, amount="1.00", classification=None):
        return Event(date(2026, 1, day), date(2026, 2, payable), Decimal(amount), classification)

    def test_exact_multiset_preserves_duplicates_and_uncertainty(self):
        event = self.event()
        result = compare([event, event], [event, event, event])
        self.assertEqual(result["exact_matches"], 2)
        self.assertEqual(result["classification_uncertain"], 2)
        self.assertEqual(result["archive_unmatched"], 1)
        self.assertEqual(result["issuer_unmatched"], 0)

    def test_residual_pair_counts_each_difference(self):
        result = compare([self.event(classification="income")],
                         [self.event(payable=3, amount="1.001", classification="capital")])
        self.assertEqual(result["residual_pairs"], 1)
        self.assertEqual(result["payable_date_differences"], 1)
        self.assertEqual(result["amount_differences"], 1)
        self.assertEqual(result["classification_differences"], 1)
        self.assertEqual(result["classification_uncertain"], 0)

    def test_missing_classification_is_uncertain(self):
        result = compare([self.event()], [self.event(classification="income")])
        self.assertEqual(result["classification_uncertain"], 1)
        self.assertEqual(result["classification_differences"], 0)

    def test_exact_pairs_removed_before_residual_pair_and_order_irrelevant(self):
        common = self.event(classification="income")
        left = [common, self.event(amount="2", classification="income")]
        right = [self.event(amount="3", classification="income"), common]
        result = compare(left, right)
        self.assertEqual(result, compare(left[::-1], right[::-1]))
        self.assertEqual(result["exact_matches"], 1)
        self.assertEqual(result["residual_pairs"], 1)
        self.assertEqual(result["amount_differences"], 1)

    def test_ambiguous_residuals_remain_unmatched(self):
        left = [self.event(amount="1"), self.event(amount="2")]
        right = [self.event(amount="3"), self.event(amount="4")]
        result = compare(left, right)
        self.assertEqual(result["ambiguous_groups"], 1)
        self.assertEqual(result["issuer_unmatched"], 2)
        self.assertEqual(result["archive_unmatched"], 2)
        self.assertEqual(result["residual_pairs"], 0)
        self.assertEqual(result["amount_differences"], 0)

    def test_one_sided_date_not_ambiguous(self):
        result = compare([self.event(), self.event()], [])
        self.assertEqual(result["issuer_unmatched"], 2)
        self.assertEqual(result["ambiguous_groups"], 0)


if __name__ == "__main__":
    unittest.main()
