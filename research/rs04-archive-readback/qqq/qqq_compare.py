"""Compare issuer and archive distribution events without exposing values."""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class Event:
    ex_date: date
    payable_date: date
    amount: Decimal
    classification: str | None = None


def compare(issuer: list[Event], archive: list[Event]) -> dict:
    """Compare multisets; classify only unambiguous residual pairs.

    Classification must use the same explicitly defined taxonomy on both sides.
    Missing classifications are counted as uncertain for each compared pair.
    """
    counts = {
        "exact_matches": 0,
        "residual_pairs": 0,
        "payable_date_differences": 0,
        "amount_differences": 0,
        "classification_differences": 0,
        "classification_uncertain": 0,
        "issuer_unmatched": 0,
        "archive_unmatched": 0,
        "ambiguous_groups": 0,
    }
    issuer_counts, archive_counts = Counter(issuer), Counter(archive)
    for event in issuer_counts.keys() & archive_counts.keys():
        matched = min(issuer_counts[event], archive_counts[event])
        counts["exact_matches"] += matched
        if event.classification is None:
            counts["classification_uncertain"] += matched
        issuer_counts[event] -= matched
        archive_counts[event] -= matched

    issuer_by_date = defaultdict(list)
    archive_by_date = defaultdict(list)
    for event, count in issuer_counts.items():
        issuer_by_date[event.ex_date].extend([event] * count)
    for event, count in archive_counts.items():
        archive_by_date[event.ex_date].extend([event] * count)

    for ex_date in issuer_by_date.keys() | archive_by_date.keys():
        left, right = issuer_by_date[ex_date], archive_by_date[ex_date]
        if len(left) == len(right) == 1:
            first, second = left[0], right[0]
            counts["residual_pairs"] += 1
            counts["payable_date_differences"] += first.payable_date != second.payable_date
            counts["amount_differences"] += first.amount != second.amount
            if first.classification is None or second.classification is None:
                counts["classification_uncertain"] += 1
            else:
                counts["classification_differences"] += first.classification != second.classification
        else:
            counts["issuer_unmatched"] += len(left)
            counts["archive_unmatched"] += len(right)
            if left and right:
                counts["ambiguous_groups"] += 1
    return counts
