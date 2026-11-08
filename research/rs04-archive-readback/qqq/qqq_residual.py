"""Build the bounded locator payload for an approved private consumer."""

from collections import Counter

from qqq_compare import Event, compare


EXPECTED_COUNTS = {
    "exact_matches": 18,
    "residual_pairs": 2,
    "payable_date_differences": 2,
    "amount_differences": 0,
    "classification_differences": 0,
    "classification_uncertain": 20,
    "issuer_unmatched": 0,
    "archive_unmatched": 1,
    "ambiguous_groups": 0,
}


def locate_residuals(issuer: list[Event], archive: list[Event]) -> dict:
    """Return at most three locator records after reproducing the prior result."""
    if compare(issuer, archive) != EXPECTED_COUNTS:
        return {"status": "PARKED", "reason_code": "QQQ_PRIOR_RESULT_NOT_REPRODUCED"}

    issuer_counts, archive_counts = Counter(issuer), Counter(archive)
    public = {event.ex_date: event for event in (issuer_counts - archive_counts).elements()}
    private = list((archive_counts - issuer_counts).elements())
    paired = sorted((event for event in private if event.ex_date in public), key=lambda event: event.ex_date)
    extra, = (event for event in private if event.ex_date not in public)
    return {
        "status": "QQQ_RESIDUALS_LOCATED",
        "payable_date_differences": [{
            "ex_date": event.ex_date.isoformat(),
            "issuer_payable_date": public[event.ex_date].payable_date.isoformat(),
            "archive_payable_date": event.payable_date.isoformat(),
            "archive_minus_issuer_days": (event.payable_date - public[event.ex_date].payable_date).days,
        } for event in paired],
        "archive_unmatched": {
            "ex_date": extra.ex_date.isoformat(),
            "payable_date": extra.payable_date.isoformat(),
            "amount_usd_per_share": format(extra.amount, "f"),
        },
    }
