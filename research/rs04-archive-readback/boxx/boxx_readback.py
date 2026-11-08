"""Compare the single fixed BOXX event with its public cash amount."""
import os
from datetime import date
from decimal import Decimal

from soxl_reconcile import archive_events


MEMBERS = ("registration.json", "manifest.json", "actions/BOXX/page-001.json")
EX_DATE = date(2024, 8, 13)
PAYABLE_DATE = date(2024, 8, 14)
CBOE_AMOUNT = Decimal("0.290590")
OLD_REFERENCE = Decimal("0.29")
RESULTS = {
    "BOXX_MATCH_CBOE_EXACT",
    "BOXX_DIFFERENT_EXACT_SAME_CENT",
    "BOXX_DIFFERENT_CENT",
}


def compare_boxx(client, data, exact_read):
    bucket = client.bucket(data["input_root"].split("/")[2])
    prefix = f'research/v2/input/{data["dataset"]}/'
    remaining = iter(MEMBERS)

    def read_member(name, reference):
        if name != next(remaining, None):
            raise ValueError("BOXX_READ_SCOPE_MISMATCH")
        return exact_read(bucket, prefix + name, reference)

    [(ex_date, payable_date, amount)] = archive_events(
        data, read_member, symbols=("BOXX",)
    )["BOXX"]["cash"]
    if (ex_date, payable_date) != (EX_DATE, PAYABLE_DATE) or next(remaining, None) is not None:
        return "BOXX_EVENT_MISMATCH"
    if amount == OLD_REFERENCE:
        return "BOXX_PRIOR_RECEIPT_MISMATCH"
    if amount == CBOE_AMOUNT:
        return "BOXX_MATCH_CBOE_EXACT"
    # This half-open interval rounds to USD 0.29 at cent precision, half up.
    if Decimal("0.285") <= amount < Decimal("0.295"):
        return "BOXX_DIFFERENT_EXACT_SAME_CENT"
    return "BOXX_DIFFERENT_CENT"


def main():
    code = "BOXX_RUNNER_NOT_APPROVED"
    if os.environ.get("RUNNER_OS") == "Linux" and os.environ.get("RUNNER_TEMP"):
        try:
            from google.cloud import storage
            from inspect_gcs import binding, exact_read

            data = binding()
            code = compare_boxx(storage.Client(project=data["project_id"]), data, exact_read)
        except Exception:
            # SDK and parser exceptions may include object names or raw fields.
            code = "BOXX_READ_STOPPED"
    print(code)
    return 0 if code in RESULTS else 2


if __name__ == "__main__":
    raise SystemExit(main())
