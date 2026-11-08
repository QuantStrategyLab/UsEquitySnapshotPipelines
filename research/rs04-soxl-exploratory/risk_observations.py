"""B2 observations from the same realized book, with no sizing effect."""

DRAWDOWN_LIMIT = 0.20


def drawdown_observations(points):
    eligible = opening_observed = opening_crossings = close_crossings = fee_crossings = 0
    sessions_below_floor = 0
    episodes = []
    # A sliced window may start underwater from an earlier, unobserved origin.
    left_censored = points[0]["start"] < points[0]["prior_peak"]
    start = None
    underwater_sessions = 0
    for index, row in enumerate(points):
        floor = (1 - DRAWDOWN_LIMIT) * row["prior_peak"]
        if row["start"] >= floor:
            eligible += 1
            if row["open_nav"] is not None:
                opening_observed += 1
                opening_crossings += row["open_nav"] < floor
            close_crossings += row["prefill"] < floor
        fee_crossings += row["prefill"] >= floor and row["nav"] < floor
        sessions_below_floor += row["nav"] < floor

        if row["nav"] < row["prior_peak"]:
            if not left_censored and start is None:
                start = index
            underwater_sessions += 1
        elif left_censored or start is not None:
            episodes.append({
                "first_underwater": None if left_censored else points[start]["fill"],
                "recovered_on": row["fill"],
                "sessions_to_recovery": None if left_censored else index - start,
                "underwater_sessions_observed": underwater_sessions,
                "left_censored": left_censored, "right_censored": False,
            })
            left_censored, start, underwater_sessions = False, None, 0
    if left_censored or start is not None:
        episodes.append({
            "first_underwater": None if left_censored else points[start]["fill"],
            "recovered_on": None, "sessions_to_recovery": None,
            "underwater_sessions_observed": underwater_sessions,
            "left_censored": left_censored, "right_censored": True,
        })
    return {
        "drawdown_limit": DRAWDOWN_LIMIT,
        "opening_eligible_sessions": eligible,
        "opening_observed_sessions": opening_observed,
        "opening_gap_crossings": opening_crossings if opening_observed else None,
        "prefill_close_crossings": close_crossings,
        "fee_crossings": fee_crossings,
        "post_cost_sessions_below_floor": sessions_below_floor,
        "peak_recovery_episodes": episodes,
    }
