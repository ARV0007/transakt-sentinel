"""
Pure functions over lists of Transakt payments. No network, no MCP — which is
exactly why they live here: every number an agent sees comes from this file,
and this file can be tested with plain lists.
"""

from statistics import median

from .transakt_client import parse_ts


def max_in_sliding_window(timestamps, window_s):
    """Largest number of events inside ANY window of `window_s` seconds."""
    ts = sorted(timestamps)
    best, left = 0, 0
    for right in range(len(ts)):
        while ts[right] - ts[left] > window_s:
            left += 1
        best = max(best, right - left + 1)
    return best


def activity_summary(payments, window_minutes):
    """How busy a merchant has been, and whether it came in one burst."""
    times = [parse_ts(p["createdAt"]).timestamp() for p in payments]
    failed = sum(1 for p in payments if p.get("status") == "FAILED")
    total = len(payments)
    per_min = total / window_minutes if window_minutes else 0.0
    return {
        "window_minutes": window_minutes,
        "payments": total,
        "failed": failed,
        "failed_ratio": round(failed / total, 3) if total else 0.0,
        "avg_per_minute": round(per_min, 2),
        "max_in_any_2_minutes": max_in_sliding_window(times, 120),
        "first_at": min(payments, key=lambda p: p["createdAt"])["createdAt"] if payments else None,
        "last_at": max(payments, key=lambda p: p["createdAt"])["createdAt"] if payments else None,
    }


def amount_profile(payments, exclude_payment_id=None):
    """A merchant's normal ticket size, so one payment can be compared against it."""
    amounts = sorted(p["amountPaise"] for p in payments
                     if p.get("id") != exclude_payment_id and p.get("amountPaise") is not None)
    if not amounts:
        return {"payments": 0, "median_paise": None, "p95_paise": None, "max_paise": None}
    p95 = amounts[min(len(amounts) - 1, int(round(0.95 * (len(amounts) - 1))))]
    return {
        "payments": len(amounts),
        "median_paise": int(median(amounts)),
        "p95_paise": p95,
        "max_paise": amounts[-1],
    }
