"""Prometheus metrics for the investigator. Scraped at GET /metrics."""

from prometheus_client import Counter, Gauge, Histogram

CASES_OPENED = Counter("sentinel_cases_opened_total", "New cases opened")
FLAGS_ATTACHED = Counter("sentinel_case_flags_attached_total", "Flags folded into an already-open case")
RECOMMENDATIONS = Counter("sentinel_recommendations_total", "Recommendations written", ["action", "source"])
GROUNDING_WARNINGS = Counter("sentinel_grounding_warnings_total", "LLM rationales quoting numbers not in the evidence")
TOOL_ERRORS = Counter("sentinel_specialist_errors_total", "Specialists that could not gather evidence", ["specialist"])
DECISIONS = Counter("sentinel_decisions_total", "Analyst decisions", ["decision", "agreed"])
INVESTIGATION_SECONDS = Histogram("sentinel_investigation_seconds", "Case opened -> waiting for an analyst",
                                  buckets=(0.5, 1, 2, 5, 10, 20, 40, 80))
CASES_BY_STATUS = Gauge("sentinel_cases", "Cases by status", ["status"])
