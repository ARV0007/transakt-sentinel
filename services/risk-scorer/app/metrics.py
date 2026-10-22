"""Prometheus metrics for the scorer. Scraped at GET /metrics."""

from prometheus_client import Counter, Gauge, Histogram

SCORED = Counter("sentinel_payments_scored_total", "Payments scored", ["flagged"])
REASONS = Counter("sentinel_flag_reasons_total", "Reason codes on flagged payments", ["reason"])
MODEL_SECONDS = Histogram("sentinel_model_seconds", "Time inside the model's predict call",
                          buckets=(0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.05))
EVENT_LAG = Histogram("sentinel_event_lag_seconds", "Payment settled (Kafka timestamp) -> scored",
                      buckets=(0.5, 1, 2, 5, 10, 30, 60, 300))
REPLAYS = Counter("sentinel_replayed_events_total", "Kafka redeliveries skipped as already scored")
MODEL_INFO = Gauge("sentinel_model_info", "Loaded model (value is always 1)", ["version", "source", "state"])
