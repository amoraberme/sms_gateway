from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

QUEUE_DEPTH = Gauge(
    "sms_queue_depth",
    "Current number of queued messages in Redis per priority tier",
    ["priority"],
)

CARRIER_SUBMITS_TOTAL = Counter(
    "sms_carrier_submits_total",
    "Total carrier /submit HTTP attempts by carrier and status outcome",
    ["carrier", "status_code"],
)

DLR_DELAY_SECONDS = Histogram(
    "sms_dlr_delay_seconds",
    "Delay between carrier submission and final DLR processing in seconds",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0),
)

WEBHOOK_DELIVERIES_TOTAL = Counter(
    "sms_webhook_deliveries_total",
    "Total outbound tenant webhook delivery attempts by outcome",
    ["status"],
)
