"""
Alert Rules — single source of truth for the six anomaly-detection
thresholds used by the Alert Detection Engine.

Kept as plain, Spark-agnostic Python data so:
  1. The rules are readable/auditable by a non-Spark person (e.g. a
     network operations lead reviewing "what counts as an alert").
  2. New rules can be added by appending to THRESHOLD_RULES without
     touching any Spark/streaming code.

NOTE on bandwidth: the simulator produces bandwidth_mbps as a raw
throughput reading, not a percentage. "Bandwidth utilization > 95%"
(per the business requirement) is computed against an assumed link
capacity — see BANDWIDTH_CAPACITY_MBPS below.
"""

# Assumed maximum link capacity for every simulated device. In a real
# telecom deployment this would come from each device's actual
# provisioned capacity (stored in the device registry / inventory
# system) rather than a single global constant.
BANDWIDTH_CAPACITY_MBPS = 1000.0

# Each rule: the column to evaluate, the threshold that triggers it,
# the alert_type/severity to record, and a Java-format description
# template (used with Spark's format_string, hence %s / %.2f syntax
# instead of Python's .format()).
THRESHOLD_RULES = [
    {
        "column": "cpu_usage",
        "threshold": 90.0,
        "alert_type": "CPU_HIGH",
        "severity": "HIGH",
        "description_template": "CPU usage on %s reached %.2f%%, exceeding the 90%% threshold.",
    },
    {
        "column": "memory_usage",
        "threshold": 90.0,
        "alert_type": "MEMORY_HIGH",
        "severity": "HIGH",
        "description_template": "Memory usage on %s reached %.2f%%, exceeding the 90%% threshold.",
    },
    {
        "column": "packet_loss",
        "threshold": 5.0,
        "alert_type": "PACKET_LOSS_HIGH",
        "severity": "HIGH",
        "description_template": "Packet loss on %s reached %.2f%%, exceeding the 5%% threshold.",
    },
    {
        "column": "latency_ms",
        "threshold": 200.0,
        "alert_type": "LATENCY_HIGH",
        "severity": "MEDIUM",
        "description_template": "Latency on %s reached %.2f ms, exceeding the 200 ms threshold.",
    },
    {
        "column": "bandwidth_utilization_pct",
        "threshold": 95.0,
        "alert_type": "BANDWIDTH_SATURATION",
        "severity": "MEDIUM",
        "description_template": "Bandwidth utilization on %s reached %.2f%%, exceeding the 95%% threshold.",
    },
]

# DEVICE_DOWN is an equality check (status == "DOWN"), not a numeric
# threshold, so it's handled as its own case in the job.
DEVICE_DOWN_ALERT = {
    "alert_type": "DEVICE_DOWN",
    "severity": "CRITICAL",
    "description_template": "Device %s is reporting DOWN status.",
}
