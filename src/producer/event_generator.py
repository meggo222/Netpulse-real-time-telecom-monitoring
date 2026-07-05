"""
Event Generator — turns a device's baseline profile into a realistic,
slightly-noisy metric snapshot each cycle, with occasional injected
anomalies so the downstream alerting engine (Phase 4) has real signal
to detect.
"""

import random
import uuid
from datetime import datetime, timezone

# Probability that any given device experiences an anomaly THIS cycle.
# Kept low so alerts feel like real rare events, not constant noise.
ANOMALY_PROBABILITY = 0.06

# Once a device goes into an anomaly, it stays "unhealthy" for a few
# cycles in a row (mirrors real incidents: they don't resolve in 1 tick).
ANOMALY_DURATION_CYCLES = 3

ANOMALY_TYPES = [
    "cpu_spike",
    "memory_spike",
    "packet_loss_spike",
    "latency_spike",
    "bandwidth_saturation",
    "device_down",
]


class DeviceState:
    """Tracks whether a device is currently mid-anomaly across cycles."""

    def __init__(self, device):
        self.device = device
        self.anomaly_type = None
        self.anomaly_cycles_left = 0

    def maybe_start_anomaly(self, rng: random.Random):
        if self.anomaly_cycles_left == 0 and rng.random() < ANOMALY_PROBABILITY:
            self.anomaly_type = rng.choice(ANOMALY_TYPES)
            self.anomaly_cycles_left = ANOMALY_DURATION_CYCLES

    def tick(self):
        if self.anomaly_cycles_left > 0:
            self.anomaly_cycles_left -= 1
            if self.anomaly_cycles_left == 0:
                self.anomaly_type = None


def _wobble(value: float, spread: float, lo: float, hi: float) -> float:
    """Small random walk around a baseline value, clamped to [lo, hi]."""
    result = value + random.uniform(-spread, spread)
    return max(lo, min(hi, result))


def generate_event(state: DeviceState) -> dict:
    """
    Produces one event dict for a device, applying its baseline +
    normal noise, and overlaying anomaly effects if the device is
    currently in an anomalous state.
    """
    d = state.device

    cpu_usage = _wobble(d["baseline_cpu"], 5, 5, 100)
    memory_usage = _wobble(d["baseline_memory"], 5, 5, 100)
    bandwidth_mbps = _wobble(d["baseline_bandwidth"], 40, 10, 1000)
    packet_loss = max(0.0, random.uniform(0, 0.5))
    latency_ms = _wobble(d["baseline_latency"], 5, 1, 300)
    temperature = _wobble(d["baseline_temperature"], 2, 20, 100)
    status = "UP"

    anomaly = state.anomaly_type
    if anomaly == "cpu_spike":
        cpu_usage = random.uniform(91, 100)
    elif anomaly == "memory_spike":
        memory_usage = random.uniform(91, 100)
    elif anomaly == "packet_loss_spike":
        packet_loss = random.uniform(5.5, 15)
    elif anomaly == "latency_spike":
        latency_ms = random.uniform(210, 450)
    elif anomaly == "bandwidth_saturation":
        bandwidth_mbps = random.uniform(960, 1000)
    elif anomaly == "device_down":
        status = "DOWN"
        cpu_usage = 0.0
        memory_usage = 0.0
        bandwidth_mbps = 0.0
        packet_loss = 100.0
        latency_ms = 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "device_id": d["device_id"],
        "device_type": d["device_type"],
        "region": d["region"],
        "cpu_usage": round(cpu_usage, 2),
        "memory_usage": round(memory_usage, 2),
        "bandwidth_mbps": round(bandwidth_mbps, 2),
        "packet_loss": round(packet_loss, 2),
        "latency_ms": round(latency_ms, 2),
        "temperature": round(temperature, 2),
        "status": status,
    }
