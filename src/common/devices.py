"""
Device Registry — defines the fixed set of simulated network devices.

Each device keeps its own baseline metric ranges so the simulation feels
consistent over time (a router in Cairo behaves like *that* router every
cycle, not like random noise every time).
"""

import random

# Egyptian telecom regions used for realism across the whole project
REGIONS = ["Cairo", "Giza", "Alexandria", "Sharqia", "Mansoura", "Aswan"]

DEVICE_TYPE_PREFIX = {
    "router": "RTR",
    "switch": "SWT",
    "firewall": "FWL",
    "base_station": "BST",
}

# How many of each device type to simulate. Total = 26 devices.
DEVICE_TYPE_COUNTS = {
    "router": 8,
    "switch": 8,
    "firewall": 4,
    "base_station": 6,
}

REGION_CODE = {
    "Cairo": "CAI",
    "Giza": "GIZ",
    "Alexandria": "ALX",
    "Sharqia": "SHR",
    "Mansoura": "MNS",
    "Aswan": "ASW",
}


def build_device_registry(seed: int = 42):
    """
    Builds a deterministic list of device definitions. Deterministic (seeded)
    so device_id's stay stable across simulator restarts, which matters once
    we have a `devices` dimension table in PostgreSQL (Phase 5) that these
    IDs need to match.
    """
    rng = random.Random(seed)
    devices = []

    for device_type, count in DEVICE_TYPE_COUNTS.items():
        prefix = DEVICE_TYPE_PREFIX[device_type]
        for i in range(1, count + 1):
            region = rng.choice(REGIONS)
            region_code = REGION_CODE[region]
            device_id = f"{prefix}-{region_code}-{i:03d}"

            devices.append({
                "device_id": device_id,
                "device_type": device_type,
                "region": region,
                # Baseline ("healthy") ranges — the generator wobbles
                # around these values each cycle.
                "baseline_cpu": rng.uniform(20, 45),
                "baseline_memory": rng.uniform(30, 55),
                "baseline_bandwidth": rng.uniform(200, 900),
                "baseline_latency": rng.uniform(5, 40),
                "baseline_temperature": rng.uniform(35, 50),
            })

    return devices


if __name__ == "__main__":
    # Quick manual check: run `python devices.py` to print the registry.
    for d in build_device_registry():
        print(d)
