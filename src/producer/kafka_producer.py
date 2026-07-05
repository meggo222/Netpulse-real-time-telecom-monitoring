"""
Network Event Simulator — main entry point.

Every CYCLE_SECONDS, iterates over the full device registry, generates one
event per device (with a chance of anomaly injection), and publishes it to
the `network-events` Kafka topic, keyed by device_id.

Run from the project root:
    python src/producer/kafka_producer.py
"""

import json
import logging
import random
import sys
import time
from pathlib import Path

# Allow running this file directly (adds project src/ to the path)
sys.path.append(str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Producer

from common.devices import build_device_registry
from producer.event_generator import DeviceState, generate_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("network-simulator")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # host machine -> container port mapping
TOPIC_NAME = "network-events"
CYCLE_SECONDS = 5


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for key={msg.key()}: {err}")
    # Successful deliveries are intentionally not logged one-by-one to
    # keep the console readable; see the per-cycle summary log instead.


def main():
    devices = build_device_registry()
    states = {d["device_id"]: DeviceState(d) for d in devices}

    log.info(f"Loaded {len(devices)} simulated devices.")
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    rng = random.Random()

    cycle = 0
    try:
        while True:
            cycle += 1
            sent = 0
            anomalies_this_cycle = 0

            for device_id, state in states.items():
                state.maybe_start_anomaly(rng)
                event = generate_event(state)
                state.tick()

                if event["status"] == "DOWN" or event["cpu_usage"] > 90 or \
                   event["memory_usage"] > 90 or event["packet_loss"] > 5 or \
                   event["latency_ms"] > 200 or event["bandwidth_mbps"] > 950:
                    anomalies_this_cycle += 1

                producer.produce(
                    topic=TOPIC_NAME,
                    key=device_id,
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                sent += 1

            producer.poll(0)  # trigger delivery callbacks without blocking
            producer.flush(timeout=5)

            log.info(
                f"Cycle {cycle}: sent {sent} events "
                f"({anomalies_this_cycle} showing anomalous readings)."
            )
            time.sleep(CYCLE_SECONDS)

    except KeyboardInterrupt:
        log.info("Stopping simulator (Ctrl+C received). Flushing remaining messages...")
        producer.flush(timeout=10)
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
