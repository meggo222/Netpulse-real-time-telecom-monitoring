"""
Raw Ingestion Job — Phase 3 (updated in Phase 5 to also write to PostgreSQL).

Reads the `network-events` Kafka topic continuously, parses each message
against the shared NETWORK_EVENT_SCHEMA, and:
  1. Prints a live preview to the console (for local verification).
  2. Persists the parsed events as Parquet under a local "bronze" layer.
  3. Writes verbatim rows into PostgreSQL `raw_network_events`.
  4. Writes an enriched row (bandwidth %, soft health classification)
     into PostgreSQL `processed_network_events`.

Run with (from inside the spark-master container):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.cores.max=1 \
      /opt/spark-apps/raw_ingestion_job.py
"""

import sys
from functools import partial
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, lit

from schemas import NETWORK_EVENT_SCHEMA
from alert_rules import BANDWIDTH_CAPACITY_MBPS
from postgres_sink import write_batch_to_postgres

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # internal Docker network address
TOPIC_NAME = "network-events"

BRONZE_PATH = "/opt/spark-apps/data/bronze/network_events"
CHECKPOINT_PATH = "/opt/spark-apps/checkpoints/raw_ingestion"
CHECKPOINT_PATH_PG_RAW = "/opt/spark-apps/checkpoints/pg_raw_events"
CHECKPOINT_PATH_PG_PROCESSED = "/opt/spark-apps/checkpoints/pg_processed_events"

# "Soft" warning thresholds — deliberately lower than the Phase 4 hard
# alert thresholds, so processed_network_events.health_status can flag
# a device trending toward trouble before it actually breaches an
# alert rule.
WARNING_CPU = 80.0
WARNING_MEMORY = 80.0
WARNING_PACKET_LOSS = 3.0
WARNING_LATENCY = 150.0
WARNING_BANDWIDTH_PCT = 85.0


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TelecomRawIngestion")
        .config("spark.sql.shuffle.partitions", "3")  # matches our 3 Kafka partitions
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Spark's default INFO logging is too noisy for this use case

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka gives us key/value as bytes; decode value as a UTF-8 JSON string,
    # then parse it against our known schema.
    parsed = (
        raw_stream
        .selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_value")
        .withColumn("data", from_json(col("json_value"), NETWORK_EVENT_SCHEMA))
        .select("kafka_key", "data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn(
            "bandwidth_utilization_pct",
            (col("bandwidth_mbps") / lit(BANDWIDTH_CAPACITY_MBPS)) * 100,
        )
        .withColumn(
            "health_status",
            when(col("status") == "DOWN", "CRITICAL")
            .when(
                (col("cpu_usage") > WARNING_CPU)
                | (col("memory_usage") > WARNING_MEMORY)
                | (col("packet_loss") > WARNING_PACKET_LOSS)
                | (col("latency_ms") > WARNING_LATENCY)
                | (col("bandwidth_utilization_pct") > WARNING_BANDWIDTH_PCT),
                "WARNING",
            )
            .otherwise("HEALTHY"),
        )
        .drop("timestamp")
    )

    # --- Sink 1: console, for live human verification while developing ---
    console_query = (
        parsed.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Sink 2: Parquet bronze layer (kept for archival / reprocessing) ---
    parquet_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Sink 3: PostgreSQL raw_network_events (verbatim columns only) ---
    raw_for_postgres = parsed.select(
        "event_id", "event_timestamp", "device_id", "device_type", "region",
        "cpu_usage", "memory_usage", "bandwidth_mbps", "packet_loss",
        "latency_ms", "temperature", "status",
    )
    pg_raw_query = (
        raw_for_postgres.writeStream
        .foreachBatch(partial(write_batch_to_postgres, table_name="raw_network_events"))
        .option("checkpointLocation", CHECKPOINT_PATH_PG_RAW)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Sink 4: PostgreSQL processed_network_events (enriched columns only) ---
    processed_for_postgres = parsed.select(
        "event_id", "event_timestamp", "device_id", "device_type", "region",
        "bandwidth_utilization_pct", "health_status",
    )
    pg_processed_query = (
        processed_for_postgres.writeStream
        .foreachBatch(partial(write_batch_to_postgres, table_name="processed_network_events"))
        .option("checkpointLocation", CHECKPOINT_PATH_PG_PROCESSED)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    monitor_streams([console_query, parquet_query, pg_raw_query, pg_processed_query])


def monitor_streams(queries):
    """
    Polls all active streaming queries and immediately surfaces the
    exception of any query that dies — Spark's default behavior lets a
    failed sink die silently while other sinks keep running, which is
    exactly what caused the "no rows in PostgreSQL, but console still
    printing" bug during development. This makes failures loud instead.
    """
    import time

    reported = set()
    try:
        while any(q.isActive for q in queries):
            for q in queries:
                if not q.isActive and q.name not in reported and q.exception() is not None:
                    reported.add(q.name)
                    print(f"\n\n!!! STREAM '{q.name}' FAILED !!!\n{q.exception()}\n\n", flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        for q in queries:
            q.stop()


if __name__ == "__main__":
    main()
