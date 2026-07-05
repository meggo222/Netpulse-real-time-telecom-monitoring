"""
Alert Detection Job — Phase 4 (updated in Phase 5 to also write to PostgreSQL).

Reads the same `network-events` Kafka topic as the raw ingestion job,
evaluates every event against the six threshold rules defined in
alert_rules.py, and emits ONE alert row per rule violated (a device
breaching 3 rules at once produces 3 separate alert rows — see the
Phase 4 design discussion for why). Each alert is written to the
console (for live viewing) and to the PostgreSQL `alerts` table.

Run with (from inside the spark-master container):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.cores.max=1 \
      /opt/spark-apps/alert_detection_job.py
"""

import sys
from functools import partial
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

from schemas import NETWORK_EVENT_SCHEMA
from alert_rules import THRESHOLD_RULES, DEVICE_DOWN_ALERT, BANDWIDTH_CAPACITY_MBPS
from postgres_sink import write_batch_to_postgres

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "network-events"

ALERTS_PATH = "/opt/spark-apps/data/silver/alerts"
CHECKPOINT_PATH = "/opt/spark-apps/checkpoints/alert_detection"
CHECKPOINT_PATH_PG = "/opt/spark-apps/checkpoints/pg_alerts"

ALERT_STRUCT_TYPE = StructType([
    StructField("alert_type", StringType()),
    StructField("severity", StringType()),
    StructField("description", StringType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TelecomAlertDetection")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def parse_events(spark: SparkSession):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        # Alerts are about what's happening NOW, so unlike the raw
        # ingestion job (which replays full history), we only care
        # about events produced after this job starts.
        .option("startingOffsets", "latest")
        .load()
    )

    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .withColumn("data", F.from_json(F.col("json_value"), NETWORK_EVENT_SCHEMA))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn(
            "bandwidth_utilization_pct",
            (F.col("bandwidth_mbps") / F.lit(BANDWIDTH_CAPACITY_MBPS)) * 100,
        )
        .drop("timestamp")
    )


def build_candidate_alert_columns():
    """
    One `when(condition, struct(...))` expression per rule. Rows that
    don't breach a given rule get NULL for that rule's struct — nulls
    are filtered out after assembling them into an array.
    """
    candidates = []

    for rule in THRESHOLD_RULES:
        condition = F.col(rule["column"]) > F.lit(rule["threshold"])
        description = F.format_string(
            rule["description_template"],
            F.col("device_id"),
            F.col(rule["column"]),
        )
        candidates.append(
            F.when(condition, F.struct(
                F.lit(rule["alert_type"]).alias("alert_type"),
                F.lit(rule["severity"]).alias("severity"),
                description.alias("description"),
            ))
        )

    # DEVICE_DOWN: equality check, not a numeric threshold
    down_condition = F.col("status") == F.lit("DOWN")
    down_description = F.format_string(
        DEVICE_DOWN_ALERT["description_template"],
        F.col("device_id"),
    )
    candidates.append(
        F.when(down_condition, F.struct(
            F.lit(DEVICE_DOWN_ALERT["alert_type"]).alias("alert_type"),
            F.lit(DEVICE_DOWN_ALERT["severity"]).alias("severity"),
            down_description.alias("description"),
        ))
    )

    return candidates


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events = parse_events(spark)
    candidate_alert_cols = build_candidate_alert_columns()

    alerts = (
        events
        .withColumn("candidate_alerts", F.array(*candidate_alert_cols))
        .withColumn("alert", F.explode(F.expr("filter(candidate_alerts, x -> x is not null)")))
        .select(
            F.expr("uuid()").alias("alert_id"),
            F.col("event_timestamp").alias("alert_timestamp"),
            F.col("device_id"),
            F.col("device_type"),
            F.col("region"),
            F.col("alert.alert_type").alias("alert_type"),
            F.col("alert.severity").alias("severity"),
            F.col("alert.description").alias("description"),
        )
    )

    console_query = (
        alerts.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    parquet_query = (
        alerts.writeStream
        .format("parquet")
        .option("path", ALERTS_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    pg_alerts_query = (
        alerts.writeStream
        .foreachBatch(partial(write_batch_to_postgres, table_name="alerts"))
        .option("checkpointLocation", CHECKPOINT_PATH_PG)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    monitor_streams([console_query, parquet_query, pg_alerts_query])


def monitor_streams(queries):
    """See raw_ingestion_job.py for why this exists."""
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
