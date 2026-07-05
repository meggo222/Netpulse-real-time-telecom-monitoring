"""
Shared Spark schema for network events.

Defining this once and importing it everywhere (raw ingestion, alerting,
future aggregation jobs) keeps every job reading Kafka in perfect
agreement about field names and types — a schema drift between jobs is
a classic source of silent data bugs in streaming pipelines.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

NETWORK_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),  # parsed to TimestampType downstream
    StructField("device_id", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("region", StringType(), nullable=False),
    StructField("cpu_usage", DoubleType(), nullable=True),
    StructField("memory_usage", DoubleType(), nullable=True),
    StructField("bandwidth_mbps", DoubleType(), nullable=True),
    StructField("packet_loss", DoubleType(), nullable=True),
    StructField("latency_ms", DoubleType(), nullable=True),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])
