"""
Health Check DAG — Phase 6.

Runs hourly. Verifies:
  1. Kafka broker is accepting connections.
  2. PostgreSQL is reachable and responsive.
  3. Events have actually landed in the last hour (a strong signal the
     producer or the Spark ingestion job silently died, which is easy
     to miss without an automated check like this).
"""

from __future__ import annotations

import os
import socket
from datetime import timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

KAFKA_HOST = "kafka"
KAFKA_PORT = 29092

DB_CONFIG = dict(
    host=os.environ.get("APP_DB_HOST", "postgres_app"),
    port=int(os.environ.get("APP_DB_PORT", 5432)),
    dbname=os.environ.get("APP_DB_NAME", "telecom_monitoring"),
    user=os.environ.get("APP_DB_USER", "telecom_admin"),
    password=os.environ.get("APP_DB_PASSWORD", ""),
)


def check_kafka_connectivity():
    """
    A lightweight TCP-level probe — enough to confirm the broker is
    accepting connections without pulling in a full Kafka client
    library just for a health check.
    """
    with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=5):
        pass
    print(f"Kafka broker reachable at {KAFKA_HOST}:{KAFKA_PORT}")


def check_postgres_connectivity():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
    finally:
        conn.close()
    print("PostgreSQL reachable and responsive.")


def check_recent_events():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw_network_events "
                "WHERE ingested_at > now() - interval '1 hour';"
            )
            count = cur.fetchone()[0]
    finally:
        conn.close()

    print(f"{count} events ingested in the last hour.")
    if count == 0:
        raise RuntimeError(
            "No network events ingested in the last hour — "
            "the producer or the Spark ingestion job may be down."
        )


default_args = {
    "owner": "ahmed",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="telecom_pipeline_health_check",
    description="Hourly health check for Kafka, PostgreSQL, and pipeline liveness.",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["telecom", "monitoring", "health-check"],
) as dag:

    kafka_check = PythonOperator(
        task_id="check_kafka_connectivity",
        python_callable=check_kafka_connectivity,
    )

    postgres_check = PythonOperator(
        task_id="check_postgres_connectivity",
        python_callable=check_postgres_connectivity,
    )

    recent_events_check = PythonOperator(
        task_id="check_recent_events",
        python_callable=check_recent_events,
    )

    [kafka_check, postgres_check] >> recent_events_check
