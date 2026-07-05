"""
Daily Aggregation DAG — Phase 6.

Runs once daily. For the DAG's logical date (`ds`):
  1. Aggregates raw_network_events + alerts into one row per device in
     daily_monitoring_summary (upsert via ON CONFLICT).
  2. Generates a simple Markdown report summarizing that day.

Tip while developing: don't wait a full day to see this work — open
the Airflow UI, find `telecom_daily_aggregation`, and use the
"Trigger DAG" button to run it immediately against today's data.
"""

from __future__ import annotations

import os
from datetime import timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DB_CONFIG = dict(
    host=os.environ.get("APP_DB_HOST", "postgres_app"),
    port=int(os.environ.get("APP_DB_PORT", 5432)),
    dbname=os.environ.get("APP_DB_NAME", "telecom_monitoring"),
    user=os.environ.get("APP_DB_USER", "telecom_admin"),
    password=os.environ.get("APP_DB_PASSWORD", ""),
)

REPORTS_DIR = "/opt/airflow/reports"

UPSERT_SQL = """
    INSERT INTO daily_monitoring_summary (
        summary_date, device_id, device_type, region,
        total_events, avg_cpu_usage, avg_memory_usage,
        avg_bandwidth_mbps, avg_latency_ms, avg_packet_loss,
        total_alerts, critical_alerts, high_alerts, medium_alerts
    )
    SELECT
        %(ds)s::date AS summary_date,
        e.device_id,
        MAX(e.device_type),
        MAX(e.region),
        COUNT(*),
        AVG(e.cpu_usage),
        AVG(e.memory_usage),
        AVG(e.bandwidth_mbps),
        AVG(e.latency_ms),
        AVG(e.packet_loss),
        COALESCE((SELECT COUNT(*) FROM alerts a
                  WHERE a.device_id = e.device_id
                    AND a.alert_timestamp::date = %(ds)s::date), 0),
        COALESCE((SELECT COUNT(*) FROM alerts a
                  WHERE a.device_id = e.device_id
                    AND a.alert_timestamp::date = %(ds)s::date
                    AND a.severity = 'CRITICAL'), 0),
        COALESCE((SELECT COUNT(*) FROM alerts a
                  WHERE a.device_id = e.device_id
                    AND a.alert_timestamp::date = %(ds)s::date
                    AND a.severity = 'HIGH'), 0),
        COALESCE((SELECT COUNT(*) FROM alerts a
                  WHERE a.device_id = e.device_id
                    AND a.alert_timestamp::date = %(ds)s::date
                    AND a.severity = 'MEDIUM'), 0)
    FROM raw_network_events e
    WHERE e.event_timestamp::date = %(ds)s::date
    GROUP BY e.device_id
    ON CONFLICT (summary_date, device_id) DO UPDATE SET
        total_events        = EXCLUDED.total_events,
        avg_cpu_usage       = EXCLUDED.avg_cpu_usage,
        avg_memory_usage    = EXCLUDED.avg_memory_usage,
        avg_bandwidth_mbps  = EXCLUDED.avg_bandwidth_mbps,
        avg_latency_ms      = EXCLUDED.avg_latency_ms,
        avg_packet_loss     = EXCLUDED.avg_packet_loss,
        total_alerts        = EXCLUDED.total_alerts,
        critical_alerts     = EXCLUDED.critical_alerts,
        high_alerts         = EXCLUDED.high_alerts,
        medium_alerts       = EXCLUDED.medium_alerts,
        computed_at         = now();
"""


def compute_daily_summary(ds, **_):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, {"ds": ds})
            affected = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    print(f"Daily summary computed for {ds}: {affected} device rows upserted.")


def generate_daily_report(ds, **_):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT device_id, region, total_events, total_alerts,
                       critical_alerts, high_alerts, medium_alerts
                FROM daily_monitoring_summary
                WHERE summary_date = %(ds)s::date
                ORDER BY total_alerts DESC;
                """,
                {"ds": ds},
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, f"daily_report_{ds}.md")

    lines = [f"# Daily Network Monitoring Report — {ds}", ""]
    if not rows:
        lines.append("No data recorded for this date.")
    else:
        total_events = sum(r[2] for r in rows)
        total_alerts = sum(r[3] for r in rows)
        lines.append(f"- **Total events:** {total_events}")
        lines.append(f"- **Total alerts:** {total_alerts}")
        lines.append("")
        lines.append("| Device | Region | Events | Alerts | Critical | High | Medium |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in rows:
            lines.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[6]} |")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Report written to {report_path}")


default_args = {
    "owner": "ahmed",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="telecom_daily_aggregation",
    description="Computes daily_monitoring_summary and generates a daily Markdown report.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    tags=["telecom", "monitoring", "aggregation"],
) as dag:

    summary_task = PythonOperator(
        task_id="compute_daily_summary",
        python_callable=compute_daily_summary,
    )

    report_task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
    )

    summary_task >> report_task
