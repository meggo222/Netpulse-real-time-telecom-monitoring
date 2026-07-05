-- =============================================================================
-- Real-Time Telecom Network Monitoring & Alerting System
-- Phase 5: PostgreSQL Schema
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. DEVICES (dimension table)
-- Every event / alert references a device here. Seeded once from the
-- simulator's device registry (see 002_seed_devices.sql) so the set of
-- device_id's is guaranteed to match what the simulator actually sends.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS devices (
    device_id       VARCHAR(20) PRIMARY KEY,
    device_type     VARCHAR(20) NOT NULL,
    region          VARCHAR(30) NOT NULL,
    current_status  VARCHAR(10) NOT NULL DEFAULT 'UP',
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_devices_region ON devices(region);
CREATE INDEX IF NOT EXISTS idx_devices_type   ON devices(device_type);


-- -----------------------------------------------------------------------------
-- 2. RAW_NETWORK_EVENTS
-- Verbatim copy of every event consumed from Kafka. Written by
-- raw_ingestion_job.py. This is the system's "source of truth" bronze
-- layer — nothing here is ever modified after insert.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_network_events (
    event_id         UUID PRIMARY KEY,
    event_timestamp  TIMESTAMPTZ NOT NULL,
    device_id        VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type      VARCHAR(20) NOT NULL,
    region           VARCHAR(30) NOT NULL,
    cpu_usage        DOUBLE PRECISION,
    memory_usage     DOUBLE PRECISION,
    bandwidth_mbps   DOUBLE PRECISION,
    packet_loss      DOUBLE PRECISION,
    latency_ms       DOUBLE PRECISION,
    temperature      DOUBLE PRECISION,
    status           VARCHAR(10),
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_device_time ON raw_network_events(device_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_time        ON raw_network_events(event_timestamp);


-- -----------------------------------------------------------------------------
-- 3. PROCESSED_NETWORK_EVENTS
-- Enriched version of each raw event: adds computed bandwidth utilization
-- and a soft "traffic light" health classification (lighter thresholds
-- than the hard alert rules) — useful for trend dashboards that want to
-- see degradation building up BEFORE it becomes a full alert.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_network_events (
    event_id                   UUID PRIMARY KEY REFERENCES raw_network_events(event_id),
    event_timestamp            TIMESTAMPTZ NOT NULL,
    device_id                  VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type                VARCHAR(20) NOT NULL,
    region                     VARCHAR(30) NOT NULL,
    bandwidth_utilization_pct  DOUBLE PRECISION,
    health_status              VARCHAR(10) NOT NULL DEFAULT 'HEALTHY', -- HEALTHY / WARNING / CRITICAL
    processed_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_device_time ON processed_network_events(device_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_events_health      ON processed_network_events(health_status);


-- -----------------------------------------------------------------------------
-- 4. ALERTS
-- One row per rule violated per event (see Phase 4 design decision).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id         UUID PRIMARY KEY,
    alert_timestamp  TIMESTAMPTZ NOT NULL,
    device_id        VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type      VARCHAR(20) NOT NULL,
    region           VARCHAR(30) NOT NULL,
    alert_type       VARCHAR(30) NOT NULL,
    severity         VARCHAR(10) NOT NULL,
    description      TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_device_time ON alerts(device_id, alert_timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_severity     ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_region       ON alerts(region);
CREATE INDEX IF NOT EXISTS idx_alerts_type         ON alerts(alert_type);


-- -----------------------------------------------------------------------------
-- 5. DAILY_MONITORING_SUMMARY
-- One row per (day, device) — populated by an Airflow DAG in Phase 6,
-- not by the streaming jobs. Composite PK doubles as the natural
-- "upsert key" for a daily batch aggregation job.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_monitoring_summary (
    summary_date        DATE NOT NULL,
    device_id           VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type         VARCHAR(20) NOT NULL,
    region               VARCHAR(30) NOT NULL,
    total_events         INTEGER NOT NULL DEFAULT 0,
    avg_cpu_usage         DOUBLE PRECISION,
    avg_memory_usage      DOUBLE PRECISION,
    avg_bandwidth_mbps    DOUBLE PRECISION,
    avg_latency_ms        DOUBLE PRECISION,
    avg_packet_loss       DOUBLE PRECISION,
    total_alerts          INTEGER NOT NULL DEFAULT 0,
    critical_alerts       INTEGER NOT NULL DEFAULT 0,
    high_alerts           INTEGER NOT NULL DEFAULT 0,
    medium_alerts         INTEGER NOT NULL DEFAULT 0,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (summary_date, device_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date   ON daily_monitoring_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_daily_summary_region ON daily_monitoring_summary(region);
