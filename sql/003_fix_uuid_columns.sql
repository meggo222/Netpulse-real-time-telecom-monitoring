-- =============================================================================
-- Fix: event_id / alert_id as UUID caused silent JDBC write failures from
-- Spark (StringType -> UUID column type mismatch on the PostgreSQL JDBC
-- driver). Switching to VARCHAR(36) keeps the same guaranteed-unique
-- identifier without the driver compatibility issue.
--
-- Safe to run: these three tables are currently empty (only `devices`
-- has data, and this script does not touch `devices`).
-- =============================================================================

DROP TABLE IF EXISTS processed_network_events;
DROP TABLE IF EXISTS raw_network_events;
DROP TABLE IF EXISTS alerts;

CREATE TABLE raw_network_events (
    event_id         VARCHAR(36) PRIMARY KEY,
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

CREATE INDEX idx_raw_events_device_time ON raw_network_events(device_id, event_timestamp);
CREATE INDEX idx_raw_events_time        ON raw_network_events(event_timestamp);

CREATE TABLE processed_network_events (
    event_id                   VARCHAR(36) PRIMARY KEY REFERENCES raw_network_events(event_id),
    event_timestamp            TIMESTAMPTZ NOT NULL,
    device_id                  VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type                VARCHAR(20) NOT NULL,
    region                     VARCHAR(30) NOT NULL,
    bandwidth_utilization_pct  DOUBLE PRECISION,
    health_status              VARCHAR(10) NOT NULL DEFAULT 'HEALTHY',
    processed_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_processed_events_device_time ON processed_network_events(device_id, event_timestamp);
CREATE INDEX idx_processed_events_health      ON processed_network_events(health_status);

CREATE TABLE alerts (
    alert_id         VARCHAR(36) PRIMARY KEY,
    alert_timestamp  TIMESTAMPTZ NOT NULL,
    device_id        VARCHAR(20) NOT NULL REFERENCES devices(device_id),
    device_type      VARCHAR(20) NOT NULL,
    region           VARCHAR(30) NOT NULL,
    alert_type       VARCHAR(30) NOT NULL,
    severity         VARCHAR(10) NOT NULL,
    description      TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alerts_device_time ON alerts(device_id, alert_timestamp);
CREATE INDEX idx_alerts_severity     ON alerts(severity);
CREATE INDEX idx_alerts_region       ON alerts(region);
CREATE INDEX idx_alerts_type         ON alerts(alert_type);
