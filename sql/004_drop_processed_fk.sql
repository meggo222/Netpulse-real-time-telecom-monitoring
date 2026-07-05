-- =============================================================================
-- Fix: processed_network_events.event_id had a Foreign Key referencing
-- raw_network_events.event_id. Since the two tables are populated by
-- two INDEPENDENT Spark Structured Streaming queries (each with its own
-- micro-batch trigger), there's no guarantee the raw row commits before
-- the processed row tries to insert — causing FK violations that fail
-- the processed_network_events write silently.
--
-- Fix: drop the FK. The device_id -> devices(device_id) relationship
-- already ties both tables to the same dimension, which is what
-- actually matters for querying/joining in Grafana later.
-- =============================================================================

ALTER TABLE processed_network_events
    DROP CONSTRAINT IF EXISTS processed_network_events_event_id_fkey;
