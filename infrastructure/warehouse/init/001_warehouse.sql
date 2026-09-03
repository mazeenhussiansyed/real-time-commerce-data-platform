CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS raw.bronze_events (
    event_id TEXT PRIMARY KEY,
    ingestion_run_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    operation CHAR(1) NOT NULL
        CHECK (operation IN ('r', 'c', 'u', 'd')),
    source_timestamp_ms BIGINT,
    connector_timestamp_ms BIGINT,
    source_lsn BIGINT,
    source_transaction_id BIGINT,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    key_json TEXT,
    before_json TEXT,
    after_json TEXT,
    payload_json TEXT,
    value_json TEXT NOT NULL,
    schema_name TEXT,
    invalid_reason TEXT,
    source_table TEXT NOT NULL,
    event_date DATE NOT NULL,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT bronze_events_kafka_position_unique
        UNIQUE (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
);

CREATE INDEX IF NOT EXISTS bronze_events_source_table_idx
    ON raw.bronze_events (source_table);

CREATE INDEX IF NOT EXISTS bronze_events_operation_idx
    ON raw.bronze_events (operation);

CREATE INDEX IF NOT EXISTS bronze_events_event_timestamp_idx
    ON raw.bronze_events (event_timestamp);

CREATE INDEX IF NOT EXISTS bronze_events_event_date_idx
    ON raw.bronze_events (event_date);

CREATE TABLE IF NOT EXISTS audit.warehouse_load_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (
            status IN (
                'running',
                'succeeded',
                'failed'
            )
        ),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    source_record_count BIGINT NOT NULL DEFAULT 0,
    inserted_record_count BIGINT NOT NULL DEFAULT 0,
    existing_record_count BIGINT NOT NULL DEFAULT 0,
    failed_record_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS warehouse_load_runs_status_idx
    ON audit.warehouse_load_runs (status);

CREATE INDEX IF NOT EXISTS warehouse_load_runs_started_at_idx
    ON audit.warehouse_load_runs (started_at);
