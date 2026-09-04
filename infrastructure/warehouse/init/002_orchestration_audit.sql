CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    backfill_start_date DATE,
    backfill_end_date DATE,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    duration_seconds NUMERIC(12, 3),
    failed_task_id TEXT,
    error_message TEXT,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pipeline_runs_mode_check
        CHECK (run_mode IN ('incremental', 'backfill')),

    CONSTRAINT pipeline_runs_status_check
        CHECK (status IN ('running', 'success', 'failed')),

    CONSTRAINT pipeline_runs_backfill_window_check
        CHECK (
            (
                run_mode = 'incremental'
                AND backfill_start_date IS NULL
                AND backfill_end_date IS NULL
            )
            OR
            (
                run_mode = 'backfill'
                AND backfill_start_date IS NOT NULL
                AND backfill_end_date IS NOT NULL
                AND backfill_start_date <= backfill_end_date
            )
        ),

    CONSTRAINT pipeline_runs_completion_check
        CHECK (
            (
                status = 'running'
                AND completed_at IS NULL
            )
            OR
            (
                status IN ('success', 'failed')
                AND completed_at IS NOT NULL
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_started
    ON audit.pipeline_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_backfill_window
    ON audit.pipeline_runs (
        backfill_start_date,
        backfill_end_date
    )
    WHERE run_mode = 'backfill';

COMMENT ON TABLE audit.pipeline_runs IS
    'Run-level audit history for Airflow incremental and backfill pipelines.';

COMMENT ON COLUMN audit.pipeline_runs.run_id IS
    'Unique Airflow DAG run identifier used for idempotent audit updates.';

COMMENT ON COLUMN audit.pipeline_runs.details IS
    'Additional verified pipeline metrics stored as JSON.';
