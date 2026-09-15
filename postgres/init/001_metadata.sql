CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.pipeline_execution (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    records_processed BIGINT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_pipeline_execution
        UNIQUE (pipeline_name, execution_id),

    CONSTRAINT ck_pipeline_execution_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    CONSTRAINT ck_pipeline_execution_records
        CHECK (
            records_processed IS NULL
            OR records_processed >= 0
        ),

    CONSTRAINT ck_pipeline_execution_dates
        CHECK (
            finished_at IS NULL
            OR finished_at >= started_at
        )
);