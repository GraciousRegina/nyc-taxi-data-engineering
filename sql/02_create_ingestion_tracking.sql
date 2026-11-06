
CREATE TABLE IF NOT EXISTS public.ingestion_tracking (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    status VARCHAR(20) NOT NULL
        CHECK (status IN ('running', 'completed', 'failed')),
    row_count BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year, month)
);
