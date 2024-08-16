CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS {jobs_table} (
    key TEXT PRIMARY KEY,
    lock_id BIGSERIAL,
    queued BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now()),
    job JSONB,
    queue TEXT,
    status TEXT,
    scheduled BIGINT,
    ttl BIGINT
);
"""

CREATE_JOBS_DEQUEUE_INDEX = """
CREATE INDEX IF NOT EXISTS saq_jobs_dequeue_idx ON {jobs_table} (status, queue, scheduled)
"""
