CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS {jobs_table} (
    key TEXT PRIMARY KEY,
    lock_id BIGSERIAL,
    queued BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now()),
    job JSONB,
    queue TEXT,
    status TEXT,
    scheduled BIGINT
);
"""
