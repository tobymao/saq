CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS {jobs_table} (
    key TEXT PRIMARY KEY,
    queued BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now()),
    function TEXT,
    job JSONB,
    queue TEXT,
    status TEXT,
    scheduled BIGINT
);
"""
