CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS {jobs_table} (
    key TEXT PRIMARY KEY,
    lock_key SERIAL,
    queued BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now()),
    job BYTEA NOT NULL,
    queue TEXT,
    status TEXT,
    scheduled BIGINT,
    expire_at BIGINT
);
"""

CREATE_JOBS_DEQUEUE_INDEX = """
CREATE INDEX IF NOT EXISTS saq_jobs_dequeue_idx ON {jobs_table} (status, queue, scheduled)
"""

CREATE_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS {stats_table} (
    worker_id TEXT PRIMARY KEY,
    stats JSONB,
    expire_at BIGINT
);
"""

DDL_STATEMENTS = [
    CREATE_JOBS_TABLE,
    CREATE_JOBS_DEQUEUE_INDEX,
    CREATE_STATS_TABLE,
]
