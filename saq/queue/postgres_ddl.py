CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS {jobs_table} (
    key TEXT PRIMARY KEY,
    lock_key SERIAL NOT NULL,
    job BYTEA NOT NULL,
    queue TEXT NOT NULL,
    status TEXT NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    group_key TEXT,
    scheduled BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
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
    metadata JSONB,
    expire_at BIGINT
);
"""

CREATE_WORKER_METADATA_TABLE = """
CREATE TABLE IF NOT EXISTS {worker_metadata_table} (
    worker_id TEXT PRIMARY KEY,
    queue_key TEXT NOT NULL,
    expire_at BIGINT NOT NULL,
    ip_address TEXT,
    metadata JSONB 
);
"""

DDL_STATEMENTS = [
    CREATE_JOBS_TABLE,
    CREATE_JOBS_DEQUEUE_INDEX,
    CREATE_STATS_TABLE,
    CREATE_WORKER_METADATA_TABLE,
]
