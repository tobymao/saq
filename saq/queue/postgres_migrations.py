import typing as t

from psycopg.sql import Identifier, SQL, Composed
from textwrap import dedent


def get_migrations(
    jobs_table: Identifier,
    stats_table: Identifier,
) -> t.List[t.Tuple[int, t.List[Composed]]]:
    return [
        (
            1,
            [
                SQL(
                    dedent("""
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
""")
                ).format(jobs_table=jobs_table),
                SQL(
                    dedent("""
CREATE INDEX IF NOT EXISTS saq_jobs_dequeue_idx ON {jobs_table} (status, queue, scheduled);
""")
                ).format(jobs_table=jobs_table),
                SQL(
                    dedent("""
CREATE TABLE IF NOT EXISTS {stats_table} (
    worker_id TEXT PRIMARY KEY,
    stats JSONB,
    expire_at BIGINT
);
""")
                ).format(stats_table=stats_table),
            ],
        ),
        (
            2,
            [
                SQL(
                    dedent("""
ALTER TABLE {stats_table} ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE {stats_table} ADD COLUMN IF NOT EXISTS queue_key TEXT;
CREATE INDEX IF NOT EXISTS saq_stats_expire_at_idx ON {stats_table} (expire_at);
CREATE INDEX IF NOT EXISTS saq_stats_queue_key_idx ON {stats_table} (queue_key);
        """)
                ).format(stats_table=stats_table),
            ],
        ),
    ]
