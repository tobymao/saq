import unittest

from psycopg.sql import Identifier

from saq.queue.postgres_migrations import get_migrations


class TestMigrations(unittest.TestCase):
    def test_migration_versions_are_increasing(self) -> None:
        migrations = get_migrations(
            Identifier("jobs_table"),
            Identifier("stats_table"),
        )
        versions = [migration[0] for migration in migrations]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))

    def test_highest_migration_equal_or_smaller_than_length(self) -> None:
        migrations = get_migrations(
            Identifier("jobs_table"),
            Identifier("stats_table"),
        )
        versions = [migration[0] for migration in migrations]
        assert max(versions) <= len(migrations)
