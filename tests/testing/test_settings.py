from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from saq.queue import JobError
from saq.testing import TestQueue
from tests.testing import tasks


@patch("tests.testing.tasks.queue", new=TestQueue(settings=tasks.settings))
class TestSimple(IsolatedAsyncioTestCase):
    async def test_indirect_apply(self) -> None:
        self.assertEqual(await tasks.applies_a_job(), 18)

    async def test_apply_add(self) -> None:
        self.assertEqual(await tasks.queue.apply("add", val1=5, val2=7), 12)

    async def test_apply_boom(self) -> None:
        with self.assertRaisesRegex(JobError, "Boom!"):
            await tasks.queue.apply("boom", value="Boom!")
