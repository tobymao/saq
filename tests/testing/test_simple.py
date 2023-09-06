from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from saq import Job, Status
from saq.testing import TestQueue
from tests.testing import tasks


@patch("tests.testing.tasks.queue", new=TestQueue())
class TestSimple(IsolatedAsyncioTestCase):
    async def test_queue(self) -> None:
        job: Job = await tasks.queue.enqueue("add", val1=3, val2=5, timeout=10)  # type: ignore

        self.assertIsInstance(job, Job)
        self.assertEqual(job.function, "add")
        self.assertEqual(job.timeout, 10)
        self.assertEqual(job.status, Status.QUEUED)
        self.assertEqual(job.kwargs, {"val1": 3, "val2": 5})

    async def test_apply_fail(self) -> None:
        with self.assertRaises(AssertionError):
            await tasks.queue.apply("add", val1=3, val2=5)

    async def test_indirect_apply_fail(self) -> None:
        with self.assertRaises(AssertionError):
            await tasks.applies_a_job()

    async def test_indirect_enqueue(self) -> None:
        job = await tasks.enqueues_a_job()
        self.assertIsInstance(job, Job)
