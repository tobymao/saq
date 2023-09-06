from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from saq import Status, Job
from saq.queue import JobError
from saq.testing import TestQueue, TestWorker
from tests.testing import tasks

worker = TestWorker(tasks.settings)


@patch("tests.testing.tasks.queue", new=TestQueue(worker=worker))
class TestSimple(IsolatedAsyncioTestCase):
    async def test_queue(self) -> None:
        job: Job = await tasks.queue.enqueue("add", val1=3, val2=5, timeout=10)  # type: ignore

        self.assertIsInstance(job, Job)

        await worker.process_job(job)

        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, 8)

    async def test_indirect_apply(self) -> None:
        self.assertEqual(await tasks.applies_a_job(), 18)

    async def test_apply_add(self) -> None:
        self.assertEqual(await tasks.queue.apply("add", val1=3, val2=5), 8)

    async def test_apply_boom(self) -> None:
        with self.assertRaisesRegex(JobError, "Boom!"):
            await tasks.queue.apply("boom", value="Boom!")
