from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from saq import Status, Job
from saq.testing import TestQueue, TestWorker
from tests.testing.tasks import enqueues_a_job, settings, enqueues_a_job_that_retries


class TestAssertions(IsolatedAsyncioTestCase):
    async def test_indirect_enqueue(self) -> None:
        with patch("tests.testing.tasks.queue", new=TestQueue()) as testqueue:
            job = await enqueues_a_job()

            testqueue.assertEnqueuedTimes("add", 1)
            testqueue.assertNotEnqueued("boom")
            testqueue.assertNotRetried("add")

            self.assertEqual([job], testqueue.getEnqueued("add"))

    async def test_indirect_enqueue_with_kwargs(self) -> None:
        with patch("tests.testing.tasks.queue", new=TestQueue()) as testqueue:
            await enqueues_a_job()

            testqueue.assertEnqueuedTimes("add", 1, kwargs={"val1": 7, "val2": 11})
            testqueue.assertNotEnqueued("boom")
            testqueue.assertNotRetried("add")

    async def test_testqueue_both_settings_worker(self) -> None:
        worker = TestWorker(settings)
        with self.assertRaises(AssertionError):
            TestQueue(settings=settings, worker=worker)

    async def test_failed_enequeued_times(self) -> None:
        with patch("tests.testing.tasks.queue", new=TestQueue()) as testqueue:
            await enqueues_a_job()

            with self.assertRaises(AssertionError):
                testqueue.assertNotEnqueued("add")
            with self.assertRaises(AssertionError):
                testqueue.assertEnqueuedTimes("add", 2)
            with self.assertRaises(AssertionError):
                testqueue.assertEnqueuedTimes("boom", 1)
            with self.assertRaises(AssertionError):
                testqueue.assertRetriedTimes("add", 1)

    async def test_enqueue_retry(self) -> None:
        worker = TestWorker(settings)
        with patch(
            "tests.testing.tasks.queue", new=TestQueue(worker=worker)
        ) as testqueue:
            job: Job = await enqueues_a_job_that_retries()
            self.assertIsNotNone(job)
            self.assertEqual(job.status, Status.QUEUED)
            testqueue.assertEnqueuedTimes("boom", 1)
            testqueue.assertNotRetried("boom")

            with self.assertRaises(AssertionError):
                testqueue.assertRetriedTimes("boom", 1)

            await worker.process_job(job)

            self.assertEqual(job.status, Status.QUEUED)
            testqueue.assertRetriedTimes("boom", 1)
            testqueue.assertRetriedTimes("boom", 1, kwargs={"value": "For retrying"})

            with self.assertRaises(AssertionError):
                testqueue.assertNotRetried("boom")

            await worker.process_job(job)
            testqueue.assertRetriedTimes("boom", 2)

            await job.abort("Gave up")
            self.assertEqual(job.status, Status.ABORTED)
