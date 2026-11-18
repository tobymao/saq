import json
import unittest
from unittest.mock import AsyncMock

from saq.job import Job, Status
from saq.queue.http import HttpQueue


class TestHttpQueue(unittest.IsolatedAsyncioTestCase):
    async def test_iter_jobs(self) -> None:
        queue = HttpQueue(url="http://test.example.com")
        jobs = [Job("a", queue=queue), Job("b", queue=queue)]
        queue._send = AsyncMock(return_value=json.dumps([job.to_dict() for job in jobs]))  # type: ignore[method-assign]

        result = [job async for job in queue.iter_jobs(statuses=[Status.QUEUED])]

        self.assertEqual([job.key for job in result], [job.key for job in jobs])
        queue._send.assert_awaited_once_with("iter_jobs", statuses=[Status.QUEUED], batch_size=100)
