from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from saq.queue.redis import PubSubMultiplexer


class FailingPubSub:
    """A pubsub whose connection is down, so every read fails."""

    def __init__(self) -> None:
        self.get_message_calls = 0
        self.psubscribe = mock.AsyncMock()
        self.punsubscribe = mock.AsyncMock()

    async def get_message(self, **_kwargs: object) -> None:
        self.get_message_calls += 1
        await asyncio.sleep(0)
        raise ConnectionError("connection is down")


class TestPubSubMultiplexer(unittest.IsolatedAsyncioTestCase):
    async def test_pauses_after_error(self) -> None:
        pubsub = FailingPubSub()
        multiplexer = PubSubMultiplexer(pubsub, "saq:")  # type: ignore[arg-type]
        self.addAsyncCleanup(multiplexer.close)

        with mock.patch("saq.queue.redis.logger") as logger:
            await multiplexer.start()
            task = multiplexer._daemon_task
            assert task is not None
            await asyncio.sleep(0.3)

            # without a pause between failures this is thousands of calls
            self.assertLessEqual(pubsub.get_message_calls, 3)
            self.assertGreaterEqual(pubsub.get_message_calls, 1)
            self.assertEqual(logger.exception.call_count, pubsub.get_message_calls)
            self.assertFalse(task.done())

            await multiplexer.close()
            await asyncio.wait({task}, timeout=1)

        self.assertTrue(task.done())
        self.assertTrue(task.cancelled() or task.exception() is None)
