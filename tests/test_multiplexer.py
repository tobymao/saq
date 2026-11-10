from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from saq.queue.redis import PubSubMultiplexer

# asyncio.sleep is patched in the test below, so the fake pubsub keeps a real one to yield with
real_sleep = asyncio.sleep


class FailingPubSub:
    """A pubsub whose connection is down, so every read fails."""

    def __init__(self) -> None:
        self.get_message_calls = 0
        self.psubscribe = mock.AsyncMock()
        self.punsubscribe = mock.AsyncMock()

    async def get_message(self, **_kwargs: object) -> None:
        self.get_message_calls += 1
        await real_sleep(0)
        raise ConnectionError("connection is down")


class TestPubSubMultiplexer(unittest.IsolatedAsyncioTestCase):
    async def test_pauses_after_error(self) -> None:
        pubsub = FailingPubSub()
        multiplexer = PubSubMultiplexer(pubsub, "saq:")  # type: ignore[arg-type]
        self.addAsyncCleanup(multiplexer.close)
        delays: list[float] = []

        async def sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 2:
                # stand in for a close() landing during the pause
                raise asyncio.CancelledError

        with (
            mock.patch("asyncio.sleep", sleep),
            mock.patch("saq.queue.redis.logger") as logger,
        ):
            await multiplexer.start()
            task = multiplexer._daemon_task
            assert task is not None
            await asyncio.wait({task}, timeout=1)

            # a pause of one second after each failure, and the loop keeps going
            self.assertEqual(delays, [1, 1])
            self.assertEqual(pubsub.get_message_calls, 2)
            self.assertEqual(logger.exception.call_count, 2)

        self.assertTrue(task.done())
        # cancelling during the pause is not caught, unlike cancelling during a read
        self.assertTrue(task.cancelled())
