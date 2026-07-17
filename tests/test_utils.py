import asyncio
import time
import unittest

from saq.utils import exponential_backoff, wait_for


class TestUtils(unittest.IsolatedAsyncioTestCase):
    async def test_exponential_backoff(self) -> None:
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=0, max_delay=0, jitter=False), 0
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=1, max_delay=0, jitter=False), 0
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=1, max_delay=1, jitter=False), 1
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=2, base_delay=1, max_delay=10, jitter=False), 2
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=3, base_delay=1, max_delay=10, jitter=False), 4
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=4, base_delay=1, max_delay=10, jitter=False), 8
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=5, base_delay=1, max_delay=10, jitter=False),
            10,
        )

        backoff = exponential_backoff(attempts=1, base_delay=1, max_delay=1, jitter=True)
        self.assertTrue(0 <= backoff < 1)

    async def test_wait_for(self) -> None:
        async def value() -> int:
            return 42

        self.assertEqual(await wait_for(value(), 1), 42)

        async def boom() -> None:
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            await wait_for(boom(), 1)

    async def test_wait_for_swallowed_cancellation(self) -> None:
        """wait_for must not hang when the task swallows its first cancellation.

        Before Python 3.12, a nested asyncio.wait_for racing a completing future
        swallows the cancellation that stdlib asyncio.wait_for sends on timeout
        (bpo-42130), which deadlocked Queue.listen via stdlib wait_for.
        """

        async def swallows_first_cancel() -> None:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                pass
            await asyncio.sleep(30)

        start = time.monotonic()
        with self.assertRaises(asyncio.TimeoutError):
            await wait_for(swallows_first_cancel(), 0.01)
        self.assertLess(time.monotonic() - start, 5)
