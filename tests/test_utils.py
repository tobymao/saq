import unittest

from saq.utils import exponential_backoff


class TestUtils(unittest.IsolatedAsyncioTestCase):
    async def test_exponential_backoff(self) -> None:
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=0, max_delay=0, jitter=False),
            0,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=1, max_delay=0, jitter=False),
            0,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=1, base_delay=1, max_delay=1, jitter=False),
            1,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=2, base_delay=1, max_delay=10, jitter=False),
            2,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=3, base_delay=1, max_delay=10, jitter=False),
            4,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=4, base_delay=1, max_delay=10, jitter=False),
            8,
        )
        self.assertAlmostEqual(
            exponential_backoff(attempts=5, base_delay=1, max_delay=10, jitter=False),
            10,
        )

        backoff = exponential_backoff(
            attempts=1,
            base_delay=1,
            max_delay=1,
            jitter=True,
        )
        self.assertTrue(0 <= backoff < 1)
