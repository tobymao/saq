import unittest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from aiohttp import ClientResponseError, ClientError, ServerTimeoutError

from saq.queue.http import HttpQueue


class TestHttpRetry(unittest.IsolatedAsyncioTestCase):
    """Test HttpQueue retry functionality."""

    async def asyncSetUp(self) -> None:
        """Set up test fixtures."""
        self.queue = HttpQueue(
            url="http://test.example.com",
            max_retries=3,
            retry_delay=0.1,
            retry_backoff=2.0,
            retry_jitter=False,  # Disable jitter for predictable tests
        )
        # Mock the session to avoid actual HTTP calls
        self.queue.session = MagicMock()

    async def asyncTearDown(self) -> None:
        """Clean up test fixtures."""
        # No need to close mock session
        pass

    def create_response_mock(self, response_text="success", should_raise=None):
        """Helper to create a properly mocked aiohttp response."""
        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value=response_text)
        # raise_for_status is a synchronous method, not async
        mock_response.raise_for_status = Mock()
        if should_raise:
            mock_response.raise_for_status.side_effect = should_raise
        else:
            mock_response.raise_for_status.return_value = None

        # Create async context manager
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        return mock_cm

    async def test_retry_on_client_timeout(self) -> None:
        """Test retry behavior on ServerTimeoutError exception."""
        # Mock the post request to fail twice, then succeed
        success_response = self.create_response_mock("success")

        self.queue.session.post.side_effect = [
            ServerTimeoutError("Timeout 1"),
            ServerTimeoutError("Timeout 2"),
            success_response,
        ]

        with patch("asyncio.sleep") as mock_sleep:
            result = await self.queue._send("enqueue", job="test")

            # Should succeed after 2 retries
            self.assertEqual(result, "success")
            # Should have made 3 attempts total
            self.assertEqual(self.queue.session.post.call_count, 3)
            # Should have slept twice (after first two failures)
            self.assertEqual(mock_sleep.call_count, 2)
            # Check exponential backoff: 0.1s, then 0.2s
            actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
            expected_delays = [0.1, 0.2]
            self.assertEqual(actual_delays, expected_delays)

    async def test_retry_on_server_error(self) -> None:
        """Test retry behavior on 5xx server errors."""
        # Mock 500 internal server error
        server_error = ClientResponseError(
            request_info=Mock(), history=(), status=500, message="Internal Server Error"
        )

        success_response = self.create_response_mock("success")

        self.queue.session.post.side_effect = [server_error, success_response]

        with patch("asyncio.sleep") as mock_sleep:
            result = await self.queue._send("finish", job="test")

            # Should succeed after 1 retry
            self.assertEqual(result, "success")
            self.assertEqual(self.queue.session.post.call_count, 2)
            self.assertEqual(mock_sleep.call_count, 1)

    async def test_no_retry_on_client_error(self) -> None:
        """Test no retry behavior on 4xx client errors."""
        # Mock 404 not found error
        client_error = ClientResponseError(
            request_info=Mock(), history=(), status=404, message="Not Found"
        )

        self.queue.session.post.side_effect = client_error

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ClientResponseError):
                await self.queue._send("enqueue", job="test")

            # Should not retry on 4xx errors
            self.assertEqual(self.queue.session.post.call_count, 1)
            self.assertEqual(mock_sleep.call_count, 0)

    async def test_dequeue_never_retries(self) -> None:
        """Test that dequeue operations never retry."""
        # Mock timeout that would normally trigger retry
        timeout_error = ServerTimeoutError()
        self.queue.session.post.side_effect = timeout_error

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ServerTimeoutError):
                await self.queue._send("dequeue", timeout=5)

            # Should not retry dequeue operations
            self.assertEqual(self.queue.session.post.call_count, 1)
            self.assertEqual(mock_sleep.call_count, 0)

    async def test_max_retries_exhausted(self) -> None:
        """Test behavior when max retries are exhausted."""
        # Mock continuous timeouts
        self.queue.session.post.side_effect = ServerTimeoutError()

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ServerTimeoutError):
                await self.queue._send("enqueue", job="test")

            # Should attempt max_retries + 1 times (4 total)
            self.assertEqual(self.queue.session.post.call_count, 4)
            # Should sleep 3 times (after first 3 failures)
            self.assertEqual(mock_sleep.call_count, 3)

    async def test_exponential_backoff_timing(self) -> None:
        """Test exponential backoff delay calculation."""
        # Mock continuous failures
        self.queue.session.post.side_effect = ServerTimeoutError()

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ServerTimeoutError):
                await self.queue._send("enqueue", job="test")

            # Check exponential backoff progression
            expected_delays = [0.1, 0.2, 0.4]  # base=0.1, backoff=2.0
            actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
            self.assertEqual(actual_delays, expected_delays)

    async def test_custom_retry_configuration(self) -> None:
        """Test custom retry configuration."""
        custom_queue = HttpQueue(
            url="http://test.example.com",
            max_retries=1,
            retry_delay=0.5,
            retry_backoff=3.0,
            retry_jitter=False,
        )
        custom_queue.session = MagicMock()
        custom_queue.session.post.side_effect = ServerTimeoutError()

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ServerTimeoutError):
                await custom_queue._send("enqueue", job="test")

            # Should attempt max_retries + 1 times (2 total)
            self.assertEqual(custom_queue.session.post.call_count, 2)
            # Should sleep once with custom delay
            self.assertEqual(mock_sleep.call_count, 1)
            mock_sleep.assert_called_with(0.5)

    async def test_retry_disabled(self) -> None:
        """Test retry behavior when max_retries=0."""
        no_retry_queue = HttpQueue(url="http://test.example.com", max_retries=0, retry_delay=0.1)
        no_retry_queue.session = MagicMock()
        no_retry_queue.session.post.side_effect = ServerTimeoutError()

        with patch("asyncio.sleep") as mock_sleep:
            with self.assertRaises(ServerTimeoutError):
                await no_retry_queue._send("enqueue", job="test")

            # Should attempt only once
            self.assertEqual(no_retry_queue.session.post.call_count, 1)
            # Should not sleep
            self.assertEqual(mock_sleep.call_count, 0)

    async def test_success_on_first_attempt(self) -> None:
        """Test no retry when operation succeeds on first attempt."""
        success_response = self.create_response_mock("success")
        self.queue.session.post.return_value = success_response

        with patch("asyncio.sleep") as mock_sleep:
            result = await self.queue._send("enqueue", job="test")

            # Should succeed immediately
            self.assertEqual(result, "success")
            self.assertEqual(self.queue.session.post.call_count, 1)
            self.assertEqual(mock_sleep.call_count, 0)

    async def test_retry_with_jitter(self) -> None:
        """Test retry behavior with jitter enabled."""
        jitter_queue = HttpQueue(
            url="http://test.example.com", max_retries=2, retry_delay=1.0, retry_jitter=True
        )
        jitter_queue.session = MagicMock()
        jitter_queue.session.post.side_effect = ServerTimeoutError()

        with patch("asyncio.sleep") as mock_sleep, patch(
            "saq.utils.random", return_value=0.5
        ):  # Mock random for predictable jitter
            with self.assertRaises(ServerTimeoutError):
                await jitter_queue._send("enqueue", job="test")

            # Should have added jitter (delays * 0.5)
            expected_delays = [0.5, 1.0]  # base * 2^attempt * jitter
            actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
            self.assertEqual(actual_delays, expected_delays)

    async def test_should_retry_on_exception_classification(self) -> None:
        """Test exception classification for retry decisions."""
        # Test retryable exceptions
        self.assertTrue(self.queue._should_retry_on_exception(ServerTimeoutError()))
        self.assertTrue(self.queue._should_retry_on_exception(ClientError("connection error")))

        # Test 5xx responses (retryable)
        server_error = ClientResponseError(
            request_info=Mock(), history=(), status=500, message="Internal Server Error"
        )
        self.assertTrue(self.queue._should_retry_on_exception(server_error))

        # Test 4xx responses (not retryable)
        client_error = ClientResponseError(
            request_info=Mock(), history=(), status=404, message="Not Found"
        )
        self.assertFalse(self.queue._should_retry_on_exception(client_error))

        # Test other exceptions (not retryable)
        self.assertFalse(self.queue._should_retry_on_exception(ValueError("invalid value")))

    async def test_retry_on_different_operations(self) -> None:
        """Test retry behavior on different HTTP operations."""
        success_response = self.create_response_mock("success")

        # Test operations that should retry
        retryable_operations = ["enqueue", "finish", "retry", "abort", "update", "notify"]

        for operation in retryable_operations:
            with self.subTest(operation=operation):
                self.queue.session.post.side_effect = [ServerTimeoutError(), success_response]

                with patch("asyncio.sleep"):
                    result = await self.queue._send(operation, job="test")

                    # Should succeed after retry
                    self.assertEqual(result, "success")
                    # Reset for next iteration
                    self.queue.session.post.reset_mock()
