"""
Tests for Result Caching (Module C.3).
"""
from __future__ import annotations

import json
import unittest
from unittest import mock

from saq.queue.base import Queue
from saq.job import Job, Status


class _CacheQueue(Queue):
    """In-memory queue stub for result cache testing."""

    def __init__(self, name: str = "test", result_cache_ttl: int = 0) -> None:
        super().__init__(name=name, dump=None, load=None, result_cache_ttl=result_cache_ttl)
        self._cache_store: dict[str, bytes] = {}

    async def disconnect(self) -> None:
        pass

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> dict:
        return {
            "workers": {}, "name": self.name, "queued": 0,
            "active": 0, "scheduled": 0, "jobs": [],
        }

    async def count(self, kind) -> int:
        return 0

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        return []

    async def _update(self, job, status=None, **kwargs) -> None:
        pass

    async def job(self, job_key: str):
        return None

    async def jobs(self, job_keys):
        return []

    async def iter_jobs(self, statuses=None, batch_size=100, **kwargs):
        return
        yield  # make it an async generator

    async def abort(self, job, error: str, ttl: float = 5) -> None:
        pass

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0):
        return None

    async def write_worker_info(self, worker_id: str, info, ttl: int) -> None:
        pass

    async def _retry(self, job, error: str | None) -> None:
        pass

    async def _finish(self, job, status, *, result=None, error=None) -> None:
        pass

    async def _enqueue(self, job):
        return job

    async def _get_cached_result(self, cache_key: str):
        data = self._cache_store.get(cache_key)
        if data is None:
            return None
        return json.loads(data)

    async def _set_cached_result(self, cache_key: str, result, ttl: int) -> None:
        self._cache_store[cache_key] = json.dumps(result).encode()


class TestCacheKey(unittest.TestCase):
    """Test _cache_key determinism."""

    def setUp(self):
        self.queue = _CacheQueue()

    def test_cache_key_deterministic(self):
        """Same function+kwargs produce same cache key."""
        key1 = self.queue._cache_key("func", {"a": 1, "b": 2})
        key2 = self.queue._cache_key("func", {"a": 1, "b": 2})
        self.assertEqual(key1, key2)

    def test_cache_key_order_independent(self):
        """Kwargs order doesn't affect cache key (sort_keys=True)."""
        key1 = self.queue._cache_key("func", {"a": 1, "b": 2})
        key2 = self.queue._cache_key("func", {"b": 2, "a": 1})
        self.assertEqual(key1, key2)

    def test_cache_key_different_function(self):
        """Different function names produce different cache keys."""
        key1 = self.queue._cache_key("func_a", {"a": 1})
        key2 = self.queue._cache_key("func_b", {"a": 1})
        self.assertNotEqual(key1, key2)

    def test_cache_key_different_kwargs(self):
        """Different kwargs produce different cache keys."""
        key1 = self.queue._cache_key("func", {"a": 1})
        key2 = self.queue._cache_key("func", {"a": 2})
        self.assertNotEqual(key1, key2)

    def test_cache_key_complex_kwargs(self):
        """Complex nested kwargs produce valid cache keys."""
        key = self.queue._cache_key("func", {"nested": {"a": [1, 2, 3]}, "b": True})
        self.assertTrue(key.startswith("saq:cache:"))
        self.assertEqual(len(key), len("saq:cache:") + 64)  # SHA-256 hex = 64 chars


class TestCacheKeyFormat(unittest.TestCase):
    """Test cache key format details."""

    def test_cache_key_starts_with_prefix(self):
        """Cache key uses saq:cache: prefix."""
        queue = _CacheQueue()
        key = queue._cache_key("my_func", {"x": 1})
        self.assertTrue(key.startswith("saq:cache:"))

    def test_cache_key_sha256_hex(self):
        """Cache key suffix is a valid 64-char hex string (SHA-256)."""
        queue = _CacheQueue()
        key = queue._cache_key("my_func", {"x": 1})
        suffix = key[len("saq:cache:"):]
        self.assertEqual(len(suffix), 64)
        int(suffix, 16)  # Should not raise — valid hex


class TestCacheApplyBehavior(unittest.IsolatedAsyncioTestCase):
    """Test apply() caching behavior using mocks for map()."""

    async def test_apply_cache_hit_returns_cached(self):
        """apply() with use_cache=True returns cached result without calling map."""
        queue = _CacheQueue(result_cache_ttl=60)
        cache_key = queue._cache_key("func", {"a": 1})
        queue._cache_store[cache_key] = b'"cached_result"'

        with mock.patch.object(queue, "map") as mock_map:
            result = await queue.apply("func", use_cache=True, a=1)
            self.assertEqual(result, "cached_result")
            mock_map.assert_not_called()

    async def test_apply_cache_miss_calls_map(self):
        """apply() with use_cache=True calls map() when cache misses."""
        queue = _CacheQueue(result_cache_ttl=60)

        async def fake_map(*args, **kwargs):
            return ["fresh_result"]

        with mock.patch.object(queue, "map", side_effect=fake_map) as mock_map:
            result = await queue.apply("func", use_cache=True, a=1)
            self.assertEqual(result, "fresh_result")
            mock_map.assert_called_once()

    async def test_apply_cache_miss_stores_result(self):
        """apply() stores result in cache after computing it."""
        queue = _CacheQueue(result_cache_ttl=60)

        async def fake_map(*args, **kwargs):
            return ["computed_value"]

        with mock.patch.object(queue, "map", side_effect=fake_map):
            await queue.apply("func", use_cache=True, a=1)

        cache_key = queue._cache_key("func", {"a": 1})
        self.assertIn(cache_key, queue._cache_store)
        self.assertEqual(json.loads(queue._cache_store[cache_key]), "computed_value")

    async def test_apply_use_cache_false_no_lookup(self):
        """apply() with use_cache=False never checks cache."""
        queue = _CacheQueue(result_cache_ttl=60)
        cache_key = queue._cache_key("func", {"a": 1})
        queue._cache_store[cache_key] = b'"cached"'

        async def fake_map(*args, **kwargs):
            return ["from_map"]

        with mock.patch.object(queue, "map", side_effect=fake_map) as mock_map:
            result = await queue.apply("func", use_cache=False, a=1)
            self.assertEqual(result, "from_map")
            mock_map.assert_called_once()

    async def test_apply_no_ttl_no_cache(self):
        """apply() with result_cache_ttl=0 doesn't cache even with use_cache=True."""
        queue = _CacheQueue(result_cache_ttl=0)
        cache_key = queue._cache_key("func", {"a": 1})
        queue._cache_store[cache_key] = b'"cached"'

        async def fake_map(*args, **kwargs):
            return ["from_map"]

        with mock.patch.object(queue, "map", side_effect=fake_map) as mock_map:
            result = await queue.apply("func", use_cache=True, a=1)
            self.assertEqual(result, "from_map")
            mock_map.assert_called_once()

    async def test_apply_does_not_cache_job_errors(self):
        """apply() doesn't cache JobError results."""
        from saq.queue.base import JobError

        queue = _CacheQueue(result_cache_ttl=60)
        job = Job(function="func", queue=queue)
        job.status = Status.FAILED
        job.error = "something went wrong"
        error = JobError(job)

        async def fake_map(*args, **kwargs):
            return [error]

        with mock.patch.object(queue, "map", side_effect=fake_map):
            result = await queue.apply("func", use_cache=True, a=1)
            self.assertIsInstance(result, JobError)

        # Verify nothing was cached
        self.assertEqual(len(queue._cache_store), 0)


class TestCacheBackendMethods(unittest.IsolatedAsyncioTestCase):
    """Test the _CacheQueue's in-memory cache backend."""

    async def test_get_cached_result_miss(self):
        """_get_cached_result returns None for missing key."""
        queue = _CacheQueue(result_cache_ttl=60)
        result = await queue._get_cached_result("saq:cache:nonexistent")
        self.assertIsNone(result)

    async def test_get_cached_result_hit(self):
        """_get_cached_result returns deserialized value for existing key."""
        queue = _CacheQueue(result_cache_ttl=60)
        queue._cache_store["saq:cache:abc"] = b'{"key": "value"}'
        result = await queue._get_cached_result("saq:cache:abc")
        self.assertEqual(result, {"key": "value"})

    async def test_set_cached_result(self):
        """_set_cached_result stores serialized value."""
        queue = _CacheQueue(result_cache_ttl=60)
        await queue._set_cached_result("saq:cache:abc", {"data": 42}, ttl=60)
        self.assertIn("saq:cache:abc", queue._cache_store)
        self.assertEqual(json.loads(queue._cache_store["saq:cache:abc"]), {"data": 42})


if __name__ == "__main__":
    unittest.main()
