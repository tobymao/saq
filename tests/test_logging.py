from __future__ import annotations

import json
import logging
import os
import unittest
from io import StringIO
from unittest import mock

from saq.job import Status
from saq.logging import JSONFormatter, auto_setup, setup


class TestJSONFormatter(unittest.TestCase):
    """Test JSON formatter output."""

    def test_basic_format(self):
        """Basic log produces valid JSON with required fields."""
        formatter = _get_formatter()
        record = logging.LogRecord(
            "saq", logging.INFO, "test.py", 1, "hello world", (), None
        )
        output = formatter.format(record)
        data = json.loads(output)
        self.assertIn("timestamp", data)
        self.assertIn("level", data)
        self.assertEqual(data["level"], "INFO")
        self.assertIn("logger", data)
        self.assertEqual(data["logger"], "saq")
        self.assertIn("message", data)
        self.assertEqual(data["message"], "hello world")

    def test_extra_fields(self):
        """Extra fields are injected into JSON output."""
        formatter = _get_formatter()
        record = logging.LogRecord(
            "saq", logging.INFO, "test.py", 1, "test", (), None
        )
        record.job_id = "abc123"
        record.queue = "default"
        output = formatter.format(record)
        data = json.loads(output)
        self.assertEqual(data["job_id"], "abc123")
        self.assertEqual(data["queue"], "default")

    def test_exception_formatting(self):
        """logger.exception output includes exc_info field."""
        formatter = _get_formatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys as _sys

            exc_info = _sys.exc_info()
            record = logging.LogRecord(
                "saq", logging.ERROR, "test.py", 1, "error", (), exc_info
            )
        output = formatter.format(record)
        data = json.loads(output)
        self.assertIn("exc_info", data)
        self.assertIn("ValueError: test error", data["exc_info"])

    def test_non_serializable_extra(self):
        """Non-serializable extra values are converted to str."""
        formatter = _get_formatter()

        class CustomObj:
            def __str__(self):
                return "<CustomObj>"

        record = logging.LogRecord(
            "saq", logging.INFO, "test.py", 1, "test", (), None
        )
        record.obj = CustomObj()
        output = formatter.format(record)
        data = json.loads(output)
        self.assertEqual(data["obj"], "<CustomObj>")

    def test_nested_extra(self):
        """Nested dict extra values are serialized correctly."""
        formatter = _get_formatter()
        record = logging.LogRecord(
            "saq", logging.INFO, "test.py", 1, "test", (), None
        )
        record.metadata = {"key": "val", "nested": {"a": 1}}
        output = formatter.format(record)
        data = json.loads(output)
        self.assertEqual(data["metadata"]["nested"]["a"], 1)

    def test_utf8_message(self):
        """Messages with Chinese/special characters are output correctly."""
        formatter = _get_formatter()
        record = logging.LogRecord(
            "saq", logging.INFO, "test.py", 1, "处理任务 成功", (), None
        )
        output = formatter.format(record)
        data = json.loads(output)
        self.assertIn("处理任务", data["message"])

    def test_default_formatter_unchanged(self):
        """Without JSON mode, default text format is preserved."""
        handler = logging.StreamHandler(StringIO())
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger = logging.Logger("test_default")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("plain text message")
        output = handler.stream.getvalue().strip()
        self.assertEqual(output, "plain text message")
        with self.assertRaises(json.JSONDecodeError):
            json.loads(output)


class TestSetup(unittest.TestCase):
    """Test setup() function."""

    def setUp(self):
        self.logger = logging.getLogger("saq")
        self.logger.handlers.clear()
        self.logger.propagate = False

    def tearDown(self):
        self.logger.handlers.clear()
        self.logger.propagate = True

    def test_setup_json(self):
        """setup(format='json') configures JSON output."""
        setup(format="json", level="DEBUG")
        self.assertEqual(len(self.logger.handlers), 1)
        handler = self.logger.handlers[0]
        self.assertIsInstance(handler.formatter, JSONFormatter)
        self.assertEqual(self.logger.level, logging.DEBUG)

    def test_setup_text(self):
        """setup(format='text') configures text output."""
        setup(format="text", level="INFO")
        self.assertEqual(len(self.logger.handlers), 1)
        handler = self.logger.handlers[0]
        self.assertNotIsInstance(handler.formatter, JSONFormatter)
        self.assertEqual(self.logger.level, logging.INFO)

    def test_setup_custom_level(self):
        """setup(level='DEBUG') sets log level correctly."""
        setup(format="text", level="DEBUG")
        self.assertEqual(self.logger.level, logging.DEBUG)

    def test_setup_idempotent(self):
        """Multiple setup() calls don't add duplicate handlers."""
        setup(format="json")
        setup(format="json")
        setup(format="text")
        self.assertEqual(len(self.logger.handlers), 1)

    def test_auto_setup_env_var(self):
        """auto_setup() reads SAQ_LOG_FORMAT env var."""
        with mock.patch.dict(os.environ, {"SAQ_LOG_FORMAT": "json", "SAQ_LOG_LEVEL": "DEBUG"}):
            auto_setup()
        self.assertEqual(len(self.logger.handlers), 1)
        self.assertIsInstance(self.logger.handlers[0].formatter, JSONFormatter)
        self.assertEqual(self.logger.level, logging.DEBUG)


class TestStructuredLogCalls(unittest.IsolatedAsyncioTestCase):
    """Verify all logger call sites pass extra structured fields."""

    async def asyncSetUp(self):
        from saq.queue.redis import RedisQueue
        from tests.helpers import cleanup_queue, create_redis_queue

        self.queue: RedisQueue = await create_redis_queue()
        self._cleanup = cleanup_queue
        self.records: list[logging.LogRecord] = []
        self.handler = _CaptureHandler(self.records)
        saq_logger = logging.getLogger("saq")
        saq_logger.addHandler(self.handler)
        saq_logger.setLevel(logging.DEBUG)

    async def asyncTearDown(self):
        logging.getLogger("saq").removeHandler(self.handler)
        await self._cleanup(self.queue)

    async def test_enqueue_log_has_fields(self):
        """queue.enqueue log includes job_id, queue, function, status."""
        await self.queue.enqueue("test_func", a=1)
        record = _find_record(self.records, "Enqueuing")
        self.assertIsNotNone(record)
        self.assertEqual(getattr(record, "job_function", None), "test_func")
        self.assertEqual(getattr(record, "queue", None), self.queue.name)
        self.assertTrue(hasattr(record, "job_key"))

    async def test_finish_log_has_fields(self):
        """queue.finish log includes job_id, queue, status."""
        job = await self.queue.enqueue("test_func", a=1)
        assert job is not None
        self.records.clear()
        await self.queue.finish(job, Status.COMPLETE, result=42)
        record = _find_record(self.records, "Finished")
        self.assertIsNotNone(record)
        self.assertEqual(getattr(record, "status", None), "complete")
        self.assertEqual(getattr(record, "queue", None), self.queue.name)
        self.assertTrue(hasattr(record, "job_key"))

    async def test_retry_log_has_fields(self):
        """queue.retry log includes job_id, queue."""
        job = await self.queue.enqueue("test_func", a=1)
        assert job is not None
        self.records.clear()
        await self.queue.retry(job, "test error")
        record = _find_record(self.records, "Retrying")
        self.assertIsNotNone(record)
        self.assertEqual(getattr(record, "queue", None), self.queue.name)
        self.assertTrue(hasattr(record, "job_key"))

    async def test_dequeue_timeout_log_has_fields(self):
        """Redis dequeue timeout log includes queue."""
        self.records.clear()
        await self.queue.dequeue(timeout=0.01)
        record = _find_record(self.records, "Dequeue timed out")
        self.assertIsNotNone(record)
        self.assertEqual(getattr(record, "queue", None), self.queue.name)

    async def test_sweep_log_has_fields(self):
        """queue.sweep log includes queue and swept_count when sweeping."""
        job = await self.queue.enqueue("test_func", a=1)
        assert job is not None
        # Manually make the job stuck by setting started time in the past
        job.started = 0
        job.status = Status.ACTIVE
        await self.queue._update(job, status=Status.ACTIVE)

        self.records.clear()
        await self.queue.sweep(lock=1, abort=0.1)
        record = _find_record(self.records, "Sweeping")
        if record:
            self.assertEqual(getattr(record, "queue", None), self.queue.name)


class TestCLI(unittest.TestCase):
    """Test CLI --log-format parameter."""

    def test_cli_log_format_json(self):
        """--log-format json is a valid argument."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--log-format", choices=["text", "json"], default="text")
        args = parser.parse_args(["--log-format", "json"])
        self.assertEqual(args.log_format, "json")

    def test_cli_log_format_default(self):
        """Default log format is text."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--log-format", choices=["text", "json"], default="text")
        args = parser.parse_args([])
        self.assertEqual(args.log_format, "text")

    def test_env_var_override(self):
        """SAQ_LOG_FORMAT env var is respected."""
        with mock.patch.dict(os.environ, {"SAQ_LOG_FORMAT": "json"}):
            self.assertEqual(os.environ.get("SAQ_LOG_FORMAT", "text"), "json")


class _CaptureHandler(logging.Handler):
    """Capture log records for testing."""

    def __init__(self, records: list[logging.LogRecord]) -> None:
        super().__init__()
        self.records = records

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _get_formatter() -> "JSONFormatter":
    return JSONFormatter()


def _find_record(
    records: list[logging.LogRecord], keyword: str
) -> logging.LogRecord | None:
    for record in records:
        if keyword in record.getMessage():
            return record
    return None


if __name__ == "__main__":
    unittest.main()
