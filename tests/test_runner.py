import logging
import typing as t
import unittest
from unittest import mock
from unittest.mock import MagicMock

from saq.runner import run

if t.TYPE_CHECKING:
    from unittest.mock import MagicMock


class TestRunner(unittest.TestCase):
    def setUp(self):
        self.settings = "settings"

    @mock.patch("saq.runner.logging.basicConfig")
    @mock.patch("saq.runner.start")
    def test_runner_quiet_mode(self, start_mock: MagicMock, basic_config_mock: MagicMock) -> None:
        run(self.settings, quiet=True)

        basic_config_mock.assert_not_called()
        start_mock.assert_called_once()

    @mock.patch("saq.runner.logging.basicConfig")
    @mock.patch("saq.runner.start")
    def test_runner_logging_warning_level(
        self, start_mock: MagicMock, basic_config_mock: MagicMock
    ) -> None:
        run(self.settings, quiet=False, verbose=0)

        basic_config_mock.assert_called_once_with(level=logging.WARNING)
        start_mock.assert_called_once()

    @mock.patch("saq.runner.logging.basicConfig")
    @mock.patch("saq.runner.start")
    def test_runner_logging_info_level(
        self, start_mock: MagicMock, basic_config_mock: MagicMock
    ) -> None:
        run(self.settings, quiet=False, verbose=1)

        basic_config_mock.assert_called_once_with(level=logging.INFO)
        start_mock.assert_called_once()

    @mock.patch("saq.runner.logging.basicConfig")
    @mock.patch("saq.runner.start")
    def test_runner_logging_debug_level(
        self, start_mock: MagicMock, basic_config_mock: MagicMock
    ) -> None:
        run(self.settings, quiet=False, verbose=2)

        basic_config_mock.assert_called_once_with(level=logging.DEBUG)
        start_mock.assert_called_once()

    @mock.patch("saq.runner.sys.exit")
    @mock.patch("saq.runner.check_health")
    def test_runner_check_health(self, check_mock: MagicMock, sys_exit_mock) -> None:
        run(
            self.settings,
            check=True,
        )

        check_mock.assert_called_once_with(self.settings)
        sys_exit_mock.assert_called_once()

    @mock.patch("saq.runner.start")
    def test_runner_with_one_worker(self, start_mock: MagicMock) -> None:
        run(
            self.settings,
            workers=1,
        )

        start_mock.assert_called_once_with(
            self.settings,
            web=False,
            extra_web_settings=None,
            port=8080,
        )

    @mock.patch("saq.runner.multiprocessing.Process")
    @mock.patch("saq.runner.start")
    def test_runner_with_multiple_workers(
        self, start_mock: MagicMock, process_class_mock: MagicMock
    ) -> None:
        process = mock.MagicMock()
        process_class_mock.return_value = process

        run(
            self.settings,
            workers=2,
        )

        process_class_mock.assert_called_once_with(target=start_mock, args=(self.settings,))
        process.start.assert_called_once()

        start_mock.assert_called_once_with(
            self.settings,
            web=False,
            extra_web_settings=None,
            port=8080,
        )

    @mock.patch("saq.runner.start")
    def test_runner_keyboard_interrupt_suppression(self, start_mock: MagicMock) -> None:
        start_mock.side_effect = KeyboardInterrupt

        try:
            run(self.settings)
        except KeyboardInterrupt:
            self.fail("run() raised KeyboardInterrupt exception")
