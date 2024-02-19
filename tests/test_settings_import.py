from __future__ import annotations

import contextlib
import secrets
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from saq.worker import import_settings


class TestSettingsImport(unittest.TestCase):
    def setUp(self) -> None:
        self.cm = cm = contextlib.ExitStack()

        tempdir = Path(cm.enter_context(tempfile.TemporaryDirectory()))
        root_module_name = "foo" + secrets.token_urlsafe(2)
        file_tree = [
            tempdir / root_module_name / "__init__.py",
            tempdir / root_module_name / "bar" / "__init__.py",
            tempdir / root_module_name / "bar" / "settings.py",
        ]
        for path in file_tree:
            path.parent.mkdir(exist_ok=True, parents=True)
            path.touch()

        file_tree[-1].write_text(
            textwrap.dedent(
                """
                static = {
                    "functions": ["pretend_its_a_fn"],
                    "concurrency": 100
                }

                def factory():
                    return {
                        "functions": ["pretend_its_some_other_fn"],
                        "concurrency": static["concurrency"] + 100
                    }
                """
            ).strip()
        )
        sys.path.append(str(tempdir))

        self.module_path = f"{root_module_name}.bar.settings"

    def tearDown(self) -> None:
        self.cm.close()

    def test_imports_settings_from_module_path(self) -> None:
        settings = import_settings(self.module_path + ".static")

        self.assertDictEqual(
            settings,
            {
                "functions": ["pretend_its_a_fn"],
                "concurrency": 100,
            },
        )

    def test_calls_settings_factory(self) -> None:
        settings = import_settings(self.module_path + ".factory")

        self.assertDictEqual(
            settings,
            {
                "functions": ["pretend_its_some_other_fn"],
                "concurrency": 200,
            },
        )
