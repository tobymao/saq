# pylint: disable=unused-import
try:
    from typing import Literal, TypedDict
except ImportError:
    from typing_extensions import Literal, TypedDict  # type:ignore[misc]
