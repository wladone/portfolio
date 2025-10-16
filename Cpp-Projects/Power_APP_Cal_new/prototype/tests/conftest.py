"""
Pytest configuration for this test suite.

Provides typing-only stubs for common fixtures so IDEs can infer types
without changing runtime behavior. The real fixtures are provided by
pytest-qt; these definitions are only evaluated by type checkers.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from pytestqt.qtbot import QtBot  # noqa: F401
    import pytest

    @pytest.fixture  # type: ignore[misc]
    def qtbot() -> "QtBot":  # noqa: ANN001
        ...  # typing-only stub

