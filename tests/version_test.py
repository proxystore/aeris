from __future__ import annotations

import aeris


def test_package_version() -> None:
    assert isinstance(aeris.__version__, str)
