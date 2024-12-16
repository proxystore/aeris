"""foo module."""

from __future__ import annotations


def bar(data: list[int] | None = None) -> int | None:
    """Sum list of integers.

    Args:
        data (list[int], None): optional list of integers (default: None).

    Returns:
        `None` if `data=None` else the integer sum of `data`.
    """
    if data is not None:
        return sum(data)
    return None
