from __future__ import annotations


class BadMessageTypeError(Exception):
    """Received message with unexpected type."""

    pass


class HandleClosedError(Exception):
    """Agent handle has been closed."""
