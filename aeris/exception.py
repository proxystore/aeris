from __future__ import annotations


class BadIdentifierError(Exception):
    """Entity associated with the identifier is unknown."""

    pass


class BadMessageTypeError(Exception):
    """Received message with unexpected type."""

    pass


class HandleClosedError(Exception):
    """Agent handle has been closed."""


class MailboxClosedError(Exception):
    """Mailbox is closed and cannot send or receive messages."""

    pass
