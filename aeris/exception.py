from __future__ import annotations


class BadIdentifierError(Exception):
    """Entity associated with the identifier is unknown."""

    pass


class BadMessageTypeError(Exception):
    """Received message with unexpected type."""

    pass


class BadRequestError(Exception):
    """Request message is malformed."""

    pass


class HandleClosedError(Exception):
    """Agent handle has been closed."""

    pass


class MailboxClosedError(Exception):
    """Mailbox is closed and cannot send or receive messages."""

    pass
