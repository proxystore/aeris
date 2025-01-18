from __future__ import annotations

from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier


class BadIdentifierError(Exception):
    """Entity associated with the identifier is unknown."""

    def __init__(self, uid: Identifier) -> None:
        super().__init__(f'Unknown identifier {uid}.')


class HandleClosedError(Exception):
    """Agent handle has been closed."""

    def __init__(self, aid: AgentIdentifier, cid: ClientIdentifier) -> None:
        super().__init__(f'Handle to {aid} with {cid} has been closed.')


class MailboxClosedError(Exception):
    """Mailbox is closed and cannot send or receive messages."""

    def __init__(self, uid: Identifier) -> None:
        super().__init__(f'Mailbox for {uid} has been closed.')
