from __future__ import annotations

from aeris.identifier import AgentIdentifier
from aeris.identifier import Identifier


class BadIdentifierError(Exception):
    """Entity associated with the identifier is unknown."""

    def __init__(self, uid: Identifier) -> None:
        super().__init__(f'Unknown identifier {uid}.')


class HandleClosedError(Exception):
    """Agent handle has been closed."""

    def __init__(self, aid: AgentIdentifier, hid: Identifier) -> None:
        super().__init__(f'Handle to {aid} bound to {hid} has been closed.')


class HandleNotBoundError(Exception):
    """Handle to agent is in an unbound state.

    An unbound handle (typically, an instance of `UnboundRemoteHandle`) is
    initialized with a target agent ID and exchange, but does not have an
    identifier itself. Thus, the handle does not have a mailbox in the exchange
    to receive response messages.

    A handle must be bound to be used, either as a unique client with its own
    mailbox or as bound to a running agent where it shares a mailbox with that
    running agent. To create a client bound handle, use
    `handle.bind_as_client()`.

    Any agent behavior that has a handle to another agent as an instance
    attribute will be automatically bound to the agent when the agent begins
    running.
    """

    def __init__(self, aid: AgentIdentifier) -> None:
        super().__init__(
            f'Handle to {aid} is not bound as a client nor to a running '
            'agent. See the exception docstring for troubleshooting.',
        )


class MailboxClosedError(Exception):
    """Mailbox is closed and cannot send or receive messages."""

    def __init__(self, uid: Identifier) -> None:
        super().__init__(f'Mailbox for {uid} has been closed.')
