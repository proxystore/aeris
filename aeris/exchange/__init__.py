from __future__ import annotations

import sys
from concurrent.futures import Future
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import Message

T = TypeVar('T')

__all__ = ['Exchange', 'Mailbox']


class MailboxClosedError(Exception):
    pass


@runtime_checkable
class Mailbox(Protocol):
    """Mailbox protocol.

    Each entity (i.e., agent or client) in a multi-agent system as an
    associated mailbox in an exchange containing an ordered collection
    messages for that entity. A message can either be an action request
    to an agent, a response to an action request, or a control message.
    """

    def send(self, message: Message) -> None:
        """Send a message to this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        ...

    def recv(self) -> Message:
        """Get the next message from this mailbox."""
        ...

    def close(self) -> None:
        """Close the mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        ...


@runtime_checkable
class Exchange(Protocol):
    """Message exchange protocol.

    A message exchange hosts the mailboxes for each entity (i.e., agent or
    client) in a multi-agent system.
    """

    def close(self) -> None:
        """Close the exchange."""
        ...

    def register_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system.

        Args:
            name: Optional human-readable name for the agent.
        """
        ...

    def register_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a mailbox for a new client in the system.

        Args:
            name: Optional human-readable name for the client.
        """
        ...

    def create_handle(self, aid: AgentIdentifier) -> Handle:
        """Create a handle to an agent in the system.

        A handle enables a client to invoke actions on the agent.

        Note:
            It is not possible to create a handle to a client since a handle
            is essentially a new client of a specific agent.

        Args:
            aid: Identifier of agent in the system to create a handle to.

        Returns:
            Handle to the agent.

        Raises:
            TypeError: if `aid` is not an instance of
                [`AgentIdentifier`][aeris.identifier.AgentIdentifier].
        """
        ...

    def get_mailbox(self, uid: Identifier) -> Mailbox | None:
        """Get the mailbox for an entity in the system.

        Args:
            uid: Identifier of entity in the system.

        Returns:
            Mailbox if the entity entity exists in the system otherwise `None`.
        """
        ...
