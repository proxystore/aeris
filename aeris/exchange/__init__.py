from __future__ import annotations

import sys
from types import TracebackType
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import Message

__all__ = ['Exchange', 'ExchangeMixin']

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


@runtime_checkable
class Exchange(Protocol):
    """Message exchange client protocol.

    A message exchange hosts mailboxes for each entity (i.e., agent or
    client) in a multi-agent system. This protocol defines the client
    interface to an arbitrary exchange.

    Warning:
        Exchange implementations should be efficiently pickleable so that
        agents and remote clients can establish client connections to the
        same exchange.
    """

    def close(self) -> None:
        """Close the exchange client.

        Note:
            This does not alter the state of the exchange.
        """
        ...

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        ...

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        ...

    def create_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a new agent identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the agent.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        ...

    def create_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a new client identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the client.

        Returns:
            Unique identifier for the client's mailbox.
        """
        ...

    def create_handle(
        self,
        aid: AgentIdentifier,
    ) -> UnboundRemoteHandle[BehaviorT]:
        """Create a new handle to an agent.

        A handle enables a client to invoke actions on the agent.

        Note:
            It is not possible to create a handle to a client since a handle
            is essentially a new client of a specific agent.

        Args:
            aid: Identifier of the agent to create a handle to.

        Returns:
            Handle to the agent.

        Raises:
            BadIdentifierError: if a mailbox for `aid` does not exist.
            TypeError: if `aid` is not an instance of
                [`AgentIdentifier`][aeris.identifier.AgentIdentifier].
        """
        ...

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        ...

    def recv(self, uid: Identifier) -> Message:
        """Receive the next message addressed to an entity.

        Args:
            uid: Identifier of the entity requesting it's next message.

        Returns:
            Next message in the entity's mailbox.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        ...


class ExchangeMixin:
    """Mixin class that adds basic methods to an exchange implementation.

    This adds a simple `repr`/`str`, context manager support, and the
    `create_agent`, `create_client`, and `create_handle` methods.
    """

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self: Exchange,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{id(self)}>'

    def create_agent(
        self: Exchange,
        name: str | None = None,
    ) -> AgentIdentifier:
        """Create a new agent identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the agent.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        aid = AgentIdentifier.new(name=name)
        self.create_mailbox(aid)
        return aid

    def create_client(
        self: Exchange,
        name: str | None = None,
    ) -> ClientIdentifier:
        """Create a new client identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the client.

        Returns:
            Unique identifier for the client's mailbox.
        """
        cid = ClientIdentifier.new(name=name)
        self.create_mailbox(cid)
        return cid

    def create_handle(
        self: Exchange,
        aid: AgentIdentifier,
    ) -> UnboundRemoteHandle[BehaviorT]:
        """Create a new handle to an agent.

        A handle enables a client to invoke actions on the agent.

        Note:
            It is not possible to create a handle to a client since a handle
            is essentially a new client of a specific agent.

        Args:
            aid: Identifier of the agent to create an handle to.

        Returns:
            Handle to the agent.

        Raises:
            TypeError: if `aid` is not an instance of
                [`AgentIdentifier`][aeris.identifier.AgentIdentifier].
        """
        if not isinstance(aid, AgentIdentifier):
            raise TypeError(
                f'Handle must be created from an {AgentIdentifier.__name__} '
                f'but got identifier with type {type(aid).__name__}.',
            )
        return UnboundRemoteHandle(self, aid)
