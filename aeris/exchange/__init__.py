from __future__ import annotations

import sys
from types import TracebackType
from typing import Any
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

    def create_agent(
        self,
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
    ) -> AgentIdentifier[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Behavior type of the agent.
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
        aid: AgentIdentifier[BehaviorT],
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

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentIdentifier[Any], ...]:
        """Discover peer agents with a given behavior.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        ...

    def get_mailbox(self, uid: Identifier) -> Mailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: Identifier of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
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
        behavior: type[BehaviorT],
        *,
        name: str | None = None,
    ) -> AgentIdentifier[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Type of the behavior this agent will implement.
            name: Optional human-readable name for the agent.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        aid: AgentIdentifier[BehaviorT] = AgentIdentifier.new(name=name)
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
        aid: AgentIdentifier[BehaviorT],
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


@runtime_checkable
class Mailbox(Protocol):
    """Client protocol that listens to incoming messages to a mailbox."""

    @property
    def exchange(self) -> Exchange:
        """Exchange client."""
        ...

    @property
    def mailbox_id(self) -> Identifier:
        """Mailbox address/identifier."""
        ...

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        ...

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        ...
