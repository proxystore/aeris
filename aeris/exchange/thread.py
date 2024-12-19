from __future__ import annotations

import queue
from typing import Any

from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier


class ThreadMailbox:
    """Thread-safe queue-based mailbox."""

    def __init__(self) -> None:
        self._mail: queue.Queue[Any] = queue.Queue()

    def send(self, message: Any) -> None:
        """Send a message to this mailbox."""
        self._mail.put(message)

    def recv(self) -> Any:
        """Get the next message from this mailbox."""
        return self._mail.get(block=True)


class ThreadExchange:
    """Message exchange for threaded agents.

    This exchange uses [`Queues`][queue.Queue] as mailboxes for agents
    running as separate threads within the same process. This exchange
    is helpful for local testing.
    """

    def __init__(self) -> None:
        self.mailboxes: dict[Identifier, ThreadMailbox] = {}

    def register_agent(self) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system."""
        aid = AgentIdentifier.new()
        self.mailboxes[aid] = ThreadMailbox()
        return aid

    def register_client(self) -> ClientIdentifier:
        """Create a mailbox for a new client in the system."""
        cid = ClientIdentifier.new()
        self.mailboxes[cid] = ThreadMailbox()
        return cid

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
        if not isinstance(aid, AgentIdentifier):
            raise TypeError(
                f'Handle must be created from an {AgentIdentifier.__name__} '
                f'but got identifier with type {type(aid).__name__}.',
            )
        return Handle(aid, self)

    def get_mailbox(self, uid: Identifier) -> ThreadMailbox | None:
        """Get the mailbox for an entity in the system.

        Args:
            uid: Identifier of entity in the system.

        Returns:
            Mailbox if the entity entity exists in the system otherwise `None`.
        """
        return self.mailboxes.get(uid)
