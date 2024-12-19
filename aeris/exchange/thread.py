from __future__ import annotations

import queue
import sys
from typing import Union

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

from aeris.exchange import MailboxClosedError
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import Message

DEFAULT_PRIORITY = 0
CLOSE_PRIORITY = DEFAULT_PRIORITY + 1
CLOSE_SENTINAL = object()

MailboxQueueItem: TypeAlias = tuple[int, Union[Message, object]]
MailboxQueue: TypeAlias = queue.PriorityQueue[MailboxQueueItem]


class ThreadMailbox:
    """Thread-safe queue-based mailbox."""

    def __init__(self, queue: MailboxQueue) -> None:
        self._queue = queue
        self._closed = False

    def send(self, message: Message) -> None:
        """Send a message to this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError
        self._queue.put((DEFAULT_PRIORITY, message))

    def recv(self) -> Message:
        """Get the next message from this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError

        _, message = self._queue.get(block=True)
        if message is CLOSE_SENTINAL:
            raise MailboxClosedError
        assert isinstance(message, Message)
        return message

    def close(self) -> None:
        """Close the mailbox."""
        if not self._closed:
            self._closed = True
            self._queue.put((CLOSE_PRIORITY, CLOSE_SENTINAL))


class ThreadExchange:
    """Message exchange for threaded agents.

    This exchange uses [`Queues`][queue.Queue] as mailboxes for agents
    running as separate threads within the same process. This exchange
    is helpful for local testing.
    """

    def __init__(self) -> None:
        self._queues: dict[Identifier, MailboxQueue] = {}

    def register_agent(self) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system."""
        aid = AgentIdentifier.new()
        self._queues[aid] = queue.PriorityQueue()
        return aid

    def register_client(self) -> ClientIdentifier:
        """Create a mailbox for a new client in the system."""
        cid = ClientIdentifier.new()
        self._queues[cid] = queue.PriorityQueue()
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
        try:
            queue = self._queues[uid]
        except KeyError:
            return None
        return ThreadMailbox(queue)
