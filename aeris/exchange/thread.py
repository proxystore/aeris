from __future__ import annotations

import dataclasses
import logging
import queue
import sys
from collections import defaultdict
from types import TracebackType

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import TypeAlias
else:  # pragma: <3.10 cover
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import Message

logger = logging.getLogger()

DEFAULT_PRIORITY = 0
CLOSE_PRIORITY = DEFAULT_PRIORITY + 1
CLOSE_SENTINAL = object()


@dataclasses.dataclass(order=True)
class _MailboxQueueItem:
    priority: int
    message: Message | object = dataclasses.field(compare=False)


MailboxQueue: TypeAlias = queue.PriorityQueue[_MailboxQueueItem]


class ThreadMailbox:
    """Thread-safe queue-based mailbox."""

    def __init__(self, queue: MailboxQueue) -> None:
        self._queue = queue
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def send(self, message: Message) -> None:
        """Send a message to this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError
        self._queue.put(_MailboxQueueItem(DEFAULT_PRIORITY, message))

    def recv(self) -> Message:
        """Get the next message from this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError

        item = self._queue.get(block=True)
        message = item.message
        if message is CLOSE_SENTINAL:
            raise MailboxClosedError
        assert isinstance(message, Message)
        return message

    def close(self) -> None:
        """Close the mailbox."""
        if not self._closed:
            self._closed = True
            self._queue.put(_MailboxQueueItem(CLOSE_PRIORITY, CLOSE_SENTINAL))


class ThreadExchange:
    """Message exchange for threaded agents.

    This exchange uses [`Queues`][queue.Queue] as mailboxes for agents
    running as separate threads within the same process. This exchange
    is helpful for local testing.
    """

    def __init__(self) -> None:
        self._queues: dict[Identifier, MailboxQueue] = {}
        self._mailboxes: dict[Identifier, list[ThreadMailbox]] = defaultdict(
            list,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{id(self)}>'

    def close(self) -> None:
        """Close the exchange."""
        pass

    def register_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system.

        Args:
            name: Optional human-readable name for the agent.
        """
        aid = AgentIdentifier.new(name=name)
        self._queues[aid] = queue.PriorityQueue()
        logger.info(f'{self} registered {aid}')
        return aid

    def register_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a mailbox for a new client in the system.

        Args:
            name: Optional human-readable name for the client.
        """
        cid = ClientIdentifier.new(name=name)
        self._queues[cid] = queue.PriorityQueue()
        logger.info(f'{self} registered {cid}')
        return cid

    def unregister(self, identifier: Identifier) -> None:
        """Unregister the entity (either agent or client).

        Args:
            identifier: Identifier of the entity to unregister.
        """
        self._queues.pop(identifier, None)
        mailboxes = self._mailboxes.pop(identifier, None)
        if mailboxes is not None:
            for mailbox in mailboxes:
                mailbox.close()
        logger.info(f'{self} unregistered {identifier}')

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

    def get_mailbox(self, uid: Identifier) -> ThreadMailbox:
        """Get the mailbox for an entity in the system.

        Args:
            uid: Identifier of entity in the system.

        Returns:
            Mailbox for the entity.

        Raises:
            BadIdentifierError: if an entity with `uid` is not
                registered with the exchange.
        """
        try:
            queue = self._queues[uid]
        except KeyError as e:
            raise BadIdentifierError(
                f'{uid} is not registered with this exchange.',
            ) from e
        mailbox = ThreadMailbox(queue)
        self._mailboxes[uid].append(mailbox)
        return mailbox
