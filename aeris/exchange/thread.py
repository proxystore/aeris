from __future__ import annotations

import logging
import pickle

from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.exchange.queue import Queue
from aeris.exchange.queue import QueueClosedError
from aeris.identifier import AgentIdentifier
from aeris.identifier import Identifier
from aeris.message import Message

logger = logging.getLogger(__name__)


class ThreadExchange(ExchangeMixin):
    """Local process message exchange for threaded agents.

    This exchange uses [`Queues`][queue.Queue] as mailboxes for agents
    running as separate threads within the same process. This exchange
    is helpful for local testing but not much more.
    """

    def __init__(self) -> None:
        self._queues: dict[Identifier, Queue[Message]] = {}
        self._behaviors: dict[Identifier, Behavior] = {}

    def __getstate__(self) -> None:
        raise pickle.PicklingError(
            f'{type(self).__name__} cannot be safely pickled.',
        )

    def close(self) -> None:
        """Close the exchange.

        Unlike most exchange clients, this will close all of the mailboxes.
        """
        for queue in self._queues.values():
            queue.close()
        logger.debug('Closed exchange (%s)', self)

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        if uid not in self._queues or self._queues[uid].closed():
            self._queues[uid] = Queue()
            logger.debug('Created mailbox for %s (%s)', uid, self)

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exists.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        queue = self._queues.get(uid, None)
        if queue is not None and not queue.closed():
            queue.close()
            logger.debug('Closed mailbox for %s (%s)', uid, self)

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentIdentifier, ...]:
        """Discover peer agents with a given behavior.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        ...

    def get_mailbox(self, uid: Identifier) -> ThreadMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: Identifier of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
        """
        queue = self._queues.get(uid, None)
        if queue is None:
            raise BadIdentifierError(uid)
        return ThreadMailbox(uid, self, queue)

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        queue = self._queues.get(uid, None)
        if queue is None:
            raise BadIdentifierError(uid)
        try:
            queue.put(message)
            logger.debug('Sent %s to %s', type(message).__name__, uid)
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e


class ThreadMailbox:
    """Client protocol that listens to incoming messages to a mailbox."""

    def __init__(
        self,
        uid: Identifier,
        exchange: ThreadExchange,
        queue: Queue[Message],
    ) -> None:
        self._uid = uid
        self._exchange = exchange
        self._queue = queue

    @property
    def exchange(self) -> ThreadExchange:
        """Exchange client."""
        return self._exchange

    @property
    def mailbox_id(self) -> Identifier:
        """Mailbox address/identifier."""
        return self._uid

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        pass

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
        try:
            message = self._queue.get(timeout=timeout)
            logger.debug(
                'Received %s to %s',
                type(message).__name__,
                self.mailbox_id,
            )
            return message
        except QueueClosedError as e:
            raise MailboxClosedError(self.mailbox_id) from e
