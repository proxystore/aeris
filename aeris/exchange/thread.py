from __future__ import annotations

import logging
import pickle

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.exchange.queue import Queue
from aeris.exchange.queue import QueueClosedError
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

    def recv(self, uid: Identifier) -> Message:
        """Receive the next message address to an entity.

        Args:
            uid: Identifier of the entity request it's next message.

        Returns:
            Next message in the entity's mailbox.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        queue = self._queues.get(uid, None)
        if queue is None:
            raise BadIdentifierError(uid)
        try:
            message = queue.get()
            logger.debug('Received %s to %s', type(message).__name__, uid)
            return message
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e
