from __future__ import annotations

import logging
import pickle
from typing import Any
from typing import TypeVar

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeMixin
from academy.exchange.queue import Queue
from academy.exchange.queue import QueueClosedError
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.identifier import EntityId
from academy.message import Message

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class ThreadExchange(ExchangeMixin):
    """Local process message exchange for threaded agents.

    This exchange uses [`Queues`][queue.Queue] as mailboxes for agents
    running as separate threads within the same process. This exchange
    is helpful for local testing but not much more.
    """

    def __init__(self) -> None:
        self._queues: dict[EntityId, Queue[Message]] = {}
        self._behaviors: dict[AgentId[Any], type[Behavior]] = {}

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

    def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> AgentId[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Type of the behavior this agent will implement.
            agent_id: Specify the ID of the agent. Randomly generated
                default.
            name: Optional human-readable name for the agent. Ignored if
                `agent_id` is provided.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        aid = AgentId.new(name=name) if agent_id is None else agent_id
        if aid not in self._queues or self._queues[aid].closed():
            self._queues[aid] = Queue()
            self._behaviors[aid] = behavior
            logger.debug('Registered %s in %s', aid, self)
        return aid

    def register_client(
        self,
        name: str | None = None,
    ) -> ClientId:
        """Create a new client identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the client.

        Returns:
            Unique identifier for the client's mailbox.
        """
        cid = ClientId.new(name=name)
        self._queues[cid] = Queue()
        logger.debug('Registered %s in %s', cid, self)
        return cid

    def terminate(self, uid: EntityId) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exists.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        queue = self._queues.get(uid, None)
        if queue is not None and not queue.closed():
            queue.close()
            if isinstance(uid, AgentId):
                self._behaviors.pop(uid, None)
            logger.debug('Closed mailbox for %s (%s)', uid, self)

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given behavior.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        found: list[AgentId[Any]] = []
        for aid, type_ in self._behaviors.items():
            if behavior is type_ or (
                allow_subclasses and issubclass(type_, behavior)
            ):
                found.append(aid)
        alive = tuple(aid for aid in found if not self._queues[aid].closed())
        return alive

    def get_mailbox(self, uid: EntityId) -> ThreadMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: EntityId of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
        """
        queue = self._queues.get(uid, None)
        if queue is None:
            raise BadEntityIdError(uid)
        return ThreadMailbox(uid, self, queue)

    def send(self, uid: EntityId, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        queue = self._queues.get(uid, None)
        if queue is None:
            raise BadEntityIdError(uid)
        try:
            queue.put(message)
            logger.debug('Sent %s to %s', type(message).__name__, uid)
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e


class ThreadMailbox:
    """Client protocol that listens to incoming messages to a mailbox."""

    def __init__(
        self,
        uid: EntityId,
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
    def mailbox_id(self) -> EntityId:
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
