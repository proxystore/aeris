from __future__ import annotations

import dataclasses
import io
import logging
import queue
import socket
import sys
import threading
from types import TracebackType
from typing import Any
from typing import get_args

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
from aeris.exchange.message import BaseExchangeMessage
from aeris.exchange.message import ExchangeMessage
from aeris.exchange.message import ForwardMessage
from aeris.exchange.message import RegisterMessage
from aeris.exchange.message import ResponseMessage
from aeris.exchange.message import UnregisterMessage
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import BaseMessage
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


class SimpleMailbox:
    """Thread-safe queue-based mailbox."""

    def __init__(self, uid: Identifier, exchange: SimpleExchange) -> None:
        self.uid = uid
        self._exchange = exchange
        self._queue: MailboxQueue = queue.PriorityQueue()
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

    def _push(self, message: Message) -> None:
        if self._closed:
            raise MailboxClosedError
        self._queue.put(_MailboxQueueItem(DEFAULT_PRIORITY, message))

    def send(self, message: Message) -> None:
        """Send a message to this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError
        wrapped = ForwardMessage(
            src=message.src,
            dest=message.dest,
            message=message.model_dump_json(),
        )
        self._exchange._send_server_message(wrapped)

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
        assert isinstance(message, get_args(Message))
        return message

    def close(self) -> None:
        """Close the mailbox."""
        if not self._closed:
            self._closed = True
            self._queue.put(_MailboxQueueItem(CLOSE_PRIORITY, CLOSE_SENTINAL))
            self._exchange.unregister(self.uid)


class SimpleExchange:
    """Simple exchange client.

    Args:
        host: Host of the exchange server.
        port: Port of the exchange server.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

        self._socket = socket.create_connection((self.host, self.port))
        self._socket.setblocking(False)
        self._handler_thread = threading.Thread(
            target=self._handle_server_messages,
        )
        self._handler_thread.start()
        logging.debug('%s started server message handler thread', self)
        self._mailboxes: dict[Identifier, SimpleMailbox] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __getnewargs_ex__(
        self,
    ) -> tuple[tuple[str, int], dict[str, Any]]:
        return ((self.host, self.port), {})

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{id(self)}>'

    def _handle_server_messages(self) -> None:
        buffer = io.BytesIO()
        while True:
            try:
                raw = self._socket.recv(1024)
            except BlockingIOError:
                continue
            except OSError:
                break
            if len(raw) == 0:
                break
            buffer.write(raw)
            buffer.seek(0)
            start_index = 0
            for line in buffer:
                start_index += len(line)

                raw = line.strip()
                if len(raw) == 0:
                    continue

                message = BaseExchangeMessage.model_deserialize(raw)

                if isinstance(message, ForwardMessage):
                    wrapped = BaseMessage.model_from_json(message.message)
                    self._mailboxes[message.dest]._push(wrapped)
                elif (
                    isinstance(message, ResponseMessage)
                    and not message.success
                ):
                    logger.warning(
                        'Got bad response from exchange: %s',
                        message,
                    )
                else:
                    logger.warning(
                        'Unhandled message type from exchange: %s',
                        message,
                    )

            if start_index > 0:
                buffer.seek(start_index)
                remaining = buffer.read()
                buffer.truncate(0)
                buffer.seek(0)
                buffer.write(remaining)
            else:
                buffer.seek(0, 2)
        buffer.close()

    def _send_server_message(self, message: ExchangeMessage) -> None:
        self._socket.send(message.model_serialize() + b'\n')

    def close(self) -> None:
        """Close the connection to the exchange."""
        logger.debug('%s closing socket connection to server', self)
        self._socket.close()
        self._handler_thread.join(timeout=1)

    def register_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system.

        Args:
            name: Optional human-readable name for the agent.
        """
        aid = AgentIdentifier.new(name=name)
        self._mailboxes[aid] = SimpleMailbox(aid, self)
        message = RegisterMessage(src=aid)
        self._send_server_message(message)
        logger.info(f'{self} registered {aid}')
        return aid

    def register_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a mailbox for a new client in the system.

        Args:
            name: Optional human-readable name for the client.
        """
        cid = ClientIdentifier.new(name=name)
        self._mailboxes[cid] = SimpleMailbox(cid, self)
        message = RegisterMessage(src=cid)
        self._send_server_message(message)
        logger.info(f'{self} registered {cid}')
        return cid

    def unregister(self, uid: Identifier) -> None:
        """Unregister the entity (either agent or client).

        Args:
            uid: Identifier of the entity to unregister.
        """
        mailbox = self._mailboxes.pop(uid, None)
        if mailbox is not None:
            mailbox.close()
        message = UnregisterMessage(src=uid)
        self._send_server_message(message)
        logger.info(f'{self} unregistered {uid}')

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

    def get_mailbox(self, uid: Identifier) -> SimpleMailbox:
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
            return self._mailboxes[uid]
        except KeyError as e:
            raise BadIdentifierError(
                f'{uid} is not registered with this exchange.',
            ) from e
