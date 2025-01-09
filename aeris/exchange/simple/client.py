from __future__ import annotations

import base64
import dataclasses
import io
import json
import logging
import pickle
import queue
import socket
import sys
import threading
import uuid
from types import TracebackType
from typing import Any

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
        pickled = base64.b64encode(pickle.dumps(message)).decode('ascii')
        payload = {
            'kind': 'message',
            'src': str(message.src._uid),
            'dest': str(message.dest._uid),
            'message': pickled,
        }
        self._exchange._send_server_message(payload)

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
        # TODO: Would be great to use identifiers everywhere which would
        # require updating server to parse control messages.
        self._mailboxes: dict[uuid.UUID, SimpleMailbox] = {}

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

                line_str = line.decode().strip()
                if len(line_str) == 0:
                    continue
                payload = json.loads(line_str)
                kind = payload['kind'].strip().lower()

                if kind == 'message':
                    client = uuid.UUID(payload['dest'])
                    message: Message = pickle.loads(
                        base64.b64decode(payload['message']),
                    )
                    self._mailboxes[client]._push(message)
                elif payload['status'] != 'success':
                    logger.warning(
                        'Got bad payload from exchange: %s',
                        payload,
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

    def _send_server_message(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode()
        self._socket.send(encoded + b'\n')

    def close(self) -> None:
        """Close the connection to the exchange."""
        logger.debug('%s closing socket connection to server', self)
        self._socket.close()
        self._handler_thread.join(timeout=1)

    def _register(self, uid: Identifier) -> None:
        # TODO: expose _uid as public property.
        self._mailboxes[uid._uid] = SimpleMailbox(uid, self)
        payload = {'kind': 'register', 'src': str(uid._uid)}
        encoded = json.dumps(payload).encode() + b'\n'
        self._socket.send(encoded)

    def register_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system.

        Args:
            name: Optional human-readable name for the agent.
        """
        aid = AgentIdentifier.new(name=name)
        self._register(aid)
        logger.info(f'{self} registered {aid}')
        return aid

    def register_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a mailbox for a new client in the system.

        Args:
            name: Optional human-readable name for the client.
        """
        cid = ClientIdentifier.new(name=name)
        self._register(cid)
        logger.info(f'{self} registered {cid}')
        return cid

    def unregister(self, uid: Identifier) -> None:
        """Unregister the entity (either agent or client).

        Args:
            uid: Identifier of the entity to unregister.
        """
        mailbox = self._mailboxes.pop(uid._uid, None)
        if mailbox is not None:
            mailbox.close()
        payload = {'kind': 'unregister', 'src': str(uid._uid)}
        encoded = json.dumps(payload).encode() + b'\n'
        self._socket.send(encoded)
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
            return self._mailboxes[uid._uid]
        except KeyError as e:
            raise BadIdentifierError(
                f'{uid} is not registered with this exchange.',
            ) from e
