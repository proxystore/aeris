from __future__ import annotations

import base64
import enum
import logging
import socket
import sys
import threading
import uuid
from types import TracebackType
from typing import Any
from typing import get_args

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import redis
import zmq

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.exchange.queue import Queue
from aeris.exchange.queue import QueueClosedError
from aeris.identifier import Identifier
from aeris.message import BaseMessage
from aeris.message import Message

logger = logging.getLogger(__name__)

_MESSAGE_ACK = '<PEER_MESSAGE_RECV_ACK>'
_THREAD_START_TIMEOUT = 5
_THREAD_JOIN_TIMEOUT = 5
_SOCKET_POLL_TIMEOUT_MS = 50


class _MailboxState(enum.Enum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class HybridExchange(ExchangeMixin):
    """Hybrid exchange.

    The hybrid exchange uses peer-to-peer communication via ZMQ and a
    central Redis server for mailbox state and queueing messages for
    offline entities.

    Args:
        redis_host: Redis server hostname.
        redis_port: Redis server port.
        redis_kwargs: Extra keyword arguments to pass to
            [`redis.Redis()`][redis.Redis].
    """

    _address_cache: dict[Identifier, str]
    _redis_client: redis.Redis
    _socket_pool: _ZMQSocketPool

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        *,
        namespace: str | None = None,
        redis_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._namespace = (
            namespace
            if namespace is not None
            else uuid_to_base32(uuid.uuid4())
        )
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_kwargs = redis_kwargs if redis_kwargs is not None else {}

        self._init_connections()

    def _init_connections(self) -> None:
        self._address_cache = {}
        self._redis_client = redis.Redis(
            host=self._redis_host,
            port=self._redis_port,
            decode_responses=True,
            **self._redis_kwargs,
        )
        self._socket_pool = _ZMQSocketPool()

    def __getstate__(self) -> dict[str, Any]:
        return {
            '_redis_host': self._redis_host,
            '_redis_port': self._redis_port,
            '_redis_kwargs': self._redis_kwargs,
            '_namespace': self._namespace,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._init_connections()

    def __repr__(self) -> str:
        redis_addr = f'{self._redis_host}:{self._redis_port}'
        return (
            f'{type(self).__name__}(namespace={self._namespace}, '
            f'redis={redis_addr})'
        )

    def __str__(self) -> str:
        redis_addr = f'{self._redis_host}:{self._redis_port}'
        return f'{type(self).__name__}<{redis_addr}; {self._namespace}>'

    def _address_key(self, uid: Identifier) -> str:
        return f'{self._namespace}:{uuid_to_base32(uid.uid)}:address'

    def _status_key(self, uid: Identifier) -> str:
        return f'{self._namespace}:{uuid_to_base32(uid.uid)}:status'

    def _queue_key(self, uid: Identifier) -> str:
        return f'{self._namespace}:{uuid_to_base32(uid.uid)}:queue'

    def close(self) -> None:
        """Close the exchange interface."""
        self._redis_client.close()
        self._socket_pool.close()
        logger.debug('Closed exchange (%s)', self)

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        This sets the state of the mailbox to active in the Redis server.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        self._redis_client.set(
            self._status_key(uid),
            _MailboxState.ACTIVE.value,
        )
        logger.debug('Created mailbox for %s (%s)', uid, self)

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        This sets the state of the mailbox to inactive in the Redis server,
        and deletes any queued messages in Redis.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        self._redis_client.set(
            self._status_key(uid),
            _MailboxState.INACTIVE.value,
        )
        self._redis_client.delete(self._queue_key(uid))
        logger.debug('Closed mailbox for %s (%s)', uid, self)

    def get_mailbox(self, uid: Identifier) -> HybridMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: Identifier of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
        """
        status = self._redis_client.get(self._status_key(uid))
        if status is None:
            raise BadIdentifierError(uid)
        return HybridMailbox(uid, self)

    def _send_direct(self, address: str, message: Message) -> None:
        # TODO: what errors does this raise?
        self._socket_pool.send(address, message)
        logger.debug(
            'Sent %s to %s via p2p',
            type(message).__name__,
            message.dest,
        )

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        To send a message, the client first checks that the state of the
        mailbox in Redis is active; otherwise, an error is raised. Then,
        the client checks to see if the peer entity is available by
        checking for an address of the peer in Redis. If the peer's address
        is found, the message is sent directly to the peer via ZMQ; otherwise,
        the message is put in a Redis queue for later retrieval.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        address = self._address_cache.get(uid, None)
        if address is not None:
            try:
                # This is as optimistic as possible. If the address of the
                # peer is cached, we assume the mailbox is still active and
                # the peer is still listening.
                self._send_direct(address, message)
            except zmq.ZMQError:
                # Our optimism let us down so clear the cache and try the
                # standard flow.
                self._address_cache.pop(uid)
            else:
                return

        status = self._redis_client.get(self._status_key(uid))
        if status is None:
            raise BadIdentifierError(uid)
        elif status == _MailboxState.INACTIVE.value:
            raise MailboxClosedError(uid)

        maybe_address = self._redis_client.get(self._address_key(uid))
        try:
            # This branching is a little odd. We want to fall back to
            # Redis for message sending on two conditions: direct send fails
            # or no address was found. We raise a TypeError if no address
            # was found as a shortcut to get to the fall back.
            if isinstance(maybe_address, str):
                self._send_direct(maybe_address, message)
                self._address_cache[uid] = maybe_address
            else:
                raise TypeError
        except (TypeError, zmq.ZMQError):
            self._redis_client.rpush(
                self._queue_key(uid),
                message.model_dump_json(),
            )
            logger.debug(
                'Sent %s to %s via redis',
                type(message).__name__,
                uid,
            )


class HybridMailbox:
    """Client protocol that listens to incoming messages to a mailbox.

    This class acts as the endpoint for messages sent to a particular
    mailbox. This is done via starting two threads once initialized:
    (1) a ZMQ server thread that listens for messages from peers, and
    (2) a thread that checks the Redis server for any offline messages and
    state changes to the mailbox (i.e., mailbox closure).

    Args:
        uid: Identifier of the mailbox.
        exchange: Exchange client.
    """

    def __init__(self, uid: Identifier, exchange: HybridExchange) -> None:
        self._uid = uid
        self._exchange = exchange
        self._messages: Queue[Message] = Queue()

        self._closed = threading.Event()
        self._socket_poll_timeout_ms = _SOCKET_POLL_TIMEOUT_MS

        self._server_host = socket.gethostbyname(socket.gethostname())
        self._server_port: int | None = None
        self._server_running = threading.Event()
        self._server_thread = threading.Thread(target=self._zmq_server)
        self._server_thread.start()
        self._server_running.wait(timeout=_THREAD_START_TIMEOUT)

        self._redis_thread = threading.Thread(target=self._redis_watcher)
        self._redis_running = threading.Event()
        assert self._server_port is not None
        self.exchange._redis_client.set(
            self.exchange._address_key(uid),
            f'tcp://{self._server_host}:{self._server_port}',
        )
        self._redis_thread.start()
        self._redis_running.wait(timeout=_THREAD_START_TIMEOUT)

    @property
    def exchange(self) -> HybridExchange:
        """Exchange client."""
        return self._exchange

    @property
    def mailbox_id(self) -> Identifier:
        """Mailbox address/identifier."""
        return self._uid

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _pull_messages_from_redis(self, timeout: int = 1) -> None:
        # Note: we use blpop instead of lpop here for the timeout.
        raw = self.exchange._redis_client.blpop(
            [self.exchange._queue_key(self.mailbox_id)],
            timeout=timeout,
        )
        if raw is None:
            return

        # Only passed one key to blpop to result is [key, item]
        assert isinstance(raw, (tuple, list))
        assert len(raw) == 2  # noqa: PLR2004
        message = BaseMessage.model_from_json(raw[1])
        assert isinstance(message, get_args(Message))
        logger.debug(
            'Received %s to %s via redis',
            type(message).__name__,
            self.mailbox_id,
        )
        self._messages.put(message)

    def _redis_watcher(self) -> None:
        self._redis_running.set()
        logger.debug('Started redis watcher thread for %s', self.mailbox_id)
        try:
            while not self._closed.is_set():
                status = self.exchange._redis_client.get(
                    self.exchange._status_key(self.mailbox_id),
                )
                if status is None:  # pragma: no cover
                    raise AssertionError(
                        f'Status for mailbox {self.mailbox_id} did not exist '
                        'in Redis server. This means that something '
                        'incorrectly deleted the key.',
                    )
                elif status == _MailboxState.INACTIVE.value:
                    self._messages.close()
                    break

                self._pull_messages_from_redis(timeout=1)
        except Exception:
            logger.exception(
                'Error in redis watcher thread for %s',
                self.mailbox_id,
            )
        finally:
            logger.debug(
                'Stopped redis watcher thread for %s',
                self.mailbox_id,
            )

    def _zmq_server(self) -> None:
        context = zmq.Context()
        context.setsockopt(zmq.LINGER, 0)
        socket = context.socket(zmq.REP)
        host = f'tcp://{self._server_host}'
        self._server_port = socket.bind_to_random_port(host)
        self._server_running.set()
        logger.debug(
            'Started mailbox server thread for %s on %s:%s',
            self.mailbox_id,
            host,
            self._server_port,
        )

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        try:
            while not self._closed.is_set():
                sockets = dict(
                    poller.poll(timeout=self._socket_poll_timeout_ms),
                )
                if sockets.get(socket) != zmq.POLLIN:
                    continue

                message_json = socket.recv_string()
                message = BaseMessage.model_from_json(message_json)
                logger.debug(
                    'Received %s to %s via p2p',
                    type(message).__name__,
                    self.mailbox_id,
                )
                self._messages.put(message)

                socket.send_string(_MESSAGE_ACK)
        except Exception:
            logger.exception(
                'Error in mailbox server thread for %s',
                self.mailbox_id,
            )
        finally:
            self._server_running.clear()
            socket.close()
            context.term()
            logger.debug(
                'Stopped mailbox server thread for %s',
                self.mailbox_id,
            )

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        self._closed.set()
        self.exchange._redis_client.delete(
            self.exchange._address_key(self.mailbox_id),
        )

        self._server_thread.join(_THREAD_JOIN_TIMEOUT)
        if self._server_thread.is_alive():  # pragma: no cover
            raise TimeoutError(
                'Mailbox server thread failed to exit within '
                f'{_THREAD_JOIN_TIMEOUT} seconds.',
            )
        self._server_running.clear()

        self._redis_thread.join(_THREAD_JOIN_TIMEOUT)
        if self._redis_thread.is_alive():  # pragma: no cover
            raise TimeoutError(
                'Redis watcher thread failed to exit within '
                f'{_THREAD_JOIN_TIMEOUT} seconds.',
            )
        self._redis_running.clear()

        self._messages.close()

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed. Note that this will
                be cast to an int which is required by the Redis API.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        # TODO: check if _closed.is_set() and raise different error.
        try:
            return self._messages.get(timeout=timeout)
        except QueueClosedError:
            raise MailboxClosedError(self.mailbox_id) from None


class _ZMQSocketPool:
    def __init__(self) -> None:
        self._context = zmq.Context()
        self._sockets: dict[str, zmq.Socket[bytes]] = {}

    def close(self) -> None:
        for address in tuple(self._sockets.keys()):
            self.close_socket(address)
        self._context.destroy()

    def close_socket(self, address: str) -> None:
        socket = self._sockets.get(address, None)
        if socket is not None:  # pragma: no branch
            socket.close()

    def get_socket(self, address: str) -> zmq.Socket[bytes]:
        try:
            raise KeyError
            return self._sockets[address]
        except KeyError:
            socket = self._context.socket(zmq.REQ)
            socket.setsockopt(zmq.CONFLATE, 1)
            socket.connect(address)
            self._sockets[address] = socket
            print(f'created socket to {address}')
            return socket

    def send(self, address: str, message: Message) -> None:
        socket = self.get_socket(address)
        try:
            print(f'sending {address}')
            socket.send_string(message.model_dump_json())
            print('sent')
            socket.send_string('')
            response = socket.recv_string()
            print('recv')
        except zmq.ZMQError:
            self.close_socket(address)
            raise
        assert response == _MESSAGE_ACK


def uuid_to_base32(uid: uuid.UUID) -> str:
    """Encode a UUID as a trimmed base32 string."""
    uid_bytes = uid.bytes
    base32_bytes = base64.b32encode(uid_bytes).rstrip(b'=')
    return base32_bytes.decode('utf-8')
