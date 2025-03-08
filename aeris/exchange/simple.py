"""Simple message exchange client and server.

To start the exchange:
```bash
python -m aeris.exchange.simple --host localhost --port 1234
```

Connect to the exchange through the client.
```python
from aeris.exchange.simple import SimpleExchange

with SimpleExchange('localhost', 1234) as exchange:
    aid = exchange.register_agent()
    mailbox = exchange.get_mailbox(aid)
    ...
    mailbox.close()
```
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import enum
import logging
import multiprocessing
import pickle
import signal
import sys
import threading
import time
import uuid
from collections.abc import Generator
from collections.abc import Sequence
from concurrent.futures import Future
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import TypeAdapter

from aeris.exception import BadIdentifierError
from aeris.exception import ExchangeClosedError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.exchange.queue import AsyncQueue
from aeris.exchange.queue import QueueClosedError
from aeris.identifier import Identifier
from aeris.logging import init_logging
from aeris.message import Message
from aeris.socket import SimpleSocket
from aeris.socket import SocketClosedError
from aeris.socket import TCP_MESSAGE_DELIM
from aeris.socket import wait_connection

logger = logging.getLogger(__name__)

DEFAULT_SERVER_TIMEOUT = 30


class _BadRequestError(Exception):
    pass


class _ExchangeMessageType(enum.Enum):
    CREATE_MAILBOX = 'create-mailbox'
    CHECK_MAILBOX = 'check-mailbox'
    CLOSE_MAILBOX = 'close-mailbox'
    SEND_MESSAGE = 'send-message'
    REQUEST_MESSAGE = 'request-message'


class _BaseExchangeMessage(BaseModel):
    """Base exchange message."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        frozen=True,
        use_enum_values=False,
        validate_default=True,
    )

    tag: uuid.UUID = Field(default_factory=uuid.uuid4)
    kind: _ExchangeMessageType = Field()
    src: Identifier = Field()
    dest: Optional[Identifier] = Field(None)  # noqa: UP007
    payload: Optional[Message] = Field(None)  # noqa: UP007

    @classmethod
    def model_deserialize(cls, raw: bytes) -> _ExchangeMessage:
        dump = raw.decode()
        return TypeAdapter(_ExchangeMessage).validate_json(dump)

    def model_serialize(self) -> bytes:
        dump = self.model_dump_json()
        return dump.encode()


class _ExchangeRequestMessage(_BaseExchangeMessage):
    mtype: Literal['request'] = Field('request', repr=False)

    def response(
        self,
        *,
        error: Exception | None = None,
        payload: Message | None = None,
    ) -> _ExchangeResponseMessage:
        return _ExchangeResponseMessage(
            tag=self.tag,
            kind=self.kind,
            src=self.src,
            dest=self.dest,
            error=error,
            payload=payload,
        )


class _ExchangeResponseMessage(_BaseExchangeMessage):
    mtype: Literal['response'] = Field('response', repr=False)
    error: Optional[Exception] = Field(None)  # noqa: UP007

    @field_serializer('error', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> str:
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('error', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, _ExchangeResponseMessage):
            return False
        self_dump = self.model_dump()
        other_dump = other.model_dump()
        return (
            isinstance(self_dump.pop('error'), type(other_dump.pop('error')))
            and self_dump == other_dump
        )

    @property
    def success(self) -> bool:
        return self.error is None


_ExchangeMessage = Union[_ExchangeRequestMessage, _ExchangeResponseMessage]


class SimpleExchange(ExchangeMixin):
    """Simple exchange client.

    Args:
        hostname: Host name of the exchange server.
        port: Port of the exchange server.
        timeout: Timeout when waiting for server responses.
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        timeout: float | None = DEFAULT_SERVER_TIMEOUT,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout = timeout

        self._socket = SimpleSocket(
            self.hostname,
            self.port,
            timeout=self.timeout,
        )
        self._handler_thread = threading.Thread(
            target=self._listen_server_messages,
            name=f'{self}-message-handler',
        )
        self._handler_thread.start()

        self._pending: dict[uuid.UUID, Future[_ExchangeResponseMessage]] = {}

    def __reduce__(
        self,
    ) -> tuple[type[Self], tuple[str, int]]:
        return (type(self), (self.hostname, self.port))

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(hostname={self.hostname}, '
            f'port={self.port}, timeout={self.timeout})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.hostname}:{self.port}>'

    def _handle_message(
        self,
        message: _ExchangeMessage,
    ) -> None:
        if isinstance(message, _ExchangeResponseMessage):
            if message.success:
                self._pending[message.tag].set_result(message)
            else:
                assert message.error is not None
                self._pending[message.tag].set_exception(message.error)
        else:
            logger.warning(
                'Dropping bad message from %s with type %s',
                type(message).__name__,
                self,
            )

    def _listen_server_messages(self) -> None:
        logging.debug('Listening for messages from %s', self)
        while True:
            try:
                raw = self._socket.recv()
            except SocketClosedError:
                break
            except OSError:
                logger.exception(
                    'Error when listening for messages from %s',
                    self,
                )
                break

            try:
                message = _BaseExchangeMessage.model_deserialize(raw)
            except Exception:  # pragma: no cover
                logger.exception(
                    'Failed to deserialize message from %s',
                    self,
                )
                break
            else:
                self._handle_message(message)

        logging.debug('Finished listening for messages from %s', self)

    def _send_request(
        self,
        request: _ExchangeRequestMessage,
        timeout: float | None = None,
    ) -> _ExchangeResponseMessage:
        future: Future[_ExchangeResponseMessage] = Future()
        self._pending[request.tag] = future
        self._socket.send(request.model_serialize())
        response = future.result(timeout=timeout)
        del self._pending[request.tag]
        return response

    def close(self) -> None:
        """Close this exchange client."""
        timeout = 5
        self._socket.close()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ExchangeClosedError())
        self._handler_thread.join(timeout=timeout)
        if self._handler_thread.is_alive():
            raise TimeoutError(
                'Exchange message listener thread did not exit '
                f'within {timeout} seconds.',
            )
        logger.debug('Closed exchange (%s)', self)

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=uid,
        )
        response = self._send_request(request, timeout=self.timeout)
        assert response.success
        logger.debug('Created mailbox for %s (%s)', uid, self)

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exists.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CLOSE_MAILBOX,
            src=uid,
        )
        response = self._send_request(request, timeout=self.timeout)
        assert response.success
        logger.debug('Closed mailbox for %s (%s)', uid, self)

    def get_mailbox(self, uid: Identifier) -> SimpleMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: Identifier of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
        """
        return SimpleMailbox(uid, self, timeout=self.timeout)

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.SEND_MESSAGE,
            src=message.src,
            dest=uid,
            payload=message,
        )
        response = self._send_request(request)
        assert response.success
        logger.debug('Sent %s to %s', type(message).__name__, uid)


class SimpleMailbox:
    """Client protocol that listens to incoming messages to a mailbox.

    Args:
        uid: Identifier of the mailbox.
        exchange: Exchange client.
        timeout: Optional timeout when querying mailbox status in exchange.

    Raises:
        BadIdentifierError: if a mailbox with `uid` does not exist.
    """

    def __init__(
        self,
        uid: Identifier,
        exchange: SimpleExchange,
        *,
        timeout: float | None = None,
    ) -> None:
        self._uid = uid
        self._exchange = exchange

        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CHECK_MAILBOX,
            src=self._uid,
        )
        # This will raise BadIdentifierError if the mailbox does not exist.
        response = self._exchange._send_request(request, timeout=timeout)
        assert response.success

    @property
    def exchange(self) -> SimpleExchange:
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
        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.REQUEST_MESSAGE,
            src=self.mailbox_id,
        )
        try:
            response = self.exchange._send_request(request, timeout=timeout)
        except ExchangeClosedError as e:
            raise MailboxClosedError(self.mailbox_id) from e
        assert response.success
        assert response.payload is not None
        logger.debug(
            'Received %s to %s',
            type(response).__name__,
            self.mailbox_id,
        )
        return response.payload


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[Identifier, AsyncQueue[Message]] = {}

    def check_mailbox(self, uid: Identifier) -> bool:
        return uid in self._mailboxes

    def create_mailbox(self, uid: Identifier) -> None:
        if uid not in self._mailboxes or self._mailboxes[uid].closed():
            self._mailboxes[uid] = AsyncQueue()
            logger.info('Created mailbox for %s', uid)

    async def close_mailbox(self, uid: Identifier) -> None:
        mailbox = self._mailboxes.get(uid, None)
        if mailbox is not None:
            await mailbox.close()
            logger.info('Closed mailbox for %s', uid)

    async def get(self, uid: Identifier) -> Message:
        try:
            return await self._mailboxes[uid].get()
        except KeyError as e:
            raise BadIdentifierError(uid) from e
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e

    async def put(self, message: Message) -> None:
        try:
            await self._mailboxes[message.dest].put(message)
        except KeyError as e:
            raise BadIdentifierError(message.dest) from e
        except QueueClosedError as e:
            raise MailboxClosedError(message.dest) from e


class SimpleServer:
    """Simple asyncio mailbox exchange server.

    Args:
        host: Host interface to bind to.
        port: Port to bind to.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.manager = _MailboxManager()
        self._handle_request_tasks: set[asyncio.Task[None]] = set()

    def __repr__(self) -> str:
        return f'{type(self).__name__}(hostname={self.host}, port={self.port})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.host}:{self.port}>'

    async def _handle_request(  # noqa: C901,PLR0912
        self,
        message: _ExchangeRequestMessage,
    ) -> _ExchangeResponseMessage:
        if message.kind is _ExchangeMessageType.CREATE_MAILBOX:
            self.manager.create_mailbox(message.src)
            response = message.response()
        elif message.kind is _ExchangeMessageType.CHECK_MAILBOX:
            if self.manager.check_mailbox(message.src):
                response = message.response()
            else:
                id_error = BadIdentifierError(message.src)
                response = message.response(error=id_error)
        elif message.kind is _ExchangeMessageType.CLOSE_MAILBOX:
            await self.manager.close_mailbox(message.src)
            response = message.response()
        elif message.kind is _ExchangeMessageType.SEND_MESSAGE:
            if message.dest is None or message.payload is None:
                error = _BadRequestError(
                    'Dest identifier and payload message must be specified.',
                )
                response = message.response(error=error)
            else:
                try:
                    await self.manager.put(message.payload)
                except Exception as e:
                    response = message.response(error=e)
                else:
                    response = message.response()
        elif message.kind is _ExchangeMessageType.REQUEST_MESSAGE:
            try:
                next_message = await self.manager.get(message.src)
            except Exception as e:
                response = message.response(error=e)
            else:
                response = message.response(payload=next_message)
        else:
            raise AssertionError('Unreachable.')

        return response

    async def _handle_request_task(
        self,
        request: _ExchangeRequestMessage,
        writer: asyncio.StreamWriter,
    ) -> None:
        start = time.perf_counter()
        logger.debug('Handling server request (%s)', request)
        response = await self._handle_request(request)
        encoded = response.model_serialize()
        writer.write(encoded + TCP_MESSAGE_DELIM)
        await writer.drain()
        logger.debug(
            'Completed server request in %.1f ms (%s)',
            1000 * (time.perf_counter() - start),
            response,
        )

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.debug('Started new client handler')
        while not reader.at_eof():
            try:
                raw = await reader.readuntil(TCP_MESSAGE_DELIM)
            except asyncio.IncompleteReadError:
                reader.feed_eof()
                continue

            try:
                message = _BaseExchangeMessage.model_deserialize(raw)
            except Exception:
                logger.exception('Failed to parse message from client')
                break

            if isinstance(message, _ExchangeRequestMessage):
                # Handle requests in separate tasks so we don't block the
                # handle client coroutine.
                task = asyncio.create_task(
                    self._handle_request_task(message, writer),
                )
                self._handle_request_tasks.add(task)
                task.add_done_callback(self._handle_request_tasks.discard)
            else:
                logger.warning(
                    'Dropping bad message with type %s',
                    type(message).__name__,
                )

        writer.close()
        await writer.wait_closed()
        logger.debug('Finished client handler')

    async def serve_forever(self, stop: asyncio.Future[None]) -> None:
        """Accept and handles connections forever."""
        server = await asyncio.start_server(
            self._handle_client,
            host=self.host,
            port=self.port,
        )

        async with server:
            await server.start_serving()
            logger.info(
                'Exchange listening on %s:%s (ctrl-C to exit)',
                self.host,
                self.port,
            )
            await stop
            logger.info('Closing exchange...')
            for task in tuple(self._handle_request_tasks):  # pragma: no cover
                task.cancel('Exchange has been closed.')
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
            server.close_clients()
        logger.info('Closed exchange!')


async def serve_forever(
    server: SimpleServer,
    stop: asyncio.Future[None] | None = None,
) -> None:
    """Serve the exchange forever until a stop signal is received.

    This function registers signal handlers for SIGINT and SIGTERM.

    Args:
        server: Server instance to run.
        stop: Optional future that when set will indicate that the server
            should stop.
    """
    loop = asyncio.get_running_loop()
    stop = loop.create_future() if stop is None else stop
    # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    logger.debug('Registered signal handlers for SIGINT and SIGTERM')

    await server.serve_forever(stop)

    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)


def _run(
    host: str,
    port: int,
    *,
    level: str | int = logging.INFO,
    logfile: str | None = None,
) -> None:
    init_logging(level, logfile=logfile)
    server = SimpleServer(host=host, port=port)
    asyncio.run(serve_forever(server))


@contextlib.contextmanager
def spawn_simple_exchange(
    host: str = '0.0.0.0',
    port: int = 5463,
    *,
    level: int | str = logging.WARNING,
    timeout: float | None = None,
) -> Generator[SimpleExchange]:
    """Context manager that spawns a simple exchange in a subprocess.

    This function spawns a new process (rather than forking) and wait to
    return until a connection with the exchange has been established.
    When exiting the context manager, `SIGINT` will be sent to the exchange
    process. If the process does not exit within 5 seconds, it will be
    killed.

    Args:
        host: Host the exchange should listen on.
        port: Port the exchange should listen on.
        level: Logging level.
        timeout: Connection timeout when waiting for exchange to start.

    Returns:
        Exchange interface connected to the spawned exchange.
    """
    # Fork is not safe in multi-threaded context.
    multiprocessing.set_start_method('spawn')

    exchange_process = multiprocessing.Process(
        target=_run,
        args=(host, port),
        kwargs={'level': level},
    )
    exchange_process.start()

    logger.info('Starting exchange server...')
    wait_connection(host, port, timeout=timeout)
    logger.info('Started exchange server!')

    try:
        with SimpleExchange(host, port, timeout=timeout) as exchange:
            yield exchange
    finally:
        logger.info('Terminating exchange server...')
        wait = 5
        exchange_process.terminate()
        exchange_process.join(timeout=wait)
        if exchange_process.exitcode is None:  # pragma: no cover
            logger.info(
                'Killing exchange server after waiting %s seconds',
                wait,
            )
            exchange_process.kill()
        else:
            logger.info('Terminated exchange server!')
        exchange_process.close()


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--log-level', default='INFO')

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    _run(args.host, args.port, level=args.log_level)

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
