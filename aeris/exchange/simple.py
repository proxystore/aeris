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
import io
import logging
import pickle
import signal
import socket
import sys
import threading
import uuid
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
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.exchange.queue import AsyncQueue
from aeris.exchange.queue import QueueClosedError
from aeris.identifier import Identifier
from aeris.message import Message

logger = logging.getLogger(__name__)

DEFAULT_SERVER_TIMEOUT = 30


class _BadRequestError(Exception):
    pass


class _ExchangeMessageType(enum.Enum):
    CREATE_MAILBOX = 'create-mailbox'
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

    mid: uuid.UUID = Field(default_factory=uuid.uuid4)
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
            mid=self.mid,
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
        timeout: float = DEFAULT_SERVER_TIMEOUT,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout = timeout

        self._socket = socket.create_connection(
            (self.hostname, self.port),
            timeout=self.timeout,
        )
        self._socket.setblocking(False)
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
            logger.debug(
                'Received message from server (exchange=%s, message=%r)',
                self,
                message,
            )
            if message.success:
                self._pending[message.mid].set_result(message)
            else:
                assert message.error is not None
                self._pending[message.mid].set_exception(message.error)
        else:
            logger.warning(
                'Dropping bad message from server with type %s (exchange=%s)',
                type(message).__name__,
                self,
            )

    def _listen_server_messages(self) -> None:
        logging.debug('Listening for server messages in %s', self)
        buffer = io.BytesIO()
        while True:
            try:
                raw = self._socket.recv(1024)
            except BlockingIOError:
                continue
            except OSError:
                break

            if len(raw) == 0:  # pragma: no cover
                break

            buffer.write(raw)
            buffer.seek(0)
            start_index = 0
            for line in buffer:
                start_index += len(line)

                raw = line.strip()
                if len(raw) == 0:  # pragma: no cover
                    continue

                try:
                    message = _BaseExchangeMessage.model_deserialize(raw)
                except Exception:
                    logger.exception(
                        'Failed to deserialize message from server in %s',
                        self,
                    )
                    return
                self._handle_message(message)

            if start_index > 0:
                buffer.seek(start_index)
                remaining = buffer.read()
                buffer.truncate(0)
                buffer.seek(0)
                buffer.write(remaining)
            else:  # pragma: no cover
                buffer.seek(0, 2)
        buffer.close()
        logging.debug('Finished listening for server messages in %s', self)

    def _send_request(
        self,
        request: _ExchangeRequestMessage,
    ) -> _ExchangeResponseMessage:
        future: Future[_ExchangeResponseMessage] = Future()
        self._pending[request.mid] = future
        self._socket.send(request.model_serialize() + b'\n')
        logger.debug(
            'Sent message to server (exchange=%s, message=%r)',
            self,
            request,
        )
        response = future.result(timeout=self.timeout)
        del self._pending[request.mid]
        return response

    def close(self) -> None:
        """Close this exchange client."""
        self._socket.close()
        self._handler_thread.join(timeout=1)
        logger.debug('Closed exchange client %s', self)

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
        response = self._send_request(request)
        assert response.success

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
        response = self._send_request(request)
        assert response.success

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
        request = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.REQUEST_MESSAGE,
            src=uid,
        )
        response = self._send_request(request)
        assert response.success
        assert response.payload is not None
        return response.payload


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[Identifier, AsyncQueue[Message]] = {}

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

    async def _handle_request(
        self,
        message: _ExchangeRequestMessage,
    ) -> _ExchangeResponseMessage:
        logger.debug('Handling server request (%s)', message)
        if message.kind is _ExchangeMessageType.CREATE_MAILBOX:
            self.manager.create_mailbox(message.src)
            response = message.response()
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
        response = await self._handle_request(request)
        encoded = response.model_serialize()
        writer.write(encoded + b'\n')
        await writer.drain()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.debug('Started new client handler')
        while not reader.at_eof():
            raw = await reader.readline()
            if raw == b'':
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
                'Server listening on %s:%s (ctrl-C to exit)',
                self.host,
                self.port,
            )
            await stop
            logger.info('Closing server...')
            for task in tuple(self._handle_request_tasks):  # pragma: no cover
                task.cancel('Server has been closed.')
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
            server.close_clients()


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


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--log-level', default='INFO')

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    logging.basicConfig(
        format='[%(asctime)s] %(levelname)-5s (%(name)s) :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=args.log_level,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    server = SimpleServer(host=args.host, port=args.port)
    asyncio.run(serve_forever(server))

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
