from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import io
import logging
import queue
import signal
import socket
import sys
import threading
from collections.abc import AsyncGenerator
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import cast
from typing import Generic
from typing import get_args
from typing import TypeVar

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

logger = logging.getLogger(__name__)

T = TypeVar('T')

DEFAULT_PRIORITY = 0
CLOSE_PRIORITY = DEFAULT_PRIORITY + 1
CLOSE_SENTINAL = object()


@dataclasses.dataclass(order=True)
class _QueueItem(Generic[T]):
    priority: int
    message: T | object = dataclasses.field(compare=False)


class SimpleMailbox:
    """Thread-safe queue-based mailbox."""

    def __init__(self, uid: Identifier, exchange: SimpleExchange) -> None:
        self.uid = uid
        self._exchange = exchange
        self._queue: queue.PriorityQueue[_QueueItem[Message]] = (
            queue.PriorityQueue()
        )
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
        self._queue.put(_QueueItem(DEFAULT_PRIORITY, message))

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
            self._queue.put(_QueueItem(CLOSE_PRIORITY, CLOSE_SENTINAL))
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


class _AsyncQueue(Generic[T]):
    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[_QueueItem[T]] = (
            asyncio.PriorityQueue()
        )
        self._closed = False

    async def close(self, immediate: bool = False) -> None:
        if not self.closed():
            self._closed = True
            priority = CLOSE_PRIORITY if immediate else DEFAULT_PRIORITY
            await self._queue.put(_QueueItem(priority, CLOSE_SENTINAL))

    def closed(self) -> bool:
        return self._closed

    async def get(self) -> T:
        item = await self._queue.get()
        if item.message is CLOSE_SENTINAL:
            raise MailboxClosedError
        return cast(T, item.message)

    async def put(self, message: T) -> None:
        if self.closed():
            raise MailboxClosedError
        await self._queue.put(_QueueItem(DEFAULT_PRIORITY, message))

    async def subscribe(self) -> AsyncGenerator[T]:
        while True:
            try:
                yield await self.get()
            except MailboxClosedError:
                return


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[Identifier, _AsyncQueue[ForwardMessage]] = {}

    def register(self, uid: Identifier) -> None:
        if uid not in self._mailboxes or self._mailboxes[uid].closed():
            # If the old mailbox was closed, it gets thrown away.
            self._mailboxes[uid] = _AsyncQueue()

    async def unregister(self, uid: Identifier) -> None:
        if uid in self._mailboxes:
            await self._mailboxes[uid].close(immediate=True)

    async def send(self, message: ForwardMessage) -> None:
        try:
            await self._mailboxes[message.dest].put(message)
        except KeyError as e:
            raise BadIdentifierError(
                f'No mailbox associated with {message.dest}',
            ) from e

    async def subscribe(
        self,
        uid: Identifier,
    ) -> AsyncGenerator[ForwardMessage]:
        try:
            return self._mailboxes[uid].subscribe()
        except KeyError as e:
            raise BadIdentifierError(
                f'No mailbox associated with {uid}',
            ) from e


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
        self._subscriber_tasks: dict[Identifier, asyncio.Task[None]] = {}

    async def _subscribe(
        self,
        uid: Identifier,
        writer: asyncio.StreamWriter,
    ) -> None:
        messages = await self.manager.subscribe(uid)
        logger.info('Started subscriber task for %s', uid)

        while not writer.is_closing():
            try:
                message = await asyncio.wait_for(
                    messages.__anext__(),
                    timeout=1,
                )
            except asyncio.TimeoutError:
                continue
            except StopAsyncIteration:
                break

            encoded = message.model_serialize()
            writer.write(encoded)
            writer.write(b'\n')
            try:
                await writer.drain()
                logger.debug('Sent message to %s: %s', uid, message)
            except OSError:
                logger.warning(
                    'Failed to send message to %s: %s',
                    uid,
                    message,
                )

        writer.close()
        await writer.wait_closed()
        logger.info('Exited subscriber task for %s', uid)

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.debug('Started new client handle')
        while not reader.at_eof():
            raw = await reader.readline()
            if raw == b'':
                reader.feed_eof()
                continue

            message = BaseExchangeMessage.model_deserialize(raw)
            logger.debug('Received: %s', message)
            response: ResponseMessage | None = None

            if isinstance(message, ForwardMessage):
                await self.manager.send(message)
                response = message.response()
            elif isinstance(message, RegisterMessage):
                self.manager.register(message.src)
                task = asyncio.create_task(
                    self._subscribe(message.src, writer),
                    name=f'{message.src.uid}-subscriber',
                )
                self._subscriber_tasks[message.src] = task
                logger.info('Registered client %s', message.src)
                response = message.response()
            elif isinstance(message, UnregisterMessage):
                await self.manager.unregister(message.src)
                task = self._subscriber_tasks.pop(message.src)
                await task
                logger.info('Unregistered client %s', message.src)
                response = message.response()
            else:
                logger.warning('Unhandled message type: %s', type(message))
                break

            if response is not None:
                encoded = response.model_serialize()
                writer.write(encoded)
                writer.write(b'\n')
                await writer.drain()

        writer.close()
        await writer.wait_closed()
        logger.info('Exited client handle')

    async def serve_forever(
        self,
        stop: asyncio.Future[None] | None = None,
    ) -> None:
        """Accept and handles connections forever.

        This method registered signal handlers for SIGINT and SIGTERM
        for gracefully closing the server.
        """
        server = await asyncio.start_server(
            self._handle,
            host=self.host,
            port=self.port,
        )

        # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
        loop = asyncio.get_running_loop()
        stop = loop.create_future() if stop is None else stop
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
        logger.debug('Registered signal handlers for SIGINT and SIGTERM')

        async with server:
            await server.start_serving()
            logger.info(
                'Server listening on %s:%s (ctrl-C to exit)',
                self.host,
                self.port,
            )
            await stop
            logger.info('Closing server...')
            for task in self._subscriber_tasks.values():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
            server.close_clients()

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
    asyncio.run(server.serve_forever())

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
