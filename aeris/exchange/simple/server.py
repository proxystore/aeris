from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import logging
import signal
import sys
from collections.abc import AsyncGenerator
from collections.abc import Sequence
from typing import cast
from typing import Generic
from typing import TypeVar

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange.message import BaseExchangeMessage
from aeris.exchange.message import ForwardMessage
from aeris.exchange.message import RegisterMessage
from aeris.exchange.message import ResponseMessage
from aeris.exchange.message import UnregisterMessage
from aeris.identifier import Identifier

T = TypeVar('T')

DEFAULT_PRIORITY = 0
CLOSE_PRIORITY = DEFAULT_PRIORITY + 1
CLOSE_SENTINAL = object()

logger = logging.getLogger(__name__)


@dataclasses.dataclass(order=True)
class _QueueItem(Generic[T]):
    priority: int
    message: T | object = dataclasses.field(compare=False)


class _AsyncMailbox(Generic[T]):
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

    async def recv(self) -> T:
        item = await self._queue.get()
        if item.message is CLOSE_SENTINAL:
            raise MailboxClosedError
        return cast(T, item.message)

    async def send(self, message: T) -> None:
        if self.closed():
            raise MailboxClosedError
        await self._queue.put(_QueueItem(DEFAULT_PRIORITY, message))

    async def subscribe(self) -> AsyncGenerator[T]:
        while True:
            try:
                yield await self.recv()
            except MailboxClosedError:
                return


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[Identifier, _AsyncMailbox[ForwardMessage]] = {}

    def register(self, uid: Identifier) -> None:
        if uid not in self._mailboxes or self._mailboxes[uid].closed():
            # If the old mailbox was closed, it gets thrown away.
            self._mailboxes[uid] = _AsyncMailbox()

    async def unregister(self, uid: Identifier) -> None:
        if uid in self._mailboxes:
            await self._mailboxes[uid].close(immediate=True)

    async def send(self, message: ForwardMessage) -> None:
        try:
            await self._mailboxes[message.dest].send(message)
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


class MailboxServer:
    """Async mailbox exchange server.

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

    server = MailboxServer(host=args.host, port=args.port)
    asyncio.run(server.serve_forever())

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
