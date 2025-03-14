from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import sys
import threading
import time
from types import TracebackType
from typing import Callable

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

logger = logging.getLogger(__name__)

_BAD_FILE_DESCRIPTOR_ERRNO = 9
TCP_CHUNK_SIZE = 1024
TCP_MESSAGE_DELIM = b'\r\n'


class SocketClosedError(Exception):
    """Socket is already closed."""

    pass


class SocketOpenError(Exception):
    """Failed to open socket."""

    pass


class SimpleSocket:
    """Simple socket wrapper.

    Configures a client connection using a blocking TCP socket over IPv4.
    The send and recv methods handle byte encoding, message delimiters, and
    partial message buffering.

    Note:
        This class can be used as a context manager.

    Args:
        host: Host address to connect to.
        port: Port to connect to.
        timeout: Connection establish timeout.

    Raises:
        SocketOpenError: if creating the socket fails. The `__cause__` of
            the exception will be set to the underlying `OSError`.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        timeout: float | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.closed = False
        try:
            self.socket = socket.create_connection(
                (self.host, self.port),
                timeout=self.timeout,
            )
        except OSError as e:
            raise SocketOpenError() from e
        # Stores leftover bytes after delimiter from last call to recv
        # to be used by the next call to recv
        self._buffer = bytearray()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}(host={self.host}, port={self.port})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.host}:{self.port}>'

    def close(self, shutdown: bool = True) -> None:
        """Close the socket."""
        if self.closed:
            return
        if shutdown:  # pragma: no branch
            with contextlib.suppress(OSError):
                # Some platforms may raise ENOTCONN here
                self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.closed = True

    def send(self, message: bytes) -> None:
        """Send bytes to the socket.

        Note:
            This is a noop if the message is empty.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        message_size = len(message)
        sent_size = 0

        while sent_size < message_size:
            if sent_size + TCP_CHUNK_SIZE < message_size:
                payload = message[sent_size : sent_size + TCP_CHUNK_SIZE]
            else:
                payload = message[sent_size:]
                payload += TCP_MESSAGE_DELIM
            try:
                self.socket.sendall(payload)
            except OSError as e:
                if e.errno == _BAD_FILE_DESCRIPTOR_ERRNO:
                    raise SocketClosedError() from e
                else:
                    raise
            else:
                sent_size += TCP_CHUNK_SIZE

    def send_string(self, message: str) -> None:
        """Send a string to the socket.

        Strings are encoded with UTF-8.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        self.send(message.encode('utf-8'))

    def recv(self) -> bytes:
        """Receive the next message from the socket.

        Returns:
            Bytes containing the message.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        buffer = self._buffer

        # If our prior buffer already contains a complete message,
        # we don't need to call recv()
        if TCP_MESSAGE_DELIM not in buffer:
            while True:
                payload = _recv_from_socket(self.socket)
                buffer.extend(payload)
                if TCP_MESSAGE_DELIM in payload:
                    break

        message, delim, new = buffer.partition(TCP_MESSAGE_DELIM)
        assert delim == TCP_MESSAGE_DELIM
        self._buffer = new

        return bytes(message)

    def recv_string(self) -> str:
        """Receive the next message from the socket.

        Returns:
            Message decoded as a UTF-8 string.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        return self.recv().decode('utf-8')


class SimpleSocketServer:
    """Simple asyncio TCP socket server.

    Args:
        handler: Callback that handles a message and returns the response
            string. The handler is called synchronously within the client
            handler so it should not perform any heavy/blocking operations.
        host: Host to bind to.
        port: Port to bind to. If `None`, a random port is bound to.
        timeout: Seconds to wait for the server to startup and shutdown.
    """

    def __init__(
        self,
        handler: Callable[[bytes], bytes],
        *,
        host: str = '0.0.0.0',
        port: int | None = None,
        timeout: float | None = 5,
    ) -> None:
        self.host = host
        self.port = port if port is not None else open_port()
        self.handler = handler
        self.timeout = timeout
        self._started = threading.Event()
        self._signal_stop: asyncio.Future[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._client_tasks: set[asyncio.Task[None]] = set()

    async def _write_message(
        self,
        writer: asyncio.StreamWriter,
        message: bytes,
    ) -> None:
        message_size = len(message)
        sent_size = 0

        while sent_size < message_size:
            if sent_size + TCP_CHUNK_SIZE < message_size:
                payload = message[sent_size : sent_size + TCP_CHUNK_SIZE]
            else:
                payload = message[sent_size:]
                payload += TCP_MESSAGE_DELIM
            writer.write(payload)
            sent_size += TCP_CHUNK_SIZE

        await writer.drain()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            buffer = bytearray()
            while not reader.at_eof():
                try:
                    raw = await reader.read(TCP_CHUNK_SIZE)
                except asyncio.IncompleteReadError:  # pragma: no cover
                    reader.feed_eof()
                    continue
                else:
                    buffer.extend(raw)
                    if TCP_MESSAGE_DELIM not in raw:
                        continue

                if len(buffer) == 0:  # pragma: no cover
                    break

                while True:
                    message, delim, new = buffer.partition(TCP_MESSAGE_DELIM)
                    response = self.handler(message)
                    await self._write_message(writer, response)
                    buffer = new
                    if TCP_MESSAGE_DELIM not in new:
                        break
        except Exception:  # pragma: no cover
            logger.exception('Error in client handler task.')
            raise
        finally:
            writer.close()
            await writer.wait_closed()

    async def _create_client_task(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.create_task(self._handle_client(reader, writer))
        self._client_tasks.add(task)
        task.add_done_callback(self._client_tasks.discard)

    async def serve_forever(self, stop: asyncio.Future[None]) -> None:
        """Accept and handles connections forever.

        Args:
            stop: An asyncio future that this method blocks on. Can be used
                to signal externally that the coroutine should exit.
        """
        self._signal_stop = stop
        server = await asyncio.start_server(
            self._create_client_task,
            host=self.host,
            port=self.port,
        )
        logger.debug('TCP server listening at %s:%s', self.host, self.port)
        self._started.set()

        async with server:
            await server.start_serving()
            await self._signal_stop

            for task in tuple(self._client_tasks):
                task.cancel('Server has been closed.')
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if sys.version_info >= (3, 13):  # pragma: >=3.13 cover
            server.close_clients()
        self._started.clear()
        logger.debug('TCP server finished at %s:%s', self.host, self.port)

    def start_server_thread(self) -> None:
        """Start the server in a new thread."""
        with self._lock:
            loop = asyncio.new_event_loop()
            stop = loop.create_future()

            def _target() -> None:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.serve_forever(stop))
                loop.close()

            self._loop = loop
            self._thread = threading.Thread(
                target=_target,
                name='socket-server',
            )
            self._thread.start()
            self._started.wait(self.timeout)

    def stop_server_thread(self) -> None:
        """Stop the server thread."""
        with self._lock:
            if self._loop is None or self._thread is None:
                return
            assert self._signal_stop is not None
            self._loop.call_soon_threadsafe(self._signal_stop.set_result, None)
            self._thread.join(timeout=self.timeout)
            if self._thread.is_alive():  # pragma: no cover
                raise TimeoutError(
                    'Server thread did not gracefully exit '
                    f'within {self.timeout}s.',
                )
            self._loop = None
            self._thread = None


def _recv_from_socket(
    socket: socket.socket,
    nbytes: int = TCP_CHUNK_SIZE,
) -> bytes:
    while True:
        try:
            payload = socket.recv(nbytes)
        except BlockingIOError:
            continue
        except OSError as e:
            if e.errno == _BAD_FILE_DESCRIPTOR_ERRNO:
                raise SocketClosedError() from e
            else:
                raise

        if payload == b'':
            raise SocketClosedError()

        return payload


_used_ports: set[int] = set()


def open_port() -> int:
    """Return open port.

    Source: https://stackoverflow.com/questions/2838244
    """
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        if port not in _used_ports:  # pragma: no branch
            _used_ports.add(port)
            return port


def wait_connection(
    host: str,
    port: int,
    *,
    sleep: float = 0.01,
    timeout: float | None = None,
) -> None:
    """Wait for a socket connection to be established.

    Repeatedly tries to open and close a socket connection to `host:port`.
    If successful, the function returns. If unsuccessful before the timeout,
    a `TimeoutError` is raised. The function will sleep for `sleep` seconds
    in between successive connection attempts.

    Args:
        host: Host address to connect to.
        port: Host port to connect to.
        sleep: Seconds to sleep after unsuccessful connections.
        timeout: Maximum number of seconds to wait for successful connections.
    """
    sleep = min(sleep, timeout) if timeout is not None else sleep
    waited = 0.0

    while True:
        try:
            start = time.perf_counter()
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as e:
            connection_time = time.perf_counter() - start
            waited += connection_time
            if timeout is not None and waited >= timeout:
                raise TimeoutError from e
            time.sleep(sleep)
            waited += sleep
