from __future__ import annotations

import contextlib
import socket
import socketserver
import sys
import threading
import time
from types import TracebackType
from typing import Any
from typing import Callable

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


_BAD_FILE_DESCRIPTOR_ERRNO = 9
TCP_MESSAGE_DELIM = b'\r\n'


class SocketClosedError(Exception):
    """Socket is already closed."""

    pass


class SocketOpenError(Exception):
    """Failed to open socket."""

    pass


class SimpleSocket:
    """Simple socket wrapper.

    Configures a client connection using a nonblocking TCP socket over IPv4.
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
            self.socket.setblocking(False)
        except OSError as e:
            raise SocketOpenError() from e
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
            self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.closed = True

    def send(self, message: bytes) -> None:
        """Send bytes to the socket.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        payload = message + TCP_MESSAGE_DELIM
        try:
            self.socket.sendall(payload)
        except OSError as e:
            if e.errno == _BAD_FILE_DESCRIPTOR_ERRNO:
                raise SocketClosedError() from e
            else:
                raise

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
        while True:
            payload = _recv_from_socket(self.socket)
            current, delim, new = payload.partition(TCP_MESSAGE_DELIM)
            assert len(current) > 0
            self._buffer.extend(current)
            if delim != b'':
                message = bytes(self._buffer)
                self._buffer = bytearray(new)
                break

        return message

    def recv_string(self) -> str:
        """Receive the next message from the socket.

        Returns:
            Message decoded as a UTF-8 string.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        return self.recv().decode('utf-8')


class _SimpleSocketServerHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        buffer = bytearray()
        while True:
            try:
                payload = _recv_from_socket(self.request)
            except SocketClosedError:
                self.request.close()
                return

            current, delim, new = payload.partition(TCP_MESSAGE_DELIM)
            assert len(current) > 0
            buffer.extend(current)

            if delim != b'':  # pragma: no branch
                message = buffer.decode('utf-8')
                buffer = bytearray(new)

                assert hasattr(self.server, 'handler')
                response = self.server.handler(message)

                payload = response.encode('utf-8') + TCP_MESSAGE_DELIM
                self.request.sendall(payload)


class SimpleSocketServer(socketserver.TCPServer):
    """Simple TCP socket server.

    Args:
        handler: Callback that handles a message and returns the response
            string.
        host: Host to bind to.
        poll_interval: Polling interval in seconds to check if the
            server should shutdown.
        port: Port to bind to. If 0, a random port is bound to.
    """

    def __init__(
        self,
        handler: Callable[[str], str],
        *,
        host: str = '0.0.0.0',
        poll_interval: float = 0.05,
        port: int = 0,
    ) -> None:
        self.handler = handler
        self.poll_interval = poll_interval
        self._client_sockets: set[socket.socket] = set()
        self._serving = threading.Event()
        self._lock = threading.RLock()

        super().__init__((host, port), _SimpleSocketServerHandler)

    @property
    def host(self) -> str:
        """Host the server is bound to."""
        host, _ = self.server_address
        assert isinstance(host, str)
        return host

    @property
    def port(self) -> int:
        """Port the server is bound to."""
        return self.server_address[1]

    def close(self) -> None:
        """Close the server."""
        self.socket.shutdown(socket.SHUT_RDWR)
        self.server_close()

    def close_request(
        self,
        request: socket.socket | tuple[bytes, socket.socket],
    ) -> None:
        """Close a request socket."""
        conn = request[1] if isinstance(request, tuple) else request
        with self._lock:
            with contextlib.suppress(KeyError):
                self._client_sockets.remove(conn)
            with contextlib.suppress(OSError):
                # Can fail if the client already shutdown the socket
                conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    def get_request(self) -> tuple[socket.socket, Any]:
        """Get a request socket."""
        conn, address = self.socket.accept()
        self._client_sockets.add(conn)
        return conn, address

    def stop_serving(self) -> None:
        """Notify the server to stop serving."""
        with self._lock:
            for conn in tuple(self._client_sockets):
                self.close_request(conn)
        if self._serving.is_set():
            self.shutdown()

    def serve_forever_default(self) -> None:
        """Server until `stop_serving` is called."""
        self._serving.set()
        try:
            self.serve_forever(self.poll_interval)
        finally:
            self._serving.clear()


def _recv_from_socket(socket: socket.socket, nbytes: int = 1024) -> bytes:
    while True:
        try:
            # TODO: use select here?
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
