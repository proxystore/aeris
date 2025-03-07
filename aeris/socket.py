from __future__ import annotations

import socket
import sys
import time
from types import TracebackType

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


_BAD_FILE_DESCRIPTOR_ERRNO = 9


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

    delimiter = b'\r\n'

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float | None = 5,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
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

    def close(self) -> None:
        """Close the socket."""
        self.socket.close()

    def send(self, message: bytes) -> None:
        """Send bytes to the socket.

        Args:
            message: Message to send.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        payload = message + self.delimiter
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

    def _recv_next(self) -> bytes:
        while True:
            try:
                payload = self.socket.recv(1024)
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

    def recv(self) -> bytes:
        """Receive the next message from the socket.

        Returns:
            Bytes containing the message.

        Raises:
            SocketClosedError: if the socket was closed.
            OSError: if an error occurred.
        """
        while True:
            payload = self._recv_next()
            current, delim, new = payload.partition(self.delimiter)
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
