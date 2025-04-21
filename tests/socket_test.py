from __future__ import annotations

import random
import socket
import string
import sys
from collections.abc import Generator
from unittest import mock

import pytest

from academy.socket import _BAD_FILE_DESCRIPTOR_ERRNO
from academy.socket import _make_header
from academy.socket import _recv_from_socket
from academy.socket import address_by_hostname
from academy.socket import address_by_interface
from academy.socket import MESSAGE_CHUNK_SIZE
from academy.socket import SimpleSocket
from academy.socket import SimpleSocketServer
from academy.socket import SocketClosedError
from academy.socket import SocketOpenError
from academy.socket import wait_connection
from testing.constant import TEST_CONNECTION_TIMEOUT


@mock.patch('socket.create_connection')
def test_create_simple_socket(mock_create_connection) -> None:
    with SimpleSocket('localhost', 0) as socket:
        assert 'localhost' in repr(socket)
        assert 'localhost' in str(socket)


@mock.patch('socket.create_connection')
def test_create_simple_socket_error(mock_create_connection) -> None:
    mock_create_connection.side_effect = OSError()
    with pytest.raises(SocketOpenError):
        SimpleSocket('localhost', 0)


@mock.patch('socket.create_connection')
def test_simple_socket_close_idempotency(mock_create_connection) -> None:
    socket = SimpleSocket('localhost', 0)
    socket.close()
    socket.close()


@mock.patch('socket.create_connection')
def test_simple_socket_send(mock_create_connection) -> None:
    mock_socket = mock.MagicMock()
    mock_create_connection.return_value = mock_socket
    with SimpleSocket('localhost', 0) as socket:
        # Should be a no-op
        socket.send(b'')

        socket.send_string('hello, world!')
        # Called once for header and once for payload
        assert mock_socket.sendall.call_count == 2  # noqa: PLR2004

        mock_socket.sendall.side_effect = OSError('Mocked.')
        with pytest.raises(OSError, match='Mocked.'):
            socket.send_string('hello, again!')

        mock_socket.sendall.side_effect = OSError(
            _BAD_FILE_DESCRIPTOR_ERRNO,
            'Bad file descriptor.',
        )
        with pytest.raises(SocketClosedError):
            socket.send_string('hello, again!')


@mock.patch('socket.create_connection')
def test_simple_socket_send_multipart(mock_create_connection) -> None:
    mock_socket = mock.MagicMock()
    mock_create_connection.return_value = mock_socket
    size = int(2.5 * MESSAGE_CHUNK_SIZE)
    message = ''.join(
        [random.choice(string.ascii_uppercase) for _ in range(size)],
    )
    with SimpleSocket('localhost', 0) as socket:
        socket.send_string(message)
        assert mock_socket.sendall.call_count == 4  # noqa: PLR2004


@mock.patch('socket.create_connection')
@mock.patch('academy.socket._recv_from_socket')
def test_simple_socket_recv_multipart(
    mock_recv,
    mock_create_connection,
) -> None:
    messages = [b'hello, world!', b'part2']
    # Mock received parts to include two messages split across three parts
    # followed by a socket close event.
    mock_recv.side_effect = [
        _make_header(messages[0]),
        b'hello, ',
        b'world!',
        _make_header(messages[1]),
        messages[1],
        SocketClosedError(),
    ]
    with SimpleSocket('localhost', 0) as socket:
        assert socket.recv_string() == 'hello, world!'
        assert socket.recv_string() == 'part2'
        with pytest.raises(SocketClosedError):
            assert socket.recv_string()


def test_recv_from_socket_nonblocking() -> None:
    message = b'hello, world!'
    socket = mock.MagicMock()
    socket.recv.side_effect = [BlockingIOError, BlockingIOError, message]
    assert _recv_from_socket(socket) == message


def test_recv_from_socket_close_on_empty_string() -> None:
    socket = mock.MagicMock()
    socket.recv.return_value = b''
    with pytest.raises(SocketClosedError):
        _recv_from_socket(socket)


def test_recv_from_socket_os_error() -> None:
    socket = mock.MagicMock()
    socket.recv.side_effect = OSError('Mocked.')
    with pytest.raises(OSError, match='Mocked.'):
        _recv_from_socket(socket)


def test_recv_from_socket_bad_file_descriptor() -> None:
    socket = mock.MagicMock()
    socket.recv.side_effect = OSError(
        _BAD_FILE_DESCRIPTOR_ERRNO,
        'Bad file descriptor.',
    )
    with pytest.raises(SocketClosedError):
        _recv_from_socket(socket)


@pytest.fixture
def simple_socket_server() -> Generator[SimpleSocketServer]:
    server = SimpleSocketServer(
        handler=lambda s: s,
        host='localhost',
        port=None,
        timeout=TEST_CONNECTION_TIMEOUT,
    )

    server.start_server_thread()
    yield server
    server.stop_server_thread()


def test_simple_socket_server_connect(
    simple_socket_server: SimpleSocketServer,
) -> None:
    for _ in range(3):
        with SimpleSocket(
            simple_socket_server.host,
            simple_socket_server.port,
            timeout=TEST_CONNECTION_TIMEOUT,
        ):
            pass


def test_simple_socket_server_ping_pong(
    simple_socket_server: SimpleSocketServer,
) -> None:
    message = 'hello, world!'
    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        for _ in range(3):
            socket.send_string(message)
            assert socket.recv_string() == message


def test_simple_socket_server_packed(
    simple_socket_server: SimpleSocketServer,
) -> None:
    # Pack many messages into one buffer to be send
    messages = [b'first message', b'seconds message', b'third message']
    buffer = b''.join(_make_header(m) + m for m in messages)

    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        socket.socket.sendall(buffer)
        for expected in messages:
            assert socket.recv() == expected


def test_simple_socket_server_multipart(
    simple_socket_server: SimpleSocketServer,
) -> None:
    # Generate >1024 bytes of data since _recv_from_socket reads in 1kB
    # chunks. This test forces recv() to buffer incomplete chunks.
    first_parts = [random.randbytes(500) for _ in range(3)]
    second_part = b'second message!'
    # socket.recv_string() will not return the delimiter so add after
    # computing the expected string
    first_expected = b''.join(first_parts)
    parts = [
        _make_header(first_expected),
        first_parts[0],
        first_parts[1],
        first_parts[2],
        _make_header(second_part),
        second_part,
    ]

    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        for part in parts:
            socket.socket.sendall(part)
        assert socket.recv() == first_expected
        assert socket.recv() == second_part


def test_simple_socket_server_client_disconnect_early(
    simple_socket_server: SimpleSocketServer,
) -> None:
    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ):
        # Client disconnects without sending anything
        pass


def test_get_address_by_hostname() -> None:
    assert isinstance(address_by_hostname(), str)


@pytest.mark.skipif(
    sys.platform == 'darwin',
    reason='Test does not run on darwin',
)
def test_get_address_by_interface() -> None:  # pragma: darwin no cover
    for _, ifname in socket.if_nameindex():
        try:
            assert isinstance(address_by_interface(ifname), str)
        except Exception:  # pragma: no cover
            continue
        else:
            break
    else:  # pragma: no cover
        raise RuntimeError('Failed to find a valid address by interface.')


def test_wait_connection() -> None:
    with mock.patch('socket.create_connection'):
        wait_connection('localhost', port=0)


def test_wait_connection_timeout() -> None:
    with mock.patch('socket.create_connection', side_effect=OSError()):
        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0, timeout=0)

        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0.01, timeout=0.05)
