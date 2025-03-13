from __future__ import annotations

import random
import string
from collections.abc import Generator
from unittest import mock

import pytest

from aeris.socket import _BAD_FILE_DESCRIPTOR_ERRNO
from aeris.socket import _recv_from_socket
from aeris.socket import SimpleSocket
from aeris.socket import SimpleSocketServer
from aeris.socket import SocketClosedError
from aeris.socket import SocketOpenError
from aeris.socket import TCP_MESSAGE_DELIM
from aeris.socket import wait_connection
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
        socket.send_string('hello, world!')
        mock_socket.sendall.assert_called_once()

        mock_socket.sendall.side_effect = OSError('Mocked.')
        with pytest.raises(OSError, match='Mocked.'):
            socket.send_string('')

        mock_socket.sendall.side_effect = OSError(
            _BAD_FILE_DESCRIPTOR_ERRNO,
            'Bad file descriptor.',
        )
        with pytest.raises(SocketClosedError):
            socket.send_string('')


@mock.patch('socket.create_connection')
@mock.patch('aeris.socket._recv_from_socket')
def test_simple_socket_recv_packed(mock_recv, mock_create_connection) -> None:
    # Mock received parts to include two messages split across three parts
    # followed by a socket close event.
    messages = ['first message', 'seconds message', 'third message']
    buffer = b''.join(
        [message.encode('utf-8') + TCP_MESSAGE_DELIM for message in messages],
    )
    mock_recv.side_effect = [buffer, SocketClosedError()]
    with SimpleSocket('localhost', 0) as socket:
        for expected in messages:
            assert socket.recv_string() == expected
        with pytest.raises(SocketClosedError):
            assert socket.recv_string()


@mock.patch('socket.create_connection')
@mock.patch('aeris.socket._recv_from_socket')
def test_simple_socket_recv_multipart(
    mock_recv,
    mock_create_connection,
) -> None:
    # Mock received parts to include two messages split across three parts
    # followed by a socket close event.
    mock_recv.side_effect = [
        b'hello, ',
        b'world!' + TCP_MESSAGE_DELIM,
        b'part2' + TCP_MESSAGE_DELIM,
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
    buffer = b''.join(
        [message + TCP_MESSAGE_DELIM for message in messages],
    )

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
    first_parts = [
        ''.join(random.choice(string.ascii_uppercase) for _ in range(500))
        for _ in range(3)
    ]
    second_part = 'second message!'
    # socket.recv_string() will not return the delimiter so add after
    # computing the expected string
    first_expected = ''.join(first_parts)
    parts = [
        first_parts[0],
        first_parts[1],
        first_parts[2] + TCP_MESSAGE_DELIM.decode('utf-8'),
        second_part + TCP_MESSAGE_DELIM.decode('utf-8'),
    ]

    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        for part in parts:
            socket.socket.sendall(part.encode('utf-8'))
        assert socket.recv_string() == first_expected
        assert socket.recv_string() == second_part


def test_wait_connection() -> None:
    with mock.patch('socket.create_connection'):
        wait_connection('localhost', port=0)


def test_wait_connection_timeout() -> None:
    with mock.patch('socket.create_connection', side_effect=OSError()):
        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0, timeout=0)

        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0.01, timeout=0.05)
