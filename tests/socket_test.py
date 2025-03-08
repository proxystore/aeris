from __future__ import annotations

import threading
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
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


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
def test_simple_socket_recv(mock_recv, mock_create_connection) -> None:
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
        poll_interval=0.01,
        port=0,
    )
    started = threading.Event()

    def _serve() -> None:
        started.set()
        server.serve_forever_default()

    thread = threading.Thread(target=_serve)
    thread.start()
    started.wait(TEST_THREAD_JOIN_TIMEOUT)

    yield server

    server.stop_serving()
    server.close()


def test_simple_socket_server_ping(
    simple_socket_server: SimpleSocketServer,
) -> None:
    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        message = 'hello, world!'
        socket.send_string(message)
        assert socket.recv_string() == message


def test_simple_socket_multipart(
    simple_socket_server: SimpleSocketServer,
) -> None:
    with SimpleSocket(
        simple_socket_server.host,
        simple_socket_server.port,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as socket:
        socket.socket.sendall(b'hello, ')
        socket.socket.sendall(b'world!' + TCP_MESSAGE_DELIM)
        assert socket.recv_string() == 'hello, world!'


def test_wait_connection() -> None:
    with mock.patch('socket.create_connection'):
        wait_connection('localhost', port=0)


def test_wait_connection_timeout() -> None:
    with mock.patch('socket.create_connection', side_effect=OSError()):
        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0, timeout=0)

        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0.01, timeout=0.05)
