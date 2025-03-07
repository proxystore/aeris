from __future__ import annotations

from unittest import mock

import pytest

from aeris.socket import SimpleSocket
from aeris.socket import wait_connection


@mock.patch('socket.create_connection')
def test_create_simple_socket(mock_create_connection) -> None:
    with SimpleSocket('localhost', 0) as socket:
        assert 'localhost' in repr(socket)
        assert 'localhost' in str(socket)


def test_wait_connection() -> None:
    with mock.patch('socket.create_connection'):
        wait_connection('localhost', port=0)


def test_wait_connection_timeout() -> None:
    with mock.patch('socket.create_connection', side_effect=OSError()):
        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0, timeout=0)

        with pytest.raises(TimeoutError):
            wait_connection('localhost', port=0, sleep=0.01, timeout=0.05)
