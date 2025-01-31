from __future__ import annotations

import asyncio
import threading
from collections.abc import Generator

import pytest

from aeris.exchange.simple import SimpleServer
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from aeris.socket import wait_connection
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.sys import open_port


@pytest.fixture
def exchange() -> Generator[ThreadExchange]:
    with ThreadExchange() as exchange:
        yield exchange


@pytest.fixture
def launcher() -> Generator[ThreadLauncher]:
    with ThreadLauncher() as launcher:
        yield launcher


@pytest.fixture
def simple_exchange_server() -> Generator[tuple[str, int]]:
    host, port = 'localhost', open_port()
    server = SimpleServer(host, port)
    loop = asyncio.new_event_loop()
    stop = loop.create_future()

    def _target() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.serve_forever(stop))
        loop.close()

    handle = threading.Thread(target=_target, name=f'{server}-fixture')
    handle.start()

    # Wait for server to be listening
    wait_connection(host, port, timeout=TEST_CONNECTION_TIMEOUT)

    yield host, port

    loop.call_soon_threadsafe(stop.set_result, None)
    handle.join(timeout=TEST_CONNECTION_TIMEOUT)
    if handle.is_alive():  # pragma: no cover
        raise TimeoutError(
            'Server thread did not gracefully exit within '
            f'{TEST_CONNECTION_TIMEOUT} seconds.',
        )
