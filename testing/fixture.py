from __future__ import annotations

import asyncio
import socket
import threading
import time
from collections.abc import Generator

import pytest

from aeris.exchange import Exchange
from aeris.exchange.simple import SimpleServer
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP
from testing.sys import open_port


@pytest.fixture
def exchange() -> Generator[Exchange]:
    with ThreadExchange() as exchange:
        yield exchange


@pytest.fixture
def launcher(exchange: Exchange) -> Generator[ThreadLauncher]:
    with ThreadLauncher(exchange) as launcher:
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
    waited = 0.0
    while True:
        try:
            start = time.perf_counter()
            with socket.create_connection(
                (host, port),
                timeout=TEST_LOOP_SLEEP,
            ):
                break
        except OSError as e:  # pragma: no cover
            if waited > TEST_CONNECTION_TIMEOUT:
                raise TimeoutError from e
            end = time.perf_counter()
            sleep = max(0, TEST_LOOP_SLEEP - (end - start))
            time.sleep(sleep)
            waited += sleep

    yield host, port

    loop.call_soon_threadsafe(stop.set_result, None)
    handle.join(timeout=TEST_CONNECTION_TIMEOUT)
    if handle.is_alive():  # pragma: no cover
        raise TimeoutError(
            'Server thread did not gracefully exit within '
            f'{TEST_CONNECTION_TIMEOUT} seconds.',
        )
