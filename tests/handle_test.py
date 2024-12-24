from __future__ import annotations

import time
from concurrent.futures import Future

from aeris.behavior import action
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from testing.constant import TEST_SLEEP


class Counter:
    def __init__(self) -> None:
        self._count = 0

    def setup(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    @action
    def add(self, value: int) -> None:
        self._count += value

    @action
    def count(self) -> int:
        return self._count

    @action
    def fails(self) -> None:
        raise Exception()


def test_create_and_close_handle() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.register_agent()
        with exchange.create_handle(aid) as handle:
            assert isinstance(repr(handle), str)
            assert isinstance(str(handle), str)


def test_handle_operations() -> None:
    exchange = ThreadExchange()
    launcher = ThreadLauncher(exchange)
    behavior = Counter()

    handle = launcher.launch(behavior)

    assert handle.ping() > 0

    add_future: Future[None] = handle.action('add', 1)
    add_future.result()

    count_future: Future[int] = handle.action('count')
    assert count_future.result() == 1

    fails_future: Future[None] = handle.action('fails')
    assert isinstance(fails_future.exception(), Exception)

    handle.shutdown()

    handle.close()
    launcher.close()
    exchange.close()


class Sleeper:
    def setup(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    @action
    def sleep(self, sleep: float) -> None:
        time.sleep(sleep)


def test_cancel_futures() -> None:
    exchange = ThreadExchange()
    launcher = ThreadLauncher(exchange)
    behavior = Sleeper()

    handle = launcher.launch(behavior)
    future: Future[None] = handle.action('sleep', TEST_SLEEP)
    handle.close(wait_futures=False)
    assert future.cancelled()

    launcher.close()
    exchange.close()
