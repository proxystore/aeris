from __future__ import annotations

import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

from aeris.exchange import Exchange
from aeris.exchange.simple import SimpleExchange
from aeris.launcher import Launcher
from aeris.launcher.executor import ExecutorLauncher
from testing.behavior import SleepBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP


def test_protocol(exchange: Exchange) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    with ExecutorLauncher(exchange, executor) as launcher:
        assert isinstance(launcher, Launcher)
        assert isinstance(repr(launcher), str)
        assert isinstance(str(launcher), str)


def test_launch_agents_threads(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    executor = ThreadPoolExecutor(max_workers=2)
    with ExecutorLauncher(exchange, executor) as launcher:
        handle1 = launcher.launch(behavior).bind_as_client()
        handle2 = launcher.launch(behavior).bind_as_client()

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.shutdown()
        handle2.shutdown()

        handle1.close()
        handle2.close()


def test_launch_agents_processes(
    simple_exchange_server: tuple[str, int],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    context = multiprocessing.get_context('spawn')
    executor = ProcessPoolExecutor(max_workers=2, mp_context=context)
    host, port = simple_exchange_server

    with SimpleExchange(host, port) as exchange:
        with ExecutorLauncher(exchange, executor) as launcher:
            handle1 = launcher.launch(behavior).bind_as_client()
            handle2 = launcher.launch(behavior).bind_as_client()

            assert handle1.ping(timeout=TEST_CONNECTION_TIMEOUT) > 0
            assert handle2.ping(timeout=TEST_CONNECTION_TIMEOUT) > 0

            handle1.shutdown()
            handle2.shutdown()

            handle1.close()
            handle2.close()
