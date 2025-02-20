from __future__ import annotations

import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pytest

from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exchange import Exchange
from aeris.exchange.simple import SimpleExchange
from aeris.launcher import Launcher
from aeris.launcher.executor import ExecutorLauncher
from testing.behavior import SleepBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP


def test_protocol() -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    with ExecutorLauncher(executor) as launcher:
        assert isinstance(launcher, Launcher)
        assert isinstance(repr(launcher), str)
        assert isinstance(str(launcher), str)


def test_launch_agents_threads(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    executor = ThreadPoolExecutor(max_workers=2)
    with ExecutorLauncher(executor) as launcher:
        handle1 = launcher.launch(behavior, exchange).bind_as_client()
        handle2 = launcher.launch(behavior, exchange).bind_as_client()

        assert len(launcher.running()) == 2  # noqa: PLR2004

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.shutdown()
        handle2.shutdown()

        handle1.close()
        handle2.close()

        launcher.wait(handle1.agent_id)
        launcher.wait(handle2.agent_id)

        assert len(launcher.running()) == 0


def test_launch_agents_processes(
    simple_exchange_server: tuple[str, int],
) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    context = multiprocessing.get_context('spawn')
    executor = ProcessPoolExecutor(max_workers=2, mp_context=context)
    host, port = simple_exchange_server

    with SimpleExchange(host, port) as exchange:
        with ExecutorLauncher(executor) as launcher:
            handle1 = launcher.launch(behavior, exchange).bind_as_client()
            handle2 = launcher.launch(behavior, exchange).bind_as_client()

            assert handle1.ping(timeout=TEST_CONNECTION_TIMEOUT) > 0
            assert handle2.ping(timeout=TEST_CONNECTION_TIMEOUT) > 0

            handle1.shutdown()
            handle2.shutdown()

            handle1.close()
            handle2.close()


def test_wait_bad_identifier(exchange: Exchange) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    with ExecutorLauncher(executor) as launcher:
        agent_id = exchange.create_agent()

        with pytest.raises(BadIdentifierError):
            launcher.wait(agent_id)


def test_wait_timeout(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    executor = ThreadPoolExecutor(max_workers=1)
    with ExecutorLauncher(executor) as launcher:
        handle = launcher.launch(behavior, exchange).bind_as_client()

        with pytest.raises(TimeoutError):
            launcher.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)

        handle.shutdown()
        handle.close()


class FailOnStartupBehavior(Behavior):
    def on_setup(self) -> None:
        raise RuntimeError('Agent startup failed')


def test_wait_ignore_agent_errors(exchange: Exchange) -> None:
    behavior = FailOnStartupBehavior()
    executor = ThreadPoolExecutor(max_workers=1)
    launcher = ExecutorLauncher(executor)

    handle = launcher.launch(behavior, exchange).bind_as_client()
    launcher.wait(handle.agent_id)
    handle.close()

    with pytest.raises(RuntimeError, match='Agent startup failed'):
        launcher.close()
    executor.shutdown()
