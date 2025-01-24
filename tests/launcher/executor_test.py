from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from aeris.exchange import Exchange
from aeris.launcher import Launcher
from aeris.launcher.executor import ExecutorLauncher
from testing.behavior import SleepBehavior
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
