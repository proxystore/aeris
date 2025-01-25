from __future__ import annotations

import time

from aeris.exchange import Exchange
from aeris.launcher import Launcher
from aeris.launcher.thread import ThreadLauncher
from testing.behavior import SleepBehavior
from testing.constant import TEST_LOOP_SLEEP


def test_protocol(exchange: Exchange) -> None:
    with ThreadLauncher(exchange) as launcher:
        assert isinstance(launcher, Launcher)
        assert isinstance(repr(launcher), str)
        assert isinstance(str(launcher), str)


def test_launch_agents(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with ThreadLauncher(exchange) as launcher:
        handle1 = launcher.launch(behavior).bind_as_client()
        handle2 = launcher.launch(behavior).bind_as_client()

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.shutdown()
        handle2.shutdown()

        handle1.close()
        handle2.close()
