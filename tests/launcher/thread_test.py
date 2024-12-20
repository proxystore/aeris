from __future__ import annotations

import threading
import time

from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.launcher import Launcher
from aeris.launcher.thread import ThreadLauncher
from testing.constant import TEST_LOOP_SLEEP


class SimpleBehavior:
    def __init__(self) -> None:
        self.steps = 0

    def setup(self) -> None:
        pass

    def shutdown(self) -> None:
        assert self.steps > 0

    @loop
    def count(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            self.steps += 1
            time.sleep(TEST_LOOP_SLEEP)


def test_launch_agents() -> None:
    behavior = SimpleBehavior()
    exchange = ThreadExchange()
    launcher = ThreadLauncher(exchange)
    assert isinstance(launcher, Launcher)

    handle1 = launcher.launch(behavior)
    handle2 = launcher.launch(behavior)

    time.sleep(5 * TEST_LOOP_SLEEP)

    launcher.shutdown()

    handle1.close()
    handle2.close()
