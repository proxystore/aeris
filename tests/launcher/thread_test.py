from __future__ import annotations

import threading
import time

from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.launcher import Launcher
from aeris.launcher.thread import ThreadLauncher
from testing.constant import TEST_LOOP_SLEEP


class Simple(Behavior):
    def __init__(self) -> None:
        self.steps = 0

    def shutdown(self) -> None:
        assert self.steps > 0

    @loop
    def count(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            self.steps += 1
            time.sleep(TEST_LOOP_SLEEP)


def test_launch_agents() -> None:
    behavior = Simple()
    exchange = ThreadExchange()

    with ThreadLauncher(exchange) as launcher:
        assert isinstance(launcher, Launcher)
        assert isinstance(repr(launcher), str)
        assert isinstance(str(launcher), str)

        handle1 = launcher.launch(behavior)
        handle2 = launcher.launch(behavior)

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.close()
        handle2.close()
