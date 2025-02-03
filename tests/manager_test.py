from __future__ import annotations

import time

from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from aeris.manager import Manager
from testing.behavior import SleepBehavior
from testing.constant import TEST_LOOP_SLEEP


def test_protocol() -> None:
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        assert isinstance(repr(manager), str)
        assert isinstance(str(manager), str)


def test_basic_usage() -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        manager.launch(behavior)
        manager.launch(behavior)

        time.sleep(5 * TEST_LOOP_SLEEP)
