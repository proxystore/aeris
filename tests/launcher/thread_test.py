from __future__ import annotations

import time

import pytest

from aeris.exception import BadIdentifierError
from aeris.exchange import Exchange
from aeris.launcher import Launcher
from aeris.launcher.thread import ThreadLauncher
from testing.behavior import SleepBehavior
from testing.constant import TEST_LOOP_SLEEP


def test_protocol() -> None:
    with ThreadLauncher() as launcher:
        assert isinstance(launcher, Launcher)
        assert isinstance(repr(launcher), str)
        assert isinstance(str(launcher), str)


def test_launch_agents(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with ThreadLauncher() as launcher:
        handle1 = launcher.launch(behavior, exchange).bind_as_client()
        handle2 = launcher.launch(behavior, exchange).bind_as_client()

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.shutdown()
        handle2.shutdown()

        handle1.close()
        handle2.close()

        launcher.wait(handle1.agent_id)
        launcher.wait(handle2.agent_id)


def test_wait_bad_identifier(exchange: Exchange) -> None:
    with ThreadLauncher() as launcher:
        agent_id = exchange.create_agent()

        with pytest.raises(BadIdentifierError):
            launcher.wait(agent_id)


def test_wait_timeout(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with ThreadLauncher() as launcher:
        handle = launcher.launch(behavior, exchange).bind_as_client()

        with pytest.raises(TimeoutError):
            launcher.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)

        handle.shutdown()
        handle.close()
