from __future__ import annotations

import time

import pytest

from academy.exception import BadEntityIdError
from academy.exchange import Exchange
from academy.launcher import Launcher
from academy.launcher.thread import ThreadLauncher
from testing.behavior import EmptyBehavior
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

        assert len(launcher.running()) == 2  # noqa: PLR2004

        time.sleep(5 * TEST_LOOP_SLEEP)

        handle1.shutdown()
        handle2.shutdown()

        handle1.close()
        handle2.close()

        launcher.wait(handle1.agent_id)
        launcher.wait(handle2.agent_id)

        assert len(launcher.running()) == 0


def test_wait_bad_identifier(exchange: Exchange) -> None:
    with ThreadLauncher() as launcher:
        agent_id = exchange.register_agent(EmptyBehavior)

        with pytest.raises(BadEntityIdError):
            launcher.wait(agent_id)


def test_wait_timeout(exchange: Exchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with ThreadLauncher() as launcher:
        handle = launcher.launch(behavior, exchange).bind_as_client()

        with pytest.raises(TimeoutError):
            launcher.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)

        handle.shutdown()
        handle.close()
