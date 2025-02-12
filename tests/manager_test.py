from __future__ import annotations

import time

import pytest

from aeris.exception import BadIdentifierError
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher
from aeris.manager import Manager
from aeris.message import PingRequest
from aeris.message import PingResponse
from testing.behavior import SleepBehavior
from testing.constant import TEST_LOOP_SLEEP
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


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


def test_reply_to_requests_with_error() -> None:
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        client_id = manager.exchange.create_client()
        request = PingRequest(src=client_id, dest=manager.mailbox_id)
        manager.exchange.send(request.dest, request)
        response = manager.exchange.recv(client_id)
        assert isinstance(response, PingResponse)
        assert isinstance(response.exception, TypeError)


def test_wait_bad_identifier(exchange: ThreadExchange) -> None:
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        agent_id = manager.exchange.create_agent()

        with pytest.raises(BadIdentifierError):
            manager.wait(agent_id)


def test_wait_timeout(exchange: ThreadExchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        handle = manager.launch(behavior)

        with pytest.raises(TimeoutError):
            manager.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)


def test_shutdown_bad_identifier(exchange: ThreadExchange) -> None:
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        agent_id = manager.exchange.create_agent()

        with pytest.raises(BadIdentifierError):
            manager.shutdown(agent_id)


def test_shutdown_nonblocking(exchange: ThreadExchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        handle = manager.launch(behavior)
        manager.shutdown(handle.agent_id, blocking=False)
        manager.wait(handle.agent_id, timeout=TEST_THREAD_JOIN_TIMEOUT)


def test_shutdown_blocking(exchange: ThreadExchange) -> None:
    behavior = SleepBehavior(TEST_LOOP_SLEEP)
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),
    ) as manager:
        handle = manager.launch(behavior)
        manager.shutdown(handle.agent_id, blocking=True)
        manager.wait(handle.agent_id, timeout=TEST_LOOP_SLEEP)
