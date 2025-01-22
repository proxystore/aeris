from __future__ import annotations

import threading

import pytest

from aeris.agent import Agent
from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.message import ShutdownRequest


class Waiter(Behavior):
    def __init__(self) -> None:
        self.setup_event = threading.Event()
        self.loop_event = threading.Event()
        self.shutdown_event = threading.Event()

    def setup(self) -> None:
        self.setup_event.set()

    def shutdown(self) -> None:
        self.shutdown_event.set()

    @loop
    def waiter(self, shutdown: threading.Event) -> None:
        self.loop_event.wait()
        shutdown.wait()

    @loop
    def setter(self, shutdown: threading.Event) -> None:
        self.loop_event.set()
        shutdown.wait()


def test_agent_run() -> None:
    behavior = Waiter()
    agent = Agent(behavior)
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    def run() -> None:
        agent()

    thread = threading.Thread(target=run)
    thread.start()
    agent.behavior.setup_event.wait()
    agent.behavior.loop_event.wait()
    agent.shutdown()
    agent.wait(timeout=1)
    thread.join(timeout=1)

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


def test_agent_shutdown() -> None:
    behavior = Waiter()
    agent = Agent(behavior)

    agent.shutdown()
    agent.wait()
    agent.run()

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


class Counter(Behavior):
    def __init__(self) -> None:
        self._count = 0

    @action
    def add(self, value: int) -> None:
        self._count += value

    @action
    def count(self) -> int:
        return self._count


def test_agent_message_listener() -> None:
    behavior = Counter()
    exchange = ThreadExchange()

    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(behavior, aid=aid, exchange=exchange)
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    thread = threading.Thread(target=agent)
    thread.start()

    # Ping the agent
    ping = PingRequest(src=cid, dest=aid)
    exchange.send(aid, ping)
    message = exchange.recv(cid)
    assert isinstance(message, PingResponse)

    # Invoke actions
    value = 42
    request = ActionRequest(
        src=cid,
        dest=aid,
        action='add',
        args=(value,),
    )
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result is None

    request = ActionRequest(
        src=cid,
        dest=aid,
        action='count',
    )
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result == value

    # Invoke unknown action
    request = ActionRequest(
        src=cid,
        dest=aid,
        action='foo',
    )
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert isinstance(message.exception, TypeError)
    assert 'foo' in str(message.exception)

    # Shutdown the agent
    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=1)
    assert not thread.is_alive()

    exchange.close()


def test_agent_listener_bad_message_type() -> None:
    behavior = Counter()
    exchange = ThreadExchange()

    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(behavior, aid=aid, exchange=exchange)

    exchange.send(aid, PingResponse(src=cid, dest=aid))

    with pytest.raises(TypeError, match='PingResponse'):
        agent.run()

    exchange.close()
