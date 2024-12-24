from __future__ import annotations

import threading

from aeris.agent import Agent
from aeris.behavior import action
from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.message import ShutdownRequest


class Waiter:
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


class Counter:
    def __init__(self) -> None:
        self._count = 0

    def setup(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    @action
    def add(self, value: int) -> None:
        self._count += value

    @action
    def count(self) -> int:
        return self._count


def test_agent_message_listener() -> None:
    behavior = Counter()
    exchange = ThreadExchange()

    aid = exchange.register_agent()
    cid = exchange.register_client()

    agent_mailbox = exchange.get_mailbox(aid)
    assert agent_mailbox is not None
    client_mailbox = exchange.get_mailbox(cid)
    assert client_mailbox is not None

    agent = Agent(behavior, aid=aid, exchange=exchange)
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    thread = threading.Thread(target=agent)
    thread.start()

    # Ping the agent
    ping = PingRequest(src=cid, dest=aid)
    agent_mailbox.send(ping)
    message = client_mailbox.recv()
    assert isinstance(message, PingResponse)

    # Invoke actions
    value = 42
    request = ActionRequest(
        src=cid,
        dest=aid,
        action='add',
        args=(value,),
    )
    agent_mailbox.send(request)
    message = client_mailbox.recv()
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result is None

    request = ActionRequest(
        src=cid,
        dest=aid,
        action='count',
    )
    agent_mailbox.send(request)
    message = client_mailbox.recv()
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result == value

    # Invoke unknown action
    request = ActionRequest(
        src=cid,
        dest=aid,
        action='foo',
    )
    agent_mailbox.send(request)
    message = client_mailbox.recv()
    assert isinstance(message, ActionResponse)
    assert isinstance(message.exception, RuntimeError)
    assert 'foo' in str(message.exception)

    # Shutdown the agent
    shutdown = ShutdownRequest(src=cid, dest=aid)
    agent_mailbox.send(shutdown)

    thread.join(timeout=1)
    assert not thread.is_alive()
