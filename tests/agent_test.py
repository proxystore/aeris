from __future__ import annotations

import threading
from concurrent.futures import Future

import pytest

from aeris.agent import Agent
from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.exchange.thread import ThreadExchange
from aeris.handle import AgentRemoteHandle
from aeris.handle import ClientRemoteHandle
from aeris.handle import Handle
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import AgentIdentifier
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


def test_agent_log_bad_response() -> None:
    behavior = Counter()
    agent = Agent(behavior)
    response = PingResponse(
        src=AgentIdentifier.new(),
        dest=AgentIdentifier.new(),
    )
    agent._response_handler(response)


class Bindings(Behavior):
    def __init__(
        self,
        unbound: UnboundRemoteHandle[Counter],
        client_bound: ClientRemoteHandle[Counter],
        agent_bound: AgentRemoteHandle[Counter],
        self_bound: AgentRemoteHandle[Counter],
    ) -> None:
        self.unbound = unbound
        self.client_bound = client_bound
        self.agent_bound = agent_bound
        self.self_bound = self_bound

    def setup(self) -> None:
        assert isinstance(self.unbound, AgentRemoteHandle)
        assert isinstance(self.client_bound, ClientRemoteHandle)
        assert isinstance(self.agent_bound, AgentRemoteHandle)
        assert isinstance(self.self_bound, AgentRemoteHandle)

        assert self.unbound.hid is not None
        assert self.unbound.hid == self.agent_bound.hid == self.self_bound.hid

    def shutdown(self) -> None:
        self.unbound.close()
        self.client_bound.close()
        self.agent_bound.close()
        self.self_bound.close()


def test_agent_run_bind_handles() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.create_agent()
        behavior = Bindings(
            unbound=UnboundRemoteHandle(exchange, exchange.create_agent()),
            client_bound=ClientRemoteHandle(exchange, exchange.create_agent()),
            agent_bound=AgentRemoteHandle(
                exchange,
                exchange.create_agent(),
                exchange.create_agent(),
            ),
            self_bound=AgentRemoteHandle(
                exchange,
                exchange.create_agent(),
                aid,
            ),
        )
        agent = Agent(behavior, aid=aid, exchange=exchange)

        agent.shutdown()
        agent.wait()
        agent.run()  # Exits immediately because we called shutdown


class DuplicateBindings(Behavior):
    def __init__(
        self,
        handle1: UnboundRemoteHandle[Counter],
        handle2: UnboundRemoteHandle[Counter],
    ) -> None:
        self.handle1 = handle1
        self.handle2 = handle2


def test_agent_run_duplicate_handles_error() -> None:
    with ThreadExchange() as exchange:
        self_aid = exchange.create_agent()
        remote_aid = exchange.create_agent()
        behavior = DuplicateBindings(
            handle1=UnboundRemoteHandle(exchange, remote_aid),
            handle2=UnboundRemoteHandle(exchange, remote_aid),
        )
        agent = Agent(behavior, aid=self_aid, exchange=exchange)

        error = (
            f'{agent} already has a handle bound to a remote agent '
            f'with {remote_aid}.'
        )
        with pytest.raises(RuntimeError, match=error):
            agent.run()


class Foo(Behavior):
    def __init__(self, bar: Handle[Bar]) -> None:
        self.bar = bar

    def shutdown(self) -> None:
        assert isinstance(self.bar, AgentRemoteHandle)
        self.bar.shutdown()

    @action
    def run(self, value: int) -> int:
        return self.bar.action('double', value).result()


class Bar(Behavior):
    @action
    def double(self, value: int) -> int:
        return 2 * value


def test_agent_to_handle_handles() -> None:
    with ThreadExchange() as exchange:
        foo_id = exchange.create_agent()
        bar_id = exchange.create_agent()

        foo_handle: UnboundRemoteHandle[Foo] = exchange.create_handle(foo_id)
        bar_handle: UnboundRemoteHandle[Bar] = exchange.create_handle(bar_id)

        foo_behavior = Foo(bar_handle)
        bar_behavior = Bar()

        foo_agent = Agent(foo_behavior, aid=foo_id, exchange=exchange)
        bar_agent = Agent(bar_behavior, aid=bar_id, exchange=exchange)

        foo_thread = threading.Thread(target=foo_agent)
        bar_thread = threading.Thread(target=bar_agent)

        foo_thread.start()
        bar_thread.start()

        foo_client = foo_handle.bind_as_client()
        future: Future[int] = foo_client.action('run', 1)
        assert future.result() == 2  # noqa: PLR2004

        foo_client.shutdown()

        foo_thread.join(timeout=1)
        bar_thread.join(timeout=1)
