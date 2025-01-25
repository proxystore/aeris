from __future__ import annotations

import threading
from concurrent.futures import Future

import pytest

from aeris.agent import Agent
from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.exchange import Exchange
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
from testing.behavior import CounterBehavior
from testing.behavior import EmptyBehavior
from testing.behavior import ErrorBehavior
from testing.constant import TEST_THREAD_JOIN_TIMEOUT


class SignalingBehavior(Behavior):
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
    agent = Agent(SignalingBehavior())
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    def run() -> None:
        agent()

    thread = threading.Thread(target=run)
    thread.start()
    agent.behavior.setup_event.wait()
    agent.behavior.loop_event.wait()
    agent.shutdown()
    agent.wait(timeout=TEST_THREAD_JOIN_TIMEOUT)
    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


def test_agent_shutdown() -> None:
    agent = Agent(SignalingBehavior())

    agent.shutdown()
    agent.wait()
    agent.run()

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


def test_agent_shutdown_message(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(EmptyBehavior(), aid=aid, exchange=exchange)
    thread = threading.Thread(target=agent)
    thread.start()

    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    assert not thread.is_alive()


def test_agent_ping_message(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(EmptyBehavior(), aid=aid, exchange=exchange)
    assert isinstance(repr(agent), str)
    assert isinstance(str(agent), str)

    thread = threading.Thread(target=agent)
    thread.start()

    ping = PingRequest(src=cid, dest=aid)
    exchange.send(aid, ping)
    message = exchange.recv(cid)
    assert isinstance(message, PingResponse)

    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    assert not thread.is_alive()


def test_agent_action_message(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(CounterBehavior(), aid=aid, exchange=exchange)
    thread = threading.Thread(target=agent)
    thread.start()

    value = 42
    request = ActionRequest(src=cid, dest=aid, action='add', args=(value,))
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result is None

    request = ActionRequest(src=cid, dest=aid, action='count')
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert message.exception is None
    assert message.result == value

    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    assert not thread.is_alive()


def test_agent_action_message_error(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(ErrorBehavior(), aid=aid, exchange=exchange)
    thread = threading.Thread(target=agent)
    thread.start()

    request = ActionRequest(src=cid, dest=aid, action='fails')
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert isinstance(message.exception, RuntimeError)
    assert 'This action always fails.' in str(message.exception)

    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    assert not thread.is_alive()


def test_agent_action_message_unknown(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    cid = exchange.create_client()

    agent = Agent(EmptyBehavior(), aid=aid, exchange=exchange)
    thread = threading.Thread(target=agent)
    thread.start()

    request = ActionRequest(src=cid, dest=aid, action='null')
    exchange.send(aid, request)
    message = exchange.recv(cid)
    assert isinstance(message, ActionResponse)
    assert isinstance(message.exception, AttributeError)
    assert 'null' in str(message.exception)

    shutdown = ShutdownRequest(src=cid, dest=aid)
    exchange.send(aid, shutdown)

    thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    assert not thread.is_alive()


def test_agent_log_bad_response() -> None:
    agent = Agent(EmptyBehavior())
    response = PingResponse(
        src=AgentIdentifier.new(),
        dest=AgentIdentifier.new(),
    )
    agent._response_handler(response)


class HandleBindingBehavior(Behavior):
    def __init__(
        self,
        unbound: UnboundRemoteHandle[EmptyBehavior],
        client_bound: ClientRemoteHandle[EmptyBehavior],
        agent_bound: AgentRemoteHandle[EmptyBehavior],
        self_bound: AgentRemoteHandle[EmptyBehavior],
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


def test_agent_run_bind_handles(exchange: Exchange) -> None:
    aid = exchange.create_agent()
    behavior = HandleBindingBehavior(
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


class DuplicateBindingsBehavior(Behavior):
    def __init__(
        self,
        handle1: UnboundRemoteHandle[EmptyBehavior],
        handle2: UnboundRemoteHandle[EmptyBehavior],
    ) -> None:
        self.handle1 = handle1
        self.handle2 = handle2


def test_agent_run_duplicate_handles_error(exchange: Exchange) -> None:
    self_aid = exchange.create_agent()
    remote_aid = exchange.create_agent()
    behavior = DuplicateBindingsBehavior(
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


class RunBehavior(Behavior):
    def __init__(self, doubler: Handle[DoubleBehavior]) -> None:
        self.doubler = doubler

    def shutdown(self) -> None:
        assert isinstance(self.doubler, AgentRemoteHandle)
        self.doubler.shutdown()

    @action
    def run(self, value: int) -> int:
        return self.doubler.action('double', value).result()


class DoubleBehavior(Behavior):
    @action
    def double(self, value: int) -> int:
        return 2 * value


def test_agent_to_handle_handles(exchange: Exchange) -> None:
    runner_id = exchange.create_agent()
    doubler_id = exchange.create_agent()

    runner_handle: UnboundRemoteHandle[RunBehavior] = exchange.create_handle(
        runner_id,
    )
    doubler_handle: UnboundRemoteHandle[DoubleBehavior] = (
        exchange.create_handle(doubler_id)
    )

    runner_behavior = RunBehavior(doubler_handle)
    doubler_behavior = DoubleBehavior()

    runner_agent = Agent(runner_behavior, aid=runner_id, exchange=exchange)
    doubler_agent = Agent(doubler_behavior, aid=doubler_id, exchange=exchange)

    runner_thread = threading.Thread(target=runner_agent)
    doubler_thread = threading.Thread(target=doubler_agent)

    runner_thread.start()
    doubler_thread.start()

    runner_client = runner_handle.bind_as_client()
    future: Future[int] = runner_client.action('run', 1)
    assert future.result() == 2  # noqa: PLR2004

    runner_client.shutdown()

    runner_thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
    doubler_thread.join(timeout=TEST_THREAD_JOIN_TIMEOUT)
