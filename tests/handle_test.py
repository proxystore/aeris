from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from unittest import mock

import pytest

from aeris.exception import HandleClosedError
from aeris.exception import HandleNotBoundError
from aeris.exchange import Exchange
from aeris.exchange.thread import ThreadExchange
from aeris.handle import BoundRemoteHandle
from aeris.handle import ClientRemoteHandle
from aeris.handle import Handle
from aeris.handle import ProxyHandle
from aeris.handle import RemoteHandle
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.launcher.thread import ThreadLauncher
from aeris.message import PingRequest
from testing.behavior import CounterBehavior
from testing.behavior import EmptyBehavior
from testing.behavior import ErrorBehavior
from testing.behavior import SleepBehavior
from testing.constant import TEST_SLEEP


def test_proxy_handle_protocol() -> None:
    behavior = EmptyBehavior()
    handle = ProxyHandle(behavior)
    assert isinstance(handle, Handle)
    assert str(behavior) in str(handle)
    assert repr(behavior) in repr(handle)


def test_proxy_handle_actions() -> None:
    handle = ProxyHandle(CounterBehavior())
    assert handle.action('add', 1).result() is None
    assert handle.action('count').result() == 1


def test_proxy_handle_errors() -> None:
    handle = ProxyHandle(ErrorBehavior())
    with pytest.raises(RuntimeError, match='This action always fails.'):
        handle.action('fails').result()
    with pytest.raises(AttributeError, match='null'):
        handle.action('null').result()


def test_unbound_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    handle: UnboundRemoteHandle[Any]
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) == str(handle)
            assert repr(reconstructed) == repr(handle)


def test_unbound_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    handle: UnboundRemoteHandle[Any]
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        client_bound: ClientRemoteHandle[Any]
        with handle.bind_as_client() as client_bound:
            assert isinstance(client_bound, ClientRemoteHandle)
        agent_bound: BoundRemoteHandle[Any]
        with handle.bind_to_mailbox(AgentIdentifier.new()) as agent_bound:
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_unbound_remote_handle_errors(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    handle: UnboundRemoteHandle[Any]
    with UnboundRemoteHandle(exchange, agent_id) as handle:
        request = PingRequest(src=ClientIdentifier.new(), dest=agent_id)
        with pytest.raises(HandleNotBoundError):
            handle._send_request(request)
        with pytest.raises(HandleNotBoundError):
            handle.action('foo')
        with pytest.raises(HandleNotBoundError):
            handle.ping()
        with pytest.raises(HandleNotBoundError):
            handle.shutdown()


def test_remote_handle_closed_error(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    handles: list[RemoteHandle[Any]] = [
        BoundRemoteHandle(exchange, agent_id, exchange.create_agent()),
        ClientRemoteHandle(exchange, agent_id, exchange.create_client()),
    ]
    for handle in handles:
        handle.close()
        assert handle.mailbox_id is not None
        with pytest.raises(HandleClosedError):
            handle.action('foo')
        with pytest.raises(HandleClosedError):
            handle.ping()
        with pytest.raises(HandleClosedError):
            handle.shutdown()


def test_agent_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    mailbox_id = exchange.create_agent()
    handle: BoundRemoteHandle[Any]
    with BoundRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) != str(handle)
            assert repr(reconstructed) != repr(handle)
            assert reconstructed.agent_id == handle.agent_id


def test_agent_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    mailbox_id = exchange.create_agent()
    handle: BoundRemoteHandle[Any]
    with BoundRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        assert isinstance(handle.mailbox_id, AgentIdentifier)
        client_bound: ClientRemoteHandle[Any]
        with handle.bind_as_client() as client_bound:
            assert isinstance(client_bound, ClientRemoteHandle)
        with pytest.raises(
            ValueError,
            match=f'Cannot create handle to {handle.agent_id}',
        ):
            handle.bind_to_mailbox(handle.agent_id)
        agent_bound: BoundRemoteHandle[Any]
        with handle.bind_to_mailbox(handle.mailbox_id) as agent_bound:
            assert agent_bound is handle
        with handle.bind_to_mailbox(AgentIdentifier.new()) as agent_bound:
            assert agent_bound is not handle
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_client_remote_handle_serialize(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    mailbox_id = exchange.create_client()
    handle: ClientRemoteHandle[Any]
    with ClientRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        # Note: don't call pickle.dumps here because ThreadExchange
        # is not pickleable so we test __reduce__ directly.
        class_, args = handle.__reduce__()
        with class_(*args) as reconstructed:
            assert isinstance(reconstructed, UnboundRemoteHandle)
            assert str(reconstructed) != str(handle)
            assert repr(reconstructed) != repr(handle)
            assert reconstructed.agent_id == handle.agent_id


def test_client_remote_handle_bind(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()
    mailbox_id = exchange.create_client()
    handle: ClientRemoteHandle[Any]
    with ClientRemoteHandle(exchange, agent_id, mailbox_id) as handle:
        assert handle.bind_as_client() is handle
        client_bound: ClientRemoteHandle[Any]
        with handle.bind_as_client(exchange.create_client()) as client_bound:
            assert client_bound is not handle
            assert isinstance(client_bound, ClientRemoteHandle)
        agent_bound: BoundRemoteHandle[Any]
        with handle.bind_to_mailbox(AgentIdentifier.new()) as agent_bound:
            assert isinstance(agent_bound, BoundRemoteHandle)


def test_client_remote_handle_log_bad_response(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = EmptyBehavior()
    handle: RemoteHandle[Any]
    with launcher.launch(behavior, exchange) as handle:
        client = handle.bind_as_client()
        assert client.mailbox_id is not None
        # Should log but not crash
        client.exchange.send(
            client.mailbox_id,
            PingRequest(src=client.agent_id, dest=client.mailbox_id),
        )
        assert client.ping() > 0


@pytest.mark.filterwarnings(
    'ignore:.*:pytest.PytestUnhandledThreadExceptionWarning',
)
def test_client_remote_handle_recv_thread_crash(exchange: Exchange) -> None:
    agent_id = exchange.create_agent()

    with mock.patch(
        'aeris.handle.ClientRemoteHandle._recv_responses',
        side_effect=Exception(),
    ):
        handle: ClientRemoteHandle[Any] = ClientRemoteHandle(
            exchange,
            agent_id,
        )

    with pytest.raises(
        RuntimeError,
        match='This likely means the listener thread crashed.',
    ):
        handle.close()


def test_client_remote_handle_actions(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = CounterBehavior()
    with launcher.launch(behavior, exchange).bind_as_client() as handle:
        assert handle.ping() > 0

        add_future: Future[None] = handle.action('add', 1)
        add_future.result()

        count_future: Future[int] = handle.action('count')
        assert count_future.result() == 1

        handle.shutdown()


def test_client_remote_handle_errors(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = ErrorBehavior()
    with launcher.launch(behavior, exchange).bind_as_client() as handle:
        with pytest.raises(RuntimeError, match='This action always fails.'):
            handle.action('fails').result()
        with pytest.raises(AttributeError, match='null'):
            handle.action('null').result()


def test_client_remote_handle_wait_futures(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = SleepBehavior()
    handle = launcher.launch(behavior, exchange).bind_as_client()

    future: Future[None] = handle.action('sleep', TEST_SLEEP)
    handle.close(wait_futures=True)
    future.result(timeout=0)


def test_client_remote_handle_cancel_futures(
    exchange: ThreadExchange,
    launcher: ThreadLauncher,
) -> None:
    behavior = SleepBehavior()
    handle = launcher.launch(behavior, exchange).bind_as_client()

    future: Future[None] = handle.action('sleep', TEST_SLEEP)
    handle.close(wait_futures=False)
    assert future.cancelled()
