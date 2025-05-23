from __future__ import annotations

import logging
from unittest import mock

import pytest
import requests

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange.cloud.client import HttpExchange
from academy.exchange.cloud.client import spawn_http_exchange
from academy.identifier import ClientId
from academy.message import PingRequest
from academy.socket import open_port
from testing.behavior import EmptyBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_SLEEP


def test_simple_exchange_repr() -> None:
    with HttpExchange('localhost', 0) as exchange:
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)


def test_create_terminate(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        exchange.register_agent(
            EmptyBehavior,
            agent_id=aid,
        )  # Idempotency check
        exchange.terminate(aid)
        exchange.terminate(aid)  # Idempotency check


def test_create_mailbox_bad_identifier(
    http_exchange_server: tuple[str, int],
) -> None:
    host, port = http_exchange_server
    cid = ClientId.new()
    with HttpExchange(host, port) as exchange:
        with pytest.raises(BadEntityIdError):
            exchange.get_mailbox(cid)


def test_send_and_recv(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        cid = exchange.register_client()
        aid = exchange.register_agent(EmptyBehavior)

        message = PingRequest(src=cid, dest=aid)
        exchange.send(aid, message)

        mailbox = exchange.get_mailbox(aid)
        assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT) == message
        mailbox.close()


def test_send_bad_identifer(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    cid = ClientId.new()
    with HttpExchange(host, port) as exchange:
        message = PingRequest(src=cid, dest=cid)
        with pytest.raises(BadEntityIdError):
            exchange.send(cid, message)


def test_send_mailbox_closed(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        exchange.terminate(aid)
        message = PingRequest(src=aid, dest=aid)
        with pytest.raises(MailboxClosedError):
            exchange.send(aid, message)


def test_recv_timeout(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        mailbox = exchange.get_mailbox(aid)
        with mock.patch.object(
            exchange._session,
            'get',
            side_effect=requests.exceptions.Timeout,
        ):
            with pytest.raises(TimeoutError):
                assert mailbox.recv(timeout=TEST_SLEEP)


def test_recv_mailbox_closed(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        exchange.terminate(aid)
        mailbox = exchange.get_mailbox(aid)
        with pytest.raises(MailboxClosedError):
            assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT)


class A(Behavior): ...


class B(Behavior): ...


class C(B): ...


def test_exchange_discover(http_exchange_server: tuple[str, int]) -> None:
    host, port = http_exchange_server
    with HttpExchange(host, port) as exchange:
        bid = exchange.register_agent(B)
        cid = exchange.register_agent(C)
        did = exchange.register_agent(C)
        exchange.terminate(did)

        assert len(exchange.discover(A)) == 0
        assert exchange.discover(B, allow_subclasses=False) == (bid,)
        assert exchange.discover(B, allow_subclasses=True) == (bid, cid)


def test_additional_headers() -> None:
    with HttpExchange(
        'localhost',
        0,
        {'Authorization': 'fake auth'},
    ) as exchange:
        assert 'Authorization' in exchange._session.headers


def test_spawn_http_exchange() -> None:
    with spawn_http_exchange(
        'localhost',
        open_port(),
        level=logging.ERROR,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as exchange:
        assert isinstance(exchange, HttpExchange)
