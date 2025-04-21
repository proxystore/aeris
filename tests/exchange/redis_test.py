from __future__ import annotations

import pickle
from typing import Any
from unittest import mock

import pytest

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import Exchange
from academy.exchange.redis import RedisExchange
from academy.handle import RemoteHandle
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.message import PingRequest
from testing.behavior import EmptyBehavior
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.redis import MockRedis


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_basic_usage(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.register_agent(EmptyBehavior)
        exchange.register_agent(
            EmptyBehavior,
            agent_id=aid,
        )  # Idempotency check
        cid = exchange.register_client()

        assert isinstance(aid, AgentId)
        assert isinstance(cid, ClientId)

        mailbox = exchange.get_mailbox(aid)

        for _ in range(3):
            message = PingRequest(src=cid, dest=aid)
            exchange.send(aid, message)
            assert mailbox.recv() == message

        mailbox.close()
        mailbox.close()  # Idempotency check

        exchange.terminate(aid)
        exchange.terminate(cid)
        exchange.terminate(cid)  # Idempotency check


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_bad_identifier_error(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        uid = ClientId.new()
        with pytest.raises(BadEntityIdError):
            exchange.send(uid, PingRequest(src=uid, dest=uid))
        with pytest.raises(BadEntityIdError):
            exchange.get_mailbox(uid)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_closed_error(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        mailbox = exchange.get_mailbox(aid)
        exchange.terminate(aid)
        with pytest.raises(MailboxClosedError):
            exchange.send(aid, PingRequest(src=aid, dest=aid))
        with pytest.raises(MailboxClosedError):
            mailbox.recv()
        mailbox.close()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_get_handle_to_client(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        handle: RemoteHandle[Any] = exchange.get_handle(aid)
        handle.close()

        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.get_handle(ClientId.new())  # type: ignore[arg-type]


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_timeout(mock_redis) -> None:
    with RedisExchange(
        'localhost',
        port=0,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        mailbox = exchange.get_mailbox(aid)
        with pytest.raises(TimeoutError):
            mailbox.recv(timeout=0.001)
        mailbox.close()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_serialization(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        pickled = pickle.dumps(exchange)
        reconstructed = pickle.loads(pickled)
        assert isinstance(reconstructed, RedisExchange)
        reconstructed.close()


class A(Behavior): ...


class B(Behavior): ...


class C(B): ...


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_discover(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        bid = exchange.register_agent(B)
        cid = exchange.register_agent(C)
        did = exchange.register_agent(C)
        exchange.terminate(did)

        assert len(exchange.discover(A)) == 0
        assert exchange.discover(B, allow_subclasses=False) == (bid,)
        assert exchange.discover(B, allow_subclasses=True) == (bid, cid)
