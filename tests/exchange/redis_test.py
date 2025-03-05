from __future__ import annotations

import pickle
from typing import Any
from unittest import mock

import pytest

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.exchange.redis import RedisExchange
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.redis import MockRedis


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_basic_usage(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.create_agent()
        cid = exchange.create_client()
        exchange.create_mailbox(cid)  # Idempotency check

        assert isinstance(aid, AgentIdentifier)
        assert isinstance(cid, ClientIdentifier)

        mailbox = exchange.get_mailbox(aid)

        for _ in range(3):
            message = PingRequest(src=cid, dest=aid)
            exchange.send(aid, message)
            assert mailbox.recv() == message

        mailbox.close()
        mailbox.close()  # Idempotency check

        exchange.close_mailbox(aid)
        exchange.close_mailbox(cid)
        exchange.close_mailbox(cid)  # Idempotency check


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_bad_identifier_error(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        uid = AgentIdentifier.new()
        with pytest.raises(BadIdentifierError):
            exchange.send(uid, PingRequest(src=uid, dest=uid))
        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(uid)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_closed_error(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = exchange.create_agent()
        mailbox = exchange.get_mailbox(aid)
        exchange.close_mailbox(aid)
        with pytest.raises(MailboxClosedError):
            exchange.send(aid, PingRequest(src=aid, dest=aid))
        with pytest.raises(MailboxClosedError):
            mailbox.recv()
        mailbox.close()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_create_handle_to_client(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = exchange.create_agent()
        handle: RemoteHandle[Any] = exchange.create_handle(aid)
        handle.close()

        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.create_handle(ClientIdentifier.new())  # type: ignore[arg-type]


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_timeout(mock_redis) -> None:
    with RedisExchange(
        'localhost',
        port=0,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as exchange:
        aid = exchange.create_agent()
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
