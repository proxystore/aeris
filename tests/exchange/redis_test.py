from __future__ import annotations

import pickle
from unittest import mock

import pytest

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange.redis import RedisExchange
from aeris.exchange.redis import RedisMailbox
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.redis import MockRedis


def test_mailbox_behavior() -> None:
    uid = AgentIdentifier.new()
    client = MockRedis()

    with RedisMailbox(uid, client) as mailbox:
        message = PingRequest(src=uid, dest=uid)

        for _ in range(3):
            mailbox.send(message)
            received = mailbox.recv()
            assert isinstance(received, PingRequest)
            assert received == message

    mailbox.close()  # Idempotent check

    with pytest.raises(MailboxClosedError):
        mailbox.send(message)

    with pytest.raises(MailboxClosedError):
        mailbox.recv()


def test_mailbox_timeout() -> None:
    uid = AgentIdentifier.new()
    client = MockRedis()

    with RedisMailbox(uid, client, timeout=TEST_CONNECTION_TIMEOUT) as mailbox:
        with pytest.raises(TimeoutError):
            mailbox.recv()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_serialization(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        assert isinstance(str(exchange), str)
        assert isinstance(repr(exchange), str)
        pickled = pickle.dumps(exchange)
        reconstructed = pickle.loads(pickled)
        assert isinstance(reconstructed, RedisExchange)
        reconstructed.close()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_behavior(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = exchange.register_agent()
        cid = exchange.register_client()

        handle = exchange.create_handle(aid)
        assert isinstance(handle, Handle)
        handle.close()

        mailbox = exchange.get_mailbox(cid)
        assert isinstance(mailbox, RedisMailbox)

        exchange.unregister(aid)
        exchange.unregister(cid)
        exchange.unregister(cid)  # Idempotent check


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_bad_identifier(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        aid = AgentIdentifier.new()

        with pytest.raises(BadIdentifierError):
            exchange.create_handle(aid)

        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(aid)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_exchange_create_handle_wrong_entity_type(mock_redis) -> None:
    with RedisExchange('localhost', port=0) as exchange:
        with pytest.raises(TypeError):
            exchange.create_handle(ClientIdentifier.new())  # type: ignore[arg-type]
