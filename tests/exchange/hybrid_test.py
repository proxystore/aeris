from __future__ import annotations

import logging
import pickle
from unittest import mock

import pytest

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange.hybrid import HybridExchange
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_SLEEP
from testing.constant import TEST_THREAD_JOIN_TIMEOUT
from testing.redis import MockRedis


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_open_close_exchange(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_serialize_exchange(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        dumped = pickle.dumps(exchange)
        reconstructed = pickle.loads(dumped)
        reconstructed.close()


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_key_namespaces(mock_redis) -> None:
    namespace = 'foo'
    uid = ClientIdentifier.new()
    with HybridExchange(
        redis_host='localhost',
        redis_port=0,
        namespace=namespace,
    ) as exchange:
        assert exchange._address_key(uid).startswith(f'{namespace}:')
        assert exchange._status_key(uid).startswith(f'{namespace}:')
        assert exchange._queue_key(uid).startswith(f'{namespace}:')


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_send_bad_identifier(mock_redis) -> None:
    uid = ClientIdentifier.new()
    message = PingRequest(src=uid, dest=uid)
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        with pytest.raises(BadIdentifierError):
            exchange.send(uid, message)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_send_mailbox_closed(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        uid = exchange.create_client()
        exchange.close_mailbox(uid)
        message = PingRequest(src=uid, dest=uid)
        with pytest.raises(MailboxClosedError):
            exchange.send(uid, message)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_create_close_mailbox(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        uid = exchange.create_client()
        with exchange.get_mailbox(uid) as mailbox:
            assert mailbox.exchange is exchange
            assert mailbox.mailbox_id == uid


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_create_mailbox_bad_identifier(mock_redis) -> None:
    uid = ClientIdentifier.new()
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(uid)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_send_to_mailbox_direct(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        aid = exchange.create_agent()
        cid = exchange.create_client()
        with exchange.get_mailbox(aid) as mailbox:
            message = PingRequest(src=cid, dest=aid)
            for _ in range(3):
                exchange.send(aid, message)
                assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT) == message


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_send_to_mailbox_indirect(mock_redis) -> None:
    messages = 3
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        aid = exchange.create_agent()
        cid = exchange.create_client()
        message = PingRequest(src=cid, dest=aid)
        for _ in range(messages):
            exchange.send(aid, message)
        with exchange.get_mailbox(aid) as mailbox:
            for _ in range(messages):
                assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT) == message


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_recv_closed(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        aid = exchange.create_agent()
        with exchange.get_mailbox(aid) as mailbox:
            exchange.close_mailbox(aid)
            with pytest.raises(MailboxClosedError):
                mailbox.recv(timeout=TEST_SLEEP)


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_mailbox_redis_error_logging(mock_redis, caplog) -> None:
    caplog.set_level(logging.ERROR)
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        aid = exchange.create_agent()
        with mock.patch(
            'aeris.exchange.hybrid.HybridMailbox._pull_messages_from_redis',
            side_effect=RuntimeError('Mock thread error.'),
        ):
            mailbox = exchange.get_mailbox(aid)
            mailbox._redis_thread.join(TEST_THREAD_JOIN_TIMEOUT)
            mailbox.close()

            assert any(
                f'Error in redis watcher thread for {aid}' in record.message
                for record in caplog.records
                if record.levelname == 'ERROR'
            )


@mock.patch('redis.Redis', side_effect=MockRedis)
def test_send_to_mailbox_bad_cached_address(mock_redis) -> None:
    with HybridExchange(redis_host='localhost', redis_port=0) as exchange:
        aid = exchange.create_agent()
        cid = exchange.create_client()
        message = PingRequest(src=cid, dest=aid)

        with exchange.get_mailbox(aid) as mailbox:
            exchange.send(aid, message)
            assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT) == message

        # Address of mailbox is now in the exchanges cache but
        # the mailbox is no longer listening on that address.
        assert aid in exchange._address_cache

        # This send will try the cached address, fail, catch the error,
        # and retry via redis.
        exchange.send(aid, message)
        with exchange.get_mailbox(aid) as mailbox:
            assert mailbox.recv(timeout=TEST_CONNECTION_TIMEOUT) == message
