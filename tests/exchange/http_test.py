from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from unittest import mock

import pytest
import pytest_asyncio
import requests
from aiohttp.test_utils import TestClient
from aiohttp.test_utils import TestServer
from aiohttp.web import Application
from aiohttp.web import Request

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange.http import _BAD_REQUEST_CODE
from academy.exchange.http import _MailboxManager
from academy.exchange.http import _main
from academy.exchange.http import _NOT_FOUND_CODE
from academy.exchange.http import create_app
from academy.exchange.http import HttpExchange
from academy.exchange.http import spawn_http_exchange
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


def test_server_cli() -> None:
    with mock.patch('academy.exchange.http._run'):
        assert _main(['--port', '0']) == 0


def test_spawn_http_exchange() -> None:
    with spawn_http_exchange(
        'localhost',
        open_port(),
        level=logging.ERROR,
        timeout=TEST_CONNECTION_TIMEOUT,
    ) as exchange:
        assert isinstance(exchange, HttpExchange)


@pytest.mark.asyncio
async def test_mailbox_manager_create_close() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    # Should do nothing since mailbox doesn't exist
    await manager.terminate(uid)
    assert not manager.check_mailbox(uid)
    manager.create_mailbox(uid)
    assert manager.check_mailbox(uid)
    manager.create_mailbox(uid)  # Idempotent check
    await manager.terminate(uid)
    await manager.terminate(uid)  # Idempotent check


@pytest.mark.asyncio
async def test_mailbox_manager_send_recv() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    manager.create_mailbox(uid)

    message = PingRequest(src=uid, dest=uid)
    await manager.put(message)
    assert await manager.get(uid) == message

    await manager.terminate(uid)


@pytest.mark.asyncio
async def test_mailbox_manager_bad_identifier() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(BadEntityIdError):
        await manager.get(uid)

    with pytest.raises(BadEntityIdError):
        await manager.put(message)


@pytest.mark.asyncio
async def test_mailbox_manager_mailbox_closed() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    manager.create_mailbox(uid)
    await manager.terminate(uid)
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(MailboxClosedError):
        await manager.get(uid)

    with pytest.raises(MailboxClosedError):
        await manager.put(message)


@pytest_asyncio.fixture
async def cli() -> AsyncGenerator[TestClient[Request, Application]]:
    app = create_app()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.mark.asyncio
async def test_create_mailbox_validation_error(cli) -> None:
    response = await cli.post('/mailbox', json={'mailbox': 'foo'})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_terminate_validation_error(cli) -> None:
    response = await cli.delete('/mailbox', json={'mailbox': 'foo'})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_discover_validation_error(cli) -> None:
    response = await cli.get('/discover', json={})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid arguments'


@pytest.mark.asyncio
async def test_check_mailbox_validation_error(cli) -> None:
    response = await cli.get('/mailbox', json={'mailbox': 'foo'})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid mailbox ID'


@pytest.mark.asyncio
async def test_send_mailbox_validation_error(cli) -> None:
    response = await cli.put('/message', json={'message': 'foo'})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid message'


@pytest.mark.asyncio
async def test_recv_mailbox_validation_error(cli) -> None:
    response = await cli.get('/message', json={'mailbox': 'foo'})
    assert response.status == _BAD_REQUEST_CODE
    assert await response.text() == 'Missing or invalid mailbox ID'

    response = await cli.get(
        '/message',
        json={'mailbox': ClientId.new().model_dump_json()},
    )
    assert response.status == _NOT_FOUND_CODE
    assert await response.text() == 'Unknown mailbox ID'
