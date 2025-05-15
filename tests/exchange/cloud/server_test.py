from __future__ import annotations

import multiprocessing
import pathlib
import time
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient
from aiohttp.test_utils import TestServer
from aiohttp.web import Application
from aiohttp.web import Request

from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange.cloud.client import HttpExchange
from academy.exchange.cloud.config import ExchangeAuthConfig
from academy.exchange.cloud.config import ExchangeServingConfig
from academy.exchange.cloud.exceptions import UnauthorizedError
from academy.exchange.cloud.login import AcademyExchangeScopes
from academy.exchange.cloud.server import _BAD_REQUEST_CODE
from academy.exchange.cloud.server import _FORBIDDEN_CODE
from academy.exchange.cloud.server import _MailboxManager
from academy.exchange.cloud.server import _main
from academy.exchange.cloud.server import _NOT_FOUND_CODE
from academy.exchange.cloud.server import _OKAY_CODE
from academy.exchange.cloud.server import _run
from academy.exchange.cloud.server import _UNAUTHORIZED_CODE
from academy.exchange.cloud.server import create_app
from academy.identifier import ClientId
from academy.message import PingRequest
from academy.socket import open_port
from testing.ssl import SSLContextFixture


def test_server_cli(tmp_path: pathlib.Path) -> None:
    data = """\
host = "localhost"
port = 1234
certfile = "/path/to/cert.pem"
keyfile = "/path/to/privkey.pem"

[auth]
method = "globus"

[auth.kwargs]
client_id = "ABC"
"""

    filepath = tmp_path / 'exchange.toml'
    with open(filepath, 'w') as f:
        f.write(data)

    with mock.patch('academy.exchange.cloud.server._run'):
        assert _main(['--config', str(filepath)]) == 0


def test_server_run() -> None:
    config = ExchangeServingConfig(host='127.0.0.1', port=open_port())
    context = multiprocessing.get_context('spawn')
    process = context.Process(target=_run, args=(config,))
    process.start()

    while True:
        try:
            exchange = HttpExchange(config.host, config.port, scheme='http')
            exchange.register_client()
        except OSError:  # pragma: no cover
            time.sleep(0.01)
        else:
            # Coverage doesn't detect the singular break but it does
            # get executed to break from the loop
            break  # pragma: no cover
    process.terminate()
    process.join()


def test_server_run_ssl(ssl_context: SSLContextFixture) -> None:
    config = ExchangeServingConfig(host='127.0.0.1', port=open_port())
    config.certfile = ssl_context.certfile
    config.keyfile = ssl_context.keyfile

    context = multiprocessing.get_context('spawn')
    process = context.Process(target=_run, args=(config,))
    process.start()

    while True:
        try:
            exchange = HttpExchange(
                config.host,
                config.port,
                scheme='https',
                ssl_verify=False,
            )
            exchange.register_client()
        except OSError:  # pragma: no cover
            time.sleep(0.01)
        else:
            # Coverage doesn't detect the singular break but it does
            # get executed to break from the loop
            break  # pragma: no cover
    process.terminate()
    process.join()


@pytest.mark.asyncio
async def test_mailbox_manager_create_close() -> None:
    manager = _MailboxManager()
    user_id = str(uuid.uuid4())
    uid = ClientId.new()
    # Should do nothing since mailbox doesn't exist
    await manager.terminate(user_id, uid)
    assert not manager.check_mailbox(user_id, uid)
    manager.create_mailbox(user_id, uid)
    assert manager.check_mailbox(user_id, uid)
    manager.create_mailbox(user_id, uid)  # Idempotent check

    bad_user = str(uuid.uuid4())  # Authentication check
    with pytest.raises(UnauthorizedError):
        manager.create_mailbox(bad_user, uid)
    with pytest.raises(UnauthorizedError):
        manager.check_mailbox(bad_user, uid)
    with pytest.raises(UnauthorizedError):
        await manager.terminate(bad_user, uid)

    await manager.terminate(user_id, uid)
    await manager.terminate(user_id, uid)  # Idempotent check


@pytest.mark.asyncio
async def test_mailbox_manager_send_recv() -> None:
    manager = _MailboxManager()
    user_id = str(uuid.uuid4())
    bad_user = str(uuid.uuid4())
    uid = ClientId.new()
    manager.create_mailbox(user_id, uid)

    message = PingRequest(src=uid, dest=uid)
    with pytest.raises(UnauthorizedError):
        await manager.put(bad_user, message)
    await manager.put(user_id, message)

    with pytest.raises(UnauthorizedError):
        await manager.get(bad_user, uid)
    assert await manager.get(user_id, uid) == message

    await manager.terminate(user_id, uid)


@pytest.mark.asyncio
async def test_mailbox_manager_bad_identifier() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(BadEntityIdError):
        await manager.get(None, uid)

    with pytest.raises(BadEntityIdError):
        await manager.put(None, message)


@pytest.mark.asyncio
async def test_mailbox_manager_mailbox_closed() -> None:
    manager = _MailboxManager()
    uid = ClientId.new()
    manager.create_mailbox(None, uid)
    await manager.terminate(None, uid)
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(MailboxClosedError):
        await manager.get(None, uid)

    with pytest.raises(MailboxClosedError):
        await manager.put(None, message)


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


@pytest.mark.asyncio
async def test_null_auth_cli() -> None:
    auth = ExchangeAuthConfig()
    app = create_app(auth)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/message', json={'mailbox': 'foo'})
        assert response.status == _BAD_REQUEST_CODE
        assert await response.text() == 'Missing or invalid mailbox ID'

        response = await client.get(
            '/message',
            json={'mailbox': ClientId.new().model_dump_json()},
        )
        assert response.status == _NOT_FOUND_CODE
        assert await response.text() == 'Unknown mailbox ID'


@pytest.mark.asyncio
@mock.patch('globus_sdk.ConfidentialAppAuthClient.oauth2_token_introspect')
async def test_globus_auth_cli(
    mock_token_response,
) -> None:
    auth = ExchangeAuthConfig(
        method='globus',
        kwargs={'client_id': str(uuid.uuid4()), 'client_secret': ''},
    )
    token_meta: dict[str, Any] = {
        'active': True,
        'username': 'username',
        'client_id': str(uuid.uuid4()),
        'email': 'username@example.com',
        'name': 'User Name',
        'aud': [AcademyExchangeScopes.resource_server],
    }
    mock_token_response.return_value = token_meta
    app = create_app(auth)
    async with TestClient(TestServer(app)) as client:
        response = await client.get(
            '/discover',
            json={'behavior': 'foo', 'allow_subclasses': False},
            headers={'Authorization': 'Bearer token'},
        )
        assert response.status == _OKAY_CODE


@pytest.mark.asyncio
@mock.patch('globus_sdk.ConfidentialAppAuthClient.oauth2_token_introspect')
async def test_globus_auth_cli_unauthorized(
    mock_token_response,
) -> None:
    auth = ExchangeAuthConfig(
        method='globus',
        kwargs={'client_id': str(uuid.uuid4()), 'client_secret': ''},
    )
    token_meta: dict[str, Any] = {
        'active': True,
        'username': 'username',
        'client_id': str(uuid.uuid4()),
        'email': 'username@example.com',
        'name': 'User Name',
        'aud': [AcademyExchangeScopes.resource_server],
    }
    mock_token_response.return_value = token_meta
    app = create_app(auth)
    async with TestClient(TestServer(app)) as client:
        response = await client.get(
            '/discover',
            json={'behavior': 'foo', 'allow_subclasses': False},
        )
        assert response.status == _UNAUTHORIZED_CODE


@pytest.mark.asyncio
@mock.patch('globus_sdk.ConfidentialAppAuthClient.oauth2_token_introspect')
async def test_globus_auth_cli_forbidden(
    mock_token_response,
) -> None:
    auth = ExchangeAuthConfig(
        method='globus',
        kwargs={'client_id': str(uuid.uuid4()), 'client_secret': ''},
    )
    token_meta: dict[str, Any] = {
        'active': False,
    }
    mock_token_response.return_value = token_meta
    app = create_app(auth)
    async with TestClient(TestServer(app)) as client:
        response = await client.get(
            '/discover',
            json={'behavior': 'foo', 'allow_subclasses': False},
            headers={'Authorization': 'Bearer token'},
        )
        assert response.status == _FORBIDDEN_CODE
