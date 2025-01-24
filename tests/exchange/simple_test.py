from __future__ import annotations

import asyncio
import pickle
import socket
import threading
import time
from collections.abc import Generator
from unittest import mock

import pytest

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.exchange.simple import _BadRequestError
from aeris.exchange.simple import _BaseExchangeMessage
from aeris.exchange.simple import _ExchangeMessage
from aeris.exchange.simple import _ExchangeMessageType
from aeris.exchange.simple import _ExchangeRequestMessage
from aeris.exchange.simple import _ExchangeResponseMessage
from aeris.exchange.simple import _MailboxManager
from aeris.exchange.simple import _main
from aeris.exchange.simple import _serve_forever
from aeris.exchange.simple import SimpleExchange
from aeris.exchange.simple import SimpleServer
from aeris.identifier import AgentIdentifier
from aeris.message import PingRequest
from aeris.message import PingResponse
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP
from testing.constant import TEST_SLEEP
from testing.sys import open_port


@pytest.fixture
def simple_exchange_server() -> Generator[tuple[str, int]]:
    host, port = 'localhost', open_port()
    server = SimpleServer(host, port)
    loop = asyncio.new_event_loop()
    stop = loop.create_future()

    def _target() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.serve_forever(stop))
        loop.close()

    handle = threading.Thread(target=_target)
    handle.start()

    # Wait for server to be listening
    waited = 0.0
    while True:
        try:
            start = time.perf_counter()
            with socket.create_connection(
                (host, port),
                timeout=TEST_LOOP_SLEEP,
            ):
                break
        except OSError as e:  # pragma: no cover
            if waited > TEST_CONNECTION_TIMEOUT:
                raise TimeoutError from e
            end = time.perf_counter()
            sleep = max(0, TEST_LOOP_SLEEP - (end - start))
            time.sleep(sleep)
            waited += sleep

    yield host, port

    loop.call_soon_threadsafe(stop.set_result, None)
    handle.join(timeout=TEST_CONNECTION_TIMEOUT)
    if handle.is_alive():  # pragma: no cover
        raise TimeoutError(
            'Server thread did not gracefully exit within '
            f'{TEST_CONNECTION_TIMEOUT} seconds.',
        )


@pytest.mark.parametrize(
    'message',
    (
        _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=AgentIdentifier.new(),
        ),
        _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=AgentIdentifier.new(),
            dest=AgentIdentifier.new(),
            payload=PingRequest(
                src=AgentIdentifier.new(),
                dest=AgentIdentifier.new(),
            ),
        ),
        _ExchangeResponseMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=AgentIdentifier.new(),
            payload=PingResponse(
                src=AgentIdentifier.new(),
                dest=AgentIdentifier.new(),
            ),
        ),
        _ExchangeResponseMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=AgentIdentifier.new(),
            error=Exception(),
        ),
    ),
)
def test_serialize_exchange_message(message: _ExchangeMessage) -> None:
    raw = message.model_serialize()
    reconstructed = _BaseExchangeMessage.model_deserialize(raw)
    assert message == reconstructed
    # Some message types implement custom __eq__ so this covers the
    # comparison to random object type check
    assert message != 'message'


def test_exchange_response_success() -> None:
    response = _ExchangeResponseMessage(
        kind=_ExchangeMessageType.CREATE_MAILBOX,
        src=AgentIdentifier.new(),
        payload=PingResponse(
            src=AgentIdentifier.new(),
            dest=AgentIdentifier.new(),
        ),
    )
    assert response.success

    response = _ExchangeResponseMessage(
        kind=_ExchangeMessageType.CREATE_MAILBOX,
        src=AgentIdentifier.new(),
        error=Exception(),
    )
    assert not response.success


@pytest.mark.asyncio
async def test_mailbox_manager_create_close() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    manager.create_mailbox(uid)
    manager.create_mailbox(uid)  # Idempotent check
    await manager.close_mailbox(uid)
    await manager.close_mailbox(uid)  # Idempotent check


@pytest.mark.asyncio
async def test_mailbox_manager_send_recv() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    manager.create_mailbox(uid)

    message = PingRequest(src=uid, dest=uid)
    await manager.put(message)
    assert await manager.get(uid) == message

    await manager.close_mailbox(uid)


@pytest.mark.asyncio
async def test_mailbox_manager_bad_identifier() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(BadIdentifierError):
        await manager.get(uid)

    with pytest.raises(BadIdentifierError):
        await manager.put(message)


@pytest.mark.asyncio
async def test_mailbox_manager_mailbox_closed() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    manager.create_mailbox(uid)
    await manager.close_mailbox(uid)
    message = PingRequest(src=uid, dest=uid)

    with pytest.raises(MailboxClosedError):
        await manager.get(uid)

    with pytest.raises(MailboxClosedError):
        await manager.put(message)


def test_server_cli() -> None:
    with mock.patch('aeris.exchange.simple._serve_forever'):
        assert _main(['--port', '0']) == 0


@pytest.mark.asyncio
async def test_server_serve_forever() -> None:
    server = SimpleServer('localhost', port=0)
    assert isinstance(repr(server), str)
    assert isinstance(str(server), str)

    stop = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(_serve_forever(server, stop))
    await asyncio.sleep(TEST_SLEEP)
    stop.set_result(None)
    await task
    task.result()


@pytest.mark.asyncio
async def test_server_handle_create_mailbox() -> None:
    server = SimpleServer('localhost', port=0)
    request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.CREATE_MAILBOX,
        src=AgentIdentifier.new(),
    )
    expected = request.response()

    response = await server._handle_request(request)
    assert response == expected

    response = await server._handle_request(request)  # Idempotent check
    assert response == expected


@pytest.mark.asyncio
async def test_server_handle_close_mailbox() -> None:
    server = SimpleServer('localhost', port=0)
    request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.CLOSE_MAILBOX,
        src=AgentIdentifier.new(),
    )
    expected = request.response()

    response = await server._handle_request(request)
    assert response == expected

    response = await server._handle_request(request)  # Idempotent check
    assert response == expected


@pytest.mark.asyncio
async def test_server_handle_send_request_message() -> None:
    server = SimpleServer('localhost', port=0)
    uid = AgentIdentifier.new()
    server.manager.create_mailbox(uid)

    send_request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.SEND_MESSAGE,
        src=uid,
        dest=uid,
        payload=PingRequest(src=uid, dest=uid),
    )
    expected = send_request.response()
    send_response = await server._handle_request(send_request)
    assert send_response == expected

    recv_request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.REQUEST_MESSAGE,
        src=uid,
    )
    expected = recv_request.response(payload=send_request.payload)
    recv_response = await server._handle_request(recv_request)
    assert recv_response == expected


@pytest.mark.asyncio
async def test_server_handle_malformed_send_message() -> None:
    server = SimpleServer('localhost', port=0)
    uid = AgentIdentifier.new()
    server.manager.create_mailbox(uid)

    request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.SEND_MESSAGE,
        src=uid,
    )
    response = await server._handle_request(request)
    assert isinstance(response.error, _BadRequestError)


@pytest.mark.asyncio
async def test_server_handle_send_request_error() -> None:
    server = SimpleServer('localhost', port=0)
    uid = AgentIdentifier.new()

    send_request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.SEND_MESSAGE,
        src=uid,
        dest=uid,
        payload=PingRequest(src=uid, dest=uid),
    )
    send_response = await server._handle_request(send_request)
    assert isinstance(send_response.error, BadIdentifierError)

    recv_request = _ExchangeRequestMessage(
        kind=_ExchangeMessageType.REQUEST_MESSAGE,
        src=uid,
    )
    recv_response = await server._handle_request(recv_request)
    assert isinstance(recv_response.error, BadIdentifierError)


@pytest.mark.asyncio
async def test_server_handle_parse_message_error() -> None:
    server = SimpleServer('localhost', port=0)

    reader = mock.Mock(spec=asyncio.StreamReader)
    reader.at_eof.return_value = False
    reader.readline.return_value = b'random-data'
    writer = mock.Mock(spec=asyncio.StreamWriter)

    await server._handle_client(reader, writer)


@pytest.mark.asyncio
async def test_server_handle_drop_bad_type() -> None:
    server = SimpleServer('localhost', port=0)

    message = _ExchangeResponseMessage(
        kind=_ExchangeMessageType.CREATE_MAILBOX,
        src=AgentIdentifier.new(),
    )
    reader = mock.Mock(spec=asyncio.StreamReader)
    reader.at_eof.return_value = False
    reader.readline.side_effect = [
        message.model_serialize(),
        # Pass random data to make _handle_client exit
        b'random-data',
    ]
    writer = mock.Mock(spec=asyncio.StreamWriter)

    await server._handle_client(reader, writer)


def test_exchange_create_close_mailbox(
    simple_exchange_server: tuple[str, int],
) -> None:
    host, port = simple_exchange_server
    with SimpleExchange(host, port) as exchange:
        uid = AgentIdentifier.new()
        exchange.create_mailbox(uid)
        exchange.create_mailbox(uid)  # Idempotent check
        exchange.close_mailbox(uid)
        exchange.close_mailbox(uid)  # Idempotent check


def test_exchange_serialize(simple_exchange_server: tuple[str, int]) -> None:
    host, port = simple_exchange_server
    with SimpleExchange(host, port) as exchange1:
        assert isinstance(exchange1, Exchange)
        pickled = pickle.dumps(exchange1)
        with pickle.loads(pickled) as exchange2:
            assert isinstance(exchange2, Exchange)
            assert repr(exchange1) == repr(exchange2)
            assert str(exchange1) == str(exchange2)


def test_exchange_drops_bad_server_message_type(
    simple_exchange_server: tuple[str, int],
) -> None:
    host, port = simple_exchange_server
    with SimpleExchange(host, port) as exchange:
        # Server should never send back a request type
        message = _ExchangeRequestMessage(
            kind=_ExchangeMessageType.CREATE_MAILBOX,
            src=AgentIdentifier.new(),
        )
        # Server should log but otherwise drop the message
        exchange._handle_message(message)


def test_exchange_disconnect_message_parse_error(
    simple_exchange_server: tuple[str, int],
) -> None:
    host, port = simple_exchange_server
    with mock.patch('socket.socket') as mock_socket:
        mock_socket.return_value.recv.return_value = b'random-bytes'

        with SimpleExchange(host, port) as exchange:
            # Message handler thread will start, immediately read the bad
            # data, fail to parse it as a message, and exit so we just
            # wait on the thread here.
            exchange._handler_thread.join(timeout=TEST_CONNECTION_TIMEOUT)
            assert not exchange._handler_thread.is_alive()


def test_exchange_send_messages(
    simple_exchange_server: tuple[str, int],
) -> None:
    host, port = simple_exchange_server
    with SimpleExchange(host, port) as exchange:
        aid1 = exchange.create_agent()
        aid2 = exchange.create_agent()
        message = PingRequest(src=aid1, dest=aid2)
        exchange.send(aid2, message)
        assert exchange.recv(aid2) == message


def test_exchange_send_recv_bad_identifier(
    simple_exchange_server: tuple[str, int],
) -> None:
    host, port = simple_exchange_server
    with SimpleExchange(host, port) as exchange:
        aid = AgentIdentifier.new()
        message = PingRequest(src=aid, dest=aid)

        with pytest.raises(BadIdentifierError):
            exchange.send(aid, message)
        with pytest.raises(BadIdentifierError):
            exchange.recv(aid)
