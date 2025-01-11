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
from aeris.exchange import Mailbox
from aeris.exchange.message import ForwardMessage
from aeris.exchange.simple import _AsyncQueue
from aeris.exchange.simple import _MailboxManager
from aeris.exchange.simple import _main
from aeris.exchange.simple import _serve_forever
from aeris.exchange.simple import SimpleExchange
from aeris.exchange.simple import SimpleServer
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest
from testing.constant import TEST_CONNECTION_TIMEOUT
from testing.constant import TEST_LOOP_SLEEP
from testing.sys import open_port


@pytest.fixture
def server_thread() -> Generator[tuple[str, int]]:
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
            with socket.create_connection((host, port)):
                break
        except OSError as e:
            if waited > TEST_CONNECTION_TIMEOUT:  # pragma: no cover
                raise TimeoutError from e
            time.sleep(TEST_LOOP_SLEEP)
            waited += TEST_LOOP_SLEEP

    yield host, port

    loop.call_soon_threadsafe(stop.set_result, None)
    handle.join(timeout=TEST_CONNECTION_TIMEOUT)
    if handle.is_alive():  # pragma: no cover
        raise TimeoutError(
            'Server thread did not gracefully exit within '
            f'{TEST_CONNECTION_TIMEOUT} seconds.',
        )


@pytest.mark.asyncio
async def test_async_queue() -> None:
    queue: _AsyncQueue[str] = _AsyncQueue()

    message = 'foo'
    await queue.put(message)
    received = await queue.get()
    assert message == received

    await queue.close()
    await queue.close()  # Idempotent check

    assert queue.closed()
    with pytest.raises(MailboxClosedError):
        await queue.put(message)
    with pytest.raises(MailboxClosedError):
        await queue.get()


@pytest.mark.asyncio
async def test_async_queue_subscribe() -> None:
    queue: _AsyncQueue[int] = _AsyncQueue()

    await queue.put(1)
    await queue.put(2)
    await queue.put(3)
    await queue.close(immediate=False)

    messages = [m async for m in queue.subscribe()]
    assert set(messages) == {1, 2, 3}


@pytest.mark.asyncio
async def test_mailbox_manager_registration() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()

    manager.register(uid)
    manager.register(uid)  # Idempotent check

    await manager.unregister(uid)
    await manager.unregister(uid)  # Idempotent check


@pytest.mark.asyncio
async def test_mailbox_manager_subscribe() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    messages = [
        ForwardMessage(src=uid, dest=uid, message='1'),
        ForwardMessage(src=uid, dest=uid, message='2'),
        ForwardMessage(src=uid, dest=uid, message='3'),
    ]

    manager.register(uid)
    for message in messages:
        await manager.send(message)

    received: list[ForwardMessage] = []
    async for message in await manager.subscribe(uid):  # pragma: no branch
        received.append(message)
        if len(received) == len(messages):
            break

    assert received == messages
    await manager.unregister(uid)


@pytest.mark.asyncio
async def test_mailbox_manager_bad_identifier() -> None:
    manager = _MailboxManager()
    uid = AgentIdentifier.new()
    message = ForwardMessage(src=uid, dest=uid, message='foo')

    with pytest.raises(BadIdentifierError):
        await manager.send(message)

    with pytest.raises(BadIdentifierError):
        await manager.subscribe(uid)


def test_mailbox_serve() -> None:
    with mock.patch('aeris.exchange.simple._serve_forever'):
        assert _main(['--port', '0']) == 0


@pytest.mark.asyncio
async def test_mailbox_server_serve_forever() -> None:
    server = SimpleServer('localhost', open_port())
    assert isinstance(repr(server), str)
    assert isinstance(str(server), str)

    stop = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(_serve_forever(server, stop))
    await asyncio.sleep(0.01)
    stop.set_result(None)
    await task
    task.result()


@pytest.mark.asyncio
async def test_client_serialize(server_thread: tuple[str, int]) -> None:
    host, port = server_thread
    with SimpleExchange(host, port) as exchange1:
        pickled = pickle.dumps(exchange1)
        with pickle.loads(pickled) as exchange2:
            assert repr(exchange1) == repr(exchange2)
            assert str(exchange1) == str(exchange2)


@pytest.mark.asyncio
async def test_client_register(server_thread: tuple[str, int]) -> None:
    host, port = server_thread
    with SimpleExchange(host, port) as exchange:
        aid = exchange.register_agent()
        cid = exchange.register_client()
        exchange.unregister(aid)
        exchange.unregister(aid)  # Idempotent check
        exchange.unregister(cid)


@pytest.mark.asyncio
async def test_mailbox_errors(server_thread: tuple[str, int]) -> None:
    host, port = server_thread
    with SimpleExchange(host, port) as exchange:
        aid = exchange.register_agent()

        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(AgentIdentifier.new())

        mailbox = exchange.get_mailbox(aid)
        mailbox.close()

        message = PingRequest(src=aid, dest=AgentIdentifier.new())

        with pytest.raises(MailboxClosedError):
            mailbox._push(message)

        with pytest.raises(MailboxClosedError):
            mailbox.send(message)

        with pytest.raises(MailboxClosedError):
            mailbox.recv()

        mailbox.close()


@pytest.mark.asyncio
async def test_client_send_messages(server_thread: tuple[str, int]) -> None:
    host, port = server_thread
    with SimpleExchange(host, port) as exchange:
        assert isinstance(exchange, Exchange)
        aid1 = exchange.register_agent()
        aid2 = exchange.register_agent()

        with (
            exchange.get_mailbox(aid1) as mailbox1,
            exchange.get_mailbox(aid2) as mailbox2,
        ):
            assert isinstance(mailbox1, Mailbox)
            message = PingRequest(src=aid1, dest=aid2)
            mailbox1.send(message)
            assert mailbox2.recv() == message


@pytest.mark.asyncio
async def test_create_handle(server_thread: tuple[str, int]) -> None:
    host, port = server_thread
    with SimpleExchange(host, port) as exchange:
        cid = ClientIdentifier.new()
        with pytest.raises(TypeError):
            exchange.create_handle(cid)  # type: ignore[arg-type]

        aid = exchange.register_agent()
        handle = exchange.create_handle(aid)
        handle.close()
