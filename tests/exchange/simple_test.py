from __future__ import annotations

import asyncio
import multiprocessing
import socket
import time
from collections.abc import AsyncGenerator
from collections.abc import Generator
from unittest import mock

import pytest
import pytest_asyncio

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.exchange import Mailbox
from aeris.exchange.message import ForwardMessage
from aeris.exchange.simple import _AsyncQueue
from aeris.exchange.simple import _MailboxManager
from aeris.exchange.simple import _main
from aeris.exchange.simple import SimpleExchange
from aeris.exchange.simple import SimpleServer
from aeris.identifier import AgentIdentifier
from aeris.message import PingRequest
from testing.sys import open_port


@pytest_asyncio.fixture
async def server() -> AsyncGenerator[SimpleServer]:
    server = SimpleServer('localhost', open_port())
    aserver = await asyncio.start_server(
        server._handle,
        host=server.host,
        port=server.port,
    )

    async with aserver:
        await aserver.start_serving()
        while not aserver.is_serving():
            await asyncio.sleep(0.001)
        yield server


@pytest.fixture
def server_process() -> Generator[tuple[str, int]]:
    context = multiprocessing.get_context('spawn')
    host, port = 'localhost', open_port()
    args = ['--host', 'localhost', '--port', str(port), '--log-level', 'DEBUG']
    handle = context.Process(target=_main, args=(args,))
    handle.start()

    # Wait for server to be listening
    timeout = 5
    waited = 0.0
    wait = 0.01
    while True:
        try:
            with socket.create_connection((host, port)):
                break
        except OSError as e:
            if waited > timeout:
                raise TimeoutError from e
            time.sleep(wait)
            waited += wait

    yield host, port

    handle.terminate()
    handle.join(timeout=5)


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
    async for message in await manager.subscribe(uid):
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
    with mock.patch(
        'aeris.exchange.simple.SimpleServer.serve_forever',
    ):
        assert _main(['--port', '0']) == 0


@pytest.mark.asyncio
async def test_mailbox_server_serve_forever() -> None:
    server = SimpleServer('localhost', open_port())
    stop = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(server.serve_forever(stop))
    await asyncio.sleep(0.01)
    stop.set_result(None)
    await task
    task.result()


@pytest.mark.asyncio
async def test_client_register(server: SimpleServer) -> None:
    with SimpleExchange(server.host, server.port) as exchange:
        aid = exchange.register_agent()
        cid = exchange.register_client()
        exchange.unregister(aid)
        exchange.unregister(cid)


@pytest.mark.asyncio
async def test_client_send_messages(server_process) -> None:
    host, port = server_process
    with SimpleExchange(host, port) as exchange:
        assert isinstance(exchange, Exchange)
        aid1 = exchange.register_agent()
        aid2 = exchange.register_agent()
        mailbox1 = exchange.get_mailbox(aid1)
        assert isinstance(mailbox1, Mailbox)
        mailbox2 = exchange.get_mailbox(aid2)
        message = PingRequest(src=aid1, dest=aid2)
        mailbox1.send(message)
        assert mailbox2.recv() == message
        mailbox1.close()
        mailbox2.close()
