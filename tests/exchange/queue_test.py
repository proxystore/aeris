from __future__ import annotations

import pytest

from aeris.exchange.queue import AsyncQueue
from aeris.exchange.queue import Queue
from aeris.exchange.queue import QueueClosedError


@pytest.mark.asyncio
async def test_async_queue() -> None:
    queue: AsyncQueue[str] = AsyncQueue()

    message = 'foo'
    await queue.put(message)
    received = await queue.get()
    assert message == received

    await queue.close()
    await queue.close()  # Idempotent check

    assert queue.closed()
    with pytest.raises(QueueClosedError):
        await queue.put(message)
    with pytest.raises(QueueClosedError):
        await queue.get()


@pytest.mark.asyncio
async def test_async_queue_subscribe() -> None:
    queue: AsyncQueue[int] = AsyncQueue()

    await queue.put(1)
    await queue.put(2)
    await queue.put(3)
    await queue.close(immediate=False)

    messages = [m async for m in queue.subscribe()]
    assert set(messages) == {1, 2, 3}


def test_queue() -> None:
    queue: Queue[str] = Queue()

    message = 'foo'
    queue.put(message)
    received = queue.get()
    assert message == received

    queue.close()
    queue.close()  # Idempotent check

    queue.closed()
    with pytest.raises(QueueClosedError):
        queue.put(message)
    with pytest.raises(QueueClosedError):
        queue.get()
