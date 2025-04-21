from __future__ import annotations

import pytest

from academy.exchange.queue import AsyncQueue
from academy.exchange.queue import Queue
from academy.exchange.queue import QueueClosedError


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


def test_queue() -> None:
    queue: Queue[str] = Queue()

    message = 'foo'
    queue.put(message)
    received = queue.get()
    assert message == received

    with pytest.raises(TimeoutError):
        queue.get(timeout=0.001)

    queue.close()
    queue.close()  # Idempotent check

    queue.closed()
    with pytest.raises(QueueClosedError):
        queue.put(message)
    with pytest.raises(QueueClosedError):
        queue.get()
    with pytest.raises(QueueClosedError):
        queue.get()
