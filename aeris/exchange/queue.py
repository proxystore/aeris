from __future__ import annotations

import asyncio
import dataclasses
import queue
from collections.abc import AsyncGenerator
from typing import cast
from typing import Generic
from typing import TypeVar

T = TypeVar('T')

DEFAULT_PRIORITY = 0
CLOSE_PRIORITY = DEFAULT_PRIORITY + 1
CLOSE_SENTINAL = object()


class QueueClosedError(Exception):
    """Queue has been closed exception."""

    pass


@dataclasses.dataclass(order=True)
class _Item(Generic[T]):
    priority: int
    value: T | object = dataclasses.field(compare=False)


class AsyncQueue(Generic[T]):
    """Simple async queue.

    This is a simple backport of Python 3.13 queues which have a shutdown
    method and exception type.
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[_Item[T]] = asyncio.PriorityQueue()
        self._closed = False

    async def close(self, immediate: bool = False) -> None:
        """Close the queue.

        This will cause `get` and `put` to raise `QueueClosedError`.

        Args:
            immediate: Close the queue immediately, rather than once the
                queue is empty.
        """
        if not self.closed():
            self._closed = True
            priority = CLOSE_PRIORITY if immediate else DEFAULT_PRIORITY
            await self._queue.put(_Item(priority, CLOSE_SENTINAL))

    def closed(self) -> bool:
        """Check if the queue has been closed."""
        return self._closed

    async def get(self) -> T:
        """Remove and return the next item from the queue (blocking)."""
        item = await self._queue.get()
        if item.value is CLOSE_SENTINAL:
            raise QueueClosedError
        return cast(T, item.value)

    async def put(self, item: T) -> None:
        """Put an item on the queue."""
        if self.closed():
            raise QueueClosedError
        await self._queue.put(_Item(DEFAULT_PRIORITY, item))

    async def subscribe(self) -> AsyncGenerator[T]:
        """Create a generator that yields items from the queue."""
        while True:
            try:
                yield await self.get()
            except QueueClosedError:
                return


class Queue(Generic[T]):
    """Simple queue.

    This is a simple backport of Python 3.13 queues which have a shutdown
    method and exception type.
    """

    def __init__(self) -> None:
        self._queue: queue.PriorityQueue[_Item[T]] = queue.PriorityQueue()
        self._closed = False

    def close(self, immediate: bool = False) -> None:
        """Close the queue.

        This will cause `get` and `put` to raise `QueueClosedError`.

        Args:
            immediate: Close the queue immediately, rather than once the
                queue is empty.
        """
        if not self.closed():
            self._closed = True
            priority = CLOSE_PRIORITY if immediate else DEFAULT_PRIORITY
            self._queue.put(_Item(priority, CLOSE_SENTINAL))

    def closed(self) -> bool:
        """Check if the queue has been closed."""
        return self._closed

    def get(self) -> T:
        """Remove and return the next item from the queue (blocking)."""
        item = self._queue.get()
        if item.value is CLOSE_SENTINAL:
            raise QueueClosedError
        return cast(T, item.value)

    def put(self, item: T) -> None:
        """Put an item on the queue."""
        if self.closed():
            raise QueueClosedError
        self._queue.put(_Item(DEFAULT_PRIORITY, item))
