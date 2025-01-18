from __future__ import annotations

import functools
import logging
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from concurrent.futures import wait
from types import TracebackType
from typing import Any
from typing import Callable
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import Concatenate
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import Concatenate
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.exception import HandleClosedError
from aeris.exception import MailboxClosedError
from aeris.identifier import AgentIdentifier
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.message import ShutdownRequest

if TYPE_CHECKING:
    from aeris.exchange import Exchange

logger = logging.getLogger(__name__)

P = ParamSpec('P')
R = TypeVar('R')


def _validate_state(
    method: Callable[Concatenate[Handle, P], R],
) -> Callable[Concatenate[Handle, P], R]:
    @functools.wraps(method)
    def _wrapper(self: Handle, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._closed:
            raise HandleClosedError(self.aid, self.cid)
        return method(self, *args, **kwargs)

    return _wrapper


class Handle:
    """Client handle to a running agent.

    A handle enables a client to invoke actions on an agent.

    Note:
        When a `Handle` instance is pickled and unpickled, such as when
        communicated along with an agent dispatched to run in another
        process, the `Handle` will register itself as a new client with the
        exchange. In other words, every `Handle` instance is a unique client
        of the exchange.

    Args:
        aid: Identifier of the agent.
        exchange: Message exchange used to communicate with agent.
    """

    def __init__(self, aid: AgentIdentifier, exchange: Exchange) -> None:
        self.aid = aid
        self.cid = exchange.create_client()

        self.exchange = exchange

        self._futures: dict[uuid.UUID, Future[Any]] = {}
        self._listener_thread = threading.Thread(target=self._result_listener)
        self._listener_thread.start()
        self._closed = False

        logger.info('Initialized handle to %s with %s', self.aid, self.cid)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __getnewargs_ex__(
        self,
    ) -> tuple[
        tuple[AgentIdentifier, Exchange],
        dict[str, Any],
    ]:
        return ((self.aid, self.exchange), {})

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f'{name}(aid={self.aid!r}, cid={self.cid!r}, '
            f'exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<{self.aid}; {self.cid}>'

    def _result_listener(self) -> None:
        logger.debug('Started result listener thread for %s', self.cid)
        while True:
            try:
                message = self.exchange.recv(self.cid)
            except MailboxClosedError:
                break

            if isinstance(message, ActionResponse):
                future = self._futures.pop(message.mid)
                if message.exception is not None:
                    future.set_exception(message.exception)
                else:
                    future.set_result(message.result)
            elif isinstance(message, PingResponse):
                future = self._futures.pop(message.mid)
                future.set_result(None)
            else:
                logger.error(
                    'Received invalid message response type %s from %s',
                    type(message).__name__,
                    self.aid,
                )
        logger.debug('Exiting result listener thread for %s', self.cid)

    def close(
        self,
        wait_futures: bool = True,
        *,
        timeout: float | None = None,
    ) -> None:
        """Close this handle.

        Args:
            wait_futures: Wait to return until all pending futures are done
                executing. If `False`, pending futures are cancelled.
            timeout: Optional timeout used when `wait=True`.

        Raises:
            RuntimeError: if the result message listener thread is not alive
                when `close()` is called indicating the listener thread likely
                crashed.
        """
        self._closed = True

        if wait_futures:
            logger.debug('Waiting on pending futures for %s', self.cid)
            wait(list(self._futures.values()), timeout=timeout)
        else:
            logger.debug('Cancelling pending futures for %s', self.cid)
            for future in self._futures:
                self._futures[future].cancel()

        if not self._listener_thread.is_alive():
            raise RuntimeError(
                f'Result message listener for {self.cid} is not alive. '
                'This likely means the listener thread crashed.',
            )

        self.exchange.close_mailbox(self.cid)
        self._listener_thread.join()

        logger.info('Closed handle with %s', self.cid)

    @_validate_state
    def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[R]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.
        """
        request = ActionRequest(
            src=self.cid,
            dest=self.aid,
            action=action,
            args=args,
            kwargs=kwargs,
        )
        future: Future[R] = Future()
        self._futures[request.mid] = future
        self.exchange.send(self.aid, request)
        logger.debug(
            'Sent action request from %s to %s (action=%r)',
            self.cid,
            self.aid,
            action,
        )
        return future

    @_validate_state
    def ping(self, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Args:
            timeout: Optional timeout in seconds to wait for the response.

        Returns:
            Round-trip time in seconds.

        Raises:
            TimeoutError: if the timeout is exceeded.
        """
        start = time.perf_counter()
        request = PingRequest(src=self.cid, dest=self.aid)
        future: Future[None] = Future()
        self._futures[request.mid] = future
        self.exchange.send(self.aid, request)
        logger.debug('Sent ping from %s to %s', self.cid, self.aid)
        future.result(timeout=timeout)
        elapsed = time.perf_counter() - start
        logger.debug(
            'Received ping from %s to %s in %.3f ms',
            self.cid,
            self.aid,
            elapsed / 1000,
        )
        return elapsed

    @_validate_state
    def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.
        """
        request = ShutdownRequest(src=self.cid, dest=self.aid)
        self.exchange.send(self.aid, request)
        logger.debug('Sent shutdown request from %s to %s', self.cid, self.aid)
