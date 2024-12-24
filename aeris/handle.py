from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import Future
from typing import Any
from typing import TYPE_CHECKING
from typing import TypeVar

import aeris
from aeris.identifier import AgentIdentifier
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.message import ShutdownRequest

if TYPE_CHECKING:
    from aeris.exchange import Exchange

T = TypeVar('T')


class Handle:
    """Client handle to a running agent.

    A handle enables a client to invoke actions on an agent.

    Args:
        aid: Identifier of the agent.
        exchange: Message exchange used to communicate with agent.
    """

    def __init__(self, aid: AgentIdentifier, exchange: Exchange) -> None:
        self.aid = aid
        self.exchange = exchange

        agent_mailbox = exchange.get_mailbox(self.aid)
        assert agent_mailbox is not None
        self._agent_mailbox = agent_mailbox

        self._cid = exchange.register_client()
        client_mailbox = exchange.get_mailbox(self._cid)
        assert client_mailbox is not None
        self._client_mailbox = client_mailbox

        self._futures: dict[uuid.UUID, Future[Any]] = {}
        self._listener_thread = threading.Thread(target=self._result_listener)
        self._listener_thread.start()

    def __repr__(self) -> str:
        name = type(self).__name__
        return f'{name}(aid={self.aid!r}, exchange={self.exchange!r})'

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<{self.aid}; {self.exchange}>'

    def _result_listener(self) -> None:
        while True:
            try:
                message = self._client_mailbox.recv()
            except aeris.exchange.MailboxClosedError:
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
                # TODO: log this better?
                raise AssertionError('Unreachable.')

    def close(self) -> None:
        """Close this handle."""
        # TODO: wait or cancel futures?
        self._client_mailbox.close()
        self._listener_thread.join()

    def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.
        """
        # TODO: some of these methods should raise errors if the handle is
        # closed.
        request = ActionRequest(
            src=self._cid,
            dest=self.aid,
            action=action,
            args=args,
            kwargs=kwargs,
        )
        future: Future[T] = Future()
        self._futures[request.mid] = future
        self._agent_mailbox.send(request)
        return future

    def ping(self) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Returns:
            Round-trip time in seconds.
        """
        # TODO: add timeout
        start = time.perf_counter()
        request = PingRequest(src=self._cid, dest=self.aid)
        future: Future[None] = Future()
        self._futures[request.mid] = future
        self._agent_mailbox.send(request)
        future.result()
        end = time.perf_counter()
        return end - start

    def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.
        """
        request = ShutdownRequest(src=self._cid, dest=self.aid)
        self._agent_mailbox.send(request)
