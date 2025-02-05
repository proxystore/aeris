from __future__ import annotations

import abc
import logging
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from concurrent.futures import wait
from types import TracebackType
from typing import Any
from typing import Generic
from typing import get_args
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.exception import HandleClosedError
from aeris.exception import HandleNotBoundError
from aeris.exception import MailboxClosedError
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.message import RequestMessage
from aeris.message import ResponseMessage
from aeris.message import ShutdownRequest
from aeris.message import ShutdownResponse

if TYPE_CHECKING:
    from aeris.exchange import Exchange

logger = logging.getLogger(__name__)

P = ParamSpec('P')
R = TypeVar('R')
BehaviorT_co = TypeVar('BehaviorT_co', bound=Behavior, covariant=True)


@runtime_checkable
class Handle(Protocol[BehaviorT_co]):
    """Agent handle protocol.

    A handle enables a client or agent to invoke actions on another agent.
    """

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
        ...


class ProxyHandle(Generic[BehaviorT_co]):
    """Proxy handle.

    A proxy handle is thin wrapper around a
    [`Behavior`][aeris.behavior.Behavior] instance that is useful for testing
    behaviors that are initialized with a handle to another agent without
    needing to spawn agents. This wrapper invokes actions synchronously.
    """

    def __init__(self, behavior: BehaviorT_co) -> None:
        self.behavior = behavior

    def __repr__(self) -> str:
        return f'{type(self).__name__}(behavior={self.behavior!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.behavior}>'

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
        future: Future[R] = Future()
        try:
            method = getattr(self.behavior, action)
            result = method(*args, **kwargs)
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(result)
        return future


class RemoteHandle(Generic[BehaviorT_co], abc.ABC):
    """Handle to a remote agent.

    This is an abstract base class with three possible concrete types
    representing the three remote handle states: unbound, client bound, or
    agent bound.

    An unbound handle does not have an identifier for itself which means it
    cannot send messages to the remote agent because the handle does not
    know the return address to use. Thus, unbound handles need to be bound
    with `bind_as_client` or `bind_to_agent` before they can be used.

    A client bound handle has a unique handle identifier and mailbox for
    itself. An agent bound handle shares the identifier and mailbox of the
    running agent it is bound to (this is a different agent to the target
    agent of the handle).

    Note:
        When an instance is pickled and unpickled, such as when
        communicated along with an agent dispatched to run in another
        process, the handle will be unpickled into an `UnboundRemoteHandle`.
        Thus, the handle will need to be bound before use.

    Args:
        exchange: Message exchange used for agent communication.
        aid: Identifier of the target agent of this handle.
        hid: Identifier of this handle. If unbound, this is `None`. If bound
            as a client, this is a `ClientIdentifier`. If bound to a running
            agent, this is the ID of that agent.
    """

    def __init__(
        self,
        exchange: Exchange,
        aid: AgentIdentifier,
        hid: Identifier | None = None,
    ) -> None:
        self.exchange = exchange
        self.aid = aid
        self.hid = hid

        self._futures: dict[uuid.UUID, Future[Any]] = {}
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __reduce__(
        self,
    ) -> tuple[
        type[UnboundRemoteHandle[Any]],
        tuple[Exchange, AgentIdentifier],
    ]:
        return (UnboundRemoteHandle, (self.exchange, self.aid))

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(aid={self.aid!r}, hid={self.hid!r}, '
            f'exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<{self.aid}; {self.hid}>'

    def _process_response(self, response: ResponseMessage) -> None:
        if isinstance(response, (ActionResponse, PingResponse)):
            future = self._futures.pop(response.mid)
            if response.exception is not None:
                future.set_exception(response.exception)
            elif isinstance(response, ActionResponse):
                future.set_result(response.result)
            elif isinstance(response, PingResponse):
                future.set_result(None)
            else:
                raise AssertionError('Unreachable.')
        elif isinstance(response, ShutdownResponse):  # pragma: no cover
            # Shutdown responses are not implemented yet.
            pass
        else:
            raise AssertionError('Unreachable.')

    def _send_request(self, request: RequestMessage) -> None:
        self.exchange.send(request.dest, request)

    @abc.abstractmethod
    def bind_as_client(
        self,
        cid: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Note:
            This is an abstract method. Each remote handle variant implements
            different semantics.

        Args:
            cid: Client identifier to be used by this handle. If `None`, a
                new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        ...

    @abc.abstractmethod
    def bind_to_mailbox(
        self,
        uid: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            uid: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        ...

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
        """
        self._closed = True

        if len(self._futures) == 0:
            return
        if wait_futures:
            logger.debug('Waiting on pending futures for %s', self)
            wait(list(self._futures.values()), timeout=timeout)
        else:
            logger.debug('Cancelling pending futures for %s', self)
            for future in self._futures:
                self._futures[future].cancel()

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

        Raises:
            HandleClosedError: if the handle was closed.
        """
        if self.hid is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.aid, self.hid)

        request = ActionRequest(
            src=self.hid,
            dest=self.aid,
            action=action,
            args=args,
            kwargs=kwargs,
        )
        future: Future[R] = Future()
        self._futures[request.mid] = future
        self._send_request(request)
        logger.debug(
            'Sent action request from %s to %s (action=%r)',
            self.hid,
            self.aid,
            action,
        )
        return future

    def ping(self, *, timeout: float | None = None) -> float:
        """Ping the agent.

        Ping the agent and wait to get a response. Agents process messages
        in order so the round-trip time will include processing time of
        earlier messages in the queue.

        Args:
            timeout: Optional timeout in seconds to wait for the response.

        Returns:
            Round-trip time in seconds.

        Raises:
            HandleClosedError: if the handle was closed.
            TimeoutError: if the timeout is exceeded.
        """
        if self.hid is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.aid, self.hid)

        start = time.perf_counter()
        request = PingRequest(src=self.hid, dest=self.aid)
        future: Future[None] = Future()
        self._futures[request.mid] = future
        self._send_request(request)
        logger.debug('Sent ping from %s to %s', self.hid, self.aid)
        future.result(timeout=timeout)
        elapsed = time.perf_counter() - start
        logger.debug(
            'Received ping from %s to %s in %.3f ms',
            self.hid,
            self.aid,
            elapsed / 1000,
        )
        return elapsed

    def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Raises:
            HandleClosedError: if the handle was closed.
        """
        if self.hid is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.aid, self.hid)

        request = ShutdownRequest(src=self.hid, dest=self.aid)
        self._send_request(request)
        logger.debug('Sent shutdown request from %s to %s', self.hid, self.aid)


class UnboundRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent that is unbound.

    Warning:
        An unbound handle must be bound before use. Otherwise all methods
        will raise an `HandleNotBoundError` when attempting to send a message
        to the remote agent.

    Args:
        aid: Identifier of the agent.
        exchange: Message exchange used for agent communication.
    """

    def __init__(self, exchange: Exchange, aid: AgentIdentifier) -> None:
        super().__init__(exchange, aid)

    def __repr__(self) -> str:
        name = type(self).__name__
        return f'{name}(aid={self.aid!r}, exchange={self.exchange!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.aid}>'

    def _send_request(self, request: RequestMessage) -> None:
        raise HandleNotBoundError(self.aid)

    def bind_as_client(
        self,
        cid: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            cid: Client identifier to be used by this handle. If `None`, a
                new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        return ClientRemoteHandle(self.exchange, self.aid, cid)

    def bind_to_mailbox(
        self,
        uid: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            uid: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        return BoundRemoteHandle(self.exchange, self.aid, uid)

    def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[R]:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.aid)

    def ping(self, *, timeout: float | None = None) -> float:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.aid)

    def shutdown(self) -> None:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.aid)


class BoundRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent bound to a running agent.

    Note:
        When a handle is bound to a running agent, the running agent and the
        handle share a mailbox. Thus, the running agent is responsible for
        forwarding response messages it receives back to the appropriate
        bound handle.

    Args:
        exchange: Message exchange used for agent communication.
        aid: Identifier of the target agent of this handle.
        hid: Identifier of the mailbox that this handle is bound to (e.g.,
            the mailbox of a running agent that holds this handle).
    """

    def __init__(
        self,
        exchange: Exchange,
        aid: AgentIdentifier,
        hid: Identifier,
    ) -> None:
        if aid == hid:
            raise ValueError(
                f'Cannot create handle to {aid} that is bound to itself. '
                'Check that the values of `aid` and `hid` are different.',
            )
        super().__init__(exchange, aid)
        self.hid = hid

    def bind_as_client(
        self,
        cid: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            cid: Client identifier to be used by this handle. If `None`, a
                new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        return ClientRemoteHandle(self.exchange, self.aid, cid)

    def bind_to_mailbox(
        self,
        uid: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            uid: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        if uid == self.hid:
            return self
        else:
            return BoundRemoteHandle(self.exchange, self.aid, uid)

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
        """
        super().close(wait_futures, timeout=timeout)
        logger.info('Closed handle to %s with %s', self.aid, self.hid)


class ClientRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent bound as a unique client.

    Args:
        exchange: Message exchange used for agent communication.
        aid: Identifier of the target agent of this handle.
        hid: Client identifier of this handle. If `None`, a new identifier
            will be created using the exchange.
    """

    def __init__(
        self,
        exchange: Exchange,
        aid: AgentIdentifier,
        hid: ClientIdentifier | None = None,
    ) -> None:
        super().__init__(exchange, aid)

        if hid is None:
            hid = self.exchange.create_client()
        self.hid = hid
        self._recv_thread = threading.Thread(
            target=self._recv_responses,
            name=f'{self}-message-handler',
        )
        self._recv_thread.start()

    def _recv_responses(self) -> None:
        logger.debug('Started result listener thread for %s', self.hid)
        assert self.hid is not None

        while True:
            try:
                message = self.exchange.recv(self.hid)
            except MailboxClosedError:
                break

            if isinstance(message, get_args(ResponseMessage)):
                self._process_response(message)
            else:
                logger.error(
                    'Received invalid message response type %s from %s',
                    type(message).__name__,
                    self.aid,
                )

        logger.debug('Exiting result listener thread for %s', self.hid)

    def bind_as_client(
        self,
        cid: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            cid: Client identifier to be used by this handle. If `None` or
                equal to this handle's ID, self will be returned. Otherwise, a
                new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        if cid is None or cid == self.hid:
            return self
        else:
            return ClientRemoteHandle(self.exchange, self.aid, cid)

    def bind_to_mailbox(
        self,
        uid: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            uid: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        return BoundRemoteHandle(self.exchange, self.aid, uid)

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
            RuntimeError: if the response message listener thread is not alive
                when `close()` is called indicating the listener thread likely
                crashed.
        """
        super().close(wait_futures, timeout=timeout)

        assert isinstance(self.hid, ClientIdentifier)
        if not self._recv_thread.is_alive():
            raise RuntimeError(
                f'Result message listener for {self.hid} is not alive. '
                'This likely means the listener thread crashed.',
            )

        self.exchange.close_mailbox(self.hid)
        self._recv_thread.join()

        logger.info('Closed handle to %s with %s', self.aid, self.hid)
