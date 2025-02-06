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
    mailbox bound.

    An unbound handle does not have a mailbox for itself which means it
    cannot send messages to the remote agent because the handle does not
    know the return address to use. Thus, unbound handles need to be bound
    with `bind_as_client` or `bind_to_mailbox` before they can be used.

    A client bound handle has a unique handle identifier and mailbox for
    itself. A mailbox bound handle shares its identifier and mailbox with
    another entity (e.g., a running agent).

    Note:
        When an instance is pickled and unpickled, such as when
        communicated along with an agent dispatched to run in another
        process, the handle will be unpickled into an `UnboundRemoteHandle`.
        Thus, the handle will need to be bound before use.

    Args:
        exchange: Message exchange used for agent communication.
        agent_id: Identifier of the target agent of this handle.
        mailbox_id: Identifier of the mailbox this handle receives messages to.
            If unbound, this is `None`.
    """

    def __init__(
        self,
        exchange: Exchange,
        agent_id: AgentIdentifier,
        mailbox_id: Identifier | None = None,
    ) -> None:
        self.exchange = exchange
        self.agent_id = agent_id
        self.mailbox_id = mailbox_id
        # Unique identifier for each handle object; used to disambiguate
        # messages when multiple handles are bound to the same mailbox.
        self.handle_id = uuid.uuid4()

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
        return (UnboundRemoteHandle, (self.exchange, self.agent_id))

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(agent_id={self.agent_id!r}, '
            f'mailbox_id={self.mailbox_id!r}, exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<agent: {self.agent_id}; mailbox: {self.mailbox_id}>'

    def _process_response(self, response: ResponseMessage) -> None:
        if isinstance(response, (ActionResponse, PingResponse)):
            future = self._futures.pop(response.tag)
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
        client_id: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Note:
            This is an abstract method. Each remote handle variant implements
            different semantics.

        Args:
            client_id: Client identifier to be used by this handle. If `None`,
                a new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        ...

    @abc.abstractmethod
    def bind_to_mailbox(
        self,
        mailbox_id: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            mailbox_id: Identifier of the mailbox to bind to.

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
        if self.mailbox_id is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.agent_id, self.mailbox_id)

        request = ActionRequest(
            src=self.mailbox_id,
            dest=self.agent_id,
            label=self.handle_id,
            action=action,
            args=args,
            kwargs=kwargs,
        )
        future: Future[R] = Future()
        self._futures[request.tag] = future
        self._send_request(request)
        logger.debug(
            'Sent action request from %s to %s (action=%r)',
            self.mailbox_id,
            self.agent_id,
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
        if self.mailbox_id is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.agent_id, self.mailbox_id)

        start = time.perf_counter()
        request = PingRequest(
            src=self.mailbox_id,
            dest=self.agent_id,
            label=self.handle_id,
        )
        future: Future[None] = Future()
        self._futures[request.tag] = future
        self._send_request(request)
        logger.debug('Sent ping from %s to %s', self.mailbox_id, self.agent_id)
        future.result(timeout=timeout)
        elapsed = time.perf_counter() - start
        logger.debug(
            'Received ping from %s to %s in %.3f ms',
            self.mailbox_id,
            self.agent_id,
            elapsed / 1000,
        )
        return elapsed

    def shutdown(self) -> None:
        """Instruct the agent to shutdown.

        This is non-blocking and will only send the message.

        Raises:
            HandleClosedError: if the handle was closed.
        """
        if self.mailbox_id is None:
            # UnboundRemoteHandle overrides these methods and is the only
            # handle state variant where hid is None.
            raise AssertionError(
                'Method should not be reachable in unbound state.',
            )
        if self._closed:
            raise HandleClosedError(self.agent_id, self.mailbox_id)

        request = ShutdownRequest(
            src=self.mailbox_id,
            dest=self.agent_id,
            label=self.handle_id,
        )
        self._send_request(request)
        logger.debug(
            'Sent shutdown request from %s to %s',
            self.mailbox_id,
            self.agent_id,
        )


class UnboundRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent that is unbound.

    Warning:
        An unbound handle must be bound before use. Otherwise all methods
        will raise an `HandleNotBoundError` when attempting to send a message
        to the remote agent.

    Args:
        exchange: Message exchange used for agent communication.
        agent_id: Identifier of the agent.
    """

    def __init__(self, exchange: Exchange, agent_id: AgentIdentifier) -> None:
        super().__init__(exchange, agent_id=agent_id)

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f'{name}(agent_id={self.agent_id!r}, exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<agent: {self.agent_id}>'

    def _send_request(self, request: RequestMessage) -> None:
        raise HandleNotBoundError(self.agent_id)

    def bind_as_client(
        self,
        client_id: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            client_id: Client identifier to be used by this handle. If `None`,
                a new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        return ClientRemoteHandle(self.exchange, self.agent_id, client_id)

    def bind_to_mailbox(
        self,
        mailbox_id: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            mailbox_id: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        return BoundRemoteHandle(self.exchange, self.agent_id, mailbox_id)

    def action(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[R]:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)

    def ping(self, *, timeout: float | None = None) -> float:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)

    def shutdown(self) -> None:
        """Raises [`HandleNotBoundError`][aeris.exception.HandleNotBoundError]."""  # noqa: E501
        raise HandleNotBoundError(self.agent_id)


class BoundRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent bound to an existing mailbox.

    Args:
        exchange: Message exchange used for agent communication.
        agent_id: Identifier of the target agent of this handle.
        mailbox_id: Identifier of the mailbox this handle receives messages to.
    """

    def __init__(
        self,
        exchange: Exchange,
        agent_id: AgentIdentifier,
        mailbox_id: Identifier,
    ) -> None:
        if agent_id == mailbox_id:
            raise ValueError(
                f'Cannot create handle to {agent_id} that is bound to itself. '
                'Check that the values of `agent_id` and `mailbox_id` '
                'are different.',
            )
        super().__init__(exchange, agent_id, mailbox_id)

    def bind_as_client(
        self,
        client_id: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            client_id: Client identifier to be used by this handle. If `None`,
                a new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        return ClientRemoteHandle(self.exchange, self.agent_id, client_id)

    def bind_to_mailbox(
        self,
        mailbox_id: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            mailbox_id: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        if mailbox_id == self.mailbox_id:
            return self
        else:
            return BoundRemoteHandle(self.exchange, self.agent_id, mailbox_id)

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
        logger.debug('Closed handle (%s)', self)


class ClientRemoteHandle(RemoteHandle[BehaviorT_co]):
    """Handle to a remote agent bound as a unique client.

    Args:
        exchange: Message exchange used for agent communication.
        agent_id: Identifier of the target agent of this handle.
        client_id: Client identifier of this handle. If `None`, a new
            identifier will be created using the exchange. Note this will
            become the `mailbox_id` attribute of the handle.
    """

    def __init__(
        self,
        exchange: Exchange,
        agent_id: AgentIdentifier,
        client_id: ClientIdentifier | None = None,
    ) -> None:
        if client_id is None:
            client_id = exchange.create_client()
        super().__init__(exchange, agent_id, client_id)
        self._recv_thread = threading.Thread(
            target=self._recv_responses,
            name=f'{self}-message-handler',
        )
        self._recv_thread.start()

    def _recv_responses(self) -> None:
        logger.debug('Started result listener thread for %s', self)
        assert self.mailbox_id is not None

        while True:
            try:
                message = self.exchange.recv(self.mailbox_id)
            except MailboxClosedError:
                break

            if isinstance(message, get_args(ResponseMessage)):
                self._process_response(message)
            else:
                logger.error(
                    'Received invalid message response type %s from %s',
                    type(message).__name__,
                    self.agent_id,
                )

        logger.debug('Exiting result listener thread for %s', self)

    def bind_as_client(
        self,
        client_id: ClientIdentifier | None = None,
    ) -> ClientRemoteHandle[BehaviorT_co]:
        """Bind the handle as a unique client in the exchange.

        Args:
            client_id: Client identifier to be used by this handle. If `None`
                or equal to this handle's ID, self will be returned. Otherwise,
                a new identifier will be created using the exchange.

        Returns:
            Remote handle bound to the client identifier.
        """
        if client_id is None or client_id == self.mailbox_id:
            return self
        else:
            return ClientRemoteHandle(self.exchange, self.agent_id, client_id)

    def bind_to_mailbox(
        self,
        mailbox_id: Identifier,
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind the handle to an existing mailbox.

        Args:
            mailbox_id: Identifier of the mailbox to bind to.

        Returns:
            Remote handle bound to the identifier.
        """
        return BoundRemoteHandle(self.exchange, self.agent_id, mailbox_id)

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

        assert isinstance(self.mailbox_id, ClientIdentifier)
        if not self._recv_thread.is_alive():
            raise RuntimeError(
                f'Result message listener for {self.mailbox_id} is not alive. '
                'This likely means the listener thread crashed.',
            )

        self.exchange.close_mailbox(self.mailbox_id)
        self._recv_thread.join()

        logger.debug('Closed handle (%s)', self)
