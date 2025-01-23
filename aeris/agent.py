from __future__ import annotations

import enum
import logging
import threading
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import Any
from typing import Generic
from typing import get_args
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.handle import AgentRemoteHandle
from aeris.handle import ClientRemoteHandle
from aeris.handle import ProxyHandle
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import Identifier
from aeris.message import ActionRequest
from aeris.message import Message
from aeris.message import PingRequest
from aeris.message import RequestMessage
from aeris.message import ResponseMessage
from aeris.message import ShutdownRequest

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class _AgentMode(enum.Enum):
    INDIVIDUAL = 'individual'
    SYSTEM = 'system'


class _AgentStatus(enum.Enum):
    INITIALIZED = 'initialized'
    STARTING = 'starting'
    RUNNING = 'running'
    TERMINTATING = 'terminating'
    SHUTDOWN = 'shutdown'


class Agent(Generic[BehaviorT]):
    """Executable agent.

    An agent executes predefined [`Behavior`][aeris.behavior.Behavior]. An
    agent can operate independently or as part of a broader multi-agent
    system.

    Args:
        behavior: Behavior that the agent will exhibit.
        aid: Identifier of this agent in a multi-agent system.
        exchange: Message exchange of multi-agent system.
    """

    def __init__(
        self,
        behavior: BehaviorT,
        *,
        aid: AgentIdentifier | None = None,
        exchange: Exchange | None = None,
    ) -> None:
        self.aid = aid if aid is not None else AgentIdentifier.new()
        self.behavior = behavior
        self.exchange = exchange
        self.done = threading.Event()

        if self.exchange is None:
            self._mode = _AgentMode.INDIVIDUAL
        else:
            self._mode = _AgentMode.SYSTEM

        self._actions = behavior.behavior_actions()
        self._loops = behavior.behavior_loops()

        self._start_loops_lock = threading.Lock()
        self._loop_futures: tuple[Future[None], ...] | None = None
        self._action_pool = ThreadPoolExecutor()
        self._action_futures: dict[ActionRequest, Future[None]] = {}
        # The key in bound_handles is the identifier of the remote agent
        # the handle targets. Each running agent should only have a single
        # handle to any given remote agent.
        self._bound_handles: dict[Identifier, AgentRemoteHandle[Any]] = {}

        self._status = _AgentStatus.INITIALIZED

    def __call__(self) -> None:
        """Alias for [run()][aeris.agent.Agent.run]."""
        self.run()

    def __repr__(self) -> str:
        name = type(self).__name__
        if self._mode == _AgentMode.INDIVIDUAL:
            return f'{name}(aid={self.aid!r}, behavior={self.behavior!r})'
        else:
            return (
                f'{name}(aid={self.aid!r}, behavior={self.behavior!r}, '
                f'exchange={self.exchange!r})'
            )

    def __str__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        return f'{name}<{behavior}; {self.aid}>'

    def action(self, action: str, args: Any, kwargs: Any) -> Any:
        """Invoke an action of the agent.

        Args:
            action: Name of action to invoke.
            args: Tuple of positional arguments.
            kwargs: Dictionary of keyword arguments.

        Returns:
            Result of the action.

        Raises:
            TypeError: if an action with this name is not implemented by
                the behavior of the agent.
        """
        logger.debug('Invoking "%s" action on %s', action, self.aid)
        if action not in self._actions:
            raise TypeError(
                f'Agent[{type(self.behavior).__name__}] does not have an '
                f'action named "{action}".',
            )
        return self._actions[action](*args, **kwargs)

    def _bind_handle(
        self,
        attr: str,
        handle: AgentRemoteHandle[Any] | UnboundRemoteHandle[Any],
    ) -> None:
        if handle.aid in self._bound_handles:
            raise RuntimeError(
                f'{self} already has a handle bound to a remote agent with '
                f'{handle.aid}. The duplicate handle should be removed from '
                'the behavior instance.',
            )
        bound = handle.bind_to_agent(self.aid)
        # Replace the handle attribute on the behavior with a handle bound
        # to this agent.
        setattr(self.behavior, attr, bound)
        self._bound_handles[bound.aid] = bound
        logger.debug(
            'Bound remote handle to %s to running agent with %s',
            handle.aid,
            self.aid,
        )

    def _bind_handles(self) -> None:
        for attr, handle in self.behavior.behavior_handles().items():
            if isinstance(
                handle,
                (ClientRemoteHandle, ProxyHandle),
            ):  # pragma: no cover
                # Ignore proxy handles and already bound client handles.
                pass
            elif isinstance(handle, UnboundRemoteHandle):
                self._bind_handle(attr, handle)
            elif isinstance(handle, AgentRemoteHandle):
                if handle.hid != self.aid:
                    self._bind_handle(attr, handle)
            else:
                raise AssertionError('Unreachable.')

    def _send_response(self, response: ResponseMessage) -> None:
        assert self.exchange is not None
        try:
            self.exchange.send(response.dest, response)
        except (BadIdentifierError, MailboxClosedError):
            logger.warning(
                'Failed to send response from %s to %s. '
                'This likely means the destination mailbox was '
                'removed from the exchange.',
                self.aid,
                response.dest,
            )

    def _execute_action(self, request: ActionRequest) -> None:
        try:
            result = self.action(request.action, request.args, request.kwargs)
        except Exception as e:
            response = request.error(exception=e)
        else:
            response = request.response(result=result)
        self._send_response(response)

    def _response_handler(self, response: ResponseMessage) -> None:
        try:
            handle = self._bound_handles[response.src]
        except KeyError:
            logger.exception(
                'Receieved a response message from %s but no handle to '
                'that agent is bound to this agent.',
                response.src,
            )
        else:
            handle._process_response(response)

    def _request_handler(self, request: RequestMessage) -> None:
        if isinstance(request, ActionRequest):
            future = self._action_pool.submit(self._execute_action, request)
            self._action_futures[request] = future
            future.add_done_callback(
                lambda _: self._action_futures.pop(request),
            )
        elif isinstance(request, PingRequest):
            logger.info('Ping request received by %s', self.aid)
            self._send_response(request.response())
        elif isinstance(request, ShutdownRequest):
            self.shutdown()
        else:
            raise AssertionError('Unreachable.')

    def _message_handler(self, message: Message) -> None:
        if isinstance(message, get_args(ResponseMessage)):
            self._response_handler(message)
        elif isinstance(message, get_args(RequestMessage)):
            self._request_handler(message)
        else:
            raise AssertionError('Unreachable.')

    def _message_listener(self) -> None:
        assert self.exchange is not None
        logger.info('Message listener started for %s', self.aid)

        while True:
            try:
                message = self.exchange.recv(self.aid)
            except MailboxClosedError:
                break
            else:
                self._message_handler(message)

    def run(self) -> None:
        """Run the agent.

        1. Binds all unbound handles to remote agents to this agent.
        1. Calls [`Behavior.setup()`][aeris.behavior.Behavior.setup].
        1. Starts threads for all control loops defined on the agent's
           [`Behavior`][aeris.behavior.Behavior].
        1. Starts a thread for listening to messages from the
           [`Exchange`][aeris.exchange.Exchange] (if provided).
        1. Waits for the threads to exit.
        1. Calls [`Behavior.shutdown()`][aeris.behavior.Behavior.shutdown].

        Raises:
            BadMessageTypeError: if the agent receives a message that is not
                a valid request type.
        """
        self._status = _AgentStatus.STARTING
        self._bind_handles()
        self.behavior.setup()

        futures: list[Future[None]] = []
        with ThreadPoolExecutor(max_workers=len(self._loops) + 1) as pool:
            with self._start_loops_lock:
                if self.exchange is not None:
                    futures.append(pool.submit(self._message_listener))

                for method in self._loops.values():
                    futures.append(pool.submit(method, self.done))

                self._loop_futures = tuple(futures)
                self._status = _AgentStatus.RUNNING

            logger.info('Started agent with %s', self.aid)

            for future in as_completed(futures):
                future.result()

        self.behavior.shutdown()
        self._status = _AgentStatus.SHUTDOWN
        logger.info('Shutdown agent with %s', self.aid)

    def shutdown(self) -> None:
        """Notify control loops to shutdown.

        Sets the shutdown event passed to each control loop method and closes
        the agent's mailbox in the exchange.
        """
        logger.info('Shutdown requested for %s', self.aid)
        self.done.set()
        self._status = _AgentStatus.TERMINTATING

        with self._start_loops_lock:
            self._action_pool.shutdown()
            if self.exchange is not None:
                self.exchange.close_mailbox(self.aid)

    def wait(self, timeout: float | None = None) -> None:
        """Wait for control loops to exit.

        Tip:
            This should typically be called after
            [`shutdown()`][aeris.agent.Agent.shutdown] has been called.

        Args:
            timeout: How long to wait for threads to exit gracefully.
        """
        with self._start_loops_lock:
            if self._loop_futures is None:
                return

            wait(self._loop_futures, timeout=timeout)
