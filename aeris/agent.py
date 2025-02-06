from __future__ import annotations

import enum
import logging
import threading
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Generic
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.handle import BoundRemoteHandle
from aeris.handle import ClientRemoteHandle
from aeris.handle import ProxyHandle
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.message import ActionRequest
from aeris.message import PingRequest
from aeris.message import RequestMessage
from aeris.message import ResponseMessage
from aeris.message import ShutdownRequest
from aeris.multiplex import MailboxMultiplexer

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class _AgentState(enum.Enum):
    INITIALIZED = 'initialized'
    STARTING = 'starting'
    RUNNING = 'running'
    TERMINTATING = 'terminating'
    SHUTDOWN = 'shutdown'


# Helper for Agent.__reduce__ which cannot handle the keyword arguments
# of the Agent constructor.
def _agent_trampoline(
    behavior: BehaviorT,
    agent_id: AgentIdentifier,
    exchange: Exchange,
    close_exchange: bool,
) -> Agent[BehaviorT]:
    return Agent(
        behavior,
        agent_id=agent_id,
        exchange=exchange,
        close_exchange=close_exchange,
    )


class Agent(Generic[BehaviorT]):
    """Executable agent.

    An agent executes predefined [`Behavior`][aeris.behavior.Behavior]. An
    agent can operate independently or as part of a broader multi-agent
    system.

    Note:
        An agent can only be run once. After `shutdown()` is called, later
        operations will raise a `RuntimeError`.

    Args:
        behavior: Behavior that the agent will exhibit.
        agent_id: Identifier of this agent in a multi-agent system.
        exchange: Message exchange of multi-agent system. The agent will close
            the exchange when it finished running.
        close_exchange: Close the `exchange` object when the agent is
            shutdown.
    """

    def __init__(
        self,
        behavior: BehaviorT,
        *,
        agent_id: AgentIdentifier,
        exchange: Exchange,
        close_exchange: bool = False,
    ) -> None:
        self.agent_id = agent_id
        self.behavior = behavior
        self.exchange = exchange
        self.close_exchange = close_exchange

        self._actions = behavior.behavior_actions()
        self._loops = behavior.behavior_loops()

        self._shutdown = threading.Event()
        self._state_lock = threading.Lock()
        self._state = _AgentState.INITIALIZED

        self._action_pool: ThreadPoolExecutor | None = None
        self._action_futures: dict[ActionRequest, Future[None]] = {}
        self._loop_pool: ThreadPoolExecutor | None = None
        self._loop_futures: set[Future[None]] = set()

        self._multiplexer = MailboxMultiplexer(
            self.agent_id,
            self.exchange,
            request_handler=self._request_handler,
        )

    def __call__(self) -> None:
        """Alias for [run()][aeris.agent.Agent.run]."""
        self.run()

    def __repr__(self) -> str:
        name = type(self).__name__
        return (
            f'{name}(agent_id={self.agent_id!r}, behavior={self.behavior!r}, '
            f'exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        return f'{name}<{behavior}; {self.agent_id}>'

    def __reduce__(self) -> Any:
        return (
            _agent_trampoline,
            (self.behavior, self.agent_id, self.exchange, self.close_exchange),
        )

    def _bind_handle(
        self,
        attr: str,
        handle: BoundRemoteHandle[Any] | UnboundRemoteHandle[Any],
    ) -> None:
        bound = self._multiplexer.bind(handle)
        # Replace the handle attribute on the behavior with a handle bound
        # to this agent.
        setattr(self.behavior, attr, bound)
        logger.debug(
            'Bound handle to %s to running agent with %s',
            handle.agent_id,
            self.agent_id,
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
            elif isinstance(handle, BoundRemoteHandle):
                if handle.mailbox_id != self.agent_id:
                    self._bind_handle(attr, handle)
            else:
                raise AssertionError('Unreachable.')

    def _send_response(self, response: ResponseMessage) -> None:
        try:
            self.exchange.send(response.dest, response)
        except (BadIdentifierError, MailboxClosedError):
            logger.warning(
                'Failed to send response from %s to %s. '
                'This likely means the destination mailbox was '
                'removed from the exchange.',
                self.agent_id,
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

    def _request_handler(self, request: RequestMessage) -> None:
        if isinstance(request, ActionRequest):
            # The _request_handler should only be called within the message
            # handler thread which is only started after the _action_pool
            # is initialized.
            assert self._action_pool is not None
            future = self._action_pool.submit(self._execute_action, request)
            self._action_futures[request] = future
            future.add_done_callback(
                lambda _: self._action_futures.pop(request),
            )
        elif isinstance(request, PingRequest):
            logger.info('Ping request received by %s', self.agent_id)
            self._send_response(request.response())
        elif isinstance(request, ShutdownRequest):
            self.signal_shutdown()
        else:
            raise AssertionError('Unreachable.')

    def action(self, action: str, args: Any, kwargs: Any) -> Any:
        """Invoke an action of the agent.

        Args:
            action: Name of action to invoke.
            args: Tuple of positional arguments.
            kwargs: Dictionary of keyword arguments.

        Returns:
            Result of the action.

        Raises:
            AttributeError: if an action with this name is not implemented by
                the behavior of the agent.
        """
        logger.debug('Invoking "%s" action on %s', action, self.agent_id)
        if action not in self._actions:
            raise AttributeError(
                f'Agent[{type(self.behavior).__name__}] does not have an '
                f'action named "{action}".',
            )
        return self._actions[action](*args, **kwargs)

    def run(self) -> None:
        """Run the agent.

        Starts the agent, waits for another thread to call `signal_shutdown()`,
        and then shuts down the agent.

        Raises:
            Exception: Any exceptions raised inside threads.
        """
        try:
            self.start()
            self._shutdown.wait()
        finally:
            self.shutdown()

    def start(self) -> None:
        """Start the agent.

        Note:
            This method is idempotent; it will return if the agent is
            already running. However, it will raise an error if the agent
            is shutdown.

        1. Binds all unbound handles to remote agents to this agent.
        1. Calls [`Behavior.setup()`][aeris.behavior.Behavior.setup].
        1. Starts threads for all control loops defined on the agent's
           [`Behavior`][aeris.behavior.Behavior].
        1. Starts a thread for listening to messages from the
           [`Exchange`][aeris.exchange.Exchange] (if provided).

        Raises:
            RuntimeError: If the agent has been shutdown.
        """
        if self._state is _AgentState.SHUTDOWN:
            raise RuntimeError('Agent has already been shutdown.')
        elif self._state is _AgentState.RUNNING:
            return

        with self._state_lock:
            self._state = _AgentState.STARTING
            self._bind_handles()
            self.behavior.setup()
            self._action_pool = ThreadPoolExecutor()
            self._loop_pool = ThreadPoolExecutor(
                max_workers=len(self._loops) + 1,
            )

            for method in self._loops.values():
                loop_future = self._loop_pool.submit(method, self._shutdown)
                self._loop_futures.add(loop_future)

            listener_future = self._loop_pool.submit(self._multiplexer.listen)
            self._loop_futures.add(listener_future)

            self._state = _AgentState.RUNNING

        logger.info('Running agent (%s; %s)', self.agent_id, self.behavior)

    def shutdown(self) -> None:
        """Shutdown the agent.

        Note:
            This method is idempotent.

        1. Sets the shutdown [`Event`][threading.Event] passed to all control
           loops.
        1. Waits for any currently executing actions to complete.
        1. Closes the agent's mailbox indicating that no further messages
           will be processed.
        1. Waits for the control loop and message listener threads to exit.
        1. Optionally closes the exchange.
        1. Calls [`Behavior.shutdown()`][aeris.behavior.Behavior.shutdown].

        Raises:
            Exception: Any exceptions raised inside threads.
        """
        if self._state is _AgentState.SHUTDOWN:
            return

        logger.debug('Shutting down agent... (%s)', self.agent_id)
        with self._state_lock:
            self._state = _AgentState.TERMINTATING
            self._shutdown.set()

            # Wait for currently running actions to complete.
            if self._action_pool is not None:
                self._action_pool.shutdown(wait=True, cancel_futures=True)

            # Cause the multiplexer message listener thread to exit by closing
            # the mailbox the multiplexer is listening to.
            self._multiplexer.close_mailbox()

            # Wait on all the loops, raising any exceptions.
            for future in self._loop_futures:
                future.result()
            if self._loop_pool is not None:
                self._loop_pool.shutdown()

            if self.close_exchange:
                self.exchange.close()

            self.behavior.shutdown()
            self._state = _AgentState.SHUTDOWN

        logger.info('Shutdown agent (%s)', self.agent_id)

    def signal_shutdown(self) -> None:
        """Signal that the agent should exit.

        If the agent has not started, this will cause the agent to immediately
        shutdown when next started. If the agent is shutdown, this has no
        effect.
        """
        self._shutdown.set()
