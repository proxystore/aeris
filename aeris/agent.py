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
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.behavior import get_actions
from aeris.behavior import get_loops
from aeris.exception import BadMessageTypeError
from aeris.exchange import Exchange
from aeris.exchange import Mailbox
from aeris.exchange import MailboxClosedError
from aeris.identifier import AgentIdentifier
from aeris.message import ActionRequest
from aeris.message import Message
from aeris.message import PingRequest
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
        self.aid = aid
        self.behavior = behavior
        self.exchange = exchange
        self.done = threading.Event()

        if self.aid is None and self.exchange is None:
            self._mode = _AgentMode.INDIVIDUAL
        else:
            self._mode = _AgentMode.SYSTEM

        self._actions = get_actions(behavior)
        self._loops = get_loops(behavior)

        self._futures: tuple[Future[None], ...] | None = None
        self._start_loops_lock = threading.Lock()
        self._mailbox: Mailbox | None = None

        self._status = _AgentStatus.INITIALIZED

    def __call__(self) -> None:
        """Alias for [run()][aeris.agent.Agent.run]."""
        self.run()

    def __repr__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        if self._mode == _AgentMode.INDIVIDUAL:
            return f'{name}({behavior})'
        else:
            return (
                f'{name}'
                f'({behavior}, aid={self.aid!r}, exchange={self.exchange!r})'
            )

    def __str__(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        status = self._status.value
        if self._mode == _AgentMode.INDIVIDUAL:
            return f'{name}[{behavior}]<{status}>'
        else:
            return f'{name}[{behavior}]<{status}; {self.aid}; {self.exchange}>'

    def _log_prefix(self) -> str:
        name = type(self).__name__
        behavior = type(self.behavior).__name__
        if self._mode == _AgentMode.INDIVIDUAL:
            return f'{name}[{behavior}]'
        else:
            return f'{name}[{behavior};{self.aid}]'

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
        logger.debug(f'{self._log_prefix()} executing "{action}"')
        if action not in self._actions:
            raise TypeError(
                f'Agent[{type(self.behavior).__name__}] does not have an '
                f'action named "{action}".',
            )
        return self._actions[action](*args, **kwargs)

    def _message_handler(self, message: Message) -> Message | None:
        if isinstance(message, ActionRequest):
            try:
                result = self.action(
                    message.action,
                    message.args,
                    message.kwargs,
                )
            except Exception as e:
                return message.response(exception=e)
            else:
                return message.response(result=result)
        elif isinstance(message, PingRequest):
            logger.info(f'{self._log_prefix()} received ping')
            return message.response()
        elif isinstance(message, ShutdownRequest):
            self.shutdown()
            return None
        else:
            raise BadMessageTypeError(
                f'Agent cannot handle message type: {message}',
            )

    def _message_listener(self) -> None:
        if self._mailbox is None:
            raise AssertionError(
                'Message listener started without initializing mailbox.',
            )
        assert self.exchange is not None

        logger.info(f'{self._log_prefix()} is listening for incoming messages')

        while True:
            try:
                message = self._mailbox.recv()
            except MailboxClosedError:
                break

            response = self._message_handler(message)

            if response is not None:
                dest = self.exchange.get_mailbox(response.dest)
                assert dest is not None
                dest.send(response)

    def run(self) -> None:
        """Run the agent.

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
        self.behavior.setup()

        futures: list[Future[None]] = []
        with ThreadPoolExecutor(max_workers=len(self._loops) + 1) as pool:
            with self._start_loops_lock:
                if self.exchange is not None and self.aid is not None:
                    self._mailbox = self.exchange.get_mailbox(self.aid)
                    futures.append(pool.submit(self._message_listener))

                for method in self._loops.values():
                    futures.append(pool.submit(method, self.done))

                self._futures = tuple(futures)
                self._status = _AgentStatus.RUNNING

            logger.info(f'{self._log_prefix()} started')

            for future in as_completed(futures):
                future.result()

        self.behavior.shutdown()
        self._status = _AgentStatus.SHUTDOWN
        logger.info(f'{self._log_prefix()} shutdown')

    def shutdown(self) -> None:
        """Notify control loops to shutdown.

        Sets the shutdown event passed to each control loop method and closes
        the incoming messages mailbox.
        """
        logger.info(f'{self._log_prefix()} shutdown requested')
        self.done.set()
        self._status = _AgentStatus.TERMINTATING

        with self._start_loops_lock:
            if self._mailbox is not None:
                self._mailbox.close()

    def wait(self, timeout: float | None = None) -> None:
        """Wait for control loops to exit.

        Tip:
            This should typically be called after
            [`shutdown()`][aeris.agent.Agent.shutdown] has been called.

        Args:
            timeout: How long to wait for threads to exit gracefully.
        """
        with self._start_loops_lock:
            if self._futures is None:
                return

            wait(self._futures, timeout=timeout)
