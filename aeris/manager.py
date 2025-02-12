from __future__ import annotations

import contextlib
import logging
import sys
import threading
from types import TracebackType
from typing import Any
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.handle import BoundRemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.launcher import Launcher
from aeris.message import RequestMessage
from aeris.multiplex import MailboxMultiplexer

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class Manager:
    """Launch and manage running agents.

    The manager is provided as convenience to reduce common boilerplate code
    for spawning agents and managing handles. Each manager registers itself
    as a client in the exchange (i.e., each manager has its own mailbox).
    Handles created by the manager are bound to this mailbox.

    Tip:
        This class can be used as a context manager. Upon exiting the context,
        running agents will be shutdown, any agent handles created by the
        manager will be closed, and the exchange and launcher will be closed.

    Note:
        The manager takes ownership of the exchange and launcher interfaces.
        This means the manager will be responsible for closing them once the
        manager is closed.

    Args:
        exchange: Exchange that agents and clients will use for communication.
        launcher: Launcher used to execute agents remotely.
    """

    def __init__(self, exchange: Exchange, launcher: Launcher) -> None:
        self._exchange = exchange
        self._launcher = launcher

        self._mailbox_id = exchange.create_client()
        self._multiplexer = MailboxMultiplexer(
            self.mailbox_id,
            self._exchange,
            self._handle_request,
        )
        self._handles: dict[AgentIdentifier, BoundRemoteHandle[Any]] = {}
        self._listener_thread = threading.Thread(
            target=self._multiplexer.listen,
            name=f'multiplexer-{self.mailbox_id.uid}-listener',
        )
        self._listener_thread.start()
        logger.info(
            'Initialized manager (%s; %s; %s',
            self._mailbox_id,
            self._exchange,
            self._launcher,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}'
            f'(exchange={self._exchange!r}, launcher={self._launcher!r})'
        )

    def __str__(self) -> str:
        return (
            f'{type(self).__name__}<{self.mailbox_id}, {self._exchange}, '
            f'{self._launcher}>'
        )

    @property
    def exchange(self) -> Exchange:
        """Exchange interface."""
        return self._exchange

    @property
    def launcher(self) -> Launcher:
        """Launcher interface."""
        return self._launcher

    @property
    def mailbox_id(self) -> ClientIdentifier:
        """Identifier of the mailbox used by this manager."""
        return self._mailbox_id

    def _handle_request(self, request: RequestMessage) -> None:
        response = request.error(
            TypeError(
                f'Client with {self.mailbox_id} cannot fulfill requests.',
            ),
        )
        self.exchange.send(response.dest, response)

    def close(self) -> None:
        """Close the manager and cleanup resources.

        1. Call shutdown on all running agents.
        1. Close all handles created by the manager.
        1. Close the mailbox associated with the manager.
        1. Close the exchange.
        1. Close the launcher.
        """
        for agent_id in self.launcher.running():
            handle = self._handles[agent_id]
            with contextlib.suppress(MailboxClosedError):
                handle.shutdown()
        logger.debug('Instructed managed agents to shutdown')
        self._multiplexer.close_bound_handles()
        self._multiplexer.close_mailbox()
        self._listener_thread.join()
        self.exchange.close()
        self.launcher.close()
        logger.info('Closed manager (%s)', self.mailbox_id)

    def launch(
        self,
        behavior: BehaviorT,
        *,
        agent_id: AgentIdentifier | None = None,
    ) -> BoundRemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Note:
            Compared to `Launcher.launch()`, this method will inject the
            exchange and return a client-bound handle.

        Args:
            behavior: Behavior the agent should implement.
            agent_id: Specify ID of the launched agent. If `None`, a new
                agent ID will be created within the exchange.

        Returns:
            Handle (client bound) used to interact with the agent.
        """
        unbound = self.launcher.launch(
            behavior,
            exchange=self.exchange,
            agent_id=agent_id,
        )
        logger.info('Launched agent (%s; %s)', unbound.agent_id, behavior)
        bound = self._multiplexer.bind(unbound)
        self._handles[bound.agent_id] = bound
        logger.debug('Bound agent handle to manager (%s)', bound)
        return bound

    def shutdown(
        self,
        agent_id: AgentIdentifier,
        *,
        blocking: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Shutdown a launched agent.

        Args:
            agent_id: ID of launched agent.
            blocking: Wait for the agent to exit before returning.
            timeout: Optional timeout is seconds when `blocking=True`.

        Raises:
            BadIdentifierError: If an agent with `agent_id` was not
                launched by this launcher.
            TimeoutError: If `timeout` was exceeded while blocking for agent.
        """
        try:
            handle = self._handles[agent_id]
        except KeyError:
            raise BadIdentifierError(agent_id) from None

        with contextlib.suppress(MailboxClosedError):
            handle.shutdown()

        if blocking:
            self.wait(agent_id, timeout=timeout)

    def wait(
        self,
        agent_id: AgentIdentifier,
        *,
        timeout: float | None = None,
    ) -> None:
        """Wait for a launched agent to exit.

        Args:
            agent_id: ID of launched agent.
            timeout: Optional timeout in seconds to wait for agent.

        Raises:
            BadIdentifierError: If an agent with `agent_id` was not
                launched by this launcher.
            TimeoutError: If `timeout` was exceeded while waiting for agent.
        """
        self.launcher.wait(agent_id, timeout=timeout)
