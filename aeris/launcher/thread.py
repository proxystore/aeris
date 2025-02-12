from __future__ import annotations

import dataclasses
import logging
import sys
import threading
from types import TracebackType
from typing import Any
from typing import Generic
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.agent import Agent
from aeris.behavior import Behavior
from aeris.exception import BadIdentifierError
from aeris.exchange import Exchange
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


@dataclasses.dataclass
class _RunningAgent(Generic[BehaviorT]):
    agent: Agent[BehaviorT]
    thread: threading.Thread


class ThreadLauncher:
    """Local thread launcher.

    Launch agents in threads within the current process. This launcher is
    useful for local testing as the GIL will limit the performance and
    scalability of agents.
    """

    def __init__(self) -> None:
        self._agents: dict[AgentIdentifier, _RunningAgent[Any]] = {}

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
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return f'{type(self).__name__}'

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        logger.debug('Waiting for agents to shutdown...')
        for aid in self._agents:
            self._agents[aid].agent.shutdown()
        for aid in self._agents:
            self._agents[aid].thread.join()
        logger.debug('Closed launcher (%s)', self)

    def launch(
        self,
        behavior: BehaviorT,
        exchange: Exchange,
        *,
        agent_id: AgentIdentifier | None = None,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.
            agent_id: Specify ID of the launched agent. If `None`, a new
                agent ID will be created within the exchange.

        Returns:
            Mailbox used to communicate with agent.
        """
        agent_id = exchange.create_agent() if agent_id is None else agent_id

        agent = Agent(
            behavior,
            agent_id=agent_id,
            exchange=exchange,
            close_exchange=False,
        )
        thread = threading.Thread(target=agent, name=f'{self}-{agent_id}')
        thread.start()
        self._agents[agent_id] = _RunningAgent(agent, thread)
        logger.debug('Launched agent (%s; %s)', agent_id, behavior)

        return exchange.create_handle(agent_id)

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
        try:
            agent = self._agents[agent_id]
        except KeyError:
            raise BadIdentifierError(agent_id) from None

        agent.thread.join(timeout=timeout)
        if agent.thread.is_alive():
            raise TimeoutError()
