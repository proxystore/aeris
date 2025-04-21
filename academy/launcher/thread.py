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

from academy.agent import Agent
from academy.agent import AgentRunConfig
from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exchange import Exchange
from academy.handle import RemoteHandle
from academy.identifier import AgentId

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
        self._agents: dict[AgentId[Any], _RunningAgent[Any]] = {}

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
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.
            agent_id: Specify ID of the launched agent. If `None`, a new
                agent ID will be created within the exchange.
            name: Readable name of the agent. Ignored if `agent_id` is
                provided.

        Returns:
            Mailbox used to communicate with agent.
        """
        agent_id = (
            exchange.register_agent(type(behavior), name=name)
            if agent_id is None
            else agent_id
        )

        agent = Agent(
            behavior,
            agent_id=agent_id,
            exchange=exchange,
            config=AgentRunConfig(close_exchange_on_exit=False),
        )
        thread = threading.Thread(target=agent, name=f'{self}-{agent_id}')
        thread.start()
        self._agents[agent_id] = _RunningAgent(agent, thread)
        logger.debug('Launched agent (%s; %s)', agent_id, behavior)

        return exchange.get_handle(agent_id)

    def running(self) -> set[AgentId[Any]]:
        """Get a set of IDs for all running agents.

        Returns:
            Set of agent IDs corresponding to all agents launched by this \
            launcher that have not completed yet.
        """
        running: set[AgentId[Any]] = set()
        for agent_id, agent in self._agents.items():
            if agent.thread.is_alive():
                running.add(agent_id)
        return running

    def wait(
        self,
        agent_id: AgentId[Any],
        *,
        timeout: float | None = None,
    ) -> None:
        """Wait for a launched agent to exit.

        Args:
            agent_id: ID of launched agent.
            timeout: Optional timeout in seconds to wait for agent.

        Raises:
            BadEntityIdError: If an agent with `agent_id` was not
                launched by this launcher.
            TimeoutError: If `timeout` was exceeded while waiting for agent.
        """
        try:
            agent = self._agents[agent_id]
        except KeyError:
            raise BadEntityIdError(agent_id) from None

        agent.thread.join(timeout=timeout)
        if agent.thread.is_alive():
            raise TimeoutError()
