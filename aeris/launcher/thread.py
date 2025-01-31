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
        return f'{type(self).__name__}<>'

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        logger.debug('Waiting for all agents to shutdown...')
        for aid in self._agents:
            self._agents[aid].agent.shutdown()
        for aid in self._agents:
            self._agents[aid].thread.join()
        logger.info('Closed %s', self)

    def launch(
        self,
        behavior: BehaviorT,
        exchange: Exchange,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.

        Returns:
            Mailbox used to communicate with agent.
        """
        aid = exchange.create_agent()

        agent = Agent(
            behavior,
            aid=aid,
            exchange=exchange,
            close_exchange=False,
        )
        thread = threading.Thread(target=agent, name=f'{self}-{aid}')
        thread.start()
        self._agents[aid] = _RunningAgent(agent, thread)
        logger.info('Launched %s', agent)

        return exchange.create_handle(aid)
